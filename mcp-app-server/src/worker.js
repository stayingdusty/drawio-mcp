/**
 * Cloudflare Workers entry point for the draw.io MCP App server.
 *
 * Uses 4 Durable Objects to manage MCP sessions, sharded by session ID.
 * This spreads memory across multiple DOs while keeping costs low.
 *
 * Pre-requisite: run `node src/build-html.js` to generate src/generated-html.js.
 * Wrangler's [build] command does this automatically before bundling.
 */

import { createServer } from "./shared.js";
import { WebStandardStreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/webStandardStreamableHttp.js";
import { html, xmlReference, mermaidReference, shapeIndex, faviconBase64 } from "./generated-html.js";

const CORS_HEADERS =
{
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
  "Access-Control-Allow-Headers": "Authorization, Content-Type, mcp-session-id, mcp-protocol-version",
  "Access-Control-Expose-Headers": "mcp-session-id, mcp-protocol-version",
};

/** Add CORS headers to an existing Response. */
function withCors(response)
{
  const patched = new Response(response.body, response);

  for (const [k, v] of Object.entries(CORS_HEADERS))
  {
    patched.headers.set(k, v);
  }

  return patched;
}

/**
 * Durable Object that manages MCP sessions for its shard.
 * Maintains a Map of session IDs to their server/transport instances.
 */
export class MCPSessionManager
{
  constructor(state, env)
  {
    this.state = state;
    this.env = env;
    this.sessions = new Map(); // sessionId -> { server, transport, lastAccess }
    this.lastCleanup = 0; // Timestamp of last cleanup
    this.debug = env.DEBUG === "true";
  }

  log(msg)
  {
    if (this.debug) console.log(msg);
  }

  async fetch(request)
  {
    // CORS preflight
    if (request.method === "OPTIONS")
    {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    this.log(`[request] ${request.method} session=${(request.headers.get("mcp-session-id") || "none").slice(0, 8)}`);

    // Extract or generate session ID.
    // For new sessions, ensure the generated UUID routes back to this shard.
    const existingSessionId = request.headers.get("mcp-session-id");
    let sessionId;

    if (existingSessionId)
    {
      sessionId = existingSessionId;
    }
    else
    {
      const shardName = request.headers.get("x-shard-name") || "shard-0";
      const shardIndex = parseInt(shardName.split("-")[1], 10) || 0;

      // Generate UUIDs until we get one whose first hex char maps to this shard.
      // On average takes NUM_SHARDS attempts (4), so very fast.
      do
      {
        sessionId = crypto.randomUUID();
      }
      while (parseInt(sessionId.charAt(0), 16) % NUM_SHARDS !== shardIndex);
    }

    // Get or create session.
    // GET requests (SSE streams) without a valid session must be rejected.
    // POST requests with an unknown session ID (stale/expired) must return
    // 404 so the client knows to re-initialize with a fresh session.
    // Only POST requests WITHOUT a session ID (new connections) create sessions.
    let session = this.sessions.get(sessionId);
    const isNewSession = !session;

    if (!session)
    {
      if (request.method === "GET")
      {
        this.log(`[sessions] REJECTED GET without valid session id=${sessionId.slice(0, 8)} total=${this.sessions.size}`);
        return withCors(new Response("Session not found. Send a POST to initialize first.", { status: 400 }));
      }

      if (existingSessionId)
      {
        // Client sent a session ID we don't recognize — the session was
        // cleaned up or lost (e.g. after a deploy). Re-create it
        // transparently so the client doesn't need to re-initialize.
        this.log(`[session-recreate] stale=${existingSessionId.slice(0, 8)} total=${this.sessions.size}`);
      }
      else
      {
        this.log(`[session-create] domain=${this.env.DOMAIN || "UNDEFINED"} session=${sessionId.slice(0, 8)}`);
      }

      const server = createServer(html, { domain: this.env.DOMAIN, xmlReference, mermaidReference, shapeIndex });
      const transport = new WebStandardStreamableHTTPServerTransport(
      {
        sessionIdGenerator: function() { return sessionId; },
      });

      const self = this;
      transport.onerror = function(err)
      {
        self.log(`[transport-error] session=${sessionId.slice(0, 8)} error=${err.message}`);
      };

      await server.connect(transport);

      // For re-created sessions (stale ID), mark the transport as
      // already initialized so it accepts non-initialize requests.
      // The MCP Server is ready after connect(), only the transport
      // gate needs to be opened.
      if (existingSessionId)
      {
        transport._initialized = true;
        transport.sessionId = sessionId;
      }

      session =
      {
        server,
        transport,
        lastAccess: Date.now(),
        createdAt: Date.now(),
      };

      this.sessions.set(sessionId, session);
    }

    // Update last access time
    session.lastAccess = Date.now();

    const now = Date.now();

    // Periodic cleanup (throttled to once per minute)
    if (now - this.lastCleanup > 60 * 1000)
    {
      this.cleanupStaleSessions();
      this.lastCleanup = now;
    }

    // Log the JSON-RPC method for POST requests
    let rpcMethod = "";

    if (request.method === "POST")
    {
      try
      {
        const cloned = request.clone();
        const body = await cloned.json();
        rpcMethod = Array.isArray(body)
          ? body.map(function(m) { return m.method || "response"; }).join(",")
          : (body.method || "response");
        this.log(`[rpc] ${rpcMethod} session=${sessionId.slice(0, 8)}${isNewSession ? " NEW" : ""}`);
      }
      catch (e)
      {
        this.log(`[rpc] body-parse-failed session=${sessionId.slice(0, 8)} error=${e.message}`);
      }
    }

    // DELETE = client explicitly terminating the session. Remove it immediately.
    if (request.method === "DELETE")
    {
      this.log(`[session-delete] session=${sessionId.slice(0, 8)}`);
      const resp = await session.transport.handleRequest(request);
      this.sessions.delete(sessionId);
      return withCors(resp);
    }

    // Handle the MCP request.
    // Clients that accept text/event-stream (e.g. Claude Desktop) use SSE as usual.
    // Clients that accept application/json (e.g. Claude.ai) get a plain JSON response
    // via the SDK's enableJsonResponse code path. When a client accepts both, prefer JSON.
    const acceptHeader = request.headers.get("accept") || "";
    const acceptsJson = acceptHeader.includes("application/json");
    const acceptsSSE = acceptHeader.includes("text/event-stream");
    const wantsSSE = acceptsSSE && !acceptsJson;
    const mode = wantsSSE ? "SSE" : "JSON";
    const startTime = Date.now();
    let response;

    try
    {
      if (wantsSSE)
      {
        response = await session.transport.handleRequest(request);
      }
      else
      {
        const headers = new Headers(request.headers);
        headers.set("accept", "application/json, text/event-stream");

        const patchedRequest = new Request(request, { headers });

        session.transport._enableJsonResponse = true;

        try
        {
          response = await session.transport.handleRequest(patchedRequest);
        }
        finally
        {
          session.transport._enableJsonResponse = false;
        }
      }
    }
    catch (err)
    {
      const elapsed = Date.now() - startTime;
      this.log(`[response] ERROR ${request.method} session=${sessionId.slice(0, 8)} mode=${mode} elapsed=${elapsed}ms error=${err.message}`);
      throw err;
    }

    const elapsed = Date.now() - startTime;
    const status = response ? response.status : "null";
    this.log(`[response] ${request.method} session=${sessionId.slice(0, 8)} mode=${mode} status=${status} elapsed=${elapsed}ms`);

    // Log response body for key methods to debug what Claude.ai sees
    const debugMethods = ["resources/list", "resources/read", "tools/call", "tools/list"];

    if (this.debug && response && mode === "JSON" && debugMethods.includes(rpcMethod))
    {
      try
      {
        const respClone = response.clone();
        const respBody = await respClone.text();

        if (rpcMethod === "resources/list" || rpcMethod === "resources/read")
        {
          // Log full response (minus the HTML blob for resources/read)
          const parsed = JSON.parse(respBody);

          if (parsed.result && parsed.result.contents)
          {
            // resources/read — log metadata but truncate the HTML text
            const summary = parsed.result.contents.map(function(c)
            {
              return {
                uri: c.uri,
                mimeType: c.mimeType,
                textLength: c.text ? c.text.length : 0,
                _meta: c._meta,
              };
            });
            console.log(`[response-body] ${rpcMethod} session=${sessionId.slice(0, 8)} contents=${JSON.stringify(summary)}`);
          }
          else
          {
            // resources/list — log the full response (small)
            console.log(`[response-body] ${rpcMethod} session=${sessionId.slice(0, 8)} body=${respBody.slice(0, 2000)}`);
          }
        }
        else
        {
          // tools/list, tools/call — truncate at 500 chars
          console.log(`[response-body] ${rpcMethod} session=${sessionId.slice(0, 8)} body=${respBody.slice(0, 500)}`);
        }
      }
      catch (e)
      {
        console.log(`[response-body] ${rpcMethod} parse-failed session=${sessionId.slice(0, 8)} error=${e.message}`);
      }
    }

    return withCors(response);
  }

  /**
   * Remove sessions that haven't been accessed in the last 5 minutes.
   */
  cleanupStaleSessions()
  {
    const now = Date.now();
    const STALE_TIMEOUT = 5 * 60 * 1000;
    let cleaned = 0;
    const removedIds = [];

    for (const [id, session] of this.sessions.entries())
    {
      if (now - session.lastAccess > STALE_TIMEOUT)
      {
        const idleMinutes = Math.round((now - session.lastAccess) / 60000);
        const ageMinutes = Math.round((now - session.createdAt) / 60000);
        removedIds.push({ id: id.slice(0, 8), idle: idleMinutes, age: ageMinutes });

        // Close transport and server to release resources
        try { session.transport.close(); } catch (e) { /* ignore */ }
        try { session.server.close(); } catch (e) { /* ignore */ }

        this.sessions.delete(id);
        cleaned++;
      }
    }

    // Session summary (logged here once per minute instead of every request)
    let oldestAge = 0;
    let maxIdle = 0;
    let idleOver1m = 0;

    for (const [, s] of this.sessions.entries())
    {
      const age = now - s.createdAt;
      const idle = now - s.lastAccess;

      if (age > oldestAge) oldestAge = age;
      if (idle > maxIdle) maxIdle = idle;
      if (idle > 60000) idleOver1m++;
    }

    this.log(`[cleanup] checked=${this.sessions.size + cleaned} removed=${cleaned} remaining=${this.sessions.size} oldest=${Math.round(oldestAge / 60000)}m maxIdle=${Math.round(maxIdle / 60000)}m idle>1m=${idleOver1m}`);

    if (removedIds.length > 0)
    {
      this.log(`[cleanup] removed: ${JSON.stringify(removedIds)}`);
    }
  }
}

/**
 * Build OAuth 2.0 Authorization Server Metadata for the given base URL.
 * Enables MCP clients that require OAuth (e.g. Copilot Studio) to discover
 * the auth endpoints. Since this server is public and needs no real auth,
 * the flow issues tokens without requiring credentials.
 */
function buildOAuthMetadata(baseUrl)
{
  return {
    issuer: baseUrl,
    authorization_endpoint: baseUrl + "/oauth/authorize",
    token_endpoint: baseUrl + "/oauth/token",
    registration_endpoint: baseUrl + "/oauth/register",
    response_types_supported: ["code"],
    grant_types_supported: ["authorization_code"],
    code_challenge_methods_supported: ["S256"],
    token_endpoint_auth_methods_supported: ["none"],
    scopes_supported: ["mcp"],
  };
}

/**
 * Handle GET /.well-known/oauth-authorization-server
 * Required by OAuth 2.0 clients (e.g. Copilot Studio) for server discovery.
 */
function handleOAuthMetadata(request)
{
  const url = new URL(request.url);
  const baseUrl = url.origin;
  return new Response(JSON.stringify(buildOAuthMetadata(baseUrl)),
  {
    headers: { "Content-Type": "application/json", ...CORS_HEADERS },
  });
}

/**
 * Handle POST /oauth/register (RFC 7591 Dynamic Client Registration).
 * Returns a public client ID so any OAuth client can register.
 */
async function handleOAuthRegister(request)
{
  let body = {};

  try
  {
    body = await request.json();
  }
  catch (e) { /* ignore malformed body */ }

  const registration =
  {
    client_id: "drawio-mcp-public-client",
    client_secret: "drawio-mcp-public-secret",
    redirect_uris: body.redirect_uris || [],
    grant_types: ["authorization_code"],
    response_types: ["code"],
    token_endpoint_auth_method: "none",
    client_name: body.client_name || "draw.io MCP Client",
  };

  return new Response(JSON.stringify(registration),
  {
    status: 201,
    headers: { "Content-Type": "application/json", ...CORS_HEADERS },
  });
}

/**
 * Handle GET /oauth/authorize
 * Immediately redirects to redirect_uri with an authorization code.
 * No login required — this server is public.
 */
function handleOAuthAuthorize(request)
{
  const url = new URL(request.url);
  const redirectUri = url.searchParams.get("redirect_uri");
  const state = url.searchParams.get("state");

  if (!redirectUri)
  {
    return new Response("Missing redirect_uri", { status: 400, headers: CORS_HEADERS });
  }

  let callbackUrl;

  try
  {
    callbackUrl = new URL(redirectUri);
  }
  catch (e)
  {
    return new Response("Invalid redirect_uri", { status: 400, headers: CORS_HEADERS });
  }

  callbackUrl.searchParams.set("code", "drawio-mcp-auth-code");

  if (state)
  {
    callbackUrl.searchParams.set("state", state);
  }

  return Response.redirect(callbackUrl.toString(), 302);
}

/**
 * Handle POST /oauth/token
 * Accepts any authorization code and returns a public bearer token.
 */
async function handleOAuthToken()
{
  const token =
  {
    access_token: "drawio-mcp-public-token",
    token_type: "bearer",
    expires_in: 86400,
    scope: "mcp",
  };

  return new Response(JSON.stringify(token),
  {
    headers: { "Content-Type": "application/json", ...CORS_HEADERS },
  });
}

/**
 * Number of Durable Object shards to spread sessions across.
 * Sessions are routed to a shard based on the first hex char of the session ID.
 */
const NUM_SHARDS = 4;

/**
 * Pick a shard name (0..NUM_SHARDS-1) from a session ID.
 * New sessions (no session ID header) get a random shard.
 */
function getShardName(sessionId)
{
  if (!sessionId)
  {
    return "shard-" + Math.floor(Math.random() * NUM_SHARDS);
  }

  // Use first hex char of the UUID to deterministically pick a shard
  const firstChar = sessionId.charAt(0);
  const index = parseInt(firstChar, 16) % NUM_SHARDS;

  return "shard-" + index;
}

/**
 * Main Worker: routes /mcp requests to one of NUM_SHARDS Durable Objects.
 */
export default
{
  async fetch(request, env)
  {
    // CORS preflight
    if (request.method === "OPTIONS")
    {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    const url = new URL(request.url);

    // Serve favicon so Google's favicon service picks up the draw.io logo
    if (url.pathname === "/favicon.ico" || url.pathname === "/favicon.png")
    {
      const bytes = Uint8Array.from(atob(faviconBase64), function(c) { return c.charCodeAt(0); });

      return new Response(bytes,
      {
        headers:
        {
          "Content-Type": "image/png",
          "Cache-Control": "public, max-age=604800",
          ...CORS_HEADERS,
        },
      });
    }

    // OAuth 2.0 endpoints (required by Copilot Studio and other OAuth-enforcing MCP clients)
    if (url.pathname === "/.well-known/oauth-authorization-server")
    {
      return handleOAuthMetadata(request);
    }

    if (url.pathname === "/oauth/register" && request.method === "POST")
    {
      return handleOAuthRegister(request);
    }

    if (url.pathname === "/oauth/authorize")
    {
      return handleOAuthAuthorize(request);
    }

    if (url.pathname === "/oauth/token" && request.method === "POST")
    {
      return handleOAuthToken();
    }

    // Only serve /mcp
    if (url.pathname !== "/mcp")
    {
      return new Response("Not Found", { status: 404 });
    }

    // Route to a Durable Object shard based on session ID
    const sessionId = request.headers.get("mcp-session-id");
    const shardName = getShardName(sessionId);
    const durableObjectId = env.MCP_SESSION_MANAGER.idFromName(shardName);
    const stub = env.MCP_SESSION_MANAGER.get(durableObjectId);

    // Pass the shard name to the DO so it can generate compatible session IDs
    const doRequest = new Request(request, {
      headers: new Headers([...request.headers.entries(), ["x-shard-name", shardName]]),
    });

    return stub.fetch(doRequest);
  },
};
