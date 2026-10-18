// Agent Bluetooth — relay server

import { WebSocketServer, WebSocket } from "ws";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { upsertAgent, setAgentOffline, setAgentOnline, listDiscoverableAgents, getAgent, expireDiscoverable, createProvider, listProviders, deleteProvider, validateToken, type Db } from "./schema.js";

interface AgentSession {
  ws: WebSocket;
  orgId: string;
  agentId: string;
  id: string;
  displayName: string;
  description: string;
  cardJson: string;
}

interface PendingRequest {
  resolve: (data: any) => void;
  timer: ReturnType<typeof setTimeout>;
}

export interface ServerOpts {
  db: Db;
  port: number;
  publicUrl: string;
  adminKeys?: string[];
}

export function startServer(opts: ServerOpts) {
  const { db, port, publicUrl, adminKeys = [] } = opts;
  const adminKeySet = new Set(adminKeys);

  function requireAdmin(req: IncomingMessage, res: ServerResponse): boolean {
    if (adminKeySet.size === 0) return true; // no keys configured = open
    const auth = req.headers.authorization || "";
    const token = auth.startsWith("Bearer ") ? auth.slice(7) : "";
    if (token && adminKeySet.has(token)) return true;
    json(res, 401, { error: "admin_key_required" });
    return false;
  }
  const sessions = new Map<string, AgentSession>();
  const pending = new Map<string, PendingRequest>();

  const httpServer = createServer((req, res) => handleHttp(req, res));
  const wss = new WebSocketServer({ server: httpServer });

  wss.on("connection", (ws) => {
    let session: AgentSession | null = null;

    ws.on("message", (raw) => {
      let msg: any;
      try { msg = JSON.parse(String(raw)); } catch { return; }

      if (msg.type === "register") {
        // Token-based auth: validate token to get provider identity
        let orgId = msg.orgId || "";
        let providerRef = "";
        let providerName = "";
        let providerId = "";
        if (msg.token) {
          const provider = validateToken(db, msg.token);
          if (!provider) {
            ws.send(JSON.stringify({ type: "error", error: "invalid_token" }));
            ws.close();
            return;
          }
          // Use providerId as orgId — server-authoritative identity
          orgId = provider.providerId;
          providerRef = provider.id;
          providerName = provider.providerName;
          providerId = provider.providerId;
        }
        if (!orgId || !msg.agentId) {
          ws.send(JSON.stringify({ type: "error", error: "orgId and agentId required (use token or orgId)" }));
          return;
        }
        const id = `${orgId}/${msg.agentId}`;
        session = { ws, orgId, agentId: msg.agentId, id, displayName: msg.card?.name || msg.agentId, description: msg.card?.description || "", cardJson: JSON.stringify(msg.card || {}) };
        sessions.set(id, session);

        upsertAgent(db, {
          orgId, agentId: msg.agentId, providerRef,
          displayName: msg.card?.name || msg.agentId,
          description: msg.card?.description || "",
          cardJson: JSON.stringify({ ...(msg.card || {}), provider: providerName, providerId }),
          apiKeyHash: "", discoverable: msg.discoverable ?? false,
        });

        const currentAgent = getAgent(db, orgId, msg.agentId);
        ws.send(JSON.stringify({
          type: "registered",
          publicUrl: `${publicUrl}/u/${orgId}/${msg.agentId}`,
          agentPath: `${orgId}/${msg.agentId}`,
          orgId,
          providerId,
          providerName,
          discoverable: currentAgent?.discoverable ?? false,
        }));
        console.log(`[bluetooth] agent registered: ${id} (provider=${providerId || 'none'}, discoverable: ${msg.discoverable ?? false})`);
      }

      if (msg.type === "response" && msg.requestId) {
        const p = pending.get(msg.requestId);
        if (p) { clearTimeout(p.timer); pending.delete(msg.requestId); p.resolve({ status: msg.status || 200, headers: msg.headers || {}, body: msg.body || "" }); }
      }

      // ── Pair request: A wants to pair with B (async, no token) ──
      if (msg.type === "pair_request" && session && msg.targetOrgId && msg.targetAgentId) {
        const targetId = `${msg.targetOrgId}/${msg.targetAgentId}`;
        const targetSession = sessions.get(targetId);
        if (!targetSession || targetSession.ws.readyState !== WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: "pair_response", success: false, error: "agent_offline" }));
        } else {
          const targetAgent = getAgent(db, msg.targetOrgId, msg.targetAgentId);
          if (!targetAgent || !targetAgent.discoverable) {
            ws.send(JSON.stringify({ type: "pair_response", success: false, error: "not_discoverable" }));
          } else {
            const requestId = msg.requestId || crypto.randomUUID();
            // Forward to target — no token, just identity info
            targetSession.ws.send(JSON.stringify({
              type: "pair_request",
              requestId,
              fromOrgId: session.orgId,
              fromAgentId: session.agentId,
              fromName: msg.name || session.displayName,
              fromCard: msg.card,
              fromEndpoint: msg.userId ? `${publicUrl}/u/${session.orgId}/${session.agentId}/${msg.userId}` : `${publicUrl}/u/${session.orgId}/${session.agentId}`,
            }));
            // Immediately ack to sender — request delivered, waiting for user confirmation
            ws.send(JSON.stringify({ type: "pair_response", success: true, status: "pending", requestId }));
            console.log(`[bluetooth] pair_request forwarded: ${session.id} -> ${targetId} (requestId=${requestId})`);
          }
        }
      }

      // ── Pair accept: B confirms pairing with A ──
      if (msg.type === "pair_accept" && session && msg.targetOrgId && msg.targetAgentId) {
        const targetId = `${msg.targetOrgId}/${msg.targetAgentId}`;
        const targetSession = sessions.get(targetId);
        if (targetSession && targetSession.ws.readyState === WebSocket.OPEN) {
          targetSession.ws.send(JSON.stringify({
            type: "pair_accepted",
            requestId: msg.requestId,
            fromOrgId: session.orgId,
            fromAgentId: session.agentId,
            fromName: msg.name,
            fromCard: msg.card,
            fromToken: msg.token,  // B's comm token for A to use
            fromEndpoint: `${publicUrl}/u/${session.orgId}/${session.agentId}`,
          }));
          console.log(`[bluetooth] pair_accept: ${session.id} -> ${targetId}`);
        }
      }

      // ── Pair reject: B rejects A's request ──
      if (msg.type === "pair_reject" && session && msg.targetOrgId && msg.targetAgentId) {
        const targetId = `${msg.targetOrgId}/${msg.targetAgentId}`;
        const targetSession = sessions.get(targetId);
        if (targetSession && targetSession.ws.readyState === WebSocket.OPEN) {
          targetSession.ws.send(JSON.stringify({
            type: "pair_rejected",
            requestId: msg.requestId,
            fromOrgId: session.orgId,
            fromAgentId: session.agentId,
          }));
          console.log(`[bluetooth] pair_reject: ${session.id} -> ${targetId}`);
        }
      }

      // ── Pair complete: A sends back its comm token after receiving B's ──
      if (msg.type === "pair_complete" && session && msg.targetOrgId && msg.targetAgentId) {
        const targetId = `${msg.targetOrgId}/${msg.targetAgentId}`;
        const targetSession = sessions.get(targetId);
        if (targetSession && targetSession.ws.readyState === WebSocket.OPEN) {
          targetSession.ws.send(JSON.stringify({
            type: "pair_completed",
            requestId: msg.requestId,
            fromOrgId: session.orgId,
            fromAgentId: session.agentId,
            fromName: msg.name,
            fromCard: msg.card,
            fromToken: msg.token,  // A's comm token for B to use
            fromEndpoint: `${publicUrl}/u/${session.orgId}/${session.agentId}`,
          }));
          console.log(`[bluetooth] pair_complete: ${session.id} -> ${targetId}`);
        }
      }


      if (msg.type === "set_discoverable" && session) {
        // Parse JWT exp to set discoverable_until
        let expiresAt = 0;
        if (msg.discoverable && msg.pairToken) {
          try {
            const payload = JSON.parse(Buffer.from(msg.pairToken.split(".")[1], "base64url").toString());
            if (payload.exp) expiresAt = payload.exp * 1000; // JWT exp is seconds
          } catch { /* ignore parse errors */ }
        }
        db.prepare("UPDATE agents SET discoverable = ?, pair_token = ?, discoverable_until = ?, updated_at = ? WHERE id = ?").run(
          msg.discoverable ? 1 : 0,
          msg.discoverable ? (msg.pairToken || "") : "",
          msg.discoverable ? expiresAt : 0,
          Date.now(),
          session.id,
        );
        ws.send(JSON.stringify({ type: "discoverable_updated", discoverable: !!msg.discoverable }));
        console.log(`[bluetooth] ${session.id} discoverable=${!!msg.discoverable} expiresAt=${expiresAt ? new Date(expiresAt).toISOString() : "none"}`);
      }

      if (msg.type === "update_card" && session) {
        const card = msg.card || {};
        db.prepare("UPDATE agents SET display_name = ?, description = ?, card_json = ?, updated_at = ? WHERE id = ?").run(
          card.name || session.displayName,
          card.description || "",
          JSON.stringify(card),
          Date.now(),
          session.id,
        );
        session.displayName = card.name || session.displayName;
        session.description = card.description || "";
        session.cardJson = JSON.stringify(card);
        ws.send(JSON.stringify({ type: "card_updated" }));
        console.log(`[bluetooth] ${session.id} card updated: ${card.name}`);
      }

      if (msg.type === "ping") {
        ws.send('{"type":"pong"}');
        if (session) setAgentOnline(db, session.id);
      }
    });

    ws.on("close", () => {
      if (session) { sessions.delete(session.id); setAgentOffline(db, session.id); console.log(`[bluetooth] agent disconnected: ${session.id}`); }
    });
    ws.on("error", () => {
      if (session) { sessions.delete(session.id); setAgentOffline(db, session.id); }
    });
  });

  async function handleHttp(req: IncomingMessage, res: ServerResponse) {
    const url = req.url || "/";
    const method = req.method || "GET";

    // CORS
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
    if (method === "OPTIONS") { res.writeHead(204); res.end(); return; }

    // Discovery
    if (url === "/discovery" || url === "/discovery/") {
      const agents = listDiscoverableAgents(db);
      const cards = agents.map((a) => ({
        orgId: a.orgId, agentId: a.agentId, name: a.displayName,
        description: a.description, online: a.online,
        url: `${publicUrl}/u/${a.orgId}/${a.agentId}`,
        card: JSON.parse(a.cardJson),
      }));
      json(res, 200, { agents: cards });
      return;
    }

    // Agent Card
    const cardMatch = url.match(/^\/u\/([^/]+)\/([^/]+)\/\.well-known\/agent-card\.json$/);
    if (cardMatch && method === "GET") {
      const agent = getAgent(db, cardMatch[1], cardMatch[2]);
      if (!agent) { json(res, 404, { error: "not_found" }); return; }
      const card = JSON.parse(agent.cardJson);
      card.url = `${publicUrl}/u/${agent.orgId}/${agent.agentId}`;
      json(res, 200, card);
      return;
    }

    // A2A proxy: /u/{orgId}/{agentId}/...
    const proxyMatch = url.match(/^\/u\/([^/]+)\/([^/]+)(\/.*)?$/);
    if (proxyMatch) {
      const [, orgId, agentId, subPath] = proxyMatch;
      const id = `${orgId}/${agentId}`;
      const session = sessions.get(id);
      if (!session || session.ws.readyState !== WebSocket.OPEN) {
        json(res, 502, { error: "agent_offline", message: "Agent is not connected" });
        return;
      }

      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(chunk as Buffer);
      const body = Buffer.concat(chunks).toString("utf-8");

      const requestId = crypto.randomUUID();
      const responsePromise = new Promise<{ status: number; headers: Record<string, string>; body: string }>((resolve) => {
        const timer = setTimeout(() => { pending.delete(requestId); resolve({ status: 504, headers: {}, body: '{"error":"gateway_timeout"}' }); }, 5 * 60 * 1000);
        pending.set(requestId, { resolve, timer });
      });

      session.ws.send(JSON.stringify({
        type: "request", requestId, method, path: subPath || "/",
        headers: Object.fromEntries(Object.entries(req.headers).filter(([k]) => !["host", "connection", "upgrade"].includes(k))),
        body,
      }));

      const resp = await responsePromise;
      for (const [k, v] of Object.entries(resp.headers)) res.setHeader(k, v);
      res.writeHead(resp.status, { "Content-Type": resp.headers["content-type"] || "application/json" });
      res.end(resp.body);
      return;
    }

    // ── Admin: Provider management (requires admin key) ──
    if (url === "/admin/providers" && method === "GET") {
      if (!requireAdmin(req, res)) return;
      json(res, 200, { providers: listProviders(db) });
      return;
    }
    if (url === "/admin/providers" && method === "POST") {
      if (!requireAdmin(req, res)) return;
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(chunk as Buffer);
      let body: any;
      try { body = JSON.parse(Buffer.concat(chunks).toString()); } catch { json(res, 400, { error: "invalid_json" }); return; }
      const { providerId, providerName } = body;
      if (!providerId) { json(res, 400, { error: "providerId required" }); return; }
      try {
        const result = createProvider(db, providerId, providerName || providerId);
        console.log(`[bluetooth] provider created: ${providerId}`);
        json(res, 201, { ok: true, provider: result.provider, token: result.token });
      } catch (err: any) {
        if (err.message?.includes("UNIQUE")) json(res, 409, { error: "providerId already exists" });
        else json(res, 500, { error: String(err) });
      }
      return;
    }
    const providerDeleteMatch = url.match(/^\/admin\/providers\/([^/]+)$/);
    if (providerDeleteMatch && method === "DELETE") {
      if (!requireAdmin(req, res)) return;
      const ok = deleteProvider(db, decodeURIComponent(providerDeleteMatch[1]));
      json(res, ok ? 200 : 404, ok ? { ok: true } : { error: "not_found" });
      return;
    }

    // Health
    if (url === "/healthz") { res.writeHead(200); res.end("ok"); return; }

    // Stats
    if (url === "/stats") {
      json(res, 200, {
        connectedAgents: sessions.size,
        discoverableAgents: listDiscoverableAgents(db).length,
      });
      return;
    }

    json(res, 404, { error: "not_found" });
  }

  function json(res: ServerResponse, status: number, data: unknown) {
    res.writeHead(status, { "Content-Type": "application/json" });
    res.end(JSON.stringify(data));
  }

  httpServer.listen(port, () => {
    console.log(`\n  🔵 Agent Bluetooth server running on port ${port}`);
    console.log(`  📡 Discovery: ${publicUrl}/discovery`);
    console.log(`  ❤️  Health:    ${publicUrl}/healthz\n`);
  });

  // Periodic cleanup: expire discoverable agents whose pair token has expired
  const cleanupTimer = setInterval(() => {
    // Find about-to-expire agents first, so we can notify them
    const now = Date.now();
    const expiring = db.prepare(
      "SELECT id FROM agents WHERE discoverable = 1 AND discoverable_until > 0 AND discoverable_until < ?"
    ).all(now) as { id: string }[];
    if (expiring.length > 0) {
      const expired = expireDiscoverable(db);
      console.log(`[bluetooth] expired ${expired} discoverable agent(s)`);
      // Notify connected sessions
      for (const row of expiring) {
        const s = sessions.get(row.id);
        if (s && s.ws.readyState === WebSocket.OPEN) {
          s.ws.send(JSON.stringify({ type: "discoverable_expired" }));
        }
      }
    }
  }, 30_000); // check every 30s
  cleanupTimer.unref();

  return { httpServer, wss };
}
