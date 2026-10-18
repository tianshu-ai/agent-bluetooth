// Agent Bluetooth — relay server

import { WebSocketServer, WebSocket } from "ws";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { upsertAgent, setAgentOffline, setAgentOnline, listDiscoverableAgents, getAgent, getAgentById, expireDiscoverable, createProvider, listProviders, deleteProvider, validateToken, buildAgentId, buildAgentPath, type Db } from "./schema.js";

interface AgentSession {
  ws: WebSocket;
  orgId: string;
  agentId: string;
  userId: string;
  id: string;
  displayName: string;
  description: string;
  cardJson: string;
}

/** Per-WS connection state: one connection can register multiple agents */
interface WsConnection {
  ws: WebSocket;
  orgId: string;
  providerId: string;
  providerRef: string;
  providerName: string;
  agents: Map<string, AgentSession>;  // id -> session
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
    if (adminKeySet.size === 0) return true;
    const auth = req.headers.authorization || "";
    const token = auth.startsWith("Bearer ") ? auth.slice(7) : "";
    if (token && adminKeySet.has(token)) return true;
    json(res, 401, { error: "admin_key_required" });
    return false;
  }

  // Global agent session lookup (agent id -> session)
  const sessions = new Map<string, AgentSession>();
  const pending = new Map<string, PendingRequest>();

  const httpServer = createServer((req, res) => handleHttp(req, res));
  const wss = new WebSocketServer({ server: httpServer });

  wss.on("connection", (ws) => {
    const conn: WsConnection = { ws, orgId: "", providerId: "", providerRef: "", providerName: "", agents: new Map() };

    ws.on("message", (raw) => {
      let msg: any;
      try { msg = JSON.parse(String(raw)); } catch { return; }

      if (msg.type === "register") {
        let orgId = conn.orgId;
        let providerRef = conn.providerRef;
        let providerName = conn.providerName;
        let providerId = conn.providerId;

        // First register on this connection: authenticate
        if (!orgId && msg.token) {
          const provider = validateToken(db, msg.token);
          if (!provider) {
            ws.send(JSON.stringify({ type: "error", error: "invalid_token" }));
            ws.close();
            return;
          }
          orgId = provider.providerId;
          providerRef = provider.id;
          providerName = provider.providerName;
          providerId = provider.providerId;
          conn.orgId = orgId;
          conn.providerRef = providerRef;
          conn.providerName = providerName;
          conn.providerId = providerId;
        } else if (!orgId && msg.orgId) {
          orgId = msg.orgId;
          conn.orgId = orgId;
        }

        if (!orgId || !msg.agentId) {
          ws.send(JSON.stringify({ type: "error", error: "orgId and agentId required (use token or orgId)" }));
          return;
        }

        const userId = msg.userId || "";
        const id = buildAgentId(orgId, msg.agentId, userId || undefined);
        const session: AgentSession = {
          ws, orgId, agentId: msg.agentId, userId, id,
          displayName: msg.card?.name || msg.agentId,
          description: msg.card?.description || "",
          cardJson: JSON.stringify(msg.card || {}),
        };

        // Track in both connection-local and global maps
        conn.agents.set(id, session);
        sessions.set(id, session);

        upsertAgent(db, {
          orgId, agentId: msg.agentId, userId, providerRef,
          displayName: msg.card?.name || msg.agentId,
          description: msg.card?.description || "",
          cardJson: JSON.stringify({ ...(msg.card || {}), provider: providerName, providerId }),
          apiKeyHash: "", discoverable: msg.discoverable ?? false,
        });

        const agentPath = buildAgentPath(orgId, msg.agentId, userId || undefined);
        const currentAgent = getAgent(db, orgId, msg.agentId, userId || undefined);
        ws.send(JSON.stringify({
          type: "registered",
          publicUrl: `${publicUrl}${agentPath}`,
          agentPath: agentPath.slice(3),
          orgId,
          providerId,
          providerName,
          userId,
          agentId: msg.agentId,
          discoverable: currentAgent?.discoverable ?? false,
        }));
        console.log(`[bluetooth] agent registered: ${id} (provider=${providerId || 'none'}, userId=${userId || 'none'}, discoverable: ${msg.discoverable ?? false}) [${conn.agents.size} on this ws]`);
      }

      // ── Unregister a single agent from this connection ──
      if (msg.type === "unregister" && msg.agentId) {
        const userId = msg.userId || "";
        const id = buildAgentId(conn.orgId, msg.agentId, userId || undefined);
        const session = conn.agents.get(id);
        if (session) {
          conn.agents.delete(id);
          sessions.delete(id);
          setAgentOffline(db, id);
          ws.send(JSON.stringify({ type: "unregistered", agentId: msg.agentId, userId }));
          console.log(`[bluetooth] agent unregistered: ${id}`);
        }
      }

      if (msg.type === "response" && msg.requestId) {
        const p = pending.get(msg.requestId);
        if (p) { clearTimeout(p.timer); pending.delete(msg.requestId); p.resolve({ status: msg.status || 200, headers: msg.headers || {}, body: msg.body || "" }); }
      }

      // ── Resolve the "from" session for pairing messages ──
      // For multi-agent connections, use msg.fromUserId/msg.fromAgentId to find the right session
      function resolveFromSession(): AgentSession | null {
        if (conn.agents.size === 1) return conn.agents.values().next().value!;
        // Try to find by explicit fields
        const fromAgentId = msg.fromAgentId || msg.agentId;
        const fromUserId = msg.fromUserId || msg.userId || "";
        if (fromAgentId) {
          const id = buildAgentId(conn.orgId, fromAgentId, fromUserId || undefined);
          return conn.agents.get(id) || null;
        }
        return conn.agents.values().next().value || null;
      }

      // ── Pair request: A wants to pair with B ──
      if (msg.type === "pair_request" && msg.targetOrgId && msg.targetAgentId) {
        const fromSession = resolveFromSession();
        if (!fromSession) return;
        const targetUserId = msg.targetUserId || "";
        const targetId = buildAgentId(msg.targetOrgId, msg.targetAgentId, targetUserId || undefined);
        const targetSession = sessions.get(targetId);
        if (!targetSession || targetSession.ws.readyState !== WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: "pair_response", success: false, error: "agent_offline" }));
        } else {
          const targetAgent = getAgentById(db, targetId);
          if (!targetAgent || !targetAgent.discoverable) {
            ws.send(JSON.stringify({ type: "pair_response", success: false, error: "not_discoverable" }));
          } else {
            const requestId = msg.requestId || crypto.randomUUID();
            const fromPath = buildAgentPath(fromSession.orgId, fromSession.agentId, fromSession.userId || undefined);
            targetSession.ws.send(JSON.stringify({
              type: "pair_request",
              requestId,
              fromOrgId: fromSession.orgId,
              fromAgentId: fromSession.agentId,
              fromUserId: fromSession.userId,
              fromName: msg.name || fromSession.displayName,
              fromCard: msg.card,
              fromEndpoint: `${publicUrl}${fromPath}`,
            }));
            ws.send(JSON.stringify({ type: "pair_response", success: true, status: "pending", requestId }));
            console.log(`[bluetooth] pair_request forwarded: ${fromSession.id} -> ${targetId} (requestId=${requestId})`);
          }
        }
      }

      // ── Pair accept ──
      if (msg.type === "pair_accept" && msg.targetOrgId && msg.targetAgentId) {
        const fromSession = resolveFromSession();
        if (!fromSession) return;
        const targetUserId = msg.targetUserId || "";
        const targetId = buildAgentId(msg.targetOrgId, msg.targetAgentId, targetUserId || undefined);
        const targetSession = sessions.get(targetId);
        if (targetSession && targetSession.ws.readyState === WebSocket.OPEN) {
          const fromPath = buildAgentPath(fromSession.orgId, fromSession.agentId, fromSession.userId || undefined);
          targetSession.ws.send(JSON.stringify({
            type: "pair_accepted",
            requestId: msg.requestId,
            fromOrgId: fromSession.orgId,
            fromAgentId: fromSession.agentId,
            fromUserId: fromSession.userId,
            fromName: msg.name,
            fromCard: msg.card,
            fromToken: msg.token,
            fromEndpoint: `${publicUrl}${fromPath}`,
          }));
          console.log(`[bluetooth] pair_accept: ${fromSession.id} -> ${targetId}`);
        }
      }

      // ── Pair reject ──
      if (msg.type === "pair_reject" && msg.targetOrgId && msg.targetAgentId) {
        const fromSession = resolveFromSession();
        if (!fromSession) return;
        const targetUserId = msg.targetUserId || "";
        const targetId = buildAgentId(msg.targetOrgId, msg.targetAgentId, targetUserId || undefined);
        const targetSession = sessions.get(targetId);
        if (targetSession && targetSession.ws.readyState === WebSocket.OPEN) {
          targetSession.ws.send(JSON.stringify({
            type: "pair_rejected",
            requestId: msg.requestId,
            fromOrgId: fromSession.orgId,
            fromAgentId: fromSession.agentId,
            fromUserId: fromSession.userId,
          }));
          console.log(`[bluetooth] pair_reject: ${fromSession.id} -> ${targetId}`);
        }
      }

      // ── Pair complete ──
      if (msg.type === "pair_complete" && msg.targetOrgId && msg.targetAgentId) {
        const fromSession = resolveFromSession();
        if (!fromSession) return;
        const targetUserId = msg.targetUserId || "";
        const targetId = buildAgentId(msg.targetOrgId, msg.targetAgentId, targetUserId || undefined);
        const targetSession = sessions.get(targetId);
        if (targetSession && targetSession.ws.readyState === WebSocket.OPEN) {
          const fromPath = buildAgentPath(fromSession.orgId, fromSession.agentId, fromSession.userId || undefined);
          targetSession.ws.send(JSON.stringify({
            type: "pair_completed",
            requestId: msg.requestId,
            fromOrgId: fromSession.orgId,
            fromAgentId: fromSession.agentId,
            fromUserId: fromSession.userId,
            fromName: msg.name,
            fromCard: msg.card,
            fromToken: msg.token,
            fromEndpoint: `${publicUrl}${fromPath}`,
          }));
          console.log(`[bluetooth] pair_complete: ${fromSession.id} -> ${targetId}`);
        }
      }

      if (msg.type === "set_discoverable") {
        // Resolve which agent to set discoverable
        const agentId = msg.agentId;
        const userId = msg.userId || "";
        let targetId: string | undefined;
        if (agentId) {
          targetId = buildAgentId(conn.orgId, agentId, userId || undefined);
        } else if (conn.agents.size === 1) {
          targetId = conn.agents.keys().next().value;
        }
        if (!targetId || !conn.agents.has(targetId)) return;

        let expiresAt = 0;
        if (msg.discoverable && msg.pairToken) {
          try {
            const payload = JSON.parse(Buffer.from(msg.pairToken.split(".")[1], "base64url").toString());
            if (payload.exp) expiresAt = payload.exp * 1000;
          } catch { /* ignore */ }
        }
        db.prepare("UPDATE agents SET discoverable = ?, pair_token = ?, discoverable_until = ?, updated_at = ? WHERE id = ?").run(
          msg.discoverable ? 1 : 0,
          msg.discoverable ? (msg.pairToken || "") : "",
          msg.discoverable ? expiresAt : 0,
          Date.now(),
          targetId,
        );
        ws.send(JSON.stringify({ type: "discoverable_updated", discoverable: !!msg.discoverable, agentId, userId }));
        console.log(`[bluetooth] ${targetId} discoverable=${!!msg.discoverable} expiresAt=${expiresAt ? new Date(expiresAt).toISOString() : "none"}`);
      }

      if (msg.type === "update_card") {
        const card = msg.card || {};
        // Resolve which agent to update
        const agentId = msg.agentId;
        const userId = msg.userId || "";
        let targetId: string | undefined;
        if (agentId) {
          targetId = buildAgentId(conn.orgId, agentId, userId || undefined);
        } else if (conn.agents.size === 1) {
          targetId = conn.agents.keys().next().value;
        }
        if (!targetId || !conn.agents.has(targetId)) return;

        const session = conn.agents.get(targetId)!;
        db.prepare("UPDATE agents SET display_name = ?, description = ?, card_json = ?, updated_at = ? WHERE id = ?").run(
          card.name || session.displayName,
          card.description || "",
          JSON.stringify(card),
          Date.now(),
          targetId,
        );
        session.displayName = card.name || session.displayName;
        session.description = card.description || "";
        session.cardJson = JSON.stringify(card);
        ws.send(JSON.stringify({ type: "card_updated", agentId, userId }));
        console.log(`[bluetooth] ${targetId} card updated: ${card.name}`);
      }

      if (msg.type === "ping") {
        ws.send('{"type":"pong"}');
        for (const session of conn.agents.values()) {
          setAgentOnline(db, session.id);
        }
      }
    });

    ws.on("close", () => {
      for (const [id, session] of conn.agents) {
        sessions.delete(id);
        setAgentOffline(db, id);
      }
      if (conn.agents.size > 0) {
        console.log(`[bluetooth] ws disconnected: ${conn.agents.size} agent(s) from ${conn.orgId} went offline`);
      }
      conn.agents.clear();
    });
    ws.on("error", () => {
      for (const [id] of conn.agents) {
        sessions.delete(id);
        setAgentOffline(db, id);
      }
      conn.agents.clear();
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
        orgId: a.orgId, agentId: a.agentId, userId: a.userId || undefined,
        name: a.displayName,
        description: a.description, online: a.online,
        url: `${publicUrl}${buildAgentPath(a.orgId, a.agentId, a.userId || undefined)}`,
        card: JSON.parse(a.cardJson),
      }));
      json(res, 200, { agents: cards });
      return;
    }

    // Agent Card: /u/{orgId}/{agentId}/.well-known/agent-card.json
    //          or /u/{orgId}/{agentId}/{userId}/.well-known/agent-card.json
    const cardMatch = url.match(/^\/u\/([^/]+)\/([^/]+)(?:\/([^/.][^/]*))?\/\.well-known\/agent-card\.json$/);
    if (cardMatch && method === "GET") {
      const [, cOrgId, cAgentId, cUserId] = cardMatch;
      const agent = getAgent(db, cOrgId, cAgentId, cUserId);
      if (!agent) { json(res, 404, { error: "not_found" }); return; }
      const card = JSON.parse(agent.cardJson);
      card.url = `${publicUrl}${buildAgentPath(agent.orgId, agent.agentId, agent.userId || undefined)}`;
      json(res, 200, card);
      return;
    }

    // A2A proxy: /u/{orgId}/{agentId}/{userId?}/...
    const proxyMatch3 = url.match(/^\/u\/([^/]+)\/([^/]+)\/([^/]+)(\/.*)?$/);
    const proxyMatch2 = url.match(/^\/u\/([^/]+)\/([^/]+)(\/.*)?$/);

    if (proxyMatch3 || proxyMatch2) {
      let pOrgId: string, pAgentId: string, pUserId: string, subPath: string;

      if (proxyMatch3) {
        const [, o, a, seg3, seg4] = proxyMatch3;
        const withUserId = buildAgentId(o, a, seg3);
        if (sessions.has(withUserId) || getAgentById(db, withUserId)) {
          pOrgId = o; pAgentId = a; pUserId = seg3; subPath = seg4 || "/";
        } else {
          pOrgId = o; pAgentId = a; pUserId = ""; subPath = `/${seg3}${seg4 || ""}`;
        }
      } else {
        const [, o, a, sp] = proxyMatch2!;
        pOrgId = o; pAgentId = a; pUserId = ""; subPath = sp || "/";
      }

      const id = buildAgentId(pOrgId, pAgentId, pUserId || undefined);
      const targetSession = sessions.get(id);
      if (!targetSession || targetSession.ws.readyState !== WebSocket.OPEN) {
        const fallbackId = buildAgentId(pOrgId, pAgentId);
        const fallbackSession = pUserId ? sessions.get(fallbackId) : null;
        if (fallbackSession && fallbackSession.ws.readyState === WebSocket.OPEN) {
          await proxyToSession(fallbackSession, req, res, method, subPath);
        } else {
          json(res, 502, { error: "agent_offline", message: "Agent is not connected" });
        }
        return;
      }

      await proxyToSession(targetSession, req, res, method, subPath);
      return;
    }

    // ── Admin: Provider management ──
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

  async function proxyToSession(targetSession: AgentSession, req: IncomingMessage, res: ServerResponse, method: string, subPath: string) {
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(chunk as Buffer);
    const body = Buffer.concat(chunks).toString("utf-8");

    const requestId = crypto.randomUUID();
    const responsePromise = new Promise<{ status: number; headers: Record<string, string>; body: string }>((resolve) => {
      const timer = setTimeout(() => { pending.delete(requestId); resolve({ status: 504, headers: {}, body: '{"error":"gateway_timeout"}' }); }, 5 * 60 * 1000);
      pending.set(requestId, { resolve, timer });
    });

    targetSession.ws.send(JSON.stringify({
      type: "request", requestId, method, path: subPath || "/",
      headers: Object.fromEntries(Object.entries(req.headers).filter(([k]) => !["host", "connection", "upgrade"].includes(k))),
      body,
    }));

    const resp = await responsePromise;
    for (const [k, v] of Object.entries(resp.headers)) res.setHeader(k, v);
    res.writeHead(resp.status, { "Content-Type": resp.headers["content-type"] || "application/json" });
    res.end(resp.body);
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

  // Periodic cleanup
  const cleanupTimer = setInterval(() => {
    const now = Date.now();
    const expiring = db.prepare(
      "SELECT id FROM agents WHERE discoverable = 1 AND discoverable_until > 0 AND discoverable_until < ?"
    ).all(now) as { id: string }[];
    if (expiring.length > 0) {
      const expired = expireDiscoverable(db);
      console.log(`[bluetooth] expired ${expired} discoverable agent(s)`);
      for (const row of expiring) {
        const s = sessions.get(row.id);
        if (s && s.ws.readyState === WebSocket.OPEN) {
          s.ws.send(JSON.stringify({ type: "discoverable_expired" }));
        }
      }
    }
  }, 30_000);
  cleanupTimer.unref();

  return { httpServer, wss };
}
