// Agent Bluetooth — relay server

import { WebSocketServer, WebSocket } from "ws";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { upsertAgent, setAgentOffline, setAgentOnline, listDiscoverableAgents, getAgent, expireDiscoverable, type Db } from "./schema.js";

interface AgentSession {
  ws: WebSocket;
  orgId: string;
  agentId: string;
  id: string;
  displayName: string;
}

interface PendingRequest {
  resolve: (data: any) => void;
  timer: ReturnType<typeof setTimeout>;
}

export interface ServerOpts {
  db: Db;
  port: number;
  publicUrl: string;
}

export function startServer(opts: ServerOpts) {
  const { db, port, publicUrl } = opts;
  const sessions = new Map<string, AgentSession>();
  const pending = new Map<string, PendingRequest>();

  const httpServer = createServer((req, res) => handleHttp(req, res));
  const wss = new WebSocketServer({ server: httpServer });

  wss.on("connection", (ws) => {
    let session: AgentSession | null = null;

    ws.on("message", (raw) => {
      let msg: any;
      try { msg = JSON.parse(String(raw)); } catch { return; }

      if (msg.type === "register" && msg.orgId && msg.agentId) {
        const id = `${msg.orgId}/${msg.agentId}`;
        session = { ws, orgId: msg.orgId, agentId: msg.agentId, id, displayName: msg.card?.name || msg.agentId };
        sessions.set(id, session);

        upsertAgent(db, {
          orgId: msg.orgId, agentId: msg.agentId,
          displayName: msg.card?.name || msg.agentId,
          description: msg.card?.description || "",
          cardJson: JSON.stringify(msg.card || {}),
          apiKeyHash: "", discoverable: msg.discoverable ?? false,
        });

        const currentAgent = getAgent(db, msg.orgId, msg.agentId);
        ws.send(JSON.stringify({
          type: "registered",
          publicUrl: `${publicUrl}/u/${msg.orgId}/${msg.agentId}`,
          agentPath: `${msg.orgId}/${msg.agentId}`,
          discoverable: currentAgent?.discoverable ?? false,
        }));
        console.log(`[bluetooth] agent registered: ${id} (discoverable: ${msg.discoverable ?? false})`);
      }

      if (msg.type === "response" && msg.requestId) {
        const p = pending.get(msg.requestId);
        if (p) { clearTimeout(p.timer); pending.delete(msg.requestId); p.resolve({ status: msg.status || 200, headers: msg.headers || {}, body: msg.body || "" }); }
      }

      // ── Pair request: A wants to pair with B ──
      if (msg.type === "pair_request" && session && msg.targetOrgId && msg.targetAgentId) {
        const targetId = `${msg.targetOrgId}/${msg.targetAgentId}`;
        const targetSession = sessions.get(targetId);
        if (!targetSession || targetSession.ws.readyState !== WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: "pair_response", success: false, error: "agent_offline" }));
        } else {
          // Get target's pair token from DB
          const targetAgent = getAgent(db, msg.targetOrgId, msg.targetAgentId);
          if (!targetAgent || !targetAgent.discoverable) {
            ws.send(JSON.stringify({ type: "pair_response", success: false, error: "not_discoverable" }));
          } else {
            const requestId = crypto.randomUUID();
            const pairPending = new Promise<any>((resolve_pair) => {
              const timer = setTimeout(() => { pending.delete(requestId); resolve_pair({ success: false, error: "timeout" }); }, 30000);
              pending.set(requestId, { resolve: resolve_pair, timer });
            });
            targetSession.ws.send(JSON.stringify({
              type: "pair_request",
              requestId,
              fromOrgId: session.orgId,
              fromAgentId: session.agentId,
              fromName: msg.name || session.displayName,
              fromCard: msg.card,
              fromToken: msg.token,
              fromEndpoint: `${publicUrl}/u/${session.orgId}/${session.agentId}`,
            }));
            pairPending.then((resp: any) => {
              ws.send(JSON.stringify({
                type: "pair_response",
                success: resp.success ?? false,
                token: resp.token,  // B's long-lived comm token from pair_accept
                name: resp.name,
                card: resp.card,
                endpoint: resp.success ? `${publicUrl}/u/${msg.targetOrgId}/${msg.targetAgentId}` : undefined,
              }));
            });
          }
        }
      }

      // ── Pair accept: B responds to A's pair request ──
      if (msg.type === "pair_accept" && msg.requestId) {
        const p = pending.get(msg.requestId);
        if (p) {
          clearTimeout(p.timer);
          pending.delete(msg.requestId);
          p.resolve({ success: true, token: msg.token, name: msg.name, card: msg.card });
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
