// Agent Bluetooth — relay server

import { WebSocketServer, WebSocket } from "ws";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { upsertAgent, setAgentOffline, setAgentOnline, listDiscoverableAgents, getAgent, type Db } from "./schema.js";

interface AgentSession {
  ws: WebSocket;
  orgId: string;
  agentId: string;
  id: string;
}

interface PendingRequest {
  resolve: (data: { status: number; headers: Record<string, string>; body: string }) => void;
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
        session = { ws, orgId: msg.orgId, agentId: msg.agentId, id };
        sessions.set(id, session);

        upsertAgent(db, {
          orgId: msg.orgId, agentId: msg.agentId,
          displayName: msg.card?.name || msg.agentId,
          description: msg.card?.description || "",
          cardJson: JSON.stringify(msg.card || {}),
          apiKeyHash: "", discoverable: msg.discoverable ?? false,
        });

        ws.send(JSON.stringify({
          type: "registered",
          publicUrl: `${publicUrl}/u/${msg.orgId}/${msg.agentId}`,
          agentPath: `${msg.orgId}/${msg.agentId}`,
        }));
        console.log(`[bluetooth] agent registered: ${id} (discoverable: ${msg.discoverable ?? false})`);
      }

      if (msg.type === "response" && msg.requestId) {
        const p = pending.get(msg.requestId);
        if (p) { clearTimeout(p.timer); pending.delete(msg.requestId); p.resolve({ status: msg.status || 200, headers: msg.headers || {}, body: msg.body || "" }); }
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

  return { httpServer, wss };
}
