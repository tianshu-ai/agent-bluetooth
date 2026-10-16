# 🔵 Agent Bluetooth

A2A relay server for cross-network agent discovery and communication.

Like Bluetooth — agents turn on "discoverable" mode to be found, then pair and communicate through the relay.

## Quick Start

```bash
npm install
npm run build
npm start
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `3002` | Server port |
| `PUBLIC_URL` | `http://localhost:3002` | Public URL for agent endpoints |
| `DB_PATH` | `./data/bluetooth.db` | SQLite database path |

## API

### Discovery

```
GET /discovery          → List all discoverable agents
GET /stats              → Server statistics
GET /healthz            → Health check
```

### Per-Agent A2A Endpoints

```
GET  /u/{orgId}/{agentId}/.well-known/agent-card.json  → Agent Card
POST /u/{orgId}/{agentId}/*                             → A2A proxy (relayed via WebSocket)
```

### WebSocket (for agents)

```
ws://host:port/

→ {"type":"register", "orgId":"...", "agentId":"...", "card":{...}, "discoverable":true}
← {"type":"registered", "publicUrl":"http://host:port/u/org/agent", "agentPath":"org/agent"}
```

## Architecture

```
Agent A (intranet) ──WS──► Agent Bluetooth (internet) ◄──WS── Agent B (intranet)
                                    │
                           External A2A Client ──HTTP──►
```

- Agents connect outbound via WebSocket (NAT-friendly)
- Hub assigns public URLs and proxies A2A requests
- No message storage — relay only
- Agent Cards stored in registry for discovery

## License

Apache-2.0
