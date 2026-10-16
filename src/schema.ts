// Agent Bluetooth — database schema

import Database from "better-sqlite3";
import { createHash, randomBytes } from "node:crypto";

export type Db = Database.Database;

export function openDb(path: string): Db {
  const db = new Database(path);
  db.pragma("journal_mode = WAL");
  db.exec(`
    CREATE TABLE IF NOT EXISTS agents (
      id            TEXT PRIMARY KEY,
      org_id        TEXT NOT NULL,
      agent_id      TEXT NOT NULL,
      display_name  TEXT NOT NULL DEFAULT '',
      description   TEXT NOT NULL DEFAULT '',
      card_json     TEXT NOT NULL DEFAULT '{}',
      api_key_hash  TEXT NOT NULL DEFAULT '',
      discoverable  INTEGER NOT NULL DEFAULT 0,
      online        INTEGER NOT NULL DEFAULT 0,
      last_seen_at  INTEGER NOT NULL DEFAULT 0,
      created_at    INTEGER NOT NULL DEFAULT 0,
      updated_at    INTEGER NOT NULL DEFAULT 0,
      UNIQUE(org_id, agent_id)
    );
    CREATE INDEX IF NOT EXISTS idx_agents_discoverable ON agents(discoverable, online);
  `);
  return db;
}

// ── API key helpers ──

export function generateApiKey(): string {
  return "sk_bt_" + randomBytes(24).toString("base64url");
}

export function hashApiKey(key: string): string {
  return createHash("sha256").update(key).digest("hex");
}

// ── Agent CRUD ──

export interface Agent {
  id: string;
  orgId: string;
  agentId: string;
  displayName: string;
  description: string;
  cardJson: string;
  discoverable: boolean;
  online: boolean;
  lastSeenAt: number;
}

export function upsertAgent(
  db: Db,
  opts: { orgId: string; agentId: string; displayName: string; description: string; cardJson: string; apiKeyHash: string; discoverable: boolean },
): Agent {
  const id = `${opts.orgId}/${opts.agentId}`;
  const now = Date.now();
  db.prepare(`
    INSERT INTO agents (id, org_id, agent_id, display_name, description, card_json, api_key_hash, discoverable, online, last_seen_at, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      display_name = excluded.display_name,
      description = excluded.description,
      card_json = excluded.card_json,
      api_key_hash = CASE WHEN excluded.api_key_hash != '' THEN excluded.api_key_hash ELSE agents.api_key_hash END,
      discoverable = agents.discoverable,
      online = 1,
      last_seen_at = excluded.last_seen_at,
      updated_at = excluded.updated_at
  `).run(id, opts.orgId, opts.agentId, opts.displayName, opts.description, opts.cardJson, opts.apiKeyHash, opts.discoverable ? 1 : 0, now, now, now);

  return {
    id, orgId: opts.orgId, agentId: opts.agentId, displayName: opts.displayName,
    description: opts.description, cardJson: opts.cardJson, discoverable: opts.discoverable,
    online: true, lastSeenAt: now,
  };
}

export function setAgentOffline(db: Db, id: string) {
  db.prepare("UPDATE agents SET online = 0, updated_at = ? WHERE id = ?").run(Date.now(), id);
}

export function setAgentOnline(db: Db, id: string) {
  db.prepare("UPDATE agents SET online = 1, last_seen_at = ?, updated_at = ? WHERE id = ?").run(Date.now(), Date.now(), id);
}

export function listDiscoverableAgents(db: Db): Agent[] {
  return (db.prepare("SELECT * FROM agents WHERE discoverable = 1 ORDER BY online DESC, last_seen_at DESC").all() as any[]).map(rowToAgent);
}

export function getAgent(db: Db, orgId: string, agentId: string): Agent | null {
  const row = db.prepare("SELECT * FROM agents WHERE org_id = ? AND agent_id = ?").get(orgId, agentId) as any;
  return row ? rowToAgent(row) : null;
}

function rowToAgent(r: any): Agent {
  return {
    id: r.id, orgId: r.org_id, agentId: r.agent_id,
    displayName: r.display_name, description: r.description,
    cardJson: r.card_json, discoverable: r.discoverable === 1,
    online: r.online === 1, lastSeenAt: r.last_seen_at,
  };
}
