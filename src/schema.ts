// Agent Bluetooth — database schema

import Database from "better-sqlite3";
import { createHash, randomBytes } from "node:crypto";

export type Db = Database.Database;

export function openDb(path: string): Db {
  const db = new Database(path);
  db.pragma("journal_mode = WAL");
  db.exec(`
    CREATE TABLE IF NOT EXISTS providers (
      id              TEXT PRIMARY KEY,
      provider_id     TEXT NOT NULL UNIQUE,
      provider_name   TEXT NOT NULL DEFAULT '',
      api_token_hash  TEXT NOT NULL,
      created_at      INTEGER NOT NULL DEFAULT 0,
      updated_at      INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS agents (
      id            TEXT PRIMARY KEY,
      org_id        TEXT NOT NULL,
      agent_id      TEXT NOT NULL,
      user_id       TEXT NOT NULL DEFAULT '',
      provider_ref  TEXT NOT NULL DEFAULT '',
      display_name  TEXT NOT NULL DEFAULT '',
      description   TEXT NOT NULL DEFAULT '',
      card_json     TEXT NOT NULL DEFAULT '{}',
      api_key_hash  TEXT NOT NULL DEFAULT '',
      discoverable  INTEGER NOT NULL DEFAULT 0,
      pair_token    TEXT NOT NULL DEFAULT '',
      discoverable_until INTEGER NOT NULL DEFAULT 0,
      online        INTEGER NOT NULL DEFAULT 0,
      last_seen_at  INTEGER NOT NULL DEFAULT 0,
      created_at    INTEGER NOT NULL DEFAULT 0,
      updated_at    INTEGER NOT NULL DEFAULT 0,
      UNIQUE(org_id, agent_id, user_id)
    );
    CREATE INDEX IF NOT EXISTS idx_agents_discoverable ON agents(discoverable, online);
  `);
  // Migrations for existing installs
  try { db.exec("ALTER TABLE agents ADD COLUMN provider_ref TEXT NOT NULL DEFAULT ''"); } catch { /* already exists */ }
  try { db.exec("ALTER TABLE agents ADD COLUMN user_id TEXT NOT NULL DEFAULT ''"); } catch { /* already exists */ }
  // Migrate unique constraint: old (org_id, agent_id) → new (org_id, agent_id, user_id)
  // SQLite can't ALTER constraints, but the new table creation handles fresh installs.
  // For existing DBs, duplicate (org_id, agent_id) rows with different user_id will just work
  // since the PK is `id` which now includes userId.
  return db;
}

// ── Token helpers ──

export function generateToken(): string {
  return "bt_" + randomBytes(32).toString("base64url");
}

export function hashToken(token: string): string {
  return createHash("sha256").update(token).digest("hex");
}

// ── API key helpers (legacy) ──

export function generateApiKey(): string {
  return "sk_bt_" + randomBytes(24).toString("base64url");
}

export function hashApiKey(key: string): string {
  return createHash("sha256").update(key).digest("hex");
}

// ── Provider CRUD ──

export interface Provider {
  id: string;
  providerId: string;
  providerName: string;
  createdAt: number;
}

export function createProvider(db: Db, providerId: string, providerName: string): { provider: Provider; token: string } {
  const id = "prov_" + randomBytes(8).toString("hex");
  const token = generateToken();
  const now = Date.now();
  db.prepare(
    "INSERT INTO providers (id, provider_id, provider_name, api_token_hash, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)"
  ).run(id, providerId, providerName, hashToken(token), now, now);
  return { provider: { id, providerId, providerName, createdAt: now }, token };
}

export function listProviders(db: Db): Provider[] {
  return (db.prepare("SELECT * FROM providers ORDER BY created_at DESC").all() as any[]).map(r => ({
    id: r.id, providerId: r.provider_id, providerName: r.provider_name, createdAt: r.created_at,
  }));
}

export function deleteProvider(db: Db, providerId: string): boolean {
  const result = db.prepare("DELETE FROM providers WHERE provider_id = ?").run(providerId);
  return (result as any).changes > 0;
}

export function validateToken(db: Db, token: string): Provider | null {
  const hash = hashToken(token);
  const row = db.prepare("SELECT * FROM providers WHERE api_token_hash = ?").get(hash) as any;
  if (!row) return null;
  return { id: row.id, providerId: row.provider_id, providerName: row.provider_name, createdAt: row.created_at };
}

// ── Agent ID helpers ──

/** Build the canonical agent id: orgId/agentId or orgId/agentId:userId */
export function buildAgentId(orgId: string, agentId: string, userId?: string): string {
  return userId ? `${orgId}/${agentId}:${userId}` : `${orgId}/${agentId}`;
}

/** Build the public URL path for an agent */
export function buildAgentPath(orgId: string, agentId: string, userId?: string): string {
  return userId ? `/u/${orgId}/${agentId}/${userId}` : `/u/${orgId}/${agentId}`;
}

// ── Agent CRUD ──

export interface Agent {
  id: string;
  orgId: string;
  agentId: string;
  userId: string;
  providerRef: string;
  displayName: string;
  description: string;
  cardJson: string;
  discoverable: boolean;
  pairToken: string;
  online: boolean;
  lastSeenAt: number;
}

export function upsertAgent(
  db: Db,
  opts: { orgId: string; agentId: string; userId?: string; providerRef: string; displayName: string; description: string; cardJson: string; apiKeyHash: string; discoverable: boolean },
): Agent {
  const userId = opts.userId || "";
  const id = buildAgentId(opts.orgId, opts.agentId, userId || undefined);
  const now = Date.now();
  db.prepare(`
    INSERT INTO agents (id, org_id, agent_id, user_id, provider_ref, display_name, description, card_json, api_key_hash, discoverable, online, last_seen_at, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      provider_ref = excluded.provider_ref,
      display_name = excluded.display_name,
      description = excluded.description,
      card_json = excluded.card_json,
      api_key_hash = CASE WHEN excluded.api_key_hash != '' THEN excluded.api_key_hash ELSE agents.api_key_hash END,
      discoverable = agents.discoverable,
      online = 1,
      last_seen_at = excluded.last_seen_at,
      updated_at = excluded.updated_at
  `).run(id, opts.orgId, opts.agentId, userId, opts.providerRef, opts.displayName, opts.description, opts.cardJson, opts.apiKeyHash, opts.discoverable ? 1 : 0, now, now, now);

  return {
    id, orgId: opts.orgId, agentId: opts.agentId, userId,
    providerRef: opts.providerRef,
    displayName: opts.displayName, description: opts.description, cardJson: opts.cardJson,
    discoverable: opts.discoverable, pairToken: "", online: true, lastSeenAt: now,
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

export function getAgent(db: Db, orgId: string, agentId: string, userId?: string): Agent | null {
  if (userId) {
    const row = db.prepare("SELECT * FROM agents WHERE org_id = ? AND agent_id = ? AND user_id = ?").get(orgId, agentId, userId) as any;
    return row ? rowToAgent(row) : null;
  }
  // Legacy: match without userId (first match)
  const row = db.prepare("SELECT * FROM agents WHERE org_id = ? AND agent_id = ?").get(orgId, agentId) as any;
  return row ? rowToAgent(row) : null;
}

export function getAgentById(db: Db, id: string): Agent | null {
  const row = db.prepare("SELECT * FROM agents WHERE id = ?").get(id) as any;
  return row ? rowToAgent(row) : null;
}

function rowToAgent(r: any): Agent {
  return {
    id: r.id, orgId: r.org_id, agentId: r.agent_id,
    userId: r.user_id || "",
    providerRef: r.provider_ref || "",
    displayName: r.display_name, description: r.description,
    cardJson: r.card_json, discoverable: r.discoverable === 1,
    pairToken: r.pair_token || "",
    online: r.online === 1, lastSeenAt: r.last_seen_at,
  };
}

/** Clear discoverable + pair_token for agents whose discoverable_until has passed. */
export function expireDiscoverable(db: Db) {
  const now = Date.now();
  const result = db.prepare(
    "UPDATE agents SET discoverable = 0, pair_token = '', discoverable_until = 0 WHERE discoverable = 1 AND discoverable_until > 0 AND discoverable_until < ?"
  ).run(now);
  return (result as any).changes || 0;
}
