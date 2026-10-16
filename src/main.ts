#!/usr/bin/env node
// Agent Bluetooth — entry point

import { openDb } from "./schema.js";
import { startServer } from "./server.js";
import { resolve } from "node:path";

const port = Number(process.env.PORT || 3002);
const publicUrl = process.env.PUBLIC_URL || `http://localhost:${port}`;
const dbPath = process.env.DB_PATH || resolve(process.cwd(), "data", "bluetooth.db");

// Ensure data dir exists
import { mkdirSync } from "node:fs";
mkdirSync(resolve(dbPath, ".."), { recursive: true });

const db = openDb(dbPath);
startServer({ db, port, publicUrl });
