# Agentic Mesh — Complete Deployment & Implementation Guide

> **Author:** Advaith J  
> **Version:** 1.0  
> **Last Updated:** September 2026  
> **License:** Private

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture Deep Dive](#2-architecture-deep-dive)
3. [Prerequisites](#3-prerequisites)
4. [Quick Start — Single Node](#4-quick-start--single-node)
5. [Environment Configuration Reference](#5-environment-configuration-reference)
6. [Multi-Node Mesh Setup](#6-multi-node-mesh-setup)
7. [AI Agent Configuration](#7-ai-agent-configuration)
8. [React Dashboard](#8-react-dashboard)
9. [Docker Deployment](#9-docker-deployment)
10. [CLI Commands Reference](#10-cli-commands-reference)
11. [WebSocket API Reference](#11-websocket-api-reference)
12. [Testing & Verification](#12-testing--verification)
13. [Project File Structure](#13-project-file-structure)
14. [Troubleshooting](#14-troubleshooting)
15. [Production Checklist](#15-production-checklist)

---

## 1. Project Overview

**Agentic Mesh** is an autonomous peer-to-peer (P2P) database mesh with AI-powered schema enforcement. Every laptop or server runs the **same codebase** — each node independently maintains a SQLite database and synchronizes state with other nodes via libp2p GossipSub, using vector clocks for causal ordering.

### Core Capabilities

| Capability | Technology |
|---|---|
| P2P Networking | libp2p (TCP transport + mDNS discovery + GossipSub) |
| Database | SQLite via `better-sqlite3` (3NF normalized schema) |
| Replication | Vector clock–based causal sync via `_mesh_log` |
| AI Agent | Ollama (local LLM) — 3-tier inference strategy |
| Dashboard | React + Vite + TailwindCSS (real-time WebSocket) |
| Gateway | Express + WebSocket (WS) server |

### How It Works

```
┌──────────────────────────────────────────────────────────────┐
│                      AGENTIC MESH NODE                       │
│                                                              │
│  ┌──────────┐   ┌──────────┐   ┌───────────┐               │
│  │  SQLite   │◄──│  Sync    │◄──│  GossipSub │◄── P2P mesh  │
│  │  (3NF DB) │   │  Engine  │   │  (libp2p)  │               │
│  └────┬─────┘   └──────────┘   └───────────┘               │
│       │                                                      │
│  ┌────▼─────┐   ┌──────────┐   ┌───────────┐               │
│  │ Schema   │   │ AI Agent │   │ WebSocket │──► Dashboard   │
│  │ Validator│   │ (Ollama) │   │ Gateway   │──► CLI         │
│  └──────────┘   └──────────┘   └───────────┘               │
└──────────────────────────────────────────────────────────────┘
```

Each node:
1. **Writes locally** to SQLite, recording every mutation in `_mesh_log` with a vector clock timestamp.
2. **Gossips** the transaction envelope to all connected peers via GossipSub.
3. **Receives** remote transactions, validates them against the local schema, and applies them atomically.
4. **Syncs** on reconnection by comparing vector clocks and replaying missing log entries.

---

## 2. Architecture Deep Dive

### 2.1 Source Code Layers

```
src/
├── agents/          # AI agent layer (Ollama, chat, planner, router, validator)
│   ├── agent-chat.js       # 3-tier chat: Instant → Fast AI → Heavy AI
│   ├── ollama.js           # Ollama SDK wrapper, prewarming, model management
│   ├── router.js           # Fast-path (structured JSON) vs AI-path (NL) routing
│   ├── planner.js          # NL → SQL operation plan decomposition
│   ├── executor.js         # Atomic multi-operation execution
│   ├── validator.js        # AI audit of recent transactions
│   └── model-validator.js  # Validates model-generated operation plans
│
├── db/              # Database layer
│   ├── sqlite.js           # Schema creation, seeding, CRUD, mesh log
│   ├── sync.js             # VectorClock class, SyncEngine (merge, replay)
│   ├── mutation.js         # Safe mutation execution with schema validation
│   └── schema-validator.js # Allowlist & constraint validation
│
├── p2p/             # Peer-to-peer networking layer
│   ├── node.js             # libp2p node factory (TCP + mDNS + GossipSub)
│   ├── gossip.js           # Topic subscriptions, message encoding/routing
│   └── discovery.js        # PeerRegistry with EventEmitter for join/leave
│
├── websocket/       # Gateway layer
│   └── server.js           # Express HTTP + WebSocket server, SSE streaming
│
├── api/             # CLI layer
│   └── routes.js           # Interactive readline CLI
│
└── utils/           # Shared utilities
    ├── config.js           # Environment variable loader + defaults
    ├── protocol.js         # Message envelope creation, encode/decode, LRU cache
    └── logger.js           # Color-coded structured logging
```

### 2.2 Database Schema (3NF)

```sql
-- Product categories
CREATE TABLE categories (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    description TEXT
);

-- Items linked to categories
CREATE TABLE items (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    category_id INTEGER NOT NULL,
    name        TEXT NOT NULL,
    price       REAL NOT NULL CHECK(price > 0),
    sku         TEXT UNIQUE NOT NULL,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE RESTRICT
);

-- Suppliers
CREATE TABLE suppliers (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT NOT NULL UNIQUE,
    contact_email TEXT
);

-- Many-to-many item-supplier linkage
CREATE TABLE item_suppliers (
    item_id     INTEGER NOT NULL,
    supplier_id INTEGER NOT NULL,
    PRIMARY KEY (item_id, supplier_id),
    FOREIGN KEY (item_id)     REFERENCES items(id)     ON DELETE CASCADE,
    FOREIGN KEY (supplier_id) REFERENCES suppliers(id)  ON DELETE CASCADE
);

-- Replication log (vector clock + operation journal)
CREATE TABLE _mesh_log (
    id           TEXT PRIMARY KEY,
    operation    TEXT NOT NULL,       -- INSERT | UPDATE | DELETE
    table_name   TEXT NOT NULL,
    row_data     TEXT NOT NULL,       -- JSON-serialized row
    vector_clock TEXT NOT NULL,       -- JSON-serialized vector clock
    peer_id      TEXT NOT NULL,       -- Originating peer ID
    applied_at   DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

### 2.3 Vector Clock Replication

1. **Local write** — `SyncEngine.recordLocalWrite()` increments the local peer's clock, logs to `_mesh_log`, and creates a gossip envelope.
2. **Gossip broadcast** — The envelope is published to the `mesh:transactions` GossipSub topic.
3. **Remote apply** — `SyncEngine.applyRemoteTransaction()` checks for duplicates, validates the schema, executes the write atomically, and merges the remote vector clock.
4. **Catch-up sync** — On peer reconnection, a `SYNC_REQUEST` is sent with the local vector clock. The peer replies with `SYNC_RESPONSE` containing all log entries the requester is missing.

### 2.4 AI Chat Tiers

| Tier | Trigger | Latency | Engine |
|------|---------|---------|--------|
| **Instant** | Greetings, identity, help, status, schema, templates, DB queries | < 1 ms | Local regex + templates |
| **Fast AI** | General coding questions, explanations | 2-8 sec | `qwen2.5-coder:1.5b` (948 MB) |
| **Heavy AI** | Complex architecture, debugging, design | 30-90 sec | `gemma4:e2b` (7.2 GB) — **optional** |

---

## 3. Prerequisites

### 3.1 All Nodes

| Requirement | Version | Install |
|---|---|---|
| **Node.js** | v20 LTS or v22 LTS | [nodejs.org](https://nodejs.org) |
| **npm** | v10+ (bundled with Node.js) | Bundled |
| **Git** | Any recent version | [git-scm.com](https://git-scm.com) |
| **C++ Build Tools** | For `better-sqlite3` native compilation | See below |

#### C++ Build Tools by Platform

**Windows:**
```powershell
# Option A: Install Visual Studio Build Tools
winget install Microsoft.VisualStudio.2022.BuildTools

# Option B: Use npm windows-build-tools (admin PowerShell)
npm install -g windows-build-tools
```

**macOS:**
```bash
xcode-select --install
```

**Linux (Debian/Ubuntu):**
```bash
sudo apt-get update && sudo apt-get install -y python3 make g++
```

### 3.2 AI Agent Node Only (Optional)

| Requirement | Version | Install |
|---|---|---|
| **Ollama** | Latest | [ollama.com](https://ollama.com) |
| **LLM Model** | `qwen2.5-coder:1.5b` (fast) | `ollama pull qwen2.5-coder:1.5b` |
| **LLM Model** | `gemma4:e2b` (heavy, optional) | `ollama pull gemma4:e2b` |

### 3.3 Network Requirements

- All nodes must be on the **same local network** (Wi-Fi or LAN) for mDNS auto-discovery.
- **Ports used per node:** One TCP port for P2P (e.g., 9001) and one for WebSocket gateway (e.g., 3001).
- Ensure your firewall allows inbound TCP on the configured P2P and WS ports.

---

## 4. Quick Start — Single Node

### Step 1: Clone or Copy the Project

```bash
git clone <your-repo-url> agentic-mesh
cd agentic-mesh
```

### Step 2: Install Dependencies

```bash
npm install
```

> This installs all runtime and dev dependencies including `better-sqlite3` (native SQLite binding), `libp2p`, `ollama` SDK, React, and Vite.

### Step 3: Configure Environment

Create a `.env` file in the project root:

```env
NODE_NAME=alpha
NODE_ROLE=peer
P2P_PORT=9001
WS_PORT=3001
DB_PATH=./data/alpha.db
OLLAMA_HOST=http://127.0.0.1:11434
OLLAMA_MODEL=qwen2.5-coder:1.5b
OLLAMA_NUM_CTX=1024
OLLAMA_NUM_PREDICT=128
OLLAMA_TEMPERATURE=0.2
OLLAMA_TIMEOUT_MS=90000
OLLAMA_KEEP_ALIVE=5m
```

### Step 4: (Optional) Install and Start Ollama

```bash
# Install model
ollama pull qwen2.5-coder:1.5b

# Start Ollama server (if not auto-started)
ollama serve
```

### Step 5: Boot the Node

```bash
npm start
# or: node index.js
```

You should see:

```
═══════════════════════════════════════════════
  AGENTIC MESH — Autonomous P2P Database Node 
═══════════════════════════════════════════════
Node Name: alpha
Node Role: peer
P2P Port:  9001
WS Port:   3001
DB Path:   ./data/alpha.db
AI Model:  qwen2.5-coder:1.5b

[DB]    Database ready — 3 categories, 6 items, 2 suppliers
[P2P]   Peer ID: 12D3KooW...
[AI]    Model prewarm complete: qwen2.5-coder:1.5b [Processor: 100% CPU]

═══════════════════════════════════════════════
  All systems online. Node is ready.          
═══════════════════════════════════════════════
[alpha] > 
```

### Step 6: (Optional) Start the Dashboard

In a **separate terminal**:

```bash
npm run dev:dashboard
```

Open `http://localhost:5173` — the dashboard connects to `ws://localhost:3001`.

---

## 5. Environment Configuration Reference

| Variable | Default | Description |
|---|---|---|
| `NODE_NAME` | Random string | Human-readable node name (e.g., `alpha`, `bravo`, `delta`) |
| `NODE_ROLE` | `peer` | Node role: `peer` (data only) or `ai-agent` (data + AI) |
| `P2P_PORT` | `9004` | TCP port for libp2p P2P communication |
| `WS_PORT` | `3004` | Port for Express + WebSocket gateway |
| `DB_PATH` | `./data/mesh.db` | Path to SQLite database file |
| `OLLAMA_HOST` | `http://127.0.0.1:11434` | Ollama API server URL |
| `OLLAMA_MODEL` | `qwen2.5-coder:1.5b` | Default LLM model for AI chat |
| `OLLAMA_NUM_CTX` | `1024` | Context window size (tokens) |
| `OLLAMA_NUM_PREDICT` | `128` | Max output tokens per response |
| `OLLAMA_TEMPERATURE` | `0.2` | Sampling temperature (lower = more deterministic) |
| `OLLAMA_TIMEOUT_MS` | `90000` | Server-side generation timeout (ms) |
| `OLLAMA_KEEP_ALIVE` | `5m` | How long to keep model loaded in Ollama RAM |

### Port Assignment Convention

| Node Name | P2P Port | WS Port | DB Path |
|---|---|---|---|
| alpha | 9001 | 3001 | `./data/alpha.db` |
| bravo | 9002 | 3002 | `./data/bravo.db` |
| charlie | 9003 | 3003 | `./data/charlie.db` |
| delta | 9004 | 3004 | `./data/delta.db` |
| echo | 9005 | 3005 | `./data/echo.db` |

> **Rule:** Each node on the same machine must use unique ports and a unique DB path.

---

## 6. Multi-Node Mesh Setup

### 6.1 Same Machine (Multiple Terminals)

Open separate terminals for each node:

**Terminal 1 — Node Alpha:**
```bash
# Using .env file (edit .env first)
node index.js

# OR using inline environment variables (Linux/macOS):
NODE_NAME=alpha P2P_PORT=9001 WS_PORT=3001 DB_PATH=./data/alpha.db node index.js
```

**Terminal 2 — Node Bravo:**
```bash
NODE_NAME=bravo P2P_PORT=9002 WS_PORT=3002 DB_PATH=./data/bravo.db node index.js
```

**Terminal 3 — Node Delta (AI Agent):**
```bash
NODE_NAME=delta NODE_ROLE=ai-agent P2P_PORT=9004 WS_PORT=3004 DB_PATH=./data/delta.db node index.js
```

> **Windows PowerShell alternative:**
> ```powershell
> $env:NODE_NAME="bravo"; $env:P2P_PORT="9002"; $env:WS_PORT="3002"; $env:DB_PATH="./data/bravo.db"; node index.js
> ```

### 6.2 Different Laptops (Same Network)

#### On Laptop A (Node Alpha):

1. Clone and install:
   ```bash
   git clone <repo> agentic-mesh && cd agentic-mesh && npm install
   ```

2. Create `.env`:
   ```env
   NODE_NAME=alpha
   NODE_ROLE=peer
   P2P_PORT=9001
   WS_PORT=3001
   DB_PATH=./data/alpha.db
   ```

3. Start: `node index.js`

#### On Laptop B (Node Bravo):

1. Clone and install:
   ```bash
   git clone <repo> agentic-mesh && cd agentic-mesh && npm install
   ```

2. Create `.env`:
   ```env
   NODE_NAME=bravo
   NODE_ROLE=peer
   P2P_PORT=9002
   WS_PORT=3002
   DB_PATH=./data/bravo.db
   ```

3. Start: `node index.js`

4. **Verify peer connection** — in Bravo's CLI, type `peers`:
   ```
   [bravo] > peers
   ┌─────────────────────────────────────┬───────────┐
   │ Peer ID                             │ Connected │
   ├─────────────────────────────────────┼───────────┤
   │ 12D3KooWAlpha...                    │ 1.2s ago  │
   └─────────────────────────────────────┴───────────┘
   ```

### 6.3 How mDNS Discovery Works

- Each node broadcasts its Peer ID and multiaddr via **mDNS** (multicast DNS) on the local network.
- When a new peer is discovered, the `PeerRegistry` automatically dials and connects.
- On successful connection, a `SYNC_REQUEST` is sent to catch up on any missed transactions.
- **No manual configuration needed** — nodes auto-discover each other on the same LAN.

### 6.4 Verifying Mesh Sync

1. **On Node Alpha CLI:**
   ```
   [alpha] > propose {"table":"items","operation":"INSERT","data":{"category_id":1,"name":"Monitor","price":399.99,"sku":"SKU-E010"}}
   ```

2. **On Node Bravo CLI:**
   ```
   [bravo] > db
   ```
   The new "Monitor" item should appear in Bravo's database within milliseconds.

---

## 7. AI Agent Configuration

### 7.1 Making a Node an AI Agent

Set `NODE_ROLE=ai-agent` in `.env`. This enables:
- AI chat via WebSocket/dashboard
- NL to SQL operation planning
- AI audit of recent transactions

### 7.2 Model Management

```bash
# List installed models
ollama list

# Pull the fast coding model (recommended)
ollama pull qwen2.5-coder:1.5b

# Pull the heavy model (optional, for architecture questions)
ollama pull gemma4:e2b

# Check what's currently loaded in RAM
ollama ps

# Unload a model from RAM (free memory)
ollama stop gemma4:e2b

# Remove a model entirely
ollama rm gemma4:e2b
```

### 7.3 Performance Strategy

**CPU-Only Machines (Most Laptops):**

| Setting | Value | Why |
|---|---|---|
| `OLLAMA_MODEL` | `qwen2.5-coder:1.5b` | 948 MB, fast on CPU |
| `OLLAMA_NUM_CTX` | `1024` | Minimal context = fast inference |
| `OLLAMA_NUM_PREDICT` | `128` | Short responses |
| `OLLAMA_TEMPERATURE` | `0.2` | Deterministic code output |
| `OLLAMA_KEEP_ALIVE` | `5m` | Keep model warm between requests |

> **Warning:** Never load `gemma4:e2b` (7.2 GB) and `qwen2.5-coder:1.5b` simultaneously on a CPU-only machine. Use `ollama stop gemma4:e2b` to unload the heavy model before using the fast one.

**GPU Machines:**
- You can use larger models like `gemma4:e2b` or `qwen2.5-coder:7b`.
- Increase `OLLAMA_NUM_CTX` to `4096` and `OLLAMA_NUM_PREDICT` to `512`.

### 7.4 Prewarming

At boot, the node automatically sends a 1-token "ping" to Ollama to load the model into memory. Boot logs will show:

```
[AI] Model prewarm complete: qwen2.5-coder:1.5b [Processor: 100% CPU] | Startup: 3200ms | Warm-up: 1800ms
```

### 7.5 Chat Tier Details

**Tier 1 — Instant Templates (no model call):**
- `hi`, `hello`, `help`, `status`
- `show schema`, `show database items`
- `give Java even/odd code`, `hello world in python`
- `show websocket client code`, `show rest api endpoints`

**Tier 2 — Fast AI (streaming with qwen2.5-coder:1.5b):**
- General coding questions
- Explanations and tutorials
- Any message not matching Tier 1 patterns or Tier 3 mutation keywords

**Tier 3 — Buffered AI (mutation planning):**
- Messages containing `add`, `create`, `insert`, `update`, `delete`, `remove`
- Full schema context sent to model for safe SQL plan generation
- Returns structured operation plan for approval

---

## 8. React Dashboard

### 8.1 Development Mode

```bash
npm run dev:dashboard
```

Opens on `http://localhost:5173`. Hot-reloads on code changes.

### 8.2 Production Build

```bash
npm run build:dashboard
```

Output is in `dist/`. Serve with any static file server.

### 8.3 Dashboard Features

- **Real-time peer topology** — force-directed graph of connected nodes
- **Live database view** — categories, items, suppliers with auto-refresh
- **Transaction log** — real-time feed of INSERT/UPDATE/DELETE operations
- **AI Agent chat** — mode selector (Fast / Deep), streaming responses
- **Mesh log** — vector clock and replication status

### 8.4 Connecting Dashboard to a Different Node

The dashboard connects to `ws://localhost:<WS_PORT>`. To connect to a remote node:
1. Open the dashboard in your browser.
2. The WebSocket URL can be configured in the dashboard UI or by editing the source.
3. For remote nodes: `ws://<node-ip>:<WS_PORT>`

---

## 9. Docker Deployment

### 9.1 Single Node with Docker

```bash
# Build the image
docker build -t agentic-mesh .

# Run a single node
docker run -d \
  --name mesh-alpha \
  -e NODE_NAME=alpha \
  -e P2P_PORT=9001 \
  -e WS_PORT=3001 \
  -e DB_PATH=/app/data/mesh.db \
  -p 9001:9001 \
  -p 3001:3001 \
  agentic-mesh
```

### 9.2 Full 4-Node Mesh with Docker Compose

```bash
# Start all nodes + Ollama
docker compose up -d

# View logs
docker compose logs -f

# Stop everything
docker compose down
```

The `docker-compose.yml` defines:

| Service | Node Name | P2P Port | WS Port | Model |
|---|---|---|---|---|
| `node-alpha` | alpha | 9001 | 3001 | gemma3:4b |
| `node-bravo` | bravo | 9002 | 3002 | gemma3:4b |
| `node-charlie` | charlie | 9003 | 3003 | gemma3:4b |
| `node-delta` | delta (ai-agent) | 9004 | 3004 | agentic-mesh-coder |
| `ollama` | — | 11434 | — | — |

> **Note:** Update `docker-compose.yml` model references to `qwen2.5-coder:1.5b` for CPU-only deployments.

### 9.3 Dockerfile Overview

```dockerfile
FROM node:22-slim
RUN apt-get update && apt-get install -y python3 make g++ curl
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN mkdir -p /app/data
ENV NODE_ENV=production
CMD ["node", "index.js"]
```

---

## 10. CLI Commands Reference

Once a node is running, the interactive CLI (`[node-name] >`) supports:

| Command | Description |
|---|---|
| `db` | Show all database tables (categories, items, suppliers) |
| `peers` | List connected mesh peers with connection time |
| `status` | Show node configuration and database stats |
| `sync` | Trigger a manual SYNC_REQUEST to the mesh |
| `propose <JSON>` | Submit a structured transaction |
| `ask <question>` | Route a natural language question to the AI planner |
| `audit [N]` | AI-audit the last N transactions for anomalies |
| `bad` | Run a test with intentionally malformed data |
| `exit` | Gracefully shut down the node |

### Example: Structured Transaction

```
[alpha] > propose {"table":"items","operation":"INSERT","data":{"category_id":2,"name":"JavaScript Guide","price":39.99,"sku":"SKU-B002"}}
```

### Example: AI Planning

```
[delta] > ask Add a new supplier TechParts with email parts@tech.com
```

---

## 11. WebSocket API Reference

### Connection

```javascript
const ws = new WebSocket('ws://localhost:3001');
```

### Message Format (Client to Server)

```json
{
  "type": "<action>",
  "payload": { }
}
```

### Supported Actions

| Type | Payload | Description |
|---|---|---|
| `getState` | `{}` | Get full database state |
| `getPeers` | `{}` | Get connected peers list |
| `getAgentStatus` | `{}` | Check AI model availability |
| `propose` | `{ table, operation, data }` | Submit a transaction |
| `plan` | `{ request: "NL string" }` | NL to operation plan |
| `execute` | `{ operations: [...] }` | Execute planned operations |
| `audit` | `{ count: N }` | AI-audit last N transactions |
| `chat` | `{ message: "...", history: [...] }` | Buffered AI chat |
| `chatStream` | `{ message: "...", history: [...] }` | SSE-streamed AI chat |
| `sync` | `{}` | Trigger mesh sync |

### Server to Client Events

| Type | Description |
|---|---|
| `state` | Database state response |
| `peers` | Peer list response |
| `agentStatus` | AI model status |
| `tx:committed` | Local transaction committed |
| `tx:replicated` | Remote transaction applied |
| `tx:conflict` | Schema validation conflict |
| `peer:joined` | New peer discovered |
| `peer:connected` | Peer connection established |
| `peer:left` | Peer disconnected |
| `sync:completed` | Sync catch-up finished |
| `ai:completed` | AI audit result |
| `chat:response` | Buffered chat answer |
| `chat:token` | Streaming token chunk |
| `chat:done` | Stream complete |
| `chat:error` | Chat error |

### Example: WebSocket Client (Node.js)

```javascript
import WebSocket from 'ws';

const ws = new WebSocket('ws://localhost:3001');

ws.on('open', () => {
  // Get database state
  ws.send(JSON.stringify({ type: 'getState' }));
  
  // Submit a transaction
  ws.send(JSON.stringify({
    type: 'propose',
    payload: {
      table: 'items',
      operation: 'INSERT',
      data: { category_id: 1, name: 'Webcam', price: 79.99, sku: 'SKU-E011' }
    }
  }));
});

ws.on('message', (data) => {
  const msg = JSON.parse(data);
  console.log(`[${msg.type}]`, msg.data || msg.payload);
});
```

### Example: WebSocket Client (Browser)

```javascript
const ws = new WebSocket('ws://localhost:3001');

ws.onopen = () => {
  ws.send(JSON.stringify({ type: 'getState' }));
};

ws.onmessage = (event) => {
  const msg = JSON.parse(event.data);
  console.log(msg.type, msg);
};
```

---

## 12. Testing and Verification

### 12.1 Unit Tests

```bash
npm test
```

Runs all tests in `test/`:

| Test File | Coverage |
|---|---|
| `vector-clock.test.js` | VectorClock merge, increment, comparison |
| `sync.test.js` | SyncEngine record, apply, dedup, catch-up |
| `schema-validator.test.js` | Allowlist, FK, constraint validation |
| `mutation.test.js` | Safe INSERT/UPDATE/DELETE execution |
| `model-validator.test.js` | AI plan validation |
| `api.test.js` | WebSocket API handlers |

### 12.2 Two-Node Smoke Test

```bash
npm run demo:smoke
```

Runs `test/two-node-smoke.js` — boots two in-process nodes, performs transactions on each, and verifies cross-node replication.

### 12.3 Manual Verification Checklist

- [ ] **Boot:** Node prints "All systems online" without errors
- [ ] **Database:** `db` CLI command shows 3 categories, 6 items, 2 suppliers
- [ ] **Peers:** Second node shows first node in `peers` output
- [ ] **Sync:** Transaction on Node A appears on Node B within < 1 second
- [ ] **AI Chat:** Dashboard chat returns response for "hello" (instant) and "explain async/await" (Fast AI)
- [ ] **Schema Validation:** `bad` CLI command shows constraint violation errors
- [ ] **Dashboard:** React UI loads at `http://localhost:5173` and shows live data

---

## 13. Project File Structure

```
agentic-mesh/
│
├── .env                    # Environment configuration (per-node)
├── .gitignore              # Git ignore rules
├── .dockerignore           # Docker build exclusions
├── Dockerfile              # Container image definition
├── docker-compose.yml      # Multi-node Docker orchestration
├── package.json            # Dependencies and scripts
├── package-lock.json       # Dependency lock file
├── vite.config.js          # Vite + React + Tailwind configuration
├── eslint.config.js        # ESLint flat config
│
├── index.js                # ★ MAIN ENTRY POINT — boots all subsystems
├── index.html              # Vite HTML entry for React dashboard
├── run-demo.js             # Multi-node demo runner
├── test-ai.js              # Quick AI model test
├── test-planner.js         # Quick planner test
│
├── src/
│   ├── agents/             # AI agent layer
│   │   ├── agent-chat.js          # 3-tier chat engine
│   │   ├── ollama.js              # Ollama SDK wrapper
│   │   ├── router.js              # Fast-path vs AI-path routing
│   │   ├── planner.js             # NL to SQL planner
│   │   ├── executor.js            # Multi-operation executor
│   │   ├── validator.js           # AI transaction auditor
│   │   └── model-validator.js     # Model plan validator
│   │
│   ├── db/                 # Database layer
│   │   ├── sqlite.js              # Schema, CRUD, mesh log
│   │   ├── sync.js                # VectorClock + SyncEngine
│   │   ├── mutation.js            # Safe mutation execution
│   │   └── schema-validator.js    # Allowlist + constraint checks
│   │
│   ├── p2p/                # P2P networking layer
│   │   ├── node.js                # libp2p node factory
│   │   ├── gossip.js              # GossipSub pub/sub
│   │   └── discovery.js           # mDNS PeerRegistry
│   │
│   ├── websocket/          # Gateway layer
│   │   └── server.js              # Express + WebSocket server
│   │
│   ├── api/                # CLI layer
│   │   └── routes.js              # Interactive CLI
│   │
│   ├── utils/              # Shared utilities
│   │   ├── config.js              # Environment config loader
│   │   ├── protocol.js            # Message envelope + LRU cache
│   │   └── logger.js              # Color-coded logging
│   │
│   ├── AgenticMeshDashboard.jsx   # ★ Main React dashboard component
│   ├── App.jsx             # React app root
│   ├── App.css             # App styles
│   ├── main.jsx            # Vite React entry point
│   └── index.css           # Global CSS (Tailwind)
│
├── test/                   # Test suite
│   ├── vector-clock.test.js
│   ├── sync.test.js
│   ├── schema-validator.test.js
│   ├── mutation.test.js
│   ├── model-validator.test.js
│   ├── api.test.js
│   └── two-node-smoke.js
│
├── data/                   # SQLite database files (auto-created)
├── dist/                   # Vite production build output
├── docs/                   # Documentation
├── public/                 # Static assets
├── training/               # Training data / notes
├── paper/                  # Research paper (do not modify)
└── node_modules/           # npm packages
```

---

## 14. Troubleshooting

### Node Won't Start

| Symptom | Fix |
|---|---|
| `better-sqlite3` build error | Install C++ build tools (see Prerequisites 3.1) |
| `EADDRINUSE` port error | Another process is using that port. Change `P2P_PORT` or `WS_PORT` |
| `Cannot find module` | Run `npm install` again |

### Peers Not Discovering Each Other

| Symptom | Fix |
|---|---|
| No peers shown in `peers` command | Ensure both nodes are on the **same LAN/Wi-Fi** |
| Firewall blocking mDNS | Allow UDP port 5353 (mDNS) and your P2P TCP port |
| Ports not unique | Each node on the same machine must use different `P2P_PORT` and `WS_PORT` |

### AI Chat Not Responding

| Symptom | Fix |
|---|---|
| "Ollama not available" | Start Ollama: `ollama serve` |
| "Model not installed" | Pull the model: `ollama pull qwen2.5-coder:1.5b` |
| Response very slow (> 30s) | Check `ollama ps` — unload heavy models: `ollama stop gemma4:e2b` |
| 100% CPU, no response | Model too large for hardware. Use `qwen2.5-coder:1.5b` |
| `TimeoutError` | Increase `OLLAMA_TIMEOUT_MS` or use a smaller model |

### Dashboard Issues

| Symptom | Fix |
|---|---|
| Dashboard blank / won't connect | Ensure the node is running on the matching `WS_PORT` |
| `WebSocket connection failed` | Check browser console; verify `ws://localhost:<WS_PORT>` is correct |
| Build errors | Run `npm run build:dashboard` and check for TypeScript/JSX errors |

### Data Sync Issues

| Symptom | Fix |
|---|---|
| Transaction not replicating | Check GossipSub: logs should show "Published TRANSACTION" |
| `tx:conflict` events | Schema validation failed — check foreign key and constraint rules |
| Duplicate transactions ignored | Normal behavior — `_mesh_log` deduplicates by transaction ID |

---

## 15. Production Checklist

### Before Deploying a New Node

- [ ] Node.js v20+ installed
- [ ] `npm install` completed without errors
- [ ] `.env` configured with unique `NODE_NAME`, `P2P_PORT`, `WS_PORT`, `DB_PATH`
- [ ] Firewall allows P2P and WS ports
- [ ] (If AI node) Ollama running with correct model pulled
- [ ] Node boots with "All systems online" message
- [ ] At least one other node visible in `peers` output
- [ ] `db` command shows expected seed data
- [ ] Test transaction replicates between nodes

### Security Considerations

- **Local network only** — mDNS discovery works on LAN. For WAN deployment, replace mDNS with static peer lists or a rendezvous server.
- **No authentication** — the current WebSocket gateway has no auth. Add JWT or API keys for production.
- **SQLite is per-node** — no shared file system required. Each node has its own `data/*.db`.
- **Ollama runs locally** — no data leaves the machine. All AI inference is on-device.

---

## Appendix A: Adding a New Node (Cheatsheet)

```bash
# 1. Clone project
git clone <repo> agentic-mesh && cd agentic-mesh

# 2. Install dependencies
npm install

# 3. Create .env (choose unique name/ports)
cat > .env << 'EOF'
NODE_NAME=echo
NODE_ROLE=peer
P2P_PORT=9005
WS_PORT=3005
DB_PATH=./data/echo.db
OLLAMA_HOST=http://127.0.0.1:11434
OLLAMA_MODEL=qwen2.5-coder:1.5b
OLLAMA_NUM_CTX=1024
OLLAMA_NUM_PREDICT=128
OLLAMA_TEMPERATURE=0.2
OLLAMA_TIMEOUT_MS=90000
OLLAMA_KEEP_ALIVE=5m
EOF

# 4. (Optional) Install Ollama + model
ollama pull qwen2.5-coder:1.5b

# 5. Start node
node index.js

# 6. Verify peers
# In CLI: peers

# 7. (Optional) Start dashboard
npm run dev:dashboard
```

---

## Appendix B: Key npm Scripts

| Script | Command | Description |
|---|---|---|
| `npm start` | `node index.js` | Start a mesh node |
| `npm run dev:node` | `node index.js` | Same as start |
| `npm run dev:dashboard` | `vite` | Start Vite dev server for React dashboard |
| `npm run build:dashboard` | `vite build` | Production build of dashboard |
| `npm test` | `node --test test/**/*.test.js` | Run all unit tests |
| `npm run demo:smoke` | `node test/two-node-smoke.js` | Two-node replication smoke test |
| `npm run lint` | `eslint .` | Lint all source files |
| `npm run docker:up` | `docker compose up -d` | Start Docker Compose stack |
| `npm run docker:down` | `docker compose down` | Stop Docker Compose stack |

---

## Appendix C: Technology Stack Summary

| Layer | Technology | Version | Purpose |
|---|---|---|---|
| Runtime | Node.js | 22 LTS | JavaScript runtime |
| Database | better-sqlite3 | 13.x | Embedded SQLite with WAL mode |
| P2P Transport | @libp2p/tcp | 11.x | TCP connections between nodes |
| P2P Discovery | @libp2p/mdns | 12.x | Zero-config LAN discovery |
| P2P PubSub | @libp2p/gossipsub | 17.x | Epidemic broadcast for transactions |
| P2P Crypto | @chainsafe/libp2p-noise | 17.x | Encrypted peer connections |
| P2P Muxer | @libp2p/mplex | 12.x | Stream multiplexing |
| AI Runtime | Ollama | Latest | Local LLM inference server |
| AI SDK | ollama (npm) | 0.6.x | JavaScript client for Ollama API |
| HTTP Server | Express | 5.x | REST API and WS upgrade |
| WebSocket | ws | 8.x | Real-time client-server communication |
| Frontend | React | 19.x | Dashboard UI framework |
| Build Tool | Vite | 8.x | Frontend bundler and dev server |
| CSS | TailwindCSS | 4.x | Utility-first CSS framework |
| UUID | uuid | 14.x | Transaction ID generation |
| Logging | chalk | 6.x | Color-coded terminal output |
