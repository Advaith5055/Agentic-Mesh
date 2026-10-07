# Agentic Mesh — Autonomous P2P Database Swarm

> **An autonomous peer-to-peer database swarm combining AI-driven planning, role-specialised mesh nodes, vector-clock causal replication, and real-time visual dashboards.**

[![Node.js](https://img.shields.io/badge/Node.js-v20%2B-339933?logo=node.js&logoColor=white)](https://nodejs.org/)
[![React](https://img.shields.io/badge/React-19-61DAFB?logo=react&logoColor=white)](https://react.dev/)
[![SQLite](https://img.shields.io/badge/SQLite-WAL-003B57?logo=sqlite&logoColor=white)](https://www.sqlite.org/)
[![libp2p](https://img.shields.io/badge/libp2p-GossipSub-blue)](https://libp2p.io/)
[![Ollama](https://img.shields.io/badge/Ollama-Local_LLM-black)](https://ollama.com/)
[![License](https://img.shields.io/badge/License-MIT-green)](#license)

---

## Table of Contents

- [Architecture Overview](#-architecture-overview)
- [Key Features](#-key-features)
- [Project Structure](#-project-structure)
- [Node Roles & Multi-Node Architecture](#-node-roles--multi-node-architecture)
- [Task State Machine & Lifecycle](#-task-state-machine--lifecycle)
- [Dual Dashboards](#-dual-dashboards)
- [Agent Activity & Instrumentation](#-agent-activity--instrumentation)
- [Workflow Security & API Authentication](#-workflow-security--api-authentication)
- [Workflow Replication & Vector Clocks](#-workflow-replication--vector-clocks)
- [Prerequisites](#-prerequisites)
- [Quick Start](#-quick-start)
- [Multi-Node Local Demo](#-multi-node-local-demo)
- [Docker Deployment](#-docker-deployment)
- [REST API Reference](#-rest-api-reference)
- [Automated Testing](#-automated-testing)
- [AI Planning & QLoRA Fine-Tuning](#-ai-planning--qlora-fine-tuning)
- [Environment Variables Reference](#-environment-variables-reference)
- [Academic Report & Diagrams](#-academic-report--diagrams)
- [Documentation](#-documentation)
- [Author](#-author)

---

## 🌟 Architecture Overview

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│              Operator Dashboards  (React 19 + Vite + Tailwind)             │
│    Planner Dashboard (:5173)              Executor Dashboard (:5174)       │
│    Kanban Board • Topology HUD            Execution Monitor • Activity     │
│    Agent Chat • Approval Modal            Replication Metrics • Conflicts  │
└───────────────┬───────────────────────────────────────┬─────────────────────┘
                │ WebSocket + REST (:3004)               │ WebSocket + REST (:3002)
┌───────────────▼───────────────┐       ┌───────────────▼───────────────────┐
│      PLANNER NODE (delta)     │       │       EXECUTOR NODE (alpha)       │
│                               │       │                                   │
│  ┌─────────────────────────┐  │       │  ┌─────────────────────────────┐  │
│  │  Task Ingestion &       │  │       │  │  Role Authorisation Guard   │  │
│  │  State Machine          │  │       │  │  (executor/peer only)       │  │
│  └──────────┬──────────────┘  │       │  └──────────┬──────────────────┘  │
│  ┌──────────▼──────────────┐  │       │  ┌──────────▼──────────────────┐  │
│  │  Localised LLM Planner  │  │       │  │  Schema Allowlist Validator │  │
│  │  (Ollama: qwen2.5-coder │  │       │  │  Tables, columns, types,   │  │
│  │   :1.5b / gemma3:4b)    │  │       │  │  FK, bounds (<1ms)         │  │
│  └──────────┬──────────────┘  │       │  └──────────┬──────────────────┘  │
│  ┌──────────▼──────────────┐  │       │  ┌──────────▼──────────────────┐  │
│  │  Risk Assessment &      │  │       │  │  Atomic Transaction Service │  │
│  │  Safety Gate            │  │       │  │  SQLite commit + rollback   │  │
│  └─────────────────────────┘  │       │  └─────────────────────────────┘  │
└───────────────┬───────────────┘       └───────────────┬───────────────────┘
                │ EXECUTION_REQUEST                     │ Atomic Commit
                │                                       │
┌───────────────▼───────────────────────────────────────▼───────────────────┐
│           Peer-to-Peer Communication Mesh  (libp2p)                      │
│  mesh:control (Presence & Handoff)  •  mesh:transactions (GossipSub Tx)  │
│  mesh:sync (Vector-Clock Diff)  •  TCP + Noise + mDNS Discovery         │
└───────────────┬───────────────────────────────────────┬───────────────────┘
                │ Vector Clock Sync                     │ Transaction Gossip
┌───────────────▼──────────────┐       ┌────────────────▼──────────────────┐
│  Local SQLite (delta.db)     │       │  Local SQLite (alpha.db)          │
│  _mesh_log • Tasks & Steps   │       │  _mesh_log • Replicated Data     │
│  V_δ=[δ:42, α:38]           │       │  V_α=[δ:42, α:39]               │
└──────────────────────────────┘       └───────────────────────────────────┘
```

---

## ✨ Key Features

| Category | Feature |
|:---|:---|
| **P2P Networking** | libp2p with TCP transport, Noise encryption, Mplex multiplexing, mDNS auto-discovery, GossipSub pub/sub |
| **Causal Replication** | Vector-clock ordering, append-only `_mesh_log`, anti-entropy sync, deduplication, deterministic log replay |
| **AI Planning** | Localised Ollama LLM (qwen2.5-coder:1.5b), structured operation DAGs (never raw SQL), 3-tier speed strategy |
| **Role Separation** | Dedicated Planner, Executor, Router, Validator, and all-in-one Peer roles with enforced authorization |
| **Workflow Engine** | 10-table schema, strict state machine (queued → planned → executing → completed), approval gates, audit trails |
| **Dual Dashboards** | Planner Dashboard (Kanban, topology, agent chat) + Executor Dashboard (execution monitor, replication metrics) |
| **Agent Activity** | Real-time hand-off tracking across Router → Planner → Executor → Validator → Mesh with per-agent performance stats |
| **Safety & Security** | Schema allowlist validation (<1ms), origin policies, API key auth, CORS restrictions, role-based mutation guards |
| **Atomic Transactions** | SQLite WAL mode, all-or-nothing atomic commits, complete rollback on error, foreign key enforcement |
| **Fine-Tuning Pipeline** | QLoRA training on Google Gemma with 32+ training examples, evaluation suite, Ollama Modelfile deployment |
| **Docker Orchestration** | 4-node docker-compose with shared Ollama sidecar, persistent volumes, bridge networking |
| **Comprehensive Tests** | 15 test files, 100+ assertions covering schema validation, vector clocks, workflow lifecycle, role enforcement, API security, replication |

---

## 📁 Project Structure

```
agentic-mesh/
├── index.js                          # Main entry point (Planner node boot sequence)
├── package.json                      # Dependencies & scripts
├── vite.config.js                    # Vite config for Planner Dashboard
├── index.html                        # Planner Dashboard HTML entry
├── docker-compose.yml                # 4-node Docker orchestration
├── Dockerfile                        # Container build
├── .env                              # Environment configuration (gitignored)
│
├── src/
│   ├── agents/
│   │   ├── activity.js               # Agent activity tracker & system sampler
│   │   ├── agent-chat.js             # Conversational AI with 3-tier speed strategy
│   │   ├── coordinator.js            # Mesh agent coordinator (role-based delegation)
│   │   ├── executor.js               # Transaction execution & proposal commit/reject
│   │   ├── instrumented.js           # Instrumentation wrappers for all agent hops
│   │   ├── model-validator.js        # Validates LLM plans against allowed schemas
│   │   ├── ollama.js                 # Ollama client with prewarm, model management
│   │   ├── planner.js                # LLM-driven operation plan generation
│   │   ├── proposals.js              # Proposal store for human approval workflow
│   │   ├── router.js                 # Task routing (AI vs. direct fast-path)
│   │   └── validator.js              # AI audit on recent transactions
│   │
│   ├── db/
│   │   ├── sqlite.js                 # SQLite init, schema, seed, WAL mode
│   │   ├── schema-validator.js       # Sub-ms schema allowlist validation
│   │   ├── mutation.js               # Atomic mutation service + _mesh_log
│   │   ├── sync.js                   # Vector clock sync engine
│   │   └── policy.js                 # Origin-based write policies
│   │
│   ├── mesh/
│   │   ├── workflow-service.js       # Distributed workflow engine (1008 lines)
│   │   └── remote-executor.js        # Planner→Executor remote handoff desk
│   │
│   ├── p2p/
│   │   ├── node.js                   # libp2p node creation & lifecycle
│   │   ├── gossip.js                 # GossipSub topic handlers
│   │   ├── discovery.js              # PeerRegistry & mDNS discovery
│   │   └── presence.js               # Live presence broadcasting (CPU/RAM)
│   │
│   ├── websocket/
│   │   └── server.js                 # Express + WebSocket gateway
│   │
│   ├── api/
│   │   └── routes.js                 # Interactive CLI & REST endpoints
│   │
│   ├── utils/
│   │   ├── config.js                 # Environment variable loader
│   │   ├── protocol.js               # Message types, envelopes, codecs
│   │   └── logger.js                 # Coloured structured logger
│   │
│   ├── AgenticMeshDashboard.jsx      # Planner Dashboard (React 19)
│   └── AgentActivityPanel.jsx        # Agent activity visualisation panel
│
├── executor-dashboard/
│   ├── vite.config.js                # Executor Dashboard Vite config (:5174)
│   ├── index.html                    # Executor Dashboard HTML entry
│   └── src/
│       ├── ExecutorMeshDashboard.jsx  # Executor Dashboard (React 19)
│       ├── AgentActivityPanel.jsx     # Agent activity panel (executor view)
│       └── main.jsx                  # React entry point
│
├── test/                             # 15 test files, 100+ assertions
│   ├── schema-validator.test.js      # Schema validation tests
│   ├── mutation.test.js              # Atomic mutation tests
│   ├── vector-clock.test.js          # Vector clock tests
│   ├── sync.test.js                  # Sync engine tests
│   ├── mesh-roles.test.js            # Role enforcement tests
│   ├── api.test.js                   # REST API tests
│   ├── workflow-lifecycle.test.js    # Task state machine tests
│   ├── workflow-validation.test.js   # Workflow validation tests
│   ├── workflow-api.test.js          # Workflow API tests
│   ├── workflow-replication.test.js  # Replication tests
│   ├── workflow-security-and-replication.test.js
│   ├── workflow-migrations.test.js   # Migration tests
│   ├── model-validator.test.js       # Model plan validation tests
│   ├── two-node-smoke.js            # E2E two-node smoke test
│   └── benchmark-metrics.js          # Performance benchmarks
│
├── training/                         # QLoRA fine-tuning pipeline
│   ├── train.jsonl                   # 32+ training examples
│   ├── eval.jsonl                    # 12 evaluation test cases
│   ├── train_qlora.py                # 4-bit QLoRA training script
│   ├── evaluate.py                   # Evaluation script
│   ├── Modelfile                     # Ollama deployment manifest
│   └── README.md                     # Training guide
│
├── report/                           # Academic LaTeX report
│   ├── main.tex                      # Full research paper
│   ├── architecture_diagram.tex      # System architecture (TikZ)
│   └── use_case_diagram.tex          # UML use case diagram (TikZ)
│
├── docs/                             # Documentation
│   ├── PROJECT_GUIDE.md              # Full project guide
│   ├── DEPLOYMENT_GUIDE.md           # Production deployment
│   ├── EXECUTOR_NODE_GUIDE.md        # Executor node setup
│   └── OLLAMA_CPU_GUIDE.md           # CPU optimization guide
│
└── paper/                            # Conference paper (LaTeX)
    ├── main.tex
    └── references.bib
```

---

## 🎭 Node Roles & Multi-Node Architecture

Agentic Mesh supports dedicated role specialisations across laptops or processes:

```text
┌───────────────────────┐       PLAN_REQUEST (Prompt)       ┌───────────────────────┐
│      Router Node      │ ────────────────────────────────► │     Planner Node      │
│  (CLI / REST / Web)   │                                   │ (Ollama + Validation) │
│                       │ ◄──────────────────────────────── │                       │
└──────────┬────────────┘       PLAN_RESPONSE (Plan ops)    └───────────────────────┘
           │                                                             ▲
           │ EXECUTION_REQUEST (Validated ops)                           │
           ▼                                                             │
┌───────────────────────┐                                                │
│     Executor Node     │                                                │
│  (Revalidates ops &   │                                                │
│   commits to SQLite)  │ ──► mesh:transactions (GossipSub) ─────────────┤
└──────────┬────────────┘                                                │
           │                                                             │
           │ EXECUTION_RESPONSE (Commit summary)                         │
           ▼                                                             │
       [Router OK]                                                 [Peer Sync]
```

### Role Matrix

| Role | Responsibility | Data Mutations | Task Execution | AI Inference |
|:---|:---|:---:|:---:|:---:|
| `peer` *(default)* | All-in-one standalone | ✅ | ✅ | ✅ |
| `router` | Gateway — receives prompts, routes to peers | ❌ | ❌ | ❌ |
| `planner` | AI planner — decomposes prompts into structured plans | ❌ | ❌ | ✅ |
| `executor` | Executor — revalidates & commits atomic transactions | ✅ | ✅ | ❌ |
| `validator` | Auditor — validates completed runs, audits logs | ❌ | ❌ | ✅ |

> [!IMPORTANT]
> **Enforced Node-Role Authorisation:**
> - Only `executor` and `peer` nodes may execute tasks or mutate the database.
> - `planner` nodes create plans but **never execute** or write to the database.
> - Unauthorised role actions are rejected with `403 FORBIDDEN` and logged to the immutable `audit_events` table.
> - Node identity is derived from trusted server configuration, ignoring client overrides.

---

## 🔄 Task State Machine & Lifecycle

The distributed workflow engine enforces a strict task state machine:

```text
               ┌───────────────────────┐
               │        QUEUED         │
               └───────────┬───────────┘
                           │ planTask()
                           ▼
               ┌───────────────────────┐  [High Risk]
               │        PLANNED        │ ──────────────────┐
               └───────────┬───────────┘                    │
                           │ executeTask()                  ▼
                           │                  ┌──────────────────────┐
                           │                  │   AWAITING_APPROVAL  │
                           │                  └──────┬──────┬────────┘
                           │          [Approved]     │      │ [Rejected]
                           │     ┌───────────────────┘      │
                           │     ▼                          ▼
                           │  (PLANNED)              ┌───────────┐
                           │     │                   │   FAILED   │
                           ▼     ▼                   └───────────┘
               ┌───────────────────────┐                    ▲
               │       EXECUTING       │ ───────────────────┘
               └───────────┬───────────┘   [Error]
                           │ [Success]
                           ▼
               ┌───────────────────────┐
               │       COMPLETED       │  (Terminal)
               └───────────────────────┘

   * Any active state ──► CANCELLED (explicit cancellation with audit)
```

**Allowed Transitions:** `queued → planned → awaiting_approval → planned` (approved) | `→ failed` (rejected) | `planned → executing → completed` | `→ failed` | `* → cancelled`

**Protection Guarantees:** Execution is strictly rejected for `queued`, `awaiting_approval`, `completed`, `failed`, `cancelled`, and already-`executing` tasks. A completed task can never execute twice.

---

## 🖥️ Dual Dashboards

### Planner Dashboard (`:5173`)
The primary operational dashboard for the Planner node:
- **Kanban Workflow Board** — Drag-and-drop task lifecycle visualisation across all states
- **2D Force-Directed Topology** — Live mesh topology with peer connections and roles
- **Agent Assistant** — Conversational AI chat with streaming responses
- **Human Approval Modal** — Review and approve/reject high-risk operation plans
- **Real-Time Telemetry HUD** — Node count, success rate, avg execution time, replication latency
- **Audit History** — Immutable timeline of all agent actions and system events
- **Replication & Conflicts** — Vector clock sync status and conflict resolution

### Executor Dashboard (`:5174`)
A dedicated monitoring dashboard for Executor nodes:
- **Execution Monitor** — Live transaction execution feed with commit/rollback status
- **Agent Activity Panel** — Step-by-step hand-off tracing (Router → Planner → Executor)
- **Replication Metrics** — Throughput, latency, and vector clock synchronisation stats
- **Conflict Viewer** — Concurrent modification detection and resolution log

---

## 📊 Agent Activity & Instrumentation

The `ActivityTracker` records every agent hand-off in the distributed pipeline:

```text
User → Router → Planner (LLM) → Validator → Executor → Mesh (GossipSub) → Peer Sync
```

- **Per-Agent Performance Stats** — Latency, call count, success/error rate for each agent type
- **Live CPU/RAM Sampling** — System resource monitoring via `os.cpus()` integration
- **Dashboard Streaming** — All activity events are broadcast via WebSocket to both dashboards
- **Instrumented Wrappers** — Every operation (plan, execute, approve, audit, sync) is wrapped with timing and error tracking

---

## 🔐 Workflow Security & API Authentication

| Security Layer | Details |
|:---|:---|
| **API Key Auth** | `x-api-key` or `Authorization: Bearer` header on all mutation endpoints |
| **Origin Policies** | Per-origin write restrictions (`api`, `nl`, `vision`, `observer`) |
| **Schema Validation** | Sub-millisecond allowlist check on tables, columns, types, bounds, FK |
| **Role Enforcement** | Server-side node role authorisation (ignores client-supplied role) |
| **CORS Restriction** | Configured origins only (no wildcard `*`) |
| **Structured Errors** | `{ error, code, status, success, errors }` — no stack trace leakage |
| **Audit Trail** | Every action logged to immutable `audit_events` table |

---

## 🌐 Workflow Replication & Vector Clocks

Every workflow record is a first-class citizen of the replication protocol:

- **Canonical Mutation Routing** — Writes to all 10 workflow tables routed through `executeMutation`
- **Audit & Mesh Log** — Every write appends to `_mesh_log` and advances the vector clock
- **Atomic Remote Commits** — Remote transactions verified against schema before atomic SQLite commit
- **Automatic Conflict Detection** — Concurrent task assignments and schema divergence logged to `conflicts`
- **Replication Metrics** — Latency, throughput, and vector clock metadata logged to `replication_metrics`
- **Deterministic Log Replay** — Missing entries replayed in creation order (`ORDER BY rowid ASC`)
- **Deduplication** — Transactions identified by unique ID; duplicates skipped automatically

---

## 📋 Prerequisites

- **Node.js** v20 or v22 LTS (ESM native support)
- **Network** — Devices on the same LAN/Wi-Fi for mDNS auto-discovery
- **Ollama** *(optional)* — For AI planning and conversational coding:
  ```bash
  # Install from https://ollama.com
  ollama pull qwen2.5-coder:1.5b
  ollama serve
  ```
  > If Ollama is offline, the entire mesh operates normally in fast-path mode.

---

## ⚡ Quick Start

### Single Node

```bash
# 1. Install dependencies
npm install

# 2. Start backend node
npm run dev:node

# 3. Start Planner Dashboard (new terminal)
npm run dev:dashboard
```

Open [http://localhost:5173](http://localhost:5173) in your browser.

### Start Executor Dashboard

```bash
# In a separate terminal
npm run dev:executor-dashboard
```

Open [http://localhost:5174](http://localhost:5174) for the Executor Dashboard.

---

## 🌐 Multi-Node Local Demo

### Terminal 1 — Planner Node (Delta)

```bash
npm run dev:node
```

- **Node**: `delta` (Planner) — P2P `:9004`, WS/API `:3004`

### Terminal 2 — Executor Node (Alpha)

**Windows:**
```cmd
cmd.exe /c "set NODE_NAME=alpha&& set NODE_ROLE=executor&& set P2P_PORT=9002&& set WS_PORT=3002&& set DB_PATH=./data/alpha.db&& node index.js"
```

**Linux / macOS:**
```bash
NODE_NAME=alpha NODE_ROLE=executor P2P_PORT=9002 WS_PORT=3002 DB_PATH=./data/alpha.db node index.js
```

### Terminal 3 — Dashboards

```bash
# Planner Dashboard
npm run dev:dashboard          # → http://localhost:5173

# Executor Dashboard (separate terminal)
npm run dev:executor-dashboard  # → http://localhost:5174
```

### Multi-Node Test Flow

```text
User Input → Router → Planner (LLM) → PLAN_RESPONSE → Executor → Atomic Commit → GossipSub → Peer Sync
```

1. **User** submits `ask Add mechanical keyboard` via CLI or `POST /api/ask`
2. **Planner** decomposes via Ollama into structured operations (`INSERT item`)
3. **Executor** revalidates against schema constraints inside an atomic transaction
4. **GossipSub** broadcasts the committed transaction to `mesh:transactions`
5. **All peers** replicate via vector-clock anti-entropy sync

---

## 🐳 Docker Deployment

The `docker-compose.yml` orchestrates a 4-node mesh with a shared Ollama sidecar:

```bash
# Start all nodes
docker compose up -d

# View logs
docker compose logs -f

# Stop
docker compose down
```

| Container | Role | P2P Port | WS Port |
|:---|:---|:---:|:---:|
| `mesh-node-alpha` | peer | 9001 | 3001 |
| `mesh-node-bravo` | peer | 9002 | 3002 |
| `mesh-node-charlie` | peer | 9003 | 3003 |
| `mesh-node-delta` | ai-agent | 9004 | 3004 |
| `agentic-mesh-ollama` | LLM sidecar | — | 11434 |

---

## 📡 REST API Reference

All endpoints served on `WS_PORT` (default `3004`):

### Core Endpoints

| Endpoint | Method | Description |
|:---|:---|:---|
| `/api/health` | `GET` | Node status, uptime, peer count, model |
| `/api/peers` | `GET` | Discovered mesh peers and multiaddrs |
| `/api/db/state` | `GET` | Database snapshot (categories, items, suppliers) |
| `/api/db/mesh-log` | `GET` | Replication log entries (`?limit=N`) |
| `/api/db/schema` | `GET` | DDL schema from SQLite master |

### Transaction & AI Endpoints

| Endpoint | Method | Description |
|:---|:---|:---|
| `/api/propose` | `POST` | Execute a fast-path transaction |
| `/api/ask` | `POST` | AI planner (preview or execute) |
| `/api/ask/confirm` | `POST` | Confirm & execute planned operations |
| `/api/sync` | `POST` | Broadcast anti-entropy sync request |
| `/api/audit` | `POST` | Trigger background AI audit |

### Workflow Endpoints *(Auth Required)*

| Endpoint | Method | Description |
|:---|:---|:---|
| `/api/nodes/register` | `POST` | Register a mesh node |
| `/api/tasks` | `GET/POST` | List / create tasks |
| `/api/tasks/:id/plan` | `POST` | Plan a queued task |
| `/api/tasks/:id/execute` | `POST` | Execute a planned task |
| `/api/tasks/:id/cancel` | `POST` | Cancel an active task |
| `/api/approvals/:id/decide` | `POST` | Approve or reject a task |
| `/api/conflicts/:id/resolve` | `POST` | Resolve a replication conflict |
| `/api/replication/metrics` | `POST` | Record replication metrics |

---

## 🧪 Automated Testing

```bash
# Run full test suite (15 test files, 100+ assertions)
npm test

# End-to-end two-node smoke test
npm run demo:smoke

# Linting
npm run lint

# Build dashboards
npm run build:dashboard
npm run build:executor-dashboard
```

### Test Coverage

| Test File | Coverage |
|:---|:---|
| `schema-validator.test.js` | Table/column/type/FK/bound validation |
| `mutation.test.js` | Atomic commit, rollback, mesh_log integrity |
| `vector-clock.test.js` | Increment, merge, comparison operations |
| `sync.test.js` | Sync engine, delta exchange, deduplication |
| `mesh-roles.test.js` | Role enforcement (planner ≠ executor) |
| `api.test.js` | REST API endpoints, auth, error handling |
| `workflow-lifecycle.test.js` | State machine transitions, terminal states |
| `workflow-validation.test.js` | Task validation, schema constraints |
| `workflow-api.test.js` | Workflow REST API integration |
| `workflow-replication.test.js` | Cross-node replication correctness |
| `workflow-security-and-replication.test.js` | Auth + role + replication combined |
| `workflow-migrations.test.js` | Schema migration idempotency |
| `model-validator.test.js` | LLM plan validation against schemas |
| `two-node-smoke.js` | E2E multi-node integration test |
| `benchmark-metrics.js` | Performance benchmarks |

---

## 🧠 AI Planning & QLoRA Fine-Tuning

### 3-Tier Speed Strategy

| Tier | Latency | Engine | Details |
|:---|:---|:---|:---|
| **Instant** | `<1s` | Pure JS / SQLite | Greetings, DB state, schema DDL, local templates |
| **Fast AI** | `2–4s` | `qwen2.5-coder:1.5b` | Slim prompt, ≤2 prior messages, 1024 ctx, 128 output tokens |
| **Heavy AI** | Slow | `gemma3:4b` | Deep architecture & complex planning (manual toggle) |

### Fine-Tuning Pipeline

```bash
# 1. Train with 4-bit QLoRA on Google Gemma
python training/train_qlora.py --model_id google/gemma-3-1b-it --epochs 3

# 2. Evaluate
python training/evaluate.py --offline

# 3. Deploy to Ollama
ollama create agentic-mesh-planner -f training/Modelfile
```

- **Training Data**: 32+ multi-table 3NF examples in `training/train.jsonl`
- **Evaluation**: 12 test cases in `training/eval.jsonl`
- See [`training/README.md`](training/README.md) for full instructions.

---

## ⚙️ Environment Variables Reference

| Variable | Default | Description |
|:---|:---|:---|
| `NODE_NAME` | `node-<random>` | Display name and originator tag |
| `NODE_ROLE` | `peer` | `peer`, `router`, `planner`, `executor`, `validator` |
| `P2P_PORT` | `9004` | TCP port for libp2p P2P transport |
| `WS_PORT` | `3004` | HTTP and WebSocket port |
| `DB_PATH` | `./data/mesh.db` | SQLite database file path |
| `EXECUTOR_PEER` | `alpha` | Preferred executor peer name |
| `BOOTSTRAP_PEERS` | — | Multiaddr(s) for fixed bootstrap peers |
| `OLLAMA_HOST` | `http://127.0.0.1:11434` | Ollama LLM endpoint |
| `OLLAMA_MODEL` | `qwen2.5-coder:1.5b` | Model name for planning/auditing |
| `OLLAMA_NUM_CTX` | `2048` | Context window size |
| `OLLAMA_NUM_PREDICT` | `512` | Max output tokens |
| `OLLAMA_TIMEOUT_MS` | `90000` | Inference timeout |
| `PLAN_TIMEOUT_MS` | `45000` | Remote PLAN_RESPONSE timeout |
| `EXECUTION_TIMEOUT_MS` | `45000` | Remote EXECUTION_RESPONSE timeout |
| `ALLOW_PUBLIC_READ` | `true` | Allow unauthenticated read endpoints |
| `SEED_DEMO_DATA` | `false` | Seed sample tasks/approvals for demos |
| `API_KEY` | `mesh-dev-key` | API key for mutation endpoints |
| `CORS_ORIGINS` | `http://localhost:5173` | Allowed CORS origins |

---

## 📐 Academic Report & Diagrams

The [`report/`](report/) directory contains a complete academic paper:

- **`report/main.tex`** — Full research paper (LaTeX)
- **`report/architecture_diagram.tex`** — System architecture diagram (TikZ)
- **`report/use_case_diagram.tex`** — UML use case diagram (TikZ)
- **`paper/main.tex`** — Conference paper with `references.bib`

Compile with:
```bash
pdflatex report/main.tex
pdflatex report/architecture_diagram.tex
pdflatex report/use_case_diagram.tex
```

---

## 📚 Documentation

| Document | Description |
|:---|:---|
| [`docs/PROJECT_GUIDE.md`](docs/PROJECT_GUIDE.md) | Complete project guide — architecture, config, API, testing |
| [`docs/DEPLOYMENT_GUIDE.md`](docs/DEPLOYMENT_GUIDE.md) | Production deployment instructions |
| [`docs/EXECUTOR_NODE_GUIDE.md`](docs/EXECUTOR_NODE_GUIDE.md) | Executor node setup & configuration |
| [`docs/OLLAMA_CPU_GUIDE.md`](docs/OLLAMA_CPU_GUIDE.md) | Ollama CPU optimisation guide |
| [`training/README.md`](training/README.md) | QLoRA fine-tuning pipeline guide |

---

## 👤 Author

**Advaith J**

---

## 📄 License

MIT
