# Agentic Mesh — Autonomous P2P Database Swarm

Agentic Mesh is an autonomous peer-to-peer (P2P) database swarm combining **Node.js ESM**, **SQLite (3NF schema with WAL)**, **libp2p + GossipSub**, **mDNS peer discovery**, **vector-clock causal replication**, **Ollama-driven AI planning and auditing**, an **Express REST API**, and a **real-time React/Vite visual dashboard**.

> **Complete documentation:** See [docs/PROJECT_GUIDE.md](docs/PROJECT_GUIDE.md) for the role architecture, Planner-node setup, full configuration reference, API guide, testing, Docker notes, and troubleshooting.

---

## 🌟 Architecture Overview

```text
┌─────────────────────────────────────────────────────────────┐
│                    React / Vite Dashboard                   │
│          Live Topology HUD • Real-Time DB • Mesh Log        │
└──────────────────────────────┬──────────────────────────────┘
                               │ WebSocket & REST (:3001/:3002)
┌──────────────────────────────▼──────────────────────────────┐
│                      Mesh Node (Node.js)                    │
│                                                             │
│  ┌──────────────────────┐        ┌───────────────────────┐  │
│  │   AI Agent Layer     │        │   Schema Validator    │  │
│  │  Planner / Auditor   │        │   Pure JS Fast-Path   │  │
│  │  (Ollama / Gemma)    │        │   <1ms Constraints    │  │
│  └──────────┬───────────┘        └───────────┬───────────┘  │
│             │                                │              │
│  ┌──────────▼────────────────────────────────▼───────────┐  │
│  │             Canonical Mutation Service                │  │
│  │         Atomic SQLite Transaction + _mesh_log         │  │
│  └──────────────────────────┬────────────────────────────┘  │
│                             │                               │
│  ┌──────────────────────────▼────────────────────────────┐  │
│  │             Vector-Clock Sync Engine                  │  │
│  │       Causal Ordering & Deterministic Mesh Log        │  │
│  └──────────────────────────┬────────────────────────────┘  │
│                             │                               │
│  ┌──────────────────────────▼────────────────────────────┐  │
│  │               SQLite 3NF Storage Engine               │  │
│  │   Categories • Items • Suppliers • item_suppliers     │  │
│  └──────────────────────────┬────────────────────────────┘  │
│                             │                               │
│  ┌──────────────────────────▼────────────────────────────┐  │
│  │              libp2p P2P Network Layer                 │  │
│  │     TCP • Noise • Mplex • mDNS • GossipSub Topics     │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Key Subsystems

1. **Storage Subsystem (`src/db/sqlite.js`)**:
   - High-performance SQLite database via `better-sqlite3` in WAL mode (`PRAGMA foreign_keys = ON`).
   - Fully normalized 3NF schema: `categories`, `items`, `suppliers`, `item_suppliers` (composite primary key), and `_mesh_log`.
2. **Deterministic Schema Validator (`src/db/schema-validator.js`)**:
   - Sub-millisecond synchronous validation checking operation allowlists (`INSERT`, `UPDATE`, `DELETE`), types, bounds (`price > 0`), foreign key integrity, and unique constraints (`sku`, `name`).
   - Strict table and column allowlists before constructing SQL to prevent injection or arbitrary manipulation.
3. **Safe Mutation Service (`src/db/mutation.js`)**:
   - Executes mutations atomically: schema validation, SQL write, and `_mesh_log` insertion occur inside a single SQLite transaction.
   - Provides all-or-nothing rollback for single writes and batch operations.
4. **Vector Clock Synchronization (`src/db/sync.js`)**:
   - Instantiable `SyncEngine` with vector clocks tracking causal ordering per node.
   - Anti-entropy sync protocol: peers exchange clocks on connection; missing log entries are delivered in deterministic FIFO insertion order.
   - Deduplication skips already-applied transactions; remote transactions are validated against local schema before atomic commit.
5. **P2P Mesh Networking (`src/p2p/`)**:
   - Built on `libp2p` with TCP transport, Noise encryption, and Mplex multiplexing.
   - Local peer discovery via mDNS with automatic peer dialing upon discovery.
   - PubSub replication over GossipSub (`mesh:transactions` and `mesh:sync` topics).
6. **AI Agent Layer (`src/agents/`)**:
   - **Safe & Structured**: The AI generates structured tool operation plans, never raw SQL.
   - Model validator (`src/agents/model-validator.js`) verifies model plans against allowed schemas before presentation or execution.
   - Optional: Operates in high-speed fast-path mode when Ollama is offline.
7. **Unified Management & Visual HUD**:
   - Interactive CLI (`src/api/routes.js`) for commands: `propose`, `ask`, `bad`, `peers`, `db`, `sync`, `audit`, `status`.
   - Web dashboard (`src/AgenticMeshDashboard.jsx`) providing live force-directed swarm topology, real-time database state, event logs, and replication status.

---

## 🎭 Node Roles & Multi-Laptop Architecture

Agentic Mesh supports dedicated role specializations across laptops or processes via `NODE_ROLE`:

```text
┌───────────────────────┐         PLAN_REQUEST (Prompt)         ┌───────────────────────┐
│      Router Node      │ ────────────────────────────────────► │     Planner Node      │
│  (CLI / REST / Web)   │                                       │ (Ollama + Validation) │
│                       │ ◄──────────────────────────────────── │                       │
└──────────┬────────────┘         PLAN_RESPONSE (Plan ops)      └───────────────────────┘
           │                                                                ▲
           │ EXECUTION_REQUEST (Validated ops)                              │
           ▼                                                                │
┌───────────────────────┐                                                   │
│     Executor Node     │                                                   │
│  (Revalidates ops &   │                                                   │
│   commits to SQLite)  │ ───► mesh:transactions (GossipSub replication) ───┤
└──────────┬────────────┘                                                   │
           │                                                                │
           │ EXECUTION_RESPONSE (Commit summary)                            │
           ▼                                                                │
       [Router OK]                                                    [Peer Sync]
```

### Supported Roles

| Role | Responsibility | Data Mutations | Task Execution | AI Inference |
| :--- | :--- | :---: | :---: | :---: |
| `peer` *(default)* | All-in-one standalone behavior (plans, executes, validates, and syncs). | Yes | Yes | Yes |
| `router` | Gateway node: receives user prompts, routes workflow requests, coordinates peers. | No (delegated) | **NO** | No |
| `planner` | Dedicated AI planner: decomposes prompts into structured plans and runs. **Cannot execute tasks or mutate database.** | **NO** | **NO** | Yes |
| `executor` | Dedicated executor: executes planned tasks, applies validated database mutations, and gossips transactions. | Yes | Yes | No |
| `validator` | Dedicated auditor: validates completed task runs and audits transaction logs. Cannot execute tasks or mutations. | **NO** | **NO** | Yes |

> [!IMPORTANT]
> **Enforced Node-Role Authorization**:
> - Only `executor` and all-in-one `peer` nodes may execute tasks or database mutations.
> - `planner` nodes create plans but must never execute tasks or write to the database.
> - `router` nodes route requests but cannot execute.
> - `validator` nodes validate completed task runs but cannot execute.
> - Direct unauthorized role actions are rejected with a structured `403 FORBIDDEN` response and logged to the immutable `audit_events` table.
> - Acting node identity is always derived from trusted server configuration (`localNodeId`, `localNodeRole`), ignoring client overrides.

---

## 🔄 Task State Machine & Lifecycle

The distributed workflow engine enforces a strict task state machine:

```text
               ┌───────────────────────┐
               │        QUEUED         │
               └───────────┬───────────┘
                           │ planTask()
                           ▼
               ┌───────────────────────┐  [Risk Level requires Approval]
               │        PLANNED        │ ───────────────────────────────┐
               └───────────┬───────────┘                                │
                           │ executeTask()                              │
                           │                                            ▼
                           │                              ┌───────────────────────────┐
                           │                              │     AWAITING_APPROVAL     │
                           │                              └───────┬───────────┬───────┘
                           │                 [Approved]           │           │ [Rejected]
                           │        ┌─────────────────────────────┘           │
                           │        │                                         ▼
                           │        ▼                             ┌───────────────────────────┐
                           │   (Status: PLANNED)                  │          FAILED           │
                           │        │                             └───────────────────────────┘
                           │        │ executeTask()                             ▲
                           ▼        ▼                                           │
               ┌───────────────────────┐          [Execution Error]             │
               │       EXECUTING       │ ───────────────────────────────────────┘
               └───────────┬───────────┘
                           │ [Success]
                           ▼
               ┌───────────────────────┐
               │       COMPLETED       │  (Terminal — cannot execute twice)
               └───────────────────────┘

   * Explicit Cancellation: Tasks in any active state can transition to CANCELLED (Terminal).
```

- **Allowed Transitions**:
  - `queued → planned`
  - `planned → awaiting_approval` (when high/critical risk triggers approval)
  - `awaiting_approval → planned` (when approved by reviewer)
  - `awaiting_approval → failed` (when rejected by reviewer)
  - `planned → executing`
  - `executing → completed` or `executing → failed`
  - `* → cancelled` (explicit cancellation transition with audit entry)
- **Protection Guarantees**: Execution is strictly rejected for `queued`, `awaiting_approval`, `completed`, `failed`, `cancelled`, and already-`executing` tasks. A completed task can never execute twice.

---

## 🔐 Workflow Security & API Authentication

Endpoints are secured with configurable API authentication and strict CORS:

- **Configurable API Key**: Set via `API_KEY` or `MESH_API_KEY` (default `mesh-dev-key` for local development).
- **Authentication Header**: Provide either `x-api-key: <token>` or `Authorization: Bearer <token>`.
- **Protected Mutation Endpoints**: All write and lifecycle endpoints require valid authentication:
  - `POST /api/nodes/register`
  - `POST /api/tasks`, `POST /api/tasks/:id/plan`, `POST /api/tasks/:id/execute`, `POST /api/tasks/:id/cancel`
  - `POST /api/approvals/:id/decide`
  - `POST /api/conflicts/:id/resolve`
  - `POST /api/replication/metrics`
  - `POST /api/propose`, `POST /api/sync`, `POST /api/ask` (when executing plans)
- **Read-Only Endpoints**: Public by default; can be locked down by setting `ALLOW_PUBLIC_READ=false`.
- **CORS Restriction**: Restricted to configured origins (`CORS_ORIGINS`, e.g. `http://localhost:5173`) instead of wildcard `*`.
- **Structured Error Responses**: Consistent error objects (`{ error, code, status, success, errors }`) without leaking raw internal stack traces.

---

## 🌱 Demo Data Seeding Configuration

Demo data generation is completely opt-in for production-grade clean startup:

- **Flag**: `SEED_DEMO_DATA=true` (or pass `{ seedDemoData: true }` to `initDatabase`).
- **Production Default**: `SEED_DEMO_DATA=false`. When disabled, the platform initializes only the schema, registers the real local node, and retains standard backward-compatible demo inventory tables without seeding fake mock nodes, tasks, GPU specs, or approvals.
- **Local Dev / Testing**: Set `SEED_DEMO_DATA=true` to populate sample tasks across all lifecycle stages, sample approvals, and conflict records for dashboard demos.

---

## 🌐 Workflow Replication & Vector Clocks

Every workflow record is a first-class citizen of the Agentic Mesh replication protocol:

- **Canonical Mutation Routing**: Writes to all 10 workflow tables (`mesh_nodes`, `node_capabilities`, `tasks`, `task_runs`, `task_steps`, `artifacts`, `approvals`, `audit_events`, `conflicts`, `replication_metrics`) are routed through `executeMutation`.
- **Audit & Mesh Log**: Every write automatically appends an entry to `_mesh_log` and advances the vector clock.
- **Atomic Remote Commits**: Remote transactions received via GossipSub or sync delta exchange are verified against strict schema constraints before committing inside an atomic SQLite transaction.
- **Automatic Conflict Detection**: Detects and logs concurrent task assignments and schema divergence to the `conflicts` table.
- **Automated Metric Recording**: Replication latency, throughput, and vector clock metadata are automatically logged to `replication_metrics` during actual sync and gossip activity.

---

### Multi-Laptop Setup & Example Configurations

Run each laptop or terminal with its dedicated `.env` configuration:

#### 1. Planner Laptop (`NODE_ROLE=planner`)
Equipped with GPU/CPU for running Ollama inference. Plans operations but does not write to the database:
```env
NODE_NAME=planner-1
NODE_ROLE=planner
P2P_PORT=9003
WS_PORT=3003
DB_PATH=./data/planner-1.db
OLLAMA_HOST=http://127.0.0.1:11434
OLLAMA_MODEL=qwen2.5-coder:1.5b
```

#### 2. Router Laptop (`NODE_ROLE=router`)
Accepts user prompts from CLI, Web dashboard, or REST API. Discovers Planner and Executor peers automatically:
```env
NODE_NAME=router-1
NODE_ROLE=router
P2P_PORT=9001
WS_PORT=3001
DB_PATH=./data/router-1.db
ROUTER_LOCAL_FALLBACK=false
```

#### 3. Executor Laptop (`NODE_ROLE=executor`)
Maintains the canonical SQLite database. Revalidates every operation locally before atomic commit:
```env
NODE_NAME=executor-1
NODE_ROLE=executor
P2P_PORT=9002
WS_PORT=3002
DB_PATH=./data/executor-1.db
```

---

### Multi-Node Test Flow

```text
Router -> Planner -> Executor -> transaction gossip -> peer sync
```

1. **User input at Router**: User runs `ask Add mechanical keyboard` in Router's CLI or calls `POST /api/ask`.
2. **Planning Delegation**: Router locates a connected Planner peer via mDNS role discovery and sends a structured `PLAN_REQUEST`.
3. **AI Planning**: Planner calls local Ollama, decomposes the prompt into structured operations (`INSERT item`), verifies the plan passes `validateModelPlan`, and replies with `PLAN_RESPONSE`.
4. **Execution Delegation**: Router receives the plan and forwards it to a connected Executor peer via `EXECUTION_REQUEST`.
5. **Revalidation & Atomic Commit**: Executor revalidates every operation against local schema constraints (`price > 0`, foreign keys, table allowlists) inside an atomic SQLite transaction.
6. **Transaction Gossip**: Upon commit, Executor broadcasts the transaction envelope to `mesh:transactions` over GossipSub.
7. **Peer Replication**: Router, Planner, and all other mesh peers replicate the committed transaction via vector-clock anti-entropy sync, keeping every node's database state consistent.

## 📋 Prerequisites

- **Node.js**: v20 or v22 LTS (ESM native support).
- **Network**: For multi-laptop testing, devices should be on the same local network / Wi-Fi subnet for mDNS auto-discovery.
- **Ollama (Optional)**: For local AI planning and conversational coding:
  1. Install Ollama from [ollama.com](https://ollama.com).
  2. Pull the default fast coding model: `ollama pull qwen2.5-coder:1.5b`.
  3. Start Ollama: `ollama serve`.
  *Note: If Ollama is offline, the entire database mesh, CLI, REST API, vector clock sync, and dashboard operate normally in fast-path mode.*

---

## 🚀 CPU Performance & Inference Strategy

Running large models (e.g. 7.2 GB Gemma or 4B models) on CPU can cause 100% CPU saturation and long token generation delays. Agentic Mesh employs an ultra-fast **3-tier speed strategy** optimized for CPU hardware:

### 1. Speed Strategy

| Tier | Latency | Engine | Details |
| :--- | :--- | :--- | :--- |
| **Instant** | `<1s` | Pure JS / SQLite | Greetings, DB state inspection, schema DDL, and local templates for common questions (Java even/odd, Hello World in Java/Python/JS, REST API client, WebSocket client, basic SQL explanation) without any LLM call. |
| **Fast AI** | `2–4s` | `qwen2.5-coder:1.5b` | Slim system prompt + user message only (zero DB schema dump, zero mesh log, $\le 2$ prior messages in history). Context: `1024`, output limit: `128` tokens, temperature: `0.2`. |
| **Heavy AI** | Slow / Optional | `gemma4:e2b` | Deep architecture & complex planning. Only loaded when manually selected via the "Deep" mode toggle. **Never load it alongside the fast coding model.** |

### 2. Ollama CPU Optimization Commands

To ensure maximum speed, keep only `qwen2.5-coder:1.5b` loaded in memory and unload any heavy models:

```bash
# 1. Pull the fast CPU-optimized coding model (986 MB)
ollama pull qwen2.5-coder:1.5b

# 2. Stop heavy models to free CPU memory and avoid resource contention
ollama stop gemma4:e2b
ollama stop agentic-mesh-coder

# 3. Verify currently loaded models
ollama ps
```

### 3. Environment Configuration (`.env`)

```env
NODE_NAME=delta
NODE_ROLE=ai-agent
P2P_PORT=9004
WS_PORT=3004
DB_PATH=./data/delta.db
OLLAMA_HOST=http://127.0.0.1:11434
OLLAMA_MODEL=qwen2.5-coder:1.5b
OLLAMA_NUM_CTX=1024
OLLAMA_NUM_PREDICT=128
OLLAMA_TEMPERATURE=0.2
OLLAMA_TIMEOUT_MS=90000
OLLAMA_KEEP_ALIVE=5m
```

## ⚡ Quick Start (Single Node)

1. **Install Dependencies**:
   ```bash
   npm install
   ```

2. **Start Backend Node (Alpha)**:
   ```bash
   npm run dev:node
   ```
   *Alpha starts on P2P port `9001`, WebSocket/API port `3001`, using database `./data/mesh.db`.*

3. **Start Dashboard in a New Terminal**:
   ```bash
   npm run dev:dashboard
   ```
   Open [http://localhost:5173](http://localhost:5173) in your browser.

---

## 🌐 Two-Node Local Demo (Alpha & Bravo)

You can run two nodes side-by-side on the same machine to verify automatic peer discovery and real-time replication.

### Terminal 1 — Node Alpha
```bash
npm run dev:node
```
- **Node Name**: `alpha`
- **P2P Port**: `9001`
- **WS / API Port**: `3001`
- **DB File**: `./data/mesh.db`

### Terminal 2 — Node Bravo
On Windows:
```cmd
cmd.exe /c "set NODE_NAME=bravo&& set P2P_PORT=9002&& set WS_PORT=3002&& set DB_PATH=./data/bravo.db&& node index.js"
```
Or on Linux / macOS / Bash:
```bash
NODE_NAME=bravo P2P_PORT=9002 WS_PORT=3002 DB_PATH=./data/bravo.db node index.js
```
- **Node Name**: `bravo`
- **P2P Port**: `9002`
- **WS / API Port**: `3002`
- **DB File**: `./data/bravo.db`

### Terminal 3 — Dashboard
```bash
npm run dev:dashboard
```
Open [http://localhost:5173](http://localhost:5173). Use the top port selector (`:3001` / `:3002`) to toggle between inspecting Node Alpha and Node Bravo in real time.

---

## 📡 REST API Reference

The node gateway exposes a REST API on port `WS_PORT` (default `3001`):

| Endpoint | Method | Description | Payload / Parameters |
| :--- | :--- | :--- | :--- |
| `/api/health` | `GET` | Node status, uptime, peer count, model name | None |
| `/api/peers` | `GET` | Discovered mesh peers and multiaddrs | None |
| `/api/db/state` | `GET` | Current database snapshot (categories, items, suppliers) | None |
| `/api/db/mesh-log` | `GET` | Monotonic replication log entries | `?limit=N` (integer, clamped 1–200, default 50) |
| `/api/db/schema` | `GET` | Plain text DDL schema from SQLite master | None |
| `/api/propose` | `POST` | Propose and execute a fast-path transaction | `{"table":"items","operation":"INSERT","data":{...}}` |
| `/api/sync` | `POST` | Broadcast anti-entropy sync request to mesh | None |
| `/api/ask` | `POST` | AI operations planner (preview or execute) | `{"prompt":"...","confirm":true\|false}` |
| `/api/ask/confirm` | `POST` | Confirm and execute a planned operation batch | `{"operations":[...]}` |
| `/api/audit` | `POST` | Trigger background 3NF/BCNF AI audit on recent logs | None |

---

## 🧠 AI Planning & Gemma Fine-Tuning (QLoRA)

Agentic Mesh includes a complete fine-tuning pipeline in the [`training/`](file:///c:/Users/Advaith%20J/OneDrive/Desktop/agentic%20mesh/training) directory for training compact models (e.g. **Google Gemma 3 1B**, **Gemma 2B**, or **Gemma 4 E2B**) using **4-bit QLoRA**.

### Key Workflow:
1. **Dataset**: 32+ realistic multi-table 3NF training examples in `training/train.jsonl` and 12 evaluation test cases in `training/eval.jsonl`.
2. **Train**:
   ```bash
   python training/train_qlora.py --model_id google/gemma-3-1b-it --epochs 3
   ```
3. **Evaluate**:
   ```bash
   python training/evaluate.py --offline
   ```
4. **Deploy to Ollama**:
   ```bash
   ollama create agentic-mesh-planner -f training/Modelfile
   ```

See the [Training Guide](file:///c:/Users/Advaith%20J/OneDrive/Desktop/agentic%20mesh/training/README.md) for full instructions.

---

## 🧪 Automated Testing

Run the automated test suite verifying schema validation, mutation atomicity, vector clocks, transaction deduplication, and sync delta selection:

```bash
# Run unit and integration tests (36 passing tests)
npm test

# Run the end-to-end two-node replication smoke test
npm run demo:smoke

# Run linting (0 warnings, 0 errors)
npm run lint

# Build dashboard production bundle
npm run build:dashboard
```

---

## ⚙️ Environment Variables Reference

| Variable | Default | Description |
| :--- | :--- | :--- |
| `NODE_NAME` | `node-<random>` | Display name and originator tag for this mesh node |
| `NODE_ROLE` | `peer` | Node role: `peer`, `router`, `planner`, `executor`, `validator` |
| `P2P_PORT` | `9004` | TCP port for libp2p P2P transport |
| `WS_PORT` | `3004` | HTTP and WebSocket port for dashboard and REST API |
| `DB_PATH` | `./data/mesh.db` | File path for SQLite database |
| `ROUTER_LOCAL_FALLBACK` | `false` | When true on a router, allows local planning/execution if peers missing |
| `PLAN_TIMEOUT_MS` | `45000` | Timeout in ms for awaiting remote PLAN_RESPONSE |
| `EXECUTION_TIMEOUT_MS` | `45000` | Timeout in ms for awaiting remote EXECUTION_RESPONSE |
| `OLLAMA_HOST` | `http://127.0.0.1:11434` | Endpoint for Ollama LLM service |
| `OLLAMA_MODEL` | `qwen2.5-coder:1.5b` | Ollama model name used for planning/auditing |
| `MAX_RETRIES` | `3` | Retry attempts for network operations |

---

## 📐 Synchronization Semantics

Agentic Mesh maintains causal consistency across autonomous peers using:
- **Vector-Clock Causal Tracking**: Each node increments its own sequence clock on local writes.
- **Append-Only Replicated Mesh Log**: All mutating operations are logged in SQLite table `_mesh_log` with unique transaction IDs, timestamps, and vector clock snapshots.
- **Deterministic Log Replay**: During catch-up sync, missing log entries are replayed sequentially in creation order (`ORDER BY rowid ASC`).
- **Deduplication & Constraint Enforcement**: Transactions are identified by a unique ID. Duplicates are skipped. Remote writes are checked against local schema constraints before committing inside an atomic transaction. Incompatible writes are rejected, guaranteeing local database integrity.
