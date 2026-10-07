# Agentic Mesh Project Guide

## 1. What this project is

Agentic Mesh is a local-first, peer-to-peer database application. Each laptop runs a Node.js process with a SQLite database and can discover nearby peers, exchange validated transactions, and repair missed changes after a disconnect. A React dashboard and REST/WebSocket gateway provide the user interface.

The project can run as a self-contained `peer` or split responsibilities between dedicated Router, Planner, Executor, and Validator laptops.

## 2. Core concepts

### Laptop node

A laptop node is one running instance of Agentic Mesh. It has its own libp2p identity, SQLite database, WebSocket/REST gateway, and local configuration. Nodes discover one another over the LAN and communicate through encrypted libp2p connections.

### Logical roles

Roles are process-level specializations selected with `NODE_ROLE`. They are not separate applications.

| Role | Responsibility | May write database data? | Uses Ollama? |
| --- | --- | --- | --- |
| `peer` | Default all-in-one node for local development and standalone operation. | Yes | Yes, where a feature needs it |
| `router` | Receives a user request and delegates planning and execution to mesh peers. | No, except optional local fallback | No |
| `planner` | Turns natural-language intent into structured database operations and validates the model output. | No | Yes |
| `executor` | Revalidates remote operations, commits safe writes atomically, and gossips committed transactions. | Yes | No |
| `validator` | Performs AI-assisted audits of plans and recent mesh-log entries. | No | Yes |

`ai-agent` remains accepted as a legacy alias for `peer`.

### Safety boundary

The model never receives raw SQL authority. It produces an array of operation objects such as:

```json
[
  {
    "operation": "INSERT",
    "table": "items",
    "data": {
      "category_id": 1,
      "name": "Mechanical keyboard",
      "price": 99.99,
      "sku": "SKU-KEY-001"
    }
  }
]
```

The executor revalidates every remote plan locally before it can be written. Allowed operations are `INSERT`, `UPDATE`, and `DELETE`; allowed tables are `categories`, `items`, `suppliers`, and `item_suppliers`.

## 3. Architecture

```text
Browser / CLI / REST client
            |
            v
       Router node
            | PLAN_REQUEST / PLAN_RESPONSE on mesh:control
            v
       Planner node (Ollama)
            | validated operation array
            v
       Executor node
            | local validation + atomic SQLite commit
            v
  mesh:transactions GossipSub topic
            |
            v
    Other laptop nodes apply the transaction
```

Every node also subscribes to `mesh:sync`. On peer connection, nodes exchange vector clocks, then send any missing mesh-log entries. Duplicate transactions are ignored and remote transactions go through the local schema checks before being applied.

## 4. Repository map

| Path | Purpose |
| --- | --- |
| `index.js` | Application bootstrap: database, P2P, sync, roles, gateway, CLI, shutdown. |
| `src/agents/` | Router, Planner, Executor, Validator, Ollama client, coordinator, proposals, activity. |
| `src/db/` | SQLite schema, mutation service, strict validator, vector-clock sync. |
| `src/p2p/` | libp2p node, discovery/peer registry, GossipSub message transport. |
| `src/mesh/` | Planner-to-executor handoff workflow. |
| `src/websocket/server.js` | Express REST API, WebSocket server, and SSE chat streaming. |
| `src/api/routes.js` | Interactive terminal CLI. |
| `src/AgenticMeshDashboard.jsx` | React dashboard. |
| `test/` | Node test suites for API, role flow, mutation rules, sync, and vector clocks. |
| `training/` | Optional Ollama fine-tuning/evaluation assets. |
| `docs/` | Deployment and performance documentation. |

## 5. Prerequisites

- Node.js 20 or 22 LTS
- npm
- A local network for automatic multi-laptop discovery
- Ollama only on Planner or Validator nodes, or on a standalone `peer` using AI features
- On Windows, C++ build tools may be needed if `better-sqlite3` has to compile locally

Install project dependencies:

```powershell
npm install
```

For the small planning model:

```powershell
ollama pull qwen2.5-coder:1.5b
ollama serve
```

## 6. Configuration reference

Create `.env` in the project root. Environment variables override defaults.

| Variable | Default | Meaning |
| --- | --- | --- |
| `NODE_NAME` | random ID | Human-friendly node name. |
| `NODE_ROLE` | `peer` | `peer`, `router`, `planner`, `executor`, or `validator`. |
| `P2P_PORT` | `9004` | libp2p TCP port. Must be unique per local process. |
| `WS_PORT` | `3004` | REST and WebSocket port. Must be unique per local process. |
| `DB_PATH` | `./data/mesh.db` | SQLite database file. Must be unique per local process. |
| `OLLAMA_HOST` | `http://127.0.0.1:11434` | Ollama server URL. |
| `OLLAMA_MODEL` | `qwen2.5-coder:1.5b` | Planning/chat model. |
| `OLLAMA_NUM_CTX` | `2048` | Ollama context window. |
| `OLLAMA_NUM_PREDICT` | `512` | Maximum model output tokens. |
| `OLLAMA_TEMPERATURE` | `0.2` | Lower values make plans more repeatable. |
| `OLLAMA_TIMEOUT_MS` | `90000` | AI request timeout in milliseconds. |
| `ROUTER_LOCAL_FALLBACK` | `false` | Lets a Router plan/execute locally when specialist peers are unavailable. |
| `PLAN_TIMEOUT_MS` | `45000` | Router wait time for a Planner response. |
| `EXECUTION_TIMEOUT_MS` | `45000` | Router wait time for an Executor response. |
| `BOOTSTRAP_PEERS` | empty | Comma-separated libp2p multiaddrs for networks where mDNS is unavailable. |
| `VISION_ENABLED` | `false` | Enables the optional visual observation lane. |
| `VISION_MODEL` | `gemma3:4b` | Ollama vision model used when the visual lane is enabled. |

## 7. Run a single node

Use this first to confirm the project works on one laptop:

```env
NODE_NAME=alpha
NODE_ROLE=peer
P2P_PORT=9001
WS_PORT=3001
DB_PATH=./data/alpha.db
OLLAMA_MODEL=qwen2.5-coder:1.5b
```

```powershell
npm run dev:node
```

In a second terminal, start the dashboard:

```powershell
npm run dev:dashboard
```

Open `http://localhost:5173`. The backend health endpoint is `http://localhost:3001/api/health`.

## 8. Run a dedicated Router, Planner, and Executor mesh

Put all three laptops on the same Wi-Fi/LAN. Copy or clone the same repository to each. Install dependencies on every laptop, but install Ollama only where the role needs it.

### Router laptop

```env
NODE_NAME=router-1
NODE_ROLE=router
P2P_PORT=9001
WS_PORT=3001
DB_PATH=./data/router-1.db
ROUTER_LOCAL_FALLBACK=false
```

### Planner laptop

```env
NODE_NAME=planner-1
NODE_ROLE=planner
P2P_PORT=9002
WS_PORT=3002
DB_PATH=./data/planner-1.db
OLLAMA_HOST=http://127.0.0.1:11434
OLLAMA_MODEL=qwen2.5-coder:1.5b
OLLAMA_TEMPERATURE=0.1
```

Start `ollama serve` on the Planner laptop, then run `npm run dev:node`.

### Executor laptop

```env
NODE_NAME=executor-1
NODE_ROLE=executor
P2P_PORT=9003
WS_PORT=3003
DB_PATH=./data/executor-1.db
```

Start each node with:

```powershell
npm run dev:node
```

Use `peers` in each CLI or `GET /api/peers` to check that the peer name and role are visible. If mDNS is blocked by a firewall or router, use the CLI `connect <ip:port>` command or set `BOOTSTRAP_PEERS` to the peer multiaddr.

### Verify the end-to-end path

On the Router node, submit a natural-language request:

```text
ask Add a Mechanical Keyboard item in category 1 for 99.99 with SKU SKU-KEY-001
```

Expected sequence:

1. Router sends `PLAN_REQUEST` to the Planner.
2. Planner asks its local Ollama model for structured operations and validates the returned plan.
3. Router sends `EXECUTION_REQUEST` to the Executor.
4. Executor validates again, commits the batch to SQLite, and publishes transaction envelopes.
5. Connected peers receive or later synchronize the committed records.

## 9. Messages and replication

| Topic | Message types | Purpose |
| --- | --- | --- |
| `mesh:transactions` | `TRANSACTION` | Broadcast committed data mutations. |
| `mesh:sync` | `SYNC_REQUEST`, `SYNC_RESPONSE` | Reconcile missed mesh-log entries using vector clocks. |
| `mesh:control` | `PEER_ANNOUNCE`, `PLAN_REQUEST`, `PLAN_RESPONSE`, `EXECUTION_REQUEST`, `EXECUTION_RESPONSE` | Announce roles and coordinate specialized nodes. |

Control messages contain a request ID and target peer ID. Pending request maps are cleaned on success or timeout, and duplicate request IDs are ignored. Network data is always validated again at the receiving authority.

## 10. Database and transaction guarantees

The local SQLite schema contains `categories`, `items`, `suppliers`, `item_suppliers`, and `_mesh_log`.

- SQLite runs with WAL and foreign keys enabled.
- Mutations use a strict allowlist for tables, columns, and operations.
- Required fields, JS types, positive price constraints, unique fields, and foreign keys are checked before writes.
- A batch uses an atomic transaction: failure rejects the batch rather than applying only some operations.
- Each committed mutation is journaled in `_mesh_log` with its originating peer and vector clock.

The validator role is an advisory AI audit. Deterministic schema validation in the mutation path is the enforcement mechanism.

## 11. User interfaces

### CLI

| Command | Purpose |
| --- | --- |
| `propose <json>` | Submit one structured, fast-path transaction. |
| `ask <text>` | Run the natural-language planning flow. |
| `bad` | Send a deliberately invalid payload to test rejection. |
| `peers` | List discovered peers and roles. |
| `connect <ip:port>` | Manually dial a peer. |
| `db` | Display local categories and items. |
| `sync` | Broadcast a sync request. |
| `audit` | Run the recent-transaction audit. |
| `status` | Show node identity, addresses, peers, and item count. |
| `exit` | Close the node gracefully. |

### REST API

The REST server runs on `WS_PORT` even though the port also hosts WebSocket connections.

| Endpoint | Method | Description |
| --- | --- | --- |
| `/api/health` | GET | Node, role, ports, model, peers, and uptime. |
| `/api/agent/status` | GET | Ollama reachability and configured/active models. |
| `/api/peers` | GET | Current peer registry, including roles. |
| `/api/db/state` | GET | Local categories, items, and suppliers. |
| `/api/db/mesh-log?limit=50` | GET | Recent replicated transaction log. |
| `/api/db/schema` | GET | Local schema information. |
| `/api/propose` | POST | Execute a single structured transaction. |
| `/api/sync` | POST | Request mesh synchronization. |
| `/api/ask` | POST | Create an AI plan; use `confirm` to control execution. |
| `/api/ask/confirm` | POST | Execute supplied structured operations. |
| `/api/audit` | POST | Audit recent transactions. |
| `/api/agent/chat` | POST | Non-streaming chat or SSE with `Accept: text/event-stream`. |
| `/api/agent/chat/stream` | POST | Streaming agent response. |
| `/api/agents/activity` | GET | Recent agent activity. |
| `/api/vision/look` | POST | Create an optional visual observation. |
| `/api/vision/see` | POST | Inspect and plan from visual input. |
| `/api/vision/observations` | GET | List visual observations. |
| `/api/proposals` | GET | List pending and completed proposals. |
| `/api/proposals/:id/approve` | POST | Approve a proposal. |
| `/api/proposals/:id/reject` | POST | Reject a proposal. |

Example plan preview:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:3001/api/ask `
  -ContentType 'application/json' `
  -Body '{"prompt":"Add a category named Peripherals","confirm":false}'
```

### Dashboard and WebSocket

The dashboard is a Vite/React app. It shows peer topology, data, transaction events, agent activity, and AI interactions. WebSocket events include peer lifecycle events, transaction commits/replication, sync completion, audit completion, and agent activity.

## 12. Optional visual lane and proposals

When `VISION_ENABLED=true`, the gateway can submit image observations to a local Ollama vision model. The resulting operations are validated and stored as proposals. A proposal is not committed until explicitly approved. Proposal commits validate again because the database may have changed while the proposal was waiting.

This is intentionally separate from normal fast-path transactions.

## 13. Docker

The repository includes a Node 22 image and a Compose setup that starts Ollama plus four example nodes. Start it with:

```powershell
npm run docker:up
```

Stop it with:

```powershell
npm run docker:down
```

The Compose file is intended for local demonstration. It uses named volumes for SQLite and Ollama data. Review and adjust roles, model names, ports, and peer-discovery expectations before using it outside a development network.

## 14. Tests and quality checks

Run the full automated suite:

```powershell
npm test
```

Run the smoke demo:

```powershell
npm run demo:smoke
```

Lint the repository:

```powershell
npm run lint
```

Build the dashboard:

```powershell
npm run build:dashboard
```

The test suite covers API behavior, role configuration and routing, Planner and Executor separation, mutation validation, atomicity, peer/sync helpers, and vector clocks.

## 15. Troubleshooting

| Symptom | Checks |
| --- | --- |
| Planner is not found | Confirm `NODE_ROLE=planner`, verify peers show role metadata, check the firewall, and verify all laptops share a LAN. |
| Ollama planning fails | Run `ollama list`, verify the configured model exists, start `ollama serve`, and check `/api/agent/status`. |
| Peers do not discover each other | Ensure mDNS is permitted on the network. Use `connect <ip:port>` or `BOOTSTRAP_PEERS` when discovery is unreliable. |
| Port already in use | Give each local node a different `P2P_PORT`, `WS_PORT`, and `DB_PATH`. |
| A plan is rejected | Review the returned schema error. The operation may have an invalid table/field, missing required data, a duplicate unique field, bad foreign key, or non-positive price. |
| Router waits until timeout | Confirm a connected Planner/Executor peer exists and its role is shown correctly. Check `PLAN_TIMEOUT_MS` and `EXECUTION_TIMEOUT_MS` only after resolving connectivity. |
| Data differs after reconnecting | Run `sync` and inspect the mesh log. Sync is designed to exchange entries missing from each peer's vector clock. |

## 16. Operating guidance

- Use `peer` for demos and local development.
- Put Ollama on the Planner laptop if you are splitting roles; it concentrates AI workload where it belongs.
- Keep an Executor database on persistent storage and back up the `data/` directory before experimentation.
- Treat the current LAN mesh configuration as a trusted local deployment. libp2p transport encryption does not by itself provide an authorization policy for untrusted participants.
- Use a smaller model and low temperature for repeatable planner output. `qwen2.5-coder:1.5b` is the intended lightweight default.

