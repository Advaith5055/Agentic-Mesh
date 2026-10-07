# Connecting a Second Laptop as an Executor Node

This guide describes the intended two-laptop Agentic Mesh deployment: the existing laptop runs a **Planner** node and the second laptop runs an **Executor** node. The Planner turns a natural-language request into a validated list of safe operations. The Executor is the only node that may commit those operations, then it replicates the resulting transaction through the mesh.

> **Important current-runtime note**
>
> The repository has role configuration, role-aware API protection, and an Executor implementation, but the current `index.js` boot path is still Planner-centric: its `onExecutionRequest` handler always logs that it is ignoring an execution request. Setting `NODE_ROLE=executor` on the second laptop therefore advertises an Executor role but does **not yet** make the process handle a Planner's `EXECUTION_REQUEST`. Complete the role-dispatcher work described in [Required Runtime Completion](#required-runtime-completion) before relying on this setup for real writes.

## 1. Architecture

```text
Laptop A: Planner (delta)                                  Laptop B: Executor (alpha)
┌──────────────────────────────────┐                      ┌──────────────────────────────────┐
│ React dashboard / CLI             │                      │ SQLite database + WAL             │
│ Ollama planner                    │                      │ Schema validator                  │
│ Validates an operation preview    │                      │ Atomic mutation service            │
│                                  │                      │ Vector clock + mesh log            │
│ Prompt → structured operation plan│                      │ Proposal approval and commit       │
└──────────────┬───────────────────┘                      └────────────────┬─────────────────┘
               │  GossipSub mesh:control                                  │
               │  EXECUTION_REQUEST (no raw SQL)                          │
               ├─────────────────────────────────────────────────────────►│
               │                                                          │
               │  EXECUTION_RESULT (committed / pending / rejected)       │
               ◄─────────────────────────────────────────────────────────┤
               │                                                          │
               │  GossipSub mesh:transactions                             │
               ◄──────────────────── committed transaction ───────────────┤
               │                                                          │
               └──── vector-clock anti-entropy sync ──────────────────────┘
```

### Responsibilities

| Component | Planner laptop | Executor laptop |
|---|---:|---:|
| Run Ollama and create plans | Yes | Not required |
| Accept a natural-language change request | Yes | Optional |
| Create raw SQL | Never | Never |
| Validate operation allowlists and constraints | Preview | Mandatory before commit |
| Approve/reject a proposal | Via dashboard/API | Stores and applies the decision |
| Write inventory/workflow records | No | Yes |
| Add a committed transaction to `_mesh_log` | No | Yes |
| Broadcast committed transactions | No | Yes |
| Replicate remote committed transactions | Yes | Yes |

The executor must never treat a plan as trusted merely because it came from the planner. It must re-run validation and use the canonical mutation service.

## 2. What Must Be Installed on Both Laptops

1. Same project revision on both laptops.
2. Node.js 20 or 22 LTS.
3. `npm install` completed inside the project folder.
4. Both devices connected to the same private LAN/Wi-Fi.
5. Windows Firewall permits inbound TCP for the P2P ports used below. Permit inbound UDP 5353 as well if using automatic mDNS discovery.

Only the Planner laptop needs Ollama for normal planning. The Executor can run without Ollama because it validates and writes deterministic structured operations.

## 3. Choose Stable Node Identities and Ports

Use different names, P2P ports, web/API ports, and database files. The sample below assumes:

| Node | Role | LAN IP example | P2P TCP | Dashboard/API/WS | Database |
|---|---|---|---:|---:|---|
| `delta` | `planner` | `192.168.1.6` | `9004` | `3004` | `data/delta.db` |
| `alpha` | `executor` | `192.168.1.20` | `9002` | `3002` | `data/alpha.db` |

Replace the example IP addresses with the actual LAN addresses. Do not use `localhost` or `127.0.0.1` to connect the two laptops; those addresses always point to the same laptop.

## 4. Planner Laptop Configuration

On Laptop A, create or update `.env` in the project root:

```env
NODE_NAME=delta
NODE_ROLE=planner
P2P_PORT=9004
WS_PORT=3004
DB_PATH=./data/delta.db

# Ollama is normally needed on the Planner only.
OLLAMA_HOST=http://127.0.0.1:11434
OLLAMA_MODEL=qwen2.5-coder:1.5b

# Prefer the executor by its advertised NODE_NAME.
EXECUTOR_PEER=alpha

# Use the exact same strong value on both laptops.
MESH_API_KEY=replace-this-with-a-long-random-secret

# Dashboard origins that are allowed to call this node's API.
CORS_ORIGINS=http://localhost:5173,http://127.0.0.1:5173
# Keep read-only dashboard views usable until browser token handling is added.
ALLOW_PUBLIC_READ=true

# Do not create fictional dashboard nodes/tasks on real laptops.
SEED_DEMO_DATA=false
```

Start it from PowerShell:

```powershell
npm install
npm run dev:node
```

It should print its peer ID and one or more listening multiaddrs. Save the LAN multiaddr, not the `127.0.0.1` address.

## 5. Executor Laptop Configuration

Copy the project to Laptop B. In its project root, create `.env`:

```env
NODE_NAME=alpha
NODE_ROLE=executor
P2P_PORT=9002
WS_PORT=3002
DB_PATH=./data/alpha.db

# Executor does not need an LLM for safe execution.
# It can remain configured, but no local Ollama process is required.
OLLAMA_HOST=http://127.0.0.1:11434
OLLAMA_MODEL=qwen2.5-coder:1.5b

MESH_API_KEY=replace-this-with-the-exact-same-long-random-secret
CORS_ORIGINS=http://192.168.1.6:5173,http://localhost:5173
ALLOW_PUBLIC_READ=true
SEED_DEMO_DATA=false
```

Install and start:

```powershell
npm install
npm run dev:node
```

Expected startup facts:

- It logs `Node Name: alpha` and `Node Role: executor`.
- It listens on TCP 9002 and exposes API/WebSocket on port 3002.
- It creates its own `data/alpha.db`; never point both laptops at the same SQLite file or shared OneDrive database path.

## 6. Connect the Laptops

### Option A — Automatic LAN discovery (recommended first)

Start both nodes while they are on the same LAN. The libp2p mDNS service discovers peers, connects them, and role presence advertises `delta` as Planner and `alpha` as Executor.

On each CLI, run:

```text
peers
```

Or query from either machine. If you later set `ALLOW_PUBLIC_READ=false`, add the `x-api-key` header to every read request too:

```powershell
Invoke-RestMethod http://localhost:3004/api/peers
Invoke-RestMethod http://localhost:3002/api/peers
```

You should see the remote peer with status `connected` and the correct role.

### Option B — Fixed bootstrap connection

Use this when mDNS is blocked by a guest Wi-Fi, firewall, VLAN, or corporate network.

1. Start the executor first.
2. Copy its logged LAN multiaddr. It has this shape:

   ```text
   /ip4/192.168.1.20/tcp/9002/p2p/12D3KooW...
   ```

3. Add it to the Planner `.env` and restart the Planner:

   ```env
   BOOTSTRAP_PEERS=/ip4/192.168.1.20/tcp/9002/p2p/12D3KooW...
   ```

4. Confirm the Planner logs `Connected to bootstrap peer` and that `peers` shows `alpha` as connected.

The connection manager retries bootstrap dialing every 15 seconds. If the executor gets a new peer identity after restart, replace the stored multiaddr with its new logged address.

## 7. Connectivity and Security Checklist

Open only what the mesh needs on the private LAN:

| Port/protocol | Direction | Purpose |
|---|---|---|
| TCP 9004 | inbound to Planner | libp2p P2P traffic |
| TCP 9002 | inbound to Executor | libp2p P2P traffic |
| UDP 5353 | LAN multicast | mDNS discovery, optional if bootstrapping |
| TCP 3004 | local or trusted browser | Planner dashboard, REST, WebSocket |
| TCP 3002 | local or trusted browser | Executor dashboard, REST, WebSocket |

Do not expose P2P or API ports directly to the public Internet. Use a VPN or private network if the laptops are not on the same LAN.

All protected write APIs require the shared `MESH_API_KEY`. For example, an authenticated health-independent write uses:

```powershell
$headers = @{ "x-api-key" = "replace-this-with-a-long-random-secret" }
Invoke-RestMethod -Method Post -Headers $headers -ContentType "application/json" `
  -Uri "http://localhost:3002/api/propose" `
  -Body '{"table":"categories","operation":"INSERT","data":{"name":"Mesh Demo","description":"Committed by executor alpha"}}'
```

For dashboard operation, configure the dashboard to connect to the Planner gateway at `http://<planner-ip>:3004` / `ws://<planner-ip>:3004/ws`. The Planner is the user-facing place to submit natural-language requests; the Executor is the trusted commit point.

> **Current dashboard authentication caveat:** the React dashboard does not yet attach an API key to its protected POST requests. It can display public read-only data when `ALLOW_PUBLIC_READ=true`, but its task-creation, approval, chat, and mutation controls receive `401` until the frontend adds a secure token-entry/session mechanism and sends `x-api-key` on those requests. For now, use authenticated REST/PowerShell calls for mutations. Do not put a production API key into a Vite client bundle.

## 8. End-to-End Task Flow

Once the executor role handler is enabled, test with a safe request such as:

```text
Add a category named Executor Mesh Test with description "created through Planner to Executor hand-off".
```

The expected sequence is:

1. Dashboard/CLI sends the request to `delta`.
2. Delta uses Ollama to create structured `INSERT` operations; it does not execute them.
3. Delta validates the plan and sends `EXECUTION_REQUEST` to alpha on `mesh:control`.
4. Alpha verifies `targetPeerId`, validates the plan again, and creates a proposal if approval is required.
5. After approval (when applicable), alpha commits with `executeMutationBatch`.
6. Alpha writes the transaction to `_mesh_log`, advances its vector clock, broadcasts it on `mesh:transactions`, and returns `EXECUTION_RESULT`.
7. Delta receives the committed transaction and updates its local replica/dashboard.

Verify both databases after completion:

```powershell
# Planner's replica
Invoke-RestMethod http://localhost:3004/api/db/state

# Executor's authoritative local state
Invoke-RestMethod http://localhost:3002/api/db/state
```

## 9. Required Runtime Completion

Before the second laptop can actually execute Planner hand-offs, make the startup path role-aware. The current `index.js` contains a Planner-specific `onExecutionRequest` handler that ignores every request. Replace that single-role behavior with a dispatcher driven by `config.NODE_ROLE`:

| Role | Must handle | Must reject/avoid |
|---|---|---|
| `planner` | `PLAN_REQUEST`, local planning, `EXECUTION_RESULT` | `EXECUTION_REQUEST` commits |
| `executor` | targeted `EXECUTION_REQUEST`, approval/proposal lifecycle, commit, `EXECUTION_RESULT` | local AI plan generation and planner-only operations |
| `router` | user request routing, `PLAN_RESPONSE`, `EXECUTION_RESPONSE` | direct mutation |
| `validator` | task-run validation/audits | planning and commit |
| `peer` | all allowed handlers for a local single-node demo | none, subject to policy |

For the Executor handler specifically:

1. Ignore a request whose `targetPeerId` is not the executor's peer ID.
2. Require the sender to be a connected/known planner peer.
3. Validate each operation with `validateMutationBatch`.
4. Create a proposal for human approval where policy requires it; otherwise commit with `executeMutationBatch`.
5. Broadcast committed mutation envelopes using `publishTransaction`.
6. Publish a targeted `EXECUTION_RESULT` to `mesh:control` with the original `requestId` and Planner peer ID.
7. Never run raw SQL or blindly trust a Planner-supplied operation.

Do not merely change `NODE_ROLE` in `.env` and assume the current process becomes a real executor. Implement and test this dispatcher first.

## 10. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `peers` shows no remote node | mDNS blocked or firewall | Permit UDP 5353 / P2P TCP ports, or use `BOOTSTRAP_PEERS`. |
| Bootstrap keeps retrying | Wrong IP, port, or peer ID | Copy the exact current LAN multiaddr from executor startup output. |
| Planner says no executor is connected | Role presence missing or stale | Confirm alpha runs with `NODE_ROLE=executor`, then inspect `GET /api/peers`. |
| Planner sends request but it is ignored | Current planner-centric entry point | Implement the executor role dispatcher in Section 9. |
| `401 Unauthorized` from API | Token missing/mismatched | Use the same `MESH_API_KEY` on both nodes and send it as `x-api-key` or Bearer token. |
| No replication | Nodes are not connected, or transaction was not committed through the canonical mutation service | Check peer status, `_mesh_log`, and executor logs. |
| Port in use | Another local process owns it | Choose unused P2P and gateway ports, then restart. |

## 11. Final Acceptance Checklist

- [ ] Planner laptop reports `NODE_ROLE=planner`.
- [ ] Executor laptop reports `NODE_ROLE=executor`.
- [ ] Each laptop has a unique database path and ports.
- [ ] Both nodes use the same strong `MESH_API_KEY`.
- [ ] `peers` shows the other node as `connected` with the correct role.
- [ ] The Executor handler processes a targeted `EXECUTION_REQUEST`.
- [ ] An approved test request commits only on the Executor.
- [ ] The committed transaction appears in both nodes' `_mesh_log`/database views after replication.
- [ ] Planner cannot commit directly; executor revalidates every incoming operation.
