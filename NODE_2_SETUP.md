# Agentic Mesh — Node 2 (Laptop B) Setup & Run Instructions

This guide provides step-by-step instructions for booting **Node 2 (Bravo)** on a second laptop or terminal session to establish a true zero-cloud peer-to-peer (P2P) database mesh with **Node 1 (Alpha)**.

---

## 📋 Prerequisites

1. **Node.js**: v20 or v22 LTS installed.
2. **Same Local Network**: Both laptops must be connected to the same Wi-Fi or LAN for automatic mDNS peer discovery.
3. **Ollama (Optional for AI path)**: Downloaded from [ollama.com](https://ollama.com) with model `gemma3:4b` (`ollama pull gemma3:4b`). If Ollama is not installed, fast-path schema validation and P2P sync will still work perfectly.

---

## 🚀 Step 1: Project Setup on Laptop B

1. **Copy or Clone Project Repository**:
   Copy the `agentic-mesh` folder to Laptop B.

2. **Open Terminal in Project Root**:
   ```bash
   cd agentic-mesh
   ```

3. **Install Dependencies**:
   ```bash
   npm install
   ```

---

## ⚡ Step 2: Boot Node 2 (Bravo)

Run the following command in terminal:

### On Windows (Command Prompt / PowerShell)
```cmd
npx cross-env NODE_NAME=bravo P2P_PORT=9002 WS_PORT=3002 DB_PATH=./data/bravo.db node index.js
```

### On macOS / Linux
```bash
NODE_NAME=bravo P2P_PORT=9002 WS_PORT=3002 DB_PATH=./data/bravo.db node index.js
```

---

## 🌐 Step 3: Verify Peer Connection

Upon booting, Node 2 will output:
```text
[INFO]   AGENTIC MESH — Autonomous P2P Database Node 
[INFO] Node Name: bravo
[INFO] P2P Port:  9002
[INFO] WS Port:   3002
[INFO] DB Path:   ./data/bravo.db
...
[P2P] Listening on /ip4/192.168.x.x/tcp/9002/p2p/12D3KooW...
[SUCCESS] All systems online. Node is ready.
[bravo] > 
```

1. **Check Discovered Peers**:
   In the `[bravo] >` CLI prompt, type:
   ```text
   peers
   ```
   *You should see Node 1 (Alpha) listed in the peer registry table.*

2. **Check Node Status**:
   ```text
   status
   ```

---

## 🧪 Step 4: Live P2P Mesh Test Scenarios

### Test 1: Fast-Path Transaction Replication
On **Node 2 (Bravo)**, propose a fast-path transaction:
```text
propose {"table":"items","operation":"INSERT","data":{"category_id":1,"name":"Wireless Headset","price":129.99,"sku":"SKU-H001"}}
```
- **Result**: Bravo commits the transaction locally in `<1ms`, records it in its vector clock ledger, and gossips it to Node 1 (Alpha).
- **Verification on Node 1**: Check Node 1's CLI log or run `db` on Node 1 to see the replicated `Wireless Headset`.

### Test 2: Verify Schema Validation (Sub-ms Rejection)
On **Node 2 (Bravo)**, trigger a malformed payload test:
```text
bad
```
- **Result**: Intercepted locally before hitting database or P2P network. Rejection details displayed:
  - Price constraint failure (`price > 0`)
  - Foreign key violation (`category_id: 999`)

### Test 3: Natural Language AI Planning
If Ollama is running on Laptop B:
```text
ask Add a new category named Gaming and add a mechanical keyboard for 89.99
```
- **Result**: AI Router passes request to Planner agent -> Decomposes into `INSERT categories` + `INSERT items` -> Validates -> Executes -> Replicates across mesh.

### Test 4: Offline Reconnection & CRDT Sync
1. Stop Node 2 (`exit` or `Ctrl+C`).
2. Make writes on Node 1 (Alpha).
3. Restart Node 2 (Bravo).
4. Run `sync` on Node 2 — Vector Clock engine exchanges state delta and replays missing transactions.

---

## 📊 Step 5: (Optional) Open Web Dashboard on Laptop B

Start the visual React dashboard:
```bash
npm run dev:dashboard
```
Open `http://localhost:5173` to see real-time peer topology and live database updates!
