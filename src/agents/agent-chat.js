/**
 * @fileoverview Agent Communication Service
 * Handles conversational Q&A and interactive tool planning for Agentic Mesh.
 * 
 * Performance tiers:
 *   Tier 1 — Instant: greetings, identity, and data queries answered without model call.
 *   Tier 2 — Streaming: general questions streamed token-by-token with slim prompt.
 *   Tier 3 — Buffered: mutation requests use full schema prompt, buffered and parsed.
 * 
 * @module agents/agent-chat
 */

import { askModel, askModelStream, getAgentStatus } from './ollama.js';
import { getSchema, getDbState } from '../db/sqlite.js';
import { validateModelPlan } from './model-validator.js';
import { config } from '../utils/config.js';

// ── Cached Model Availability ────────────────────────────────────────────────
// Avoids a full model-list round-trip on every chat message.
let _modelCache = { status: null, checkedAt: 0 };
const MODEL_CACHE_TTL_MS = 30_000;

async function cachedModelStatus() {
  const now = Date.now();
  if (now - _modelCache.checkedAt < MODEL_CACHE_TTL_MS && _modelCache.status) {
    return _modelCache.status;
  }
  const status = await getAgentStatus();
  _modelCache = { status, checkedAt: now };
  return status;
}

export function clearModelStatusCache() {
  _modelCache = { status: null, checkedAt: 0 };
}

// ── Fast-Path Pattern Matchers ───────────────────────────────────────────────
const GREETING_RE = /^(hi|hello|hey|howdy|greetings|good\s*(morning|afternoon|evening)|what'?s?\s*up|sup|yo|hola|namaste|bonjour)[\s!?.,]*$/i;
const IDENTITY_RE = /^(who|what)\s+(are\s+you|is\s+(this|your\s+(name|purpose))|can\s+you\s+do|do\s+you\s+do)/i;
const HELP_RE = /^(help|\?|commands|options|usage|what\s+can\s+i\s+ask)[\s!?.,]*$/i;
const STATUS_RE = /^(status|health|node\s+status|mesh\s+status|system\s+status|info)[\s!?.,]*$/i;
const SCHEMA_DDL_RE = /\b(show\s+schema|schema|ddl|table\s+definitions?|sql\s+schema)\b/i;
const CODE_WS_RE = /\b(websocket|ws\s+client|connect\s+to\s+ws|ws\s+api|how\s+to\s+connect)\b/i;
const CODE_REST_RE = /\b(rest\s+api|api\s+examples?|endpoints?|curl\s+example|http\s+api|rest\s+client)\b/i;
const SYNC_EXPLAIN_RE = /\b(how\s+does\s+(sync|replication|vector\s+clock)\s+work|explain\s+(sync|vector\s+clock|replication))\b/i;
const JAVA_EVEN_ODD_RE = /\b(even\s*\/?\s*odd|odd\s*\/?\s*even)\b/i;
const HELLO_WORLD_RE = /\bhello\s+world\b/i;
const SQL_EXPLAIN_RE = /\b(basic\s+sql|explain\s+sql|what\s+is\s+sql|sql\s+basics|sql\s+explanation|sql\s+tutorial)\b/i;
const DATA_QUERY_RE = /\b(show\s+(database\s+)?items?|what\s+items?|how\s+many|what\s+(items?|products?|categories|suppliers?)|list\s+(all|the|items?|categories|suppliers?)|show\s+(me\s+)?(all|the|every|items?|data|categories|suppliers?)|what('?s| is| are)\s+(in\s+(the\s+)?(db|database|stock|inventory))|count|total)\b/i;
const MUTATION_RE = /\b(add|create|insert|update|change|delete|remove|modify|set|rename|new\s+\w+\s+(for|at|priced))\b/i;

/**
 * Attempts an instant response without calling the model.
 * @param {string} userMessage - The user's input.
 * @param {import('better-sqlite3').Database} db - SQLite instance.
 * @returns {Object|null} Fast-path result or null if model inference required.
 */
function tryFastPath(userMessage, db) {
  const msg = userMessage.trim();

  if (GREETING_RE.test(msg)) {
    return {
      message: `Hello! I'm the **Agentic Mesh Coding Agent** for node **${config.NODE_NAME}** (role: \`${config.NODE_ROLE}\`, model: \`${config.OLLAMA_MODEL}\`).\n\nI can assist you with:\n• **Code & Architecture:** WebSocket/REST client code, libp2p P2P replication, vector clock causality\n• **Database Engineering:** SQLite 3NF/BCNF schemas, migrations, and DDL inspection\n• **Transaction Planning:** Safe, verified INSERT/UPDATE/DELETE operations with one-click approval\n\nWhat would you like to build or plan?`,
      operations: null,
      modelAvailable: true
    };
  }

  if (IDENTITY_RE.test(msg)) {
    return {
      message: `💻 **Agentic Mesh Coding Agent (node "${config.NODE_NAME}")**\n• **Role:** ${config.NODE_ROLE}\n• **Model:** ${config.OLLAMA_MODEL}\n• **Specialization:** Distributed database engineering, Node.js ESM, libp2p, GossipSub, and causal vector clock replication.\n\nAsk me for code snippets, API examples, schema inspection, or to generate safe database mutation plans!`,
      operations: null,
      modelAvailable: true
    };
  }

  if (HELP_RE.test(msg)) {
    return {
      message: `💡 **Agentic Mesh Coding Agent — Quick Reference:**\n\n• **Fast Local Templates (Instant):**\n  - "give Java odd/even code"\n  - "hello world in python / java / javascript"\n  - "show websocket client code"\n  - "show rest api endpoints"\n  - "basic sql explanation"\n• **Database Inspection (Instant):**\n  - "show database items", "show schema", "list categories", "what suppliers exist"\n• **Schema Planning & Mutations:**\n  - "add item Wireless Mouse for 49.99"\n  - "create category Gaming description PC peripherals"\n  - "update item 1 price to 49.99"\n  - "delete item 2"\n• **System Status:**\n  - "status" or "who are you"`,
      operations: null,
      modelAvailable: true
    };
  }

  if (STATUS_RE.test(msg)) {
    const state = getDbState(db);
    return {
      message: `⚡ **Node Status Report:**\n• **Node Name:** ${config.NODE_NAME}\n• **Role:** ${config.NODE_ROLE}\n• **Model:** ${config.OLLAMA_MODEL} (Host: ${config.OLLAMA_HOST})\n• **Database Path:** ${config.DB_PATH}\n• **Tables:** categories (${state.categories?.length || 0}), items (${state.items?.length || 0}), suppliers (${state.suppliers?.length || 0})\n• **P2P Port:** ${config.P2P_PORT} | **Gateway Port:** ${config.WS_PORT}`,
      operations: null,
      modelAvailable: true
    };
  }

  // Fast Local Template: Java even/odd code
  if (JAVA_EVEN_ODD_RE.test(msg)) {
    return {
      message: `☕ **Java Even/Odd Program:**\n\`\`\`java\npublic class EvenOdd {\n    public static void main(String[] args) {\n        int number = 7;\n        \n        if (number % 2 == 0) {\n            System.out.println(number + " is even.");\n        } else {\n            System.out.println(number + " is odd.");\n        }慶\n    }\n}\n\`\`\`\n\n**Explanation:**\nThe modulo operator \`%\` computes the remainder of integer division. If \`number % 2 == 0\`, the number divides evenly by 2 and is even; otherwise, it is odd.`,
      operations: null,
      modelAvailable: true
    };
  }

  // Fast Local Template: Hello World in Java, Python, JavaScript
  if (HELLO_WORLD_RE.test(msg)) {
    const askPython = /\bpython\b/i.test(msg);
    const askJava = /\bjava\b/i.test(msg) && !/\bjavascript|js\b/i.test(msg);
    const askJs = /\b(javascript|js)\b/i.test(msg);

    if (askPython && !askJava && !askJs) {
      return {
        message: `🐍 **Hello World in Python:**\n\`\`\`python\nprint("Hello, World!")\n\`\`\``,
        operations: null,
        modelAvailable: true
      };
    }
    if (askJava && !askPython && !askJs) {
      return {
        message: `☕ **Hello World in Java:**\n\`\`\`java\npublic class HelloWorld {\n    public static void main(String[] args) {\n        System.out.println("Hello, World!");\n    }\n}\n\`\`\``,
        operations: null,
        modelAvailable: true
      };
    }
    if (askJs && !askPython && !askJava) {
      return {
        message: `🟨 **Hello World in JavaScript:**\n\`\`\`javascript\nconsole.log("Hello, World!");\n\`\`\``,
        operations: null,
        modelAvailable: true
      };
    }

    return {
      message: `🌍 **Hello World in Java, Python, and JavaScript:**\n\n**1. Java:**\n\`\`\`java\npublic class HelloWorld {\n    public static void main(String[] args) {\n        System.out.println("Hello, World!");\n    }\n}\n\`\`\`\n\n**2. Python:**\n\`\`\`python\nprint("Hello, World!")\n\`\`\`\n\n**3. JavaScript (Node.js / Browser):**\n\`\`\`javascript\nconsole.log("Hello, World!");\n\`\`\``,
      operations: null,
      modelAvailable: true
    };
  }

  // Fast Local Template: Basic SQL explanation
  if (SQL_EXPLAIN_RE.test(msg)) {
    return {
      message: `📊 **Basic SQL (Structured Query Language) Guide:**\n\nSQL is the standard language for querying and managing relational databases like SQLite.\n\n**1. DDL (Data Definition Language) — Defining Tables:**\n\`\`\`sql\nCREATE TABLE items (\n  id INTEGER PRIMARY KEY AUTOINCREMENT,\n  name TEXT NOT NULL,\n  price REAL NOT NULL CHECK(price > 0),\n  category_id INTEGER REFERENCES categories(id)\n);\n\`\`\`\n\n**2. DML (Data Manipulation Language) — CRUD Operations:**\n• **SELECT** (Read):\n  \`\`\`sql\n  SELECT name, price FROM items WHERE price > 20 ORDER BY price DESC;\n  \`\`\`\n• **INSERT** (Create):\n  \`\`\`sql\n  INSERT INTO items (name, price, category_id) VALUES ('Keyboard', 79.99, 1);\n  \`\`\`\n• **UPDATE** (Modify):\n  \`\`\`sql\n  UPDATE items SET price = 69.99 WHERE id = 1;\n  \`\`\`\n• **DELETE** (Remove):\n  \`\`\`sql\n  DELETE FROM items WHERE id = 1;\n  \`\`\`\n\n**3. Core Principles:**\n• **Primary Key:** Unique identifier for each row.\n• **Foreign Key:** Enforces relational integrity across tables.\n• **JOIN:** Combines records across multiple tables using foreign keys.`,
      operations: null,
      modelAvailable: true
    };
  }

  if (SCHEMA_DDL_RE.test(msg)) {
    const ddl = getSchema(db);
    return {
      message: `📐 **Agentic Mesh SQLite Schema (3NF/BCNF):**\n\`\`\`sql\n${ddl}\n\`\`\``,
      operations: null,
      modelAvailable: true
    };
  }

  if (CODE_WS_RE.test(msg)) {
    return {
      message: `🔌 **WebSocket Client Integration Code:**\n\n**Node.js Client:**\n\`\`\`javascript\nimport WebSocket from 'ws';\n\nconst ws = new WebSocket('ws://localhost:${config.WS_PORT}/ws');\n\nws.on('open', () => {\n  console.log('Connected to Agentic Mesh node "${config.NODE_NAME}"');\n});\n\nws.on('message', (data) => {\n  const event = JSON.parse(data.toString());\n  console.log('Mesh Event:', event.type, event.data);\n  // Event types: 'init', 'peer:joined', 'peer:connected', 'tx:committed', 'sync:completed'\n});\n\`\`\`\n\n**Browser Client:**\n\`\`\`javascript\nconst socket = new WebSocket('ws://localhost:${config.WS_PORT}/ws');\nsocket.onmessage = (e) => {\n  const msg = JSON.parse(e.data);\n  console.log('Received:', msg);\n};\n\`\`\``,
      operations: null,
      modelAvailable: true
    };
  }

  if (CODE_REST_RE.test(msg)) {
    return {
      message: `🌐 **Agentic Mesh REST API Client Examples:**\n\n**1. JavaScript / Fetch (Node.js or Browser):**\n\`\`\`javascript\n// Query database state\nconst res = await fetch('http://localhost:${config.WS_PORT}/api/db/state');\nconst { items, categories } = await res.json();\nconsole.log('Items:', items);\n\n// Propose transaction\nconst postRes = await fetch('http://localhost:${config.WS_PORT}/api/propose', {\n  method: 'POST',\n  headers: { 'Content-Type': 'application/json' },\n  body: JSON.stringify({\n    table: 'items',\n    operation: 'INSERT',\n    data: { name: 'USB-C Hub', price: 29.99, sku: 'SKU-HUB-101', category_id: 1 }\n  })\n});\nconsole.log('Write result:', await postRes.json());\n\`\`\`\n\n**2. cURL:**\n\`\`\`bash\n# Check node status\ncurl -s http://localhost:${config.WS_PORT}/api/health\n\n# Post chat message\ncurl -s -X POST http://localhost:${config.WS_PORT}/api/agent/chat \\\n  -H "Content-Type: application/json" \\\n  -d '{"message":"show database items"}'\n\`\`\``,
      operations: null,
      modelAvailable: true
    };
  }

  if (SYNC_EXPLAIN_RE.test(msg)) {
    return {
      message: `⚡ **Vector Clock Causal Sync Engine:**\n\n1. **Local Writes:** Each node increments its own counter in the vector clock: \`clock[nodeId]++\` and logs to \`_mesh_log\`.\n2. **GossipSub:** Transmitted to peers as \`{ payload, vectorClock, peerId }\`.\n3. **Causal Check:** Receiving peer compares vector clocks:\n   - **Newer:** Applied directly to SQLite and merged element-wise maximum.\n   - **Duplicate:** Ignored safely (idempotent).\n   - **Conflict:** Detected when clocks are concurrent; resolved deterministically by peer ID.\n4. **Catch-up Sync:** On reconnect, nodes request missing transaction deltas based on their last known vector clock snapshot.`,
      operations: null,
      modelAvailable: true
    };
  }

  if (DATA_QUERY_RE.test(msg) && !MUTATION_RE.test(msg)) {
    return buildDataQueryResponse(msg, db);
  }

  if (MUTATION_RE.test(msg)) {
    const fastMutation = tryFastMutationPlan(msg, db);
    if (fastMutation) return fastMutation;
  }

  if (DATA_QUERY_RE.test(msg) && !MUTATION_RE.test(msg)) {
    return buildDataQueryResponse(msg, db);
  }

  if (MUTATION_RE.test(msg)) {
    const fastMutation = tryFastMutationPlan(msg, db);
    if (fastMutation) return fastMutation;
  }

  return null;
}

/**
 * Builds a formatted data-query answer directly from SQLite (no model call).
 */
function buildDataQueryResponse(msg, db) {
  const state = getDbState(db);
  const askItems = /\b(items?|products?|stock|inventory)\b/i.test(msg);
  const askCats = /\b(categories|category)\b/i.test(msg);
  const askSups = /\b(suppliers?|vendor)\b/i.test(msg);
  const askAll = /\b(all|everything|database|data)\b/i.test(msg) || (!askCats && !askSups);

  const parts = [];

  if (askItems || askAll) {
    const items = state.items || [];
    let s = `📦 Items (${items.length} total):\n`;
    if (items.length === 0) s += '  No items in database yet.\n';
    else items.forEach(i => { s += `  • ${i.name} — $${i.price} (SKU: ${i.sku}, Cat ID: ${i.category_id})\n`; });
    parts.push(s);
  }
  if (askCats || askAll) {
    const cats = state.categories || [];
    let s = `🏷️ Categories (${cats.length} total):\n`;
    cats.forEach(c => { s += `  • [${c.id}] ${c.name}${c.description ? ` — ${c.description}` : ''}\n`; });
    parts.push(s);
  }
  if (askSups || askAll) {
    const sups = state.suppliers || [];
    let s = `🏢 Suppliers (${sups.length} total):\n`;
    if (sups.length === 0) s += '  No suppliers registered yet.\n';
    else sups.forEach(su => { s += `  • [${su.id}] ${su.name}${su.contact_email ? ` (${su.contact_email})` : ''}\n`; });
    parts.push(s);
  }

  return { message: parts.join('\n').trim(), operations: null, modelAvailable: true };
}

/**
 * Fast-path heuristic planner for standard data mutation patterns.
 * Generates verified schema plans without invoking the heavy LLM.
 * @param {string} msg - User input.
 * @param {import('better-sqlite3').Database} db - SQLite database.
 * @returns {Object|null}
 */
function tryFastMutationPlan(msg, db) {
  // 1. Add/Create Category
  const catMatch = msg.match(/^(?:add|create|insert)(?:\s+a)?(?:\s+new)?\s+category\s+([A-Za-z0-9_\-\s]+?)(?:\s+(?:with\s+description|description|desc)\s+([A-Za-z0-9_\-.,\s]+))?$/i);
  if (catMatch) {
    const name = catMatch[1].trim();
    const description = catMatch[2] ? catMatch[2].trim() : `Category for ${name}`;
    if (name) {
      const ops = [{ operation: 'INSERT', table: 'categories', data: { name, description } }];
      const v = validateModelPlan(ops);
      if (v.valid) {
        return {
          message: `I have prepared a verified plan to create the category "${name}". Please review and approve below:`,
          operations: v.operations,
          modelAvailable: true
        };
      }
    }
  }

  // 2. Add/Insert Item
  if (/\b(?:add|insert|create)\b/i.test(msg) && (/\b(?:item|product|price|priced|for)\b/i.test(msg) || msg.includes('$'))) {
    const priceMatch = msg.match(/(?:priced\s+at|price\s+(?:is|at)?|price[:\s]+|for|\$)\s*\$?([0-9]+(?:\.[0-9]{1,2})?)/i) ||
                       msg.match(/\b([0-9]+(?:\.[0-9]{1,2})?)\s*(?:dollars?|bucks?)/i);
    const price = priceMatch ? parseFloat(priceMatch[1]) : null;

    const skuMatch = msg.match(/\bsku[:\s]+([A-Za-z0-9\-_]+)/i);

    let categoryId = 1;
    const catIdMatch = msg.match(/\bcategory\s*(?:id)?[:\s]+([0-9]+)/i);
    if (catIdMatch) {
      categoryId = parseInt(catIdMatch[1], 10);
    } else {
      const catNameMatch = msg.match(/\bcategory\s+([A-Za-z0-9_-]+)/i);
      if (catNameMatch) {
        try {
          const found = db.prepare('SELECT id FROM categories WHERE LOWER(name) = LOWER(?)').get(catNameMatch[1].trim());
          if (found) categoryId = found.id;
        } catch { /* ignore */ }
      }
    }

    let name = '';
    const itemPrefixMatch = msg.match(/(?:add|insert|create)(?:\s+a)?(?:\s+new)?(?:\s+item|\s+product)?\s+(.+?)(?:\s+price|\s+priced|\s+for|\s+with\s+sku|\s+in\s+category|\s+category|\$)/i);
    if (itemPrefixMatch) {
      name = itemPrefixMatch[1].trim().replace(/^(?:an|a)\s+/i, '');
    }

    if (name && price !== null && !isNaN(price)) {
      const autoSku = `SKU-${name.replace(/[^A-Za-z0-9]/g, '').slice(0, 4).toUpperCase()}-${Math.floor(1000 + Math.random() * 9000)}`;
      const sku = skuMatch ? skuMatch[1].toUpperCase().trim() : autoSku;
      const ops = [{
        operation: 'INSERT',
        table: 'items',
        data: { name, price, sku, category_id: categoryId }
      }];
      const v = validateModelPlan(ops);
      if (v.valid) {
        return {
          message: `I have prepared a verified plan to insert the new item "${name}" ($${price.toFixed(2)}). Please review and approve below:`,
          operations: v.operations,
          modelAvailable: true
        };
      }
    }
  }

  // 3. Delete/Remove Item
  const delMatch = msg.match(/^(?:delete|remove)\s+(?:item|product)\s+([0-9]+)$/i);
  if (delMatch) {
    const id = parseInt(delMatch[1], 10);
    let existingName = '';
    try {
      const row = db.prepare('SELECT name FROM items WHERE id = ?').get(id);
      if (row) existingName = row.name;
    } catch { /* ignore */ }

    const ops = [{ operation: 'DELETE', table: 'items', data: { id } }];
    const v = validateModelPlan(ops);
    if (v.valid) {
      return {
        message: `I have prepared a verified plan to delete item #${id}${existingName ? ` ("${existingName}")` : ''}. Please review and approve below:`,
        operations: v.operations,
        modelAvailable: true
      };
    }
  }

  // 4. Update Item Price
  const updatePriceMatch = msg.match(/^(?:update|change|set)\s+(?:item|product)\s+([0-9]+)\s+price\s+(?:to\s+)?\$?([0-9]+(?:\.[0-9]{1,2})?)$/i);
  if (updatePriceMatch) {
    const id = parseInt(updatePriceMatch[1], 10);
    const price = parseFloat(updatePriceMatch[2]);
    let existingName = '';
    try {
      const row = db.prepare('SELECT name FROM items WHERE id = ?').get(id);
      if (row) existingName = row.name;
    } catch { /* ignore */ }

    const ops = [{ operation: 'UPDATE', table: 'items', data: { id, price } }];
    const v = validateModelPlan(ops);
    if (v.valid) {
      return {
        message: `I have prepared a verified plan to update item #${id}${existingName ? ` ("${existingName}")` : ''} price to $${price.toFixed(2)}. Please review and approve below:`,
        operations: v.operations,
        modelAvailable: true
      };
    }
  }

  return null;
}

// ── Prompt Builders & Execution Tiers ───────────────────────────────────────

export const ExecutionTier = {
  INSTANT: 'instant',
  FAST_CODING: 'fast_coding',
  HEAVY_AI: 'heavy_ai'
};

/**
 * Classifies a prompt into one of the three execution tiers.
 * @param {string} userMessage
 * @param {string} [mode='fast']
 * @returns {string}
 */
export function classifyPrompt(userMessage, mode = 'fast') {
  if (mode === 'deep') return ExecutionTier.HEAVY_AI;
  if (MUTATION_RE.test(userMessage)) return ExecutionTier.HEAVY_AI;
  return ExecutionTier.FAST_CODING;
}

function buildSystemPrompt(db, tier, isMutation) {
  if (tier === ExecutionTier.FAST_CODING) {
    // Slim, CPU-safe prompt for general coding questions — zero DB schema tokens
    return `You are a concise coding assistant for Agentic Mesh node "${config.NODE_NAME}". Provide clean code with a brief explanation. Keep responses concise (under ${config.OLLAMA_NUM_PREDICT} tokens). Do not output JSON.`;
  }

  // Tier 3: Heavy AI
  if (isMutation) {
    // Mutation planning with SQLite 3NF schema
    const state = getDbState(db);
    const schemaDDL = getSchema(db);
    return `You are the Agentic Mesh Coding Agent for node "${config.NODE_NAME}" (role: ${config.NODE_ROLE}).
Your task is to plan safe, verified 3NF SQLite database mutations.

DATABASE SCHEMA:
${schemaDDL}

DATA SNAPSHOT:
Categories: ${JSON.stringify(state.categories || [])}
Items (${state.items?.length || 0}): ${JSON.stringify((state.items || []).slice(0, 10))}
Suppliers: ${JSON.stringify(state.suppliers || [])}

RULES:
1. NEVER output raw SQL.
2. For data modifications, respond with ONLY this JSON:
   {"message":"explanation","operations":[{"operation":"INSERT|UPDATE|DELETE","table":"categories|items|suppliers|item_suppliers","data":{...}}]}
3. Map category names to existing integer IDs. Auto-generate SKU (e.g. SKU-AUTO-101) if not provided.
4. For non-write questions, respond: {"message":"answer","operations":null}

Respond with ONLY a JSON object, no markdown fences.`;
  }

  // Deep architecture planning
  const schemaDDL = getSchema(db);
  return `You are a Senior Distributed Systems Architect for Agentic Mesh node "${config.NODE_NAME}".
Provide rigorous architectural and technical guidance on libp2p, GossipSub, vector clocks, and SQLite.

DATABASE SCHEMA:
${schemaDDL}`;
}

function buildFullPrompt(userMessage, conversationHistory, db, tier, isMutation) {
  const systemPrompt = buildSystemPrompt(db, tier, isMutation);

  let historyContext = '';
  if (Array.isArray(conversationHistory) && conversationHistory.length > 0) {
    // Strictly at most 2 prior messages for Fast AI to minimize CPU context processing
    const sliceCount = tier === ExecutionTier.FAST_CODING ? -2 : -4;
    const recent = conversationHistory.slice(sliceCount);
    historyContext = recent.map(h => `${h.role === 'user' ? 'User' : 'Assistant'}: ${h.content}`).join('\n');
  }

  return `${systemPrompt}\n\n${historyContext ? `RECENT CONVERSATION:\n${historyContext}\n` : ''}User: ${userMessage}\nAssistant:`;
}

// ── Response Parsing ────────────────────────────────────────────────────────

function parseModelResponse(rawResponse) {
  if (!rawResponse) return { message: 'The AI agent did not return a response.', operations: null };

  let parsed = null;
  try {
    parsed = JSON.parse(rawResponse);
  } catch {
    const jsonMatch = rawResponse.match(/\{[\s\S]*"message"[\s\S]*\}/);
    if (jsonMatch) {
      try { parsed = JSON.parse(jsonMatch[0]); } catch { parsed = null; }
    }
  }

  if (parsed && typeof parsed === 'object') {
    let operations = null;
    if (Array.isArray(parsed.operations) && parsed.operations.length > 0) {
      const v = validateModelPlan(parsed.operations);
      if (v.valid) operations = v.operations;
    }
    return { message: parsed.message || 'Plan generated.', operations };
  }

  const arrayMatch = rawResponse.match(/\[\s*\{[\s\S]*\}\s*\]/);
  if (arrayMatch) {
    try {
      const ops = JSON.parse(arrayMatch[0]);
      const v = validateModelPlan(ops);
      if (v.valid) return { message: 'I have prepared the following database operations:', operations: v.operations };
    } catch { /* ignore */ }
  }

  return { message: rawResponse.replace(/```json/g, '').replace(/```/g, '').trim(), operations: null };
}

// ── Public Chat Functions ───────────────────────────────────────────────────

/**
 * Non-streaming chat (backward compatible — used by tests and fallback).
 * @param {string} userMessage
 * @param {Array<Object>} [conversationHistory=[]]
 * @param {import('better-sqlite3').Database} [db=null]
 * @param {Object} [options={}]
 * @returns {Promise<{ message: string, operations: Array|null, modelAvailable: boolean, raw?: string }>}
 */
export async function chatWithAgent(userMessage, conversationHistory = [], db = null, options = {}) {
  const fast = tryFastPath(userMessage, db);
  if (fast) return fast;

  const status = await cachedModelStatus();
  if (!status.ollamaReachable) {
    return {
      message: `AI Agent is currently offline (Ollama unreachable at ${config.OLLAMA_HOST}).`,
      operations: null,
      modelAvailable: false
    };
  }

  if (!status.activeModel) {
    return {
      message: `Configured model "${config.OLLAMA_MODEL}" is not installed in Ollama at ${config.OLLAMA_HOST}. Available models: ${status.availableModels.join(', ') || 'none'}.`,
      operations: null,
      modelAvailable: false
    };
  }

  const mode = options.mode || 'fast';
  const tier = classifyPrompt(userMessage, mode);
  const isMutation = MUTATION_RE.test(userMessage);
  const prompt = buildFullPrompt(userMessage, conversationHistory, db, tier, isMutation);

  let targetModel = config.OLLAMA_MODEL;
  let numPredict = config.OLLAMA_NUM_PREDICT;
  let temperature = config.OLLAMA_TEMPERATURE;

  if (mode === 'deep') {
    const deepModelMatch = status.availableModels.find(m => m.includes('gemma') || m.includes('deep'));
    if (deepModelMatch) targetModel = deepModelMatch;
    numPredict = 256;
    temperature = 0.3;
  } else if (isMutation) {
    numPredict = 256;
  }

  try {
    const rawResponse = await askModel(prompt, {
      reqId: options.reqId,
      model: targetModel,
      options: {
        num_ctx: config.OLLAMA_NUM_CTX,
        num_predict: numPredict,
        temperature
      }
    });
    const result = parseModelResponse(rawResponse);
    return { ...result, modelAvailable: true, raw: rawResponse };
  } catch (err) {
    const isTimeout = err.name === 'TimeoutError' || err.name === 'AbortError' || err.message?.includes('timeout') || err.message?.includes('aborted');
    return {
      message: isTimeout
        ? 'The local model did not respond within 90 seconds. Check Ollama CPU/GPU usage and selected model.'
        : (err.message || 'Error communicating with AI agent.'),
      operations: null,
      modelAvailable: false
    };
  }
}

/**
 * Streaming chat — yields SSE-compatible event objects.
 * 
 * Events emitted:
 *   { type: 'token', content: '...' }  — a generated text fragment (non-mutations only)
 *   { type: 'done',  message, operations, modelAvailable } — final parsed result
 *   { type: 'error', message, operations: null, modelAvailable } — terminal error
 * 
 * @param {string} userMessage
 * @param {Array<Object>} [conversationHistory=[]]
 * @param {import('better-sqlite3').Database} [db=null]
 * @param {Object} [options={}]
 * @yields {{ type: string, content?: string, message?: string, operations?: Array|null, modelAvailable?: boolean }}
 */
export async function* chatWithAgentStream(userMessage, conversationHistory = [], db = null, options = {}) {
  // Tier 1 — Instant fast-path
  const fast = tryFastPath(userMessage, db);
  if (fast) { yield { type: 'done', ...fast }; return; }

  // Tier 2/3 — Model required
  const status = await cachedModelStatus();
  if (!status.ollamaReachable) {
    yield {
      type: 'error',
      message: `AI Agent offline (Ollama unreachable at ${config.OLLAMA_HOST}).`,
      operations: null,
      modelAvailable: false
    };
    return;
  }

  if (!status.activeModel) {
    yield {
      type: 'error',
      message: `Configured model "${config.OLLAMA_MODEL}" is not installed in Ollama at ${config.OLLAMA_HOST}. Available models: ${status.availableModels.join(', ') || 'none'}.`,
      operations: null,
      modelAvailable: false
    };
    return;
  }

  const mode = options.mode || 'fast';
  const tier = classifyPrompt(userMessage, mode);
  const isMutation = MUTATION_RE.test(userMessage);
  const prompt = buildFullPrompt(userMessage, conversationHistory, db, tier, isMutation);

  let targetModel = config.OLLAMA_MODEL;
  let numPredict = config.OLLAMA_NUM_PREDICT;
  let temperature = config.OLLAMA_TEMPERATURE;

  if (mode === 'deep') {
    const deepModelMatch = status.availableModels.find(m => m.includes('gemma') || m.includes('deep'));
    if (deepModelMatch) targetModel = deepModelMatch;
    numPredict = 256;
    temperature = 0.3;
  } else if (isMutation) {
    numPredict = 256;
  }

  try {
    const stream = await askModelStream(prompt, {
      reqId: options.reqId,
      model: targetModel,
      options: {
        num_ctx: config.OLLAMA_NUM_CTX,
        num_predict: numPredict,
        temperature
      }
    });

    if (!stream) {
      yield {
        type: 'error',
        message: 'Failed to connect to AI model for streaming.',
        operations: null,
        modelAvailable: true
      };
      return;
    }

    let fullResponse = '';

    if (isMutation) {
      // Tier 3 — Buffer full JSON response, then parse
      for await (const chunk of stream) {
        fullResponse += chunk.message?.content || '';
      }
      const result = parseModelResponse(fullResponse);
      yield { type: 'done', ...result, modelAvailable: true, raw: fullResponse };
    } else {
      // Tier 2 — Stream plain-text tokens in real-time
      for await (const chunk of stream) {
        const token = chunk.message?.content || '';
        if (token) {
          fullResponse += token;
          yield { type: 'token', content: token };
        }
      }

      if (!fullResponse.trim()) {
        yield {
          type: 'error',
          message: 'The local model did not respond within 90 seconds. Check Ollama CPU/GPU usage and selected model.',
          operations: null,
          modelAvailable: true
        };
        return;
      }

      yield { type: 'done', message: fullResponse.trim(), operations: null, modelAvailable: true, raw: fullResponse };
    }
  } catch (err) {
    const isTimeout = err.name === 'TimeoutError' || err.name === 'AbortError' || err.message?.includes('timeout') || err.message?.includes('aborted');
    yield {
      type: 'error',
      message: isTimeout
        ? 'The local model did not respond within 90 seconds. Check Ollama CPU/GPU usage and selected model.'
        : (err.message || 'Error communicating with AI agent.'),
      operations: null,
      modelAvailable: false
    };
  }
}
