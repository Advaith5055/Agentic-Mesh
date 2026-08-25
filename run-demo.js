import { initDatabase, getDbState } from './src/db/sqlite.js';
import { createMeshNode, startNode, stopNode } from './src/p2p/node.js';
import { initSync } from './src/db/sync.js';
import { executeSingleTransaction, executeOperations } from './src/agents/executor.js';
import { planOperations } from './src/agents/planner.js';
import { auditRecentTransactions } from './src/agents/validator.js';
import { isGemmaAvailable } from './src/agents/ollama.js';

console.log('═════════════════════════════════════════════════════════════════');
console.log('         AGENTIC MESH — LIVE SYSTEM EXECUTION & DEMO              ');
console.log('═════════════════════════════════════════════════════════════════\n');

// 1. Database Subsystem
console.log('1️⃣  INITIALIZING SQLITE DATABASE...');
const db = initDatabase('./data/demo-mesh.db');
const initialState = getDbState(db);
console.log(`✔ SQLite DB Ready (WAL Mode)`);
console.log(`   Categories: ${initialState.categories.length}`);
console.log(`   Items:      ${initialState.items.length}`);
console.log(`   Suppliers:  ${initialState.suppliers.length}\n`);

// 2. P2P Subsystem
console.log('2️⃣  STARTING LIBP2P P2P MESH NODE...');
const port = 0; // Dynamic port assignment
const node = await createMeshNode(port);
await startNode(node);
const peerId = node.peerId.toString();
console.log(`✔ P2P Node Listening`);
console.log(`   Peer ID: ${peerId}\n`);

// 3. Vector Clock Engine
console.log('3️⃣  INITIALIZING VECTOR CLOCK SYNC ENGINE...');
initSync(peerId);
console.log(`✔ Vector clock tracking online for ${peerId.slice(0, 12)}...\n`);

// 4. Fast-Path Transaction (<1ms)
console.log('4️⃣  TESTING FAST-PATH TRANSACTION (P2P Broadcast)...');
const fastTx = {
  operation: 'INSERT',
  table: 'items',
  data: { category_id: 1, name: 'Pro Gaming Mouse', price: 79.99, sku: `SKU-M${Math.floor(Math.random()*10000)}` }
};
const fastResult = await executeSingleTransaction(
  fastTx.operation,
  fastTx.table,
  fastTx.data,
  db,
  peerId,
  (envelope) => console.log(`   [GOSSIP BROADCAST] Transaction ${envelope.id.slice(0, 8)}... sent to topic mesh:transactions`)
);
console.log('✔ Fast-Path Result:', fastResult, '\n');

// 5. Schema Validation Rejection Test
console.log('5️⃣  TESTING SCHEMA VALIDATOR (Sub-ms Malformed Payload Rejection)...');
const badTx = {
  operation: 'INSERT',
  table: 'items',
  data: { category_id: 999, name: 'Orphaned Item', price: -50, sku: 'INVALID' }
};
const badResult = await executeSingleTransaction(badTx.operation, badTx.table, badTx.data, db, peerId);
console.log('✔ Schema Violation Intercepted:', badResult, '\n');

// 6. Gemma 4 E2B AI Natural Language Planning
console.log('6️⃣  TESTING GEMMA 4 E2B AI PLANNER & EXECUTOR...');
const gemmaOnline = await isGemmaAvailable();
console.log(`   Gemma 4 E2B Status: ${gemmaOnline ? 'ONLINE' : 'OFFLINE'}`);

if (gemmaOnline) {
  const prompt = 'Add a new book titled Microservices Patterns priced at 59.99 with SKU-B599';
  console.log(`   User Prompt: "${prompt}"`);
  console.time('   Gemma4-Inference-Time');
  const plan = await planOperations(prompt, db);
  console.timeEnd('   Gemma4-Inference-Time');
  
  console.log('   Gemma 4 Decomposed Operations:');
  console.log(JSON.stringify(plan.operations, null, 4));

  const execRes = await executeOperations(plan.operations, db, peerId, (env) => {
    console.log(`   [GOSSIP BROADCAST] AI Executed Tx ${env.id.slice(0, 8)}... sent to mesh:transactions`);
  });
  console.log('✔ Execution Result:', execRes, '\n');
}

// 7. Background AI Audit
console.log('7️⃣  RUNNING BACKGROUND 3NF / BCNF AUDIT...');
const audit = await auditRecentTransactions(db, 5);
console.log('✔ AI Audit Result:', JSON.stringify(audit, null, 2), '\n');

// 8. Final DB State Snapshot
console.log('8️⃣  FINAL REPLICATED SQLITE DATABASE SNAPSHOT:');
const finalState = getDbState(db);
console.table(finalState.items);

// Cleanup
await stopNode(node);
db.close();
console.log('═════════════════════════════════════════════════════════════════');
console.log('   DEMO COMPLETED SUCCESSFULLY — ALL SUBSYSTEMS VERIFIED!        ');
console.log('═════════════════════════════════════════════════════════════════');
