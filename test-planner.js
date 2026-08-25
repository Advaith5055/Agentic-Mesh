import { initDatabase } from './src/db/sqlite.js';
import { planOperations } from './src/agents/planner.js';

console.log('--- Testing Gemma 4 E2B Database Operation Planner ---');

const db = initDatabase('./data/test-mesh.db');

const request = 'Add a new laptop to Electronics priced at $999.99 with SKU-E999';
console.log(`User Prompt: "${request}"`);

console.time('Gemma4-Planner-Time');
const result = await planOperations(request, db);
console.timeEnd('Gemma4-Planner-Time');

console.log('\nResult Success:', result.success);
console.log('Decomposed Operations:');
console.log(JSON.stringify(result.operations, null, 2));

db.close();
