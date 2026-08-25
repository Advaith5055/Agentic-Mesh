import { askGemma } from './src/agents/ollama.js';

console.log('Testing Gemma 4 E2B via Ollama JS SDK...');

try {
  const response = await askGemma('Return only JSON: {"status":"online"}');
  console.log('\n--- Model Response ---');
  console.log(response);
  console.log('----------------------\n');
} catch (err) {
  console.error('Error during test:', err);
}
