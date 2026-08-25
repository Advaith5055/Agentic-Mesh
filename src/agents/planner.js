// src/agents/planner.js
import { askGemma } from './ollama.js';
import { getSchema, getDbState } from '../db/sqlite.js';

/**
 * Decomposes a natural language request into database operations using Gemma 4 E2B.
 * Does NOT give raw SQL access — generates structured database tool operations.
 * 
 * @async
 * @param {string} naturalLanguageRequest - The user's prompt.
 * @param {Object} db - The SQLite database instance.
 * @returns {Promise<Object>} An object containing the decomposed plan operations.
 */
export async function planOperations(naturalLanguageRequest, db) {
  try {
    const schemaDDL = getSchema(db);
    const state = getDbState(db);
    
    const prompt = `You are a database operations planner using Gemma 4 E2B for a 3NF SQLite database.
Do NOT output raw SQL queries. Decompose the request into database tool operations.

DATABASE SCHEMA:
${schemaDDL}

CURRENT DATA SNAPSHOT:
Categories: ${JSON.stringify(state.categories || [])}
Items (sample): ${JSON.stringify((state.items || []).slice(0, 10))}
Suppliers: ${JSON.stringify(state.suppliers || [])}

AVAILABLE TOOL OPERATIONS:
1. INSERT category: { "operation": "INSERT", "table": "categories", "data": { "name": "...", "description": "..." } }
2. INSERT item: { "operation": "INSERT", "table": "items", "data": { "category_id": N, "name": "...", "price": N, "sku": "..." } }
3. UPDATE item: { "operation": "UPDATE", "table": "items", "data": { "id": N, "price": N, ... } }
4. DELETE item: { "operation": "DELETE", "table": "items", "data": { "id": N } }

RULES:
- Map category names to their existing "id" in CURRENT DATA (e.g. "Electronics" = 1, "Books" = 2, "Clothing" = 3).
- If SKU is not specified, auto-generate a valid unique SKU (e.g. "SKU-AUTO-101").
- Return ONLY a valid JSON array of operation objects. No explanations or Markdown code blocks outside the JSON.

USER REQUEST: "${naturalLanguageRequest}"`;

    const rawResponse = await askGemma(prompt);

    if (!rawResponse) {
      return { success: false, operations: [], raw: null, error: 'Gemma model unavailable' };
    }

    let operations = [];
    try {
      operations = JSON.parse(rawResponse);
    } catch (e) {
      // Fallback regex extraction if model wraps output in markdown code block ```json ... ```
      const jsonMatch = rawResponse.match(/\[\s*\{.*\}\s*\]/s);
      if (jsonMatch) {
        try {
          operations = JSON.parse(jsonMatch[0]);
        } catch (e2) {
          return { success: false, operations: [], raw: rawResponse, error: 'Failed to parse JSON array from Gemma response' };
        }
      } else {
        return { success: false, operations: [], raw: rawResponse, error: 'No JSON array found in response' };
      }
    }

    if (!Array.isArray(operations)) {
      return { success: false, operations: [], raw: rawResponse, error: 'Parsed output is not an array' };
    }

    return { success: true, operations, raw: rawResponse };
  } catch (error) {
    return { success: false, operations: [], raw: null, error: error.message };
  }
}
