// src/agents/validator.js
import { ollamaChat } from './router.js';
import { getSchema } from '../db/sqlite.js';

/**
 * Audits a sequence of database operations for 3NF/BCNF compliance before execution.
 * @param {Array<Object>} operations - The proposed operations array.
 * @param {Object} db - The SQLite database instance.
 * @returns {Promise<Object>} An object with validity flag and an array of issues.
 */
export async function auditOperations(operations, db) {
    try {
        const schemaDDL = getSchema(db);
        
        const systemPrompt = `You are a strict 3NF/BCNF database schema auditor.
        
DATABASE SCHEMA:
${schemaDDL}
        
PLANNED OPERATIONS:
${JSON.stringify(operations)}
        
Validate each operation for:
1. All required fields present with correct types
2. Foreign keys reference valid tables and columns
3. No normalization violations (no transitive dependencies, no partial key dependencies)
4. UNIQUE and CHECK constraints respected
        
Return JSON: { "valid": true/false, "issues": [{ "index": N, "reasoning": "...", "fix": "..." }] }`;

        const rawResponse = await ollamaChat([
            { role: 'system', content: systemPrompt },
            { role: 'user', content: 'Audit the planned operations according to instructions.' }
        ]);

        if (!rawResponse) {
            return { valid: true, issues: [], warning: 'AI validator unavailable, bypassing.' }; // Bypass if AI is down
        }

        let result = { valid: false, issues: [] };
        
        try {
            result = JSON.parse(rawResponse);
        } catch (e) {
            const jsonMatch = rawResponse.match(/\{\s*"valid".*\}/s);
            if (jsonMatch) {
                try {
                    result = JSON.parse(jsonMatch[0]);
                } catch (e2) {
                    return { valid: false, issues: [{ index: -1, reasoning: 'Failed to parse AI response JSON' }] };
                }
            } else {
                return { valid: false, issues: [{ index: -1, reasoning: 'Invalid AI response format' }] };
            }
        }

        return result;
    } catch (error) {
        return { valid: false, issues: [{ index: -1, reasoning: `Exception during audit: ${error.message}` }] };
    }
}

/**
 * Audits the last N transactions from the mesh log for compliance.
 * @param {Object} db - The database instance.
 * @param {number} [count=10] - The number of recent transactions to audit.
 * @returns {Promise<Object>} The audit findings.
 */
export async function auditRecentTransactions(db, count = 10) {
    try {
        // Retrieve recent logs from _mesh_log
        const logs = db.prepare ? db.prepare(`SELECT * FROM _mesh_log ORDER BY applied_at DESC LIMIT ?`).all(count) : [];
        const schemaDDL = getSchema(db);

        const systemPrompt = `You are a database auditor. Review the following recent transactions for 3NF compliance and general consistency.
SCHEMA:
${schemaDDL}
        
TRANSACTIONS:
${JSON.stringify(logs)}`;

        const rawResponse = await ollamaChat([
             { role: 'system', content: systemPrompt },
             { role: 'user', content: 'Identify any potential schema violations or anomalies in these past transactions. Return JSON array of issues.' }
        ]);

        if (!rawResponse) return { issues: [] };

        let issues = [];
        try {
            issues = JSON.parse(rawResponse);
        } catch(e) {
            const match = rawResponse.match(/\[\s*\{.*\}\s*\]/s);
            if (match) {
                issues = JSON.parse(match[0]);
            }
        }
        
        return { issues };
    } catch (error) {
         return { issues: [{ error: error.message }] };
    }
}
