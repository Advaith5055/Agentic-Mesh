/**
 * @fileoverview Executor agent — executes validated operations against SQLite and broadcasts to mesh.
 * @module agents/executor
 */

import { insertItem, insertCategory } from '../db/sqlite.js';
import { validateTransaction } from '../db/schema-validator.js';
import { recordLocalWrite } from '../db/sync.js';
import { logger } from '../utils/logger.js';

/**
 * Executes a sequence of validated database operations atomically.
 * Each operation is schema-validated, written to SQLite, logged to _mesh_log,
 * and broadcast to the P2P network.
 * 
 * @async
 * @param {Array<Object>} operations - Array of { operation, table, data } objects.
 * @param {import('better-sqlite3').Database} db - The SQLite database instance.
 * @param {string} peerId - The local peer identifier.
 * @param {Function} broadcastFn - Callback to broadcast the local write to the network.
 * @returns {Promise<Object>} Execution result with { completed, failed, results }.
 */
export async function executeOperations(operations, db, peerId, broadcastFn) {
  const results = [];
  let completed = 0;
  let failed = 0;

  for (const op of operations) {
    try {
      // 1. Fast schema validation
      const validation = validateTransaction(op.operation, op.table, op.data, db);
      
      if (!validation.valid) {
        results.push({ operation: op, success: false, errors: validation.errors });
        failed++;
        logger.error(`Validation failed for ${op.operation} on ${op.table}: ${validation.errors.join(', ')}`);
        continue;
      }

      // 2. Execute the SQL write using the appropriate helper
      let writeResult = null;
      if (op.table === 'categories' && op.operation === 'INSERT') {
        writeResult = insertCategory(db, op.data);
      } else if (op.table === 'items' && op.operation === 'INSERT') {
        writeResult = insertItem(db, op.data);
      } else if (op.operation === 'INSERT') {
        // Generic INSERT for other tables (suppliers, item_suppliers)
        const keys = Object.keys(op.data);
        const placeholders = keys.map(() => '?').join(', ');
        const stmt = db.prepare(`INSERT INTO ${op.table} (${keys.join(', ')}) VALUES (${placeholders})`);
        writeResult = stmt.run(...Object.values(op.data));
      } else if (op.operation === 'UPDATE') {
        const setFields = Object.keys(op.data).filter(k => k !== 'id');
        const setClause = setFields.map(k => `${k} = ?`).join(', ');
        const stmt = db.prepare(`UPDATE ${op.table} SET ${setClause} WHERE id = ?`);
        writeResult = stmt.run(...setFields.map(k => op.data[k]), op.data.id);
      } else if (op.operation === 'DELETE') {
        const stmt = db.prepare(`DELETE FROM ${op.table} WHERE id = ?`);
        writeResult = stmt.run(op.data.id);
      }

      // 3. Record in _mesh_log and get broadcast envelope
      const envelope = recordLocalWrite(db, op.operation, op.table, op.data, peerId);

      // 4. Broadcast to peers
      if (broadcastFn && typeof broadcastFn === 'function') {
        await broadcastFn(envelope);
      }

      // 5. Log success
      logger.db(`Executed ${op.operation} on ${op.table} successfully`);

      results.push({ operation: op, success: true, rowId: writeResult?.lastInsertRowid });
      completed++;
    } catch (err) {
      logger.error(`Error executing ${op.operation} on ${op.table}: ${err.message}`);
      results.push({ operation: op, success: false, errors: [err.message] });
      failed++;
    }
  }

  return { completed, failed, results };
}

/**
 * Executes a single fast-path database transaction.
 * Schema validate → Execute write → Record in mesh log → Broadcast.
 * 
 * @async
 * @param {string} operation - The operation type (INSERT, UPDATE, DELETE).
 * @param {string} tableName - The target table.
 * @param {Object} data - The data payload for the operation.
 * @param {import('better-sqlite3').Database} db - The SQLite database instance.
 * @param {string} peerId - The local peer identifier.
 * @param {Function} broadcastFn - Callback to broadcast the write to the mesh.
 * @returns {Promise<Object>} The result: { success, rowId?, errors? }
 */
export async function executeSingleTransaction(operation, tableName, data, db, peerId, broadcastFn) {
  // 1. Schema validate
  const validation = validateTransaction(operation, tableName, data, db);
  if (!validation.valid) {
    logger.error(`Validation failed: ${validation.errors.join(', ')}`);
    return { success: false, errors: validation.errors };
  }

  try {
    // 2. Execute write
    let writeResult = null;
    if (tableName === 'categories' && operation === 'INSERT') {
      writeResult = insertCategory(db, data);
    } else if (tableName === 'items' && operation === 'INSERT') {
      writeResult = insertItem(db, data);
    } else if (operation === 'INSERT') {
      const keys = Object.keys(data);
      const placeholders = keys.map(() => '?').join(', ');
      const stmt = db.prepare(`INSERT INTO ${tableName} (${keys.join(', ')}) VALUES (${placeholders})`);
      writeResult = stmt.run(...Object.values(data));
    } else if (operation === 'UPDATE') {
      const setFields = Object.keys(data).filter(k => k !== 'id');
      const setClause = setFields.map(k => `${k} = ?`).join(', ');
      const stmt = db.prepare(`UPDATE ${tableName} SET ${setClause} WHERE id = ?`);
      writeResult = stmt.run(...setFields.map(k => data[k]), data.id);
    } else if (operation === 'DELETE') {
      const stmt = db.prepare(`DELETE FROM ${tableName} WHERE id = ?`);
      writeResult = stmt.run(data.id);
    }

    // 3. Record + broadcast
    const envelope = recordLocalWrite(db, operation, tableName, data, peerId);
    if (broadcastFn && typeof broadcastFn === 'function') {
      await broadcastFn(envelope);
    }

    logger.db(`Fast-path ${operation} on ${tableName} committed successfully`);

    return { success: true, rowId: writeResult?.lastInsertRowid };
  } catch (err) {
    logger.error(`Error executing ${operation} on ${tableName}: ${err.message}`);
    return { success: false, errors: [err.message] };
  }
}
