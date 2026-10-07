/**
 * @fileoverview Canonical safe mutation service for SQLite and Mesh Log.
 * Guarantees atomicity: schema validation, SQL write, and mesh log recording
 * all occur within a single SQLite transaction.
 * @module db/mutation
 */

import { validateAllowlist, validateTransaction } from './schema-validator.js';
import { insertCategory, insertItem } from './sqlite.js';
import { recordLocalWrite, getVectorClock } from './sync.js';
import { checkOriginPolicy } from './policy.js';

/**
 * Executes a raw SQL write for a validated operation on the database.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {string} operation - Operation: 'INSERT', 'UPDATE', or 'DELETE'.
 * @param {string} tableName - Target table name.
 * @param {Object} data - Row data.
 * @returns {Object} { lastInsertRowid, changes }
 */
export function executeRawWrite(db, operation, tableName, data) {
  let stmt;
  if (tableName === 'categories' && operation === 'INSERT') {
    return insertCategory(db, data);
  }
  if (tableName === 'items' && operation === 'INSERT') {
    return insertItem(db, data);
  }
  if (operation === 'INSERT') {
    const keys = Object.keys(data);
    const placeholders = keys.map(() => '?').join(', ');
    stmt = db.prepare(`INSERT INTO ${tableName} (${keys.join(', ')}) VALUES (${placeholders})`);
    return stmt.run(...Object.values(data));
  }
  if (operation === 'UPDATE') {
    if (tableName === 'item_suppliers') {
      stmt = db.prepare('UPDATE item_suppliers SET supplier_id = ? WHERE item_id = ? AND supplier_id = ?');
      return stmt.run(data.supplier_id, data.item_id, data.supplier_id);
    }
    const keys = Object.keys(data).filter(k => k !== 'id');
    const setClause = keys.map(k => `${k} = ?`).join(', ');
    stmt = db.prepare(`UPDATE ${tableName} SET ${setClause} WHERE id = ?`);
    return stmt.run(...keys.map(k => data[k]), data.id);
  }
  if (operation === 'DELETE') {
    if (tableName === 'item_suppliers') {
      stmt = db.prepare('DELETE FROM item_suppliers WHERE item_id = ? AND supplier_id = ?');
      return stmt.run(data.item_id, data.supplier_id);
    }
    stmt = db.prepare(`DELETE FROM ${tableName} WHERE id = ?`);
    return stmt.run(data.id);
  }
  throw new Error(`Unsupported operation: ${operation}`);
}

const REF_RE = /^\$ref:(\d+)$/;

/**
 * Replaces "$ref:N" values with the row id created by operation N of the same batch,
 * so a plan can create a category and then add items to it.
 *
 * @param {Object} data - Operation data.
 * @param {Array<number|undefined>} createdIds - Row id created by each earlier operation.
 * @param {number} index - Index of the operation being resolved.
 * @returns {{ data: Object, error?: string }}
 */
export function resolveRefs(data, createdIds, index) {
  if (!data || typeof data !== 'object') return { data };
  const resolved = {};
  for (const [key, value] of Object.entries(data)) {
    const match = typeof value === 'string' ? REF_RE.exec(value.trim()) : null;
    if (!match) {
      resolved[key] = value;
      continue;
    }
    const target = Number(match[1]);
    if (target >= index || createdIds[target] === undefined) {
      return { data, error: `'${key}' refers to $ref:${target}, which is not an earlier INSERT in this plan` };
    }
    resolved[key] = Number(createdIds[target]);
  }
  return { data: resolved };
}

/**
 * Validates a batch against the origin policy, table/column allowlists and schema rules
 * without writing anything. Operations are rehearsed in order, so "$ref:N" values and
 * dependencies on earlier operations are checked exactly as they will execute.
 *
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {Array<Object>} mutations - Array of { operation, table/tableName, data }.
 * @param {Object} [meta={}] - Provenance: { origin, evidenceId, producerPeerId }.
 * @returns {{ valid: boolean, errors: string[] }}
 */
export function validateMutationBatch(db, mutations, meta = {}) {
  if (!Array.isArray(mutations)) {
    return { valid: false, errors: ['Mutations must be an array.'] };
  }

  const policy = checkOriginPolicy(meta.origin, mutations, {
    evidenceId: meta.evidenceId,
    producerPeerId: meta.producerPeerId,
    db
  });
  if (!policy.valid) {
    return { valid: false, errors: policy.errors };
  }

  // Rehearse the batch in order inside a transaction that is always rolled back, so a
  // later operation can depend on an earlier one (e.g. create a category, then add an
  // item to it). Nothing is ever committed here.
  const ROLLBACK = Symbol('rollback');
  let failure = null;
  const createdIds = [];
  try {
    db.transaction(() => {
      for (let i = 0; i < mutations.length; i++) {
        const op = mutations[i];
        const targetTable = op.table || op.tableName;
        const label = `Operation ${i} (${op.operation} on ${targetTable})`;
        const allowCheck = validateAllowlist(targetTable, op.data ? Object.keys(op.data) : []);
        if (!allowCheck.valid) {
          failure = [`${label}: ${allowCheck.error}`];
          throw ROLLBACK;
        }

        const ref = resolveRefs(op.data, createdIds, i);
        if (ref.error) {
          failure = [`${label}: ${ref.error}`];
          throw ROLLBACK;
        }

        const validation = validateTransaction(op.operation, targetTable, ref.data, db);
        if (!validation.valid) {
          failure = [`${label}: ${validation.errors.join(', ')}`];
          throw ROLLBACK;
        }

        try {
          const result = executeRawWrite(db, op.operation, targetTable, ref.data);
          createdIds[i] = op.operation === 'INSERT' ? result?.lastInsertRowid : undefined;
        } catch (err) {
          failure = [`${label}: ${err.message}`];
          throw ROLLBACK;
        }
      }
      throw ROLLBACK;
    })();
  } catch (err) {
    if (err !== ROLLBACK) return { valid: false, errors: [err.message] };
  }

  return failure ? { valid: false, errors: failure } : { valid: true, errors: [] };
}

/**
 * Executes a single mutation atomically inside a SQLite transaction.
 * Validates allowlist & schema, applies write, records in _mesh_log, advances vector clock.
 * If anything fails, rolls back completely.
 *
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {import('./sync.js').SyncEngine|Object} [syncEngine] - The sync engine.
 * @param {Object} mutation - The mutation descriptor { operation, table (or tableName), data }.
 * @param {Object} [meta={}] - Provenance: { origin, evidenceId }.
 * @returns {{ success: boolean, rowId?: number, envelope?: Object, errors?: string[] }}
 */
export function executeMutation(db, syncEngine, mutation, meta = {}) {
  if (!mutation || typeof mutation !== 'object') {
    return { success: false, errors: ['Mutation descriptor must be an object.'] };
  }

  const operation = mutation.operation;
  const tableName = mutation.table || mutation.tableName;
  const data = mutation.data;

  if (!operation || !tableName || !data) {
    return { success: false, errors: ['Mutation must include operation, table (or tableName), and data.'] };
  }

  // 0. Origin policy
  const policy = checkOriginPolicy(meta.origin, [{ operation, table: tableName, data }], {
    evidenceId: meta.evidenceId,
    producerPeerId: syncEngine?.peerId,
    db
  });
  if (!policy.valid) {
    return { success: false, errors: policy.errors };
  }

  // 1. Allowlist validation
  const allowCheck = validateAllowlist(tableName, Object.keys(data));
  if (!allowCheck.valid) {
    return { success: false, errors: [allowCheck.error] };
  }

  // 2. Schema validation
  const validation = validateTransaction(operation, tableName, data, db);
  if (!validation.valid) {
    return { success: false, errors: validation.errors };
  }

  // Clock rollback safety in case of SQLite error
  const engine = syncEngine || {
    recordLocalWrite: (d, op, tbl, dt, m) => recordLocalWrite(d, op, tbl, dt, 'local', m),
    getVectorClock: () => getVectorClock()
  };
  const clock = engine.getVectorClock ? engine.getVectorClock() : null;
  const clockBackup = clock ? { ...clock.clock } : null;

  try {
    let rowId;
    let envelope;

    db.transaction(() => {
      // 3. Raw SQL write
      const writeResult = executeRawWrite(db, operation, tableName, data);
      rowId = writeResult?.lastInsertRowid;

      // 4. Mesh log entry & envelope creation
      envelope = engine.recordLocalWrite(db, operation, tableName, data, meta);
    })();

    return {
      success: true,
      rowId,
      envelope,
      errors: []
    };
  } catch (err) {
    // Restore clock if transaction failed
    if (clock && clockBackup) {
      clock.clock = clockBackup;
    }
    return {
      success: false,
      errors: [err.message]
    };
  }
}

/**
 * Executes an array of mutations atomically inside a single SQLite transaction.
 * If any mutation fails validation or execution, the entire batch is rolled back.
 * 
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {import('./sync.js').SyncEngine|Object} [syncEngine] - The sync engine.
 * @param {Array<Object>} mutations - Array of { operation, table/tableName, data }.
 * @param {Object} [meta={}] - Provenance: { origin, evidenceId }.
 * @returns {{ success: boolean, completed: number, failed: number, results: Array<Object>, envelopes: Array<Object>, errors?: string[] }}
 */
export function executeMutationBatch(db, syncEngine, mutations, meta = {}) {
  if (!Array.isArray(mutations)) {
    return {
      success: false,
      completed: 0,
      failed: 1,
      results: [],
      envelopes: [],
      errors: ['Mutations must be an array.']
    };
  }

  if (mutations.length === 0) {
    return {
      success: true,
      completed: 0,
      failed: 0,
      results: [],
      envelopes: [],
      errors: []
    };
  }

  // 1. Pre-validate origin policy, allowlist and schema for all mutations
  const check = validateMutationBatch(db, mutations, { ...meta, producerPeerId: syncEngine?.peerId });
  if (!check.valid) {
    return {
      success: false,
      completed: 0,
      failed: mutations.length,
      results: [],
      envelopes: [],
      errors: check.errors
    };
  }

  // 2. Execute all writes and mesh logging inside a single SQLite transaction
  const engine = syncEngine || {
    recordLocalWrite: (d, op, tbl, dt, m) => recordLocalWrite(d, op, tbl, dt, 'local', m),
    getVectorClock: () => getVectorClock()
  };
  const clock = engine.getVectorClock ? engine.getVectorClock() : null;
  const clockBackup = clock ? { ...clock.clock } : null;

  const results = [];
  const envelopes = [];

  const createdIds = [];
  try {
    db.transaction(() => {
      mutations.forEach((op, i) => {
        const targetTable = op.table || op.tableName;
        const ref = resolveRefs(op.data, createdIds, i);
        if (ref.error) throw new Error(`Operation ${i}: ${ref.error}`);
        const writeResult = executeRawWrite(db, op.operation, targetTable, ref.data);
        createdIds[i] = op.operation === 'INSERT' ? writeResult?.lastInsertRowid : undefined;
        // Peers receive the resolved values, never "$ref:N" placeholders.
        const envelope = engine.recordLocalWrite(db, op.operation, targetTable, ref.data, meta);

        results.push({
          operation: { ...op, data: ref.data },
          success: true,
          rowId: writeResult?.lastInsertRowid
        });
        envelopes.push(envelope);
      });
    })();

    return {
      success: true,
      completed: mutations.length,
      failed: 0,
      results,
      envelopes,
      errors: []
    };
  } catch (err) {
    // Restore clock
    if (clock && clockBackup) {
      clock.clock = clockBackup;
    }
    return {
      success: false,
      completed: 0,
      failed: mutations.length,
      results: [],
      envelopes: [],
      errors: [err.message]
    };
  }
}
