import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { initDatabase } from '../src/db/sqlite.js';
import { createSyncEngine } from '../src/db/sync.js';
import { executeMutation, executeMutationBatch } from '../src/db/mutation.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

describe('Safe Mutation Service', () => {
  let tmpDir;
  let db;
  let syncEngine;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'mesh-mutation-test-'));
    db = initDatabase(join(tmpDir, 'mutation-test.db'));
    syncEngine = createSyncEngine('node-test');
  });

  afterEach(() => {
    try {
      db.close();
      rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  });

  it('executes a single valid mutation and logs to _mesh_log atomically', () => {
    const result = executeMutation(db, syncEngine, {
      operation: 'INSERT',
      table: 'items',
      data: {
        category_id: 1,
        name: 'Atomic Mouse',
        price: 34.50,
        sku: 'SKU-ATOMIC-01'
      }
    });

    assert.equal(result.success, true);
    assert.ok(result.rowId);
    assert.ok(result.envelope);
    assert.equal(result.envelope.payload.operation, 'INSERT');

    // Verify row exists in items table
    const item = db.prepare('SELECT * FROM items WHERE sku = ?').get('SKU-ATOMIC-01');
    assert.ok(item);
    assert.equal(item.name, 'Atomic Mouse');

    // Verify row exists in _mesh_log
    const log = db.prepare('SELECT * FROM _mesh_log WHERE id = ?').get(result.envelope.payload.id);
    assert.ok(log);
    assert.equal(log.table_name, 'items');

    // Vector clock advanced
    assert.equal(syncEngine.getVectorClock().toJSON()['node-test'], 1);
  });

  it('rolls back completely when validation fails and does not modify database or mesh log', () => {
    const clockBefore = syncEngine.getVectorClock().toJSON()['node-test'] || 0;

    const result = executeMutation(db, syncEngine, {
      operation: 'INSERT',
      table: 'items',
      data: {
        category_id: 99999, // Nonexistent FK
        name: 'Invalid Item',
        price: 50.00,
        sku: 'SKU-FAIL-01'
      }
    });

    assert.equal(result.success, false);
    assert.ok(result.errors.length > 0);

    // Database unchanged
    const item = db.prepare('SELECT * FROM items WHERE sku = ?').get('SKU-FAIL-01');
    assert.equal(item, undefined);

    // No mesh log added
    const logs = db.prepare("SELECT * FROM _mesh_log WHERE row_data LIKE '%SKU-FAIL-01%'").all();
    assert.equal(logs.length, 0);

    // Vector clock was not incremented
    assert.equal(syncEngine.getVectorClock().toJSON()['node-test'] || 0, clockBefore);
  });

  it('executes atomic batch successfully when all operations are valid', () => {
    const ops = [
      {
        operation: 'INSERT',
        table: 'categories',
        data: { name: 'Gaming', description: 'Gaming gear' }
      },
      {
        operation: 'INSERT',
        table: 'items',
        data: { category_id: 1, name: 'Gaming Headset', price: 99.99, sku: 'SKU-GAME-01' }
      }
    ];

    const result = executeMutationBatch(db, syncEngine, ops);
    assert.equal(result.success, true);
    assert.equal(result.completed, 2);
    assert.equal(result.failed, 0);
    assert.equal(result.envelopes.length, 2);

    const cat = db.prepare("SELECT * FROM categories WHERE name = 'Gaming'").get();
    assert.ok(cat);
    const itm = db.prepare("SELECT * FROM items WHERE sku = 'SKU-GAME-01'").get();
    assert.ok(itm);
    assert.equal(syncEngine.getVectorClock().toJSON()['node-test'], 2);
  });

  it('atomically rolls back entire batch if any operation in the batch fails validation', () => {
    const clockBefore = syncEngine.getVectorClock().toJSON()['node-test'] || 0;

    const ops = [
      {
        operation: 'INSERT',
        table: 'categories',
        data: { name: 'Outdoor', description: 'Outdoor gear' }
      },
      {
        operation: 'INSERT',
        table: 'items',
        data: { category_id: 9999, name: 'Tent', price: 199.99, sku: 'SKU-TENT-01' } // Bad category_id
      }
    ];

    const result = executeMutationBatch(db, syncEngine, ops);
    assert.equal(result.success, false);
    assert.equal(result.completed, 0);
    assert.equal(result.failed, 2);

    // First operation must NOT have been committed!
    const cat = db.prepare("SELECT * FROM categories WHERE name = 'Outdoor'").get();
    assert.equal(cat, undefined);

    // Vector clock unchanged
    assert.equal(syncEngine.getVectorClock().toJSON()['node-test'] || 0, clockBefore);
  });

  it('handles composite primary key table item_suppliers for DELETE and UPDATE', () => {
    // Initial seeded link: item_id 1, supplier_id 1 (Laptop -> TechCorp)
    const existing = db.prepare('SELECT 1 FROM item_suppliers WHERE item_id = 1 AND supplier_id = 1').get();
    assert.ok(existing);

    // DELETE on item_suppliers
    const delResult = executeMutation(db, syncEngine, {
      operation: 'DELETE',
      table: 'item_suppliers',
      data: { item_id: 1, supplier_id: 1 }
    });
    assert.equal(delResult.success, true);

    const afterDel = db.prepare('SELECT 1 FROM item_suppliers WHERE item_id = 1 AND supplier_id = 1').get();
    assert.equal(afterDel, undefined);
  });
});
