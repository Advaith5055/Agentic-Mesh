import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { initDatabase } from '../src/db/sqlite.js';
import { 
  initSync, 
  getVectorClock, 
  recordLocalWrite, 
  applyRemoteTransaction, 
  handleSyncRequest,
  createSyncEngine 
} from '../src/db/sync.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

describe('Sync Engine & Mesh Replication', () => {
  let tmpDir;
  let db;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'mesh-sync-test-'));
    db = initDatabase(join(tmpDir, 'sync-test.db'));
    initSync('peer-local');
  });

  afterEach(() => {
    try {
      db.close();
      rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  });

  it('records local writes in _mesh_log and advances vector clock', () => {
    const clockBefore = getVectorClock().toJSON()['peer-local'] || 0;
    
    const envelope = recordLocalWrite(db, 'INSERT', 'items', {
      category_id: 1,
      name: 'Sync Item 1',
      price: 19.99,
      sku: 'SKU-SYNC-001'
    }, 'peer-local');

    assert.ok(envelope.id);
    assert.equal(envelope.type, 'TRANSACTION');
    assert.equal(envelope.sender, 'peer-local');
    
    const clockAfter = getVectorClock().toJSON()['peer-local'];
    assert.equal(clockAfter, clockBefore + 1);

    // Verify row in _mesh_log
    const log = db.prepare('SELECT * FROM _mesh_log WHERE id = ?').get(envelope.payload.id);
    assert.ok(log);
    assert.equal(log.operation, 'INSERT');
    assert.equal(log.table_name, 'items');
  });

  it('applies a valid remote transaction to local DB and _mesh_log', () => {
    const remoteEnvelope = {
      payload: {
        id: 'remote-tx-100',
        operation: 'INSERT',
        tableName: 'items',
        rowData: JSON.stringify({
          category_id: 2,
          name: 'Remote Distributed Book',
          price: 39.99,
          sku: 'SKU-REMOTE-001'
        }),
        vectorClock: JSON.stringify({ 'peer-remote': 1 }),
        peerId: 'peer-remote'
      }
    };

    const result = applyRemoteTransaction(db, remoteEnvelope, 'peer-local');
    assert.equal(result.applied, true);

    // Verify written to items table
    const item = db.prepare('SELECT * FROM items WHERE sku = ?').get('SKU-REMOTE-001');
    assert.ok(item);
    assert.equal(item.name, 'Remote Distributed Book');

    // Verify logged in _mesh_log
    const log = db.prepare('SELECT * FROM _mesh_log WHERE id = ?').get('remote-tx-100');
    assert.ok(log);

    // Verify vector clock merged
    assert.equal(getVectorClock().toJSON()['peer-remote'], 1);
  });

  it('deduplicates already applied remote transactions exactly once', () => {
    const remoteEnvelope = {
      payload: {
        id: 'remote-tx-dup-test',
        operation: 'INSERT',
        tableName: 'items',
        rowData: JSON.stringify({
          category_id: 1,
          name: 'Unique Item',
          price: 25.00,
          sku: 'SKU-DUP-001'
        }),
        vectorClock: JSON.stringify({ 'peer-remote': 2 }),
        peerId: 'peer-remote'
      }
    };

    const firstRun = applyRemoteTransaction(db, remoteEnvelope, 'peer-local');
    assert.equal(firstRun.applied, true);

    // Second run with the same transaction ID
    const secondRun = applyRemoteTransaction(db, remoteEnvelope, 'peer-local');
    assert.equal(secondRun.applied, false);
    assert.equal(secondRun.duplicate, true);
    assert.equal(secondRun.reason, 'Already applied');

    // Verify items count only increased by 1
    const count = db.prepare('SELECT COUNT(*) as count FROM items WHERE sku = ?').get('SKU-DUP-001').count;
    assert.equal(count, 1);
  });

  it('detects and rejects conflicting remote transactions violating constraints', () => {
    const badEnvelope = {
      payload: {
        id: 'remote-tx-bad',
        operation: 'INSERT',
        tableName: 'items',
        rowData: JSON.stringify({
          category_id: 9999, // Foreign key violation
          name: 'Invalid Item',
          price: -5.00,       // Negative price violation
          sku: 'SKU-BAD'
        }),
        vectorClock: JSON.stringify({ 'peer-remote': 3 }),
        peerId: 'peer-remote'
      }
    };

    const result = applyRemoteTransaction(db, badEnvelope, 'peer-local');
    assert.equal(result.applied, false);
    assert.equal(result.conflict, true);
    assert.ok(result.errors.length > 0);
  });

  it('handles sync request delta selection correctly', () => {
    // Write 3 transactions from peer-local
    recordLocalWrite(db, 'INSERT', 'items', {
      category_id: 1, name: 'Item A', price: 10, sku: 'SKU-A'
    }, 'peer-local');

    recordLocalWrite(db, 'INSERT', 'items', {
      category_id: 1, name: 'Item B', price: 20, sku: 'SKU-B'
    }, 'peer-local');

    recordLocalWrite(db, 'INSERT', 'items', {
      category_id: 1, name: 'Item C', price: 30, sku: 'SKU-C'
    }, 'peer-local');

    // Case 1: Remote peer has seen 0 operations -> needs all 3
    const deltaEmpty = handleSyncRequest(db, { 'peer-local': 0 });
    assert.equal(deltaEmpty.length, 3);

    // Case 2: Remote peer has seen 2 operations -> only needs the 3rd
    const deltaPartial = handleSyncRequest(db, { 'peer-local': 2 });
    assert.equal(deltaPartial.length, 1);
    assert.ok(deltaPartial[0].row_data.includes('SKU-C'));

    // Case 3: Remote peer is completely up-to-date -> needs 0
    const deltaCaughtUp = handleSyncRequest(db, { 'peer-local': 3 });
    assert.equal(deltaCaughtUp.length, 0);
  });

  it('supports multiple independent SyncEngine instances in memory without cross-talk', () => {
    const dbB = initDatabase(join(tmpDir, 'sync-test-b.db'));
    try {
      const engineA = createSyncEngine('node-alpha');
      const engineB = createSyncEngine('node-beta');

      assert.equal(engineA.getVectorClock().toJSON()['node-alpha'], 0);
      assert.equal(engineB.getVectorClock().toJSON()['node-beta'], 0);
      assert.equal(engineA.getVectorClock().toJSON()['node-beta'], undefined);

      const envA = engineA.recordLocalWrite(db, 'INSERT', 'categories', {
        name: 'Alpha Cat',
        description: 'Alpha category'
      });

      assert.equal(engineA.getVectorClock().toJSON()['node-alpha'], 1);
      assert.equal(engineB.getVectorClock().toJSON()['node-alpha'], undefined);

      // Apply A's transaction to engine B (which has dbB)
      const result = engineB.applyRemoteTransaction(dbB, envA);
      assert.equal(result.applied, true);
      assert.equal(engineB.getVectorClock().toJSON()['node-alpha'], 1);
      assert.equal(engineB.getVectorClock().toJSON()['node-beta'], 0);
    } finally {
      dbB.close();
    }
  });
});
