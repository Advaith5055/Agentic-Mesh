import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { initDatabase, getReplicationMetrics, getConflicts } from '../src/db/sqlite.js';
import { createSyncEngine } from '../src/db/sync.js';
import {
  recordConflict,
  resolveConflict,
  recordReplicationMetric
} from '../src/mesh/workflow-service.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

describe('Workflow Platform Replication, Vector Clocks & Conflict Resolution', () => {
  let tmpDir;
  let db;
  let syncEngine;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'mesh-wf-rep-'));
    db = initDatabase(join(tmpDir, 'rep-test.db'));
    syncEngine = createSyncEngine('node-replica-1');
  });

  afterEach(() => {
    try {
      db.close();
      rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  });

  it('records replication metrics with latency and vector clock metadata', () => {
    const metric = recordReplicationMetric(db, {
      source_peer_id: 'node-replica-1',
      target_peer_id: 'node-replica-2',
      latency_ms: 5.4,
      status: 'success',
      vector_clock_meta: { 'node-replica-1': 12 },
      bytes_transferred: 4096
    });

    assert.ok(metric.id);
    assert.equal(metric.latency_ms, 5.4);
    assert.equal(metric.status, 'success');

    const metricsList = getReplicationMetrics(db);
    assert.ok(metricsList.some(m => m.id === metric.id));
  });

  it('detects, records, and resolves replication conflicts with audit tracking', () => {
    // Record conflict
    const conflict = recordConflict(db, {
      conflict_type: 'concurrent_task_assignment',
      affected_entity_type: 'tasks',
      affected_entity_id: 'task-102',
      peer_id: 'node-replica-2',
      details: { local_assigned: 'node-alpha', remote_assigned: 'node-gamma' }
    });

    assert.equal(conflict.resolution_state, 'detected');
    assert.equal(conflict.conflict_type, 'concurrent_task_assignment');

    const detectedList = getConflicts(db, 'detected');
    assert.ok(detectedList.some(c => c.id === conflict.id));

    // Resolve conflict
    const resolved = resolveConflict(db, conflict.id, {
      chosen_resolution: 'Assigned node-alpha based on lower peer ID hash.',
      resolved_by: 'lead_operator'
    });

    assert.equal(resolved.resolution_state, 'resolved');
    assert.ok(resolved.resolved_at);

    // Verify audit event
    const audit = db.prepare("SELECT * FROM audit_events WHERE entity_id = ? AND action = 'CONFLICT_RESOLVED'").get(conflict.id);
    assert.ok(audit, 'Audit event must be logged on conflict resolution');
    assert.equal(audit.actor, 'lead_operator');
  });

  it('records write operations in _mesh_log and advances vector clock causally', () => {
    const clockBefore = syncEngine.getVectorClock().toJSON();
    const seqBefore = clockBefore['node-replica-1'] || 0;

    const envelope = syncEngine.recordLocalWrite(
      db,
      'INSERT',
      'tasks',
      { id: 'task-sync-1', title: 'Replicated Task', user_prompt: 'Test sync', status: 'queued' }
    );

    assert.ok(envelope);
    assert.equal(envelope.type, 'TRANSACTION');

    const clockAfter = syncEngine.getVectorClock().toJSON();
    const seqAfter = clockAfter['node-replica-1'] || 0;
    assert.equal(seqAfter, seqBefore + 1, 'Vector clock sequence must increment on local write');

    const logRow = db.prepare('SELECT * FROM _mesh_log WHERE id = ?').get(envelope.payload.id);
    assert.ok(logRow, 'Operation must be logged to _mesh_log');
    assert.equal(logRow.table_name, 'tasks');
  });
});
