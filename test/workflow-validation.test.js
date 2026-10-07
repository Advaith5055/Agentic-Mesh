import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { initDatabase } from '../src/db/sqlite.js';
import { validateTransaction, validateAllowlist, ALLOWED_TABLES, ALLOWED_COLUMNS } from '../src/db/schema-validator.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

describe('Workflow Platform Allowlist & Schema Validation', () => {
  let tmpDir;
  let db;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'mesh-wf-val-'));
    db = initDatabase(join(tmpDir, 'val-test.db'));
  });

  afterEach(() => {
    try {
      db.close();
      rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  });

  it('contains all 10 workflow tables in ALLOWED_TABLES and defines column allowlists', () => {
    const required = [
      'mesh_nodes',
      'node_capabilities',
      'tasks',
      'task_runs',
      'task_steps',
      'artifacts',
      'approvals',
      'audit_events',
      'conflicts',
      'replication_metrics'
    ];

    for (const table of required) {
      assert.ok(ALLOWED_TABLES.has(table), `ALLOWED_TABLES must contain ${table}`);
      assert.ok(ALLOWED_COLUMNS[table] instanceof Set, `ALLOWED_COLUMNS[${table}] must be a Set`);
      assert.ok(ALLOWED_COLUMNS[table].size >= 3, `${table} must have at least 3 allowed columns`);
    }
  });

  it('rejects forbidden columns and unknown tables strictly', () => {
    const badTable = validateAllowlist('unauthorized_admin_table', ['id', 'name']);
    assert.equal(badTable.valid, false);
    assert.ok(badTable.error.includes("is not in allowed tables list"));

    const badCol = validateAllowlist('tasks', ['id', 'title', 'malicious_sql_payload']);
    assert.equal(badCol.valid, false);
    assert.ok(badCol.error.includes("is not allowed on table 'tasks'"));
  });

  it('validates valid task insertion successfully', () => {
    const taskData = {
      id: 'task-test-1',
      title: 'Analyze Vector Clocks',
      user_prompt: 'Run verification across connected peers',
      task_type: 'analysis',
      priority: 'high',
      status: 'queued',
      requested_by: 'tester'
    };

    const res = validateTransaction('INSERT', 'tasks', taskData, db);
    assert.equal(res.valid, true);
    assert.equal(res.errors.length, 0);
  });

  it('rejects task with invalid status check constraint', () => {
    const taskData = {
      id: 'task-test-invalid',
      title: 'Invalid Task',
      user_prompt: 'Bad status test',
      status: 'unauthorized_status'
    };

    const res = validateTransaction('INSERT', 'tasks', taskData, db);
    assert.equal(res.valid, false);
    assert.ok(res.errors.some(e => e.includes('status must be one of')));
  });

  it('rejects foreign key violation for nonexistent task_id on artifacts', () => {
    const artifactData = {
      id: 'art-orphan-1',
      task_id: 'nonexistent-task-999',
      name: 'Orphan Artifact',
      artifact_type: 'report/json',
      content: '{"data":1}'
    };

    const res = validateTransaction('INSERT', 'artifacts', artifactData, db);
    assert.equal(res.valid, false);
    assert.ok(res.errors.some(e => e.includes('Foreign key violation')));
  });

  it('rejects foreign key violation for nonexistent node_id on node_capabilities', () => {
    const capData = {
      id: 'cap-orphan-1',
      node_id: 'nonexistent-node-999',
      models: 'test-model',
      max_concurrency: 2
    };

    const res = validateTransaction('INSERT', 'node_capabilities', capData, db);
    assert.equal(res.valid, false);
    assert.ok(res.errors.some(e => e.includes('Foreign key violation')));
  });

  it('enforces check constraint on node_capabilities max_concurrency >= 1', () => {
    const capData = {
      id: 'cap-bad-concurrency',
      node_id: 'node-delta', // exists from seed
      max_concurrency: 0
    };

    const res = validateTransaction('INSERT', 'node_capabilities', capData, db);
    assert.equal(res.valid, false);
    assert.ok(res.errors.some(e => e.includes('max_concurrency must be at least 1')));
  });

  it('enforces check constraint on replication_metrics latency_ms >= 0', () => {
    const repData = {
      id: 'rep-negative',
      source_peer_id: 'node-1',
      target_peer_id: 'node-2',
      latency_ms: -5.5,
      status: 'success'
    };

    const res = validateTransaction('INSERT', 'replication_metrics', repData, db);
    assert.equal(res.valid, false);
    assert.ok(res.errors.some(e => e.includes('latency_ms must be non-negative')));
  });

  it('enforces unique peer_id constraint on mesh_nodes', () => {
    // Ensure an initial node with peer_id exists
    db.prepare(`
      INSERT OR IGNORE INTO mesh_nodes (id, name, role, peer_id, status)
      VALUES (?, ?, ?, ?, ?)
    `).run('node-existing-peer', 'Existing Node', 'planner', '12D3KooW-DELTA-PLANNER', 'active');

    const nodeData = {
      id: 'node-duplicate-peer',
      name: 'Duplicate Node',
      role: 'planner',
      peer_id: '12D3KooW-DELTA-PLANNER'
    };

    const res = validateTransaction('INSERT', 'mesh_nodes', nodeData, db);
    assert.equal(res.valid, false);
    assert.ok(res.errors.some(e => e.includes('Unique constraint violation')));
  });

  it('validates UPDATE and DELETE on workflow tables requiring primary key id', () => {
    // Delete requires valid id that exists in DB
    const delResNonexistent = validateTransaction('DELETE', 'tasks', { id: 'nonexistent-task-id' }, db);
    assert.equal(delResNonexistent.valid, false);
    assert.ok(delResNonexistent.errors.some(e => e.includes('Row to delete not found')));

    const delResMissing = validateTransaction('DELETE', 'tasks', {}, db);
    assert.equal(delResMissing.valid, false);
    assert.ok(delResMissing.errors.some(e => e.includes('DELETE requires an identifier')));

    // Update requires valid id
    const updateResMissing = validateTransaction('UPDATE', 'tasks', { title: 'New Title' }, db);
    assert.equal(updateResMissing.valid, false);
    assert.ok(updateResMissing.errors.some(e => e.includes('UPDATE requires an id field')));
  });
});
