import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { initDatabase } from '../src/db/sqlite.js';
import { createSyncEngine } from '../src/db/sync.js';
import { startWebSocketServer } from '../src/websocket/server.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

describe('Workflow Platform REST API Contracts', () => {
  let tmpDir;
  let db;
  let syncEngine;
  let serverInstance;
  let port;
  let baseUrl;

  beforeEach(async () => {
    tmpDir = mkdtempSync(join(tmpdir(), 'mesh-wf-api-'));
    db = initDatabase(join(tmpDir, 'api-test.db'), { seedDemoData: true });
    syncEngine = createSyncEngine('node-test-api');

    port = Math.floor(21000 + Math.random() * 25000);
    baseUrl = `http://127.0.0.1:${port}`;

    serverInstance = startWebSocketServer(port, {
      node: { peerId: { toString: () => 'node-test-api' } },
      db,
      syncEngine,
      peerRegistry: { getPeerCount: () => 1, toJSON: () => [] },
      apiKey: 'mesh-dev-key',
      localNodeId: 'node-test-api',
      localNodeRole: 'peer'
    });

    await new Promise(r => setTimeout(r, 100));
  });

  afterEach(async () => {
    try {
      if (serverInstance?.server) {
        serverInstance.server.close();
      }
      db.close();
      rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  });

  it('GET /api/nodes returns registered mesh nodes and capabilities', async () => {
    const res = await fetch(`${baseUrl}/api/nodes`);
    assert.equal(res.status, 200);
    const nodes = await res.json();
    assert.ok(Array.isArray(nodes));
    assert.ok(nodes.length >= 1, 'Should contain seeded nodes');
    assert.ok(nodes[0].name);
    assert.ok(nodes[0].role);
  });

  it('POST /api/nodes/register registers a new node and capabilities', async () => {
    const res = await fetch(`${baseUrl}/api/nodes/register`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': 'mesh-dev-key' },
      body: JSON.stringify({
        node: {
          id: 'node-omega',
          name: 'omega-synthesizer',
          role: 'planner',
          peer_id: '12D3KooW-OMEGA-PEER',
          status: 'active'
        },
        capabilities: {
          models: 'deepseek-coder:6.7b',
          tools: 'planOperations',
          max_concurrency: 4
        }
      })
    });

    assert.equal(res.status, 200);
    const data = await res.json();
    assert.equal(data.success, true);
    assert.equal(data.node.name, 'omega-synthesizer');
  });

  it('handles task lifecycle via REST: create -> plan -> execute', async () => {
    // 1. Create task
    const createRes = await fetch(`${baseUrl}/api/tasks`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': 'mesh-dev-key' },
      body: JSON.stringify({
        title: 'REST API Lifecycle Task',
        user_prompt: 'Process and verify ledger consistency',
        task_type: 'verification',
        priority: 'low'
      })
    });

    assert.equal(createRes.status, 201);
    const createData = await createRes.json();
    const taskId = createData.task.id;
    assert.equal(createData.task.status, 'queued');

    // 2. Plan task
    const planRes = await fetch(`${baseUrl}/api/tasks/${taskId}/plan`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': 'mesh-dev-key' },
      body: JSON.stringify({ plannerNodeId: 'node-delta', riskLevel: 'low' })
    });

    assert.equal(planRes.status, 200);
    const planData = await planRes.json();
    assert.equal(planData.task.status, 'planned');

    // 3. Execute task
    const execRes = await fetch(`${baseUrl}/api/tasks/${taskId}/execute`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': 'mesh-dev-key' },
      body: JSON.stringify({ executorNodeId: 'node-alpha' })
    });

    assert.equal(execRes.status, 200);
    const execData = await execRes.json();
    assert.equal(execData.success, true);
    assert.equal(execData.status, 'completed');

    // 4. Verify task details endpoint
    const detailsRes = await fetch(`${baseUrl}/api/tasks/${taskId}`);
    assert.equal(detailsRes.status, 200);
    const details = await detailsRes.json();
    assert.equal(details.id, taskId);
    assert.equal(details.status, 'completed');
    assert.ok(details.steps.length >= 3);
    assert.ok(details.artifacts.length >= 1);
    assert.ok(details.audit_events.length >= 2);
  });

  it('GET /api/approvals and POST /api/approvals/:id/decide enforce approval flow', async () => {
    // Seeded task-103 is awaiting approval
    const apprRes = await fetch(`${baseUrl}/api/approvals`);
    assert.equal(apprRes.status, 200);
    const approvals = await apprRes.json();
    assert.ok(Array.isArray(approvals));
    const pending = approvals.find(a => a.decision === 'pending');
    assert.ok(pending, 'Expected a pending approval in seeded data');

    // Submit decision
    const decideRes = await fetch(`${baseUrl}/api/approvals/${pending.id}/decide`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': 'mesh-dev-key' },
      body: JSON.stringify({
        decision: 'approved',
        reviewer: 'compliance_officer',
        reason: 'Authorized for production'
      })
    });

    assert.equal(decideRes.status, 200);
    const decideData = await decideRes.json();
    assert.equal(decideData.approval.decision, 'approved');
  });

  it('GET /api/metrics/operational returns aggregated operational metrics', async () => {
    const res = await fetch(`${baseUrl}/api/metrics/operational`);
    assert.equal(res.status, 200);
    const metrics = await res.json();
    assert.ok(metrics.activeNodes >= 1);
    assert.ok(typeof metrics.taskSuccessRate === 'number');
    assert.ok(typeof metrics.avgExecutionTimeMs === 'number');
    assert.ok(typeof metrics.replicationLatencyMs === 'number');
    assert.ok(typeof metrics.failedValidationsCount === 'number');
    assert.ok(metrics.tasksTotal >= 1);
  });

  it('POST /api/conflicts/:id/resolve resolves replication conflicts via API', async () => {
    // conflict conf-1 exists from seed or we can create one
    const listRes = await fetch(`${baseUrl}/api/conflicts`);
    assert.equal(listRes.status, 200);
    const conflicts = await listRes.json();
    assert.ok(Array.isArray(conflicts));

    // Resolve conf-1
    const resolveRes = await fetch(`${baseUrl}/api/conflicts/conf-1/resolve`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': 'mesh-dev-key' },
      body: JSON.stringify({
        chosen_resolution: 'Vector clock conflict resolved via REST API.',
        resolved_by: 'api_admin'
      })
    });

    assert.equal(resolveRes.status, 200);
    const resolveData = await resolveRes.json();
    assert.equal(resolveData.conflict.resolution_state, 'resolved');
  });
});
