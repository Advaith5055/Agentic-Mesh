import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { initDatabase } from '../src/db/sqlite.js';
import { createSyncEngine } from '../src/db/sync.js';
import { startWebSocketServer } from '../src/websocket/server.js';
import {
  createTask,
  planTask,
  executeTask,
  submitApprovalDecision,
  cancelTask,
  TaskStatus,
  ApprovalDecision
} from '../src/mesh/workflow-service.js';

describe('Focused Workflow Security, State Machine, Seeding & Replication Tests', () => {
  let tmpDir1;
  let tmpDir2;
  let db1;
  let db2;
  let syncEngine1;
  let syncEngine2;

  beforeEach(() => {
    tmpDir1 = mkdtempSync(join(tmpdir(), 'mesh-sec-node1-'));
    tmpDir2 = mkdtempSync(join(tmpdir(), 'mesh-sec-node2-'));
    db1 = initDatabase(join(tmpDir1, 'node1.db'), { seedDemoData: false });
    db2 = initDatabase(join(tmpDir2, 'node2.db'), { seedDemoData: false });
    syncEngine1 = createSyncEngine('node-peer-1');
    syncEngine2 = createSyncEngine('node-peer-2');
  });

  afterEach(() => {
    try {
      db1?.close();
      db2?.close();
      rmSync(tmpDir1, { recursive: true, force: true });
      rmSync(tmpDir2, { recursive: true, force: true });
    } catch {}
  });

  // 1. A planner cannot execute a task or mutate data
  it('1. planner cannot execute a task or mutate data: rejected with 403 and audit event', () => {
    // Register explicit planner node
    db1.prepare(`
      INSERT INTO mesh_nodes (id, name, role, peer_id, status)
      VALUES (?, ?, ?, ?, ?)
    `).run('node-planner-1', 'Planner Node 1', 'planner', 'peer-planner-1', 'active');

    const task = createTask(db1, {
      title: 'Planner Execution Attempt',
      user_prompt: 'Attempting execution by planner'
    });

    planTask(db1, task.id, { plannerNodeId: 'node-planner-1', riskLevel: 'low' });

    assert.throws(() => {
      executeTask(db1, task.id, { executorNodeId: 'node-planner-1' });
    }, (err) => {
      assert.equal(err.status, 403);
      assert.equal(err.code, 'FORBIDDEN');
      assert.ok(err.message.includes('not authorized'));
      return true;
    });

    // Verify audit event was logged
    const audit = db1.prepare(`
      SELECT * FROM audit_events
      WHERE action = 'ROLE_AUTHORIZATION_DENIED' AND actor = 'node-planner-1'
    `).get();
    assert.ok(audit, 'Audit event must be logged on role denial');
  });

  // 2. A router cannot execute a task
  it('2. router cannot execute a task: rejected with 403 and audit event', () => {
    db1.prepare(`
      INSERT INTO mesh_nodes (id, name, role, peer_id, status)
      VALUES (?, ?, ?, ?, ?)
    `).run('node-router-1', 'Router Node 1', 'router', 'peer-router-1', 'active');

    const task = createTask(db1, {
      title: 'Router Execution Attempt',
      user_prompt: 'Attempting execution by router'
    });

    planTask(db1, task.id, { plannerNodeId: 'node-delta', plannerRole: 'planner', riskLevel: 'low' });

    assert.throws(() => {
      executeTask(db1, task.id, { executorNodeId: 'node-router-1' });
    }, (err) => {
      assert.equal(err.status, 403);
      assert.equal(err.code, 'FORBIDDEN');
      return true;
    });

    const audit = db1.prepare(`
      SELECT * FROM audit_events
      WHERE action = 'ROLE_AUTHORIZATION_DENIED' AND actor = 'node-router-1'
    `).get();
    assert.ok(audit, 'Audit event must be logged for router execution attempt');
  });

  // 3. A validator cannot execute a task
  it('3. validator cannot execute a task: rejected with 403 and audit event', () => {
    db1.prepare(`
      INSERT INTO mesh_nodes (id, name, role, peer_id, status)
      VALUES (?, ?, ?, ?, ?)
    `).run('node-validator-1', 'Validator Node 1', 'validator', 'peer-validator-1', 'active');

    const task = createTask(db1, {
      title: 'Validator Execution Attempt',
      user_prompt: 'Attempting execution by validator'
    });

    planTask(db1, task.id, { plannerNodeId: 'node-delta', plannerRole: 'planner', riskLevel: 'low' });

    assert.throws(() => {
      executeTask(db1, task.id, { executorNodeId: 'node-validator-1' });
    }, (err) => {
      assert.equal(err.status, 403);
      assert.equal(err.code, 'FORBIDDEN');
      return true;
    });

    const audit = db1.prepare(`
      SELECT * FROM audit_events
      WHERE action = 'ROLE_AUTHORIZATION_DENIED' AND actor = 'node-validator-1'
    `).get();
    assert.ok(audit, 'Audit event must be logged for validator execution attempt');
  });

  // 4. Only a planned task can execute
  it('4. only a planned task can execute (queued, cancelled, etc. rejected)', () => {
    const task = createTask(db1, {
      title: 'Unplanned Execution Attempt',
      user_prompt: 'Direct execution without planning'
    });
    assert.equal(task.status, TaskStatus.QUEUED);

    // Queued task cannot execute
    assert.throws(() => {
      executeTask(db1, task.id, { executorNodeId: 'node-alpha', executorRole: 'executor' });
    }, /must be planned before execution/i);

    // Cancelled task cannot execute
    cancelTask(db1, task.id, { reason: 'User cancelled before planning' });
    const cancelledTask = db1.prepare('SELECT status FROM tasks WHERE id = ?').get(task.id);
    assert.equal(cancelledTask.status, TaskStatus.CANCELLED);

    assert.throws(() => {
      executeTask(db1, task.id, { executorNodeId: 'node-alpha', executorRole: 'executor' });
    }, /has been cancelled and cannot execute/i);
  });

  // 5. A completed task cannot execute twice
  it('5. completed task cannot execute twice', () => {
    const task = createTask(db1, {
      title: 'Idempotency Double-Execution Check',
      user_prompt: 'Run task once and test second attempt'
    });

    planTask(db1, task.id, { plannerNodeId: 'node-delta', plannerRole: 'planner', riskLevel: 'low' });
    const firstExec = executeTask(db1, task.id, { executorNodeId: 'node-alpha', executorRole: 'executor' });
    assert.equal(firstExec.success, true);
    assert.equal(firstExec.status, TaskStatus.COMPLETED);

    // Second execution attempt MUST be rejected
    assert.throws(() => {
      executeTask(db1, task.id, { executorNodeId: 'node-alpha', executorRole: 'executor' });
    }, /has already completed and cannot execute again/i);
  });

  // 6. Pending approval blocks execution and rejection permanently fails the task
  it('6. pending approval blocks execution and rejection permanently fails the task', () => {
    const task = createTask(db1, {
      title: 'High-Risk Operation Requiring Sign-Off',
      user_prompt: 'Execute schema migration and drop table index',
      priority: 'critical'
    });

    planTask(db1, task.id, { plannerNodeId: 'node-delta', plannerRole: 'planner', riskLevel: 'critical' });
    const plannedTask = db1.prepare('SELECT status FROM tasks WHERE id = ?').get(task.id);
    assert.equal(plannedTask.status, TaskStatus.AWAITING_APPROVAL);

    // Blocked from executing while pending
    assert.throws(() => {
      executeTask(db1, task.id, { executorNodeId: 'node-alpha', executorRole: 'executor' });
    }, /approval .* is pending review/i);

    // Reject approval
    const approval = db1.prepare('SELECT id FROM approvals WHERE task_id = ?').get(task.id);
    submitApprovalDecision(db1, approval.id, {
      decision: ApprovalDecision.REJECTED,
      reviewer: 'security-director',
      reason: 'Rejected due to insufficient backup verification'
    });

    const failedTask = db1.prepare('SELECT status FROM tasks WHERE id = ?').get(task.id);
    assert.equal(failedTask.status, TaskStatus.FAILED);

    // Rejected/failed task cannot execute
    assert.throws(() => {
      executeTask(db1, task.id, { executorNodeId: 'node-alpha', executorRole: 'executor' });
    }, /has failed and cannot execute/i);
  });

  // 7. Workflow task and approval changes replicate correctly between two isolated nodes
  it('7. workflow task and approval changes replicate correctly between two isolated nodes', () => {
    // Node 1 registers itself via canonical mutation service with syncEngine
    db1.prepare(`
      INSERT INTO mesh_nodes (id, name, role, peer_id, status)
      VALUES (?, ?, ?, ?, ?)
    `).run('node-peer-1', 'Peer Node 1', 'peer', 'peer-id-1', 'active');
    db2.prepare(`
      INSERT INTO mesh_nodes (id, name, role, peer_id, status)
      VALUES (?, ?, ?, ?, ?)
    `).run('node-peer-1', 'Peer Node 1', 'peer', 'peer-id-1', 'active');

    // Node 1 creates task and plans it
    const task = createTask(db1, {
      title: 'Replication Verification Task',
      user_prompt: 'Test multi-node replication of task and approval records',
      priority: 'high'
    }, { syncEngine: syncEngine1 });

    planTask(db1, task.id, {
      plannerNodeId: 'node-peer-1',
      plannerRole: 'peer',
      riskLevel: 'high',
      syncEngine: syncEngine1
    });

    // Read node 1 mesh logs
    const logs1 = db1.prepare('SELECT * FROM _mesh_log ORDER BY rowid ASC').all();
    assert.ok(logs1.length >= 2, 'Node 1 must have generated _mesh_log entries');

    // Apply all log transactions to Node 2 via syncEngine2
    for (const log of logs1) {
      const envelope = {
        payload: {
          id: log.id,
          operation: log.operation,
          tableName: log.table_name,
          rowData: log.row_data,
          vectorClock: log.vector_clock,
          peerId: log.peer_id
        }
      };
      const result = syncEngine2.applyRemoteTransaction(db2, envelope);
      assert.equal(result.applied, true, `Failed applying remote transaction: ${result.errors?.join(', ') || result.error}`);
    }

    // Verify task exists on Node 2 with identical status and attributes
    const node2Task = db2.prepare('SELECT * FROM tasks WHERE id = ?').get(task.id);
    assert.ok(node2Task, 'Task must replicate to Node 2');
    assert.equal(node2Task.title, 'Replication Verification Task');
    assert.equal(node2Task.status, TaskStatus.AWAITING_APPROVAL);

    // Verify approval exists on Node 2
    const node2Approval = db2.prepare('SELECT * FROM approvals WHERE task_id = ?').get(task.id);
    assert.ok(node2Approval, 'Approval must replicate to Node 2');
    assert.equal(node2Approval.risk_level, 'high');

    // Verify vector clock on Node 2 advanced causally
    const clockNode2 = syncEngine2.vectorClock.clock;
    assert.ok(clockNode2['node-peer-1'] >= 1, 'Node 2 vector clock must advance for producer node');
  });

  // 8. Remote invalid workflow mutations are rejected without partial writes
  it('8. remote invalid workflow mutations are rejected without partial writes', () => {
    const invalidEnvelope = {
      payload: {
        id: 'remote-invalid-tx-1',
        operation: 'INSERT',
        tableName: 'tasks',
        rowData: JSON.stringify({
          id: 'bad-task-1',
          title: 'Malformed Task',
          user_prompt: 'Missing status check violation',
          status: 'invalid_status_enum' // check constraint violation
        }),
        vectorClock: JSON.stringify({ 'node-remote': 1 }),
        peerId: 'node-remote'
      }
    };

    const res = syncEngine1.applyRemoteTransaction(db1, invalidEnvelope);
    assert.equal(res.applied, false);
    assert.ok(res.errors.some(e => e.includes('status must be one of')));

    // Ensure no task was inserted
    const taskRow = db1.prepare('SELECT * FROM tasks WHERE id = ?').get('bad-task-1');
    assert.equal(taskRow, undefined);

    // Ensure conflict was automatically recorded in conflicts table
    const conflict = db1.prepare("SELECT * FROM conflicts WHERE affected_entity_id = 'bad-task-1'").get();
    assert.ok(conflict, 'A conflict record must be automatically logged upon remote rejection');
  });

  // 9. Demo data is absent by default and present only when explicitly enabled
  it('9. demo data is absent by default and present only when explicitly enabled', () => {
    // db1 was created with seedDemoData: false
    const nodeCountDefault = db1.prepare('SELECT COUNT(*) as c FROM mesh_nodes').get().c;
    const taskCountDefault = db1.prepare('SELECT COUNT(*) as c FROM tasks').get().c;
    const approvalCountDefault = db1.prepare('SELECT COUNT(*) as c FROM approvals').get().c;

    assert.equal(nodeCountDefault, 0, 'No demo nodes should be present by default');
    assert.equal(taskCountDefault, 0, 'No demo tasks should be present by default');
    assert.equal(approvalCountDefault, 0, 'No demo approvals should be present by default');

    // Create database with explicit seedDemoData: true
    const tmpDemo = mkdtempSync(join(tmpdir(), 'mesh-demo-data-'));
    try {
      const demoDb = initDatabase(join(tmpDemo, 'demo.db'), { seedDemoData: true });
      const nodeCountSeeded = demoDb.prepare('SELECT COUNT(*) as c FROM mesh_nodes').get().c;
      const taskCountSeeded = demoDb.prepare('SELECT COUNT(*) as c FROM tasks').get().c;
      const approvalCountSeeded = demoDb.prepare('SELECT COUNT(*) as c FROM approvals').get().c;

      assert.ok(nodeCountSeeded >= 4, 'Seeded demo data should include at least 4 mesh nodes');
      assert.ok(taskCountSeeded >= 5, 'Seeded demo data should include demo tasks');
      assert.ok(approvalCountSeeded >= 1, 'Seeded demo data should include demo approvals');
      demoDb.close();
    } finally {
      rmSync(tmpDemo, { recursive: true, force: true });
    }
  });

  // 10. Protected mutation endpoints reject unauthenticated requests
  it('10. protected mutation endpoints reject unauthenticated requests with 401', async () => {
    const port = Math.floor(22000 + Math.random() * 20000);
    const serverInstance = startWebSocketServer(port, {
      db: db1,
      syncEngine: syncEngine1,
      apiKey: 'secret-production-token',
      allowPublicRead: true,
      localNodeRole: 'peer'
    });

    const baseUrl = `http://127.0.0.1:${port}`;

    try {
      await new Promise(r => setTimeout(r, 100));

      // 1. Unauthenticated task creation -> 401
      const resCreateNoAuth = await fetch(`${baseUrl}/api/tasks`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title: 'Unauthorized Task', user_prompt: 'Should fail' })
      });
      assert.equal(resCreateNoAuth.status, 401);
      const errCreate = await resCreateNoAuth.json();
      assert.equal(errCreate.code, 'UNAUTHORIZED');

      // 2. Unauthenticated node registration -> 401
      const resRegNoAuth = await fetch(`${baseUrl}/api/nodes/register`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ node: { id: 'node-rogue', name: 'rogue', role: 'executor' } })
      });
      assert.equal(resRegNoAuth.status, 401);

      // 3. Unauthenticated proposal -> 401
      const resProposeNoAuth = await fetch(`${baseUrl}/api/propose`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ table: 'items', operation: 'INSERT', data: { name: 'Unauthorized item' } })
      });
      assert.equal(resProposeNoAuth.status, 401);

      // 4. Authenticated task creation with correct x-api-key -> 201
      const resCreateAuth = await fetch(`${baseUrl}/api/tasks`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': 'secret-production-token'
        },
        body: JSON.stringify({ title: 'Authorized Task', user_prompt: 'Authorized create' })
      });
      assert.equal(resCreateAuth.status, 201);
      const createData = await resCreateAuth.json();
      assert.ok(createData.task.id);
    } finally {
      serverInstance.server.close();
    }
  });
});
