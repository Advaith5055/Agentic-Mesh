/**
 * @fileoverview Architectural Repair Forensic Test Suite.
 * Validates all 10 architectural invariants defined in Section 21.
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

import { initDatabase } from '../src/db/sqlite.js';
import { createSyncEngine } from '../src/db/sync.js';
import { executeMutationBatch } from '../src/db/mutation.js';
import {
  createTask,
  planTask,
  dispatchTask,
  approveTask,
  rejectTask,
  executeTask,
  executeApprovedTask,
  submitApprovalDecision,
  TaskStatus,
  ApprovalDecision
} from '../src/mesh/workflow-service.js';
import { startWebSocketServer } from '../src/websocket/server.js';
import { MeshCoordinator } from '../src/agents/coordinator.js';
import { MessageType, createEnvelope } from '../src/utils/protocol.js';

describe('Architectural Repair Invariants Test Suite (Section 21)', () => {
  let tmpDir;
  let db;
  let syncEngine;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'arch-repair-'));
    db = initDatabase(join(tmpDir, 'test.db'), { seedDemoData: true });
    syncEngine = createSyncEngine('node-test');
  });

  afterEach(() => {
    try {
      db.close();
      rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  });

  // ── Test 1: Planner node rejected from executing database mutations ───────
  it('Test 1: Planner node cannot execute database mutations or commit directly', () => {
    const operations = [
      { operation: 'INSERT', table: 'categories', data: { name: 'Illegal Planner Mutation' } }
    ];

    // executeMutationBatch strictly blocks planner role
    assert.throws(() => {
      executeMutationBatch(db, operations, {
        actorId: 'node-delta',
        actorRole: 'planner',
        syncEngine
      });
    }, /cannot execute mutations/i);

    // executeApprovedTask strictly blocks planner role
    const task = createTask(db, { title: 'Test Task', user_prompt: 'Insert category', priority: 'medium' });
    planTask(db, task.id, { plannerNodeId: 'node-delta' });
    dispatchTask(db, task.id, { requestedBy: 'operator' });
    approveTask(db, task.id, { reviewer: 'operator' });

    assert.throws(() => {
      executeApprovedTask(db, task.id, {
        executorNodeId: 'node-delta',
        executorRole: 'planner',
        syncEngine,
        mutations: operations
      });
    }, /cannot execute tasks/i);
  });

  // ── Test 2: Task cannot execute directly from PLANNED status ───────────────
  it('Test 2: Task in PLANNED status cannot be directly executed (throws 409 / Conflict)', () => {
    const task = createTask(db, { title: 'Direct Exec Test', user_prompt: 'Run now', priority: 'high' });
    planTask(db, task.id, { plannerNodeId: 'node-delta' });

    const plannedTask = db.prepare('SELECT * FROM tasks WHERE id = ?').get(task.id);
    assert.equal(plannedTask.status, TaskStatus.PLANNED);

    assert.throws(() => {
      executeTask(db, task.id, { executorNodeId: 'node-alpha', executorRole: 'executor', syncEngine });
    }, /cannot execute: task .* is in status 'planned'/i);
  });

  // ── Test 3: Task cannot execute from AWAITING_APPROVAL status ──────────────
  it('Test 3: Task in AWAITING_APPROVAL status cannot be executed before approval', () => {
    const task = createTask(db, { title: 'Awaiting Exec Test', user_prompt: 'Test awaiting', priority: 'high' });
    planTask(db, task.id, { plannerNodeId: 'node-delta' });
    dispatchTask(db, task.id, { requestedBy: 'operator' });

    const awaitingTask = db.prepare('SELECT * FROM tasks WHERE id = ?').get(task.id);
    assert.equal(awaitingTask.status, TaskStatus.AWAITING_APPROVAL);

    assert.throws(() => {
      executeTask(db, task.id, { executorNodeId: 'node-alpha', executorRole: 'executor', syncEngine });
    }, /cannot execute: approval .* is pending review/i);
  });

  // ── Test 4: Approval transition strictly moves task to APPROVED ───────────
  it('Test 4: Approval decision strictly moves task from AWAITING_APPROVAL to APPROVED', () => {
    const task = createTask(db, { title: 'Approval Transition', user_prompt: 'Approve me', priority: 'medium' });
    planTask(db, task.id, { plannerNodeId: 'node-delta' });
    const { approval } = dispatchTask(db, task.id, { requestedBy: 'operator' });

    const { task: approvedTask, approval: decidedAppr } = approveTask(db, task.id, {
      reviewer: 'lead_architect',
      reason: 'Verified safe'
    });

    assert.equal(decidedAppr.decision, ApprovalDecision.APPROVED);
    assert.equal(approvedTask.status, TaskStatus.APPROVED);

    const dbTask = db.prepare('SELECT * FROM tasks WHERE id = ?').get(task.id);
    assert.equal(dbTask.status, TaskStatus.APPROVED);
  });

  // ── Test 5: Rejection transition strictly moves task to REJECTED ───────────
  it('Test 5: Rejection decision strictly moves task from AWAITING_APPROVAL to REJECTED', () => {
    const task = createTask(db, { title: 'Rejection Transition', user_prompt: 'Reject me', priority: 'medium' });
    planTask(db, task.id, { plannerNodeId: 'node-delta' });
    dispatchTask(db, task.id, { requestedBy: 'operator' });

    const { task: rejectedTask, approval: decidedAppr } = rejectTask(db, task.id, {
      reviewer: 'security_officer',
      reason: 'Too dangerous'
    });

    assert.equal(decidedAppr.decision, ApprovalDecision.REJECTED);
    assert.equal(rejectedTask.status, TaskStatus.REJECTED);

    const dbTask = db.prepare('SELECT * FROM tasks WHERE id = ?').get(task.id);
    assert.equal(dbTask.status, TaskStatus.REJECTED);
  });

  // ── Test 6: Executor node executes approved task and commits mutations ─────
  it('Test 6: Executor node executes approved task and commits mutations atomically', () => {
    const task = createTask(db, { title: 'Insert New Category', user_prompt: 'Create Books category', priority: 'low' });
    planTask(db, task.id, {
      plannerNodeId: 'node-delta',
      operations: [{ operation: 'INSERT', table: 'categories', data: { name: 'Books and Audio' } }]
    });
    dispatchTask(db, task.id, { requestedBy: 'operator' });
    approveTask(db, task.id, { reviewer: 'operator' });

    const outcome = executeApprovedTask(db, task.id, {
      executorNodeId: 'node-alpha',
      executorRole: 'executor',
      syncEngine,
      mutations: [{ operation: 'INSERT', table: 'categories', data: { name: 'Books and Audio' } }]
    });

    assert.equal(outcome.success, true);
    assert.equal(outcome.status, TaskStatus.COMPLETED);

    const category = db.prepare("SELECT * FROM categories WHERE name = 'Books and Audio'").get();
    assert.ok(category);
    assert.equal(category.name, 'Books and Audio');

    const completedTask = db.prepare('SELECT * FROM tasks WHERE id = ?').get(task.id);
    assert.equal(completedTask.status, TaskStatus.COMPLETED);
  });

  // ── Test 7: Approval bypass attempt is caught and audited ─────────────────
  it('Test 7: Approval bypass attempt is rejected and recorded in audit log', () => {
    const task = createTask(db, { title: 'Bypass Attempt', user_prompt: 'Sneak past approval', priority: 'high' });
    planTask(db, task.id, { plannerNodeId: 'node-delta' });
    dispatchTask(db, task.id, { requestedBy: 'attacker' });

    // Attempt direct execution while still awaiting approval
    assert.throws(() => {
      executeApprovedTask(db, task.id, {
        executorNodeId: 'node-alpha',
        executorRole: 'executor',
        syncEngine
      });
    }, /must be in 'approved' status/i);

    // Verify audit event
    const auditEvent = db.prepare("SELECT * FROM audit_events WHERE entity_id = ? AND action = 'EXECUTION_REJECTED_UNAPPROVED'").get(task.id);
    assert.ok(auditEvent, 'Must log security audit event for unapproved execution attempt');
  });

  // ── Test 8 & 9: Distributed handoff & Vector clock causal sync ────────────
  it('Test 8 & 9: Distributed handoff awaits approval, advances vector clock on execute, and syncs', async () => {
    let broadcastMessages = [];
    let committedTxs = [];

    const coordinatorExecutor = new MeshCoordinator({
      node: {
        peerId: { toString: () => 'executor-alpha' },
        services: { pubsub: { publish: async (t, d) => { broadcastMessages.push({ topic: t, data: d }); } } }
      },
      peerRegistry: {
        getPeer: () => ({ peerId: 'executor-alpha', role: 'executor' }),
        findAvailablePeerByRole: () => ({ peerId: 'executor-alpha', role: 'executor' }),
        getPeerCount: () => 1
      },
      db,
      syncEngine,
      broadcastTx: (env) => { committedTxs.push(env); },
      nodeRole: 'executor'
    });

    const task = createTask(db, { title: 'Distributed Flow Task', user_prompt: 'Add Drone Gear', priority: 'medium' });
    planTask(db, task.id, { plannerNodeId: 'planner-delta' });

    // 1. Planner sends EXECUTION_REQUEST to Executor
    const execReq = createEnvelope(MessageType.EXECUTION_REQUEST, {
      requestId: 'req-dist-1',
      taskId: task.id,
      operations: [{ operation: 'INSERT', table: 'categories', data: { name: 'Drone Gear' } }],
      title: task.title,
      prompt: task.user_prompt
    }, 'planner-delta');

    await coordinatorExecutor.handleExecutionRequest(execReq, 'planner-delta');

    // Task must be AWAITING_APPROVAL on executor
    const awaitingTask = db.prepare('SELECT * FROM tasks WHERE id = ?').get(task.id);
    assert.equal(awaitingTask.status, TaskStatus.AWAITING_APPROVAL);

    // Initial vector clock value
    const vclockBefore = syncEngine.getVectorClock().get('node-test') || 0;

    // 2. Operator sends APPROVAL to Executor
    const approvalEnvelope = createEnvelope(MessageType.APPROVAL, {
      requestId: 'appr-req-1',
      taskId: task.id,
      decision: 'approved',
      reviewer: 'human_operator'
    }, 'planner-delta');

    await coordinatorExecutor.handleApproval(approvalEnvelope, 'planner-delta');

    // 3. Executor executes, commits, and advances vector clock
    const vclockAfter = syncEngine.getVectorClock().get('node-test') || 0;
    assert.ok(vclockAfter > vclockBefore, 'Vector clock must advance on executor commit');

    const doneTask = db.prepare('SELECT * FROM tasks WHERE id = ?').get(task.id);
    assert.equal(doneTask.status, TaskStatus.COMPLETED);

    const category = db.prepare("SELECT * FROM categories WHERE name = 'Drone Gear'").get();
    assert.ok(category);
  });
});

describe('Architectural Repair REST API Contracts (Test 10)', () => {
  let tmpDir;
  let db;
  let syncEngine;
  let plannerServer;
  let executorServer;
  let plannerPort;
  let executorPort;
  let plannerUrl;
  let executorUrl;

  beforeEach(async () => {
    tmpDir = mkdtempSync(join(tmpdir(), 'api-contracts-'));
    db = initDatabase(join(tmpDir, 'contracts.db'), { seedDemoData: true });
    syncEngine = createSyncEngine('node-contracts');

    plannerPort = Math.floor(25000 + Math.random() * 5000);
    executorPort = Math.floor(30000 + Math.random() * 5000);
    plannerUrl = `http://127.0.0.1:${plannerPort}`;
    executorUrl = `http://127.0.0.1:${executorPort}`;

    plannerServer = startWebSocketServer(plannerPort, {
      node: { peerId: { toString: () => 'planner-node' } },
      db,
      syncEngine,
      peerRegistry: { getPeerCount: () => 1, toJSON: () => [] },
      apiKey: 'test-key',
      localNodeId: 'planner-node',
      localNodeRole: 'planner'
    });

    executorServer = startWebSocketServer(executorPort, {
      node: { peerId: { toString: () => 'executor-node' } },
      db,
      syncEngine,
      peerRegistry: { getPeerCount: () => 1, toJSON: () => [] },
      apiKey: 'test-key',
      localNodeId: 'executor-node',
      localNodeRole: 'executor'
    });

    await new Promise(r => setTimeout(r, 100));
  });

  afterEach(() => {
    try {
      plannerServer?.server?.close();
      executorServer?.server?.close();
      db.close();
      rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  });

  it('Test 10: Validates REST API contracts for /plan, /dispatch, /approve, /reject, /execute', async () => {
    // 1. Create task
    const createRes = await fetch(`${plannerUrl}/api/tasks`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': 'test-key' },
      body: JSON.stringify({ title: 'Contract Task', user_prompt: 'Create Solar Kits', priority: 'medium' })
    });
    assert.equal(createRes.status, 201);
    const { task } = await createRes.json();
    assert.equal(task.status, 'queued');

    // 2. /plan on Planner
    const planRes = await fetch(`${plannerUrl}/api/tasks/${task.id}/plan`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': 'test-key' },
      body: JSON.stringify({ plannerNodeId: 'planner-node' })
    });
    assert.equal(planRes.status, 200);
    const planData = await planRes.json();
    assert.equal(planData.task.status, 'planned');

    // 3. /execute directly on PLANNED task fails with 409 Conflict
    const prematureExec = await fetch(`${executorUrl}/api/tasks/${task.id}/execute`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': 'test-key' }
    });
    assert.equal(prematureExec.status, 409);

    // 4. /execute on Planner node fails with 403 Forbidden
    const plannerIllegalExec = await fetch(`${plannerUrl}/api/tasks/${task.id}/execute`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': 'test-key' }
    });
    assert.equal(plannerIllegalExec.status, 403);

    // 5. /dispatch moves task to AWAITING_APPROVAL
    const dispatchRes = await fetch(`${plannerUrl}/api/tasks/${task.id}/dispatch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': 'test-key' },
      body: JSON.stringify({ requested_by: 'lead_operator' })
    });
    assert.equal(dispatchRes.status, 200);
    const dispatchData = await dispatchRes.json();
    assert.equal(dispatchData.status, 'awaiting_approval');
    assert.ok(dispatchData.approval?.id);

    // 6. /execute on AWAITING_APPROVAL task fails with 409 Conflict
    const unapprovedExec = await fetch(`${executorUrl}/api/tasks/${task.id}/execute`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': 'test-key' }
    });
    assert.equal(unapprovedExec.status, 409);

    // 7. /approve moves task to APPROVED
    const approveRes = await fetch(`${plannerUrl}/api/tasks/${task.id}/approve`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': 'test-key' },
      body: JSON.stringify({ reviewer: 'security_lead', reason: 'Verified safe' })
    });
    assert.equal(approveRes.status, 200);
    const approveData = await approveRes.json();
    assert.equal(approveData.status, 'approved');

    // 8. /execute on APPROVED task on Executor succeeds
    const execRes = await fetch(`${executorUrl}/api/tasks/${task.id}/execute`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': 'test-key' }
    });
    assert.equal(execRes.status, 200);
    const execData = await execRes.json();
    assert.equal(execData.success, true);
    assert.equal(execData.status, 'completed');
  });
});
