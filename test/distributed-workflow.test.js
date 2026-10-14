import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { v4 as uuidv4 } from 'uuid';

import { initDatabase } from '../src/db/sqlite.js';
import { createSyncEngine } from '../src/db/sync.js';
import { MeshCoordinator } from '../src/agents/coordinator.js';
import { PeerRegistry } from '../src/p2p/discovery.js';
import {
  MessageType,
  createEnvelope,
  createPlanRequest,
  createPlanResponse,
  createExecutionRequest,
  createExecutionResponse,
  decode,
  encode
} from '../src/utils/protocol.js';
import {
  createTask,
  updateTaskPlanning,
  updateTaskPlanned,
  createTaskApproval,
  submitApprovalDecision,
  executeApprovedTask,
  TaskStatus,
  ApprovalDecision,
  assertTaskTransition
} from '../src/mesh/workflow-service.js';
import { executeOperations } from '../src/agents/executor.js';

describe('Distributed Task Workflow Lifecycle (Queued -> Planning -> Planned -> Awaiting Approval -> Approved -> Executing -> Completed/Failed)', () => {
  let tmpDir1, tmpDir2;
  let dbExecutor, dbPlanner;
  let syncExecutor, syncPlanner;
  let broadcastCount = 0;

  beforeEach(() => {
    tmpDir1 = mkdtempSync(join(tmpdir(), 'mesh-dw-exec-'));
    tmpDir2 = mkdtempSync(join(tmpdir(), 'mesh-dw-plan-'));
    dbExecutor = initDatabase(join(tmpDir1, 'executor.db'));
    dbPlanner = initDatabase(join(tmpDir2, 'planner.db'));
    syncExecutor = createSyncEngine('node-alpha');
    syncPlanner = createSyncEngine('node-delta');
    broadcastCount = 0;
  });

  afterEach(() => {
    try { dbExecutor.close(); } catch {}
    try { dbPlanner.close(); } catch {}
    try { rmSync(tmpDir1, { recursive: true, force: true }); } catch {}
    try { rmSync(tmpDir2, { recursive: true, force: true }); } catch {}
  });

  // 1. Queued task creation
  it('1. stores created task in queued status with all required schema fields and audit log', () => {
    const task = createTask(dbExecutor, {
      title: 'Restock Optical Gaming Mice',
      user_prompt: 'Add 15 Wireless Ergonomic Mice under Gaming Gear category',
      task_type: 'batch_ingest',
      priority: 'high',
      requested_by: 'operator:alice'
    });

    assert.ok(task.id);
    assert.equal(task.title, 'Restock Optical Gaming Mice');
    assert.equal(task.user_prompt, 'Add 15 Wireless Ergonomic Mice under Gaming Gear category');
    assert.equal(task.status, TaskStatus.QUEUED);
    assert.equal(task.task_type, 'batch_ingest');
    assert.equal(task.priority, 'high');
    assert.equal(task.requested_by, 'operator:alice');
    assert.equal(task.plan, null);
    assert.equal(task.operations, null);
    assert.equal(task.approval_id, null);
    assert.equal(task.execution_result, null);
    assert.equal(task.error, null);

    const audit = dbExecutor.prepare("SELECT * FROM audit_events WHERE entity_id = ? AND action = 'TASK_CREATED'").get(task.id);
    assert.ok(audit, 'Audit event must be logged on task creation');
  });

  // 2. Remote PLAN_REQUEST dispatch
  it('2. dispatches remote PLAN_REQUEST through mesh GossipSub coordinator to Planner peer', async () => {
    let publishedEnvelope = null;
    const mockNode = {
      peerId: { toString: () => 'router-peer-alpha' },
      services: {
        pubsub: {
          publish: async (_topic, data) => {
            publishedEnvelope = decode(data);
          }
        }
      }
    };

    const registry = new PeerRegistry();
    registry.registerOrUpdatePeer('planner-peer-delta', { role: 'planner', status: 'connected' });

    const coordinator = new MeshCoordinator({
      node: mockNode,
      peerRegistry: registry,
      nodeRole: 'executor',
      db: dbExecutor,
      syncEngine: syncExecutor,
      planOperations: async () => {},
      executeOperations: async () => {},
      broadcastTx: async () => {}
    });

    const task = createTask(dbExecutor, {
      title: 'Plan inventory restock',
      user_prompt: 'Insert new supplier and product',
      priority: 'medium'
    });

    const planPromise = coordinator.requestPlanFromMesh({
      taskId: task.id,
      prompt: task.user_prompt,
      taskType: task.task_type,
      priority: task.priority,
      targetPeerId: 'planner-peer-delta'
    });

    await new Promise((resolve) => setImmediate(resolve));

    assert.ok(publishedEnvelope);
    assert.equal(publishedEnvelope.type, MessageType.PLAN_REQUEST);
    assert.equal(publishedEnvelope.payload.taskId, task.id);
    assert.equal(publishedEnvelope.payload.prompt, task.user_prompt);
    assert.equal(publishedEnvelope.payload.targetPeerId, 'planner-peer-delta');

    // Clean up pending timer
    for (const [, req] of coordinator.pendingPlanRequests) {
      clearTimeout(req.timer);
    }
  });

  // 3. Planner response correlation by requestId
  it('3. correlates PLAN_RESPONSE by unique requestId and returns validated operations', async () => {
    let publishedEnvelope = null;
    const mockNode = {
      peerId: { toString: () => 'executor-alpha' },
      services: {
        pubsub: {
          publish: async (_topic, data) => {
            publishedEnvelope = decode(data);
          }
        }
      }
    };

    const registry = new PeerRegistry();
    registry.registerOrUpdatePeer('planner-delta', { role: 'planner', status: 'connected' });

    const coordinator = new MeshCoordinator({
      node: mockNode,
      peerRegistry: registry,
      nodeRole: 'executor',
      db: dbExecutor,
      syncEngine: syncExecutor,
      planOperations: async () => {},
      executeOperations: async () => {},
      broadcastTx: async () => {}
    });

    const task = createTask(dbExecutor, {
      title: 'Correlated Planning Task',
      user_prompt: 'Insert categories',
      priority: 'low'
    });

    const planPromise = coordinator.requestPlanFromMesh({
      taskId: task.id,
      prompt: task.user_prompt
    });

    await new Promise((resolve) => setImmediate(resolve));

    const requestId = publishedEnvelope?.payload?.requestId;
    assert.ok(requestId, 'Should publish envelope containing requestId');

    // Planner replies with PLAN_RESPONSE correlating the requestId
    coordinator.handlePlanResponse({
      type: MessageType.PLAN_RESPONSE,
      payload: {
        requestId,
        taskId: task.id,
        success: true,
        operations: [
          { operation: 'INSERT', table: 'categories', data: { name: 'Electronics' } }
        ],
        explanation: 'Created Electronics category',
        planSummary: 'INSERT categories',
        validation: { valid: true },
        plannerPeerId: 'planner-delta'
      }
    }, 'planner-delta');

    const result = await planPromise;
    assert.equal(result.success, true);
    assert.equal(result.operations.length, 1);
    assert.equal(result.explanation, 'Created Electronics category');
    assert.equal(result.plannerPeerId, 'planner-delta');
  });

  // 4. Plan display data storage in planned status
  it('4. transitions task to planned status and stores structured plan and ordered operations', () => {
    const task = createTask(dbExecutor, {
      title: 'Store Plan Task',
      user_prompt: 'Add mechanical keyboard'
    });

    updateTaskPlanning(dbExecutor, task.id, { plannerPeerId: 'planner-delta', requestId: 'req-123' });
    const plannedTask = updateTaskPlanned(dbExecutor, task.id, {
      plannerPeerId: 'planner-delta',
      plannerRequestId: 'req-123',
      operations: [
        { operation: 'INSERT', table: 'categories', data: { name: 'Peripherals' } },
        { operation: 'INSERT', table: 'items', data: { category_id: 1, name: 'RGB Keyboard', price: 89.99, sku: 'SKU-KB-1' } }
      ],
      explanation: 'Planned 2 sequential mutations: category insert followed by item insert',
      validationResult: { valid: true }
    });

    assert.equal(plannedTask.status, TaskStatus.PLANNED);
    assert.equal(plannedTask.planner_peer_id, 'planner-delta');
    assert.equal(plannedTask.planner_request_id, 'req-123');
    assert.ok(plannedTask.operations);
    const parsedOps = JSON.parse(plannedTask.operations);
    assert.equal(parsedOps.length, 2);
    assert.equal(parsedOps[0].operation, 'INSERT');
    assert.equal(parsedOps[0].table, 'categories');
  });

  // 5. Execute creating approval without database mutation
  it('5. clicking Execute creates an approval record and moves task to awaiting_approval without mutating database', () => {
    const task = createTask(dbExecutor, {
      title: 'Execute to Approval Task',
      user_prompt: 'Insert item'
    });

    updateTaskPlanning(dbExecutor, task.id, { plannerPeerId: 'planner-delta', requestId: 'req-5' });
    const plannedTask = updateTaskPlanned(dbExecutor, task.id, {
      plannerPeerId: 'planner-delta',
      plannerRequestId: 'req-5',
      operations: [
        { operation: 'INSERT', table: 'items', data: { category_id: 1, name: 'Monitor', price: 199.99, sku: 'SKU-MON-1' } }
      ],
      explanation: 'Insert single monitor item'
    });

    const initialItemCount = dbExecutor.prepare('SELECT COUNT(*) as c FROM items').get().c;

    // Call createTaskApproval (what Execute triggers)
    const { task: awaitingTask, approval } = createTaskApproval(dbExecutor, task.id, {
      requestedBy: 'operator:bob',
      riskLevel: 'medium'
    });

    assert.equal(awaitingTask.status, TaskStatus.AWAITING_APPROVAL);
    assert.ok(approval.id);
    assert.equal(approval.decision, ApprovalDecision.PENDING);
    assert.equal(approval.risk_level, 'medium');

    // Verify database was NOT mutated
    const postCount = dbExecutor.prepare('SELECT COUNT(*) as c FROM items').get().c;
    assert.equal(postCount, initialItemCount, 'Execute click MUST NOT mutate business tables');
  });

  // 6. Reject causing no database mutation
  it('6. rejecting approval marks approval rejected, sets task to rejected/failed, and mutates no database tables', () => {
    const task = createTask(dbExecutor, {
      title: 'Rejectable Task',
      user_prompt: 'Drop or alter table'
    });

    updateTaskPlanning(dbExecutor, task.id, { plannerPeerId: 'planner-delta', requestId: 'req-6' });
    updateTaskPlanned(dbExecutor, task.id, {
      plannerPeerId: 'planner-delta',
      plannerRequestId: 'req-6',
      operations: [{ operation: 'INSERT', table: 'categories', data: { name: 'To Reject' } }]
    });

    const { approval } = createTaskApproval(dbExecutor, task.id, { riskLevel: 'high' });
    const initialCategories = dbExecutor.prepare('SELECT COUNT(*) as c FROM categories').get().c;

    const decided = submitApprovalDecision(dbExecutor, approval.id, {
      decision: ApprovalDecision.REJECTED,
      reviewer: 'security-lead',
      reason: 'Unacceptable security posture',
      targetStatus: TaskStatus.REJECTED
    });

    assert.equal(decided.decision, ApprovalDecision.REJECTED);
    const taskAfter = dbExecutor.prepare('SELECT * FROM tasks WHERE id = ?').get(task.id);
    assert.equal(taskAfter.status, TaskStatus.REJECTED);
    assert.equal(taskAfter.error, 'Unacceptable security posture');

    const finalCategories = dbExecutor.prepare('SELECT COUNT(*) as c FROM categories').get().c;
    assert.equal(finalCategories, initialCategories, 'Rejection must never mutate tables');
  });

  // 7. Approve sending EXECUTION_REQUEST & executing atomically
  it('7. approving marks task approved and executes operations atomically on executor node', () => {
    const task = createTask(dbExecutor, {
      title: 'Approvable Workflow Task',
      user_prompt: 'Insert new verified category and item'
    });

    const ops = [
      { operation: 'INSERT', table: 'categories', data: { name: 'Audio Gear' } },
      { operation: 'INSERT', table: 'items', data: { category_id: 1, name: 'Studio Headphones', price: 149.99, sku: 'SKU-AUDIO-1' } }
    ];

    updateTaskPlanning(dbExecutor, task.id, { plannerPeerId: 'planner-delta', requestId: 'req-7' });
    updateTaskPlanned(dbExecutor, task.id, {
      plannerPeerId: 'planner-delta',
      plannerRequestId: 'req-7',
      operations: ops,
      explanation: 'Add Audio Gear category and Headphones item'
    });

    const { approval } = createTaskApproval(dbExecutor, task.id);
    submitApprovalDecision(dbExecutor, approval.id, {
      decision: ApprovalDecision.APPROVED,
      reviewer: 'lead_operator',
      targetStatus: TaskStatus.APPROVED
    });

    const approvedTask = dbExecutor.prepare('SELECT status FROM tasks WHERE id = ?').get(task.id);
    assert.equal(approvedTask.status, TaskStatus.APPROVED);

    // Executor executes the approved task
    const outcome = executeApprovedTask(dbExecutor, task.id, {
      executorNodeId: 'node-alpha',
      executorRole: 'executor',
      syncEngine: syncExecutor,
      broadcastTx: async () => { broadcastCount++; },
      mutations: ops
    });

    assert.equal(outcome.success, true);
    assert.equal(outcome.status, TaskStatus.COMPLETED);

    // Verify database mutation took place
    const audioItem = dbExecutor.prepare('SELECT * FROM items WHERE sku = ?').get('SKU-AUDIO-1');
    assert.ok(audioItem);
    assert.equal(audioItem.name, 'Studio Headphones');
    assert.ok(broadcastCount >= 1, 'Replication gossip must be broadcast upon commit');
  });

  // 8. Executor-side local revalidation
  it('8. executor locally revalidates operations before execution and rejects non-allowlisted tables', () => {
    const task = createTask(dbExecutor, {
      title: 'Malicious Remote Operations',
      user_prompt: 'Access private credentials'
    });

    const badOps = [
      { operation: 'INSERT', table: 'private_security_keys', data: { secret: '123' } }
    ];

    updateTaskPlanning(dbExecutor, task.id, { plannerPeerId: 'planner-delta', requestId: 'req-8' });
    updateTaskPlanned(dbExecutor, task.id, {
      plannerPeerId: 'planner-delta',
      plannerRequestId: 'req-8',
      operations: badOps
    });

    const { approval } = createTaskApproval(dbExecutor, task.id);
    submitApprovalDecision(dbExecutor, approval.id, {
      decision: ApprovalDecision.APPROVED,
      targetStatus: TaskStatus.APPROVED
    });

    assert.throws(() => {
      executeApprovedTask(dbExecutor, task.id, {
        executorNodeId: 'node-alpha',
        executorRole: 'executor',
        syncEngine: syncExecutor,
        mutations: badOps
      });
    }, /unsupported table|disallowed table|unknown table/i);

    const taskAfter = dbExecutor.prepare('SELECT * FROM tasks WHERE id = ?').get(task.id);
    assert.equal(taskAfter.status, TaskStatus.FAILED);
  });

  // 9. Failed execution rollback
  it('9. completely rolls back entire transaction if any operation in the approved plan fails', () => {
    const initialItemCount = dbExecutor.prepare('SELECT COUNT(*) as c FROM items').get().c;
    const initialCatCount = dbExecutor.prepare('SELECT COUNT(*) as c FROM categories').get().c;

    const task = createTask(dbExecutor, {
      title: 'Partial Failure Transaction',
      user_prompt: 'First insert valid category, second insert item with negative price'
    });

    const ops = [
      { operation: 'INSERT', table: 'categories', data: { name: 'Temp Cat' } },
      { operation: 'INSERT', table: 'items', data: { category_id: 1, name: 'Negative Item', price: -99, sku: 'SKU-FAIL-NEG' } }
    ];

    updateTaskPlanning(dbExecutor, task.id, { plannerPeerId: 'planner-delta', requestId: 'req-9' });
    updateTaskPlanned(dbExecutor, task.id, {
      plannerPeerId: 'planner-delta',
      plannerRequestId: 'req-9',
      operations: ops
    });
    const { approval } = createTaskApproval(dbExecutor, task.id);
    submitApprovalDecision(dbExecutor, approval.id, { decision: ApprovalDecision.APPROVED, targetStatus: TaskStatus.APPROVED });

    assert.throws(() => {
      executeApprovedTask(dbExecutor, task.id, {
        executorNodeId: 'node-alpha',
        executorRole: 'executor',
        syncEngine: syncExecutor,
        mutations: ops
      });
    }, /price must be greater than (0|zero)/i);

    // Verify rollback: Temp Cat was NOT persisted
    const finalCatCount = dbExecutor.prepare('SELECT COUNT(*) as c FROM categories').get().c;
    const finalItemCount = dbExecutor.prepare('SELECT COUNT(*) as c FROM items').get().c;
    assert.equal(finalCatCount, initialCatCount);
    assert.equal(finalItemCount, initialItemCount);

    const taskAfter = dbExecutor.prepare('SELECT status, error FROM tasks WHERE id = ?').get(task.id);
    assert.equal(taskAfter.status, TaskStatus.FAILED);
    assert.ok(taskAfter.error);
  });

  // 10. Duplicate approval prevention (idempotence)
  it('10. prevents duplicate approval submissions with 409 Conflict', () => {
    const task = createTask(dbExecutor, { title: 'Dedup Approval Task', user_prompt: 'Plan something' });
    updateTaskPlanning(dbExecutor, task.id, { plannerPeerId: 'planner-delta', requestId: 'req-10' });
    updateTaskPlanned(dbExecutor, task.id, {
      plannerPeerId: 'planner-delta',
      plannerRequestId: 'req-10',
      operations: [{ operation: 'INSERT', table: 'categories', data: { name: 'C1' } }]
    });
    const { approval } = createTaskApproval(dbExecutor, task.id);

    submitApprovalDecision(dbExecutor, approval.id, { decision: ApprovalDecision.APPROVED });

    // Calling again with conflicting decision throws 409
    assert.throws(() => {
      submitApprovalDecision(dbExecutor, approval.id, { decision: ApprovalDecision.REJECTED });
    }, /already been decided/i);
  });

  // 11. Duplicate execution prevention (idempotence)
  it('11. returns idempotent completed state on repeated execution calls', () => {
    const task = createTask(dbExecutor, { title: 'Dedup Execution Task', user_prompt: 'Run once' });
    const ops = [{ operation: 'INSERT', table: 'categories', data: { name: 'Single Run Cat' } }];
    updateTaskPlanning(dbExecutor, task.id, { plannerPeerId: 'planner-delta', requestId: 'req-11' });
    updateTaskPlanned(dbExecutor, task.id, {
      plannerPeerId: 'planner-delta',
      plannerRequestId: 'req-11',
      operations: ops
    });
    const { approval } = createTaskApproval(dbExecutor, task.id);
    submitApprovalDecision(dbExecutor, approval.id, { decision: ApprovalDecision.APPROVED, targetStatus: TaskStatus.APPROVED });

    const first = executeApprovedTask(dbExecutor, task.id, {
      executorNodeId: 'node-alpha',
      executorRole: 'executor',
      syncEngine: syncExecutor,
      mutations: ops
    });
    assert.equal(first.success, true);
    assert.equal(first.status, TaskStatus.COMPLETED);

    // Second execution returns idempotent completed result without duplicate inserts
    const second = executeApprovedTask(dbExecutor, task.id, {
      executorNodeId: 'node-alpha',
      executorRole: 'executor',
      syncEngine: syncExecutor,
      mutations: ops
    });
    assert.equal(second.success, true);
    assert.equal(second.alreadyCompleted, true);
  });

  // 12. Invalid state transitions rejected
  it('12. strictly rejects invalid state transitions', () => {
    const task = createTask(dbExecutor, { title: 'State Test Task', user_prompt: 'Transitions' });
    assert.equal(task.status, TaskStatus.QUEUED);

    // Cannot jump from queued directly to executing
    assert.throws(() => {
      assertTaskTransition(task.status, TaskStatus.EXECUTING, task.id);
    }, /Invalid state transition/i);

    // Cannot jump from queued directly to completed
    assert.throws(() => {
      assertTaskTransition(task.status, TaskStatus.COMPLETED, task.id);
    }, /Invalid state transition/i);
  });

  // 13. Planner role refusing execution
  it('13. coordinator running with NODE_ROLE=planner strictly refuses execution requests', async () => {
    let sentEnvelope = null;
    const mockNode = {
      peerId: { toString: () => 'planner-node-delta' },
      services: {
        pubsub: {
          publish: async (_topic, data) => {
            const { decode } = await import('../src/utils/protocol.js');
            sentEnvelope = decode(data);
          }
        }
      }
    };

    const plannerCoordinator = new MeshCoordinator({
      node: mockNode,
      peerRegistry: new PeerRegistry(),
      nodeRole: 'planner',
      db: dbPlanner,
      syncEngine: syncPlanner,
      planOperations: async () => {},
      executeOperations: async () => {},
      broadcastTx: async () => {}
    });

    const execEnvelope = createEnvelope(
      MessageType.EXECUTION_REQUEST,
      createExecutionRequest({
        requestId: 'plan-refuse-1',
        operations: [{ operation: 'INSERT', table: 'categories', data: { name: 'Illegal' } }],
        routerPeerId: 'router-1'
      }),
      'router-1'
    );

    await plannerCoordinator.handleExecutionRequest(execEnvelope, 'router-1');

    assert.ok(sentEnvelope);
    assert.equal(sentEnvelope.payload.success, false);
    assert.match(sentEnvelope.payload.error, /forbidden from executing database mutations/i);
  });

  // 14. Executor database replication after approval
  it('14. replicates executed approved mutations from executor to remote peer via _mesh_log and syncEngine', () => {
    const task = createTask(dbExecutor, {
      title: 'Replication Verification Task',
      user_prompt: 'Insert replicable category'
    });

    const ops = [{ operation: 'INSERT', table: 'categories', data: { name: 'Replicated Category' } }];
    updateTaskPlanning(dbExecutor, task.id, { plannerPeerId: 'planner-delta', requestId: 'req-14' });
    updateTaskPlanned(dbExecutor, task.id, {
      plannerPeerId: 'planner-delta',
      plannerRequestId: 'req-14',
      operations: ops
    });
    const { approval } = createTaskApproval(dbExecutor, task.id);
    submitApprovalDecision(dbExecutor, approval.id, { decision: ApprovalDecision.APPROVED, targetStatus: TaskStatus.APPROVED });

    let capturedTxEnvelope = null;
    executeApprovedTask(dbExecutor, task.id, {
      executorNodeId: 'node-alpha',
      executorRole: 'executor',
      syncEngine: syncExecutor,
      broadcastTx: async (env) => { capturedTxEnvelope = env; },
      mutations: ops
    });

    assert.ok(capturedTxEnvelope);
    assert.equal(capturedTxEnvelope.payload.operation, 'INSERT');
    assert.equal(capturedTxEnvelope.payload.tableName, 'categories');

    // Remote Planner peer receives and replicates transaction into its database
    const applyOutcome = syncPlanner.applyRemoteTransaction(dbPlanner, capturedTxEnvelope);
    assert.equal(applyOutcome.applied, true);

    const replicatedCat = dbPlanner.prepare("SELECT * FROM categories WHERE name = 'Replicated Category'").get();
    assert.ok(replicatedCat, 'Category must be replicated to remote peer database');
  });

  // 15. Complete full lifecycle end-to-end
  it('15. verifies full end-to-end lifecycle: Queued -> Planning -> Planned -> Execute -> Awaiting Approval -> Approve -> Executing -> Completed', () => {
    // 1. Create (Queued)
    const task = createTask(dbExecutor, {
      title: 'Full Lifecycle E2E Task',
      user_prompt: 'End to end distributed flow',
      requested_by: 'lead_operator'
    });
    assert.equal(task.status, TaskStatus.QUEUED);

    // 2. Plan (Queued -> Planning -> Planned)
    const planningTask = updateTaskPlanning(dbExecutor, task.id, { plannerPeerId: 'peer-delta', requestId: 'req-full-1' });
    assert.equal(planningTask.status, TaskStatus.PLANNING);

    const plannedOps = [{ operation: 'INSERT', table: 'categories', data: { name: 'E2E Full Flow Category' } }];
    const plannedTask = updateTaskPlanned(dbExecutor, task.id, {
      plannerPeerId: 'peer-delta',
      plannerRequestId: 'req-full-1',
      operations: plannedOps,
      explanation: 'Planned 1 atomic insert'
    });
    assert.equal(plannedTask.status, TaskStatus.PLANNED);

    // 3. Click Execute (Planned -> Awaiting Approval)
    const { task: awaitingTask, approval } = createTaskApproval(dbExecutor, task.id);
    assert.equal(awaitingTask.status, TaskStatus.AWAITING_APPROVAL);
    assert.equal(approval.decision, ApprovalDecision.PENDING);

    // 4. Click Approve (Awaiting Approval -> Approved)
    submitApprovalDecision(dbExecutor, approval.id, {
      decision: ApprovalDecision.APPROVED,
      reviewer: 'auditor:carol',
      targetStatus: TaskStatus.APPROVED
    });
    const approvedTask = dbExecutor.prepare('SELECT status FROM tasks WHERE id = ?').get(task.id);
    assert.equal(approvedTask.status, TaskStatus.APPROVED);

    // 5. Executor executes (Approved -> Executing -> Completed)
    const finalOutcome = executeApprovedTask(dbExecutor, task.id, {
      executorNodeId: 'node-alpha',
      executorRole: 'executor',
      syncEngine: syncExecutor,
      broadcastTx: async () => {},
      mutations: plannedOps
    });

    assert.equal(finalOutcome.success, true);
    assert.equal(finalOutcome.status, TaskStatus.COMPLETED);

    const completedTask = dbExecutor.prepare('SELECT * FROM tasks WHERE id = ?').get(task.id);
    assert.equal(completedTask.status, TaskStatus.COMPLETED);
    assert.ok(completedTask.execution_result);
  });

  // 16. Cross-node Planner -> Executor handoff with agentic approval and completed status
  it('16. Executor node receives plan from Planner, registers task in SQLite, agentically approves and executes, and Planner updates task to completed', async () => {
    // 1. Create task on Planner node
    const plannerTask = createTask(dbPlanner, {
      title: 'Add Gaming Laptop',
      user_prompt: 'Add a new item Gaming Laptop in Electronics with price 1499.99',
      requested_by: 'lead_operator'
    });
    assert.equal(plannerTask.status, TaskStatus.QUEUED);

    // Planner plans task (queued -> planning -> planned)
    updateTaskPlanning(dbPlanner, plannerTask.id, { plannerPeerId: 'planner-node-delta', requestId: 'req-plan-16' });
    const ops = [
      { operation: 'INSERT', table: 'categories', data: { name: 'High End Tech' } },
      { operation: 'INSERT', table: 'items', data: { name: 'Gaming Laptop Pro', sku: 'SKU-GLAP-99', price: 1499.99, category_id: 1 } }
    ];
    updateTaskPlanned(dbPlanner, plannerTask.id, {
      plannerPeerId: 'planner-node-delta',
      plannerRequestId: 'req-plan-16',
      operations: ops,
      explanation: 'Planned 2 operations'
    });

    // Mark task executing on Planner as it dispatches to Executor
    dbPlanner.prepare('UPDATE tasks SET status = ? WHERE id = ?').run(TaskStatus.EXECUTING, plannerTask.id);

    let executorReplyEnvelope = null;
    const mockExecutorNode = {
      peerId: { toString: () => 'executor-node-alpha' },
      services: {
        pubsub: {
          publish: async (_topic, data) => {
            executorReplyEnvelope = decode(data);
          }
        }
      }
    };

    const executorCoordinator = new MeshCoordinator({
      node: mockExecutorNode,
      peerRegistry: new PeerRegistry(),
      nodeRole: 'executor',
      db: dbExecutor,
      syncEngine: syncExecutor,
      executeApprovedTask: (db, taskId, opts) => executeApprovedTask(db, taskId, { ...opts, syncEngine: syncExecutor }),
      executeOperations: async (ops) => executeOperations(ops, dbExecutor, syncExecutor),
      broadcastTx: async () => {}
    });

    const execRequestEnvelope = createEnvelope(
      MessageType.EXECUTION_REQUEST,
      createExecutionRequest({
        requestId: 'exec-req-16',
        taskId: plannerTask.id,
        title: 'Add Gaming Laptop',
        prompt: plannerTask.user_prompt,
        operations: ops,
        routerPeerId: 'planner-node-delta',
        plannerPeerId: 'planner-node-delta',
        plannerNodeName: 'delta',
        autoApprove: true
      }),
      'planner-node-delta'
    );

    // Executor receives EXECUTION_REQUEST from mesh
    await executorCoordinator.handleExecutionRequest(execRequestEnvelope, 'planner-node-delta');

    // 1. Verify task was registered on Executor
    const executorLocalTask = dbExecutor.prepare('SELECT * FROM tasks WHERE id = ?').get(plannerTask.id);
    assert.ok(executorLocalTask, 'Task must be registered on Executor');
    assert.equal(executorLocalTask.status, TaskStatus.COMPLETED, 'Executor must have agentically approved and executed task');

    // 2. Verify operations were committed into Executor SQLite database
    const insertedItem = dbExecutor.prepare("SELECT * FROM items WHERE sku = 'SKU-GLAP-99'").get();
    assert.ok(insertedItem, 'Item must be committed in Executor DB');
    assert.equal(insertedItem.price, 1499.99);

    // 3. Verify EXECUTION_RESPONSE was sent back
    assert.ok(executorReplyEnvelope, 'EXECUTION_RESPONSE envelope must be published');
    assert.equal(executorReplyEnvelope.type, MessageType.EXECUTION_RESPONSE);
    assert.equal(executorReplyEnvelope.payload.success, true);
    assert.equal(executorReplyEnvelope.payload.summary.completed, 2);

    // 4. Planner receives EXECUTION_RESPONSE
    const mockPlannerNode = {
      peerId: { toString: () => 'planner-node-delta' }
    };
    const plannerCoordinator = new MeshCoordinator({
      node: mockPlannerNode,
      peerRegistry: new PeerRegistry(),
      nodeRole: 'planner',
      db: dbPlanner,
      syncEngine: syncPlanner,
      broadcastTx: async () => {}
    });

    // Mock pending request on planner coordinator
    plannerCoordinator.pendingExecutionRequests.set('exec-req-16', {
      resolve: () => {},
      timer: setTimeout(() => {}, 10000),
      taskId: plannerTask.id
    });

    plannerCoordinator.handleExecutionResponse(executorReplyEnvelope, 'executor-node-alpha');

    // 5. Verify task on Planner is now COMPLETED (not stuck in EXECUTING!)
    const plannerFinalTask = dbPlanner.prepare('SELECT * FROM tasks WHERE id = ?').get(plannerTask.id);
    assert.equal(plannerFinalTask.status, TaskStatus.COMPLETED, 'Planner task must transition to COMPLETED');
  });
});
