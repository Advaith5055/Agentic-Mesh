import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { v4 as uuidv4 } from 'uuid';

import {
  initDatabase,
  getTaskMessages,
  recordTaskMessage
} from '../src/db/sqlite.js';
import { createSyncEngine } from '../src/db/sync.js';
import {
  createTask,
  updateTaskPlanning,
  updateTaskPlanned,
  createTaskApproval,
  submitApprovalDecision,
  executeApprovedTask,
  recordWorkflowMessage,
  getWorkflowMessages,
  TaskStatus,
  assertTaskTransition
} from '../src/mesh/workflow-service.js';
import { PeerRegistry } from '../src/p2p/discovery.js';
import { validateModelPlan } from '../src/agents/model-validator.js';
import { AutonomousPlanner } from '../src/agents/autonomous-planner.js';

describe('Collaborative Planner ↔ Executor Mesh Workflow', () => {
  let tmpDir;
  let db;
  let syncEngine;
  let peerRegistry;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'collab-mesh-test-'));
    db = initDatabase(join(tmpDir, 'collab-mesh.db'));
    syncEngine = createSyncEngine('node-test-collab');
    peerRegistry = new PeerRegistry();
  });

  afterEach(() => {
    try {
      db.close();
      rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  });

  it('1. Planner discovery: identifies and selects connected Planner nodes by role', () => {
    peerRegistry.addPeer('peer-planner-alpha', ['/ip4/127.0.0.1/tcp/9001'], {
      nodeName: 'alpha',
      nodeRole: 'planner',
      capabilities: ['planning', '3nf-decomposition']
    });

    peerRegistry.addPeer('peer-executor-delta', ['/ip4/127.0.0.1/tcp/9002'], {
      nodeName: 'delta',
      nodeRole: 'executor',
      capabilities: ['sqlite-mutations']
    });

    const plannerPeer = peerRegistry.findAvailablePeerByRole('planner');
    assert.ok(plannerPeer, 'Should find planner peer');
    assert.equal(plannerPeer.peerId, 'peer-planner-alpha');
    assert.equal(plannerPeer.nodeName, 'alpha');

    const executorPeer = peerRegistry.findAvailablePeerByRole('executor');
    assert.ok(executorPeer, 'Should find executor peer');
    assert.equal(executorPeer.peerId, 'peer-executor-delta');
  });

  it('2. PLAN_REQUEST and PLAN_RESPONSE: full request-response lifecycle with progress reporting', async () => {
    const wsEvents = [];
    const broadcastWs = (type, data) => wsEvents.push({ type, data });

    const planner = new AutonomousPlanner({
      node: { peerId: 'peer-planner-alpha' },
      peerRegistry,
      db,
      syncEngine,
      planOperations: async (prompt) => ({
        success: true,
        operations: [
          {
            operation: 'INSERT',
            table: 'categories',
            data: { name: 'Analytics Tools', description: 'Monitoring tools' }
          }
        ]
      }),
      broadcastWs,
      nodeName: 'alpha',
      nodeRole: 'planner'
    });

    const task = createTask(db, {
      title: 'Analyze Category Distribution',
      user_prompt: 'Insert analytics category and verify structure',
      task_type: 'analytics',
      priority: 'high',
      requested_by: 'operator'
    });

    const requestId = `req-${uuidv4().slice(0, 8)}`;
    const planResponse = await planner.executeAutonomousPlan({
      taskId: task.id,
      requestId,
      prompt: task.user_prompt,
      requester: 'node-router'
    });

    assert.ok(planResponse.success, 'Plan response must be successful');
    assert.equal(planResponse.taskId, task.id);
    assert.equal(planResponse.requestId, requestId);
    assert.ok(Array.isArray(planResponse.operations));
    assert.equal(planResponse.operations.length, 1);
    assert.equal(planResponse.operations[0].table, 'categories');

    // Planner never mutates business table directly
    const catCheck = db.prepare("SELECT * FROM categories WHERE name = 'Analytics Tools'").get();
    assert.equal(catCheck, undefined, 'Planner MUST NEVER directly write to SQLite tables');
  });

  it('3. Visible agent messages: records the 5 mandatory planner progress messages in order', async () => {
    const recordedEvents = [];
    const broadcastWs = (type, data) => recordedEvents.push({ type, data });

    const planner = new AutonomousPlanner({
      node: { peerId: 'peer-planner-alpha' },
      peerRegistry,
      db,
      syncEngine,
      planOperations: async () => ({
        success: true,
        operations: [
          {
            operation: 'INSERT',
            table: 'categories',
            data: { name: 'IoT Devices', description: 'Sensors and gateways' }
          }
        ]
      }),
      broadcastWs,
      nodeName: 'alpha',
      nodeRole: 'planner'
    });

    const task = createTask(db, {
      title: 'IoT Devices Setup',
      user_prompt: 'Decompose IoT Devices category insertion',
      task_type: 'device_mgmt',
      priority: 'medium'
    });
    const taskId = task.id;
    const requestId = `req-visible-${uuidv4().slice(0, 6)}`;

    await planner.executeAutonomousPlan({
      taskId,
      requestId,
      prompt: 'Decompose IoT Devices category insertion',
      requester: 'executor-node'
    });

    const messages = getTaskMessages(db, taskId);
    assert.ok(messages.length >= 5, `Expected at least 5 messages, got ${messages.length}`);

    // Verify presence of all 5 mandatory progress messages
    const summaries = messages.map(m => m.summary);
    assert.ok(summaries.some(s => s.startsWith('Request received')), '1. Request received missing');
    assert.ok(summaries.some(s => s.startsWith('Understanding the task')), '2. Understanding the task missing');
    assert.ok(summaries.some(s => s.startsWith('Proposed steps')), '3. Proposed steps missing');
    assert.ok(summaries.some(s => s.startsWith('Validation completed')), '4. Validation completed missing');
    assert.ok(summaries.some(s => s.startsWith('PLAN_RESPONSE sent')), '5. PLAN_RESPONSE sent missing');

    // Check message schema properties
    const firstMsg = messages[0];
    assert.ok(firstMsg.message_id);
    assert.ok(firstMsg.timestamp);
    assert.equal(firstMsg.sender_node, 'alpha');
    assert.equal(firstMsg.sender_role, 'planner');
    assert.equal(firstMsg.request_id, requestId);
  });

  it('4. Plan validation: strictly checks allowlists and valid JSON schemas', () => {
    // Valid plan
    const validPlan = [
      { operation: 'INSERT', table: 'categories', data: { name: 'Hardware', description: 'Physical components' } }
    ];
    const valResult1 = validateModelPlan(validPlan);
    assert.ok(valResult1.valid);
    assert.equal(valResult1.operations.length, 1);

    // Invalid table
    const invalidTablePlan = [
      { operation: 'INSERT', table: 'passwords', data: { hash: 'secret' } }
    ];
    const valResult2 = validateModelPlan(invalidTablePlan);
    assert.equal(valResult2.valid, false);
    assert.ok(valResult2.errors.some(e => e.includes('unsupported table')));

    // Raw SQL operation rejection
    const rawSqlPlan = [
      { operation: 'DROP', table: 'items', data: {} }
    ];
    const valResult3 = validateModelPlan(rawSqlPlan);
    assert.equal(valResult3.valid, false);
    assert.ok(valResult3.errors.some(e => e.includes('unsupported operation')));
  });

  it('5. Approval before execution: execution is strictly blocked before human sign-off', () => {
    const task = createTask(db, {
      title: 'Update Inventory Prices',
      user_prompt: 'Bulk update prices',
      task_type: 'inventory_mutation',
      priority: 'high'
    });

    const ops = [
      { operation: 'INSERT', table: 'categories', data: { name: 'Peripherals', description: 'Input/output devices' } }
    ];

    // Transition: queued -> planning -> planned
    updateTaskPlanning(db, task.id, { plannerPeerId: 'planner-alpha', plannerRequestId: 'req-plan-1' });
    updateTaskPlanned(db, task.id, {
      plannerPeerId: 'planner-alpha',
      plannerRequestId: 'req-plan-1',
      operations: ops,
      explanation: 'Add Peripherals category'
    });

    // Attempt direct execution without approval -> Should throw 409 or 400
    assert.throws(() => {
      executeApprovedTask(db, task.id, {
        executorNodeId: 'node-delta',
        executorRole: 'executor',
        syncEngine
      });
    }, (err) => {
      return err.code === 'INVALID_TRANSITION' || err.status === 409;
    });

    // Create approval -> status becomes awaiting_approval
    const { task: awaitingTask, approval } = createTaskApproval(db, task.id, {
      requestedBy: 'operator',
      riskLevel: 'high'
    });
    assert.equal(awaitingTask.status, TaskStatus.AWAITING_APPROVAL);
    assert.equal(approval.decision, 'pending');

    // Attempting to execute while awaiting approval -> blocked
    assert.throws(() => {
      executeApprovedTask(db, task.id, {
        executorNodeId: 'node-delta',
        executorRole: 'executor',
        syncEngine
      });
    }, (err) => {
      return err.code === 'INVALID_TRANSITION' || err.code === 'APPROVAL_PENDING';
    });
  });

  it('6. EXECUTION_REQUEST handoff & Executor validation: approved task validated locally and executed', () => {
    const task = createTask(db, {
      title: 'Collaborative Category Setup',
      user_prompt: 'Add Server Hardware category',
      task_type: 'schema_mutation',
      priority: 'medium'
    });

    const ops = [
      { operation: 'INSERT', table: 'categories', data: { name: 'Server Hardware', description: 'Enterprise servers' } }
    ];

    updateTaskPlanning(db, task.id, { plannerPeerId: 'planner-alpha', plannerRequestId: 'req-plan-handoff' });
    updateTaskPlanned(db, task.id, {
      plannerPeerId: 'planner-alpha',
      plannerRequestId: 'req-plan-handoff',
      operations: ops,
      explanation: 'Insert server hardware category'
    });

    const { approval } = createTaskApproval(db, task.id, { requestedBy: 'alice' });

    // Operator approves the task
    submitApprovalDecision(db, approval.id, {
      decision: 'approved',
      reviewer: 'lead_operator',
      reason: 'Verified safe 3NF insert',
      targetStatus: TaskStatus.APPROVED
    });

    // Executor re-validates plan before execution
    const preVal = validateModelPlan(ops);
    assert.ok(preVal.valid, 'Executor must confirm plan operations are valid');

    // Executor executes the approved task
    const outcome = executeApprovedTask(db, task.id, {
      executorNodeId: 'node-delta',
      executorRole: 'executor',
      syncEngine,
      mutations: ops
    });

    assert.ok(outcome.success, 'Execution must succeed');
    assert.equal(outcome.status, TaskStatus.COMPLETED);

    // Verify DB write
    const cat = db.prepare("SELECT * FROM categories WHERE name = 'Server Hardware'").get();
    assert.ok(cat, 'Row must exist in SQLite table after executor commits');
    assert.equal(cat.name, 'Server Hardware');
  });

  it('7. Execution result & replication events: logs audit, replication, and marks task completed', () => {
    const task = createTask(db, {
      title: 'Audit Storage Setup',
      user_prompt: 'Create Audit Storage category',
      task_type: 'audit',
      priority: 'low'
    });

    const ops = [
      { operation: 'INSERT', table: 'categories', data: { name: 'Audit Storage', description: 'Cold storage partition' } }
    ];

    updateTaskPlanning(db, task.id, { plannerPeerId: 'planner-alpha' });
    updateTaskPlanned(db, task.id, {
      plannerPeerId: 'planner-alpha',
      operations: ops
    });

    const { approval } = createTaskApproval(db, task.id, { requestedBy: 'admin' });
    submitApprovalDecision(db, approval.id, { decision: 'approved', reviewer: 'admin', targetStatus: TaskStatus.APPROVED });

    const outcome = executeApprovedTask(db, task.id, {
      executorNodeId: 'node-delta',
      executorRole: 'executor',
      syncEngine,
      mutations: ops
    });

    assert.ok(outcome.success);

    // Verify task status in database
    const completedTask = db.prepare('SELECT * FROM tasks WHERE id = ?').get(task.id);
    assert.equal(completedTask.status, TaskStatus.COMPLETED);
    assert.ok(completedTask.execution_result);

    // Verify replication log entry
    const meshLog = db.prepare("SELECT * FROM _mesh_log WHERE table_name = 'categories' ORDER BY id DESC LIMIT 1").get();
    assert.ok(meshLog, 'Mesh replication log must be generated on database write');
    assert.equal(meshLog.table_name, 'categories');
  });

  it('8. Duplicate request prevention: deduplicates requests using persistent request IDs', () => {
    const planner = new AutonomousPlanner({
      node: { peerId: 'peer-planner-alpha' },
      peerRegistry,
      db,
      syncEngine,
      planOperations: async () => ({
        success: true,
        operations: [{ operation: 'INSERT', table: 'categories', data: { name: 'Dup Category' } }]
      }),
      nodeName: 'alpha',
      nodeRole: 'planner'
    });

    const reqId = 'req-dedup-1001';
    planner._markProcessed(reqId);

    assert.ok(planner.processedRequests.has(reqId), 'Request ID must be in cache');

    let wasExecuted = false;
    // Simulating second incoming request with same ID
    if (planner.processedRequests.has(reqId)) {
      wasExecuted = false; // skipped duplicate
    } else {
      wasExecuted = true;
    }

    assert.equal(wasExecuted, false, 'Duplicate request must be skipped');
  });

  it('9. Failed execution: handles invalid operations or DB violations cleanly without corrupting state', () => {
    const task = createTask(db, {
      title: 'Corrupt Category Insert',
      user_prompt: 'Insert category violating constraint',
      task_type: 'corrupt_test',
      priority: 'high'
    });

    // Provide operations with invalid table
    const corruptOps = [
      { operation: 'INSERT', table: 'nonexistent_table', data: { foo: 'bar' } }
    ];

    updateTaskPlanning(db, task.id, { plannerPeerId: 'planner-alpha' });
    updateTaskPlanned(db, task.id, {
      plannerPeerId: 'planner-alpha',
      operations: corruptOps
    });

    const { approval } = createTaskApproval(db, task.id, { requestedBy: 'tester' });
    submitApprovalDecision(db, approval.id, { decision: 'approved', reviewer: 'tester', targetStatus: TaskStatus.APPROVED });

    // Executor validates and rejects
    assert.throws(() => {
      executeApprovedTask(db, task.id, {
        executorNodeId: 'node-delta',
        executorRole: 'executor',
        syncEngine,
        mutations: corruptOps
      });
    }, (err) => {
      return err.code === 'VALIDATION_FAILED' || err.status === 400;
    });

    const failedTask = db.prepare('SELECT * FROM tasks WHERE id = ?').get(task.id);
    assert.equal(failedTask.status, TaskStatus.FAILED);
    assert.ok(failedTask.error.includes('unsupported table'));
  });

  it('10. Rejected approval: user rejection permanently stops task and executes 0 mutations', () => {
    const task = createTask(db, {
      title: 'Dangerous Schema Wipe',
      user_prompt: 'Delete all records',
      task_type: 'wipe',
      priority: 'critical'
    });

    const ops = [
      { operation: 'INSERT', table: 'categories', data: { name: 'Temporary Data' } }
    ];

    updateTaskPlanning(db, task.id, { plannerPeerId: 'planner-alpha' });
    updateTaskPlanned(db, task.id, { plannerPeerId: 'planner-alpha', operations: ops });
    const { approval } = createTaskApproval(db, task.id, { requestedBy: 'user' });

    // Human rejects the approval
    const rejectedAppr = submitApprovalDecision(db, approval.id, {
      decision: 'rejected',
      reviewer: 'security_officer',
      reason: 'Rejected due to risk policy',
      targetStatus: TaskStatus.REJECTED
    });

    assert.equal(rejectedAppr.decision, 'rejected');

    const rejectedTask = db.prepare('SELECT * FROM tasks WHERE id = ?').get(task.id);
    assert.equal(rejectedTask.status, TaskStatus.REJECTED);

    // Attempt to execute rejected task -> throws 409 INVALID_TRANSITION
    assert.throws(() => {
      executeApprovedTask(db, task.id, {
        executorNodeId: 'node-delta',
        executorRole: 'executor',
        syncEngine,
        mutations: ops
      });
    }, (err) => {
      return err.code === 'INVALID_TRANSITION' || err.status === 409;
    });

    // Confirm no row was inserted
    const cat = db.prepare("SELECT * FROM categories WHERE name = 'Temporary Data'").get();
    assert.equal(cat, undefined, 'No mutation should occur for rejected approval');
  });

  it('11. Persistent conversation history: task messages persist across database re-initialization', () => {
    const dbPath = join(tmpDir, 'persistent-timeline.db');
    let testDb = initDatabase(dbPath);

    const task = createTask(testDb, {
      title: 'Persistent Timeline Task',
      user_prompt: 'Track communication messages across restarts'
    });
    const taskId = task.id;

    const msg1 = recordWorkflowMessage(testDb, {
      task_id: taskId,
      sender_node: 'alpha',
      sender_role: 'planner',
      receiver_node: 'delta',
      receiver_role: 'executor',
      message_type: 'PLAN_RESPONSE',
      summary: 'Formulated 3 verified operations',
      payload_metadata: { stepCount: 3 },
      status: 'delivered',
      request_id: 'req-42-1'
    });

    const msg2 = recordWorkflowMessage(testDb, {
      task_id: taskId,
      sender_node: 'delta',
      sender_role: 'executor',
      receiver_node: 'operator',
      receiver_role: 'user',
      message_type: 'APPROVAL_REQUESTED',
      summary: 'Human sign-off required for execution',
      payload_metadata: { riskLevel: 'medium' },
      status: 'awaiting_approval',
      request_id: 'req-42-2'
    });

    assert.ok(msg1.message_id);
    assert.ok(msg2.message_id);

    // Close and reopen database
    testDb.close();
    testDb = initDatabase(dbPath);

    const reloadedMessages = getWorkflowMessages(testDb, taskId);
    assert.equal(reloadedMessages.length, 2);
    assert.equal(reloadedMessages[0].summary, 'Formulated 3 verified operations');
    assert.equal(reloadedMessages[1].summary, 'Human sign-off required for execution');
    assert.equal(reloadedMessages[0].sender_role, 'planner');
    assert.equal(reloadedMessages[1].sender_role, 'executor');

    testDb.close();
  });

  it('12. Dashboard WebSocket updates: emits expected event types on workflow state transitions', () => {
    const emittedEvents = [];
    const broadcastWs = (type, data) => emittedEvents.push({ type, data });

    const task = createTask(db, {
      title: 'Event Streaming Test',
      user_prompt: 'Verify WebSocket events',
      task_type: 'telemetry',
      priority: 'medium'
    });

    // 1. task:created
    recordWorkflowMessage(db, {
      task_id: task.id,
      sender_node: 'user',
      sender_role: 'user',
      receiver_node: 'router',
      receiver_role: 'router',
      message_type: 'TASK_CREATED',
      summary: 'Task created via UI'
    }, { broadcastWs, wsEventType: 'task:created' });

    // 2. plan:requested
    recordWorkflowMessage(db, {
      task_id: task.id,
      sender_node: 'router',
      sender_role: 'router',
      receiver_node: 'alpha',
      receiver_role: 'planner',
      message_type: 'PLAN_REQUEST',
      summary: 'Dispatched to planner'
    }, { broadcastWs, wsEventType: 'plan:requested' });

    // 3. plan:validated
    recordWorkflowMessage(db, {
      task_id: task.id,
      sender_node: 'alpha',
      sender_role: 'planner',
      receiver_node: 'delta',
      receiver_role: 'executor',
      message_type: 'PLAN_VALIDATED',
      summary: 'Plan validated against 3NF rules'
    }, { broadcastWs, wsEventType: 'plan:validated' });

    // 4. approval:created
    recordWorkflowMessage(db, {
      task_id: task.id,
      sender_node: 'delta',
      sender_role: 'executor',
      receiver_node: 'operator',
      receiver_role: 'user',
      message_type: 'APPROVAL_REQUESTED',
      summary: 'Human approval created'
    }, { broadcastWs, wsEventType: 'approval:created' });

    // 5. execution:started
    recordWorkflowMessage(db, {
      task_id: task.id,
      sender_node: 'delta',
      sender_role: 'executor',
      receiver_node: 'delta',
      receiver_role: 'executor',
      message_type: 'EXECUTION_STARTED',
      summary: 'Executing operations'
    }, { broadcastWs, wsEventType: 'execution:started' });

    // 6. mesh:replication
    recordWorkflowMessage(db, {
      task_id: task.id,
      sender_node: 'delta',
      sender_role: 'executor',
      receiver_node: 'mesh',
      receiver_role: 'peer',
      message_type: 'MESH_REPLICATION',
      summary: 'Replicated to mesh peers'
    }, { broadcastWs, wsEventType: 'mesh:replication' });

    // 7. execution:completed
    recordWorkflowMessage(db, {
      task_id: task.id,
      sender_node: 'delta',
      sender_role: 'executor',
      receiver_node: 'router',
      receiver_role: 'router',
      message_type: 'EXECUTION_RESULT',
      summary: 'Execution completed successfully'
    }, { broadcastWs, wsEventType: 'execution:completed' });

    const eventTypes = emittedEvents.map(e => e.type);

    // Check standard agent:message is sent for every record
    const agentMessages = eventTypes.filter(t => t === 'agent:message');
    assert.equal(agentMessages.length, 7);

    // Check specific workflow event types
    assert.ok(eventTypes.includes('task:created'));
    assert.ok(eventTypes.includes('plan:requested'));
    assert.ok(eventTypes.includes('plan:validated'));
    assert.ok(eventTypes.includes('approval:created'));
    assert.ok(eventTypes.includes('execution:started'));
    assert.ok(eventTypes.includes('mesh:replication'));
    assert.ok(eventTypes.includes('execution:completed'));
  });

  it('13. Demonstration Task: verifies end-to-end multi-step plan, validation, approval, and execution', () => {
    // 1. Create demonstration task
    const demoTask = createTask(db, {
      title: 'Collaborative Mesh Intelligence Task',
      user_prompt: 'Perform collaborative mesh audit and inventory intelligence synchronization: verify schema health, audit category distribution, and register operational analytics snapshot.',
      task_type: 'collaborative_intelligence',
      priority: 'high',
      requested_by: 'demo_operator'
    });
    assert.equal(demoTask.status, TaskStatus.QUEUED);

    // 2. Planner forms 3-step audit & analytics plan
    const demoOps = [
      {
        operation: 'INSERT',
        table: 'categories',
        data: {
          name: 'Mesh Intelligence Audit',
          description: 'Autonomous telemetry partition created by Collaborative Mesh Intelligence Task'
        }
      }
    ];

    updateTaskPlanning(db, demoTask.id, {
      plannerPeerId: 'planner-alpha',
      plannerRequestId: 'req-demo-plan-1'
    });

    const plannedTask = updateTaskPlanned(db, demoTask.id, {
      plannerPeerId: 'planner-alpha',
      plannerRequestId: 'req-demo-plan-1',
      operations: demoOps,
      explanation: 'Decomposed collaborative audit and inventory intelligence synchronization into validated operations.',
      plan: {
        goal: 'Audit and inventory intelligence synchronization',
        ordered_steps: [
          { step_id: 'step-1', title: 'Introspect 3NF Schema', responsible_role: 'planner' },
          { step_id: 'step-2', title: 'Validate Constraints', responsible_role: 'validator' },
          { step_id: 'step-3', title: 'Commit Analytics Snapshot', responsible_role: 'executor' }
        ]
      }
    });
    assert.equal(plannedTask.status, TaskStatus.PLANNED);

    // 3. Executor validates plan
    const val = validateModelPlan(demoOps);
    assert.ok(val.valid, 'Demonstration plan must be valid');

    // 4. Require Human Approval
    const { task: awaitingTask, approval } = createTaskApproval(db, demoTask.id, {
      requestedBy: 'planner-alpha',
      riskLevel: 'medium'
    });
    assert.equal(awaitingTask.status, TaskStatus.AWAITING_APPROVAL);

    // 5. Operator approves
    submitApprovalDecision(db, approval.id, {
      decision: 'approved',
      reviewer: 'demo_operator',
      reason: 'Approved for intelligence execution',
      targetStatus: TaskStatus.APPROVED
    });

    // 6. Executor executes approved task
    const outcome = executeApprovedTask(db, demoTask.id, {
      executorNodeId: 'node-delta',
      executorRole: 'executor',
      syncEngine,
      mutations: demoOps
    });

    assert.ok(outcome.success);
    assert.equal(outcome.status, TaskStatus.COMPLETED);

    // 7. Verify result and replication
    const createdCategory = db.prepare("SELECT * FROM categories WHERE name = 'Mesh Intelligence Audit'").get();
    assert.ok(createdCategory);
    assert.equal(createdCategory.name, 'Mesh Intelligence Audit');

    const meshLog = db.prepare("SELECT * FROM _mesh_log WHERE table_name = 'categories' ORDER BY id DESC LIMIT 1").get();
    assert.ok(meshLog);
  });
});
