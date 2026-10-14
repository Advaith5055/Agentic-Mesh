/**
 * @fileoverview Autonomous Planner Engine for Agentic Mesh.
 * Receives natural-language tasks autonomously, decomposes them into ordered steps across
 * mesh roles (planner, validator, executor, reporter), inspects peer topology, validates
 * schemas, enforces strict separation of authority (never mutates business SQLite), and
 * streams visible progress events to persistent audit history and the dashboard.
 * @module agents/autonomous-planner
 */

import { v4 as uuidv4 } from 'uuid';
import { logger } from '../utils/logger.js';
import { config } from '../utils/config.js';
import {
  MessageType,
  createEnvelope,
  createPlanResponse,
  createExecutionRequest
} from '../utils/protocol.js';
import { publishControl } from '../p2p/gossip.js';
import { validateModelPlan } from './model-validator.js';
import { recordAuditEvent, recordWorkflowMessage, TaskStatus, applyWorkflowMutation } from '../mesh/workflow-service.js';

export class AutonomousPlanner {
  /**
   * @param {Object} options
   * @param {import('libp2p').Libp2p} options.node - libp2p node instance
   * @param {import('../p2p/discovery.js').PeerRegistry} options.peerRegistry - Peer registry tracking roles
   * @param {import('better-sqlite3').Database} options.db - SQLite database instance
   * @param {import('../db/sync.js').SyncEngine} [options.syncEngine] - Vector clock sync engine
   * @param {Function} options.planOperations - Function to decompose prompt into DB operations
   * @param {Function} [options.broadcastWs] - Callback to broadcast WebSocket events to dashboard
   * @param {string} [options.nodeName] - Local node name (e.g. 'delta')
   * @param {string} [options.nodeRole] - Local node role ('planner')
   */
  constructor({
    node,
    peerRegistry,
    db,
    syncEngine = null,
    planOperations,
    broadcastWs = null,
    nodeName = config.NODE_NAME,
    nodeRole = config.NODE_ROLE
  }) {
    this.node = node;
    this.peerRegistry = peerRegistry;
    this.db = db;
    this.syncEngine = syncEngine;
    this.planOperations = planOperations;
    this.broadcastWs = broadcastWs;
    this.nodeName = nodeName;
    this.nodeRole = nodeRole;

    /** @type {Set<string>} Deduplication cache for request IDs */
    this.processedRequests = new Set();
    this.maxProcessedCache = 2000;
  }

  /**
   * Broadcasts an event to connected dashboard WebSocket clients.
   * @param {string} type - WebSocket event type
   * @param {Object} data - Event payload
   */
  emitEvent(type, data) {
    if (typeof this.broadcastWs === 'function') {
      try {
        this.broadcastWs(type, data);
      } catch (err) {
        logger.debug(`[Planner] Failed to broadcast WS event ${type}: ${err.message}`);
      }
    }
  }

  /**
   * Sends a visible progress message to the persistent timeline, dashboard, and logs.
   * @param {Object} params
   * @param {string} params.taskId
   * @param {string} params.requestId
   * @param {string} params.phase - 'received' | 'understood' | 'plan_created' | 'step_assignments' | 'validation_request' | 'completed' | 'failed' | 'collaborating'
   * @param {string} params.text - Concise operational message
   * @param {Object} [params.plan]
   * @param {Array} [params.steps]
   * @param {string} [params.receiverNode='all']
   * @param {string} [params.receiverRole='executor']
   */
  sendProgressMessage({ taskId, requestId, phase, text, plan = null, steps = null, receiverNode = 'all', receiverRole = 'executor' }) {
    logger.ai(`[Planner][${phase}] ${text}`);

    const messageId = `msg-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
    const meta = {
      phase,
      plan: plan || null,
      steps: steps || plan?.ordered_steps || null,
      brain: plan?.brain || {
        model: config.OLLAMA_MODEL,
        host: config.OLLAMA_HOST,
        latencyMs: plan?.metrics?.latencyMs || null
      },
      text
    };

    const typeMap = {
      received: 'PLAN_REQUEST_RECEIVED',
      understood: 'TASK_UNDERSTOOD',
      plan_created: 'PLAN_CREATED',
      step_assignments: 'STEPS_ASSIGNED',
      validation_request: 'PLAN_VALIDATED',
      completed: 'PLAN_RESPONSE_SENT',
      collaborating: 'EXECUTION_HANDOFF',
      repair: 'PLAN_REPAIR',
      failed: 'PLAN_FAILED'
    };

    const messageType = typeMap[phase] || `PLAN_${(phase || 'info').toUpperCase()}`;

    try {
      recordWorkflowMessage(this.db, {
        task_id: taskId,
        message_id: messageId,
        sender_node: this.nodeName,
        sender_role: 'planner',
        receiver_node: receiverNode,
        receiver_role: receiverRole,
        message_type: messageType,
        summary: text,
        payload_metadata: meta,
        status: 'delivered',
        request_id: requestId,
        timestamp: new Date().toISOString()
      }, {
        broadcastWs: this.broadcastWs
      });
    } catch (_err) {
      // Fallback WS emit if DB is unavailable
      this.emitEvent('agent:message', {
        id: messageId,
        message_id: messageId,
        taskId,
        task_id: taskId,
        requestId,
        request_id: requestId,
        sender: this.nodeName,
        sender_node: this.nodeName,
        role: 'planner',
        sender_role: 'planner',
        receiver_node: receiverNode,
        receiver_role: receiverRole,
        message_type: messageType,
        summary: text,
        type: 'progress',
        phase,
        text,
        plan: plan || null,
        steps: steps || plan?.ordered_steps || null,
        timestamp: Date.now()
      });
    }
  }

  /**
   * Inspects available peer roles in the mesh before assigning work.
   * @returns {{ planner: string, executor: string, validator: string, reporter: string }}
   */
  inspectPeerRoles() {
    let executorNode = null;
    let validatorNode = null;
    let plannerNode = this.nodeName || 'delta';
    let reporterNode = this.nodeName || 'delta';

    // 1. Inspect connected peers from peerRegistry
    if (this.peerRegistry && typeof this.peerRegistry.getPeers === 'function') {
      const peers = this.peerRegistry.getPeers();
      for (const p of peers) {
        const role = p.role || p.nodeRole;
        const name = p.nodeName || p.name || p.peerId;
        if (role === 'executor' && !executorNode) executorNode = name;
        if (role === 'validator' && !validatorNode) validatorNode = name;
        if (role === 'reporter' && !reporterNode) reporterNode = name;
      }
    }

    // 2. Fallback to mesh_nodes database table
    try {
      const dbNodes = this.db.prepare("SELECT * FROM mesh_nodes WHERE status = 'active'").all();
      for (const n of dbNodes) {
        if (n.role === 'executor' && !executorNode) executorNode = n.name || n.id;
        if (n.role === 'validator' && !validatorNode) validatorNode = n.name || n.id;
        if (n.role === 'reporter' && !reporterNode) reporterNode = n.name || n.id;
      }
    } catch {}

    // 3. Fallbacks based on system architecture configuration
    if (!executorNode) {
      executorNode = config.EXECUTOR_PEER || 'alpha';
    }
    if (!validatorNode) {
      validatorNode = 'gamma';
    }

    return {
      planner: plannerNode,
      executor: executorNode,
      validator: validatorNode,
      reporter: reporterNode
    };
  }

  /**
   * Formulates a concise operational goal from a user prompt.
   * Never exposes hidden chain-of-thought.
   * @param {string} prompt
   * @returns {string}
   */
  formulateGoal(prompt) {
    if (!prompt) return 'Process mesh task';
    const clean = prompt.replace(/\s+/g, ' ').trim();
    if (clean.length <= 120) return clean;
    return clean.slice(0, 117) + '...';
  }

  /**
   * Assesses risk level based on planned operations.
   * @param {Array<Object>} operations
   * @returns {'low'|'medium'|'high'}
   */
  assessRisk(operations) {
    if (!Array.isArray(operations) || operations.length === 0) return 'low';
    const hasDelete = operations.some(o => o.operation === 'DELETE');
    if (hasDelete) return 'high';
    if (operations.length > 3) return 'high';
    const hasHighPrice = operations.some(o => o.data && typeof o.data.price === 'number' && o.data.price > 500);
    if (hasHighPrice) return 'medium';
    return 'low';
  }

  /**
   * Autonomously processes a task by ID.
   * Can be triggered on task creation or when a queued task is detected.
   * @param {string} taskId
   * @returns {Promise<Object>}
   */
  async processTaskAutonomously(taskId) {
    try {
      const task = this.db.prepare('SELECT * FROM tasks WHERE id = ?').get(taskId);
      if (!task) {
        logger.warn(`[Planner] Cannot autonomously process missing task: ${taskId}`);
        return { success: false, error: 'Task not found' };
      }

      // Check if already planned or executing
      if (task.status !== TaskStatus.QUEUED && task.status !== TaskStatus.PLANNING) {
        return { success: true, task, message: `Task already in status '${task.status}'` };
      }

      const requestId = `req-auto-${uuidv4().slice(0, 8)}`;
      logger.ai(`[Planner] Autonomously initiating planning for task ${taskId}: "${task.user_prompt}"`);

      return await this.executeAutonomousPlan({
        taskId,
        requestId,
        prompt: task.user_prompt,
        requester: task.requested_by || 'autonomous_planner'
      });
    } catch (err) {
      logger.error(`[Planner] Autonomous task processing error: ${err.message}`);
      return { success: false, error: err.message };
    }
  }

  /**
   * Handles an incoming PLAN_REQUEST control message received through GossipSub.
   * @param {Object} envelope - Control envelope
   * @param {string} fromPeer - Sender peer ID
   * @returns {Promise<Object>}
   */
  async handlePlanRequest(envelope, fromPeer) {
    const { requestId, taskId, prompt, requesterPeerId } = envelope.payload || {};
    const requester = requesterPeerId || envelope.sender || fromPeer;

    if (!requestId || !prompt) {
      logger.warn(`[Planner] Dropped malformed PLAN_REQUEST from ${requester.slice(0, 12)}... (missing requestId or prompt)`);
      return;
    }

    if (this.nodeRole !== 'planner' && this.nodeRole !== 'peer') {
      logger.warn(`[Planner] Dropped PLAN_REQUEST: node role is '${this.nodeRole}', not planner.`);
      return;
    }

    if (this.processedRequests.has(requestId)) {
      logger.p2p(`[Planner] Skipping duplicate PLAN_REQUEST [reqId:${requestId}]`);
      return;
    }
    this._markProcessed(requestId);

    const effectiveTaskId = taskId || `task-${uuidv4().slice(0, 8)}`;

    const planResponse = await this.executeAutonomousPlan({
      taskId: effectiveTaskId,
      requestId,
      prompt,
      requester,
      targetPeerId: requester
    });

    // Send validated PLAN_RESPONSE back through GossipSub
    try {
      const respEnvelope = createEnvelope(MessageType.PLAN_RESPONSE, planResponse, this.getPeerId());
      await publishControl(this.node, respEnvelope);
      logger.p2p(`[Planner] Dispatched PLAN_RESPONSE for [reqId:${requestId}] to ${requester.slice(0, 12)}...`);
    } catch (err) {
      logger.error(`[Planner] Failed to publish PLAN_RESPONSE: ${err.message}`);
    }

    return planResponse;
  }

  /**
   * Main Autonomous Planning Pipeline:
   * 1. Task Received
   * 2. Task Understood
   * 3. Inspect Peer Roles
   * 4. Decompose Steps with Validation & Retry
   * 5. Step Assignments
   * 6. Validation Verification
   * 7. Plan Completed
   * 8. Autonomous Collaboration with Executor
   *
   * @param {Object} params
   * @param {string} params.taskId
   * @param {string} params.requestId
   * @param {string} params.prompt
   * @param {string} params.requester
   * @param {string} [params.targetPeerId]
   * @returns {Promise<Object>} Structured PLAN_RESPONSE payload
   */
  async executeAutonomousPlan({ taskId, requestId, prompt, requester, targetPeerId = null }) {
    const peerId = this.getPeerId();

    // ── 1. TASK RECEIVED ─────────────────────────────────────────────────────
    this.emitEvent('plan:requested', {
      taskId,
      requestId,
      prompt,
      requester,
      timestamp: Date.now()
    });

    this.sendProgressMessage({
      taskId,
      requestId,
      phase: 'received',
      text: `Request received: Task [${requestId.slice(0, 8)}] "${prompt}". Initializing autonomous planning.`
    });

    recordAuditEvent(this.db, {
      entity_type: 'task',
      entity_id: taskId,
      action: 'PLAN_REQUEST_RECEIVED',
      actor: this.nodeName,
      details: { requestId, prompt, requester }
    });

    // ── 2. TASK UNDERSTOOD ───────────────────────────────────────────────────
    const goal = this.formulateGoal(prompt);
    this.sendProgressMessage({
      taskId,
      requestId,
      phase: 'understood',
      text: `Understanding the task: ${goal}. Introspecting schema and querying active peer topology.`
    });

    recordAuditEvent(this.db, {
      entity_type: 'task',
      entity_id: taskId,
      action: 'TASK_UNDERSTOOD',
      actor: this.nodeName,
      details: { requestId, goal, prompt }
    });

    // ── 3. INSPECT AVAILABLE PEER ROLES ──────────────────────────────────────
    const rolesMap = this.inspectPeerRoles();
    logger.ai(`[Planner] Active role topology inspected: planner=${rolesMap.planner}, executor=${rolesMap.executor}, validator=${rolesMap.validator}, reporter=${rolesMap.reporter}`);

    // ── 4. DECOMPOSE INTO STEPS & VALIDATE WITH RETRY ────────────────────────
    let currentRequestId = requestId;
    let planResult = null;
    let validation = null;
    let retryAttempt = 0;
    const maxRetries = 2;

    while (retryAttempt <= maxRetries) {
      try {
        planResult = await this.planOperations(prompt, this.db);
        if (planResult && planResult.success && Array.isArray(planResult.operations)) {
          validation = validateModelPlan(planResult.operations);
          if (validation.valid) {
            break; // Successfully formulated and validated
          }
        }

        // Encountered invalid formulation: prepare safe retry with NEW request ID
        const failureReason = validation?.errors?.join('; ') || planResult?.error || 'Unknown schema violation';
        retryAttempt++;

        if (retryAttempt <= maxRetries) {
          const nextRequestId = `${requestId}-retry-${retryAttempt}`;
          this.sendProgressMessage({
            taskId,
            requestId: currentRequestId,
            phase: 'repair',
            text: `Plan validation issue: ${failureReason}. Regenerating plan with new request ID [${nextRequestId}].`
          });
          currentRequestId = nextRequestId;
        } else {
          // Exhausted retries
          const fatalError = `Plan validation failed: ${failureReason}`;
          this.emitEvent('plan:failed', { taskId, requestId: currentRequestId, error: fatalError });
          this.sendProgressMessage({
            taskId,
            requestId: currentRequestId,
            phase: 'failed',
            text: `Planning failed: ${fatalError}`
          });
          recordAuditEvent(this.db, {
            entity_type: 'task',
            entity_id: taskId,
            action: 'PLAN_FAILED',
            actor: this.nodeName,
            details: { requestId: currentRequestId, error: fatalError }
          });
          return createPlanResponse({
            requestId: currentRequestId,
            taskId,
            success: false,
            error: fatalError,
            plannerPeerId: peerId,
            targetPeerId: targetPeerId || requester
          });
        }
      } catch (err) {
        retryAttempt++;
        if (retryAttempt > maxRetries) {
          this.emitEvent('plan:failed', { taskId, requestId: currentRequestId, error: err.message });
          return createPlanResponse({
            requestId: currentRequestId,
            taskId,
            success: false,
            error: `Autonomous planner error: ${err.message}`,
            plannerPeerId: peerId,
            targetPeerId: targetPeerId || requester
          });
        }
      }
    }

    const operations = validation.operations;
    const riskLevel = this.assessRisk(operations);
    const explanation = `Decomposed "${goal}" into 4 ordered steps across mesh roles (${operations.length} validated operation(s)).`;
    const planSummary = operations.map(o => `${o.operation} ${o.table}`).join(', ');

    // ── 5. FORMULATE ORDERED STEPS ───────────────────────────────────────────
    const ordered_steps = [
      {
        step_id: 'step-1',
        title: 'Deconstruct Instruction & Introspect Schema',
        goal: 'Analyze natural-language intent against 3NF tables and existing records',
        responsible_role: 'planner',
        assigned_node_id: rolesMap.planner,
        dependencies: [],
        required_inputs: { prompt },
        expected_outputs: { operations_count: operations.length },
        risk_level: 'low',
        validation_status: 'valid'
      },
      {
        step_id: 'step-2',
        title: 'Validate 3NF Constraints & Foreign Keys',
        goal: 'Verify column types, non-null requirements, positive prices, and category links',
        responsible_role: 'validator',
        assigned_node_id: rolesMap.validator,
        dependencies: ['step-1'],
        required_inputs: { operations },
        expected_outputs: { schema_compliance: 'passed', constraint_violations: 0 },
        risk_level: 'low',
        validation_status: 'valid'
      },
      {
        step_id: 'step-3',
        title: 'Execute Mutations Atomically',
        goal: 'Apply verified transaction batch to SQLite and advance vector clock',
        responsible_role: 'executor',
        assigned_node_id: rolesMap.executor,
        dependencies: ['step-2'],
        required_inputs: { operations, validation_verdict: 'valid' },
        expected_outputs: { committed_rows: operations.length, replication_log_id: 'auto' },
        risk_level: riskLevel,
        validation_status: 'valid'
      },
      {
        step_id: 'step-4',
        title: 'Audit & Telemetry Verification',
        goal: 'Record persistent execution receipt and broadcast dashboard telemetry',
        responsible_role: 'reporter',
        assigned_node_id: rolesMap.reporter,
        dependencies: ['step-3'],
        required_inputs: { operations_count: operations.length },
        expected_outputs: { audit_recorded: true, dashboard_notified: true },
        risk_level: 'low',
        validation_status: 'valid'
      }
    ];

    const structuredPlan = {
      task_id: taskId,
      request_id: currentRequestId,
      goal,
      explanation,
      ordered_steps,
      dependencies: ['schema_snapshot', 'active_peers'],
      responsible_role: 'planner',
      required_inputs: { prompt },
      expected_outputs: { operationsCount: operations.length },
      risk_level: riskLevel,
      validation_status: 'valid',
      operations,
      summary: planSummary,
      brain: {
        model: config.OLLAMA_MODEL,
        host: config.OLLAMA_HOST,
        latencyMs: planResult?.metrics?.latencyMs || null,
        selfCorrected: Boolean(planResult?.metrics?.selfCorrected)
      }
    };

    // ── 6. EMIT PLAN CREATED ─────────────────────────────────────────────────
    this.emitEvent('plan:created', {
      taskId,
      requestId: currentRequestId,
      plan: structuredPlan,
      operations
    });

    this.sendProgressMessage({
      taskId,
      requestId: currentRequestId,
      phase: 'plan_created',
      text: `Proposed steps: Decomposed into ${ordered_steps.length} ordered steps (${operations.length} operations). Risk level: ${riskLevel.toUpperCase()}.`,
      plan: structuredPlan
    });

    recordAuditEvent(this.db, {
      entity_type: 'task',
      entity_id: taskId,
      action: 'PLAN_CREATED',
      actor: this.nodeName,
      details: { requestId: currentRequestId, stepsCount: ordered_steps.length, riskLevel, operationsCount: operations.length }
    });

    // ── 7. EMIT STEP ASSIGNMENTS ─────────────────────────────────────────────
    for (const step of ordered_steps) {
      this.emitEvent('step:assigned', {
        taskId,
        requestId: currentRequestId,
        step,
        stepId: step.step_id,
        role: step.responsible_role,
        assignedNodeId: step.assigned_node_id
      });
    }

    this.sendProgressMessage({
      taskId,
      requestId: currentRequestId,
      phase: 'step_assignments',
      text: `Assigned steps across mesh: ${ordered_steps.map(s => `${s.step_id} (${s.responsible_role} → ${s.assigned_node_id})`).join(', ')}.`
    });

    recordAuditEvent(this.db, {
      entity_type: 'task',
      entity_id: taskId,
      action: 'STEPS_ASSIGNED',
      actor: this.nodeName,
      details: {
        requestId: currentRequestId,
        assignments: ordered_steps.map(s => ({ step: s.step_id, role: s.responsible_role, node: s.assigned_node_id }))
      }
    });

    // ── 8. VALIDATION VERIFICATION ───────────────────────────────────────────
    this.emitEvent('plan:validated', {
      taskId,
      requestId: currentRequestId,
      valid: true,
      validationStatus: 'valid',
      errors: []
    });

    this.sendProgressMessage({
      taskId,
      requestId: currentRequestId,
      phase: 'validation_request',
      text: `Validation completed: Schema allowlists and 3NF constraints passed. Validation status: VALID.`
    });

    recordAuditEvent(this.db, {
      entity_type: 'task',
      entity_id: taskId,
      action: 'PLAN_VALIDATED',
      actor: this.nodeName,
      details: { requestId: currentRequestId, validation_status: 'valid' }
    });

    // ── 9. PLAN_RESPONSE SENT ───────────────────────────────────────────────
    this.sendProgressMessage({
      taskId,
      requestId: currentRequestId,
      phase: 'completed',
      text: `PLAN_RESPONSE sent: Dispatched plan with ${operations.length} validated operations to mesh peers.`,
      plan: structuredPlan
    });

    recordAuditEvent(this.db, {
      entity_type: 'task',
      entity_id: taskId,
      action: 'PLAN_COMPLETED',
      actor: this.nodeName,
      details: { requestId: currentRequestId, goal, operationsCount: operations.length }
    });

    // ── 10. UPDATE LOCAL TASK METADATA IF TASK RECORD EXISTS ─────────────────
    try {
      const existingTask = this.db.prepare('SELECT id, status FROM tasks WHERE id = ?').get(taskId);
      if (existingTask) {
        applyWorkflowMutation(this.db, 'UPDATE', 'tasks', {
          id: taskId,
          status: TaskStatus.PLANNED,
          planner_peer_id: peerId,
          planner_request_id: currentRequestId,
          plan: JSON.stringify(structuredPlan),
          operations: JSON.stringify(operations),
          updated_at: new Date().toISOString()
        });

        this.emitEvent('task:planned', {
          id: taskId,
          status: TaskStatus.PLANNED,
          plan: structuredPlan,
          operations
        });
      }
    } catch (err) {
      logger.debug(`[Planner] Local task record update notice: ${err.message}`);
    }

    return createPlanResponse({
      requestId: currentRequestId,
      taskId,
      success: true,
      operations,
      explanation,
      planSummary,
      validation: { valid: true, operationsCount: operations.length },
      error: null,
      plannerPeerId: peerId,
      targetPeerId: targetPeerId || requester,
      raw: planResult?.raw || null,
      plan: structuredPlan
    });
  }

  /**
   * Autonomously collaborates with the Executor node by dispatching an EXECUTION_REQUEST
   * over the mesh control bus and streaming progress.
   *
   * @param {Object} params
   * @param {string} params.taskId
   * @param {string} params.requestId
   * @param {string} params.prompt
   * @param {Array<Object>} params.operations
   * @param {Object} params.structuredPlan
   * @param {string} params.executorNodeId
   */
  async collaborateWithExecutor({ taskId, requestId, prompt, operations, structuredPlan, executorNodeId }) {
    try {
      this.sendProgressMessage({
        taskId,
        requestId,
        phase: 'collaborating',
        text: `Autonomously dispatching EXECUTION_REQUEST for ${operations.length} operations to Executor node (${executorNodeId}).`
      });

      const execPayload = createExecutionRequest({
        requestId: `exec-${uuidv4().slice(0, 8)}`,
        taskId,
        title: structuredPlan?.title || prompt,
        prompt,
        plan: structuredPlan,
        operations,
        routerPeerId: this.getPeerId(),
        plannerPeerId: this.getPeerId(),
        plannerNodeName: this.nodeName,
        autoApprove: true,
        targetPeerId: executorNodeId
      });

      const envelope = createEnvelope(MessageType.EXECUTION_REQUEST, execPayload, this.getPeerId());
      await publishControl(this.node, envelope);

      logger.p2p(`[Planner] Dispatched autonomous EXECUTION_REQUEST to Executor ${executorNodeId}`);

      recordAuditEvent(this.db, {
        entity_type: 'task',
        entity_id: taskId,
        action: 'EXECUTION_DISPATCHED_TO_PEER',
        actor: this.nodeName,
        details: { executorNodeId, operationsCount: operations.length }
      });
    } catch (err) {
      logger.warn(`[Planner] Autonomous collaboration handoff notice: ${err.message}`);
    }
  }

  /**
   * Returns current peer ID string.
   * @returns {string}
   */
  getPeerId() {
    return this.node && this.node.peerId ? this.node.peerId.toString() : 'local-planner';
  }

  /**
   * Records a request ID in the processed cache.
   * @param {string} reqId
   * @private
   */
  _markProcessed(reqId) {
    if (this.processedRequests.size >= this.maxProcessedCache) {
      const oldest = this.processedRequests.values().next().value;
      if (oldest) this.processedRequests.delete(oldest);
    }
    this.processedRequests.add(reqId);
  }
}
