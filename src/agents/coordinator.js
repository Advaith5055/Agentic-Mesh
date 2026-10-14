/**
 * @fileoverview Mesh Agent Coordinator — orchestrates role-based task delegation
 * between Router, Planner, and Executor nodes in the P2P mesh network.
 * @module agents/coordinator
 */

import { v4 as uuidv4 } from 'uuid';
import { logger } from '../utils/logger.js';
import { config } from '../utils/config.js';
import {
  MessageType,
  createEnvelope,
  createPlanRequest,
  createPlanResponse,
  createExecutionRequest,
  createExecutionResponse,
  createApproval
} from '../utils/protocol.js';
import { publishControl } from '../p2p/gossip.js';
import { validateModelPlan } from './model-validator.js';
import { AutonomousPlanner } from './autonomous-planner.js';
import { recordWorkflowMessage, TaskStatus, submitApprovalDecision } from '../mesh/workflow-service.js';

/**
 * Coordinates inter-role agent communication across the mesh.
 */
export class MeshCoordinator {
  /**
   * @param {Object} options
   * @param {import('libp2p').Libp2p} options.node - libp2p node instance
   * @param {import('../p2p/discovery.js').PeerRegistry} options.peerRegistry - Peer registry tracking roles
   * @param {import('better-sqlite3').Database} options.db - SQLite database instance
   * @param {import('../db/sync.js').SyncEngine} options.syncEngine - Vector clock sync engine
   * @param {Function} options.planOperations - Local planning function (prompt, db)
   * @param {Function} options.executeOperations - Local execution function (ops, db, syncEngine, broadcastTx)
   * @param {Function} options.broadcastTx - Broadcast transaction callback
   */
  constructor({
    node,
    peerRegistry,
    db,
    syncEngine,
    planOperations,
    executeOperations,
    executeApprovedTask = null,
    broadcastTx,
    nodeRole = null,
    broadcastWs = null
  }) {
    this.node = node;
    this.peerRegistry = peerRegistry;
    this.db = db;
    this.syncEngine = syncEngine;
    this.planOperations = planOperations;
    this.executeOperations = executeOperations;
    this.executeApprovedTask = executeApprovedTask;
    this.broadcastTx = broadcastTx;
    this.broadcastWs = broadcastWs;
    this.nodeRole = nodeRole || (typeof node?.peerId?.toString === 'function' && node.peerId.toString().includes('executor') ? 'executor' : (typeof node?.peerId?.toString === 'function' && node.peerId.toString().includes('planner') ? 'planner' : (typeof node?.peerId?.toString === 'function' && node.peerId.toString().includes('router') ? 'router' : config.NODE_ROLE)));

    this.autonomousPlanner = new AutonomousPlanner({
      node: this.node,
      peerRegistry: this.peerRegistry,
      db: this.db,
      syncEngine: this.syncEngine,
      planOperations: this.planOperations,
      broadcastWs: this.broadcastWs,
      nodeName: config.NODE_NAME,
      nodeRole: this.nodeRole
    });

    /** @type {Map<string, { resolve: Function, reject: Function, timer: NodeJS.Timeout }>} */
    this.pendingPlanRequests = new Map();

    /** @type {Map<string, { resolve: Function, reject: Function, timer: NodeJS.Timeout }>} */
    this.pendingExecutionRequests = new Map();

    /** @type {Map<string, string>} Map proposalId -> taskId for remote executors */
    this.proposalToTaskMap = new Map();

    /** @type {Set<string>} Processed request IDs to prevent duplicate work */
    this.processedRequests = new Set();
    this.maxProcessedCache = 2000;
  }

  get peerId() {
    return this.node?.peerId ? this.node.peerId.toString() : '';
  }

  /**
   * Marks a request ID as seen, evicting the oldest if exceeding cache size.
   * @private
   */
  _markProcessed(requestId) {
    if (this.processedRequests.size >= this.maxProcessedCache) {
      const oldest = this.processedRequests.values().next().value;
      this.processedRequests.delete(oldest);
    }
    this.processedRequests.add(requestId);
  }

  // ───────────────────────────────────────────────────────────────────────────
  // ROUTER FLOW: DISPATCHING REQUESTS
  // ───────────────────────────────────────────────────────────────────────────

  /**
   * Dispatches a planning request to a connected Planner peer.
   * Awaits PLAN_RESPONSE with a timeout and safe request correlation.
   * 
   * @param {string|Object} params - The natural language request prompt or parameters object.
   * @param {Object} [options={}]
   * @param {number} [options.timeoutMs] - Timeout in ms.
   * @returns {Promise<{ success: boolean, operations: Array<Object>, error?: string, raw?: string, plannerPeerId?: string, explanation?: string, planSummary?: string, validation?: Object, taskId?: string }>}
   */
  async requestPlanFromMesh(params, options = {}) {
    const prompt = typeof params === 'string' ? params : (params?.prompt || '');
    const taskId = typeof params === 'object' ? params.taskId : options.taskId;
    const taskType = (typeof params === 'object' ? (params.taskType || params.task_type) : options.taskType) || 'general';
    const priority = (typeof params === 'object' ? params.priority : options.priority) || 'medium';
    const targetPeerId = (typeof params === 'object' ? (params.targetPeerId || params.targetPlannerPeerId) : options.targetPeerId) || null;
    const timeoutMs = (typeof params === 'object' ? params.timeoutMs : options.timeoutMs) || config.PLAN_TIMEOUT_MS;

    // Locate connected planner peer
    let plannerPeer = targetPeerId ? this.peerRegistry.getPeer(targetPeerId) : null;
    if (!plannerPeer) {
      const preferredPlanner = config.PLANNER_PEER || 'alpha';
      const allPeers = this.peerRegistry.getPeers();
      plannerPeer = allPeers.find(p => (p.nodeName === preferredPlanner || p.name === preferredPlanner) && p.status === 'connected')
        || this.peerRegistry.findAvailablePeerByRole('planner')
        || allPeers.find(p => p.nodeName === preferredPlanner || p.name === preferredPlanner);
    }

    // If local node is explicitly targeted or local node is a Planner with no remote peer available, plan locally
    if (targetPeerId === this.peerId || (this.nodeRole === 'planner' && !plannerPeer)) {
      logger.ai(`[Planner] Local node is Planner; formulating plan locally for task ${taskId || ''}`);
      return await this.planOperations(prompt, this.db);
    }

    if (!plannerPeer) {
      if (config.ROUTER_LOCAL_FALLBACK) {
        logger.info('[Router] No Planner peer found; falling back to local planning (ROUTER_LOCAL_FALLBACK=true)');
        return await this.planOperations(prompt, this.db);
      }
      const knownCount = this.peerRegistry.getPeerCount();
      logger.warn(`[Router] No Planner peer available in the mesh (known peers: ${knownCount})`);
      return {
        success: false,
        operations: [],
        error: `No Planner peer available in the mesh. (Ensure a node with NODE_ROLE=planner is connected, or set ROUTER_LOCAL_FALLBACK=true). Connected peers: ${knownCount}`
      };
    }

    const requestId = uuidv4();
    logger.ai(`[Router] Delegating planning to Planner ${plannerPeer.peerId.slice(0, 12)}... [reqId:${requestId}${taskId ? ` taskId:${taskId}` : ''}]`);

    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.pendingPlanRequests.delete(requestId);
        logger.warn(`[Router] Timeout (${timeoutMs}ms) waiting for PLAN_RESPONSE [reqId:${requestId}] from ${plannerPeer.peerId.slice(0, 12)}...`);
        resolve({
          success: false,
          operations: [],
          error: `Timeout (${timeoutMs}ms) waiting for PLAN_RESPONSE from Planner peer ${plannerPeer.peerId.slice(0, 12)}...`
        });
      }, timeoutMs);

      this.pendingPlanRequests.set(requestId, { resolve, timer, taskId });

      try {
        const payload = createPlanRequest({
          requestId,
          taskId,
          prompt,
          taskType,
          priority,
          requesterPeerId: this.peerId,
          targetPeerId: plannerPeer.peerId,
          timestamp: Date.now()
        });

        const envelope = createEnvelope(MessageType.PLAN_REQUEST, payload, this.peerId);

        try {
          recordWorkflowMessage(this.db, {
            task_id: taskId || `task-${requestId.slice(0, 8)}`,
            sender_node: config.NODE_NAME,
            sender_role: this.nodeRole || 'router',
            receiver_node: plannerPeer.nodeName || plannerPeer.name || plannerPeer.peerId,
            receiver_role: 'planner',
            message_type: 'PLAN_REQUEST',
            summary: `Dispatched PLAN_REQUEST to Planner node (${plannerPeer.nodeName || plannerPeer.name || 'planner'}): "${prompt}"`,
            payload_metadata: { prompt, taskType, priority },
            status: 'delivered',
            request_id: requestId
          }, {
            broadcastWs: this.broadcastWs,
            wsEventType: 'plan:requested'
          });
        } catch {}

        publishControl(this.node, envelope).catch((err) => {
          clearTimeout(timer);
          this.pendingPlanRequests.delete(requestId);
          logger.error(`[Router] Failed to publish PLAN_REQUEST: ${err.message}`);
          resolve({
            success: false,
            operations: [],
            error: `Failed to dispatch PLAN_REQUEST: ${err.message}`
          });
        });
      } catch (err) {
        clearTimeout(timer);
        this.pendingPlanRequests.delete(requestId);
        logger.error(`[Router] Failed to create PLAN_REQUEST: ${err.message}`);
        resolve({
          success: false,
          operations: [],
          error: `Failed to dispatch PLAN_REQUEST: ${err.message}`
        });
      }
    });
  }

  /**
   * Dispatches structured operations to a connected Executor peer.
   * Awaits EXECUTION_RESPONSE with a timeout and safe request correlation.
   * 
   * @param {Array<Object>|Object} params - Structured operations array or params object.
   * @param {Object} [meta={}]
   * @returns {Promise<{ completed: number, failed: number, results?: Array<any>, errors?: Array<string> }>}
   */
  async requestExecutionFromMesh(params, meta = {}) {
    const operations = Array.isArray(params) ? params : (params?.operations || []);
    const taskId = typeof params === 'object' && !Array.isArray(params) ? params.taskId : meta.taskId;
    const approvalId = typeof params === 'object' && !Array.isArray(params) ? params.approvalId : meta.approvalId;
    const prompt = typeof params === 'object' && !Array.isArray(params) ? params.prompt : meta.prompt;
    const title = typeof params === 'object' && !Array.isArray(params) ? (params.title || params.taskTitle) : meta.title;
    const plan = typeof params === 'object' && !Array.isArray(params) ? params.plan : meta.plan;
    const taskType = typeof params === 'object' && !Array.isArray(params) ? params.taskType : meta.taskType;
    const priority = typeof params === 'object' && !Array.isArray(params) ? params.priority : meta.priority;
    const approvingUser = typeof params === 'object' && !Array.isArray(params) ? params.approvingUser : meta.approvingUser;
    const approvalReason = typeof params === 'object' && !Array.isArray(params) ? params.approvalReason : meta.approvalReason;
    const targetPeerId = typeof params === 'object' && !Array.isArray(params) ? (params.targetPeerId || params.targetExecutorPeerId) : meta.targetPeerId;
    const timeoutMs = (typeof params === 'object' && !Array.isArray(params) ? params.timeoutMs : meta.timeoutMs) || config.EXECUTION_TIMEOUT_MS;

    // Locate connected executor peer
    let executorPeer = targetPeerId ? this.peerRegistry.getPeer(targetPeerId) : null;
    if (!executorPeer) {
      executorPeer = this.peerRegistry.findAvailablePeerByRole('executor');
    }

    // If local node is explicitly targeted or local node is an Executor with no remote peer available, execute locally
    if (targetPeerId === this.peerId || (this.nodeRole === 'executor' && !executorPeer)) {
      logger.db(`[Executor] Local node is Executor; executing operations locally`);
      return await this.executeOperations(operations, this.db, this.syncEngine, this.broadcastTx);
    }

    if (!executorPeer) {
      if (config.ROUTER_LOCAL_FALLBACK) {
        logger.info('[Router] No Executor peer found; executing locally (ROUTER_LOCAL_FALLBACK=true)');
        return await this.executeOperations(operations, this.db, this.syncEngine, this.broadcastTx);
      }
      const knownCount = this.peerRegistry.getPeerCount();
      logger.warn(`[Router] No Executor peer available in the mesh (known peers: ${knownCount})`);
      return {
        completed: 0,
        failed: operations.length,
        results: [],
        errors: [`No Executor peer available in the mesh. (Ensure a node with NODE_ROLE=executor is connected, or set ROUTER_LOCAL_FALLBACK=true). Connected peers: ${knownCount}`]
      };
    }

    const requestId = uuidv4();
    logger.db(`[Router] Delegating execution of ${operations.length} operations to Executor ${executorPeer.peerId.slice(0, 12)}... [reqId:${requestId}]`);

    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.pendingExecutionRequests.delete(requestId);
        logger.warn(`[Router] Timeout (${timeoutMs}ms) waiting for EXECUTION_RESPONSE [reqId:${requestId}] from ${executorPeer.peerId.slice(0, 12)}...`);
        resolve({
          completed: 0,
          failed: operations.length,
          results: [],
          errors: [`Timeout (${timeoutMs}ms) waiting for EXECUTION_RESPONSE from Executor peer ${executorPeer.peerId.slice(0, 12)}...`]
        });
      }, timeoutMs);

      this.pendingExecutionRequests.set(requestId, { resolve, timer, taskId });

      try {
        const payload = createExecutionRequest({
          requestId,
          taskId,
          approvalId,
          title,
          prompt,
          plan,
          taskType: taskType || 'collaborative_intelligence',
          priority: priority || 'high',
          plannerNodeName: config.NODE_NAME,
          autoApprove: true,
          operations,
          routerPeerId: this.peerId,
          plannerPeerId: meta.plannerPeerId || this.peerId,
          approvingUser: approvingUser || 'admin',
          approvalReason: approvalReason || '',
          targetPeerId: executorPeer.peerId
        });

        const envelope = createEnvelope(MessageType.EXECUTION_REQUEST, payload, this.peerId);

        try {
          recordWorkflowMessage(this.db, {
            task_id: taskId || `task-${requestId.slice(0, 8)}`,
            sender_node: config.NODE_NAME,
            sender_role: this.nodeRole || 'planner',
            receiver_node: executorPeer.nodeName || executorPeer.name || executorPeer.peerId,
            receiver_role: 'executor',
            message_type: 'EXECUTION_REQUEST',
            summary: `Handoff: Dispatched approved EXECUTION_REQUEST (${operations.length} operations) to Executor (${executorPeer.nodeName || executorPeer.name || 'executor'})`,
            payload_metadata: { operationsCount: operations.length, approvalId, approvingUser },
            status: 'delivered',
            request_id: requestId
          }, {
            broadcastWs: this.broadcastWs,
            wsEventType: 'execution:requested'
          });
        } catch {}

        publishControl(this.node, envelope).catch((err) => {
          clearTimeout(timer);
          this.pendingExecutionRequests.delete(requestId);
          logger.error(`[Router] Failed to publish EXECUTION_REQUEST: ${err.message}`);
          resolve({
            completed: 0,
            failed: operations.length,
            results: [],
            errors: [`Failed to dispatch EXECUTION_REQUEST: ${err.message}`]
          });
        });
      } catch (err) {
        clearTimeout(timer);
        this.pendingExecutionRequests.delete(requestId);
        logger.error(`[Router] Failed to create EXECUTION_REQUEST: ${err.message}`);
        resolve({
          completed: 0,
          failed: operations.length,
          results: [],
          errors: [`Failed to dispatch EXECUTION_REQUEST: ${err.message}`]
        });
      }
    });
  }

  /**
   * Dispatches human approval decision over the mesh network to the target Executor.
   *
   * @param {Object} params
   * @param {string} params.taskId - The task identifier.
   * @param {string} [params.approvalId] - The approval record ID.
   * @param {string} [params.decision='approved'] - 'approved' or 'rejected'.
   * @param {string} [params.reviewer='operator'] - Reviewer name.
   * @param {string} [params.reason=''] - Optional reason for decision.
   * @param {string} [params.targetPeerId] - Target executor peer ID.
   * @returns {Promise<Object>}
   */
  async publishApproval({ taskId, approvalId, decision = 'approved', reviewer = 'operator', reason = '', targetPeerId }) {
    let target = targetPeerId;
    if (!target) {
      const executorPeer = this.peerRegistry.findAvailablePeerByRole('executor');
      if (executorPeer) target = executorPeer.peerId;
    }

    const payload = createApproval({
      requestId: uuidv4(),
      taskId,
      approvalId,
      decision,
      reviewer,
      reason,
      targetPeerId: target,
      requesterPeerId: this.peerId
    });
    const envelope = createEnvelope(MessageType.APPROVAL, payload, this.peerId);
    await publishControl(this.node, envelope);
    logger.info(`[Coordinator] Broadcasted APPROVAL decision='${decision}' for task ${taskId} to target peer ${target || 'mesh'}`);
    return envelope;
  }

  // ───────────────────────────────────────────────────────────────────────────
  // RESPONSE HANDLERS (CORRELATING PROMISES)
  // ───────────────────────────────────────────────────────────────────────────

  /**
   * Handles incoming PLAN_RESPONSE message on the requesting node.
   * 
   * @param {Object} envelope - The decoded envelope.
   * @param {string} fromPeer - Sender peer ID.
   */
  handlePlanResponse(envelope, fromPeer) {
    const { requestId, taskId, success, operations, error, plannerPeerId, raw, explanation, planSummary, validation } = envelope.payload || {};
    if (!requestId) return;

    const pending = this.pendingPlanRequests.get(requestId);
    if (!pending) {
      // Not a request we are waiting for, or already timed out
      return;
    }

    clearTimeout(pending.timer);
    this.pendingPlanRequests.delete(requestId);

    const planner = plannerPeerId || fromPeer;
    if (success) {
      logger.ai(`[Router] Received successful PLAN_RESPONSE [reqId:${requestId}] from Planner ${planner.slice(0, 12)}... (${operations?.length || 0} operations)`);
      try {
        recordWorkflowMessage(this.db, {
          task_id: taskId || pending.taskId,
          sender_node: planner,
          sender_role: 'planner',
          receiver_node: config.NODE_NAME,
          receiver_role: this.nodeRole || 'router',
          message_type: 'PLAN_RESPONSE',
          summary: `Received validated plan from Planner (${planner}): ${operations?.length || 0} operations. ${explanation || ''}`,
          payload_metadata: { operations, explanation, validation },
          status: 'delivered',
          request_id: requestId
        }, {
          broadcastWs: this.broadcastWs,
          wsEventType: 'plan:received'
        });
      } catch {}

      pending.resolve({
        success: true,
        operations: operations || [],
        explanation: explanation || planSummary || `Planned ${operations?.length || 0} operations`,
        planSummary: planSummary || explanation || `Operations: ${operations?.map(o => `${o.operation} ${o.table}`).join(', ')}`,
        validation: validation || { valid: true },
        plannerPeerId: planner,
        requestId,
        taskId: taskId || pending.taskId,
        raw
      });
    } else {
      logger.warn(`[Router] Received failed PLAN_RESPONSE [reqId:${requestId}] from Planner ${planner.slice(0, 12)}...: ${error}`);
      try {
        recordWorkflowMessage(this.db, {
          task_id: taskId || pending.taskId,
          sender_node: planner,
          sender_role: 'planner',
          receiver_node: config.NODE_NAME,
          receiver_role: this.nodeRole || 'router',
          message_type: 'PLAN_FAILED',
          summary: `Planner (${planner}) rejected plan: ${error || 'Unknown error'}`,
          payload_metadata: { error },
          status: 'failed',
          request_id: requestId
        }, {
          broadcastWs: this.broadcastWs,
          wsEventType: 'plan:failed'
        });
      } catch {}

      pending.resolve({
        success: false,
        operations: [],
        error: error || 'Planning failed on remote planner',
        plannerPeerId: planner,
        requestId,
        taskId: taskId || pending.taskId,
        raw
      });
    }
  }

  /**
   * Handles incoming EXECUTION_RESPONSE message on the requesting node.
   * 
   * @param {Object} envelope - The decoded envelope.
   * @param {string} fromPeer - Sender peer ID.
   */
  handleExecutionResponse(envelope, fromPeer) {
    const payload = envelope.payload || {};
    const { requestId, taskId, success, summary, error, executorPeerId, status, proposalId } = payload;
    if (!requestId) return;

    const pending = this.pendingExecutionRequests.get(requestId);
    if (!pending) {
      // Not waiting for this request, or already timed out
      return;
    }

    const executor = executorPeerId || fromPeer;
    const targetTaskId = taskId || pending.taskId;

    // Handle intermediate awaiting_approval from remote executor node
    if (status === 'awaiting_approval' || status === 'pending_approval' || status === 'pending') {
      logger.info(`[Router] Executor ${executor} registered task [${targetTaskId || 'n/a'}] — AWAITING_APPROVAL on executor node`);

      if (proposalId) {
        pending.proposalId = proposalId;
        this.proposalToTaskMap.set(proposalId, targetTaskId);
      }

      // Update local task on Planner to AWAITING_APPROVAL
      if (targetTaskId && this.db) {
        try {
          const localTask = this.db.prepare('SELECT * FROM tasks WHERE id = ?').get(targetTaskId);
          if (localTask && localTask.status === TaskStatus.PLANNED) {
            this.db.prepare('UPDATE tasks SET status = ?, approval_id = ?, updated_at = ? WHERE id = ?')
              .run(TaskStatus.AWAITING_APPROVAL, proposalId || localTask.approval_id, new Date().toISOString(), targetTaskId);
            const updatedTask = this.db.prepare('SELECT * FROM tasks WHERE id = ?').get(targetTaskId);
            if (this.broadcastWs && updatedTask) {
              this.broadcastWs('task:status', updatedTask);
            }
          }
        } catch (dbErr) {
          logger.warn(`[Coordinator] Failed to update local task to awaiting_approval: ${dbErr.message}`);
        }
      }

      // Wait for the final execution result from the executor after human approval
      return;
    }

    clearTimeout(pending.timer);
    this.pendingExecutionRequests.delete(requestId);

    const isSuccess = success === true || status === 'committed' || status === 'completed' || (summary && summary.completed > 0) || (payload.completed && payload.completed > 0);
    const finalCompleted = summary?.completed || payload.completed || (isSuccess ? 1 : 0);
    const finalResults = summary?.results || payload.results || (isSuccess ? [{ success: true }] : []);

    if (isSuccess) {
      logger.db(`[Router] Received successful EXECUTION_RESPONSE [reqId:${requestId}] from Executor ${executor.slice(0, 12)}... (${finalCompleted} completed)`);
      if (targetTaskId && this.db) {
        try {
          this.db.prepare(`
            UPDATE tasks 
            SET status = ?, execution_result = ?, error = NULL, updated_at = ? 
            WHERE id = ?
          `).run(
            TaskStatus.COMPLETED,
            JSON.stringify(finalResults),
            new Date().toISOString(),
            targetTaskId
          );
          const updatedTask = this.db.prepare('SELECT * FROM tasks WHERE id = ?').get(targetTaskId);
          if (this.broadcastWs && updatedTask) {
            this.broadcastWs('task:status', updatedTask);
            this.broadcastWs('task:executed', { success: true, task: updatedTask, summary: { completed: finalCompleted, results: finalResults } });
            this.broadcastWs('task:completed', updatedTask);
          }
        } catch (dbErr) {
          logger.warn(`[Coordinator] Failed to update completed task status in DB: ${dbErr.message}`);
        }
      }

      try {
        recordWorkflowMessage(this.db, {
          task_id: targetTaskId,
          sender_node: executor,
          sender_role: 'executor',
          receiver_node: config.NODE_NAME,
          receiver_role: this.nodeRole || 'planner',
          message_type: 'EXECUTION_RESULT',
          summary: `Executor (${executor}) committed ${finalCompleted} operations successfully.`,
          payload_metadata: { summary: { completed: finalCompleted, results: finalResults } },
          status: 'delivered',
          request_id: requestId
        }, {
          broadcastWs: this.broadcastWs,
          wsEventType: 'execution:completed'
        });
      } catch {}

      pending.resolve({
        completed: finalCompleted,
        failed: 0,
        results: finalResults,
        errors: []
      });
    } else {
      logger.warn(`[Router] Received failed EXECUTION_RESPONSE [reqId:${requestId}] from Executor ${executor.slice(0, 12)}...: ${error}`);
      if (targetTaskId && this.db) {
        try {
          this.db.prepare(`
            UPDATE tasks 
            SET status = ?, error = ?, updated_at = ? 
            WHERE id = ?
          `).run(
            TaskStatus.FAILED,
            error || 'Execution failed on executor',
            new Date().toISOString(),
            targetTaskId
          );
          const updatedTask = this.db.prepare('SELECT * FROM tasks WHERE id = ?').get(targetTaskId);
          if (this.broadcastWs && updatedTask) {
            this.broadcastWs('task:status', updatedTask);
            this.broadcastWs('task:failed', { success: false, task: updatedTask, error });
          }
        } catch (dbErr) {
          logger.warn(`[Coordinator] Failed to update failed task status in DB: ${dbErr.message}`);
        }
      }

      try {
        recordWorkflowMessage(this.db, {
          task_id: targetTaskId,
          sender_node: executor,
          sender_role: 'executor',
          receiver_node: config.NODE_NAME,
          receiver_role: this.nodeRole || 'planner',
          message_type: 'EXECUTION_FAILED',
          summary: `Executor (${executor}) failed execution: ${error}`,
          payload_metadata: { error, summary },
          status: 'failed',
          request_id: requestId
        }, {
          broadcastWs: this.broadcastWs,
          wsEventType: 'execution:failed'
        });
      } catch {}

      pending.resolve({
        completed: summary?.completed || 0,
        failed: summary?.failed || 1,
        results: summary?.results || [],
        errors: error ? [error] : ['Execution failed on remote executor']
      });
    }
  }

  // ───────────────────────────────────────────────────────────────────────────
  // PLANNER NODE FLOW
  // ───────────────────────────────────────────────────────────────────────────

  /**
   * Handles an incoming PLAN_REQUEST on a Planner node.
   * Generates a structured operation plan via local LLM, validates it, and replies.
   * A Planner MUST NEVER execute mutations or broadcast transactions.
   * 
   * @param {Object} envelope - The decoded envelope.
   * @param {string} fromPeer - Sender peer ID.
   */
  async handlePlanRequest(envelope, fromPeer) {
    if (this.nodeRole !== 'planner' && this.nodeRole !== 'peer') {
      logger.warn(`[Coordinator] Dropped PLAN_REQUEST: this node role is '${this.nodeRole}', not planner.`);
      return;
    }
    return await this.autonomousPlanner.handlePlanRequest(envelope, fromPeer);
  }

  /**
   * Autonomously processes a task locally by task ID.
   * @param {string} taskId
   * @returns {Promise<Object>}
   */
  async planTaskAutonomously(taskId) {
    return await this.autonomousPlanner.processTaskAutonomously(taskId);
  }

  // ───────────────────────────────────────────────────────────────────────────
  // EXECUTOR NODE FLOW
  // ───────────────────────────────────────────────────────────────────────────

  /**
   * Handles an incoming EXECUTION_REQUEST on an Executor node.
   * Revalidates operations locally, executes atomically in SQLite,
   * broadcasts transaction gossip upon commit, and replies with EXECUTION_RESPONSE.
   * 
   * @param {Object} envelope - The decoded envelope.
   * @param {string} fromPeer - Sender peer ID.
   */
  async handleExecutionRequest(envelope, fromPeer) {
    const {
      requestId,
      taskId,
      title,
      taskTitle,
      prompt,
      plan,
      operations,
      routerPeerId,
      plannerPeerId,
      plannerNodeName,
      autoApprove = true
    } = envelope.payload || {};
    const router = routerPeerId || envelope.sender || fromPeer;

    if (!requestId || !Array.isArray(operations)) {
      logger.warn(`[Executor] Dropped malformed EXECUTION_REQUEST from ${router.slice(0, 12)}...`);
      return;
    }

    // Role safety enforcement: Planner nodes must NEVER execute mutations or write to SQLite
    if (this.nodeRole === 'planner') {
      logger.warn(`[Coordinator] EXECUTION_REQUEST [reqId:${requestId}] REJECTED: this node is running with nodeRole=planner and is strictly forbidden from executing database mutations.`);
      const responsePayload = createExecutionResponse({
        requestId,
        taskId,
        success: false,
        summary: { completed: 0, failed: operations.length },
        error: "Execution failed: Planner nodes are strictly forbidden from executing database mutations.",
        executorPeerId: this.peerId,
        targetPeerId: router
      });
      const respEnvelope = createEnvelope(MessageType.EXECUTION_RESPONSE, responsePayload, this.peerId);
      await publishControl(this.node, respEnvelope);
      return;
    }

    // Deduplication check
    if (this.processedRequests.has(requestId)) {
      logger.p2p(`[Executor] Skipping duplicate EXECUTION_REQUEST [reqId:${requestId}]`);
      return;
    }
    this._markProcessed(requestId);

    const finalTaskId = taskId || `task-${requestId.slice(0, 8)}`;
    const resolvedTitle = title || taskTitle || prompt || 'Mesh Collaborative Task';
    const senderName = plannerNodeName || (router === fromPeer ? (this.peerRegistry.getPeer(fromPeer)?.name || fromPeer.slice(0, 8)) : router.slice(0, 8));

    logger.db(`[Executor] Processing EXECUTION_REQUEST [reqId:${requestId} taskId:${finalTaskId}] from ${senderName} (${operations.length} operations)`);

    // Ensure the task and approval exist locally in SQLite so that UI displays it and state transitions are valid
    const now = new Date().toISOString();
    let localTask = this.db ? this.db.prepare('SELECT * FROM tasks WHERE id = ?').get(finalTaskId) : null;
    const approvalId = envelope.payload?.approvalId || `appr-${uuidv4().slice(0, 8)}`;

    if (this.db && !localTask) {
      try {
        const planStr = plan ? (typeof plan === 'string' ? plan : JSON.stringify(plan)) : null;
        const opsStr = JSON.stringify(operations);
        this.db.prepare(`
          INSERT INTO tasks (
            id, title, user_prompt, task_type, priority, status,
            requested_by, assigned_node_id, planner_peer_id, planner_request_id,
            plan, operations, created_at, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `).run(
          finalTaskId,
          resolvedTitle,
          prompt || resolvedTitle,
          envelope.payload?.taskType || 'collaborative_intelligence',
          envelope.payload?.priority || 'high',
          TaskStatus.AWAITING_APPROVAL,
          `planner:${senderName}`,
          null,
          plannerPeerId || fromPeer,
          requestId,
          planStr,
          opsStr,
          now,
          now
        );
        localTask = this.db.prepare('SELECT * FROM tasks WHERE id = ?').get(finalTaskId);
      } catch (insertErr) {
        logger.warn(`[Executor] Local task registration notice: ${insertErr.message}`);
      }
    }

    if (this.db) {
      try {
        const existingAppr = this.db.prepare('SELECT * FROM approvals WHERE task_id = ?').get(finalTaskId);
        if (!existingAppr) {
          this.db.prepare(`
            INSERT INTO approvals (
              id, task_id, request_description, risk_level, decision, operations, planner_peer_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          `).run(
            approvalId,
            finalTaskId,
            prompt || resolvedTitle,
            'medium',
            'pending',
            JSON.stringify(operations),
            plannerPeerId || fromPeer,
            now
          );
          this.db.prepare('UPDATE tasks SET approval_id = ? WHERE id = ?').run(approvalId, finalTaskId);
        }
      } catch (apprErr) {
        logger.warn(`[Executor] Local approval registration notice: ${apprErr.message}`);
      }
    }

    // Record workflow message and broadcast UI events (so card and plan pop up on connected laptop!)
    try {
      recordWorkflowMessage(this.db, {
        task_id: finalTaskId,
        sender_node: senderName,
        sender_role: 'planner',
        receiver_node: config.NODE_NAME,
        receiver_role: 'executor',
        message_type: 'EXECUTION_REQUEST_RECEIVED',
        summary: `Executor received Plan & Execution Request (${operations.length} operations) from ${senderName}. Beginning local revalidation.`,
        payload_metadata: {
          taskId: finalTaskId,
          title: resolvedTitle,
          prompt,
          plan,
          operations,
          approvalId
        },
        status: 'delivered',
        request_id: requestId
      }, {
        broadcastWs: this.broadcastWs,
        wsEventType: 'execution:requested'
      });
    } catch {}

    // Broadcast WebSocket events so UI on connected node immediately pops up the plan and approval
    if (this.broadcastWs && this.db) {
      const currentTask = this.db.prepare('SELECT * FROM tasks WHERE id = ?').get(finalTaskId);
      const currentAppr = this.db.prepare('SELECT * FROM approvals WHERE task_id = ?').get(finalTaskId);
      if (currentTask) {
        this.broadcastWs('task:created', currentTask);
        this.broadcastWs('task:planned', currentTask);
        this.broadcastWs('task:status', currentTask);
      }
      if (currentAppr) {
        this.broadcastWs('approval:created', currentAppr);
      }
      this.broadcastWs('plan:received', {
        taskId: finalTaskId,
        title: resolvedTitle,
        prompt,
        plan,
        operations,
        fromNode: senderName
      });
      this.broadcastWs('plan:popup', {
        taskId: finalTaskId,
        title: resolvedTitle,
        prompt,
        plan,
        operations,
        approval: currentAppr,
        fromNode: senderName,
        timestamp: Date.now()
      });
    }

    try {
      // 1. Locally revalidate all operations against strict allowlists and schemas.
      // Never trust remote input blindly, even from a Planner peer.
      const planValidation = validateModelPlan(operations);
      if (!planValidation.valid) {
        logger.warn(`[Executor] EXECUTION_REQUEST [reqId:${requestId}] rejected: ${planValidation.errors.join('; ')}`);
        try {
          recordWorkflowMessage(this.db, {
            task_id: finalTaskId,
            sender_node: config.NODE_NAME,
            sender_role: 'executor',
            receiver_node: router,
            receiver_role: 'planner',
            message_type: 'EXECUTION_FAILED',
            summary: `Executor pre-validation rejected plan: ${planValidation.errors.join('; ')}`,
            payload_metadata: { errors: planValidation.errors },
            status: 'failed',
            request_id: requestId
          }, {
            broadcastWs: this.broadcastWs,
            wsEventType: 'execution:failed'
          });
        } catch {}

        const responsePayload = createExecutionResponse({
          requestId,
          taskId: finalTaskId,
          success: false,
          summary: { completed: 0, failed: operations.length },
          error: `Remote plan rejected by local executor validation: ${planValidation.errors.join('; ')}`,
          executorPeerId: this.peerId,
          targetPeerId: router
        });
        const respEnvelope = createEnvelope(MessageType.EXECUTION_RESPONSE, responsePayload, this.peerId);
        await publishControl(this.node, respEnvelope);
        return;
      }

      // Pre-validation passed: emit execution:validated
      try {
        recordWorkflowMessage(this.db, {
          task_id: finalTaskId,
          sender_node: config.NODE_NAME,
          sender_role: 'executor',
          receiver_node: router,
          receiver_role: 'planner',
          message_type: 'EXECUTION_VALIDATED',
          summary: `Executor revalidated plan operations locally against 3NF schemas: VALID.`,
          payload_metadata: { operationsCount: operations.length },
          status: 'delivered',
          request_id: requestId
        }, {
          broadcastWs: this.broadcastWs,
          wsEventType: 'execution:validated'
        });
      } catch {}

      // If executeNow is explicitly requested (e.g. low-level execution tests) or ad-hoc raw operations without taskId: execute immediately
      if (envelope.payload?.executeNow === true || envelope.payload?.approved === true || !envelope.payload?.taskId) {
        return await this._executeTaskPlan(finalTaskId, planValidation.operations, router, requestId, senderName);
      }

      // Canonical production flow: Task is AWAITING_APPROVAL.
      // Executor NEVER mutates business tables before explicit approval!
      logger.info(`[Executor] Plan revalidated (${planValidation.operations.length} ops). Task ${finalTaskId} is AWAITING_APPROVAL. Waiting for human approval sign-off.`);

      const responsePayload = createExecutionResponse({
        requestId,
        taskId: finalTaskId,
        approvalId,
        success: true,
        status: 'awaiting_approval',
        summary: { awaitingApproval: true, operationsCount: planValidation.operations.length },
        error: null,
        executorPeerId: this.peerId,
        targetPeerId: router
      });
      const respEnvelope = createEnvelope(MessageType.EXECUTION_RESPONSE, responsePayload, this.peerId);
      await publishControl(this.node, respEnvelope);
      return;

    } catch (err) {
      logger.error(`[Executor] Unexpected error executing [reqId:${requestId}]: ${err.message}`);
      try {
        recordWorkflowMessage(this.db, {
          task_id: taskId || `task-${requestId.slice(0, 8)}`,
          sender_node: config.NODE_NAME,
          sender_role: 'executor',
          receiver_node: router,
          receiver_role: 'planner',
          message_type: 'EXECUTION_FAILED',
          summary: `Executor internal error: ${err.message}`,
          payload_metadata: { error: err.message },
          status: 'failed',
          request_id: requestId
        }, {
          broadcastWs: this.broadcastWs,
          wsEventType: 'execution:failed'
        });
      } catch {}

      const responsePayload = createExecutionResponse({
        requestId,
        taskId,
        success: false,
        summary: { completed: 0, failed: operations.length },
        error: `Executor internal error: ${err.message}`,
        executorPeerId: this.peerId,
        targetPeerId: router
      });
      const respEnvelope = createEnvelope(MessageType.EXECUTION_RESPONSE, responsePayload, this.peerId);
      await publishControl(this.node, respEnvelope);
    }
  }

  /**
   * Internal execution helper. Only executed AFTER explicit approval or under direct test invocation.
   * Atomically executes operations in SQLite, commits, broadcasts transaction envelope, and sends EXECUTION_RESPONSE.
   *
   * @private
   */
  async _executeTaskPlan(finalTaskId, operations, router, requestId, senderName) {
    try {
      recordWorkflowMessage(this.db, {
        task_id: finalTaskId,
        sender_node: config.NODE_NAME,
        sender_role: 'executor',
        receiver_node: router,
        receiver_role: 'planner',
        message_type: 'EXECUTION_STARTED',
        summary: `Executor starting atomic SQLite transaction for ${operations.length} operations.`,
        payload_metadata: { operationsCount: operations.length },
        status: 'delivered',
        request_id: requestId
      }, {
        broadcastWs: this.broadcastWs,
        wsEventType: 'execution:started'
      });
    } catch {}

    // Execute operations atomically
    let execResult;
    if (this.executeApprovedTask) {
      try {
        execResult = this.executeApprovedTask(this.db, finalTaskId, {
          executorNodeId: this.peerId,
          executorRole: this.nodeRole || config.NODE_ROLE || 'executor',
          syncEngine: this.syncEngine,
          broadcastTx: this.broadcastTx,
          mutations: operations
        });
      } catch (execErr) {
        execResult = { success: false, completed: 0, failed: operations.length, errors: [execErr.message] };
      }
    } else {
      execResult = await this.executeOperations(
        operations,
        this.db,
        this.syncEngine,
        this.broadcastTx
      );
    }

    if (execResult.completed > 0 && (!execResult.errors || execResult.errors.length === 0)) {
      logger.db(`[Executor] Successfully committed batch of ${execResult.completed} operations for [reqId:${requestId}]`);
      if (this.db) {
        try {
          this.db.prepare(`
            UPDATE tasks 
            SET status = ?, execution_result = ?, error = NULL, updated_at = ? 
            WHERE id = ?
          `).run(
            TaskStatus.COMPLETED,
            JSON.stringify(execResult.results || []),
            new Date().toISOString(),
            finalTaskId
          );
        } catch {}
      }

      try {
        recordWorkflowMessage(this.db, {
          task_id: finalTaskId,
          sender_node: config.NODE_NAME,
          sender_role: 'executor',
          receiver_node: 'all',
          receiver_role: 'peer',
          message_type: 'MESH_REPLICATION',
          summary: `Database mutated and vector clock causally advanced. Replicating via mesh log gossip.`,
          payload_metadata: { completed: execResult.completed },
          status: 'delivered',
          request_id: requestId
        }, {
          broadcastWs: this.broadcastWs,
          wsEventType: 'mesh:replication'
        });

        recordWorkflowMessage(this.db, {
          task_id: finalTaskId,
          sender_node: config.NODE_NAME,
          sender_role: 'executor',
          receiver_node: router,
          receiver_role: 'planner',
          message_type: 'EXECUTION_RESULT',
          summary: `Executor committed batch of ${execResult.completed} operations successfully.`,
          payload_metadata: { completed: execResult.completed, results: execResult.results },
          status: 'delivered',
          request_id: requestId
        }, {
          broadcastWs: this.broadcastWs,
          wsEventType: 'execution:completed'
        });
      } catch {}

      if (this.broadcastWs && this.db) {
        const finalTask = this.db.prepare('SELECT * FROM tasks WHERE id = ?').get(finalTaskId);
        if (finalTask) {
          this.broadcastWs('task:status', finalTask);
          this.broadcastWs('task:executed', { success: true, task: finalTask, summary: execResult });
          this.broadcastWs('task:completed', finalTask);
        }
      }

      const responsePayload = createExecutionResponse({
        requestId,
        taskId: finalTaskId,
        success: true,
        status: 'completed',
        summary: { completed: execResult.completed, results: execResult.results },
        error: null,
        executorPeerId: this.peerId,
        targetPeerId: router
      });
      const respEnvelope = createEnvelope(MessageType.EXECUTION_RESPONSE, responsePayload, this.peerId);
      await publishControl(this.node, respEnvelope);
      return execResult;
    } else {
      logger.warn(`[Executor] Execution failed for [reqId:${requestId}]: ${execResult.errors?.join('; ')}`);
      const errMsg = execResult.errors?.join('; ') || 'Constraint violation';
      if (this.db) {
        try {
          this.db.prepare('UPDATE tasks SET status = ?, error = ?, updated_at = ? WHERE id = ?')
            .run(TaskStatus.FAILED, errMsg, new Date().toISOString(), finalTaskId);
        } catch {}
      }

      try {
        recordWorkflowMessage(this.db, {
          task_id: finalTaskId,
          sender_node: config.NODE_NAME,
          sender_role: 'executor',
          receiver_node: router,
          receiver_role: 'planner',
          message_type: 'EXECUTION_FAILED',
          summary: `Database execution failed: ${errMsg}`,
          payload_metadata: { errors: execResult.errors },
          status: 'failed',
          request_id: requestId
        }, {
          broadcastWs: this.broadcastWs,
          wsEventType: 'execution:failed'
        });
      } catch {}

      const responsePayload = createExecutionResponse({
        requestId,
        taskId: finalTaskId,
        success: false,
        status: 'failed',
        summary: { completed: 0, failed: execResult.failed, results: execResult.results },
        error: `Database execution failed: ${errMsg}`,
        executorPeerId: this.peerId,
        targetPeerId: router
      });
      const respEnvelope = createEnvelope(MessageType.EXECUTION_RESPONSE, responsePayload, this.peerId);
      await publishControl(this.node, respEnvelope);
      return execResult;
    }
  }

  /**
   * Handles incoming APPROVAL message on the Executor node.
   * If approved: moves task to APPROVED -> EXECUTING, executes atomic transaction,
   * commits, broadcasts transaction gossip, and sends EXECUTION_RESPONSE.
   * If rejected: moves task to REJECTED.
   *
   * @param {Object} envelope - The decoded envelope.
   * @param {string} fromPeer - Sender peer ID.
   */
  async handleApproval(envelope, fromPeer) {
    const { taskId, approvalId, decision = 'approved', reviewer = 'operator', reason = '', requestId = uuidv4() } = envelope.payload || {};
    if (!taskId && !approvalId) {
      logger.warn('[Executor] Dropped malformed APPROVAL message (no taskId or approvalId)');
      return;
    }

    if (this.nodeRole !== 'executor' && this.nodeRole !== 'peer') {
      logger.warn(`[Coordinator] APPROVAL rejected: node role '${this.nodeRole}' is not authorized to execute.`);
      return;
    }

    const task = this.db ? this.db.prepare('SELECT * FROM tasks WHERE id = ?').get(taskId) : null;
    if (!task) {
      logger.warn(`[Executor] APPROVAL received for unknown task '${taskId}'`);
      return;
    }

    if (decision === 'rejected') {
      const appr = this.db.prepare('SELECT id FROM approvals WHERE task_id = ? AND decision = \'pending\'').get(taskId);
      if (appr) {
        submitApprovalDecision(this.db, appr.id, {
          decision: 'rejected',
          reviewer,
          reason,
          targetStatus: TaskStatus.REJECTED
        }, { syncEngine: this.syncEngine });
      } else {
        this.db.prepare('UPDATE tasks SET status = ?, error = ?, updated_at = ? WHERE id = ?')
          .run(TaskStatus.REJECTED, reason || 'Rejected by operator', new Date().toISOString(), taskId);
      }

      const updatedTask = this.db.prepare('SELECT * FROM tasks WHERE id = ?').get(taskId);
      if (this.broadcastWs && updatedTask) {
        this.broadcastWs('approval:decided', { task_id: taskId, decision: 'rejected', reviewer, reason });
        this.broadcastWs('task:status', updatedTask);
      }

      const responsePayload = createExecutionResponse({
        requestId,
        taskId,
        success: false,
        status: 'rejected',
        summary: { completed: 0, failed: 0 },
        error: reason || 'Rejected by reviewer',
        executorPeerId: this.peerId,
        targetPeerId: task.planner_peer_id || fromPeer
      });
      const respEnvelope = createEnvelope(MessageType.EXECUTION_RESPONSE, responsePayload, this.peerId);
      await publishControl(this.node, respEnvelope);
      return;
    }

    // Approved:
    let pendingApproval = approvalId ? this.db.prepare('SELECT * FROM approvals WHERE id = ?').get(approvalId) : null;
    if (!pendingApproval) {
      pendingApproval = this.db.prepare('SELECT * FROM approvals WHERE task_id = ? AND decision = \'pending\'').get(taskId);
    }

    if (pendingApproval && pendingApproval.decision === 'pending') {
      submitApprovalDecision(this.db, pendingApproval.id, {
        decision: 'approved',
        reviewer,
        reason,
        targetStatus: TaskStatus.APPROVED
      }, { syncEngine: this.syncEngine });
    } else {
      this.db.prepare('UPDATE tasks SET status = ?, updated_at = ? WHERE id = ?')
        .run(TaskStatus.APPROVED, new Date().toISOString(), taskId);
    }

    const approvedTask = this.db.prepare('SELECT * FROM tasks WHERE id = ?').get(taskId);
    if (this.broadcastWs && approvedTask) {
      this.broadcastWs('approval:decided', { id: pendingApproval?.id, decision: 'approved', reviewer, reason, task_id: taskId });
      this.broadcastWs('task:status', approvedTask);
    }

    let parsedOps = [];
    try {
      parsedOps = typeof approvedTask.operations === 'string' ? JSON.parse(approvedTask.operations) : (approvedTask.operations || []);
    } catch {
      parsedOps = [];
    }

    return await this._executeTaskPlan(taskId, parsedOps, task.planner_peer_id || fromPeer, requestId, reviewer);
  }

  // ───────────────────────────────────────────────────────────────────────────
  // PEER ANNOUNCE FLOW
  // ───────────────────────────────────────────────────────────────────────────

  /**
   * Handles incoming PEER_ANNOUNCE messages.
   * Updates peer registry with the remote peer's advertised role and node name.
   * 
   * @param {Object} envelope - The decoded envelope.
   * @param {string} fromPeer - Sender peer ID.
   */
  handlePeerAnnounce(envelope, fromPeer) {
    const { peerId, nodeName, nodeRole, status, perf } = envelope.payload || {};
    const id = peerId || envelope.sender || fromPeer;
    if (!id) return;

    const resolvedRole = nodeRole || (nodeName === 'alpha' ? 'planner' : (nodeName === 'delta' ? 'executor' : 'peer'));
    logger.p2p(`[Discovery] Received PEER_ANNOUNCE from ${id.slice(0, 12)}... name="${nodeName || 'unknown'}", role="${resolvedRole}"`);
    this.peerRegistry.registerOrUpdatePeer(id, {
      nodeName,
      name: nodeName,
      role: resolvedRole,
      status: status || 'connected',
      perf: perf || null
    });
  }
}
