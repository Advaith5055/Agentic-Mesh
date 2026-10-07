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
  createExecutionResponse
} from '../utils/protocol.js';
import { publishControl } from '../p2p/gossip.js';
import { validateModelPlan } from './model-validator.js';

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
    broadcastTx
  }) {
    this.node = node;
    this.peerRegistry = peerRegistry;
    this.db = db;
    this.syncEngine = syncEngine;
    this.planOperations = planOperations;
    this.executeOperations = executeOperations;
    this.broadcastTx = broadcastTx;

    /** @type {Map<string, { resolve: Function, reject: Function, timer: NodeJS.Timeout }>} */
    this.pendingPlanRequests = new Map();

    /** @type {Map<string, { resolve: Function, reject: Function, timer: NodeJS.Timeout }>} */
    this.pendingExecutionRequests = new Map();

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
   * @param {string} prompt - The natural language request.
   * @param {Object} [options={}]
   * @param {number} [options.timeoutMs] - Timeout in ms.
   * @returns {Promise<{ success: boolean, operations: Array<Object>, error?: string, raw?: string, plannerPeerId?: string }>}
   */
  async requestPlanFromMesh(prompt, options = {}) {
    const timeoutMs = options.timeoutMs || config.PLAN_TIMEOUT_MS;

    // Locate connected planner peer
    const plannerPeer = this.peerRegistry.findAvailablePeerByRole('planner');

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
    logger.ai(`[Router] Delegating planning to Planner ${plannerPeer.peerId.slice(0, 12)}... [reqId:${requestId}]`);

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

      this.pendingPlanRequests.set(requestId, { resolve, timer });

      try {
        const payload = createPlanRequest({
          requestId,
          prompt,
          requesterPeerId: this.peerId,
          targetPeerId: plannerPeer.peerId,
          timestamp: Date.now()
        });

        const envelope = createEnvelope(MessageType.PLAN_REQUEST, payload, this.peerId);
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
   * @param {Array<Object>} operations - Structured operations array.
   * @param {Object} [meta={}]
   * @param {string} [meta.plannerPeerId] - Optional planner peer ID.
   * @param {number} [meta.timeoutMs] - Timeout in ms.
   * @returns {Promise<{ completed: number, failed: number, results?: Array<any>, errors?: Array<string> }>}
   */
  async requestExecutionFromMesh(operations, meta = {}) {
    const timeoutMs = meta.timeoutMs || config.EXECUTION_TIMEOUT_MS;

    // Locate connected executor peer
    const executorPeer = this.peerRegistry.findAvailablePeerByRole('executor');

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

      this.pendingExecutionRequests.set(requestId, { resolve, timer });

      try {
        const payload = createExecutionRequest({
          requestId,
          operations,
          routerPeerId: this.peerId,
          plannerPeerId: meta.plannerPeerId || null,
          targetPeerId: executorPeer.peerId
        });

        const envelope = createEnvelope(MessageType.EXECUTION_REQUEST, payload, this.peerId);
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
    const { requestId, success, operations, error, plannerPeerId, raw } = envelope.payload || {};
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
      pending.resolve({
        success: true,
        operations: operations || [],
        plannerPeerId: planner,
        raw
      });
    } else {
      logger.warn(`[Router] Received failed PLAN_RESPONSE [reqId:${requestId}] from Planner ${planner.slice(0, 12)}...: ${error}`);
      pending.resolve({
        success: false,
        operations: [],
        error: error || 'Planning failed on remote planner',
        plannerPeerId: planner,
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
    const { requestId, success, summary, error, executorPeerId } = envelope.payload || {};
    if (!requestId) return;

    const pending = this.pendingExecutionRequests.get(requestId);
    if (!pending) {
      // Not waiting for this request, or already timed out
      return;
    }

    clearTimeout(pending.timer);
    this.pendingExecutionRequests.delete(requestId);

    const executor = executorPeerId || fromPeer;
    if (success) {
      logger.db(`[Router] Received successful EXECUTION_RESPONSE [reqId:${requestId}] from Executor ${executor.slice(0, 12)}... (${summary?.completed || 0} completed)`);
      pending.resolve({
        completed: summary?.completed || 0,
        failed: summary?.failed || 0,
        results: summary?.results || [],
        errors: []
      });
    } else {
      logger.warn(`[Router] Received failed EXECUTION_RESPONSE [reqId:${requestId}] from Executor ${executor.slice(0, 12)}...: ${error}`);
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
    const { requestId, prompt, requesterPeerId } = envelope.payload || {};
    const requester = requesterPeerId || envelope.sender || fromPeer;

    if (!requestId || !prompt) {
      logger.warn(`[Planner] Dropped malformed PLAN_REQUEST from ${requester.slice(0, 12)}... (missing requestId or prompt)`);
      return;
    }

    // Deduplication check
    if (this.processedRequests.has(requestId)) {
      logger.p2p(`[Planner] Skipping duplicate PLAN_REQUEST [reqId:${requestId}]`);
      return;
    }
    this._markProcessed(requestId);

    logger.ai(`[Planner] Processing PLAN_REQUEST [reqId:${requestId}] from ${requester.slice(0, 12)}... prompt: "${prompt}"`);

    try {
      // 1. Generate plan using existing planOperations (read-only against db schema/state)
      const planResult = await this.planOperations(prompt, this.db);

      if (!planResult.success) {
        logger.warn(`[Planner] PLAN_REQUEST [reqId:${requestId}] failed: ${planResult.error}`);
        const responsePayload = createPlanResponse({
          requestId,
          success: false,
          operations: [],
          error: planResult.error || 'AI model planning failed',
          plannerPeerId: this.peerId,
          targetPeerId: requester,
          raw: planResult.raw || null
        });
        const respEnvelope = createEnvelope(MessageType.PLAN_RESPONSE, responsePayload, this.peerId);
        await publishControl(this.node, respEnvelope);
        return;
      }

      // 2. Explicitly revalidate plan with validateModelPlan
      const validation = validateModelPlan(planResult.operations);
      if (!validation.valid) {
        logger.warn(`[Planner] PLAN_REQUEST [reqId:${requestId}] failed model validation: ${validation.errors.join('; ')}`);
        const responsePayload = createPlanResponse({
          requestId,
          success: false,
          operations: [],
          error: `Plan validation failed: ${validation.errors.join('; ')}`,
          plannerPeerId: this.peerId,
          targetPeerId: requester,
          raw: planResult.raw || null
        });
        const respEnvelope = createEnvelope(MessageType.PLAN_RESPONSE, responsePayload, this.peerId);
        await publishControl(this.node, respEnvelope);
        return;
      }

      // 3. Return validated plan only. DO NOT EXECUTE. DO NOT BROADCAST TX.
      logger.ai(`[Planner] Completed PLAN_REQUEST [reqId:${requestId}] for ${requester.slice(0, 12)}... outcome: SUCCESS (${validation.operations.length} operations)`);
      const responsePayload = createPlanResponse({
        requestId,
        success: true,
        operations: validation.operations,
        error: null,
        plannerPeerId: this.peerId,
        targetPeerId: requester,
        raw: planResult.raw || null
      });
      const respEnvelope = createEnvelope(MessageType.PLAN_RESPONSE, responsePayload, this.peerId);
      await publishControl(this.node, respEnvelope);

    } catch (err) {
      logger.error(`[Planner] Unexpected error handling PLAN_REQUEST [reqId:${requestId}]: ${err.message}`);
      const responsePayload = createPlanResponse({
        requestId,
        success: false,
        operations: [],
        error: `Internal planner error: ${err.message}`,
        plannerPeerId: this.peerId,
        targetPeerId: requester,
        raw: null
      });
      const respEnvelope = createEnvelope(MessageType.PLAN_RESPONSE, responsePayload, this.peerId);
      await publishControl(this.node, respEnvelope);
    }
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
    const { requestId, operations, routerPeerId, plannerPeerId } = envelope.payload || {};
    const router = routerPeerId || envelope.sender || fromPeer;

    if (!requestId || !Array.isArray(operations)) {
      logger.warn(`[Executor] Dropped malformed EXECUTION_REQUEST from ${router.slice(0, 12)}...`);
      return;
    }

    // Deduplication check
    if (this.processedRequests.has(requestId)) {
      logger.p2p(`[Executor] Skipping duplicate EXECUTION_REQUEST [reqId:${requestId}]`);
      return;
    }
    this._markProcessed(requestId);

    logger.db(`[Executor] Processing EXECUTION_REQUEST [reqId:${requestId}] from Router ${router.slice(0, 12)}... (planner: ${plannerPeerId ? plannerPeerId.slice(0, 12) : 'none'}, ${operations.length} operations)`);

    try {
      // 1. Locally revalidate all operations against strict allowlists and schemas.
      // Never trust remote input blindly, even from a Planner peer.
      const planValidation = validateModelPlan(operations);
      if (!planValidation.valid) {
        logger.warn(`[Executor] EXECUTION_REQUEST [reqId:${requestId}] rejected: ${planValidation.errors.join('; ')}`);
        const responsePayload = createExecutionResponse({
          requestId,
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

      // 2. Execute operations atomically using the canonical mutation service.
      // executeOperations enforces schema constraints, type bounds, and foreign keys.
      // If any operation fails, the batch is rolled back completely.
      // On success, broadcastTx publishes transaction gossip to mesh:transactions.
      const execResult = await this.executeOperations(
        planValidation.operations,
        this.db,
        this.syncEngine,
        this.broadcastTx
      );

      if (execResult.completed > 0 && (!execResult.errors || execResult.errors.length === 0)) {
        logger.db(`[Executor] Successfully committed batch of ${execResult.completed} operations for [reqId:${requestId}]`);
        const responsePayload = createExecutionResponse({
          requestId,
          success: true,
          summary: { completed: execResult.completed, results: execResult.results },
          error: null,
          executorPeerId: this.peerId,
          targetPeerId: router
        });
        const respEnvelope = createEnvelope(MessageType.EXECUTION_RESPONSE, responsePayload, this.peerId);
        await publishControl(this.node, respEnvelope);
      } else {
        logger.warn(`[Executor] EXECUTION_REQUEST [reqId:${requestId}] failed execution: ${execResult.errors?.join('; ')}`);
        const responsePayload = createExecutionResponse({
          requestId,
          success: false,
          summary: { completed: 0, failed: execResult.failed, results: execResult.results },
          error: `Database execution failed: ${execResult.errors?.join('; ') || 'Constraint violation'}`,
          executorPeerId: this.peerId,
          targetPeerId: router
        });
        const respEnvelope = createEnvelope(MessageType.EXECUTION_RESPONSE, responsePayload, this.peerId);
        await publishControl(this.node, respEnvelope);
      }

    } catch (err) {
      logger.error(`[Executor] Unexpected error executing [reqId:${requestId}]: ${err.message}`);
      const responsePayload = createExecutionResponse({
        requestId,
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

    logger.p2p(`[Discovery] Received PEER_ANNOUNCE from ${id.slice(0, 12)}... name="${nodeName || 'unknown'}", role="${nodeRole || 'peer'}"`);
    this.peerRegistry.registerOrUpdatePeer(id, {
      nodeName,
      role: nodeRole,
      status: status || 'connected',
      perf: perf || null
    });
  }
}
