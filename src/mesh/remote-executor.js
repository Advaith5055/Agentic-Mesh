/**
 * @fileoverview Planner side of Planner → Executor hand-offs.
 *
 * When a user enters a database-changing prompt on the Planner node (delta):
 *   1. Route (router)
 *   2. Plan (planner + LLM)
 *   3. Preview-validate (validator against local schema)
 *   4. Hand-off (mesh: send EXECUTION_REQUEST to executor node, e.g. alpha)
 *   5. Wait for EXECUTION_RESULT (pending_approval, committed, rejected, failed)
 *
 * @module mesh/remote-executor
 */

import { randomUUID } from 'node:crypto';
import { encode } from '../utils/protocol.js';
import { validateMutationBatch } from '../db/mutation.js';
import { logger } from '../utils/logger.js';
import { config } from '../utils/config.js';

export const CONTROL_TOPIC = 'mesh:control';

const describeOps = (ops = []) => ops.map(o => `${o.operation} ${o.table || o.tableName}`).join(', ');

/**
 * Creates the RemoteExecutor service for the Planner node.
 *
 * @param {Object} deps
 * @param {Object} deps.pubsub - libp2p pubsub service.
 * @param {string} deps.selfId - This planner's peer ID.
 * @param {string} deps.selfName - This planner's node name (e.g. 'delta').
 * @param {import('better-sqlite3').Database} deps.db - SQLite DB.
 * @param {import('../p2p/discovery.js').PeerRegistry} deps.peerRegistry - Peer registry.
 * @param {import('../agents/activity.js').ActivityTracker} deps.activity - Activity tracker.
 * @param {Function} deps.planOperations - Local LLM planner (prompt, db).
 * @param {string} [deps.preferredPeer] - Name of preferred executor peer (e.g. 'alpha').
 * @param {number} [deps.timeoutMs=120000] - Timeout for pending_approval response.
 * @returns {Object}
 */
export function createRemoteExecutor({
  pubsub,
  selfId,
  selfName = 'delta',
  db,
  peerRegistry,
  activity,
  planOperations,
  preferredPeer = process.env.EXECUTOR_PEER || 'alpha',
  timeoutMs = 120_000
}) {
  /** Map of requestId -> { resolve, reject, timer, task, executorName } */
  const pendingRequests = new Map();

  /** Map of proposalId -> { task, executorName } for tracking long-running approval decisions */
  const activeProposals = new Map();

  /**
   * Finds the best connected executor peer.
   * @returns {Object|null}
   */
  function pickExecutor() {
    const peers = peerRegistry.getPeers().filter(p => p.connectedAt && (p.name || p.nodeName));
    if (preferredPeer) {
      const preferred = peers.find(p => (p.name === preferredPeer || p.nodeName === preferredPeer) && (p.role === 'executor' || p.role !== 'planner'));
      if (preferred) return { ...preferred, name: preferred.name || preferred.nodeName };
    }
    const executor = peers.find(p => p.role === 'executor');
    if (executor) return { ...executor, name: executor.name || executor.nodeName };
    return null;
  }

  /**
   * Handles incoming EXECUTION_RESULT messages from mesh:control.
   * @param {string} type - Message type.
   * @param {string} fromPeer - Sender peer ID.
   * @param {Object} envelope - Control envelope.
   * @returns {boolean} True if handled.
   */
  function handle(type, fromPeer, envelope) {
    if (type !== 'EXECUTION_RESULT') return false;

    const payload = envelope?.payload || {};
    const requestId = payload.requestId;
    const proposalId = payload.proposalId;
    const status = payload.status;
    const errors = payload.errors || [];
    const executorName = payload.nodeName || peerRegistry.getPeers().find(p => p.peerId === fromPeer)?.name || 'executor';

    logger.p2p(`[Planner] Received EXECUTION_RESULT from ${executorName} [reqId:${requestId || 'n/a'}, prop:${proposalId || 'n/a'}]: status=${status}`);

    // Check if we have a waiter waiting for the initial response
    const waiter = requestId ? pendingRequests.get(requestId) : null;
    const task = waiter?.task || (proposalId ? activeProposals.get(proposalId)?.task : null);

    if (status === 'pending_approval') {
      if (task) {
        task.message('executor', 'planner', `Plan validated by ${executorName} — proposal ${proposalId} awaiting human approval on ${executorName}`);
      }
      if (proposalId && task) {
        activeProposals.set(proposalId, { task, executorName, requestId });
      }
      if (waiter) {
        clearTimeout(waiter.timer);
        pendingRequests.delete(requestId);
        waiter.resolve({
          success: true,
          status: 'pending_approval',
          proposalId,
          executorName,
          operations: payload.operations || waiter.operations,
          message: `Planned by ${selfName} and sent to executor "${executorName}" — waiting for approval on ${executorName} (${proposalId}).`
        });
      }
      return true;
    }

    if (status === 'committed') {
      const opsCount = payload.completed || 1;
      if (task) {
        task.end('Execute', true, `${executorName} committed ${opsCount} op(s)`, 0);
        task.message('executor', 'planner', `${executorName} committed ${opsCount} op(s)`);
        task.finish('done');
      }
      logger.success(`[Planner] Executor "${executorName}" committed proposal ${proposalId || requestId} (${opsCount} operations)`);
      if (proposalId) activeProposals.delete(proposalId);
      if (waiter) {
        clearTimeout(waiter.timer);
        pendingRequests.delete(requestId);
        waiter.resolve({
          success: true,
          status: 'committed',
          proposalId,
          executorName,
          completed: opsCount
        });
      }
      return true;
    }

    if (status === 'rejected') {
      const err = errors[0] || 'Rejected by executor';
      if (task) {
        task.reject('Execute', err, 0);
        task.message('executor', 'planner', `${executorName} rejected plan: ${err}`, { status: 'warn' });
        task.finish('rejected', err);
      }
      logger.warn(`[Planner] Executor "${executorName}" rejected proposal ${proposalId || requestId}: ${err}`);
      if (proposalId) activeProposals.delete(proposalId);
      if (waiter) {
        clearTimeout(waiter.timer);
        pendingRequests.delete(requestId);
        waiter.resolve({
          success: false,
          status: 'rejected',
          proposalId,
          executorName,
          error: err,
          errors
        });
      }
      return true;
    }

    if (status === 'failed') {
      const err = errors[0] || 'Execution failed on executor';
      if (task) {
        task.end('Execute', false, err, 0);
        task.message('executor', 'planner', `${executorName} execution failed: ${err}`, { status: 'error' });
        task.finish('failed', err);
      }
      logger.error(`[Planner] Executor "${executorName}" execution failed: ${err}`);
      if (proposalId) activeProposals.delete(proposalId);
      if (waiter) {
        clearTimeout(waiter.timer);
        pendingRequests.delete(requestId);
        waiter.resolve({
          success: false,
          status: 'failed',
          proposalId,
          executorName,
          error: err,
          errors
        });
      }
      return true;
    }

    return true;
  }

  /**
   * Plans a natural language request and hands it off to an executor node.
   *
   * @param {string} prompt - The natural language request.
   * @param {Object} [options={}] - Options.
   * @returns {Promise<Object>}
   */
  async function handoff(prompt, _options = {}) {
    const task = activity.startTask({
      kind: 'handoff',
      title: prompt,
      stages: [
        { name: 'Route', agent: 'router' },
        { name: 'Plan', agent: 'planner' },
        { name: 'Validate', agent: 'validator' },
        { name: 'Hand-off', agent: 'mesh' },
        { name: 'Execute', agent: 'executor' }
      ]
    });

    task.message('user', 'router', prompt);
    task.begin('Route').end('Route', true, 'database change → planner path', 0);
    task.message('router', 'planner', 'Plan database operations with local Ollama');

    // ── Stage 2: Plan ───────────────────────────────────────────────────────
    task.begin('Plan', `${config.OLLAMA_MODEL} @ ${config.OLLAMA_HOST}`);
    task.message('planner', 'llm', `Prompt → ${config.OLLAMA_MODEL}`);
    const t0 = performance.now();
    let planResult = await planOperations(prompt, db);
    let planMs = performance.now() - t0;

    if (!planResult.success) {
      task.message('llm', 'planner', `Failed: ${planResult.error}`, { status: 'error', durationMs: Math.round(planMs) });
      task.end('Plan', false, planResult.error, planMs);
      task.skip('Validate').skip('Hand-off').skip('Execute');
      task.finish('failed', planResult.error);
      return { success: false, error: planResult.error, operations: [] };
    }

    task.message('llm', 'planner', `${planResult.operations.length} op(s) in ${(planMs / 1000).toFixed(1)}s`, { durationMs: Math.round(planMs) });
    task.end('Plan', true, describeOps(planResult.operations), planMs);

    // ── Stage 3: Preview-validate ───────────────────────────────────────────
    task.message('planner', 'validator', `Check ${planResult.operations.length} op(s) against local schema`);
    task.begin('Validate');
    const t1 = performance.now();
    let check = validateMutationBatch(db, planResult.operations, { origin: 'nl' });

    if (!check.valid) {
      task.message('validator', 'planner', `Preview rejected: ${check.errors[0]} — retrying once with error feedback`, { status: 'warn' });
      const retryPrompt = `${prompt}\n\nEXECUTOR FEEDBACK: your previous plan was rejected because: ${check.errors.join('; ')}. Fix these errors (use existing IDs, do not recreate things that exist) and return the corrected JSON operations array.`;
      
      const retryT0 = performance.now();
      const retryResult = await planOperations(retryPrompt, db);
      const retryMs = performance.now() - retryT0;

      if (retryResult.success) {
        check = validateMutationBatch(db, retryResult.operations, { origin: 'nl' });
        if (check.valid) {
          planResult = retryResult;
          task.message('llm', 'planner', `Correction: ${planResult.operations.length} op(s) in ${(retryMs / 1000).toFixed(1)}s`, { durationMs: Math.round(retryMs) });
        }
      }
    }

    const valMs = performance.now() - t1;
    if (!check.valid) {
      task.reject('Validate', check.errors.join('; '), valMs);
      task.message('validator', 'user', `Plan rejected: ${check.errors[0]}`, { status: 'warn' });
      task.skip('Hand-off').skip('Execute');
      task.finish('rejected', check.errors[0]);
      return {
        success: false,
        error: `Plan rejected by validator: ${check.errors.join('; ')}`,
        errors: check.errors,
        operations: planResult.operations
      };
    }

    task.end('Validate', true, 'preview validation passed', valMs);
    task.message('validator', 'planner', 'Preview validation passed');

    // ── Stage 4: Hand-off ───────────────────────────────────────────────────
    const executor = pickExecutor();
    if (!executor) {
      task.begin('Hand-off');
      task.end('Hand-off', false, 'No executor node connected', 0);
      task.skip('Execute');
      task.finish('failed', 'No executor node connected');
      task.message('mesh', 'user', 'No executor node connected — operations will not be executed locally', { status: 'warn' });
      return {
        success: false,
        noExecutor: true,
        error: 'No executor node connected',
        message: 'No executor node connected'
      };
    }

    const requestId = randomUUID();
    task.begin('Hand-off', `EXECUTION_REQUEST → ${executor.name}`);
    task.message('planner', 'mesh', `EXECUTION_REQUEST to executor "${executor.name}" (${planResult.operations.length} op(s))`);

    const envelope = {
      id: randomUUID(),
      type: 'EXECUTION_REQUEST',
      sender: selfId,
      timestamp: Date.now(),
      payload: {
        requestId,
        targetPeerId: executor.peerId,
        plannerPeerId: selfId,
        nodeName: selfName,
        prompt,
        operations: planResult.operations
      }
    };

    // ── Stage 5: Execute (waiting for approval / result) ─────────────────────
    task.end('Hand-off', true, `sent to ${executor.name}`, 0);
    task.begin('Execute', `waiting for approval on ${executor.name}`);

    const resultPromise = new Promise((resolve) => {
      const timer = setTimeout(() => {
        pendingRequests.delete(requestId);
        task.end('Execute', false, `Executor "${executor.name}" did not answer within ${Math.round(timeoutMs / 1000)}s`, 0);
        task.finish('failed', 'Timeout waiting for executor');
        resolve({
          success: false,
          error: `Executor "${executor.name}" did not answer within ${Math.round(timeoutMs / 1000)}s`,
          operations: planResult.operations
        });
      }, timeoutMs);
      timer.unref?.();

      pendingRequests.set(requestId, {
        resolve,
        timer,
        task,
        executorName: executor.name,
        operations: planResult.operations
      });
    });

    try {
      await pubsub.publish(CONTROL_TOPIC, encode(envelope));
    } catch (pubErr) {
      task.end('Hand-off', false, pubErr.message, 0);
      task.finish('failed', pubErr.message);
      pendingRequests.delete(requestId);
      return { success: false, error: `Failed to send to executor: ${pubErr.message}` };
    }

    return resultPromise;
  }

  /**
   * Directly sends an already-formed operation batch to the executor.
   * Used by POST /api/ask/confirm.
   *
   * @param {Array<Object>} operations - Operation list.
   * @param {string} [prompt='Manual operation execution'] - Prompt or summary.
   * @returns {Promise<Object>}
   */
  async function handoffOperations(operations, prompt = 'Confirmed operations') {
    const executor = pickExecutor();
    if (!executor) {
      return { success: false, noExecutor: true, error: 'No executor node connected' };
    }

    const task = activity.startTask({
      kind: 'handoff',
      title: prompt,
      stages: [
        { name: 'Validate', agent: 'validator' },
        { name: 'Hand-off', agent: 'mesh' },
        { name: 'Execute', agent: 'executor' }
      ]
    });

    task.message('user', 'validator', `Send ${operations.length} op(s) to ${executor.name}`);
    task.begin('Validate');
    const check = validateMutationBatch(db, operations, { origin: 'nl' });
    if (!check.valid) {
      task.reject('Validate', check.errors.join('; '), 0);
      task.skip('Hand-off').skip('Execute');
      task.finish('rejected', check.errors[0]);
      return { success: false, error: check.errors[0], errors: check.errors };
    }
    task.end('Validate', true, 'valid', 0);

    const requestId = randomUUID();
    task.begin('Hand-off', `EXECUTION_REQUEST → ${executor.name}`);
    task.message('planner', 'mesh', `EXECUTION_REQUEST to executor "${executor.name}" (${operations.length} op(s))`);

    const envelope = {
      id: randomUUID(),
      type: 'EXECUTION_REQUEST',
      sender: selfId,
      timestamp: Date.now(),
      payload: {
        requestId,
        targetPeerId: executor.peerId,
        plannerPeerId: selfId,
        nodeName: selfName,
        prompt,
        operations
      }
    };

    task.end('Hand-off', true, `sent to ${executor.name}`, 0);
    task.begin('Execute', `waiting for approval on ${executor.name}`);

    const resultPromise = new Promise((resolve) => {
      const timer = setTimeout(() => {
        pendingRequests.delete(requestId);
        task.end('Execute', false, 'Timeout waiting for executor', 0);
        task.finish('failed', 'Timeout');
        resolve({ success: false, error: `Executor "${executor.name}" did not answer in time` });
      }, timeoutMs);
      timer.unref?.();

      pendingRequests.set(requestId, {
        resolve,
        timer,
        task,
        executorName: executor.name,
        operations
      });
    });

    await pubsub.publish(CONTROL_TOPIC, encode(envelope));
    return resultPromise;
  }

  return {
    pickExecutor,
    handle,
    handoff,
    handoffOperations
  };
}
