/**
 * @fileoverview Executor agent — executes validated operations against SQLite and broadcasts to mesh.
 * Delegates atomic execution to the canonical db/mutation service.
 * @module agents/executor
 */

import { executeMutation, executeMutationBatch, validateMutationBatch } from '../db/mutation.js';
import { logger } from '../utils/logger.js';
import { createSyncEngine } from '../db/sync.js';
import { ProposalStatus } from './proposals.js';

/**
 * Executes a sequence of validated database operations atomically.
 * All operations are validated and executed inside a single SQLite transaction.
 * On success, broadcasts envelopes to the network.
 *
 * @async
 * @param {Array<Object>} operations - Array of { operation, table, data } objects.
 * @param {import('better-sqlite3').Database} db - The SQLite database instance.
 * @param {string|import('../db/sync.js').SyncEngine} peerIdOrEngine - Peer identifier or SyncEngine.
 * @param {Function} [broadcastFn] - Callback to broadcast local writes to the network.
 * @param {Object} [meta={}] - Provenance: { origin, evidenceId }.
 * @returns {Promise<Object>} Execution result with { completed, failed, results, errors }.
 */
export async function executeOperations(operations, db, peerIdOrEngine, broadcastFn, meta = {}) {
  const syncEngine = typeof peerIdOrEngine === 'object' && peerIdOrEngine !== null
    ? peerIdOrEngine
    : createSyncEngine(peerIdOrEngine || 'local');

  const batchResult = executeMutationBatch(db, syncEngine, operations, meta);

  if (batchResult.success) {
    if (broadcastFn && typeof broadcastFn === 'function') {
      for (const envelope of batchResult.envelopes) {
        try {
          await broadcastFn(envelope);
        } catch (err) {
          logger.error(`Failed to broadcast mutation envelope: ${err.message}`);
        }
      }
    }
    logger.db(`Executed batch of ${batchResult.completed} operations successfully`);
    return {
      completed: batchResult.completed,
      failed: 0,
      results: batchResult.results
    };
  }

  logger.error(`Batch execution failed: ${batchResult.errors?.join(', ')}`);
  return {
    completed: 0,
    failed: batchResult.failed,
    results: batchResult.results,
    errors: batchResult.errors
  };
}

/**
 * Executes a single fast-path database transaction.
 * Supports both (operation, tableName, data, db, peerId, broadcastFn)
 * and ({ operation, table, data }, db, peerId, broadcastFn) signatures.
 *
 * @async
 * @param {string|Object} operationOrPayload - Operation type or transaction payload object.
 * @param {string|import('better-sqlite3').Database} tableNameOrDb - Target table or database instance.
 * @param {Object|string} dataOrPeerId - Row data or peer ID.
 * @param {import('better-sqlite3').Database|Function} [dbOrBroadcastFn] - Database or broadcast callback.
 * @param {string} [peerId] - Peer ID.
 * @param {Function} [broadcastFn] - Broadcast callback.
 * @returns {Promise<Object>} { success: boolean, rowId?: number, errors?: string[], envelope?: Object }
 */
export async function executeSingleTransaction(
  operationOrPayload,
  tableNameOrDb,
  dataOrPeerId,
  dbOrBroadcastFn,
  peerId,
  broadcastFn
) {
  let operation, tableName, data, db, actualPeerId, actualBroadcastFn;

  if (typeof operationOrPayload === 'object' && operationOrPayload !== null) {
    operation = operationOrPayload.operation;
    tableName = operationOrPayload.table || operationOrPayload.tableName;
    data = operationOrPayload.data;
    db = tableNameOrDb;
    actualPeerId = dataOrPeerId;
    actualBroadcastFn = dbOrBroadcastFn;
  } else {
    operation = operationOrPayload;
    tableName = tableNameOrDb;
    data = dataOrPeerId;
    db = dbOrBroadcastFn;
    actualPeerId = peerId;
    actualBroadcastFn = broadcastFn;
  }

  if (!operation || !tableName || !data) {
    return { success: false, errors: ['Missing operation, table, or data in transaction payload.'] };
  }

  const syncEngine = typeof actualPeerId === 'object' && actualPeerId !== null
    ? actualPeerId
    : createSyncEngine(actualPeerId || 'local');

  const mutationResult = executeMutation(db, syncEngine, { operation, table: tableName, data });

  if (mutationResult.success) {
    if (actualBroadcastFn && typeof actualBroadcastFn === 'function') {
      try {
        await actualBroadcastFn(mutationResult.envelope);
      } catch (err) {
        logger.error(`Failed to broadcast transaction envelope: ${err.message}`);
      }
    }
    logger.db(`Fast-path ${operation} on ${tableName} committed successfully`);
    return {
      success: true,
      rowId: mutationResult.rowId,
      envelope: mutationResult.envelope
    };
  }

  logger.error(`Execution failed for ${operation} on ${tableName}: ${mutationResult.errors?.join(', ')}`);
  return {
    success: false,
    errors: mutationResult.errors
  };
}

/**
 * Validates a plan and parks it as a pending proposal instead of executing it.
 * Used for plans that need human approval (e.g. plans derived from a photo).
 *
 * @param {Array<Object>} operations - Array of { operation, table, data } objects.
 * @param {import('better-sqlite3').Database} db - The SQLite database instance.
 * @param {import('./proposals.js').ProposalStore} store - Where pending proposals are kept.
 * @param {Object} meta - { origin, evidenceId, producerPeerId, summary, request }.
 * @returns {{ success: boolean, proposal?: Object, errors?: string[] }}
 */
export function proposeOperations(operations, db, store, meta = {}) {
  if (!Array.isArray(operations) || operations.length === 0) {
    return { success: false, errors: ['A proposal needs at least one operation.'] };
  }

  const check = validateMutationBatch(db, operations, meta);
  if (!check.valid) {
    logger.warn(`Proposal rejected by validator: ${check.errors.join(', ')}`);
    return { success: false, errors: check.errors };
  }

  const proposal = store.create({
    operations,
    origin: meta.origin,
    evidenceId: meta.evidenceId,
    summary: meta.summary,
    request: meta.request
  });
  logger.db(`Proposal ${proposal.id} created (${operations.length} ops, origin: ${meta.origin}) — awaiting approval`);
  return { success: true, proposal };
}

/**
 * Commits an approved proposal. The validator runs again at commit time because
 * the database may have changed (e.g. replicated writes) since the proposal was made.
 *
 * @async
 * @param {string} id - Proposal id.
 * @param {import('better-sqlite3').Database} db - The SQLite database instance.
 * @param {import('./proposals.js').ProposalStore} store - Proposal store.
 * @param {import('../db/sync.js').SyncEngine} syncEngine - Local sync engine.
 * @param {Function} [broadcastFn] - Callback to broadcast committed writes.
 * @returns {Promise<{ success: boolean, proposal?: Object, result?: Object, errors?: string[] }>}
 */
export async function commitProposal(id, db, store, syncEngine, broadcastFn) {
  const proposal = store.get(id);
  if (!proposal) {
    return { success: false, errors: [`Proposal '${id}' not found.`] };
  }
  if (proposal.status !== ProposalStatus.PENDING) {
    return { success: false, proposal, errors: [`Proposal '${id}' is ${proposal.status}, not pending.`] };
  }

  const result = await executeOperations(proposal.operations, db, syncEngine, broadcastFn, {
    origin: proposal.origin,
    evidenceId: proposal.evidenceId
  });

  if (result.failed > 0 || result.completed === 0) {
    store.markFailed(id, result.errors || ['Execution failed.']);
    return { success: false, proposal, errors: result.errors || ['Execution failed.'] };
  }

  store.markCommitted(id, result);
  logger.db(`Proposal ${id} approved and committed (${result.completed} ops)`);
  return { success: true, proposal, result };
}

/**
 * Rejects a pending proposal so it can no longer be committed.
 * @param {string} id - Proposal id.
 * @param {import('./proposals.js').ProposalStore} store - Proposal store.
 * @param {string} [reason=''] - Optional reason, kept for the audit trail.
 * @returns {{ success: boolean, proposal?: Object, errors?: string[] }}
 */
export function rejectProposal(id, store, reason = '') {
  const proposal = store.get(id);
  if (!proposal) {
    return { success: false, errors: [`Proposal '${id}' not found.`] };
  }
  if (proposal.status !== ProposalStatus.PENDING) {
    return { success: false, proposal, errors: [`Proposal '${id}' is ${proposal.status}, not pending.`] };
  }
  store.markRejected(id, reason);
  logger.db(`Proposal ${id} rejected${reason ? `: ${reason}` : ''}`);
  return { success: true, proposal };
}
