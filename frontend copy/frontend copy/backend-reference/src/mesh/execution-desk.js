/**
 * @fileoverview Executor side of Planner → Executor hand-offs.
 *
 * A planner node that planned a task itself sends EXECUTION_REQUEST on 'mesh:control'.
 * The executor validates the operations with its deterministic validator and parks them
 * as a pending proposal: nothing is written until a human approves it. The executor
 * reports each state change back to the planner with EXECUTION_RESULT.
 *
 *   EXECUTION_REQUEST { requestId, operations, prompt, plannerPeerId, targetPeerId, nodeName }
 *   EXECUTION_RESULT  { requestId, status: 'pending_approval'|'committed'|'rejected'|'failed',
 *                       proposalId, completed, errors, executorPeerId, targetPeerId, nodeName }
 * @module mesh/execution-desk
 */

import { randomUUID } from 'node:crypto';
import { encode } from '../utils/protocol.js';
import { validateModelPlan } from '../agents/model-validator.js';
import { proposeOperations } from '../agents/executor.js';
import { CONTROL_TOPIC } from './remote-planner.js';

/**
 * @param {Object} deps
 * @param {Object} deps.pubsub - libp2p pubsub service.
 * @param {string} deps.selfId - This executor's peer ID.
 * @param {string} deps.selfName - This executor's node name.
 * @param {import('better-sqlite3').Database} deps.db
 * @param {import('../agents/proposals.js').ProposalStore} deps.proposalStore
 * @param {Function} [deps.nameOf] - (peerId) => display name.
 * @param {Function} [deps.onEvent] - (kind, data) => void for activity / dashboard.
 */
export function createExecutionDesk({ pubsub, selfId, selfName, db, proposalStore, nameOf = (id) => id.slice(0, 8), onEvent = () => {} }) {
  const remoteByProposal = new Map();

  async function reply(plannerPeerId, requestId, fields) {
    const envelope = {
      id: randomUUID(),
      type: 'EXECUTION_RESULT',
      sender: selfId,
      timestamp: Date.now(),
      payload: { requestId, executorPeerId: selfId, targetPeerId: plannerPeerId, nodeName: selfName, ...fields }
    };
    try {
      await pubsub.publish(CONTROL_TOPIC, encode(envelope));
    } catch {
      // planner went away; the dashboard still shows the outcome
    }
    return envelope;
  }

  /**
   * Handles an EXECUTION_REQUEST from mesh:control. Returns true if the message was consumed.
   */
  async function handle(type, fromPeer, envelope) {
    if (type !== 'EXECUTION_REQUEST') return false;
    const payload = envelope?.payload || {};
    if (payload.targetPeerId && payload.targetPeerId !== selfId) return true; // for another executor
    const requestId = payload.requestId || envelope.id;
    const planner = nameOf(fromPeer);

    const shape = validateModelPlan(payload.operations);
    if (!shape.valid) {
      onEvent('rejected', { planner, requestId, errors: shape.errors, prompt: payload.prompt });
      await reply(fromPeer, requestId, { status: 'rejected', errors: shape.errors });
      return true;
    }

    const proposed = proposeOperations(shape.operations, db, proposalStore, {
      origin: 'nl',
      summary: `Planned by ${planner}${payload.prompt ? `: ${String(payload.prompt).slice(0, 160)}` : ''}`,
      request: payload.prompt || ''
    });
    if (!proposed.success) {
      onEvent('rejected', { planner, requestId, errors: proposed.errors, prompt: payload.prompt, operations: shape.operations });
      await reply(fromPeer, requestId, { status: 'rejected', errors: proposed.errors });
      return true;
    }

    remoteByProposal.set(proposed.proposal.id, { requestId, plannerPeerId: fromPeer, planner });
    proposed.proposal.from = planner;
    onEvent('pending', { planner, requestId, proposal: proposed.proposal });
    await reply(fromPeer, requestId, { status: 'pending_approval', proposalId: proposed.proposal.id });
    return true;
  }

  /**
   * Tells the planner how a proposal it sent was decided.
   * @param {string} proposalId
   * @param {{ success: boolean, result?: Object, errors?: string[] }} outcome
   * @param {'approved'|'rejected'} decision
   */
  async function notifyDecision(proposalId, outcome, decision) {
    const remote = remoteByProposal.get(proposalId);
    if (!remote) return null;
    remoteByProposal.delete(proposalId);
    const status = decision === 'rejected' ? 'rejected' : (outcome.success ? 'committed' : 'failed');
    onEvent(status, { planner: remote.planner, requestId: remote.requestId, proposalId, errors: outcome.errors });
    return reply(remote.plannerPeerId, remote.requestId, {
      status,
      proposalId,
      completed: outcome.result?.completed || 0,
      errors: outcome.errors || (decision === 'rejected' ? ['Rejected by the executor operator'] : [])
    });
  }

  return { handle, notifyDecision, isRemote: (proposalId) => remoteByProposal.has(proposalId) };
}
