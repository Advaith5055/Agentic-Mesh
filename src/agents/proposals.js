/**
 * @fileoverview In-memory store of operation plans waiting for human approval.
 * Vision-derived plans are never executed directly: the Executor parks them here
 * and only commits them after a user approves them by id.
 * @module agents/proposals
 */

import { v4 as uuidv4 } from 'uuid';
import { config } from '../utils/config.js';

export const ProposalStatus = {
  PENDING: 'pending',
  COMMITTED: 'committed',
  REJECTED: 'rejected',
  FAILED: 'failed',
  EXPIRED: 'expired'
};

const MAX_HISTORY = 200;

export class ProposalStore {
  /**
   * @param {Object} [options={}]
   * @param {number} [options.ttlMs] - How long a proposal stays approvable.
   * @param {Function} [options.now] - Clock function (injectable for tests).
   */
  constructor({ ttlMs = config.PROPOSAL_TTL_MS, now = () => Date.now() } = {}) {
    this.ttlMs = ttlMs;
    this.now = now;
    this.proposals = new Map();
  }

  /**
   * Stores a new pending proposal.
   * @param {Object} fields - { operations, origin, evidenceId, summary, request }
   * @returns {Object} The stored proposal.
   */
  create({ operations, origin, evidenceId = null, summary = '', request = '' }) {
    const createdAt = this.now();
    const proposal = {
      id: `prop-${uuidv4().slice(0, 8)}`,
      status: ProposalStatus.PENDING,
      origin,
      evidenceId,
      summary,
      request,
      operations,
      createdAt,
      expiresAt: createdAt + this.ttlMs,
      result: null,
      errors: []
    };
    this.proposals.set(proposal.id, proposal);
    this.#trim();
    return proposal;
  }

  /**
   * Returns a proposal by id, marking it expired if its TTL has passed.
   * @param {string} id
   * @returns {Object|null}
   */
  get(id) {
    const proposal = this.proposals.get(id);
    if (!proposal) return null;
    if (proposal.status === ProposalStatus.PENDING && this.now() > proposal.expiresAt) {
      proposal.status = ProposalStatus.EXPIRED;
    }
    return proposal;
  }

  /**
   * Lists proposals, newest first.
   * @param {Object} [filter={}]
   * @param {string} [filter.status] - Only return proposals with this status.
   * @returns {Array<Object>}
   */
  list({ status } = {}) {
    const all = [...this.proposals.keys()].map(id => this.get(id)).reverse();
    return status ? all.filter(p => p.status === status) : all;
  }

  markCommitted(id, result) {
    return this.#finish(id, ProposalStatus.COMMITTED, { result });
  }

  markRejected(id, reason = '') {
    return this.#finish(id, ProposalStatus.REJECTED, { errors: reason ? [reason] : [] });
  }

  markFailed(id, errors = []) {
    return this.#finish(id, ProposalStatus.FAILED, { errors });
  }

  #finish(id, status, fields) {
    const proposal = this.proposals.get(id);
    if (!proposal) return null;
    Object.assign(proposal, fields, { status, finishedAt: this.now() });
    return proposal;
  }

  #trim() {
    while (this.proposals.size > MAX_HISTORY) {
      const oldest = this.proposals.keys().next().value;
      this.proposals.delete(oldest);
    }
  }
}
