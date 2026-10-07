/**
 * @fileoverview Message protocol constants and helpers.
 * @module utils/protocol
 */

import { v4 as uuidv4 } from 'uuid';

/**
 * Enum for supported message types in the P2P network.
 * @readonly
 * @enum {string}
 */
export const MessageType = {
  TRANSACTION: 'TRANSACTION',
  SYNC_REQUEST: 'SYNC_REQUEST',
  SYNC_RESPONSE: 'SYNC_RESPONSE',
  AI_AUDIT: 'AI_AUDIT',
  PEER_ANNOUNCE: 'PEER_ANNOUNCE',
  PLAN_REQUEST: 'PLAN_REQUEST',
  PLAN_RESPONSE: 'PLAN_RESPONSE',
  EXECUTION_REQUEST: 'EXECUTION_REQUEST',
  EXECUTION_RESPONSE: 'EXECUTION_RESPONSE'
};

/**
 * Creates a structured PLAN_REQUEST payload.
 *
 * @param {Object} params
 * @param {string} [params.requestId]
 * @param {string} params.prompt
 * @param {string} params.requesterPeerId
 * @param {string} [params.targetPeerId]
 * @param {number} [params.timestamp]
 * @returns {Object}
 */
export function createPlanRequest({
  requestId = uuidv4(),
  prompt,
  requesterPeerId,
  targetPeerId = null,
  timestamp = Date.now()
}) {
  if (!prompt || typeof prompt !== 'string') {
    throw new Error('PLAN_REQUEST requires a valid string prompt');
  }
  return {
    requestId,
    prompt,
    requesterPeerId,
    targetPeerId,
    timestamp
  };
}

/**
 * Creates a structured PLAN_RESPONSE payload.
 *
 * @param {Object} params
 * @param {string} params.requestId
 * @param {boolean} params.success
 * @param {Array<Object>} [params.operations]
 * @param {string|null} [params.error]
 * @param {string} params.plannerPeerId
 * @param {string} [params.targetPeerId]
 * @param {string|null} [params.raw]
 * @returns {Object}
 */
export function createPlanResponse({
  requestId,
  success,
  operations = [],
  error = null,
  plannerPeerId,
  targetPeerId = null,
  raw = null
}) {
  return {
    requestId,
    success: Boolean(success),
    operations: Array.isArray(operations) ? operations : [],
    error: error || null,
    plannerPeerId,
    targetPeerId,
    raw: raw || null
  };
}

/**
 * Creates a structured EXECUTION_REQUEST payload.
 * Only transfers structured, validated operation objects (no raw SQL).
 *
 * @param {Object} params
 * @param {string} [params.requestId]
 * @param {Array<Object>} params.operations
 * @param {string} params.routerPeerId
 * @param {string|null} [params.plannerPeerId]
 * @param {string} [params.targetPeerId]
 * @returns {Object}
 */
export function createExecutionRequest({
  requestId = uuidv4(),
  operations,
  routerPeerId,
  plannerPeerId = null,
  targetPeerId = null
}) {
  if (!Array.isArray(operations) || operations.length === 0) {
    throw new Error('EXECUTION_REQUEST requires a non-empty operations array');
  }
  return {
    requestId,
    operations,
    routerPeerId,
    plannerPeerId,
    targetPeerId
  };
}

/**
 * Creates a structured EXECUTION_RESPONSE payload.
 *
 * @param {Object} params
 * @param {string} params.requestId
 * @param {boolean} params.success
 * @param {Object|null} [params.summary]
 * @param {string|null} [params.error]
 * @param {string} params.executorPeerId
 * @param {string} [params.targetPeerId]
 * @returns {Object}
 */
export function createExecutionResponse({
  requestId,
  success,
  summary = null,
  error = null,
  executorPeerId,
  targetPeerId = null
}) {
  return {
    requestId,
    success: Boolean(success),
    summary: summary || null,
    error: error || null,
    executorPeerId,
    targetPeerId
  };
}

/**
 * Creates a structured PEER_ANNOUNCE payload to advertise node role and name.
 *
 * @param {Object} params
 * @param {string} params.peerId
 * @param {string} params.nodeName
 * @param {string} params.nodeRole
 * @param {string} [params.status]
 * @param {string|null} [params.targetPeerId]
 * @returns {Object}
 */
export function createPeerAnnounce({
  peerId,
  nodeName,
  nodeRole,
  status = 'connected',
  targetPeerId = null,
  perf = null
}) {
  return {
    peerId,
    nodeName,
    nodeRole,
    status,
    targetPeerId,
    perf
  };
}

/**
 * Creates a message envelope for P2P transmission.
 * 
 * @param {string} type - The type of message (should be one of MessageType enum values).
 * @param {Object} payload - The actual data payload to be transmitted.
 * @param {string} peerId - The identifier of the peer sending the message.
 * @param {Object} [vectorClock] - The current vector clock for ordering events.
 * @returns {Object} The complete message envelope ready to be encoded.
 */
export function createEnvelope(type, payload, peerId, vectorClock) {
  return {
    id: uuidv4(),
    type,
    sender: peerId,
    timestamp: Date.now(),
    vectorClock,
    payload
  };
}

/**
 * Encodes a message envelope into a Uint8Array for network transmission.
 * 
 * @param {Object} envelope - The message envelope to encode.
 * @returns {Uint8Array} The encoded byte array representation.
 */
export function encode(envelope) {
  return new TextEncoder().encode(JSON.stringify(envelope));
}

/**
 * Decodes a Uint8Array payload back into a message envelope object.
 * 
 * @param {Uint8Array} uint8array - The encoded byte array received from network.
 * @returns {Object} The parsed message envelope object.
 */
export function decode(uint8array) {
  return JSON.parse(new TextDecoder().decode(uint8array));
}

/**
 * A simple LRU (Least Recently Used) cache for message deduplication.
 * Stores up to 1000 message IDs to prevent re-processing gossip messages.
 */
export class MessageCache {
  /**
   * Initializes the message cache with a fixed maximum size of 1000 entries.
   */
  constructor() {
    /** @private */
    this.cache = new Map();
    /** @private */
    this.maxSize = 1000;
  }

  /**
   * Checks if a message ID exists in the cache.
   * If it does, marks it as recently used by moving it to the end.
   * 
   * @param {string} id - The message ID to check.
   * @returns {boolean} True if the message ID was already processed, false otherwise.
   */
  has(id) {
    if (this.cache.has(id)) {
      // Move to end (most recently used)
      this.cache.delete(id);
      this.cache.set(id, true);
      return true;
    }
    return false;
  }

  /**
   * Adds a new message ID to the cache.
   * Evicts the oldest entry if the cache size exceeds maxSize.
   * 
   * @param {string} id - The message ID to add.
   */
  add(id) {
    if (this.cache.size >= this.maxSize) {
      // Map iteration returns in insertion order; first is oldest
      const oldestKey = this.cache.keys().next().value;
      this.cache.delete(oldestKey);
    }
    this.cache.set(id, true);
  }
}
