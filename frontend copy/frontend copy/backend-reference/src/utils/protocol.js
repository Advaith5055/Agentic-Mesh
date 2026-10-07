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
  AI_AUDIT: 'AI_AUDIT'
};

/**
 * Creates a message envelope for P2P transmission.
 * 
 * @param {string} type - The type of message (should be one of MessageType enum values).
 * @param {Object} payload - The actual data payload to be transmitted.
 * @param {string} peerId - The identifier of the peer sending the message.
 * @param {Object} vectorClock - The current vector clock for ordering events.
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
