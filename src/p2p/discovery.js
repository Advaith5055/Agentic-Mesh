/**
 * @module p2p/discovery
 * @description mDNS peer discovery and peer registry management.
 */

import { EventEmitter } from 'node:events';
import { logger } from '../utils/logger.js';

/**
 * Manages the registry of discovered and connected peers.
 * Extends EventEmitter to emit 'peer:joined', 'peer:connected', and 'peer:left' events.
 * 
 * @class PeerRegistry
 * @extends EventEmitter
 */
export class PeerRegistry extends EventEmitter {
  constructor() {
    super();
    /**
     * Map of peerId string to peer info object.
     * @type {Map<string, { peerId: string, addrs: string[], connectedAt: number, lastSeen: number }>}
     */
    this.peers = new Map();
  }

  /**
   * Attaches network event listeners to the libp2p node for peer discovery and connection events.
   * 
   * @param {import('libp2p').Libp2p} node - The libp2p node instance.
   */
  setupDiscovery(node) {
    try {
      // Listen for new peers being discovered
      node.addEventListener('peer:discovery', (event) => {
        const peerIdStr = event.detail.id.toString();
        const multiaddrs = event.detail.multiaddrs.map(addr => addr.toString());
        
        logger.debug(`Discovered peer: ${peerIdStr}`);
        
        if (!this.peers.has(peerIdStr)) {
          this.peers.set(peerIdStr, {
            peerId: peerIdStr,
            addrs: multiaddrs,
            connectedAt: 0, // Will be set on connect
            lastSeen: Date.now()
          });
          this.emit('peer:joined', { peerId: peerIdStr, addrs: multiaddrs });
        }
      });

      // Listen for peer connections
      node.addEventListener('peer:connect', (event) => {
        const peerIdStr = event.detail.toString();
        logger.info(`Connected to peer: ${peerIdStr}`);
        
        const peerInfo = this.peers.get(peerIdStr) || {
          peerId: peerIdStr,
          addrs: [],
        };
        
        peerInfo.connectedAt = Date.now();
        peerInfo.lastSeen = Date.now();
        this.peers.set(peerIdStr, peerInfo);
        
        this.emit('peer:connected', peerInfo);
      });

      // Listen for peer disconnections
      node.addEventListener('peer:disconnect', (event) => {
        const peerIdStr = event.detail.toString();
        logger.info(`Disconnected from peer: ${peerIdStr}`);
        
        if (this.peers.has(peerIdStr)) {
          const peerInfo = this.peers.get(peerIdStr);
          this.peers.delete(peerIdStr);
          this.emit('peer:left', peerInfo);
        }
      });
      
      logger.info('Peer discovery setup completed');
    } catch (error) {
      logger.error('Failed to setup discovery:', error);
    }
  }

  /**
   * Retrieves an array of all current peer info objects in the registry.
   * 
   * @returns {Array<{ peerId: string, addrs: string[], connectedAt: number, lastSeen: number }>}
   */
  getPeers() {
    return Array.from(this.peers.values());
  }

  /**
   * Retrieves the current count of peers in the registry.
   * 
   * @returns {number} The number of known peers.
   */
  getPeerCount() {
    return this.peers.size;
  }

  /**
   * Checks if a given peer ID is currently in the registry.
   * 
   * @param {string} peerId - The peer ID string to check.
   * @returns {boolean} True if the peer is in the registry, false otherwise.
   */
  hasPeer(peerId) {
    return this.peers.has(peerId);
  }

  /**
   * Returns a serializable representation of the peer list.
   * Useful for sending data over WebSockets or REST APIs.
   * 
   * @returns {Array<{ peerId: string, addrs: string[], connectedAt: number, lastSeen: number }>}
   */
  toJSON() {
    return this.getPeers();
  }
}
