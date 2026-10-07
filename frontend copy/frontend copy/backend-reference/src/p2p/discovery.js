/**
 * @module p2p/discovery
 * @description mDNS peer discovery and peer registry management.
 */

import { EventEmitter } from 'node:events';
import { multiaddr } from '@multiformats/multiaddr';
import { logger } from '../utils/logger.js';

const LOOPBACK_RE = /^\/(ip4\/127\.|ip6\/::1\/)/;

/**
 * Orders a peer's advertised addresses so LAN addresses are tried before loopback.
 * A remote laptop's 127.0.0.1 address points back at this machine, and when both
 * nodes use the same port, dialing it hits our own node ("Can not dial self").
 * Loopback stays as a last resort for two nodes on the same machine.
 *
 * @param {Array<{ toString(): string }>} multiaddrs
 * @returns {Array} Addresses in dial order.
 */
export function orderDialAddrs(multiaddrs = []) {
  const lan = multiaddrs.filter(a => !LOOPBACK_RE.test(a.toString()));
  const loopback = multiaddrs.filter(a => LOOPBACK_RE.test(a.toString()));
  return [...lan, ...loopback];
}

/**
 * Keeps connections to fixed peer addresses (e.g. "/ip4/192.168.1.7/tcp/9004"), for
 * networks where mDNS discovery is unreliable. Re-dials every interval while disconnected.
 *
 * @param {import('libp2p').Libp2p} node
 * @param {string[]} addrs - Multiaddr strings.
 * @param {Object} [options]
 * @param {number} [options.intervalMs=15000]
 * @param {Function} [options.toMultiaddr] - Parser (injectable for tests).
 * @returns {{ stop: Function, tick: Function }}
 */
export function keepBootstrapPeers(node, addrs, { intervalMs = 15_000, toMultiaddr = multiaddr } = {}) {
  const targets = addrs.map(a => a.trim()).filter(Boolean);
  const ipOf = (addr) => /^\/ip[46]\/([^/]+)\//.exec(addr)?.[1];

  const tick = async () => {
    const connectedIps = new Set(node.getConnections().map(c => ipOf(c.remoteAddr.toString())));
    for (const addr of targets) {
      if (connectedIps.has(ipOf(addr))) continue;
      try {
        await node.dial(toMultiaddr(addr));
        logger.p2p(`Connected to bootstrap peer ${addr}`);
      } catch (err) {
        logger.debug(`Bootstrap dial ${addr} failed: ${err.message}`);
      }
    }
  };

  if (targets.length === 0) return { stop: () => {}, tick };
  tick();
  const timer = setInterval(tick, intervalMs);
  timer.unref?.();
  return { stop: () => clearInterval(timer), tick };
}

/**
 * Dials a discovered peer, trying each address in order until one connects.
 * @param {import('libp2p').Libp2p} node
 * @param {{ id: Object, multiaddrs?: Array }} peer - The peer:discovery event detail.
 */
async function dialFirstReachable(node, peer) {
  const addrs = orderDialAddrs(peer.multiaddrs);
  if (addrs.length === 0) {
    await node.dial(peer.id);
    return;
  }
  let lastError;
  for (const addr of addrs) {
    try {
      await node.dial(addr);
      return;
    } catch (err) {
      lastError = err;
      logger.debug(`Dial ${addr.toString()} failed: ${err.message}`);
    }
  }
  throw lastError;
}

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
      node.addEventListener('peer:discovery', async (event) => {
        const peerIdStr = event.detail.id.toString();
        const multiaddrs = event.detail.multiaddrs.map(addr => addr.toString());
        
        logger.p2p(`Discovered peer via mDNS: ${peerIdStr.slice(0, 12)}...`);
        
        if (!this.peers.has(peerIdStr)) {
          this.peers.set(peerIdStr, {
            peerId: peerIdStr,
            addrs: multiaddrs,
            connectedAt: 0, // Will be set on connect
            lastSeen: Date.now()
          });
          this.emit('peer:joined', { peerId: peerIdStr, addrs: multiaddrs });
        }

        // Proactively dial discovered peer if not already connected
        try {
          const connections = typeof node.getConnections === 'function' ? node.getConnections() : [];
          const isConnected = connections.some(c => c.remotePeer.toString() === peerIdStr);
          if (!isConnected && typeof node.dial === 'function') {
            logger.p2p(`Dialing discovered peer ${peerIdStr.slice(0, 12)}...`);
            await dialFirstReachable(node, event.detail);
          }
        } catch (dialErr) {
          logger.debug(`Dial attempt to ${peerIdStr.slice(0, 12)}...: ${dialErr.message}`);
        }
      });

      // Listen for peer connections
      node.addEventListener('peer:connect', (event) => {
        const peerIdStr = event.detail.toString();
        logger.p2p(`Connected to peer: ${peerIdStr.slice(0, 12)}...`);
        
        const peerInfo = this.peers.get(peerIdStr) || {
          peerId: peerIdStr,
          addrs: [],
        };
        if (!peerInfo.addrs?.length && typeof node.getConnections === 'function') {
          // Peers dialled directly (not via mDNS) have no advertised addresses yet
          peerInfo.addrs = node.getConnections(event.detail).map(c => c.remoteAddr.toString());
        }
        
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
   * Attaches descriptive info (name, role, model…) to a known peer and notifies listeners.
   * @param {string} peerId - The peer ID string.
   * @param {Object} info - Fields to merge, e.g. { name, role, model, source }.
   * @returns {boolean} True if the peer was known and updated.
   */
  setInfo(peerId, info) {
    const peerInfo = this.peers.get(peerId);
    if (!peerInfo) return false;
    Object.assign(peerInfo, info, { lastSeen: Date.now() });
    this.emit('peer:updated', peerInfo);
    return true;
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
