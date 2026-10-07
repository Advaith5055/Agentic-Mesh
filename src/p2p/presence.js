/**
 * @module p2p/presence
 * @description Lets nodes learn each other's names, roles and models.
 *
 * Two sources, in order of preference:
 *   1. presence  — nodes announce themselves with PEER_ANNOUNCE on the 'mesh:control' topic
 *                  (the format used by the other laptops' nodes, including live CPU/RAM).
 *   2. gateway   — for peers running older code that never announce, ask the peer's own
 *                  dashboard API (/api/health) for its name. The answer is only accepted
 *                  when the peerId it reports matches the connected peer.
 */

import os from 'node:os';
import { createEnvelope, encode, decode } from '../utils/protocol.js';
import { logger } from '../utils/logger.js';

export const PRESENCE_TOPIC = 'mesh:control';
export const PRESENCE_TYPE = 'PEER_ANNOUNCE';

const ANNOUNCE_INTERVAL_MS = 4_000;
const PROBE_INTERVAL_MS = 5_000;
const GATEWAY_PORTS = [3001, 3002, 3003, 3004];

/**
 * Keeps only short, printable string fields from untrusted peer-supplied info.
 * @param {Object} raw
 * @returns {Object}
 */
export function sanitizeInfo(raw = {}) {
  const clean = {};
  for (const key of ['name', 'role', 'model', 'visionModel']) {
    if (typeof raw[key] === 'string' && raw[key].trim()) {
      clean[key] = raw[key].replace(/[^\w .:@/+-]/g, '').trim().slice(0, 64);
    }
  }
  if (Number.isInteger(raw.wsPort) && raw.wsPort > 0 && raw.wsPort < 65536) clean.wsPort = raw.wsPort;
  if (raw.perf && typeof raw.perf === 'object') {
    const perf = {};
    for (const key of ['cpuPercent', 'memPercent', 'processMemMb', 'uptimeSec']) {
      const v = Number(raw.perf[key]);
      if (Number.isFinite(v) && v >= 0) perf[key] = Math.round(v * 10) / 10;
    }
    if (Object.keys(perf).length) clean.perf = perf;
  }
  return clean;
}

/**
 * Reads a PEER_ANNOUNCE payload ({ nodeName, nodeRole, model?, perf }) into peer info.
 * @param {Object} payload
 * @returns {Object}
 */
export function infoFromAnnounce(payload = {}) {
  return sanitizeInfo({
    name: payload.nodeName,
    role: payload.nodeRole,
    model: payload.model,
    visionModel: payload.visionModel,
    wsPort: payload.wsPort,
    perf: payload.perf
  });
}

/**
 * Builds this node's PEER_ANNOUNCE payload in the shared mesh:control format.
 * @param {string} peerId
 * @param {Object} local - { name, role, model, visionModel, wsPort, cpuPercent }
 * @returns {Object}
 */
export function buildAnnounce(peerId, local) {
  return {
    peerId,
    nodeName: local.name,
    nodeRole: local.role,
    status: 'connected',
    targetPeerId: null,
    model: local.model,
    visionModel: local.visionModel,
    wsPort: local.wsPort,
    perf: {
      cpuPercent: local.cpuPercent ?? 0,
      memPercent: Math.round((1 - os.freemem() / os.totalmem()) * 1000) / 10,
      processMemMb: Math.round((process.memoryUsage().rss / 1024 / 1024) * 10) / 10,
      uptimeSec: Math.round(process.uptime()),
      timestamp: Date.now()
    }
  };
}

/**
 * LAN IPv4 addresses from a peer's multiaddrs (loopback excluded).
 * @param {string[]} addrs
 * @returns {string[]}
 */
export function lanIps(addrs = []) {
  const ips = addrs
    .map(a => /^\/ip4\/([\d.]+)\//.exec(String(a))?.[1])
    .filter(Boolean);
  const nonLoopback = ips.filter(ip => !ip.startsWith('127.'));
  return [...new Set(nonLoopback.length ? nonLoopback : ips)];
}

/**
 * Asks a peer's dashboard gateway for its name. Accepts the answer only if the
 * reported peerId matches, so another service on that port cannot spoof a name.
 *
 * @param {{ peerId: string, addrs: string[] }} peer
 * @param {Object} [options]
 * @param {number[]} [options.ports]
 * @param {Function} [options.fetchFn]
 * @returns {Promise<Object|null>} Sanitised info or null.
 */
export async function probeGateway(peer, { ports = GATEWAY_PORTS, fetchFn = fetch, timeoutMs = 1500 } = {}) {
  for (const ip of lanIps(peer.addrs)) {
    for (const port of ports) {
      try {
        const res = await fetchFn(`http://${ip}:${port}/api/health`, { signal: AbortSignal.timeout(timeoutMs) });
        if (!res.ok) continue;
        const health = await res.json();
        if (health.peerId !== peer.peerId) continue;
        return {
          ...sanitizeInfo({ name: health.nodeName, role: health.nodeRole, model: health.activeModel || health.modelName, wsPort: port }),
          source: 'gateway'
        };
      } catch {
        // closed port, firewall, or not a mesh gateway — try the next one
      }
    }
  }
  return null;
}

/**
 * Starts presence announcements and name discovery.
 *
 * @param {import('libp2p').Libp2p} node
 * @param {import('./discovery.js').PeerRegistry} peerRegistry
 * @param {Function} getLocalInfo - () => { name, role, model, visionModel, wsPort, cpuPercent }
 * @param {Object} [options]
 * @param {Function} [options.probe] - Gateway probe (injectable for tests).
 * @param {Function} [options.onControlMessage] - (type, fromPeer, envelope) for other mesh:control traffic.
 * @returns {{ announce: Function, stop: Function }}
 */
export function setupPresence(node, peerRegistry, getLocalInfo, { probe = probeGateway, onControlMessage = () => {} } = {}) {
  const pubsub = node.services.pubsub;
  const selfId = node.peerId.toString();
  pubsub.subscribe(PRESENCE_TOPIC);

  pubsub.addEventListener('message', (event) => {
    const message = event.detail;
    if (message.topic !== PRESENCE_TOPIC) return;
    try {
      const fromPeer = message.from.toString();
      const envelope = decode(message.data);
      if (!envelope || envelope.sender !== fromPeer) return;
      if (envelope.type !== PRESENCE_TYPE) {
        onControlMessage(String(envelope.type), fromPeer, envelope);
        return;
      }
      const info = infoFromAnnounce(envelope.payload);
      const known = peerRegistry.getPeers().find(p => p.peerId === fromPeer);
      const isNew = known && known.name !== info.name;
      if (peerRegistry.setInfo(fromPeer, { ...info, source: 'presence' }) && isNew) {
        logger.p2p(`Peer ${fromPeer.slice(0, 12)}... is "${info.name || 'unnamed'}" (${info.role || 'peer'})`);
      }
    } catch (err) {
      logger.debug(`Ignored malformed presence message: ${err.message}`);
    }
  });

  const announce = async () => {
    try {
      const envelope = createEnvelope(PRESENCE_TYPE, buildAnnounce(selfId, getLocalInfo()), selfId, undefined);
      delete envelope.vectorClock;
      await pubsub.publish(PRESENCE_TOPIC, encode(envelope));
    } catch {
      // no subscribed peers yet
    }
  };

  const probing = new Set();
  const probeUnnamed = async () => {
    for (const peer of peerRegistry.getPeers()) {
      if ((peer.name && peer.name !== 'unknown') || !peer.connectedAt || probing.has(peer.peerId)) continue;
      probing.add(peer.peerId);
      try {
        const info = await probe(peer);
        if (info && peerRegistry.setInfo(peer.peerId, info)) {
          logger.p2p(`Peer ${peer.peerId.slice(0, 12)}... is "${info.name}" (from its gateway on port ${info.wsPort})`);
        }
      } finally {
        probing.delete(peer.peerId);
      }
    }
  };

  peerRegistry.on('peer:connected', () => {
    // Probe immediately and also after gossip mesh forms
    probeUnnamed().catch(() => {});
    setTimeout(() => { announce(); probeUnnamed(); }, 2000).unref?.();
  });

  const timers = [
    setInterval(announce, ANNOUNCE_INTERVAL_MS),
    setInterval(probeUnnamed, PROBE_INTERVAL_MS)
  ];
  timers.forEach(t => t.unref?.());

  return { announce, probeUnnamed, stop: () => timers.forEach(clearInterval) };
}
