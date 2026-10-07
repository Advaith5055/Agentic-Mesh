/**
 * @module p2p/node
 * @description Factory and lifecycle management for the libp2p node.
 */

import { createLibp2p } from 'libp2p';
import { tcp } from '@libp2p/tcp';
import { mdns } from '@libp2p/mdns';
import { noise } from '@chainsafe/libp2p-noise';
import { mplex } from '@libp2p/mplex';
import { gossipsub } from '@libp2p/gossipsub';
import { identify } from '@libp2p/identify';
import { logger } from '../utils/logger.js';

/**
 * Creates a fully configured libp2p node for the mesh network.
 * The node is returned unstarted.
 * 
 * @async
 * @param {number} port - The TCP port on which the node should listen.
 * @returns {Promise<import('libp2p').Libp2p>} The created libp2p node instance.
 */
export async function createMeshNode(port) {
  try {
    const node = await createLibp2p({
      addresses: {
        listen: [`/ip4/0.0.0.0/tcp/${port}`]
      },
      transports: [
        tcp()
      ],
      connectionEncrypters: [
        noise()
      ],
      streamMuxers: [
        mplex()
      ],
      peerDiscovery: [
        mdns()
      ],
      services: {
        identify: identify(),
        pubsub: gossipsub({
          emitSelf: false,
          allowPublishToZeroTopicPeers: true,
          floodPublish: true,
          fallbackToFloodsub: true,
          D: 1,
          Dlo: 1,
          Dhi: 10,
          Dscore: 0,
          Dout: 0
        })
      }
    });

    logger.info(`Mesh node created on port ${port} (not started yet)`);
    return node;
  } catch (error) {
    logger.error('Failed to create mesh node:', error);
    throw error;
  }
}

/**
 * Starts the given libp2p node and logs its peer ID and multiaddrs.
 * 
 * @async
 * @param {import('libp2p').Libp2p} node - The libp2p node to start.
 * @returns {Promise<void>}
 */
export async function startNode(node) {
  try {
    await node.start();
    logger.info(`Node started successfully with Peer ID: ${node.peerId.toString()}`);
    
    // Log all addresses this node is listening on
    node.getMultiaddrs().forEach((addr) => {
      logger.info(`Listening on ${addr.toString()}`);
    });
  } catch (error) {
    logger.error('Failed to start node:', error);
    throw error;
  }
}

/**
 * Gracefully stops the given libp2p node.
 * 
 * @async
 * @param {import('libp2p').Libp2p} node - The libp2p node to stop.
 * @returns {Promise<void>}
 */
export async function stopNode(node) {
  try {
    await node.stop();
    logger.info('Node stopped gracefully');
  } catch (error) {
    logger.error('Failed to stop node:', error);
    throw error;
  }
}
