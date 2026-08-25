/**
 * @module p2p/gossip
 * @description GossipSub topic management and message protocol handling.
 */

import { config } from '../utils/config.js';
import { encode, decode, MessageCache, MessageType } from '../utils/protocol.js';
import { logger } from '../utils/logger.js';

// Initialize a cache to deduplicate incoming messages
const messageCache = new MessageCache();

/**
 * Sets up GossipSub event listeners for a given node.
 * Subscribes to configured topics and routes incoming messages to the provided handlers.
 *
 * @param {import('libp2p').Libp2p} node - The libp2p node instance.
 * @param {Object} handlers - The message handlers object.
 * @param {Function} [handlers.onTransaction] - Handler for TRANSACTION messages.
 * @param {Function} [handlers.onSyncRequest] - Handler for SYNC_REQUEST messages.
 * @param {Function} [handlers.onSyncResponse] - Handler for SYNC_RESPONSE messages.
 * @param {Function} [handlers.onAiAudit] - Handler for AI_AUDIT messages.
 */
export function setupGossip(node, handlers) {
  try {
    const pubsub = node.services.pubsub;

    if (!pubsub) {
      throw new Error('Pubsub service is not available on the node');
    }

    // Subscribe to topics
    pubsub.subscribe(config.GOSSIP_TOPIC);
    pubsub.subscribe(config.SYNC_TOPIC);
    
    logger.info(`Subscribed to topics: ${config.GOSSIP_TOPIC}, ${config.SYNC_TOPIC}`);

    // Add message event listener
    pubsub.addEventListener('message', (event) => {
      try {
        const message = event.detail;
        const fromPeer = message.from.toString();
        
        // Decode the incoming message data
        const decoded = decode(message.data);
        if (!decoded) {
          logger.error(`Failed to decode message from ${fromPeer}`);
          return;
        }

        const { id, type } = decoded;

        // Check cache for deduplication
        if (messageCache.has(id)) {
          return; // Silently skip duplicates
        }
        
        // Mark message as seen
        messageCache.add(id);

        logger.p2p(`Received ${type} message from ${fromPeer.slice(0, 12)}...`);

        // Route to appropriate handler based on type
        switch (type) {
          case MessageType.TRANSACTION:
            if (typeof handlers.onTransaction === 'function') {
              handlers.onTransaction(decoded, fromPeer);
            }
            break;
          case MessageType.SYNC_REQUEST:
            if (typeof handlers.onSyncRequest === 'function') {
              handlers.onSyncRequest(decoded, fromPeer);
            }
            break;
          case MessageType.SYNC_RESPONSE:
            if (typeof handlers.onSyncResponse === 'function') {
              handlers.onSyncResponse(decoded, fromPeer);
            }
            break;
          case MessageType.AI_AUDIT:
            if (typeof handlers.onAiAudit === 'function') {
              handlers.onAiAudit(decoded, fromPeer);
            }
            break;
          default:
            logger.error(`Unknown message type received: ${type}`);
        }
      } catch (err) {
        logger.error(`Error processing incoming gossip message: ${err.message}`);
      }
    });

    logger.info('GossipSub setup completed successfully');
  } catch (error) {
    logger.error(`Failed to setup gossip: ${error.message}`);
  }
}

/**
 * Encodes and publishes a transaction envelope to the default gossip topic.
 *
 * @async
 * @param {import('libp2p').Libp2p} node - The libp2p node instance.
 * @param {Object} envelope - The message envelope to publish.
 * @returns {Promise<void>}
 */
export async function publishTransaction(node, envelope) {
  await publishToTopic(node, config.GOSSIP_TOPIC, envelope);
}

/**
 * Encodes and publishes a sync envelope to the sync topic.
 *
 * @async
 * @param {import('libp2p').Libp2p} node - The libp2p node instance.
 * @param {Object} envelope - The message envelope to publish.
 * @returns {Promise<void>}
 */
export async function publishSync(node, envelope) {
  await publishToTopic(node, config.SYNC_TOPIC, envelope);
}

/**
 * Encodes and publishes a generic envelope to the specified topic.
 * Catches and logs errors gracefully (e.g., when no peers are connected).
 *
 * @async
 * @param {import('libp2p').Libp2p} node - The libp2p node instance.
 * @param {string} topic - The topic string to publish to.
 * @param {Object} envelope - The message envelope to publish.
 * @returns {Promise<void>}
 */
export async function publishToTopic(node, topic, envelope) {
  try {
    const pubsub = node.services.pubsub;
    if (!pubsub) {
      logger.error('Cannot publish: Pubsub service is not available');
      return;
    }

    const encodedData = encode(envelope);
    await pubsub.publish(topic, encodedData);
    
    logger.p2p(`Published ${envelope.type} to ${topic}`);
  } catch (error) {
    // Normal to hit 'InsufficientPeers' during startup — not an error
    if (error.message && error.message.includes('InsufficientPeers')) {
      logger.p2p(`No peers yet for topic ${topic} — message queued`);
    } else {
      logger.error(`Failed to publish to ${topic}: ${error.message}`);
    }
  }
}
