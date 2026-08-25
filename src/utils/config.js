/**
 * @fileoverview Environment configuration with sensible defaults.
 * @module utils/config
 */

/**
 * Configuration object containing environment variables and default values.
 * @type {Object}
 * @property {string} NODE_NAME - The unique name for this node in the mesh.
 * @property {number} P2P_PORT - Port used for Peer-to-Peer communication.
 * @property {number} WS_PORT - Port used for WebSocket communication.
 * @property {string} DB_PATH - File path for the SQLite database.
 * @property {string} OLLAMA_HOST - URL for the Ollama API host.
 * @property {string} OLLAMA_MODEL - Name of the LLM model to use with Ollama.
 * @property {number} MAX_RETRIES - Maximum number of retries for network operations.
 * @property {string} GOSSIP_TOPIC - The Libp2p pubsub topic used for transaction gossip.
 * @property {string} SYNC_TOPIC - The Libp2p pubsub topic used for state synchronization.
 */
export const config = {
  NODE_NAME:    process.env.NODE_NAME    || `node-${Math.random().toString(36).slice(2, 8)}`,
  P2P_PORT:     parseInt(process.env.P2P_PORT || '9001', 10),
  WS_PORT:      parseInt(process.env.WS_PORT  || '3001', 10),
  DB_PATH:      process.env.DB_PATH      || './data/mesh.db',
  OLLAMA_HOST:  process.env.OLLAMA_HOST  || 'http://localhost:11434',
  OLLAMA_MODEL: process.env.OLLAMA_MODEL || 'gemma4:e2b',
  MAX_RETRIES:  parseInt(process.env.MAX_RETRIES || '3', 10),
  GOSSIP_TOPIC: 'mesh:transactions',
  SYNC_TOPIC:   'mesh:sync',
};
