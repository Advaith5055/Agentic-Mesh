/**
 * @fileoverview Environment configuration with sensible defaults.
 * @module utils/config
 */

import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';

// Automatically load .env if present in project root
const envPath = resolve(process.cwd(), '.env');
if (existsSync(envPath)) {
  if (typeof process.loadEnvFile === 'function') {
    try { process.loadEnvFile(envPath); } catch { /* ignore */ }
  } else {
    try {
      const lines = readFileSync(envPath, 'utf8').split('\n');
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith('#')) continue;
        const idx = trimmed.indexOf('=');
        if (idx !== -1) {
          const key = trimmed.slice(0, idx).trim();
          const val = trimmed.slice(idx + 1).trim();
          if (process.env[key] === undefined) process.env[key] = val;
        }
      }
    } catch { /* ignore */ }
  }
}

/**
 * Configuration object containing environment variables and default values.
 * @type {Object}
 * @property {string} NODE_NAME - The unique name for this node in the mesh.
 * @property {number} P2P_PORT - Port used for Peer-to-Peer communication.
 * @property {number} WS_PORT - Port used for WebSocket communication.
 * @property {string} DB_PATH - File path for the SQLite database.
 * @property {string} OLLAMA_HOST - URL for the Ollama API host.
 * @property {string} OLLAMA_MODEL - Name of the LLM model to use with Ollama.
 * @property {number} OLLAMA_NUM_CTX - Context window size for Ollama inference.
 * @property {number} OLLAMA_NUM_PREDICT - Maximum output tokens to predict.
 * @property {number} OLLAMA_TEMPERATURE - Generation temperature for model sampling.
 * @property {number} OLLAMA_TIMEOUT_MS - Server-side generation timeout in ms.
 * @property {string} OLLAMA_KEEP_ALIVE - Ollama model keep-alive duration.
 * @property {number} MAX_RETRIES - Maximum number of retries for network operations.
 * @property {string} GOSSIP_TOPIC - The Libp2p pubsub topic used for transaction gossip.
 * @property {string} SYNC_TOPIC - The Libp2p pubsub topic used for state synchronization.
 */
export const config = {
  NODE_NAME:           process.env.NODE_NAME           || `node-${Math.random().toString(36).slice(2, 8)}`,
  NODE_ROLE:           process.env.NODE_ROLE           || 'peer',
  P2P_PORT:            parseInt(process.env.P2P_PORT   || '9004', 10),
  WS_PORT:             parseInt(process.env.WS_PORT    || '3004', 10),
  DB_PATH:             process.env.DB_PATH             || './data/mesh.db',
  OLLAMA_HOST:         process.env.OLLAMA_HOST         || 'http://127.0.0.1:11434',
  OLLAMA_MODEL:        process.env.OLLAMA_MODEL        || 'qwen2.5-coder:1.5b',
  OLLAMA_NUM_CTX:      parseInt(process.env.OLLAMA_NUM_CTX || '1024', 10),
  OLLAMA_NUM_PREDICT:  parseInt(process.env.OLLAMA_NUM_PREDICT || '128', 10),
  OLLAMA_TEMPERATURE:  parseFloat(process.env.OLLAMA_TEMPERATURE || '0.2'),
  OLLAMA_TIMEOUT_MS:   parseInt(process.env.OLLAMA_TIMEOUT_MS  || '90000', 10),
  OLLAMA_KEEP_ALIVE:   process.env.OLLAMA_KEEP_ALIVE   || '5m',
  MAX_RETRIES:         parseInt(process.env.MAX_RETRIES || '3', 10),
  GOSSIP_TOPIC:        'mesh:transactions',
  SYNC_TOPIC:          'mesh:sync',

  // Visual lane — image → vision model → observation → planner → approval → executor
  VISION_ENABLED:          process.env.VISION_ENABLED !== 'false',
  VISION_MODEL:            process.env.VISION_MODEL            || 'gemma3:4b',
  VISION_NUM_CTX:          parseInt(process.env.VISION_NUM_CTX || '4096', 10),
  VISION_NUM_PREDICT:      parseInt(process.env.VISION_NUM_PREDICT || '512', 10),
  VISION_TIMEOUT_MS:       parseInt(process.env.VISION_TIMEOUT_MS || '180000', 10),
  VISION_MAX_IMAGE_BYTES:  parseInt(process.env.VISION_MAX_IMAGE_BYTES || String(10 * 1024 * 1024), 10),
  VISION_PLAN_NUM_CTX:     parseInt(process.env.VISION_PLAN_NUM_CTX || '4096', 10),
  VISION_PLAN_NUM_PREDICT: parseInt(process.env.VISION_PLAN_NUM_PREDICT || '512', 10),
  PROPOSAL_TTL_MS:         parseInt(process.env.PROPOSAL_TTL_MS || '600000', 10),

  // Executor ↔ Planner over the mesh: 'auto' = use a connected planner node, else plan locally
  PLANNER_MODE:            process.env.PLANNER_MODE            || 'auto',
  PLANNER_PEER:            process.env.PLANNER_PEER            || '',
  REMOTE_PLAN_TIMEOUT_MS:  parseInt(process.env.REMOTE_PLAN_TIMEOUT_MS || '90000', 10),
  PLAN_MAX_ROUNDS:         parseInt(process.env.PLAN_MAX_ROUNDS || '3', 10),

  // Fixed peers to keep connected when mDNS discovery is unreliable (comma-separated multiaddrs)
  BOOTSTRAP_PEERS:         (process.env.BOOTSTRAP_PEERS || '').split(',').map(s => s.trim()).filter(Boolean),
};
