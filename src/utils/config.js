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
 * Supported node roles in the Agentic Mesh.
 * @readonly
 * @enum {string}
 */
export const NodeRole = {
  PEER: 'peer',
  ROUTER: 'router',
  PLANNER: 'planner',
  EXECUTOR: 'executor',
  VALIDATOR: 'validator'
};

export const SUPPORTED_ROLES = Object.values(NodeRole);

/**
 * Validates the NODE_ROLE environment variable or provided string.
 * Fails fast with a helpful message if an unsupported role is specified.
 *
 * @param {string} [role] - The role string to validate.
 * @returns {string} The validated, lowercase role string.
 * @throws {Error} If role is not one of SUPPORTED_ROLES.
 */
export function validateNodeRole(role) {
  const normalized = (role || 'peer').toLowerCase().trim();
  // Support legacy 'ai-agent' as alias for 'peer' so existing .env setups do not break
  if (normalized === 'ai-agent') {
    return NodeRole.PEER;
  }
  if (!SUPPORTED_ROLES.includes(normalized)) {
    throw new Error(
      `Invalid NODE_ROLE "${role}". Supported roles are: ${SUPPORTED_ROLES.join(', ')} (default: "${NodeRole.PEER}").`
    );
  }
  return normalized;
}

/**
 * Configuration object containing environment variables and default values.
 * @type {Object}
 * @property {string} NODE_NAME - The unique name for this node in the mesh.
 * @property {string} NODE_ROLE - The role of this node ('peer', 'router', 'planner', 'executor', 'validator').
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
 * @property {string} CONTROL_TOPIC - The Libp2p pubsub topic used for agent coordination.
 * @property {boolean} ROUTER_LOCAL_FALLBACK - Allow router to fall back to local planning/execution if peers missing.
 * @property {number} PLAN_TIMEOUT_MS - Timeout in ms when waiting for remote PLAN_RESPONSE.
 * @property {number} EXECUTION_TIMEOUT_MS - Timeout in ms when waiting for remote EXECUTION_RESPONSE.
 */
export const config = {
  NODE_NAME:             process.env.NODE_NAME             || `node-${Math.random().toString(36).slice(2, 8)}`,
  NODE_ROLE:             validateNodeRole(process.env.NODE_ROLE || 'peer'),
  P2P_PORT:              parseInt(process.env.P2P_PORT     || '9004', 10),
  WS_PORT:               parseInt(process.env.WS_PORT      || '3004', 10),
  DB_PATH:               process.env.DB_PATH               || './data/mesh.db',
  OLLAMA_HOST:           process.env.OLLAMA_HOST           || 'http://127.0.0.1:11434',
  OLLAMA_MODEL:          process.env.OLLAMA_MODEL          || 'qwen2.5-coder:1.5b',
  OLLAMA_NUM_CTX:        parseInt(process.env.OLLAMA_NUM_CTX || '2048', 10),
  OLLAMA_NUM_PREDICT:    parseInt(process.env.OLLAMA_NUM_PREDICT || '512', 10),
  OLLAMA_TEMPERATURE:    parseFloat(process.env.OLLAMA_TEMPERATURE || '0.2'),
  OLLAMA_TIMEOUT_MS:     parseInt(process.env.OLLAMA_TIMEOUT_MS  || '90000', 10),
  OLLAMA_KEEP_ALIVE:     process.env.OLLAMA_KEEP_ALIVE     || '5m',
  MAX_RETRIES:           parseInt(process.env.MAX_RETRIES  || '3', 10),
  GOSSIP_TOPIC:          'mesh:transactions',
  SYNC_TOPIC:            'mesh:sync',
  CONTROL_TOPIC:         'mesh:control',
  ROUTER_LOCAL_FALLBACK: process.env.ROUTER_LOCAL_FALLBACK === 'true',
  PLAN_TIMEOUT_MS:       parseInt(process.env.PLAN_TIMEOUT_MS || '45000', 10),
  EXECUTION_TIMEOUT_MS:  parseInt(process.env.EXECUTION_TIMEOUT_MS || '45000', 10),

  // Visual lane — image → vision model → observation → planner → approval → executor
  VISION_ENABLED:          process.env.VISION_ENABLED === 'true',
  VISION_MODEL:            process.env.VISION_MODEL            || 'gemma3:4b',
  VISION_NUM_CTX:          parseInt(process.env.VISION_NUM_CTX || '4096', 10),
  VISION_NUM_PREDICT:      parseInt(process.env.VISION_NUM_PREDICT || '512', 10),
  VISION_TIMEOUT_MS:       parseInt(process.env.VISION_TIMEOUT_MS || '180000', 10),
  VISION_MAX_IMAGE_BYTES:  parseInt(process.env.VISION_MAX_IMAGE_BYTES || String(10 * 1024 * 1024), 10),
  VISION_PLAN_NUM_CTX:     parseInt(process.env.VISION_PLAN_NUM_CTX || '4096', 10),
  VISION_PLAN_NUM_PREDICT: parseInt(process.env.VISION_PLAN_NUM_PREDICT || '512', 10),
  PROPOSAL_TTL_MS:         parseInt(process.env.PROPOSAL_TTL_MS || '600000', 10),

  // Planner ↔ Executor handoff configuration
  EXECUTOR_PEER:           process.env.EXECUTOR_PEER           || 'alpha',
  BOOTSTRAP_PEERS:         (process.env.BOOTSTRAP_PEERS || '').split(',').map(s => s.trim()).filter(Boolean),

  // Security & Authentication configuration
  API_KEY:                 process.env.MESH_API_KEY || process.env.API_KEY || 'mesh-dev-key',
  ALLOW_PUBLIC_READ:       process.env.ALLOW_PUBLIC_READ !== 'false',
  CORS_ORIGINS:            (process.env.CORS_ORIGINS || 'http://localhost:5173,http://127.0.0.1:5173,http://localhost:3000,http://127.0.0.1:3000').split(',').map(s => s.trim()).filter(Boolean),

  // Demo Seeding configuration
  SEED_DEMO_DATA:          process.env.SEED_DEMO_DATA === 'true'
};
