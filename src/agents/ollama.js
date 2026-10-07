/**
 * @fileoverview Ollama JS SDK Client Wrapper for local LLMs (Gemma, etc.).
 * @module agents/ollama
 */

import { Ollama } from 'ollama';
import { config } from '../utils/config.js';
import { logger } from '../utils/logger.js';

// Initialize the Ollama instance with configured host
const ollamaClient = new Ollama({ host: config.OLLAMA_HOST });

let _resolvedModel = null;

/**
 * Custom error thrown when the configured model is not available.
 */
export class ModelUnavailableError extends Error {
  constructor(message, { configuredModel, availableModels, ollamaHost }) {
    super(message);
    this.name = 'ModelUnavailableError';
    this.configuredModel = configuredModel;
    this.availableModels = availableModels;
    this.ollamaHost = ollamaHost;
  }
}

/**
 * Queries Ollama for currently loaded models in memory via /api/ps.
 * @returns {Promise<Array<{ name: string, size: number, size_vram?: number }>>}
 */
export async function getLoadedModels() {
  try {
    if (typeof ollamaClient.ps === 'function') {
      const ps = await ollamaClient.ps();
      return ps.models || [];
    }
    const res = await fetch(`${config.OLLAMA_HOST}/api/ps`, { signal: AbortSignal.timeout(3000) });
    if (res.ok) {
      const data = await res.json();
      return data.models || [];
    }
    return [];
  } catch {
    return [];
  }
}

/**
 * Queries Ollama for host reachability, installed models, active model status, and running models.
 * @returns {Promise<{ success: boolean, ollamaReachable: boolean, configuredModel: string, activeModel: string|null, availableModels: string[], loadedModels: string[], nodeRole: string }>}
 */
export async function getAgentStatus() {
  try {
    const [list, loadedModels] = await Promise.all([
      ollamaClient.list(),
      getLoadedModels()
    ]);
    const availableModels = (list.models || []).map(m => m.name);
    const match = availableModels.find(
      m => m === config.OLLAMA_MODEL || m.startsWith(`${config.OLLAMA_MODEL}:`) || m.includes(config.OLLAMA_MODEL)
    );
    return {
      success: true,
      ollamaReachable: true,
      configuredModel: config.OLLAMA_MODEL,
      activeModel: match || null,
      availableModels,
      loadedModels: (loadedModels || []).map(m => m.name || m.model),
      nodeRole: config.NODE_ROLE
    };
  } catch {
    return {
      success: false,
      ollamaReachable: false,
      configuredModel: config.OLLAMA_MODEL,
      activeModel: null,
      availableModels: [],
      loadedModels: [],
      nodeRole: config.NODE_ROLE
    };
  }
}

/**
 * Resolves the active model. Strictly requires config.OLLAMA_MODEL to exist.
 * Never silently falls back to arbitrary models.
 * @returns {Promise<string>}
 * @throws {ModelUnavailableError}
 */
export async function getActiveModel() {
  if (_resolvedModel) return _resolvedModel;

  const status = await getAgentStatus();
  if (!status.ollamaReachable) {
    throw new ModelUnavailableError(
      `Ollama host is unreachable at ${config.OLLAMA_HOST}.`,
      { configuredModel: config.OLLAMA_MODEL, availableModels: [], ollamaHost: config.OLLAMA_HOST }
    );
  }

  if (!status.activeModel) {
    throw new ModelUnavailableError(
      `Configured model "${config.OLLAMA_MODEL}" is not installed in Ollama at ${config.OLLAMA_HOST}. Available models: ${status.availableModels.join(', ') || 'none'}.`,
      { configuredModel: config.OLLAMA_MODEL, availableModels: status.availableModels, ollamaHost: config.OLLAMA_HOST }
    );
  }

  _resolvedModel = status.activeModel;
  return _resolvedModel;
}

/**
 * Prewarms the configured model at startup with a 1-token request.
 * Measures startup duration and warm-up duration, and detects processor type.
 * Never silently falls back to another installed model.
 * @param {number} [bootStartTime] - Process start timestamp in ms.
 * @returns {Promise<{ model: string, processor: string, startupDuration: number, warmupDuration: number }>}
 */
export async function prewarmModel(bootStartTime = null) {
  const model = await getActiveModel();
  const warmupStart = Date.now();

  try {
    await ollamaClient.chat({
      model,
      messages: [{ role: 'user', content: 'ping' }],
      stream: false,
      keep_alive: config.OLLAMA_KEEP_ALIVE,
      options: {
        num_ctx: config.OLLAMA_NUM_CTX,
        num_predict: 1,
        temperature: config.OLLAMA_TEMPERATURE
      },
      signal: AbortSignal.timeout(config.OLLAMA_TIMEOUT_MS)
    });
  } catch (err) {
    logger.warn(`Model prewarm ping warning: ${err.message}`);
  }

  const warmupDuration = Date.now() - warmupStart;
  const startupDuration = bootStartTime ? Date.now() - bootStartTime : 0;

  let processor = '100% CPU';
  try {
    const loaded = await getLoadedModels();
    const current = loaded.find(m => (m.name || m.model) === model || (m.name || m.model)?.startsWith(`${model}:`) || model.startsWith(m.name || m.model));
    if (current) {
      if (current.size_vram && current.size && current.size_vram >= current.size) {
        processor = '100% GPU';
      } else if (current.size_vram && current.size_vram > 0) {
        processor = `${Math.round((current.size_vram / current.size) * 100)}% GPU / CPU`;
      } else {
        processor = '100% CPU';
      }
    }
  } catch {}

  logger.ai(`Model prewarm complete: ${model} [Processor: ${processor}] | Startup: ${startupDuration}ms | Warm-up: ${warmupDuration}ms`);

  return { model, processor, startupDuration, warmupDuration };
}

/**
 * Clears cached resolved model (useful in tests).
 */
export function resetModelCache() {
  _resolvedModel = null;
}

/**
 * Sends a prompt to the configured local model using Ollama with server-side timeout.
 * 
 * @async
 * @param {string} prompt - The prompt text or instruction.
 * @param {Object} [options={}] - Additional chat options (temperature, num_predict, reqId, etc.).
 * @returns {Promise<string|null>} The generated text content or null if failed.
 */
export async function askModel(prompt, options = {}) {
  const reqId = options.reqId || 'sys';
  const timeoutMs = options.timeoutMs || config.OLLAMA_TIMEOUT_MS;
  const numCtx = options.options?.num_ctx || config.OLLAMA_NUM_CTX;
  const numPredict = options.options?.num_predict || config.OLLAMA_NUM_PREDICT;
  const temperature = options.options?.temperature !== undefined ? options.options.temperature : config.OLLAMA_TEMPERATURE;

  try {
    const model = options.model || await getActiveModel();
    logger.ai(`[reqId:${reqId}] Calling Ollama model: ${model} (timeout: ${timeoutMs}ms, num_ctx: ${numCtx}, num_predict: ${numPredict}, temp: ${temperature})`);

    const signal = options.signal || AbortSignal.timeout(timeoutMs);

    const response = await ollamaClient.chat({
      model,
      messages: [{ role: 'user', content: prompt }],
      stream: false,
      keep_alive: config.OLLAMA_KEEP_ALIVE,
      options: {
        num_ctx: numCtx,
        num_predict: numPredict,
        temperature,
        ...(options.options || {})
      },
      signal
    });

    return response.message?.content || null;
  } catch (error) {
    const isTimeout = error.name === 'TimeoutError' || error.name === 'AbortError' || error.message?.includes('timeout') || error.message?.includes('aborted');
    if (isTimeout) {
      logger.ai(`[reqId:${reqId}] Ollama request timed out after ${timeoutMs}ms`);
      throw new Error(`The local model did not respond within ${Math.round(timeoutMs / 1000)} seconds. Check Ollama CPU/GPU usage and selected model.`, { cause: error });
    }
    logger.ai(`[reqId:${reqId}] Error communicating with model via Ollama SDK: ${error.message}`);
    throw error;
  }
}

/**
 * Sends a prompt to the model and returns an async iterable of token chunks with server-side timeout.
 * 
 * @async
 * @param {string} prompt - The prompt text.
 * @param {Object} [options={}] - Additional chat options.
 * @returns {Promise<AsyncIterable|null>} Async iterable of token chunks.
 */
export async function askModelStream(prompt, options = {}) {
  const reqId = options.reqId || 'stream';
  const timeoutMs = options.timeoutMs || config.OLLAMA_TIMEOUT_MS;
  const numCtx = options.options?.num_ctx || config.OLLAMA_NUM_CTX;
  const numPredict = options.options?.num_predict || config.OLLAMA_NUM_PREDICT;
  const temperature = options.options?.temperature !== undefined ? options.options.temperature : config.OLLAMA_TEMPERATURE;

  try {
    const model = options.model || await getActiveModel();
    logger.ai(`[reqId:${reqId}] Starting Ollama stream with model: ${model} (timeout: ${timeoutMs}ms, num_ctx: ${numCtx}, num_predict: ${numPredict}, temp: ${temperature})`);

    const signal = options.signal || AbortSignal.timeout(timeoutMs);

    return await ollamaClient.chat({
      model,
      messages: [{ role: 'user', content: prompt }],
      stream: true,
      keep_alive: config.OLLAMA_KEEP_ALIVE,
      options: {
        num_ctx: numCtx,
        num_predict: numPredict,
        temperature,
        ...(options.options || {})
      },
      signal
    });
  } catch (error) {
    const isTimeout = error.name === 'TimeoutError' || error.name === 'AbortError' || error.message?.includes('timeout') || error.message?.includes('aborted');
    if (isTimeout) {
      logger.ai(`[reqId:${reqId}] Ollama stream start timed out after ${timeoutMs}ms`);
      throw new Error(`The local model did not respond within ${Math.round(timeoutMs / 1000)} seconds. Check Ollama CPU/GPU usage and selected model.`, { cause: error });
    }
    logger.ai(`[reqId:${reqId}] Error starting model stream: ${error.message}`);
    throw error;
  }
}

/**
 * Backward compatibility alias for askModel.
 */
export const askGemma = askModel;

/**
 * Checks if the configured Ollama host is responsive and the exact model is available.
 *
 * @async
 * @returns {Promise<boolean>} True if Ollama is online and model exists, false otherwise.
 */
export async function isModelAvailable() {
  try {
    const status = await getAgentStatus();
    return status.ollamaReachable && status.activeModel !== null;
  } catch {
    return false;
  }
}

/**
 * Backward compatibility alias for isModelAvailable.
 */
export const isGemmaAvailable = isModelAvailable;
