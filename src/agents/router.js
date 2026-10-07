/**
 * @fileoverview AI Router — determines fast path vs AI path and provides Ollama chat wrapper.
 * @module agents/router
 */

import { askModel, isModelAvailable } from './ollama.js';
import { logger } from '../utils/logger.js';

/**
 * Calls Ollama using configured model with messages format.
 * AI is optional — if Ollama is unreachable, returns null gracefully.
 * 
 * @param {Array<Object>} messages - The conversation messages array [{role, content}].
 * @param {Object} [options={}] - Additional options for the Ollama API.
 * @returns {Promise<string|null>} The text response from the model, or null if unreachable.
 */
export async function ollamaChat(messages, options = {}) {
  try {
    // Combine messages into a single prompt context for chat completion
    const systemMsg = messages.find(m => m.role === 'system')?.content || '';
    const userMsg = messages.filter(m => m.role !== 'system').map(m => `${m.role}: ${m.content}`).join('\n');
    const fullPrompt = systemMsg ? `${systemMsg}\n\n${userMsg}` : userMsg;

    return await askModel(fullPrompt, options);
  } catch (error) {
    logger.ai(`Ollama chat failed: ${error.message}`);
    return null;
  }
}

/**
 * Determines whether the given input should follow the fast path or AI path.
 * Fast path: structured JSON with { table, operation, data }
 * AI path: natural language string
 * 
 * @param {string|Object} input - The incoming task request.
 * @param {Object} [_db] - The database instance (optional, for context).
 * @returns {Object} The routing decision: { path: 'fast'|'ai'|'unknown', input, parsed }
 */
export function routeTask(input, _db) {
  // If the input is an object matching the structured transaction pattern
  if (typeof input === 'object' && input !== null && input.table && input.operation && input.data) {
    return {
      path: 'fast',
      input,
      parsed: input
    };
  }
  
  // If the input is a natural language string
  if (typeof input === 'string') {
    return {
      path: 'ai',
      input,
      parsed: null
    };
  }
  
  // Default fallback
  return {
    path: 'unknown',
    input,
    parsed: null
  };
}

/**
 * Checks if the model / Ollama service is available.
 * @returns {Promise<boolean>} True if available, false otherwise.
 */
export async function isOllamaAvailable() {
  return await isModelAvailable();
}
