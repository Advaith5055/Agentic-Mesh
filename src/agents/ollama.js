/**
 * @fileoverview Ollama JS SDK Client Wrapper for Gemma 4 E2B.
 * @module agents/ollama
 */

import { Ollama } from 'ollama';
import { config } from '../utils/config.js';
import { logger } from '../utils/logger.js';

// Initialize the Ollama instance with configured host
const ollamaClient = new Ollama({ host: config.OLLAMA_HOST });

/**
 * Asks Gemma a question or passes a prompt using the official Ollama JS SDK.
 * 
 * @async
 * @param {string} prompt - The prompt text or instruction for Gemma.
 * @param {Object} [options={}] - Additional chat options (temperature, format, etc.).
 * @returns {Promise<string|null>} The generated text content or null if failed.
 */
export async function askGemma(prompt, options = {}) {
  try {
    const response = await ollamaClient.chat({
      model: config.OLLAMA_MODEL,
      messages: [
        {
          role: 'user',
          content: prompt
        }
      ],
      stream: false,
      ...options
    });

    return response.message?.content || null;
  } catch (error) {
    logger.ai(`Error communicating with Gemma via Ollama SDK: ${error.message}`);
    return null;
  }
}

/**
 * Checks if the configured Ollama host is responsive and model is available.
 * 
 * @async
 * @returns {Promise<boolean>} True if Ollama is online, false otherwise.
 */
export async function isGemmaAvailable() {
  try {
    const list = await ollamaClient.list();
    const models = list.models || [];
    return models.some(m => m.name.includes(config.OLLAMA_MODEL) || m.name.includes('gemma'));
  } catch (error) {
    return false;
  }
}
