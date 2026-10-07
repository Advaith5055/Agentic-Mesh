/**
 * @fileoverview Calls the vision model with the fixed prompt contract.
 * One repair retry on invalid output, then an explicit 'uncertain' result.
 * @module vision/vlm
 */

import { visionChat } from '../agents/ollama.js';
import { config } from '../utils/config.js';
import { logger } from '../utils/logger.js';
import { VISION_SYSTEM_PROMPT, OBSERVATION_JSON_SCHEMA, buildVisionUserPrompt, buildRepairPrompt } from './prompts.js';
import { validateVisionOutput, uncertainOutput } from './observation.js';

/**
 * Asks the vision model a question about an image.
 *
 * @async
 * @param {Object} request
 * @param {Object} request.image - Result of loadImage().
 * @param {string} request.question - What to ask about the image.
 * @param {Object} [options={}]
 * @param {Function} [options.chatFn] - Chat function (injectable for tests).
 * @param {string} [options.model] - Vision model name.
 * @returns {Promise<{ output: Object, valid: boolean, attempts: number, model: string, inferMs: number, errors: string[] }>}
 * @throws {Error} If the model service is unreachable or times out.
 */
export async function analyzeImage({ image, question }, { chatFn = visionChat, model = config.VISION_MODEL } = {}) {
  const started = Date.now();
  const messages = [
    { role: 'system', content: VISION_SYSTEM_PROMPT },
    { role: 'user', content: buildVisionUserPrompt(question), images: [image.base64] }
  ];

  let errors = [];
  for (let attempt = 1; attempt <= 2; attempt++) {
    const raw = await chatFn({ model, messages, format: OBSERVATION_JSON_SCHEMA });
    const parsed = parseJson(raw);
    const check = parsed.ok ? validateVisionOutput(parsed.value) : { valid: false, errors: [parsed.error] };

    if (check.valid) {
      return { output: check.value, valid: true, attempts: attempt, model, inferMs: Date.now() - started, errors: [] };
    }

    errors = check.errors;
    logger.ai(`Vision reply attempt ${attempt} invalid: ${errors.join('; ')}`);
    messages.push({ role: 'assistant', content: String(raw ?? '').slice(0, 2000) });
    messages.push({ role: 'user', content: buildRepairPrompt(errors) });
  }

  return { output: uncertainOutput(errors), valid: false, attempts: 2, model, inferMs: Date.now() - started, errors };
}

function parseJson(raw) {
  if (typeof raw !== 'string' || !raw.trim()) return { ok: false, error: 'Empty reply.' };
  try {
    return { ok: true, value: JSON.parse(raw) };
  } catch {
    const match = raw.match(/\{[\s\S]*\}/);
    if (match) {
      try { return { ok: true, value: JSON.parse(match[0]) }; } catch { /* fall through */ }
    }
    return { ok: false, error: 'Reply is not valid JSON.' };
  }
}
