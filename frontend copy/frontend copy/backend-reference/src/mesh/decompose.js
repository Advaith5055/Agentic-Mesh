/**
 * @fileoverview Splits a big request into small ordered steps so a planner node with a
 * small model / short output limit can plan one step at a time.
 * The executor's own local LLM does the split; a plain-text splitter is the fallback.
 * @module mesh/decompose
 */

import { askModel } from '../agents/ollama.js';

const ACTION = '(?:add|create|insert|put|update|set|change|modify|rename|delete|remove|link|associate|unlink)';
const SPLIT_RE = new RegExp(`\\s*(?:;|\\.\\s+|,?\\s+(?:and\\s+)?then\\s+|,?\\s+and\\s+(?=${ACTION}\\b)|,\\s*(?=${ACTION}\\b))`, 'i');

/**
 * Rule-based split on "then", ";", sentence breaks, and "and <action verb>".
 * @param {string} request
 * @returns {string[]}
 */
export function splitHeuristically(request) {
  return String(request || '')
    .split(SPLIT_RE)
    .map(s => s.trim().replace(/[.,;]+$/, ''))
    .filter(s => s.length > 2);
}

/**
 * Asks the local LLM for an ordered list of single-purpose steps.
 *
 * @param {string} request - The user's request.
 * @param {Object} [options]
 * @param {Function} [options.askFn] - Model call (injectable for tests).
 * @param {number} [options.maxSteps=6]
 * @returns {Promise<{ steps: string[], by: 'llm'|'rules' }>}
 */
export async function decomposeTask(request, { askFn = askModel, maxSteps = 6 } = {}) {
  const prompt = `Split this database request into a short ordered list of simple steps.
Each step must do ONE kind of change (create one category, add items, update one thing...).
Keep names, prices and numbers exactly as written. Do not invent extra steps.
Return ONLY a JSON array of strings, for example ["Create category Garden", "Add a Rake for 12.00 to the Garden category"].

REQUEST: ${JSON.stringify(String(request).slice(0, 1000))}`;

  try {
    const raw = await askFn(prompt, { options: { temperature: 0, num_predict: 256 } });
    const match = String(raw || '').match(/\[[\s\S]*\]/);
    const steps = match ? JSON.parse(match[0]) : null;
    if (Array.isArray(steps) && steps.length >= 2 && steps.length <= maxSteps && steps.every(s => typeof s === 'string' && s.trim())) {
      return { steps: steps.map(s => s.trim()), by: 'llm' };
    }
  } catch {
    // fall through to the rule-based split
  }
  return { steps: splitHeuristically(request).slice(0, maxSteps), by: 'rules' };
}
