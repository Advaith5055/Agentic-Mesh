/**
 * @fileoverview Validation and normalisation of vision model output.
 * Model output is untrusted: unknown keys are dropped, strings and arrays are capped,
 * and the result is turned into an `observations` row for the Executor.
 * @module vision/observation
 */

import { v4 as uuidv4 } from 'uuid';
import { CONFIDENCE_LEVELS } from './prompts.js';

const MAX_TEXT = 2000;
const MAX_ITEMS = 50;
const MAX_ITEM_TEXT = 300;

/**
 * Validates a parsed model reply and returns a sanitised copy.
 * @param {any} raw - Parsed JSON from the model.
 * @returns {{ valid: boolean, value: Object|null, errors: string[] }}
 */
export function validateVisionOutput(raw) {
  const errors = [];
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    return { valid: false, value: null, errors: ['Reply must be a JSON object.'] };
  }

  if (typeof raw.answer !== 'string' || !raw.answer.trim()) errors.push("'answer' must be a non-empty string");
  if (typeof raw.summary !== 'string' || !raw.summary.trim()) errors.push("'summary' must be a non-empty string");
  if (!Array.isArray(raw.objects)) errors.push("'objects' must be an array");
  if (!Array.isArray(raw.visible_text)) errors.push("'visible_text' must be an array");
  if (!CONFIDENCE_LEVELS.includes(raw.confidence)) errors.push(`'confidence' must be one of ${CONFIDENCE_LEVELS.join(', ')}`);

  if (errors.length > 0) return { valid: false, value: null, errors };

  const objects = raw.objects
    .filter(o => o && typeof o === 'object' && typeof o.name === 'string' && o.name.trim())
    .slice(0, MAX_ITEMS)
    .map(o => ({
      name: o.name.trim().slice(0, MAX_ITEM_TEXT),
      ...(Number.isInteger(o.count) && o.count >= 0 ? { count: o.count } : {}),
      ...(typeof o.details === 'string' && o.details.trim() ? { details: o.details.trim().slice(0, MAX_ITEM_TEXT) } : {})
    }));

  const visibleText = raw.visible_text
    .filter(t => typeof t === 'string' && t.trim())
    .slice(0, MAX_ITEMS)
    .map(t => t.trim().slice(0, MAX_ITEM_TEXT));

  return {
    valid: true,
    value: {
      answer: raw.answer.trim().slice(0, MAX_TEXT),
      summary: raw.summary.trim().slice(0, MAX_TEXT),
      objects,
      visible_text: visibleText,
      confidence: raw.confidence
    },
    errors: []
  };
}

/**
 * Returned when the model fails to produce a valid reply twice.
 * @param {string[]} errors
 * @returns {Object}
 */
export function uncertainOutput(errors = []) {
  return {
    answer: 'The vision model did not return a valid structured answer.',
    summary: `No reliable observation (${errors.join('; ').slice(0, 300) || 'invalid output'}).`,
    objects: [],
    visible_text: [],
    confidence: 'uncertain'
  };
}

/**
 * Builds the `observations` row stored through the Executor.
 * @param {Object} params
 * @param {Object} params.output - Sanitised vision output.
 * @param {string} params.peerId - Peer that produced the observation.
 * @param {Object} params.image - Result of loadImage().
 * @param {string} params.question - The question asked.
 * @param {string} params.model - Vision model name.
 * @returns {Object} Row data for INSERT into observations.
 */
export function toObservationRow({ output, peerId, image, question, model }) {
  return {
    id: `obs-${uuidv4()}`,
    peer_id: peerId,
    source: image.source,
    image_sha256: image.sha256,
    question: (question || '').slice(0, 1000),
    summary: output.summary,
    fields: JSON.stringify({ answer: output.answer, objects: output.objects }),
    visible_text: JSON.stringify(output.visible_text),
    confidence: output.confidence,
    model
  };
}
