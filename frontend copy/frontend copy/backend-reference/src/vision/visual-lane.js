/**
 * @fileoverview The visual lane — the third request path next to the fast path and the AI path.
 *
 *   look: image → vision model → observation → Executor stores it (origin 'observer') → answer
 *   see:  look → Planner proposes ops from the observation → Validator → pending proposal
 *         → human approves → Executor commits (origin 'vision') and broadcasts
 *
 * The vision model never writes data and the planner never executes anything.
 * @module vision/visual-lane
 */

import { loadImage } from './image.js';
import { analyzeImage } from './vlm.js';
import { toObservationRow } from './observation.js';
import { planOperationsFromObservation } from '../agents/planner.js';
import { executeOperations, proposeOperations } from '../agents/executor.js';
import { getObservations } from '../db/sqlite.js';
import { Origin } from '../db/policy.js';
import { config } from '../utils/config.js';
import { logger } from '../utils/logger.js';

/**
 * Creates the visual lane bound to one node's database, sync engine and proposal store.
 *
 * @param {Object} deps
 * @param {import('better-sqlite3').Database} deps.db
 * @param {import('../db/sync.js').SyncEngine} deps.syncEngine
 * @param {import('../agents/proposals.js').ProposalStore} deps.proposalStore
 * @param {Function} [deps.broadcastFn] - Broadcasts committed envelopes to peers.
 * @param {Function} [deps.emit] - (type, data) => void, for dashboard events.
 * @param {Function} [deps.analyze] - Vision call (injectable for tests).
 * @param {Function} [deps.plan] - Planner call (injectable for tests).
 * @returns {{ look: Function, see: Function, listObservations: Function }}
 */
export function createVisualLane({
  db,
  syncEngine,
  proposalStore,
  broadcastFn,
  emit = () => {},
  analyze = analyzeImage,
  plan = planOperationsFromObservation
}) {
  /**
   * Answers a question about an image and records the observation in the mesh.
   * @param {Object} request - { image: path|Buffer|base64, question, source? }
   */
  async function look({ image: input, question, source }) {
    if (!config.VISION_ENABLED) {
      return { success: false, error: 'Vision is disabled on this node (VISION_ENABLED=false).' };
    }

    let image;
    try {
      image = loadImage(input, { source });
    } catch (err) {
      return { success: false, error: err.message };
    }

    let analysis;
    try {
      analysis = await analyze({ image, question });
    } catch (err) {
      logger.ai(`Vision model call failed: ${err.message}`);
      return { success: false, error: `Vision model unavailable: ${err.message}` };
    }

    const row = toObservationRow({ output: analysis.output, peerId: syncEngine.peerId, image, question, model: analysis.model });
    const stored = await executeOperations(
      [{ operation: 'INSERT', table: 'observations', data: row }],
      db, syncEngine, broadcastFn, { origin: Origin.OBSERVER }
    );
    const storedOk = stored.completed === 1;
    if (!storedOk) logger.warn(`Observation not stored: ${stored.errors?.join(', ')}`);

    const observation = {
      ...row,
      fields: { answer: analysis.output.answer, objects: analysis.output.objects },
      visible_text: analysis.output.visible_text
    };
    emit('vision:observation', observation);

    return {
      success: true,
      answer: analysis.output.answer,
      observation,
      stored: storedOk,
      storeErrors: storedOk ? [] : stored.errors,
      metrics: { model: analysis.model, inferMs: analysis.inferMs, attempts: analysis.attempts, imageBytes: image.bytes, validOutput: analysis.valid }
    };
  }

  /**
   * Turns a photo plus a request into a pending proposal that a human must approve.
   * @param {Object} request - { image: path|Buffer|base64, request, source? }
   */
  async function see({ image, request, source }) {
    const looked = await look({ image, question: request, source });
    if (!looked.success) return looked;

    const base = { observation: looked.observation, answer: looked.answer, metrics: looked.metrics };
    if (!looked.stored) {
      return { ...base, success: false, error: 'The observation could not be stored, so it cannot serve as evidence.' };
    }
    if (looked.observation.confidence === 'uncertain') {
      return { ...base, success: false, error: 'The vision model was uncertain about this photo; no changes were proposed.' };
    }

    const planned = await plan(request, looked.observation, db);
    if (!planned.success) {
      return { ...base, success: false, error: planned.error || 'The planner could not produce a valid plan.', raw: planned.raw };
    }
    if (planned.operations.length === 0) {
      return { ...base, success: true, proposal: null, message: planned.message || 'Nothing in the photo requires a database change.' };
    }

    const proposed = proposeOperations(planned.operations, db, proposalStore, {
      origin: Origin.VISION,
      evidenceId: looked.observation.id,
      producerPeerId: syncEngine.peerId,
      summary: planned.message || '',
      request
    });
    if (!proposed.success) {
      return { ...base, success: false, error: 'The validator rejected the proposed plan.', errors: proposed.errors, operations: planned.operations };
    }

    emit('proposal:created', proposed.proposal);
    return { ...base, success: true, proposal: proposed.proposal, message: planned.message };
  }

  return {
    look,
    see,
    listObservations: (limit = 20) => getObservations(db, limit)
  };
}
