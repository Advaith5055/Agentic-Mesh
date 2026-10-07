/**
 * @fileoverview Executor ↔ Planner conversation over the mesh.
 *
 * The executor node sends PLAN_REQUEST on 'mesh:control' to a planner peer, which plans
 * with its own local LLM and answers with PLAN_RESPONSE { requestId, success, operations }.
 * The executor's deterministic validator checks the plan; if it is rejected, the executor
 * sends the plan back with the exact validator errors and asks for a correction
 * (up to `maxRounds`). The returned plan still needs human approval before execution.
 *
 * Message format matches the planner nodes already running on the LAN:
 *   { id, type: 'PLAN_REQUEST', sender, timestamp, payload: { requestId, prompt, targetPeerId, ... } }
 *   { id, type: 'PLAN_RESPONSE', sender, timestamp, payload: { requestId, success, operations, error, plannerPeerId, raw } }
 * @module mesh/remote-planner
 */

import { randomUUID } from 'node:crypto';
import { encode } from '../utils/protocol.js';
import { validateModelPlan } from '../agents/model-validator.js';
import { validateMutationBatch } from '../db/mutation.js';
import { groundCategoryRefs } from './grounding.js';

export const CONTROL_TOPIC = 'mesh:control';

/** Appended to every request: how to reference rows created earlier in the same plan. */
export const PLAN_FORMAT_NOTE = `

(Plan format note from the executor node: if an operation needs the id of a row created by an EARLIER operation in this same plan, write the string "$ref:N" where N is that operation's index starting at 0, e.g. "category_id": "$ref:0". Return compact JSON on one line.)`;

/**
 * Prompt for planning one step of a decomposed task.
 * @param {string} task - The whole user request.
 * @param {string} step - This step.
 * @param {number} index - Step index (0-based).
 * @param {number} total - Number of steps.
 * @param {Array} planned - Operations already planned for earlier steps.
 * @returns {string}
 */
export function buildStepPrompt(task, step, index, total, planned) {
  const earlier = planned.length
    ? `Operations already planned for earlier steps (index: operation):\n${planned.map((op, i) => `${i}: ${JSON.stringify(op)}`).join('\n')}`
    : 'No operations planned yet.';
  return `Overall task: "${task}"
Plan ONLY step ${index + 1} of ${total}: ${step}
${earlier}
Return ONLY the operations for this step as a JSON array. To use the id of a row created by an earlier operation, write "$ref:N" where N is its index in the list above. Return compact JSON on one line.`;
}

/**
 * Builds the follow-up prompt that tells the planner why its plan was rejected.
 * @param {string} original - The user's request.
 * @param {Array} operations - The rejected plan.
 * @param {string[]} errors - Validator errors.
 * @param {number} round - Feedback round number (2, 3, …).
 * @returns {string}
 */
export function buildFeedbackPrompt(original, operations, errors, round) {
  return `${original}

EXECUTOR FEEDBACK (round ${round}): the executor node's validator rejected your previous plan ${JSON.stringify(operations)} because: ${errors.join('; ')}.
Fix only these problems (use existing ids instead of re-creating things that already exist) and return the corrected JSON array of operations.`;
}

/**
 * @param {Object} deps
 * @param {Object} deps.pubsub - libp2p pubsub service.
 * @param {string} deps.selfId - This node's peer ID.
 * @param {string} deps.selfName - This node's name.
 * @param {Function} deps.getPeers - () => peer registry entries ({ peerId, name, role, connectedAt }).
 * @param {number} [deps.timeoutMs=90000] - How long to wait for one PLAN_RESPONSE.
 * @param {string} [deps.preferredPeer] - Planner peer name to prefer (e.g. 'delta').
 */
export function createRemotePlanner({ pubsub, selfId, selfName, getPeers, timeoutMs = 90_000, preferredPeer = '' }) {
  const pending = new Map();

  /** Picks the planner peer: the preferred name, then role 'planner', then any named peer. */
  function pickPlanner() {
    const candidates = getPeers().filter(p => p.connectedAt && p.name);
    return candidates.find(p => preferredPeer && p.name === preferredPeer)
      || candidates.find(p => p.role === 'planner')
      || candidates.find(p => p.role !== 'executor')
      || null;
  }

  /** Routes PLAN_RESPONSE messages from mesh:control to the waiting request. */
  function handle(type, fromPeer, envelope) {
    if (type !== 'PLAN_RESPONSE') return false;
    const requestId = envelope?.payload?.requestId;
    const waiter = pending.get(requestId);
    if (!waiter || waiter.plannerPeerId !== fromPeer) return false;
    pending.delete(requestId);
    clearTimeout(waiter.timer);
    waiter.resolve({ ...envelope.payload, ms: Date.now() - waiter.startedAt });
    return true;
  }

  /** Sends one PLAN_REQUEST and waits for its PLAN_RESPONSE. */
  async function request(prompt, planner) {
    const requestId = randomUUID();
    const envelope = {
      id: randomUUID(),
      type: 'PLAN_REQUEST',
      sender: selfId,
      timestamp: Date.now(),
      payload: { requestId, prompt, targetPeerId: planner.peerId, fromPeerId: selfId, nodeName: selfName, nodeRole: 'executor' }
    };
    const response = new Promise((resolve) => {
      const timer = setTimeout(() => {
        pending.delete(requestId);
        resolve({ requestId, success: false, operations: [], error: `Planner "${planner.name}" did not answer within ${Math.round(timeoutMs / 1000)}s`, timedOut: true });
      }, timeoutMs);
      timer.unref?.();
      pending.set(requestId, { resolve, timer, plannerPeerId: planner.peerId, startedAt: Date.now() });
    });
    await pubsub.publish(CONTROL_TOPIC, encode(envelope));
    return response;
  }

  /**
   * One negotiation: request → validate (together with already-planned operations) →
   * feed errors back → repeat, up to maxRounds.
   */
  async function negotiate(promptText, planner, { db, maxRounds, onRound, onGround = () => {}, planned = [], step = null, groundText = promptText }) {
    let currentPrompt = promptText;
    let last = { success: false, operations: [], error: 'No plan', rounds: 0 };
    for (let round = 1; round <= maxRounds; round++) {
      const response = await request(currentPrompt + (step ? '' : PLAN_FORMAT_NOTE), planner);
      if (!response.success) {
        onRound({ round, planner, response, check: null, step });
        return { success: false, operations: [], error: response.error || 'Planner failed', rounds: round, timedOut: response.timedOut };
      }

      // Plans from another machine are untrusted: same allowlist + schema checks as local plans.
      const shape = validateModelPlan(response.operations);
      let operations = shape.operations;
      let fixes = [];
      if (shape.valid) {
        // Point items at the category the request actually names (planners often guess ids).
        const grounded = groundCategoryRefs(operations, { text: groundText, db, planned });
        fixes = grounded.fixes;
        operations = grounded.operations;
      }
      const check = shape.valid
        ? validateMutationBatch(db, [...planned, ...operations], { origin: 'nl' })
        : { valid: false, errors: shape.errors };
      onRound({ round, planner, response, check, step });
      if (fixes.length) onGround({ fixes, step });

      last = { success: check.valid, operations, error: check.valid ? undefined : check.errors.join('; '), rounds: round, raw: response.raw };
      if (check.valid) return last;
      currentPrompt = buildFeedbackPrompt(promptText, response.operations, check.errors, round + 1);
    }
    return { ...last, success: false };
  }

  /**
   * Asks the planner peer for a plan and negotiates corrections until the validator accepts it.
   * If the whole task cannot be planned in one go, the executor splits it into steps
   * (`decompose`) and plans them one by one with the planner.
   *
   * @param {string} prompt - User request.
   * @param {Object} options
   * @param {import('better-sqlite3').Database} options.db - Executor database (for validation).
   * @param {number} [options.maxRounds=3] - Planner rounds per negotiation, including corrections.
   * @param {Function} [options.onRound] - ({ round, planner, response, check, step }) => void.
   * @param {Function} [options.decompose] - async (prompt) => { steps, by }.
   * @param {Function} [options.onDecompose] - ({ steps, by, reason }) => void.
   * @returns {Promise<{ success: boolean, operations: Array, error?: string, planner?: Object, rounds: number, steps?: string[], raw?: string }>}
   */
  async function plan(prompt, { db, maxRounds = 3, onRound = () => {}, onGround = () => {}, decompose = null, onDecompose = () => {} } = {}) {
    const planner = pickPlanner();
    if (!planner) return { success: false, operations: [], error: 'No planner node connected', rounds: 0, noPlanner: true };

    const whole = await negotiate(prompt, planner, { db, maxRounds, onRound, onGround });
    if (whole.success || whole.timedOut || !decompose) return { ...whole, planner };

    const { steps, by } = await decompose(prompt);
    if (!Array.isArray(steps) || steps.length < 2) return { ...whole, planner };
    onDecompose({ steps, by, reason: whole.error });

    const planned = [];
    let rounds = whole.rounds;
    for (const [i, step] of steps.entries()) {
      const stepPrompt = buildStepPrompt(prompt, step, i, steps.length, planned);
      const result = await negotiate(stepPrompt, planner, { db, maxRounds, onRound, onGround, planned, step: { index: i, total: steps.length, text: step }, groundText: step });
      rounds += result.rounds;
      if (!result.success) {
        return { success: false, operations: planned, error: `Step ${i + 1} ("${step}"): ${result.error}`, planner, rounds, steps, timedOut: result.timedOut };
      }
      planned.push(...result.operations);
    }
    return { success: true, operations: planned, planner, rounds, steps };
  }

  return { pickPlanner, handle, request, plan, pendingCount: () => pending.size };
}
