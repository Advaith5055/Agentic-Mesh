/**
 * @fileoverview Wraps the node's agent entry points so every hand-off is recorded
 * by the ActivityTracker (Router → Planner → Validator → Executor → Mesh).
 * The wrapped functions keep their original signatures and return values.
 * @module agents/instrumented
 */

import { Agent } from './activity.js';
import { validateMutationBatch } from '../db/mutation.js';
import { config } from '../utils/config.js';

const describeOps = (ops = []) => ops.map(o => `${o.operation} ${o.table || o.tableName}`).join(', ');

/**
 * @param {import('./activity.js').ActivityTracker} activity
 * @param {Object} ctx - { db, peerCount: () => number }
 */
export function createInstrumentation(activity, { db, peerCount = () => 0 }) {
  const peersLabel = () => `${peerCount()} peer${peerCount() === 1 ? '' : 's'}`;

  /**
   * Validator → Executor → Mesh for an operation batch. Shared by every write path.
   * Runs the validator once on its own (sub-millisecond) so its verdict and timing
   * are visible separately; the executor validates again inside its transaction.
   */
  async function runWrite(task, operations, origin, execute) {
    task.begin('Validate', `${operations.length} op(s): ${describeOps(operations)}`);
    const t0 = performance.now();
    const check = validateMutationBatch(db, operations, { origin });
    if (!check.valid) {
      task.reject('Validate', check.errors.join('; '), performance.now() - t0);
      task.message(Agent.VALIDATOR, Agent.USER, `Rejected: ${check.errors[0]}`, { status: 'warn' });
      task.finish('rejected', check.errors[0]);
      return { valid: false, errors: check.errors };
    }
    task.end('Validate', true, 'all checks passed', performance.now() - t0);
    task.message(Agent.VALIDATOR, Agent.EXECUTOR, `Approved ${operations.length} op(s) for commit`);

    task.begin('Commit');
    const t1 = performance.now();
    const result = await execute();
    const ok = isSuccess(result);
    task.end('Commit', ok, ok ? `committed to SQLite (${origin})` : (result?.errors || []).join('; '), performance.now() - t1);
    if (!ok) {
      task.message(Agent.EXECUTOR, Agent.USER, `Commit failed: ${(result?.errors || [])[0] || 'unknown error'}`, { status: 'error' });
      task.finish('failed', (result?.errors || [])[0]);
      return { valid: true, result };
    }

    task.begin('Broadcast');
    task.message(Agent.EXECUTOR, Agent.MESH, `Gossip TRANSACTION to ${peersLabel()}`);
    task.end('Broadcast', true, peerCount() > 0 ? `sent to ${peersLabel()}` : 'no peers online — will sync on reconnect', 0);
    task.finish('done');
    return { valid: true, result };
  }

  /** Fast path: structured JSON → Validator → Executor → Mesh. */
  const fastPath = (execute) => async (payload) => {
    const op = { operation: payload.operation, table: payload.table || payload.tableName, data: payload.data };
    const task = activity.startTask({
      kind: 'fast-path',
      title: `${op.operation} ${op.table}`,
      stages: [
        { name: 'Route', agent: Agent.ROUTER },
        { name: 'Validate', agent: Agent.VALIDATOR },
        { name: 'Commit', agent: Agent.EXECUTOR },
        { name: 'Broadcast', agent: Agent.MESH }
      ]
    });
    task.message(Agent.USER, Agent.ROUTER, `Structured ${op.operation} on ${op.table}`);
    task.begin('Route').end('Route', true, 'structured JSON → fast path', 0);
    task.message(Agent.ROUTER, Agent.VALIDATOR, 'Fast path: no LLM needed');
    const out = await runWrite(task, [op], 'api', () => execute(payload));
    return out.valid ? out.result : { success: false, errors: out.errors };
  };

  /** A batch from the LLM planner or chat approval: Validator → Executor → Mesh. */
  const executeBatch = (execute, { origin = 'nl', title = 'Execute approved plan' } = {}) => async (operations) => {
    const task = activity.startTask({
      kind: 'execute',
      title: `${title}: ${describeOps(operations)}`,
      stages: [
        { name: 'Validate', agent: Agent.VALIDATOR },
        { name: 'Commit', agent: Agent.EXECUTOR },
        { name: 'Broadcast', agent: Agent.MESH }
      ]
    });
    task.message(Agent.USER, Agent.VALIDATOR, `Approved plan with ${operations?.length || 0} op(s)`);
    const out = await runWrite(task, operations || [], origin, () => execute(operations));
    return out.valid ? out.result : { completed: 0, failed: operations?.length || 0, results: [], errors: out.errors };
  };

  /** Natural language → Planner (LLM) → Validator preview. */
  const planner = (plan) => async (request) => {
    const task = activity.startTask({
      kind: 'nl-plan',
      title: request,
      stages: [
        { name: 'Route', agent: Agent.ROUTER },
        { name: 'Plan', agent: Agent.PLANNER },
        { name: 'Validate', agent: Agent.VALIDATOR }
      ]
    });
    task.message(Agent.USER, Agent.ROUTER, request);
    task.begin('Route').end('Route', true, 'natural language → AI path', 0);
    task.message(Agent.ROUTER, Agent.PLANNER, 'Decompose request into tool operations');
    task.begin('Plan', `${config.OLLAMA_MODEL} @ ${config.OLLAMA_HOST}`);
    task.message(Agent.PLANNER, Agent.LLM, `Prompt → ${config.OLLAMA_MODEL}`);
    const t0 = performance.now();
    const result = await plan(request);
    const ms = performance.now() - t0;
    task.message(Agent.LLM, Agent.PLANNER, result.success ? `${result.operations.length} op(s) in ${(ms / 1000).toFixed(1)}s` : `Failed: ${result.error}`, { status: result.success ? 'ok' : 'error', durationMs: Math.round(ms) });
    task.end('Plan', result.success, result.success ? describeOps(result.operations) : result.error, ms);
    if (!result.success) {
      task.finish('failed', result.error);
      return result;
    }

    task.message(Agent.PLANNER, Agent.VALIDATOR, `Check ${result.operations.length} op(s)`);
    task.begin('Validate');
    const t1 = performance.now();
    const check = validateMutationBatch(db, result.operations, { origin: 'nl' });
    if (check.valid) task.end('Validate', true, 'plan is valid — awaiting approval', performance.now() - t1);
    else task.reject('Validate', check.errors.join('; '), performance.now() - t1);
    task.message(Agent.VALIDATOR, Agent.USER, check.valid ? 'Plan valid — awaiting approval' : `Plan rejected: ${check.errors[0]}`, { status: check.valid ? 'ok' : 'warn' });
    task.finish(check.valid ? 'done' : 'rejected', check.valid ? 'awaiting approval' : check.errors[0]);
    return result;
  };

  /**
   * Executor ↔ remote Planner over the mesh, with validator feedback rounds.
   * Falls back to the local planner when no planner node answers.
   * @param {Object} remote - createRemotePlanner() instance.
   * @param {Function} localPlan - Already-instrumented local planner (request) => result.
   * @param {Object} [options] - { maxRounds, decompose }
   */
  const meshPlanner = (remote, localPlan, { maxRounds = 3, decompose = null } = {}) => async (request) => {
    const planner = remote.pickPlanner();
    if (!planner) return { ...(await localPlan(request)), plannedBy: 'local' };

    const who = `${planner.name} (planner node)`;
    const task = activity.startTask({
      kind: 'mesh-plan',
      title: request,
      stages: [
        { name: 'Route', agent: Agent.ROUTER },
        { name: 'Request', agent: Agent.MESH },
        { name: 'Plan', agent: Agent.PLANNER },
        { name: 'Validate', agent: Agent.VALIDATOR }
      ]
    });
    task.message(Agent.USER, Agent.ROUTER, request);
    task.begin('Route').end('Route', true, `database change → planner node ${planner.name}`, 0);
    task.begin('Request', `PLAN_REQUEST → ${who}`);
    task.begin('Plan', `${planner.name}'s local LLM`);
    const t0 = performance.now();

    const result = await remote.plan(request, {
      db,
      maxRounds,
      decompose,
      onGround: ({ fixes }) => {
        for (const f of fixes) {
          task.message(Agent.EXECUTOR, Agent.PLANNER, `Corrected "${f.item}": category_id ${JSON.stringify(f.from)} → ${JSON.stringify(f.to)} (the request names category "${f.category}")`, { status: 'warn' });
        }
      },
      onDecompose: ({ steps, by, reason }) => {
        task.message(Agent.EXECUTOR, Agent.PLANNER, `Whole-task plan failed (${String(reason).slice(0, 80)}). Splitting into ${steps.length} steps (${by === 'llm' ? "executor's LLM" : 'rules'}): ${steps.map((s, i) => `${i + 1}. ${s}`).join(' | ')}`, { status: 'warn' });
      },
      onRound: ({ round, response, check, step }) => {
        const stepLabel = step ? ` step ${step.index + 1}/${step.total} "${step.text}"` : '';
        task.message(Agent.EXECUTOR, Agent.PLANNER, `PLAN_REQUEST${stepLabel}${round > 1 ? ` (correction round ${round})` : ''} → ${who}`);
        if (!response.success) {
          task.message(Agent.PLANNER, Agent.EXECUTOR, `${planner.name}: ${response.error}`, { status: 'error', durationMs: response.ms });
          return;
        }
        task.message(Agent.PLANNER, Agent.EXECUTOR, `${planner.name} PLAN_RESPONSE: ${describeOps(response.operations)}`, { durationMs: response.ms });
        if (check?.valid) {
          task.message(Agent.VALIDATOR, Agent.EXECUTOR, `Plan from ${planner.name} accepted (round ${round})`);
        } else if (check) {
          task.message(Agent.VALIDATOR, Agent.PLANNER, `Rejected: ${check.errors[0]} — asking ${planner.name} to fix it`, { status: 'warn' });
        }
      }
    });
    const ms = performance.now() - t0;

    if (!result.success && (result.timedOut || result.rounds === 0 || !result.operations?.length)) {
      task.end('Request', !result.timedOut, result.timedOut ? 'no answer' : 'sent', ms);
      task.end('Plan', false, result.error, ms);
      task.skip('Validate', 'no plan');
      task.message(Agent.MESH, Agent.ROUTER, `${result.error} — falling back to this node's local planner`, { status: 'warn' });
      task.finish('failed', result.error);
      return { ...(await localPlan(request)), plannedBy: 'local (fallback)' };
    }

    const stepsNote = result.steps ? `, ${result.steps.length} steps` : '';
    task.end('Request', true, `${result.rounds} round(s) with ${planner.name}${stepsNote}`, 0);
    task.end('Plan', true, `${describeOps(result.operations)} — ${result.rounds} round(s)${stepsNote}`, ms);
    task.begin('Validate');
    if (result.success) {
      task.end('Validate', true, 'plan valid — awaiting approval', 0);
      task.message(Agent.VALIDATOR, Agent.USER, `Plan from ${planner.name} is valid — awaiting approval`);
      task.finish('done', 'awaiting approval');
    } else {
      task.reject('Validate', result.error, 0);
      task.message(Agent.VALIDATOR, Agent.USER, `Still invalid after ${result.rounds} rounds: ${result.error}`, { status: 'warn' });
      task.finish('rejected', result.error);
    }
    return {
      success: result.success,
      operations: result.success ? result.operations : [],
      error: result.success ? undefined : `Plan from ${planner.name} rejected after ${result.rounds} round(s): ${result.error}`,
      raw: result.raw,
      plannedBy: planner.name,
      rounds: result.rounds,
      steps: result.steps,
      planMs: Math.round(ms)
    };
  };

  /** Streaming chat: Router → Agent (instant or LLM) → Validator preview of any plan. */
  const chatStream = (stream) => async function* (message, history, ...rest) {
    const task = activity.startTask({
      kind: 'chat',
      title: message,
      stages: [
        { name: 'Route', agent: Agent.ROUTER },
        { name: 'Respond', agent: Agent.PLANNER },
        { name: 'Validate', agent: Agent.VALIDATOR }
      ]
    });
    task.message(Agent.USER, Agent.ROUTER, message);
    task.begin('Route').end('Route', true, 'chat request', 0);
    task.message(Agent.ROUTER, Agent.PLANNER, 'Answer or plan');
    task.begin('Respond');
    const t0 = performance.now();
    let firstTokenMs = null;
    let finished = false;
    try {
      for await (const chunk of stream(message, history, ...rest)) {
        if (chunk.type === 'token' && firstTokenMs === null) firstTokenMs = performance.now() - t0;
        if (chunk.type === 'done' || chunk.type === 'error') {
          finished = true;
          finishChat(task, chunk, performance.now() - t0, firstTokenMs);
        }
        yield chunk;
      }
    } finally {
      if (!finished) {
        task.end('Respond', false, 'stream ended without an answer', performance.now() - t0);
        task.finish('failed', 'no answer');
      }
    }
  };

  function finishChat(task, chunk, ms, firstTokenMs) {
    const ok = chunk.type === 'done';
    const timing = firstTokenMs !== null ? `first token ${(firstTokenMs / 1000).toFixed(1)}s, ` : '';
    task.end('Respond', ok, ok ? `${timing}${ms < 50 ? 'instant answer' : `answered in ${(ms / 1000).toFixed(1)}s`}` : chunk.message, ms);
    task.message(Agent.PLANNER, Agent.USER, ok ? (chunk.message || '').slice(0, 120) : chunk.message, { status: ok ? 'ok' : 'error', durationMs: Math.round(ms) });
    if (!ok) {
      task.finish('failed', chunk.message);
      return;
    }
    if (chunk.operations?.length) {
      task.message(Agent.PLANNER, Agent.VALIDATOR, `Check ${chunk.operations.length} proposed op(s)`);
      task.begin('Validate');
      const t1 = performance.now();
      const check = validateMutationBatch(db, chunk.operations, { origin: 'nl' });
      if (check.valid) task.end('Validate', true, 'plan valid — awaiting approval', performance.now() - t1);
      else task.reject('Validate', check.errors.join('; '), performance.now() - t1);
      task.finish(check.valid ? 'done' : 'rejected', check.valid ? 'awaiting approval' : check.errors[0]);
    } else {
      task.skip('Validate', 'no database changes');
      task.finish('done');
    }
  }

  /** Non-streaming chat. */
  const chat = (fn) => async (message, history, ...rest) => {
    const gen = chatStream(async function* () {
      const result = await fn(message, history, ...rest);
      yield result.modelAvailable === false && !result.operations ? { type: 'error', ...result } : { type: 'done', ...result };
    });
    let last;
    for await (const chunk of gen(message, history)) last = chunk;
    const { type: _type, ...result } = last;
    return result;
  };

  /** Vision lane: Vision model → store observation → (Planner → Validator → proposal). */
  const vision = (lane) => {
    const run = async (mode, request) => {
      const isSee = mode === 'see';
      const question = isSee ? request.request : request.question;
      const stages = [
        { name: 'See', agent: Agent.VISION },
        { name: 'Store', agent: Agent.EXECUTOR }
      ];
      if (isSee) stages.push({ name: 'Plan', agent: Agent.PLANNER }, { name: 'Validate', agent: Agent.VALIDATOR });
      const task = activity.startTask({ kind: 'vision', title: `${mode}: ${question || 'describe photo'}`, stages });

      task.message(Agent.USER, Agent.VISION, `Photo + "${question || ''}"`);
      task.begin('See', config.VISION_MODEL);
      const t0 = performance.now();
      const result = await lane[mode](request);
      const total = performance.now() - t0;
      const inferMs = result.metrics?.inferMs ?? total;

      if (!result.observation) {
        task.end('See', false, result.error, inferMs);
        task.message(Agent.VISION, Agent.USER, result.error, { status: 'error' });
        task.finish('failed', result.error);
        return result;
      }
      task.end('See', true, `${result.observation.confidence} confidence, ${result.metrics?.attempts || 1} attempt(s)`, inferMs);
      task.message(Agent.VISION, Agent.EXECUTOR, `Observation ${result.observation.id.slice(0, 12)}… (${result.observation.confidence})`);
      task.begin('Store').end('Store', result.stored !== false, result.stored === false ? 'observation not stored' : 'observation stored & gossiped', 0);
      if (result.stored !== false) task.message(Agent.EXECUTOR, Agent.MESH, 'Gossip observation to peers');

      if (!isSee) {
        task.message(Agent.VISION, Agent.USER, result.answer || '');
        task.finish(result.success ? 'done' : 'failed', result.error);
        return result;
      }

      if (/uncertain|evidence/i.test(result.error || '')) {
        task.skip('Plan', result.error).skip('Validate');
        task.finish('failed', result.error);
        return result;
      }
      task.message(Agent.VISION, Agent.PLANNER, 'Observation as quoted evidence');
      const planOk = !(/planner/i.test(result.error || ''));
      task.begin('Plan').end('Plan', planOk, planOk ? (result.proposal ? describeOps(result.proposal.operations) : (result.message || 'nothing to change')) : result.error, total - inferMs);
      if (!planOk) {
        task.skip('Validate');
        task.finish('failed', result.error);
        return result;
      }
      if (!result.proposal && result.success) {
        task.skip('Validate', 'no changes proposed');
        task.finish('done');
        return result;
      }
      task.message(Agent.PLANNER, Agent.VALIDATOR, 'Check vision plan (no DELETE, evidence required)');
      task.begin('Validate');
      if (result.success) task.end('Validate', true, `proposal ${result.proposal.id} awaiting approval`, 0);
      else task.reject('Validate', (result.errors || [result.error]).join('; '), 0);
      task.message(Agent.VALIDATOR, Agent.USER, result.success ? `Proposal ${result.proposal.id} awaiting approval` : 'Plan rejected', { status: result.success ? 'ok' : 'warn' });
      task.finish(result.success ? 'done' : 'rejected', result.success ? 'awaiting approval' : result.error);
      return result;
    };
    return { ...lane, look: (req) => run('look', req), see: (req) => run('see', req) };
  };

  /** Approving a pending proposal: re-validate → commit → broadcast. */
  const approveProposal = (approve) => async (id) => {
    const task = activity.startTask({
      kind: 'approval',
      title: `Approve ${id}`,
      stages: [
        { name: 'Validate', agent: Agent.VALIDATOR },
        { name: 'Commit', agent: Agent.EXECUTOR },
        { name: 'Broadcast', agent: Agent.MESH }
      ]
    });
    task.message(Agent.USER, Agent.EXECUTOR, `Approved proposal ${id}`);
    task.begin('Validate');
    const t0 = performance.now();
    const result = await approve(id);
    const ms = performance.now() - t0;
    if (!result.success) {
      const notFound = /not found|not pending/i.test((result.errors || [])[0] || '');
      if (notFound) task.end('Validate', false, (result.errors || [])[0], ms);
      else task.reject('Validate', (result.errors || [])[0], ms);
      task.finish(notFound ? 'failed' : 'rejected', (result.errors || [])[0]);
      return result;
    }
    task.end('Validate', true, 're-validated at commit time', 0);
    task.begin('Commit').end('Commit', true, `${result.result?.completed} op(s) committed (vision)`, ms);
    task.message(Agent.EXECUTOR, Agent.MESH, `Gossip ${result.result?.completed} TRANSACTION(s) to ${peersLabel()}`);
    task.begin('Broadcast').end('Broadcast', true, `sent to ${peersLabel()}`, 0);
    task.finish('done');
    return result;
  };

  /** A write replicated from a peer: Mesh → Validator → Executor (apply). */
  function remoteTransaction(fromPeer, payload, result) {
    const from = `peer ${fromPeer.slice(0, 8)}…`;
    const task = activity.startTask({
      kind: 'replication',
      title: `${payload?.operation} ${payload?.tableName} from ${from}`,
      stages: [
        { name: 'Receive', agent: Agent.MESH },
        { name: 'Validate', agent: Agent.VALIDATOR },
        { name: 'Apply', agent: Agent.EXECUTOR }
      ]
    });
    task.message(Agent.MESH, Agent.VALIDATOR, `${from}: ${payload?.operation} ${payload?.tableName} (origin ${payload?.origin || 'api'})`);
    task.begin('Receive').end('Receive', true, 'signed gossip message', 0);
    if (result.duplicate) {
      task.skip('Validate', 'already applied').skip('Apply', 'duplicate');
      task.finish('done', 'duplicate skipped');
      return;
    }
    if (result.conflict) {
      task.begin('Validate').reject('Validate', (result.errors || []).join('; '), 0);
      task.message(Agent.VALIDATOR, Agent.MESH, `Rejected replicated write: ${(result.errors || [])[0]}`, { status: 'warn' });
      task.finish('rejected', (result.errors || [])[0]);
      return;
    }
    task.begin('Validate').end('Validate', true, 'schema + origin policy passed', 0);
    task.message(Agent.VALIDATOR, Agent.EXECUTOR, 'Apply replicated write');
    task.begin('Apply').end('Apply', !!result.applied, result.applied ? 'applied to local SQLite' : result.error, 0);
    task.finish(result.applied ? 'done' : 'failed', result.error);
  }

  /** Catch-up sync summary from a peer. */
  function syncResponse(fromPeer, { applied, duplicates, conflicts, total }) {
    activity.record(Agent.MESH, true, 0);
    activity.message({
      from: Agent.MESH,
      to: Agent.EXECUTOR,
      summary: `Sync from peer ${fromPeer.slice(0, 8)}…: ${total} entries — ${applied} applied, ${duplicates} duplicates, ${conflicts} conflicts`,
      status: conflicts > 0 ? 'warn' : 'ok'
    });
  }

  function syncRequest(fromPeer, sent) {
    activity.message({ from: Agent.MESH, to: Agent.MESH, summary: `Peer ${fromPeer.slice(0, 8)}… asked to sync — sending ${sent} entries` });
  }

  return { fastPath, executeBatch, planner, meshPlanner, chat, chatStream, vision, approveProposal, remoteTransaction, syncResponse, syncRequest };
}

function isSuccess(result) {
  if (!result) return false;
  if (typeof result.success === 'boolean') return result.success;
  return (result.failed || 0) === 0 && (result.completed || 0) > 0;
}
