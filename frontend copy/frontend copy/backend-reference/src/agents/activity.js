/**
 * @fileoverview Agent activity tracker — records every hand-off between agents
 * (Router, Planner, Validator, Executor, Vision, Mesh peers), the step-by-step
 * progress of each task, and per-agent performance, and streams it to the dashboard.
 * @module agents/activity
 */

import { v4 as uuidv4 } from 'uuid';
import os from 'node:os';

export const Agent = {
  USER: 'user',
  ROUTER: 'router',
  PLANNER: 'planner',
  VALIDATOR: 'validator',
  EXECUTOR: 'executor',
  VISION: 'vision',
  MESH: 'mesh',
  LLM: 'llm'
};

/** Agents whose performance is shown on the dashboard, in display order. */
export const TRACKED_AGENTS = [Agent.ROUTER, Agent.PLANNER, Agent.VALIDATOR, Agent.EXECUTOR, Agent.VISION, Agent.MESH];

const MAX_MESSAGES = 300;
const MAX_TASKS = 60;

export class ActivityTracker {
  /**
   * @param {Object} [options={}]
   * @param {Function} [options.emit] - (type, data) => void, pushes events to the dashboard.
   * @param {Function} [options.now] - Clock (injectable for tests).
   */
  constructor({ emit = () => {}, now = () => Date.now() } = {}) {
    this.emit = emit;
    this.now = now;
    this.messages = [];
    this.tasks = new Map();
    this.stats = Object.fromEntries(TRACKED_AGENTS.map(a => [a, emptyStats()]));
    this.startedAt = now();
  }

  /**
   * Starts tracking a task that moves through a fixed list of stages.
   * @param {Object} spec
   * @param {string} spec.kind - e.g. 'fast-path', 'nl-plan', 'chat', 'vision', 'replication'.
   * @param {string} spec.title - Human-readable description.
   * @param {Array<{ name: string, agent: string }>} spec.stages - Ordered stages.
   * @returns {TaskHandle}
   */
  startTask({ kind, title, stages }) {
    const task = {
      id: `task-${uuidv4().slice(0, 8)}`,
      kind,
      title: String(title || kind).slice(0, 160),
      status: 'running',
      startedAt: this.now(),
      finishedAt: null,
      progress: 0,
      stages: stages.map(s => ({ ...s, status: 'pending', startedAt: null, durationMs: null, detail: '' }))
    };
    this.tasks.set(task.id, task);
    while (this.tasks.size > MAX_TASKS) this.tasks.delete(this.tasks.keys().next().value);
    this.#emitTask(task);
    return new TaskHandle(this, task);
  }

  /**
   * Records a message passed from one agent to another.
   * @param {Object} msg - { from, to, summary, taskId?, status?, durationMs?, meta? }
   */
  message({ from, to, summary, taskId = null, status = 'ok', durationMs = null, meta = null }) {
    const entry = {
      id: `msg-${uuidv4().slice(0, 8)}`,
      at: this.now(),
      from,
      to,
      summary: String(summary || '').slice(0, 300),
      taskId,
      status,
      durationMs,
      meta
    };
    this.messages.push(entry);
    if (this.messages.length > MAX_MESSAGES) this.messages.shift();
    this.emit('agent:message', entry);
    return entry;
  }

  /**
   * Records one unit of work by an agent for its performance numbers.
   * @param {string} agent
   * @param {boolean} ok
   * @param {number} [durationMs]
   */
  record(agent, ok, durationMs = 0, { rejected = false } = {}) {
    const s = this.stats[agent];
    if (!s) return;
    s.calls += 1;
    if (ok) s.success += 1; else s.failed += 1;
    if (rejected) s.rejected += 1;
    s.totalMs += Math.max(0, durationMs || 0);
    s.lastAt = this.now();
    s.lastOk = ok;
  }

  /**
   * Per-agent performance: success rate %, average latency, call counts.
   * @returns {Array<Object>}
   */
  agentStats() {
    return TRACKED_AGENTS.map(agent => {
      const s = this.stats[agent];
      return {
        agent,
        calls: s.calls,
        success: s.success,
        failed: s.failed,
        rejected: s.rejected,
        successRate: s.calls ? Math.round((s.success / s.calls) * 1000) / 10 : null,
        avgMs: s.calls ? Math.round(s.totalMs / s.calls) : null,
        lastAt: s.lastAt,
        lastOk: s.lastOk
      };
    });
  }

  /**
   * Full snapshot for the dashboard's initial load.
   * @param {Object} [system] - Extra system metrics to include.
   */
  snapshot(system = null) {
    return {
      agents: this.agentStats(),
      tasks: [...this.tasks.values()].reverse(),
      messages: this.messages.slice(-150),
      system
    };
  }

  #emitTask(task) {
    this.emit('agent:task', structuredClone(task));
  }

  /** @private used by TaskHandle */
  _update(task) {
    const done = task.stages.filter(s => s.status === 'done' || s.status === 'skipped' || s.status === 'rejected').length;
    task.progress = Math.round((done / task.stages.length) * 100);
    this.#emitTask(task);
  }
}

/**
 * Handle for advancing one task through its stages.
 */
export class TaskHandle {
  constructor(tracker, task) {
    this.tracker = tracker;
    this.task = task;
    this.id = task.id;
  }

  /** Marks a stage as running. */
  begin(name, detail = '') {
    const stage = this.#stage(name);
    if (!stage) return this;
    stage.status = 'running';
    stage.startedAt = this.tracker.now();
    if (detail) stage.detail = String(detail).slice(0, 200);
    this.tracker._update(this.task);
    return this;
  }

  /**
   * Completes a stage and records the agent's performance.
   * @param {string} name - Stage name.
   * @param {boolean} ok - Whether the stage succeeded.
   * @param {string} [detail] - Short result description.
   * @param {number} [durationMs] - Override the measured duration.
   */
  end(name, ok, detail = '', durationMs) {
    const stage = this.#stage(name);
    if (!stage) return this;
    const now = this.tracker.now();
    stage.durationMs = durationMs ?? (stage.startedAt ? now - stage.startedAt : 0);
    stage.status = ok ? 'done' : 'failed';
    if (detail) stage.detail = String(detail).slice(0, 200);
    this.tracker.record(stage.agent, ok, stage.durationMs);
    this.tracker._update(this.task);
    return this;
  }

  /**
   * Marks a stage as having correctly refused the work (e.g. the Validator blocking
   * an invalid write). The agent did its job, so this counts as a success for it.
   */
  reject(name, detail = '', durationMs) {
    const stage = this.#stage(name);
    if (!stage) return this;
    const now = this.tracker.now();
    stage.durationMs = durationMs ?? (stage.startedAt ? now - stage.startedAt : 0);
    stage.status = 'rejected';
    if (detail) stage.detail = String(detail).slice(0, 200);
    this.tracker.record(stage.agent, true, stage.durationMs, { rejected: true });
    this.tracker._update(this.task);
    return this;
  }

  /** Marks a stage as not needed for this task (counts towards progress). */
  skip(name, detail = '') {
    const stage = this.#stage(name);
    if (!stage) return this;
    stage.status = 'skipped';
    if (detail) stage.detail = String(detail).slice(0, 200);
    this.tracker._update(this.task);
    return this;
  }

  /** Records an agent-to-agent message belonging to this task. */
  message(from, to, summary, extra = {}) {
    return this.tracker.message({ from, to, summary, taskId: this.id, ...extra });
  }

  /**
   * Finishes the task. Remaining pending stages are skipped on success.
   * @param {'done'|'failed'|'rejected'} status - 'rejected' = stopped by a safety check.
   */
  finish(status = 'done', detail = '') {
    if (status === 'done') {
      this.task.stages.filter(s => s.status === 'pending').forEach(s => { s.status = 'skipped'; });
    }
    this.task.status = status;
    this.task.finishedAt = this.tracker.now();
    if (detail) this.task.detail = String(detail).slice(0, 200);
    this.tracker._update(this.task);
    return this;
  }

  #stage(name) {
    return this.task.stages.find(s => s.name === name);
  }
}

/**
 * Samples process CPU % (of one core) and memory between calls.
 */
export function createSystemSampler() {
  let lastCpu = process.cpuUsage();
  let lastAt = process.hrtime.bigint();
  return () => {
    const cpu = process.cpuUsage(lastCpu);
    const at = process.hrtime.bigint();
    const elapsedUs = Number(at - lastAt) / 1000;
    lastCpu = process.cpuUsage();
    lastAt = at;
    const cpuPercent = elapsedUs > 0 ? Math.min(100, ((cpu.user + cpu.system) / elapsedUs) * 100) : 0;
    const load = os.loadavg()[0];
    return {
      cpuPercent: Math.round(cpuPercent * 10) / 10,
      systemLoadPercent: Math.min(100, Math.round((load / os.cpus().length) * 1000) / 10),
      memoryMb: Math.round(process.memoryUsage().rss / 1024 / 1024),
      memPercent: Math.round((1 - os.freemem() / os.totalmem()) * 1000) / 10,
      uptimeSec: Math.round(process.uptime())
    };
  };
}

function emptyStats() {
  return { calls: 0, success: 0, failed: 0, rejected: 0, totalMs: 0, lastAt: null, lastOk: null };
}
