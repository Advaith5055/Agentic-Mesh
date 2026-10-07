/**
 * @fileoverview Origin policy — which operations each write origin may perform.
 *
 * Every mutation carries an origin describing where it came from:
 *   api      — structured JSON from the CLI / REST fast path (default, also legacy peers)
 *   nl       — operations planned by the LLM from a natural-language request
 *   vision   — operations planned from a photo observation (untrusted visual input)
 *   observer — the observation records produced by the vision model itself
 *
 * The policy runs alongside the schema validator for local writes and for
 * writes replicated from peers, so a peer cannot bypass it either.
 * @module db/policy
 */

export const Origin = {
  API: 'api',
  NL: 'nl',
  VISION: 'vision',
  OBSERVER: 'observer'
};

const DATA_TABLES = [
  // Demo inventory tables
  'categories',
  'items',
  'suppliers',
  'item_suppliers',

  // Workflow platform tables
  'mesh_nodes',
  'node_capabilities',
  'tasks',
  'task_runs',
  'task_steps',
  'artifacts',
  'approvals',
  'audit_events',
  'conflicts',
  'replication_metrics'
];

/**
 * Tables an LLM planner may target. Observations are written only by the vision lane.
 */
export const PLANNER_TABLES = new Set(DATA_TABLES);

export const ORIGIN_POLICY = {
  [Origin.API]:      { operations: new Set(['INSERT', 'UPDATE', 'DELETE']), tables: new Set(DATA_TABLES) },
  [Origin.NL]:       { operations: new Set(['INSERT', 'UPDATE', 'DELETE']), tables: new Set(DATA_TABLES) },
  [Origin.VISION]:   { operations: new Set(['INSERT', 'UPDATE']), tables: new Set(DATA_TABLES), maxOps: 20, requiresEvidence: true },
  [Origin.OBSERVER]: { operations: new Set(['INSERT']), tables: new Set(['observations']), producerMustMatch: true }
};

/**
 * Checks a batch of mutations against the policy of their origin.
 *
 * @param {string} [origin='api'] - Origin of the batch.
 * @param {Array<Object>} mutations - Array of { operation, table|tableName, data }.
 * @param {Object} [ctx={}]
 * @param {string|null} [ctx.evidenceId] - Observation id the batch is based on (vision origin).
 * @param {string} [ctx.producerPeerId] - Peer that originated the batch.
 * @param {import('better-sqlite3').Database} [ctx.db] - When given, evidence must exist locally.
 * @returns {{ valid: boolean, errors: string[] }}
 */
export function checkOriginPolicy(origin, mutations, ctx = {}) {
  const effectiveOrigin = origin || Origin.API;
  const policy = ORIGIN_POLICY[effectiveOrigin];
  if (!policy) {
    return { valid: false, errors: [`Unknown write origin '${effectiveOrigin}'.`] };
  }
  if (!Array.isArray(mutations)) {
    return { valid: false, errors: ['Mutations must be an array.'] };
  }

  const errors = [];

  if (policy.maxOps && mutations.length > policy.maxOps) {
    errors.push(`Origin '${effectiveOrigin}' allows at most ${policy.maxOps} operations per batch (got ${mutations.length}).`);
  }

  if (policy.requiresEvidence) {
    if (typeof ctx.evidenceId !== 'string' || !ctx.evidenceId) {
      errors.push(`Origin '${effectiveOrigin}' requires an evidence observation id.`);
    } else if (ctx.db) {
      const exists = ctx.db.prepare('SELECT 1 FROM observations WHERE id = ?').get(ctx.evidenceId);
      if (!exists) errors.push(`Evidence observation '${ctx.evidenceId}' does not exist.`);
    }
  }

  mutations.forEach((m, i) => {
    const op = m?.operation;
    const table = m?.table || m?.tableName;
    if (!policy.operations.has(op)) {
      errors.push(`Operation ${i}: origin '${effectiveOrigin}' may not perform ${op}.`);
    }
    if (!policy.tables.has(table)) {
      errors.push(`Operation ${i}: origin '${effectiveOrigin}' may not write to table '${table}'.`);
    }
    if (policy.producerMustMatch && m?.data?.peer_id !== ctx.producerPeerId) {
      errors.push(`Operation ${i}: observation peer_id must match the producing peer.`);
    }
  });

  return { valid: errors.length === 0, errors };
}
