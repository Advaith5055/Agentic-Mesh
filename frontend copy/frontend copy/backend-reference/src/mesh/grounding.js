/**
 * @fileoverview Executor-side grounding of planner output.
 *
 * Small planner models often map a category name to the wrong id — especially a category
 * created earlier in the same plan, whose id they cannot know. The validator cannot catch
 * this when the wrong id happens to exist. When the request text names exactly one
 * category, the executor points the item at that category: "$ref:N" for a category created
 * by operation N of this plan, or the existing id for a category already in the database.
 * @module mesh/grounding
 */

const escapeRe = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

/**
 * Categories named in the text, from this plan's INSERTs and the database.
 * @returns {Array<{ name: string, ref: string|number }>}
 */
function namedCategories(text, plannedAndCurrent, db) {
  const lower = String(text || '').toLowerCase();
  const mentioned = (name) => name && new RegExp(`\\b${escapeRe(name.toLowerCase())}\\b`).test(lower);

  const fromPlan = plannedAndCurrent
    .map((op, index) => ({ op, index }))
    .filter(({ op }) => op.operation === 'INSERT' && op.table === 'categories' && mentioned(op.data?.name))
    .map(({ op, index }) => ({ name: op.data.name, ref: `$ref:${index}` }));

  const planNames = new Set(fromPlan.map(c => c.name.toLowerCase()));
  const fromDb = db.prepare('SELECT id, name FROM categories').all()
    .filter(c => mentioned(c.name) && !planNames.has(c.name.toLowerCase()))
    .map(c => ({ name: c.name, ref: c.id }));

  return [...fromPlan, ...fromDb];
}

/**
 * Corrects item category references that contradict the category named in the text.
 *
 * @param {Array} operations - Operations for this step / plan.
 * @param {Object} ctx
 * @param {string} ctx.text - The step or request text the operations should satisfy.
 * @param {import('better-sqlite3').Database} ctx.db - Executor database.
 * @param {Array} [ctx.planned=[]] - Operations planned by earlier steps (for full-plan indexes).
 * @returns {{ operations: Array, fixes: Array<{ index: number, item: string, from: any, to: any, category: string }> }}
 */
export function groundCategoryRefs(operations, { text, db, planned = [] }) {
  const named = namedCategories(text, [...planned, ...operations], db);
  if (named.length !== 1) return { operations, fixes: [] }; // none or ambiguous: leave as planned
  const [target] = named;

  const fixes = [];
  const grounded = operations.map((op, i) => {
    const index = planned.length + i;
    if (op.table !== 'items' || !op.data || op.data.category_id === undefined) return op;
    if (typeof target.ref === 'string' && Number(target.ref.slice(5)) >= index) return op; // can only point backwards
    if (String(op.data.category_id) === String(target.ref)) return op;
    fixes.push({ index, item: op.data.name || `operation ${index}`, from: op.data.category_id, to: target.ref, category: target.name });
    return { ...op, data: { ...op.data, category_id: target.ref } };
  });
  return { operations: grounded, fixes };
}
