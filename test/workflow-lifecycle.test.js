import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { initDatabase } from '../src/db/sqlite.js';
import { createSyncEngine } from '../src/db/sync.js';
import {
  createTask,
  planTask,
  executeTask,
  submitApprovalDecision,
  validateTaskRun,
  TaskStatus,
  ApprovalDecision
} from '../src/mesh/workflow-service.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

describe('Workflow Lifecycle, Role Separation & Approval Enforcement', () => {
  let tmpDir;
  let db;
  let syncEngine;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'mesh-wf-life-'));
    db = initDatabase(join(tmpDir, 'life-test.db'));
    syncEngine = createSyncEngine('node-test-life');
  });

  afterEach(() => {
    try {
      db.close();
      rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  });

  it('creates task in queued status with audit trail', () => {
    const task = createTask(db, {
      title: 'Automated 3NF Integrity Check',
      user_prompt: 'Scan categories and items for foreign key violations',
      task_type: 'verification',
      priority: 'medium',
      requested_by: 'alice'
    });

    assert.equal(task.status, TaskStatus.QUEUED);
    assert.equal(task.title, 'Automated 3NF Integrity Check');

    const audit = db.prepare("SELECT * FROM audit_events WHERE entity_id = ? AND action = 'TASK_CREATED'").get(task.id);
    assert.ok(audit, 'Audit event must be logged on task creation');
    assert.equal(audit.actor, 'alice');
  });

  it('planner role decomposes task into planned steps without mutating business tables', () => {
    const itemsCountBefore = db.prepare('SELECT COUNT(*) as c FROM items').get().c;

    const task = createTask(db, {
      title: 'Generate Category Report',
      user_prompt: 'Summarize electronics category statistics',
      priority: 'low'
    });

    const plannedTask = planTask(db, task.id, { plannerNodeId: 'node-delta', riskLevel: 'low' });
    assert.equal(plannedTask.status, TaskStatus.PLANNED);

    const runs = db.prepare('SELECT * FROM task_runs WHERE task_id = ?').all(task.id);
    assert.equal(runs.length, 1);
    assert.equal(runs[0].planner_node, 'node-delta');

    const steps = db.prepare('SELECT * FROM task_steps WHERE task_id = ?').all(task.id);
    assert.ok(steps.length >= 3, 'Planner must produce ordered steps');

    const itemsCountAfter = db.prepare('SELECT COUNT(*) as c FROM items').get().c;
    assert.equal(itemsCountAfter, itemsCountBefore, 'Planning must NEVER mutate business tables');
  });

  it('enforces approval on high-risk task and blocks execution until approved', () => {
    const task = createTask(db, {
      title: 'Destructive DDL Schema Migration',
      user_prompt: 'Alter tables and drop legacy indexes across the cluster',
      priority: 'high'
    });

    // Planning high-risk task moves it to awaiting_approval and creates approval record
    const planned = planTask(db, task.id, { plannerNodeId: 'node-delta', riskLevel: 'high' });
    assert.equal(planned.status, TaskStatus.AWAITING_APPROVAL);

    const approval = db.prepare('SELECT * FROM approvals WHERE task_id = ?').get(task.id);
    assert.ok(approval, 'Approval record must be created for high-risk task');
    assert.equal(approval.decision, ApprovalDecision.PENDING);
    assert.equal(approval.risk_level, 'high');

    // Attempting execution while approval is pending MUST be rejected
    assert.throws(() => {
      executeTask(db, task.id, { executorNodeId: 'node-alpha', syncEngine });
    }, /cannot execute: approval .* is pending review/i);

    // Verify rejection audit event was logged
    const rejectionAudit = db.prepare("SELECT * FROM audit_events WHERE entity_id = ? AND action = 'EXECUTION_REJECTED_UNAPPROVED'").get(task.id);
    assert.ok(rejectionAudit, 'Security audit event must record unauthorized execution attempt');

    // Submit approval
    const decided = submitApprovalDecision(db, approval.id, {
      decision: ApprovalDecision.APPROVED,
      reviewer: 'lead_architect',
      reason: 'Migration verified on staging environment'
    });
    assert.equal(decided.decision, ApprovalDecision.APPROVED);

    // Now task status is PLANNED and execution succeeds
    const taskAfterApproval = db.prepare('SELECT * FROM tasks WHERE id = ?').get(task.id);
    assert.equal(taskAfterApproval.status, TaskStatus.PLANNED);

    const execOutcome = executeTask(db, task.id, { executorNodeId: 'node-alpha', syncEngine });
    assert.equal(execOutcome.success, true);
    assert.equal(execOutcome.status, TaskStatus.COMPLETED);
  });

  it('fails task if approval decision is rejected', () => {
    const task = createTask(db, {
      title: 'Risky Migration',
      user_prompt: 'Drop database indexes',
      priority: 'critical'
    });

    planTask(db, task.id, { plannerNodeId: 'node-delta', riskLevel: 'critical' });
    const approval = db.prepare('SELECT * FROM approvals WHERE task_id = ?').get(task.id);

    submitApprovalDecision(db, approval.id, {
      decision: ApprovalDecision.REJECTED,
      reviewer: 'security_auditor',
      reason: 'Unacceptable data risk'
    });

    const taskAfter = db.prepare('SELECT * FROM tasks WHERE id = ?').get(task.id);
    assert.equal(taskAfter.status, TaskStatus.FAILED);
  });

  it('atomically rolls back entire task execution if a mutation fails midway', () => {
    const task = createTask(db, {
      title: 'Batch Mutation Task',
      user_prompt: 'Insert multiple categories atomically',
      priority: 'medium'
    });

    planTask(db, task.id, { plannerNodeId: 'node-delta', riskLevel: 'low' });

    const categoriesBefore = db.prepare('SELECT COUNT(*) as c FROM categories').get().c;

    // A batch where the second operation violates check constraint or FK
    const failingMutations = [
      {
        operation: 'INSERT',
        table: 'categories',
        data: { name: 'Batch Category 1', description: 'Should roll back' }
      },
      {
        operation: 'INSERT',
        table: 'items',
        data: { name: 'Invalid Price Item', price: -50, sku: 'SKU-ROLLBACK', category_id: 1 } // price <= 0 violates check
      }
    ];

    const outcome = executeTask(db, task.id, {
      executorNodeId: 'node-alpha',
      syncEngine,
      mutations: failingMutations
    });

    assert.equal(outcome.success, false);
    assert.equal(outcome.status, TaskStatus.FAILED);
    assert.ok(outcome.error.includes('price must be greater than 0'));

    // Verify atomic rollback: Category 1 was NOT inserted
    const categoriesAfter = db.prepare('SELECT COUNT(*) as c FROM categories').get().c;
    assert.equal(categoriesAfter, categoriesBefore, 'Database writes must roll back completely on failure');
  });

  it('validator node audits completed task run and records verification verdict', () => {
    const task = createTask(db, {
      title: 'Valid Execution Workflow',
      user_prompt: 'Create standard inventory item',
      priority: 'low'
    });

    planTask(db, task.id, { plannerNodeId: 'node-delta', riskLevel: 'low' });
    const execOutcome = executeTask(db, task.id, { executorNodeId: 'node-alpha', syncEngine });
    assert.equal(execOutcome.success, true);

    const validationResult = validateTaskRun(db, execOutcome.runId, 'node-gamma');
    assert.equal(validationResult.verdict, 'VALIDATION_PASSED');
    assert.equal(validationResult.passed, true);

    const audit = db.prepare("SELECT * FROM audit_events WHERE entity_id = ? AND action = 'VALIDATION_PASSED'").get(execOutcome.runId);
    assert.ok(audit, 'Validator verdict must be recorded in audit history');
    assert.equal(audit.actor, 'node-gamma');
  });
});
