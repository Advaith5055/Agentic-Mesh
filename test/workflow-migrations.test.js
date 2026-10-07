import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { initDatabase, runWorkflowMigrations, getSchema } from '../src/db/sqlite.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

describe('Workflow Platform Safe Migrations & Schema Strategy', () => {
  let tmpDir;
  let db;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'mesh-mig-test-'));
    db = initDatabase(join(tmpDir, 'migration-test.db'));
  });

  afterEach(() => {
    try {
      db.close();
      rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  });

  it('preserves existing legacy inventory tables and initial demo seeds', () => {
    const categories = db.prepare('SELECT * FROM categories').all();
    const items = db.prepare('SELECT * FROM items').all();
    const suppliers = db.prepare('SELECT * FROM suppliers').all();
    const itemSuppliers = db.prepare('SELECT * FROM item_suppliers').all();

    assert.ok(categories.length >= 3, 'Categories should contain seeded demo data');
    assert.ok(items.length >= 6, 'Items should contain seeded demo items');
    assert.ok(suppliers.length >= 2, 'Suppliers should contain seeded demo suppliers');
    assert.ok(itemSuppliers.length >= 4, 'Item-suppliers should contain seeded links');
  });

  it('creates all 10 workflow platform tables with expected schemas', () => {
    const requiredTables = [
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

    const masterTables = db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map(r => r.name);

    for (const tbl of requiredTables) {
      assert.ok(masterTables.includes(tbl), `Table '${tbl}' must exist in SQLite schema`);
    }
  });

  it('records schema migration history in _schema_migrations', () => {
    const migrations = db.prepare('SELECT * FROM _schema_migrations ORDER BY version ASC').all();
    assert.ok(migrations.length >= 2, 'Should record at least migrations 1 and 2');
    assert.equal(migrations[0].version, 1);
    assert.equal(migrations[1].version, 2);
  });

  it('is completely idempotent when run multiple times without data corruption', () => {
    const tasksCountBefore = db.prepare('SELECT COUNT(*) as c FROM tasks').get().c;
    const nodesCountBefore = db.prepare('SELECT COUNT(*) as c FROM mesh_nodes').get().c;

    // Run migrations again
    runWorkflowMigrations(db);
    runWorkflowMigrations(db);

    const tasksCountAfter = db.prepare('SELECT COUNT(*) as c FROM tasks').get().c;
    const nodesCountAfter = db.prepare('SELECT COUNT(*) as c FROM mesh_nodes').get().c;

    assert.equal(tasksCountAfter, tasksCountBefore, 'Task count must remain unchanged after re-running migrations');
    assert.equal(nodesCountAfter, nodesCountBefore, 'Node count must remain unchanged');
  });

  it('returns valid concatenated DDL through getSchema()', () => {
    const ddl = getSchema(db);
    assert.ok(typeof ddl === 'string');
    assert.ok(ddl.includes('CREATE TABLE mesh_nodes'));
    assert.ok(ddl.includes('CREATE TABLE tasks'));
    assert.ok(ddl.includes('CREATE TABLE artifacts'));
  });
});
