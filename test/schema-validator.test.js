import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { initDatabase } from '../src/db/sqlite.js';
import { validateTransaction, validateAllowlist } from '../src/db/schema-validator.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

describe('Schema Validator & Allowlists', () => {
  let tmpDir;
  let db;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'mesh-val-test-'));
    db = initDatabase(join(tmpDir, 'test.db'));
  });

  afterEach(() => {
    try {
      db.close();
      rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  });

  it('validates table and column allowlists strictly', () => {
    assert.equal(validateAllowlist('items', ['name', 'price', 'sku', 'category_id']).valid, true);
    assert.equal(validateAllowlist('unknown_table', ['name']).valid, false);
    assert.equal(validateAllowlist('items', ['invalid_col']).valid, false);
    assert.equal(validateAllowlist('categories', ['name', 'description']).valid, true);
  });

  it('accepts valid item insert', () => {
    const data = {
      category_id: 1,
      name: 'Mechanical Keyboard',
      price: 129.99,
      sku: 'SKU-TEST-001'
    };
    const res = validateTransaction('INSERT', 'items', data, db);
    assert.equal(res.valid, true);
    assert.equal(res.errors.length, 0);
  });

  it('rejects insert with missing required fields', () => {
    const data = {
      category_id: 1,
      name: 'Incomplete Item'
      // missing price and sku
    };
    const res = validateTransaction('INSERT', 'items', data, db);
    assert.equal(res.valid, false);
    assert.ok(res.errors.some(e => e.includes("Missing required field: 'price'")));
    assert.ok(res.errors.some(e => e.includes("Missing required field: 'sku'")));
  });

  it('rejects insert with invalid types', () => {
    const data = {
      category_id: 'not-a-number',
      name: 12345,
      price: 'expensive',
      sku: true
    };
    const res = validateTransaction('INSERT', 'items', data, db);
    assert.equal(res.valid, false);
    assert.ok(res.errors.length >= 3);
  });

  it('rejects insert with non-positive price check constraint', () => {
    const data = {
      category_id: 1,
      name: 'Free Item',
      price: -10,
      sku: 'SKU-FREE-01'
    };
    const res = validateTransaction('INSERT', 'items', data, db);
    assert.equal(res.valid, false);
    assert.ok(res.errors.some(e => e.includes('price must be greater than 0')));
  });

  it('rejects foreign key violation for nonexistent category_id', () => {
    const data = {
      category_id: 99999, // does not exist in seeded categories
      name: 'Orphan Item',
      price: 49.99,
      sku: 'SKU-ORPHAN-01'
    };
    const res = validateTransaction('INSERT', 'items', data, db);
    assert.equal(res.valid, false);
    assert.ok(res.errors.some(e => e.includes('Foreign key violation')));
  });

  it('rejects unique constraint violation for existing SKU', () => {
    const data = {
      category_id: 1,
      name: 'Duplicate Laptop SKU',
      price: 899.99,
      sku: 'SKU-E001' // already seeded in initDatabase
    };
    const res = validateTransaction('INSERT', 'items', data, db);
    assert.equal(res.valid, false);
    assert.ok(res.errors.some(e => e.includes('Unique constraint violation')));
  });

  it('validates DELETE requires an identifier', () => {
    const noId = validateTransaction('DELETE', 'items', {}, db);
    assert.equal(noId.valid, false);

    const validId = validateTransaction('DELETE', 'items', { id: 1 }, db);
    assert.equal(validId.valid, true);

    const nonExistent = validateTransaction('DELETE', 'items', { id: 99999 }, db);
    assert.equal(nonExistent.valid, false);
    assert.ok(nonExistent.errors.some(e => e.includes('Row to delete not found')));
  });

  it('rejects unsupported operations strictly', () => {
    const dropOp = validateTransaction('DROP', 'items', { id: 1 }, db);
    assert.equal(dropOp.valid, false);
    assert.ok(dropOp.errors.some(e => e.includes("Operation 'DROP' is not supported")));

    const selectOp = validateTransaction('SELECT', 'items', { id: 1 }, db);
    assert.equal(selectOp.valid, false);

    const nullOp = validateTransaction(null, 'items', { id: 1 }, db);
    assert.equal(nullOp.valid, false);
  });

  it('validates DELETE and UPDATE on composite key table item_suppliers', () => {
    // Missing one key
    const missingSupplier = validateTransaction('DELETE', 'item_suppliers', { item_id: 1 }, db);
    assert.equal(missingSupplier.valid, false);

    // Existing link Laptop -> TechCorp (item_id 1, supplier_id 1)
    const validDelete = validateTransaction('DELETE', 'item_suppliers', { item_id: 1, supplier_id: 1 }, db);
    assert.equal(validDelete.valid, true);

    // Nonexistent link
    const nonexistentDelete = validateTransaction('DELETE', 'item_suppliers', { item_id: 99, supplier_id: 99 }, db);
    assert.equal(nonexistentDelete.valid, false);
  });

  it('validates UPDATE requires an id and checks existence', () => {
    const noId = validateTransaction('UPDATE', 'items', { price: 49.99 }, db);
    assert.equal(noId.valid, false);
    assert.ok(noId.errors.some(e => e.includes('UPDATE requires an id field')));

    const validUpdate = validateTransaction('UPDATE', 'items', { id: 1, price: 899.99 }, db);
    assert.equal(validUpdate.valid, true);

    const nonexistentUpdate = validateTransaction('UPDATE', 'items', { id: 9999, price: 899.99 }, db);
    assert.equal(nonexistentUpdate.valid, false);
    assert.ok(nonexistentUpdate.errors.some(e => e.includes('Row to update not found')));
  });
});
