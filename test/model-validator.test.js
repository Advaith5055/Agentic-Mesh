import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { validateModelPlan } from '../src/agents/model-validator.js';

describe('Model Output Validator', () => {
  it('validates a correct plan array', () => {
    const plan = [
      {
        operation: 'INSERT',
        table: 'categories',
        data: { name: 'Hardware', description: 'Components' }
      },
      {
        operation: 'INSERT',
        table: 'items',
        data: { category_id: 1, name: 'GPU', price: 499.99, sku: 'SKU-GPU-01' }
      }
    ];

    const res = validateModelPlan(plan);
    assert.equal(res.valid, true);
    assert.equal(res.operations.length, 2);
    assert.equal(res.errors.length, 0);
  });

  it('validates a plan object with operations property', () => {
    const plan = {
      operations: [
        {
          operation: 'UPDATE',
          table: 'items',
          data: { id: 1, price: 59.99 }
        }
      ]
    };

    const res = validateModelPlan(plan);
    assert.equal(res.valid, true);
    assert.equal(res.operations.length, 1);
  });

  it('rejects non-array or empty plans', () => {
    assert.equal(validateModelPlan(null).valid, false);
    assert.equal(validateModelPlan('DROP TABLE items').valid, false);
    assert.equal(validateModelPlan([]).valid, false);
  });

  it('rejects dangerous or disallowed operations', () => {
    const dangerousPlan = [
      {
        operation: 'DROP',
        table: 'items',
        data: { id: 1 }
      }
    ];
    const res = validateModelPlan(dangerousPlan);
    assert.equal(res.valid, false);
    assert.ok(res.errors[0].includes('unsupported operation'));
  });

  it('rejects disallowed tables', () => {
    const badTablePlan = [
      {
        operation: 'INSERT',
        table: '_mesh_log',
        data: { id: 'spoof' }
      }
    ];
    const res = validateModelPlan(badTablePlan);
    assert.equal(res.valid, false);
    assert.ok(res.errors[0].includes('unsupported table'));
  });
});
