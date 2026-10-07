import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { VectorClock } from '../src/db/sync.js';

describe('VectorClock Causal Tracking', () => {
  it('initializes with default empty clock or provided object', () => {
    const emptyClock = new VectorClock();
    assert.deepEqual(emptyClock.toJSON(), {});

    const initClock = new VectorClock({ alpha: 1, bravo: 3 });
    assert.deepEqual(initClock.toJSON(), { alpha: 1, bravo: 3 });
  });

  it('increments sequence for a specific peer', () => {
    const clock = new VectorClock();
    clock.increment('peer-alpha');
    assert.equal(clock.toJSON()['peer-alpha'], 1);
    clock.increment('peer-alpha');
    assert.equal(clock.toJSON()['peer-alpha'], 2);
    clock.increment('peer-bravo');
    assert.equal(clock.toJSON()['peer-bravo'], 1);
  });

  it('merges another vector clock using element-wise maximum', () => {
    const clockA = new VectorClock({ alpha: 3, bravo: 1, charlie: 4 });
    const clockB = new VectorClock({ alpha: 2, bravo: 5, delta: 2 });

    clockA.merge(clockB);
    assert.deepEqual(clockA.toJSON(), {
      alpha: 3,
      bravo: 5,
      charlie: 4,
      delta: 2
    });
  });

  it('merges vector clock from JSON string safely', () => {
    const clock = new VectorClock({ alpha: 1 });
    clock.merge('{"alpha": 4, "bravo": 2}');
    assert.deepEqual(clock.toJSON(), { alpha: 4, bravo: 2 });
  });

  it('correctly determines isNewerThan', () => {
    const local = new VectorClock({ alpha: 3, bravo: 2 });
    const remoteOlder = new VectorClock({ alpha: 2, bravo: 2 });
    const remoteEqual = new VectorClock({ alpha: 3, bravo: 2 });
    const remoteAhead = new VectorClock({ alpha: 3, bravo: 3 });

    assert.equal(local.isNewerThan(remoteOlder), true);
    assert.equal(local.isNewerThan(remoteEqual), false);
    assert.equal(local.isNewerThan(remoteAhead), false);
  });

  it('serializes to JSON and deserializes via fromJSON', () => {
    const original = new VectorClock({ alpha: 10, bravo: 20 });
    const jsonStr = JSON.stringify(original.toJSON());

    const fromObj = VectorClock.fromJSON(original.toJSON());
    assert.deepEqual(fromObj.toJSON(), { alpha: 10, bravo: 20 });

    const fromString = VectorClock.fromJSON(jsonStr);
    assert.deepEqual(fromString.toJSON(), { alpha: 10, bravo: 20 });

    const fromNull = VectorClock.fromJSON(null);
    assert.deepEqual(fromNull.toJSON(), {});
  });
});
