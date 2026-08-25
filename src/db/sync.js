import { createEnvelope } from '../utils/protocol.js';
import { logMeshOperation, getMeshLog } from './sqlite.js';
import { validateTransaction } from './schema-validator.js';

/**
 * VectorClock class implementing a CRDT-inspired clock for causal ordering and sync.
 */
export class VectorClock {
  /**
   * Initializes a new VectorClock.
   * @param {Object} [clock={}] - Initial clock state mapping peer IDs to sequence numbers.
   */
  constructor(clock = {}) {
    this.clock = { ...clock };
  }

  /**
   * Increments the sequence number for a specific peer.
   * @param {string} peerId - The ID of the peer to increment.
   */
  increment(peerId) {
    this.clock[peerId] = (this.clock[peerId] || 0) + 1;
  }

  /**
   * Merges another vector clock into this one by taking the element-wise maximum.
   * @param {VectorClock|Object} otherClock - The clock to merge with.
   */
  merge(otherClock) {
    const otherClockState = otherClock instanceof VectorClock ? otherClock.clock : otherClock;
    for (const [peer, seq] of Object.entries(otherClockState)) {
      this.clock[peer] = Math.max(this.clock[peer] || 0, seq);
    }
  }

  /**
   * Checks if this vector clock has entries newer than the provided clock.
   * Useful for determining if there are updates to send.
   * @param {VectorClock|Object} otherClock - The clock to compare against.
   * @returns {boolean} True if this clock is newer in at least one dimension.
   */
  isNewerThan(otherClock) {
    const otherClockState = otherClock instanceof VectorClock ? otherClock.clock : otherClock;
    for (const [peer, seq] of Object.entries(this.clock)) {
      if (seq > (otherClockState[peer] || 0)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Serializes the vector clock to a JSON object.
   * @returns {Object} JSON representation of the clock.
   */
  toJSON() {
    return { ...this.clock };
  }

  /**
   * Deserializes a vector clock from a JSON object.
   * @param {Object} json - The JSON representation of a clock.
   * @returns {VectorClock} A new VectorClock instance.
   */
  static fromJSON(json) {
    return new VectorClock(json);
  }
}

// Module-level local vector clock instance
let localVectorClock = null;
let localPeerId = null;

/**
 * Initializes the sync module and local vector clock.
 * @param {string} peerId - The local node's peer identifier.
 */
export function initSync(peerId) {
  localPeerId = peerId;
  localVectorClock = new VectorClock();
  // Ensure the local peer exists in its own clock
  localVectorClock.clock[localPeerId] = 0;
}

/**
 * Retrieves the current state of the local vector clock.
 * @returns {VectorClock} The local vector clock.
 */
export function getVectorClock() {
  return localVectorClock;
}

/**
 * Records a local write operation, updating the clock and logging to the mesh log.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {string} operation - The operation type (INSERT, UPDATE, DELETE).
 * @param {string} tableName - The table modified.
 * @param {Object} rowData - The data modified.
 * @param {string} peerId - The local peer ID.
 * @returns {Object} An envelope-ready payload for gossip broadcast.
 */
export function recordLocalWrite(db, operation, tableName, rowData, peerId) {
  // Increment local clock before recording
  localVectorClock.increment(peerId);
  
  const id = `${peerId}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  const clockSnapshot = JSON.stringify(localVectorClock.toJSON());
  const rowDataStr = JSON.stringify(rowData);

  // Log to local _mesh_log
  logMeshOperation(db, {
    id,
    operation,
    tableName,
    rowData: rowDataStr,
    vectorClock: clockSnapshot,
    peerId
  });

  // Create envelope for propagation (assuming createEnvelope signature matches)
  return createEnvelope('TRANSACTION', {
    id,
    operation,
    tableName,
    rowData: rowDataStr,
    vectorClock: clockSnapshot,
    peerId
  }, peerId);
}

/**
 * Applies a remote transaction received via gossip replication.
 * Performs conflict resolution (LWW) and schema validation.
 * 
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {Object} envelope - The gossiped transaction envelope.
 * @param {string} localPeerId - The local peer identifier.
 * @returns {Object} Result indicating if applied, or if conflicts occurred.
 */
export function applyRemoteTransaction(db, envelope, localPeerId) {
  const payload = envelope.payload;

  // Step 1: Check if already applied to avoid duplicates
  const checkStmt = db.prepare('SELECT 1 FROM _mesh_log WHERE id = ?');
  if (checkStmt.get(payload.id)) {
    return { applied: false, reason: 'Already applied' };
  }

  const parsedData = JSON.parse(payload.rowData);

  // Step 2: Run fast schema-validator against local DB state
  const validation = validateTransaction(payload.operation, payload.tableName, parsedData, db);

  if (!validation.valid) {
    // Basic LWW (Last-Writer-Wins) resolution logic based on timestamps/conflicts
    // In a real system, you might inspect validation errors (like unique constraint)
    // and compare vector clocks or timestamps to pick the winner.
    // For this example, we return conflict true if validation fails (e.g. duplicate SKU).
    return { applied: false, conflict: true, errors: validation.errors };
  }

  // Step 3: If valid, execute the write transaction
  try {
    db.transaction(() => {
      let execStmt;
      if (payload.operation === 'INSERT') {
        const keys = Object.keys(parsedData);
        const placeholders = keys.map(() => '?').join(', ');
        execStmt = db.prepare(`INSERT INTO ${payload.tableName} (${keys.join(', ')}) VALUES (${placeholders})`);
        execStmt.run(...Object.values(parsedData));
      } else if (payload.operation === 'UPDATE') {
        const keys = Object.keys(parsedData).filter(k => k !== 'id');
        const setClause = keys.map(k => `${k} = ?`).join(', ');
        execStmt = db.prepare(`UPDATE ${payload.tableName} SET ${setClause} WHERE id = ?`);
        execStmt.run(...keys.map(k => parsedData[k]), parsedData.id);
      } else if (payload.operation === 'DELETE') {
        execStmt = db.prepare(`DELETE FROM ${payload.tableName} WHERE id = ?`);
        execStmt.run(parsedData.id);
      }

      // Log to local _mesh_log
      logMeshOperation(db, {
        id: payload.id,
        operation: payload.operation,
        tableName: payload.tableName,
        rowData: payload.rowData,
        vectorClock: payload.vectorClock,
        peerId: payload.peerId
      });
    })();
  } catch (err) {
    return { applied: false, error: err.message };
  }

  // Step 3 (cont): Merge vector clocks
  const remoteClock = VectorClock.fromJSON(JSON.parse(payload.vectorClock));
  localVectorClock.merge(remoteClock);

  return { applied: true };
}

/**
 * Handles a synchronization request from a remote peer by comparing clocks.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {Object} remoteClockJson - The remote peer's clock in JSON format.
 * @returns {Array<Object>} Array of _mesh_log entries the remote peer is missing.
 */
export function handleSyncRequest(db, remoteClockJson) {
  const remoteClock = VectorClock.fromJSON(remoteClockJson);
  
  // To keep it simple without full historical vector clocks per row,
  // we fetch all logs and filter those originating from peers where the
  // sequence in the log's clock is strictly greater than the remote's knowledge.
  // In a robust implementation, you might maintain a sequence column per peer.
  
  const allLogs = getMeshLog(db, '1970-01-01T00:00:00Z'); // fetch all or some reasonable horizon
  const missingLogs = [];

  for (const log of allLogs) {
    const logClock = JSON.parse(log.vector_clock);
    const originPeer = log.peer_id;
    const originSeq = logClock[originPeer] || 0;
    const remoteSeq = remoteClock.clock[originPeer] || 0;

    // If the log's originating sequence is greater than what the remote knows, they need it
    if (originSeq > remoteSeq) {
      missingLogs.push(log);
    }
  }

  return missingLogs;
}
