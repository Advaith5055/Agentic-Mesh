import { createEnvelope, MessageType } from '../utils/protocol.js';
import { logMeshOperation, getMeshLog, insertItem, insertCategory } from './sqlite.js';
import { validateTransaction, validateAllowlist } from './schema-validator.js';

/**
 * VectorClock class implementing causal ordering and sync.
 * (Note: Used for log-based synchronization and causal ordering;
 * conflict handling uses validation rules / last-write-wins rather than full CRDTs).
 */
export class VectorClock {
  /**
   * Initializes a new VectorClock.
   * @param {Object} [clock={}] - Initial clock state mapping peer IDs to sequence numbers.
   */
  constructor(clock = {}) {
    this.clock = {};
    if (clock && typeof clock === 'object') {
      for (const [peer, seq] of Object.entries(clock)) {
        this.clock[peer] = Number(seq) || 0;
      }
    }
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
   * @param {VectorClock|Object|string} otherClock - The clock to merge with.
   */
  merge(otherClock) {
    if (!otherClock) return;
    let otherClockState;
    if (otherClock instanceof VectorClock) {
      otherClockState = otherClock.clock;
    } else if (typeof otherClock === 'string') {
      try {
        otherClockState = JSON.parse(otherClock);
      } catch {
        otherClockState = {};
      }
    } else {
      otherClockState = otherClock;
    }
    for (const [peer, seq] of Object.entries(otherClockState || {})) {
      this.clock[peer] = Math.max(this.clock[peer] || 0, Number(seq) || 0);
    }
  }

  /**
   * Checks if this vector clock has entries newer than the provided clock.
   * @param {VectorClock|Object|string} otherClock - The clock to compare against.
   * @returns {boolean} True if this clock is newer in at least one dimension.
   */
  isNewerThan(otherClock) {
    const other = otherClock instanceof VectorClock ? otherClock : VectorClock.fromJSON(otherClock);
    for (const [peer, seq] of Object.entries(this.clock)) {
      if (seq > (other.clock[peer] || 0)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Serializes the vector clock to a plain object.
   * @returns {Object} JSON-serializable representation of the clock.
   */
  toJSON() {
    return { ...this.clock };
  }

  /**
   * Deserializes a vector clock from a JSON object or string.
   * @param {Object|string} json - The JSON representation of a clock.
   * @returns {VectorClock} A new VectorClock instance.
   */
  static fromJSON(json) {
    if (!json) return new VectorClock();
    if (json instanceof VectorClock) return new VectorClock(json.clock);
    if (typeof json === 'string') {
      try {
        const parsed = JSON.parse(json);
        return new VectorClock(parsed);
      } catch {
        return new VectorClock();
      }
    }
    return new VectorClock(json);
  }
}

/**
 * SyncEngine class managing peer-specific vector clock, local logging, and remote replication.
 */
export class SyncEngine {
  /**
   * Initializes a SyncEngine for a specific node peer.
   * @param {string} peerId - The local node's peer identifier.
   * @param {Object} [options={}] - Configuration options.
   */
  constructor(peerId, options = {}) {
    this.peerId = peerId;
    this.options = options;
    this.vectorClock = new VectorClock();
    if (this.peerId) {
      this.vectorClock.clock[this.peerId] = 0;
    }
  }

  /**
   * Retrieves the current state of this engine's vector clock.
   * @returns {VectorClock} The vector clock instance.
   */
  getVectorClock() {
    return this.vectorClock;
  }

  /**
   * Records a local write operation, advancing the clock and writing to _mesh_log.
   * @param {import('better-sqlite3').Database} db - The database instance.
   * @param {string} operation - The operation type (INSERT, UPDATE, DELETE).
   * @param {string} tableName - The table modified.
   * @param {Object|string} rowData - The data modified.
   * @returns {Object} An envelope-ready payload for gossip broadcast.
   */
  recordLocalWrite(db, operation, tableName, rowData) {
    this.vectorClock.increment(this.peerId);

    const id = `${this.peerId}-${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
    const clockSnapshot = JSON.stringify(this.vectorClock.toJSON());
    const rowDataStr = typeof rowData === 'string' ? rowData : JSON.stringify(rowData);

    // Log to local _mesh_log
    logMeshOperation(db, {
      id,
      operation,
      tableName,
      rowData: rowDataStr,
      vectorClock: clockSnapshot,
      peerId: this.peerId
    });

    // Create envelope for propagation
    return createEnvelope(
      MessageType.TRANSACTION,
      {
        id,
        operation,
        tableName,
        rowData: rowDataStr,
        vectorClock: clockSnapshot,
        peerId: this.peerId
      },
      this.peerId,
      this.vectorClock.toJSON()
    );
  }

  /**
   * Applies a remote transaction received via gossip replication.
   * Performs deduplication, schema validation, allowlist check, atomic execution, and clock merge.
   *
   * @param {import('better-sqlite3').Database} db - The database instance.
   * @param {Object} envelope - The gossiped transaction envelope.
   * @returns {{ applied: boolean, duplicate?: boolean, conflict?: boolean, errors?: string[], reason?: string, error?: string }}
   */
  applyRemoteTransaction(db, envelope) {
    const payload = envelope?.payload;
    if (!payload || !payload.id) {
      return { applied: false, error: 'Invalid envelope payload.' };
    }

    const sourcePeer = payload.peerId || envelope?.sender || 'unknown';

    // Step 1: Check if already applied to avoid duplicates
    const checkStmt = db.prepare('SELECT 1 FROM _mesh_log WHERE id = ?');
    if (checkStmt.get(payload.id)) {
      if (payload.vectorClock && this.vectorClock) {
        this.vectorClock.merge(payload.vectorClock);
      }
      return { applied: false, duplicate: true, reason: 'Already applied' };
    }

    let parsedData;
    try {
      parsedData = typeof payload.rowData === 'string' ? JSON.parse(payload.rowData) : payload.rowData;
    } catch (err) {
      return { applied: false, error: `Invalid JSON in rowData: ${err.message}` };
    }

    // Step 2: Validate against table/column allowlist
    const allowCheck = validateAllowlist(payload.tableName, parsedData ? Object.keys(parsedData) : []);
    if (!allowCheck.valid) {
      this._recordConflictSafe(db, {
        conflict_type: 'remote_allowlist_violation',
        affected_entity_type: payload.tableName,
        affected_entity_id: parsedData?.id || payload.id,
        peer_id: sourcePeer,
        details: { errors: [allowCheck.error], operation: payload.operation }
      });
      this._recordReplicationMetricSafe(db, {
        source_peer_id: sourcePeer,
        target_peer_id: this.peerId,
        latency_ms: 0,
        status: 'failure',
        vector_clock_meta: payload.vectorClock
      });
      return { applied: false, conflict: true, errors: [allowCheck.error] };
    }

    // Step 3: Run schema validation against local DB state
    const validation = validateTransaction(payload.operation, payload.tableName, parsedData, db);
    if (!validation.valid) {
      this._recordConflictSafe(db, {
        conflict_type: 'remote_schema_conflict',
        affected_entity_type: payload.tableName,
        affected_entity_id: parsedData?.id || payload.id,
        peer_id: sourcePeer,
        details: { errors: validation.errors, operation: payload.operation }
      });
      this._recordReplicationMetricSafe(db, {
        source_peer_id: sourcePeer,
        target_peer_id: this.peerId,
        latency_ms: 0,
        status: 'failure',
        vector_clock_meta: payload.vectorClock
      });
      return { applied: false, conflict: true, errors: validation.errors };
    }

    // Step 3.5: Detect concurrent task assignment conflicts
    if (payload.tableName === 'tasks' && parsedData?.id) {
      const existingTask = db.prepare('SELECT * FROM tasks WHERE id = ?').get(parsedData.id);
      if (existingTask) {
        if (parsedData.assigned_node_id && existingTask.assigned_node_id && parsedData.assigned_node_id !== existingTask.assigned_node_id) {
          this._recordConflictSafe(db, {
            conflict_type: 'concurrent_task_assignment',
            affected_entity_type: 'tasks',
            affected_entity_id: parsedData.id,
            peer_id: sourcePeer,
            details: {
              local_assignment: existingTask.assigned_node_id,
              remote_assignment: parsedData.assigned_node_id,
              local_status: existingTask.status,
              remote_status: parsedData.status
            }
          });
        }
      }
    }

    // Step 4: Execute the write transaction and log to _mesh_log atomically
    try {
      db.transaction(() => {
        let execStmt;
        if (payload.tableName === 'categories' && payload.operation === 'INSERT') {
          insertCategory(db, parsedData);
        } else if (payload.tableName === 'items' && payload.operation === 'INSERT') {
          insertItem(db, parsedData);
        } else if (payload.operation === 'INSERT') {
          const keys = Object.keys(parsedData);
          const placeholders = keys.map(() => '?').join(', ');
          execStmt = db.prepare(`INSERT INTO ${payload.tableName} (${keys.join(', ')}) VALUES (${placeholders})`);
          execStmt.run(...Object.values(parsedData));
        } else if (payload.operation === 'UPDATE') {
          if (payload.tableName === 'item_suppliers') {
            execStmt = db.prepare('UPDATE item_suppliers SET supplier_id = ? WHERE item_id = ? AND supplier_id = ?');
            execStmt.run(parsedData.supplier_id, parsedData.item_id, parsedData.supplier_id);
          } else {
            const keys = Object.keys(parsedData).filter(k => k !== 'id');
            const setClause = keys.map(k => `${k} = ?`).join(', ');
            execStmt = db.prepare(`UPDATE ${payload.tableName} SET ${setClause} WHERE id = ?`);
            execStmt.run(...keys.map(k => parsedData[k]), parsedData.id);
          }
        } else if (payload.operation === 'DELETE') {
          if (payload.tableName === 'item_suppliers') {
            execStmt = db.prepare('DELETE FROM item_suppliers WHERE item_id = ? AND supplier_id = ?');
            execStmt.run(parsedData.item_id, parsedData.supplier_id);
          } else {
            execStmt = db.prepare(`DELETE FROM ${payload.tableName} WHERE id = ?`);
            execStmt.run(parsedData.id);
          }
        }

        // Log to local _mesh_log
        const rowDataStr = typeof payload.rowData === 'string' ? payload.rowData : JSON.stringify(payload.rowData);
        const vectorClockStr = typeof payload.vectorClock === 'string' ? payload.vectorClock : JSON.stringify(payload.vectorClock);

        logMeshOperation(db, {
          id: payload.id,
          operation: payload.operation,
          tableName: payload.tableName,
          rowData: rowDataStr,
          vectorClock: vectorClockStr,
          peerId: payload.peerId
        });
      })();
    } catch (err) {
      return { applied: false, error: err.message };
    }

    // Step 5: Merge remote vector clock
    if (this.vectorClock && payload.vectorClock) {
      this.vectorClock.merge(payload.vectorClock);
    }

    // Step 6: Automatically capture replication metric for actual gossip/sync activity
    const latency = envelope?.timestamp ? Math.max(0, Date.now() - envelope.timestamp) : 2;
    this._recordReplicationMetricSafe(db, {
      source_peer_id: sourcePeer,
      target_peer_id: this.peerId,
      latency_ms: latency,
      status: 'success',
      vector_clock_meta: payload.vectorClock,
      bytes_transferred: JSON.stringify(envelope || {}).length
    });

    return { applied: true };
  }

  /**
   * Helper to safely record replication conflict if table exists.
   * @private
   */
  _recordConflictSafe(db, { conflict_type, affected_entity_type, affected_entity_id, peer_id, details }) {
    try {
      const hasConflicts = db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='conflicts'").get();
      if (!hasConflicts) return;
      const id = `conf-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
      const now = new Date().toISOString();
      const detailsJson = JSON.stringify(details || {});
      db.prepare(`
        INSERT INTO conflicts (id, conflict_type, affected_entity_type, affected_entity_id, peer_id, details_json, resolution_state, created_at)
        VALUES (?, ?, ?, ?, ?, ?, 'detected', ?)
      `).run(id, conflict_type, affected_entity_type, affected_entity_id, peer_id, detailsJson, now);

      const hasAudit = db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='audit_events'").get();
      if (hasAudit) {
        db.prepare(`
          INSERT INTO audit_events (id, entity_type, entity_id, action, actor, details_json, timestamp)
          VALUES (?, ?, ?, ?, ?, ?, ?)
        `).run(`aud-${id}`, 'conflict', id, 'CONFLICT_DETECTED', peer_id || 'replication_engine', detailsJson, now);
      }
    } catch {}
  }

  /**
   * Helper to safely record replication metrics during actual sync/gossip activity.
   * @private
   */
  _recordReplicationMetricSafe(db, { source_peer_id, target_peer_id, latency_ms, status, vector_clock_meta, bytes_transferred = 0 }) {
    try {
      const hasMetrics = db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='replication_metrics'").get();
      if (!hasMetrics) return;
      const id = `rep-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
      const now = new Date().toISOString();
      const metaStr = typeof vector_clock_meta === 'object' ? JSON.stringify(vector_clock_meta) : (vector_clock_meta || '{}');
      db.prepare(`
        INSERT INTO replication_metrics (id, source_peer_id, target_peer_id, latency_ms, status, vector_clock_meta, bytes_transferred, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(id, source_peer_id, target_peer_id, Math.max(0, latency_ms || 0), status || 'success', metaStr, bytes_transferred, now);
    } catch {}
  }

  /**
   * Handles a synchronization request from a remote peer by comparing clocks.
   * Returns mesh_log entries where the originating peer sequence exceeds remote knowledge.
   *
   * @param {import('better-sqlite3').Database} db - The database instance.
   * @param {Object|string} remoteClockJson - The remote peer's clock.
   * @returns {Array<Object>} Array of _mesh_log entries the remote peer is missing.
   */
  handleSyncRequest(db, remoteClockJson) {
    const remoteClock = VectorClock.fromJSON(remoteClockJson);
    const allLogs = getMeshLog(db);
    const missingLogs = [];

    for (const log of allLogs) {
      let logClock;
      try {
        logClock = typeof log.vector_clock === 'string' ? JSON.parse(log.vector_clock) : log.vector_clock;
      } catch {
        continue;
      }
      const originPeer = log.peer_id;
      const originSeq = (logClock && logClock[originPeer]) || 0;
      const remoteSeq = remoteClock.clock[originPeer] || 0;

      // If originating sequence is greater than what remote knows, remote is missing it
      if (originSeq > remoteSeq) {
        missingLogs.push(log);
      }
    }

    return missingLogs;
  }
}

/**
 * Factory creating a new SyncEngine instance for a peer.
 * @param {string} peerId - Local peer identifier.
 * @param {Object} [options={}] - Options.
 * @returns {SyncEngine} A new SyncEngine.
 */
export function createSyncEngine(peerId, options = {}) {
  return new SyncEngine(peerId, options);
}

// Module-level default sync engine instance for backward compatibility
let defaultEngine = null;

/**
 * Initializes the default sync engine.
 * @param {string} peerId - The local node's peer identifier.
 * @returns {SyncEngine} The initialized default SyncEngine.
 */
export function initSync(peerId) {
  defaultEngine = new SyncEngine(peerId);
  return defaultEngine;
}

/**
 * Retrieves the current state of the default local vector clock.
 * @returns {VectorClock} The local vector clock.
 */
export function getVectorClock() {
  if (!defaultEngine) {
    defaultEngine = new SyncEngine('local');
  }
  return defaultEngine.getVectorClock();
}

/**
 * Records a local write using the default sync engine.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {string} operation - The operation type (INSERT, UPDATE, DELETE).
 * @param {string} tableName - The table modified.
 * @param {Object} rowData - The data modified.
 * @param {string} [peerId] - Optional peer ID override.
 * @returns {Object} An envelope-ready payload for gossip broadcast.
 */
export function recordLocalWrite(db, operation, tableName, rowData, peerId) {
  if (!defaultEngine || (peerId && defaultEngine.peerId !== peerId)) {
    defaultEngine = new SyncEngine(peerId || 'local');
  }
  return defaultEngine.recordLocalWrite(db, operation, tableName, rowData);
}

/**
 * Applies a remote transaction using the default sync engine.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {Object} envelope - The gossiped transaction envelope.
 * @param {string} [_peerId] - Unused in new design; preserved for compatibility.
 * @returns {Object}
 */
export function applyRemoteTransaction(db, envelope, _peerId) {
  if (!defaultEngine) {
    defaultEngine = new SyncEngine('local');
  }
  return defaultEngine.applyRemoteTransaction(db, envelope);
}

/**
 * Handles a synchronization request using the default sync engine.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {Object|string} remoteClockJson - The remote peer's clock.
 * @returns {Array<Object>}
 */
export function handleSyncRequest(db, remoteClockJson) {
  if (!defaultEngine) {
    defaultEngine = new SyncEngine('local');
  }
  return defaultEngine.handleSyncRequest(db, remoteClockJson);
}
