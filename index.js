/**
 * @fileoverview Agentic Mesh — Main Entry Point
 * 
 * Boots all subsystems for a single mesh node:
 *   1. SQLite database (schema + seed)
 *   2. libp2p P2P node (TCP + mDNS + GossipSub)
 *   3. Vector clock sync engine
 *   4. WebSocket gateway (Express + WS)
 *   5. Interactive CLI
 * 
 * Every laptop runs this same file. Configuration via environment variables.
 * 
 * @author Advaith J
 */

import { config } from './src/utils/config.js';
import { logger } from './src/utils/logger.js';
import { createEnvelope, MessageType } from './src/utils/protocol.js';

// Database layer
import { initDatabase, getDbState, getSchema } from './src/db/sqlite.js';
import { validateTransaction } from './src/db/schema-validator.js';
import { initSync, applyRemoteTransaction, handleSyncRequest, getVectorClock, recordLocalWrite } from './src/db/sync.js';

// P2P layer
import { createMeshNode, startNode, stopNode } from './src/p2p/node.js';
import { setupGossip, publishTransaction, publishSync } from './src/p2p/gossip.js';
import { PeerRegistry } from './src/p2p/discovery.js';

// Agent layer
import { routeTask, isOllamaAvailable } from './src/agents/router.js';
import { planOperations } from './src/agents/planner.js';
import { auditOperations, auditRecentTransactions } from './src/agents/validator.js';
import { executeOperations, executeSingleTransaction } from './src/agents/executor.js';

// Infrastructure
import { startWebSocketServer, broadcastToClients } from './src/websocket/server.js';
import { startCLI } from './src/api/routes.js';

import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

// ─────────────────────────────────────────────────────────────────────────────
// BOOT SEQUENCE
// ─────────────────────────────────────────────────────────────────────────────

async function main() {
  try {
    logger.info('═══════════════════════════════════════════════');
    logger.info('  AGENTIC MESH — Autonomous P2P Database Node ');
    logger.info('═══════════════════════════════════════════════');
    logger.info(`Node Name: ${config.NODE_NAME}`);
    logger.info(`P2P Port:  ${config.P2P_PORT}`);
    logger.info(`WS Port:   ${config.WS_PORT}`);
    logger.info(`DB Path:   ${config.DB_PATH}`);
    logger.info(`AI Model:  ${config.OLLAMA_MODEL}`);
    logger.info('');

    // ── Step 1: Initialize SQLite database ──────────────────────────────────
    logger.db('Initializing SQLite database...');
    
    // Ensure the data directory exists
    try {
      mkdirSync(dirname(config.DB_PATH), { recursive: true });
    } catch (e) {
      // Directory may already exist — that's fine
    }

    const db = initDatabase(config.DB_PATH);
    const state = getDbState(db);
    logger.db(`Database ready — ${state.categories.length} categories, ${state.items.length} items, ${state.suppliers.length} suppliers`);

    // ── Step 2: Create and start libp2p node ────────────────────────────────
    logger.p2p('Creating libp2p mesh node...');
    const node = await createMeshNode(config.P2P_PORT);
    await startNode(node);

    const peerId = node.peerId.toString();
    logger.p2p(`Peer ID: ${peerId}`);

    // ── Step 3: Initialize vector clock sync ────────────────────────────────
    logger.info('Initializing vector clock sync engine...');
    initSync(peerId);
    logger.info(`Vector clock initialized for ${peerId.slice(0, 12)}...`);

    // ── Step 4: Setup peer discovery registry ───────────────────────────────
    const peerRegistry = new PeerRegistry();
    peerRegistry.setupDiscovery(node);

    // Forward peer events to WebSocket (wss will be set after step 5)
    let wss = null;

    peerRegistry.on('peer:joined', (info) => {
      logger.p2p(`Peer joined: ${info.peerId.slice(0, 12)}...`);
      if (wss) {
        broadcastToClients(wss, {
          type: 'peer:joined',
          data: info,
          timestamp: Date.now(),
          source: config.NODE_NAME
        });
      }
    });

    peerRegistry.on('peer:left', (info) => {
      logger.p2p(`Peer left: ${info.peerId.slice(0, 12)}...`);
      if (wss) {
        broadcastToClients(wss, {
          type: 'peer:left',
          data: info,
          timestamp: Date.now(),
          source: config.NODE_NAME
        });
      }
    });

    // ── Step 5: Setup GossipSub message handlers ────────────────────────────
    logger.p2p('Setting up GossipSub message handlers...');

    /**
     * Helper to broadcast a transaction envelope to the mesh.
     * @param {Object} envelope - The transaction envelope to broadcast.
     */
    const broadcastTx = async (envelope) => {
      await publishTransaction(node, envelope);
      if (wss) {
        broadcastToClients(wss, {
          type: 'tx:committed',
          data: envelope.payload,
          timestamp: Date.now(),
          source: config.NODE_NAME
        });
      }
    };

    setupGossip(node, {
      /**
       * Handle incoming TRANSACTION messages from peers.
       * Applies the remote write to local SQLite via the sync engine.
       */
      onTransaction: (envelope, fromPeer) => {
        logger.p2p(`Processing TRANSACTION from ${fromPeer.slice(0, 12)}...`);
        
        const result = applyRemoteTransaction(db, envelope, peerId);
        
        if (result.applied) {
          logger.db(`Replicated remote transaction to local DB`);
          if (wss) {
            broadcastToClients(wss, {
              type: 'tx:replicated',
              data: { from: fromPeer, payload: envelope.payload },
              timestamp: Date.now(),
              source: config.NODE_NAME
            });
          }
        } else if (result.conflict) {
          logger.error(`Conflict on remote transaction: ${result.errors?.join(', ')}`);
          if (wss) {
            broadcastToClients(wss, {
              type: 'tx:conflict',
              data: { from: fromPeer, errors: result.errors },
              timestamp: Date.now(),
              source: config.NODE_NAME
            });
          }
        }
        // else: already applied (duplicate) — silently skip
      },

      /**
       * Handle SYNC_REQUEST — a new peer wants to catch up.
       * Send them all the mesh log entries they're missing.
       */
      onSyncRequest: async (envelope, fromPeer) => {
        logger.p2p(`Sync request from ${fromPeer.slice(0, 12)}...`);
        
        const remoteClock = envelope.payload?.vectorClock || {};
        const missingLogs = handleSyncRequest(db, remoteClock);
        
        logger.p2p(`Sending ${missingLogs.length} missing entries to peer`);
        
        const syncResponse = createEnvelope(
          MessageType.SYNC_RESPONSE,
          { logs: missingLogs, vectorClock: getVectorClock().toJSON() },
          peerId,
          getVectorClock().toJSON()
        );
        
        await publishSync(node, syncResponse);
      },

      /**
       * Handle SYNC_RESPONSE — we received catch-up data from a peer.
       * Replay missing operations in order.
       */
      onSyncResponse: (envelope, fromPeer) => {
        logger.p2p(`Sync response from ${fromPeer.slice(0, 12)}... with ${envelope.payload?.logs?.length || 0} entries`);
        
        const logs = envelope.payload?.logs || [];
        let applied = 0;
        
        for (const log of logs) {
          // Reconstruct a minimal envelope for applyRemoteTransaction
          const txEnvelope = {
            payload: {
              id: log.id,
              operation: log.operation,
              tableName: log.table_name,
              rowData: log.row_data,
              vectorClock: log.vector_clock,
              peerId: log.peer_id
            }
          };
          
          const result = applyRemoteTransaction(db, txEnvelope, peerId);
          if (result.applied) applied++;
        }
        
        logger.db(`Sync complete — applied ${applied}/${logs.length} entries`);
        
        if (wss) {
          broadcastToClients(wss, {
            type: 'sync:completed',
            data: { from: fromPeer, applied, total: logs.length },
            timestamp: Date.now(),
            source: config.NODE_NAME
          });
        }
      },

      /**
       * Handle AI_AUDIT messages — background audit results shared by peers.
       */
      onAiAudit: (envelope, fromPeer) => {
        logger.ai(`Received AI audit from ${fromPeer.slice(0, 12)}...`);
        if (wss) {
          broadcastToClients(wss, {
            type: 'ai:completed',
            data: envelope.payload,
            timestamp: Date.now(),
            source: config.NODE_NAME
          });
        }
      }
    });

    // ── Step 6: Start WebSocket gateway ─────────────────────────────────────
    logger.info('Starting WebSocket gateway...');
    const wsResult = startWebSocketServer(config.WS_PORT, {
      node,
      db,
      peerRegistry,
      executeSingleTransaction: (payload) => executeSingleTransaction(payload.operation, payload.table, payload.data, db, peerId, broadcastTx),
      planOperations: (nlRequest) => planOperations(nlRequest, db),
      executeOperations: (operations) => executeOperations(operations, db, peerId, broadcastTx),
      auditRecentTransactions: (count) => auditRecentTransactions(db, count)
    });
    wss = wsResult.wss;

    // ── Step 7: Check Ollama availability ───────────────────────────────────
    const ollamaUp = await isOllamaAvailable();
    if (ollamaUp) {
      logger.ai(`Ollama connected at ${config.OLLAMA_HOST} (model: ${config.OLLAMA_MODEL})`);
    } else {
      logger.ai(`Ollama not available at ${config.OLLAMA_HOST} — AI features disabled (fast-path still works)`);
    }

    // ── Step 8: Start interactive CLI ───────────────────────────────────────
    logger.info('Starting interactive CLI...');
    
    startCLI({
      node,
      db,
      peerRegistry,
      wss: wsResult.wss,

      // Bind broadcastTransaction to use our helper
      broadcastTransaction: broadcastTx,

      // Bind executeOperations with db, peerId, and broadcastFn curried
      executeOperations: (operations) => executeOperations(operations, db, peerId, broadcastTx),

      // Bind planOperations with db curried
      planOperations: (nlRequest) => planOperations(nlRequest, db),

      // Bind auditRecentTransactions with db curried
      auditRecentTransactions: (count) => auditRecentTransactions(db, count),

      // Pass routeTask directly
      routeTask: (input) => routeTask(input, db),

      // Bind executeSingleTransaction — the CLI passes a { table, operation, data } object
      executeSingleTransaction: async (payload) => {
        return executeSingleTransaction(payload.operation, payload.table, payload.data, db, peerId, broadcastTx);
      }
    });

    // ── Step 9: Register graceful shutdown handlers ──────────────────────────
    const shutdown = async () => {
      logger.info('Shutting down gracefully...');
      try {
        await stopNode(node);
        db.close();
        wsResult.server.close();
        logger.info('All subsystems stopped. Goodbye.');
      } catch (err) {
        logger.error(`Error during shutdown: ${err.message}`);
      }
      process.exit(0);
    };

    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);

    logger.success('═══════════════════════════════════════════════');
    logger.success('  All systems online. Node is ready.          ');
    logger.success('═══════════════════════════════════════════════');

  } catch (error) {
    logger.error(`Fatal error during boot: ${error.message}`);
    console.error(error);
    process.exit(1);
  }
}

// Boot the node
main();
