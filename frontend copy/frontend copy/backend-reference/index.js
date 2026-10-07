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
import { initDatabase, getDbState } from './src/db/sqlite.js';
import { createSyncEngine, initSync } from './src/db/sync.js';

// P2P layer
import { createMeshNode, startNode, stopNode } from './src/p2p/node.js';
import { setupGossip, publishTransaction, publishSync } from './src/p2p/gossip.js';
import { PeerRegistry, keepBootstrapPeers } from './src/p2p/discovery.js';
import { setupPresence } from './src/p2p/presence.js';

// Agent layer
import { routeTask, isOllamaAvailable } from './src/agents/router.js';
import { planOperations } from './src/agents/planner.js';
import { auditRecentTransactions } from './src/agents/validator.js';
import { executeOperations, executeSingleTransaction, commitProposal, rejectProposal } from './src/agents/executor.js';
import { ProposalStore } from './src/agents/proposals.js';
import { chatWithAgent, chatWithAgentStream, classifyPrompt, ExecutionTier } from './src/agents/agent-chat.js';
import { createRemotePlanner } from './src/mesh/remote-planner.js';
import { decomposeTask } from './src/mesh/decompose.js';
import { createExecutionDesk } from './src/mesh/execution-desk.js';
import { prewarmModel, getLoadedModels } from './src/agents/ollama.js';
import { ActivityTracker, createSystemSampler } from './src/agents/activity.js';
import { createInstrumentation } from './src/agents/instrumented.js';

// Visual lane
import { createVisualLane } from './src/vision/visual-lane.js';
import { Origin } from './src/db/policy.js';

// Infrastructure
import { startWebSocketServer, broadcastToClients } from './src/websocket/server.js';
import { startCLI } from './src/api/routes.js';

import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

// ─────────────────────────────────────────────────────────────────────────────
// BOOT SEQUENCE
// ─────────────────────────────────────────────────────────────────────────────

async function main() {
  const bootStartTime = Date.now();
  try {
    logger.info('═══════════════════════════════════════════════');
    logger.info('  AGENTIC MESH — Autonomous P2P Database Node ');
    logger.info('═══════════════════════════════════════════════');
    logger.info(`Node Name: ${config.NODE_NAME}`);
    logger.info(`Node Role: ${config.NODE_ROLE}`);
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
    } catch (_e) {
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
    const syncEngine = createSyncEngine(peerId);
    initSync(peerId); // Keep default engine synced as well
    logger.info(`Vector clock initialized for ${peerId.slice(0, 12)}...`);

    // ── Step 4: Setup peer discovery registry ───────────────────────────────
    const peerRegistry = new PeerRegistry();
    peerRegistry.setupDiscovery(node);
    if (config.BOOTSTRAP_PEERS.length) {
      logger.p2p(`Keeping fixed peers connected: ${config.BOOTSTRAP_PEERS.join(', ')}`);
      keepBootstrapPeers(node, config.BOOTSTRAP_PEERS);
    }

    // Forward peer events to WebSocket (wss will be set after step 5)
    let wss = null;

    // Agent activity: every agent hand-off, task progress and performance → dashboard
    const emitToDashboard = (type, data) => {
      if (wss) broadcastToClients(wss, { type, data, timestamp: Date.now(), source: config.NODE_NAME });
    };
    const activity = new ActivityTracker({ emit: emitToDashboard });
    const track = createInstrumentation(activity, {
      db,
      peerCount: () => (typeof node.getPeers === 'function' ? node.getPeers().length : 0)
    });

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

    peerRegistry.on('peer:connected', async (info) => {
      logger.p2p(`Peer connected: ${info.peerId.slice(0, 12)}...`);
      if (wss) {
        broadcastToClients(wss, {
          type: 'peer:connected',
          data: info,
          timestamp: Date.now(),
          source: config.NODE_NAME
        });
      }
      try {
        await triggerSync();
      } catch (syncErr) {
        logger.debug(`Auto-sync request error: ${syncErr.message}`);
      }
    });

    peerRegistry.on('peer:updated', (info) => {
      if (wss) {
        broadcastToClients(wss, {
          type: 'peer:updated',
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

    /**
     * Helper to trigger a SYNC_REQUEST with our current vector clock.
     */
    const triggerSync = async () => {
      const clock = syncEngine.getVectorClock() ? syncEngine.getVectorClock().toJSON() : {};
      const syncReq = createEnvelope(
        MessageType.SYNC_REQUEST,
        { vectorClock: clock },
        peerId,
        clock
      );
      await publishSync(node, syncReq);
      logger.p2p('Broadcasted SYNC_REQUEST to mesh');
    };

    setupGossip(node, {
      /**
       * Handle incoming TRANSACTION messages from peers.
       * Applies the remote write to local SQLite via the sync engine.
       */
      onTransaction: (envelope, fromPeer) => {
        logger.p2p(`Processing TRANSACTION from ${fromPeer.slice(0, 12)}...`);
        
        const result = syncEngine.applyRemoteTransaction(db, envelope);
        track.remoteTransaction(fromPeer, envelope.payload, result);
        
        if (result.applied) {
          logger.db(`Replicated remote transaction to local DB (${envelope.payload?.operation} on ${envelope.payload?.tableName})`);
          if (wss) {
            broadcastToClients(wss, {
              type: 'tx:replicated',
              data: { from: fromPeer, payload: envelope.payload },
              timestamp: Date.now(),
              source: config.NODE_NAME
            });
          }
        } else if (result.duplicate) {
          logger.p2p(`Skipped duplicate transaction ${envelope.payload?.id?.slice(0, 8)}...`);
        } else if (result.conflict) {
          logger.warn(`Conflict on remote transaction: ${result.errors?.join(', ')}`);
          if (wss) {
            broadcastToClients(wss, {
              type: 'tx:conflict',
              data: { from: fromPeer, errors: result.errors, payload: envelope.payload },
              timestamp: Date.now(),
              source: config.NODE_NAME
            });
          }
        }
      },

      /**
       * Handle SYNC_REQUEST — a new peer wants to catch up.
       * Send them all the mesh log entries they're missing.
       */
      onSyncRequest: async (envelope, fromPeer) => {
        logger.p2p(`Sync request from ${fromPeer.slice(0, 12)}...`);
        
        const remoteClock = envelope.payload?.vectorClock || envelope.vectorClock || {};
        const missingLogs = syncEngine.handleSyncRequest(db, remoteClock);
        
        logger.p2p(`Sending ${missingLogs.length} missing entries to peer`);
        track.syncRequest(fromPeer, missingLogs.length);
        
        const syncResponse = createEnvelope(
          MessageType.SYNC_RESPONSE,
          { logs: missingLogs, vectorClock: syncEngine.getVectorClock().toJSON() },
          peerId,
          syncEngine.getVectorClock().toJSON()
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
        let duplicates = 0;
        let conflicts = 0;
        
        for (const log of logs) {
          const txEnvelope = {
            payload: {
              id: log.id,
              operation: log.operation,
              tableName: log.table_name,
              rowData: log.row_data,
              vectorClock: log.vector_clock,
              peerId: log.peer_id,
              origin: log.origin,
              evidenceId: log.evidence_id
            }
          };
          
          const result = syncEngine.applyRemoteTransaction(db, txEnvelope);
          if (result.applied) applied++;
          else if (result.duplicate) duplicates++;
          else if (result.conflict) conflicts++;
        }
        
        logger.db(`Sync catch-up complete — applied: ${applied}, duplicates: ${duplicates}, conflicts: ${conflicts} (total: ${logs.length})`);
        track.syncResponse(fromPeer, { applied, duplicates, conflicts, total: logs.length });
        
        if (wss) {
          broadcastToClients(wss, {
            type: 'sync:completed',
            data: { from: fromPeer, applied, duplicates, conflicts, total: logs.length },
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

    // Nodes announce their name/role/model so dashboards can show who is who
    // Executor ↔ Planner: plan requests go to a planner node on the mesh
    const remotePlanner = createRemotePlanner({
      pubsub: node.services.pubsub,
      selfId: peerId,
      selfName: config.NODE_NAME,
      getPeers: () => (config.PLANNER_MODE === 'local' ? [] : peerRegistry.getPeers()),
      timeoutMs: config.REMOTE_PLAN_TIMEOUT_MS,
      preferredPeer: config.PLANNER_PEER
    });

    // Planner → Executor hand-offs (EXECUTION_REQUEST); assigned once proposals exist (step 5b)
    let executionDesk = null;

    const samplePresenceCpu = createSystemSampler();
    setupPresence(node, peerRegistry, () => ({
      name: config.NODE_NAME,
      role: config.NODE_ROLE,
      model: config.OLLAMA_MODEL,
      visionModel: config.VISION_MODEL,
      wsPort: config.WS_PORT,
      cpuPercent: samplePresenceCpu().cpuPercent
    }), {
      // Other control traffic from peers (e.g. PLAN_REQUEST / EXECUTION_REQUEST) → Agent Activity feed
      onControlMessage: (type, fromPeer, envelope) => {
        if (remotePlanner.handle(type, fromPeer, envelope)) return; // shown by the mesh-plan task
        if (type === 'EXECUTION_REQUEST' && executionDesk) {
          executionDesk.handle(type, fromPeer, envelope).catch(err => logger.error(`Execution request failed: ${err.message}`));
          return;
        }
        const name = peerRegistry.getPeers().find(p => p.peerId === fromPeer)?.name || `peer ${fromPeer.slice(0, 8)}…`;
        const detail = envelope.payload?.prompt || envelope.payload?.request || envelope.payload?.summary || '';
        activity.message({ from: 'mesh', to: 'mesh', summary: `${name} → ${type}${detail ? `: ${String(detail).slice(0, 120)}` : ''}`, meta: { type, fromPeer } });
      }
    });

    // ── Step 5b: Visual lane + proposals awaiting approval ──────────────────
    const proposalStore = new ProposalStore();
    const visualLane = track.vision(createVisualLane({ db, syncEngine, proposalStore, broadcastFn: broadcastTx, emit: emitToDashboard }));
    const nameOf = (id) => peerRegistry.getPeers().find(p => p.peerId === id)?.name || `peer ${id.slice(0, 8)}…`;
    executionDesk = createExecutionDesk({
      pubsub: node.services.pubsub,
      selfId: peerId,
      selfName: config.NODE_NAME,
      db,
      proposalStore,
      nameOf,
      onEvent: (kind, data) => {
        const ops = data.proposal?.operations || data.operations || [];
        if (kind === 'pending') {
          activity.record('validator', true, 0);
          activity.message({ from: 'planner', to: 'executor', summary: `EXECUTION_REQUEST from ${data.planner}: ${ops.length} op(s)${data.proposal?.request ? ` for "${data.proposal.request.slice(0, 80)}"` : ''}` });
          activity.message({ from: 'validator', to: 'user', summary: `Plan from ${data.planner} is valid — proposal ${data.proposal.id} awaiting your approval` });
          emitToDashboard('proposal:created', data.proposal);
        } else if (kind === 'rejected' && !data.proposalId) {
          activity.record('validator', true, 0);
          activity.message({ from: 'validator', to: 'planner', summary: `Rejected EXECUTION_REQUEST from ${data.planner}: ${(data.errors || [])[0]}`, status: 'warn' });
        } else {
          activity.message({ from: 'executor', to: 'planner', summary: `EXECUTION_RESULT → ${data.planner}: ${kind}${data.errors?.length ? ` (${data.errors[0]})` : ''}`, status: kind === 'committed' ? 'ok' : 'warn' });
        }
      }
    });
    const proposals = {
      list: (filter) => proposalStore.list(filter),
      approve: track.approveProposal(async (id) => {
        const result = await commitProposal(id, db, proposalStore, syncEngine, broadcastTx);
        emitToDashboard(result.success ? 'proposal:committed' : 'proposal:failed', result.proposal || { id, errors: result.errors });
        await executionDesk.notifyDecision(id, result, 'approved');
        return result;
      }),
      reject: (id, reason) => {
        const result = rejectProposal(id, proposalStore, reason);
        if (result.success) {
          emitToDashboard('proposal:rejected', result.proposal);
          executionDesk.notifyDecision(id, { success: false, errors: reason ? [reason] : [] }, 'rejected');
        }
        return result;
      }
    };
    // Operations planned by the LLM from natural language are tagged 'nl' in _mesh_log
    const executeNlOperations = track.executeBatch((operations) => executeOperations(operations, db, syncEngine, broadcastTx, { origin: Origin.NL }));
    const planLocal = track.planner((nlRequest) => planOperations(nlRequest, db));
    const planNl = track.meshPlanner(remotePlanner, planLocal, {
      maxRounds: config.PLAN_MAX_ROUNDS,
      // Big tasks: this executor's local LLM splits them so the planner can go step by step
      decompose: (request) => decomposeTask(request)
    });

    // Chat: database-change requests go to the planner node; everything else stays local
    const chatStreamLocal = track.chatStream((message, history, _db, options) => chatWithAgentStream(message, history, db, options));
    const chatStreamMesh = async function* (message, history, _db, options) {
      const wantsChange = classifyPrompt(message, 'fast') === ExecutionTier.HEAVY_AI;
      if (!wantsChange || !remotePlanner.pickPlanner()) {
        yield* chatStreamLocal(message, history, _db, options);
        return;
      }
      const plan = await planNl(message);
      if (!plan.success) {
        yield { type: 'error', message: plan.error || 'Planning failed', operations: null, modelAvailable: true };
        return;
      }
      const by = plan.plannedBy?.startsWith('local') ? 'this node (planner node unavailable)' : `planner node "${plan.plannedBy}"`;
      yield {
        type: 'done',
        message: `Planned by ${by}${plan.steps ? ` in ${plan.steps.length} steps` : ''}${plan.rounds > 1 ? ` over ${plan.rounds} executor ↔ planner rounds` : ''}${plan.planMs ? ` (${(plan.planMs / 1000).toFixed(1)}s)` : ''}. The executor's validator accepted it — approve to execute on "${config.NODE_NAME}".`,
        operations: plan.operations,
        modelAvailable: true
      };
    };
    const executeFastPath = track.fastPath((payload) => executeSingleTransaction(payload, db, syncEngine, broadcastTx));

    // Performance snapshot: agent success rates + node CPU/memory + LLM processor
    const sampleSystem = createSystemSampler();
    let llmInfo = { host: config.OLLAMA_HOST, model: config.OLLAMA_MODEL, visionModel: config.VISION_MODEL, processor: 'unknown', loaded: [] };
    const refreshLlmInfo = async () => {
      const loaded = await getLoadedModels();
      const current = loaded.find(m => (m.name || m.model) === config.OLLAMA_MODEL) || loaded[0];
      let processor = loaded.length ? 'CPU' : 'not loaded';
      if (current?.size && current.size_vram) {
        const gpu = Math.round((current.size_vram / current.size) * 100);
        processor = gpu >= 100 ? '100% GPU' : `${gpu}% GPU / ${100 - gpu}% CPU`;
      } else if (current) {
        processor = '100% CPU';
      }
      llmInfo = { ...llmInfo, processor, loaded: loaded.map(m => m.name || m.model) };
    };
    const getSystemStats = () => ({ ...sampleSystem(), peers: node.getPeers().length, llm: llmInfo });
    let statsTick = 0;
    setInterval(() => {
      if (statsTick++ % 5 === 0) refreshLlmInfo().catch(() => {});
      emitToDashboard('agent:stats', { agents: activity.agentStats(), system: getSystemStats() });
    }, 3000).unref();
    logger.info(`Visual lane ${config.VISION_ENABLED ? `enabled (vision model: ${config.VISION_MODEL})` : 'disabled'}`);

    // ── Step 6: Start WebSocket gateway ─────────────────────────────────────
    logger.info('Starting WebSocket gateway...');
    const wsResult = startWebSocketServer(config.WS_PORT, {
      node,
      db,
      peerRegistry,
      executeSingleTransaction: executeFastPath,
      planOperations: planNl,
      executeOperations: executeNlOperations,
      auditRecentTransactions: (count) => auditRecentTransactions(db, count),
      chatWithAgent: track.chat((message, history, _db, options) => chatWithAgent(message, history, db, options)),
      chatWithAgentStream: chatStreamMesh,
      requestSync: triggerSync,
      visualLane,
      proposals,
      activity,
      getSystemStats
    });
    wss = wsResult.wss;

    // ── Step 7: Check Ollama availability and prewarm model ──────────────────
    const ollamaUp = await isOllamaAvailable();
    if (ollamaUp) {
      logger.ai(`Ollama connected at ${config.OLLAMA_HOST} (model: ${config.OLLAMA_MODEL})`);
      try {
        await prewarmModel(bootStartTime);
      } catch (warmupErr) {
        logger.warn(`Model prewarm notice: ${warmupErr.message}`);
      }
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

      // Bind executeOperations with db, syncEngine, and broadcastFn curried
      executeOperations: executeNlOperations,

      // Bind planOperations with db curried
      planOperations: planNl,

      // Bind auditRecentTransactions with db curried
      auditRecentTransactions: (count) => auditRecentTransactions(db, count),

      // Pass routeTask directly
      routeTask: (input) => routeTask(input, db),

      // Bind executeSingleTransaction — the CLI passes a { table, operation, data } object
      executeSingleTransaction: executeFastPath,

      requestSync: triggerSync,

      visualLane,
      proposals
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
