/**
 * @fileoverview Agentic Mesh — Main Entry Point (Planner Node "delta")
 * 
 * Boots all subsystems for node "delta" (dedicated PLANNER):
 *   1. SQLite database (schema + seed)
 *   2. libp2p P2P node (TCP + mDNS + GossipSub)
 *   3. Vector clock sync engine
 *   4. Peer discovery registry + bootstrap peering
 *   5. ActivityTracker & Instrumentation for all agent hops
 *   6. Presence broadcasting with live CPU/RAM
 *   7. Plan-service for executor nodes (e.g. alpha)
 *   8. Planner-to-Executor handoff desk for local prompts
 *   9. WebSocket gateway (Express + WS) serving React dashboard
 *  10. Interactive CLI
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
import { setupGossip, publishTransaction, publishSync, publishControl } from './src/p2p/gossip.js';
import { PeerRegistry, keepBootstrapPeers } from './src/p2p/discovery.js';
import { setupPresence, infoFromAnnounce } from './src/p2p/presence.js';

// Agent layer
import { routeTask, isOllamaAvailable } from './src/agents/router.js';
import { planOperations } from './src/agents/planner.js';
import { auditRecentTransactions } from './src/agents/validator.js';
import { executeSingleTransaction, commitProposal, rejectProposal } from './src/agents/executor.js';
import { ProposalStore } from './src/agents/proposals.js';
import { chatWithAgent, chatWithAgentStream, classifyPrompt, ExecutionTier } from './src/agents/agent-chat.js';
import { prewarmModel, getLoadedModels } from './src/agents/ollama.js';
import { ActivityTracker, createSystemSampler } from './src/agents/activity.js';
import { createInstrumentation } from './src/agents/instrumented.js';
import { createRemoteExecutor } from './src/mesh/remote-executor.js';

import { registerNode } from './src/mesh/workflow-service.js';

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
      // Directory may already exist
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
    initSync(peerId);
    logger.info(`Vector clock initialized for ${peerId.slice(0, 12)}...`);

    // ── Step 4: Setup peer discovery registry & fixed bootstrap peers ───────
    const peerRegistry = new PeerRegistry();
    peerRegistry.setupDiscovery(node);
    if (config.BOOTSTRAP_PEERS && config.BOOTSTRAP_PEERS.length) {
      logger.p2p(`Keeping bootstrap peers connected: ${config.BOOTSTRAP_PEERS.join(', ')}`);
      keepBootstrapPeers(node, config.BOOTSTRAP_PEERS);
    }

    // Register this node in the distributed mesh_nodes table
    registerNode(db, {
      id: `node-${config.NODE_NAME}`,
      name: config.NODE_NAME,
      role: config.NODE_ROLE,
      peer_id: peerId,
      status: 'active',
      address: `127.0.0.1:${config.P2P_PORT}`,
      software_version: '2.0.0'
    }, {
      models: config.OLLAMA_MODEL,
      tools: 'planOperations, schemaIntrospect, routeTask, executeMutation',
      max_concurrency: 4,
      supported_task_types: 'planning, reasoning, verification, execution'
    });

    // Forward peer events to WebSocket (wss will be set after server creation)
    let wss = null;

    const emitToDashboard = (type, data) => {
      if (wss) broadcastToClients(wss, { type, data, timestamp: Date.now(), source: config.NODE_NAME });
    };

    // Agent activity tracker: every agent hand-off, task progress and stats → dashboard
    const activity = new ActivityTracker({ emit: emitToDashboard });
    const track = createInstrumentation(activity, {
      db,
      peerCount: () => (typeof node.getPeers === 'function' ? node.getPeers().length : peerRegistry.getPeerCount())
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
      const nodeName = (info.name && info.name !== 'unknown') ? info.name : (info.nodeName && info.nodeName !== 'unknown' ? info.nodeName : null);
      if (nodeName) {
        try {
          registerNode(db, {
            id: `node-${nodeName}`,
            name: nodeName,
            role: info.role || 'executor',
            peer_id: info.peerId,
            status: (info.status === 'connected' || !info.status) ? 'active' : info.status,
            address: info.addrs?.[0] || 'remote',
            software_version: '2.0.0'
          }, {
            models: info.model || 'qwen2.5-coder:1.5b',
            tools: 'executeMutation, verifyExecution',
            max_concurrency: 4,
            supported_task_types: 'execution, validation'
          });
          if (wss) {
            broadcastToClients(wss, {
              type: 'node:updated',
              data: { name: nodeName, role: info.role || 'executor' }
            });
          }
        } catch (err) {
          logger.debug(`Failed to register peer node: ${err.message}`);
        }
      }
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

    // ── Step 5: Setup broadcast helpers & proposals store ───────────────────
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

    const proposalStore = new ProposalStore();
    const proposals = {
      list: (filter) => proposalStore.list(filter),
      approve: track.approveProposal(async (id) => {
        const result = await commitProposal(id, db, proposalStore, syncEngine, broadcastTx);
        emitToDashboard(result.success ? 'proposal:committed' : 'proposal:failed', result.proposal || { id, errors: result.errors });
        return result;
      }),
      reject: (id, reason) => {
        const result = rejectProposal(id, proposalStore, reason);
        if (result.success) {
          emitToDashboard('proposal:rejected', result.proposal);
        }
        return result;
      }
    };

    // ── Step 6: Setup Planner → Executor Remote Handoff Desk ─────────────────
    const remoteExecutor = createRemoteExecutor({
      pubsub: node.services.pubsub,
      selfId: peerId,
      selfName: config.NODE_NAME,
      db,
      peerRegistry,
      activity,
      planOperations: (prompt, database) => planOperations(prompt, database || db),
      preferredPeer: config.EXECUTOR_PEER || 'alpha'
    });

    // ── Step 7: Setup Presence (announces role="planner", model, wsPort, perf)
    const samplePresenceCpu = createSystemSampler();
    setupPresence(node, peerRegistry, () => ({
      name: config.NODE_NAME,
      role: config.NODE_ROLE,
      model: config.OLLAMA_MODEL,
      visionModel: config.VISION_MODEL,
      wsPort: config.WS_PORT,
      cpuPercent: samplePresenceCpu().cpuPercent
    }), {
      onControlMessage: (type, fromPeer, envelope) => {
        if (type === 'EXECUTION_RESULT') {
          remoteExecutor.handle(type, fromPeer, envelope);
          return;
        }
        const name = peerRegistry.getPeers().find(p => p.peerId === fromPeer)?.name || `peer ${fromPeer.slice(0, 8)}…`;
        const detail = envelope.payload?.prompt || envelope.payload?.request || envelope.payload?.summary || '';
        activity.message({ from: 'mesh', to: 'mesh', summary: `${name} → ${type}${detail ? `: ${String(detail).slice(0, 120)}` : ''}`, meta: { type, fromPeer } });
      }
    });

    // ── Step 8: Setup GossipSub message handlers ────────────────────────────
    logger.p2p('Setting up GossipSub message handlers...');

    setupGossip(node, {
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
      },

      onPeerAnnounce: (envelope, fromPeer) => {
        const info = infoFromAnnounce(envelope.payload);
        peerRegistry.setInfo(fromPeer, { ...info, source: 'presence' });
      },

      // Handle PLAN_REQUEST — received from executor nodes (e.g. alpha)
      onPlanRequest: async (envelope, fromPeer) => {
        const payload = envelope.payload || {};
        // Only answer requests whose targetPeerId is delta (or missing)
        if (payload.targetPeerId && payload.targetPeerId !== peerId) {
          logger.debug(`[Planner] Ignoring PLAN_REQUEST intended for ${payload.targetPeerId}`);
          return;
        }

        const requester = payload.fromPeerId || payload.requesterPeerId || envelope.sender || fromPeer;
        const requesterName = payload.nodeName || peerRegistry.getPeers().find(p => p.peerId === requester)?.name || 'alpha';
        const requestId = payload.requestId || envelope.id;
        const prompt = payload.prompt;

        if (!requestId || !prompt) {
          logger.warn(`[Planner] Dropped malformed PLAN_REQUEST from ${fromPeer.slice(0, 12)}...`);
          return;
        }

        logger.ai(`[Planner] Serving PLAN_REQUEST [reqId:${requestId}] from ${requesterName}: "${prompt}"`);

        await track.planService(async (reqPrompt) => {
          return planOperations(reqPrompt, db);
        })({
          prompt,
          fromName: requesterName,
          respondFn: async (planResult) => {
            const responsePayload = {
              requestId,
              success: planResult.success,
              operations: planResult.operations || [],
              error: planResult.error || null,
              plannerPeerId: peerId,
              targetPeerId: requester,
              raw: planResult.raw || null
            };
            const respEnvelope = createEnvelope('PLAN_RESPONSE', responsePayload, peerId);
            await publishControl(node, respEnvelope);
          }
        });
      },

      onExecutionResponse: (envelope, fromPeer) => {
        remoteExecutor.handle('EXECUTION_RESULT', fromPeer, envelope);
      },

      onExecutionRequest: (envelope, fromPeer) => {
        logger.warn(`[Planner] Ignoring EXECUTION_REQUEST from ${fromPeer.slice(0, 12)}... (this node has role=planner, not executor)`);
      }
    });

    // ── Step 9: Configure Role-Aware Handlers & Chat Streaming ───────────────
    // Fast path: direct local write
    const executeFastPath = track.fastPath((payload) => executeSingleTransaction(payload, db, syncEngine, broadcastTx));

    // Local planner preview
    const planLocal = track.planner((nlRequest) => planOperations(nlRequest, db));

    // Operations planned for execution on delta are handed off to the executor
    const nodeExecuteOperations = async (operations) => {
      logger.info(`[Planner] Delegating execution of ${operations.length} operation(s) to executor node`);
      return remoteExecutor.handoffOperations(operations);
    };

    // Streaming chat: database-changing requests hand off to executor; Q&A remains local
    const chatStreamLocal = track.chatStream((message, history, _db, options) => chatWithAgentStream(message, history, db, options));
    const chatStreamMesh = async function* (message, history, _db, options) {
      const wantsChange = classifyPrompt(message, 'fast') === ExecutionTier.HEAVY_AI;
      if (!wantsChange) {
        yield* chatStreamLocal(message, history, _db, options);
        return;
      }

      const handoffResult = await remoteExecutor.handoff(message);
      if (handoffResult.noExecutor) {
        yield {
          type: 'error',
          message: 'No executor node connected — operations will not be executed locally',
          operations: null,
          modelAvailable: true
        };
        return;
      }

      if (!handoffResult.success) {
        yield {
          type: 'error',
          message: handoffResult.message || handoffResult.error || 'Execution request failed',
          operations: handoffResult.operations || null,
          modelAvailable: true
        };
        return;
      }

      yield {
        type: 'done',
        message: handoffResult.message || `Planned by delta and sent to executor "${handoffResult.executorName}" — waiting for approval on ${handoffResult.executorName} (${handoffResult.proposalId}).`,
        operations: handoffResult.operations,
        modelAvailable: true
      };
    };

    // ── Step 10: Performance Snapshot & 3-Second Stats Ticker ───────────────
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

    const getSystemStats = () => ({
      ...sampleSystem(),
      peers: peerRegistry ? peerRegistry.getPeerCount() : 0,
      llm: llmInfo
    });

    let statsTick = 0;
    setInterval(() => {
      if (statsTick++ % 5 === 0) refreshLlmInfo().catch(() => { });
      emitToDashboard('agent:stats', { agents: activity.agentStats(), system: getSystemStats() });
    }, 3000).unref();

    // Non-streaming chat: database-changing requests hand off to executor; Q&A remains local
    const chatLocal = track.chat((message, history, _db, options) => chatWithAgent(message, history, db, options));
    const chatMesh = async (message, history, _db, options) => {
      const wantsChange = classifyPrompt(message, 'fast') === ExecutionTier.HEAVY_AI;
      if (!wantsChange) {
        return chatLocal(message, history, _db, options);
      }
      const handoffResult = await remoteExecutor.handoff(message);
      if (handoffResult.noExecutor) {
        return {
          success: false,
          message: 'No executor node connected — operations will not be executed locally',
          operations: null,
          modelAvailable: true
        };
      }
      if (!handoffResult.success) {
        return {
          success: false,
          message: handoffResult.message || handoffResult.error || 'Execution request failed',
          operations: handoffResult.operations || null,
          modelAvailable: true
        };
      }
      return {
        success: true,
        message: handoffResult.message || `Planned by delta and sent to executor "${handoffResult.executorName}" — waiting for approval on ${handoffResult.executorName} (${handoffResult.proposalId}).`,
        operations: handoffResult.operations,
        modelAvailable: true
      };
    };

    // ── Step 11: Start WebSocket gateway & REST server ───────────────────────
    logger.info('Starting WebSocket gateway...');
    const wsResult = startWebSocketServer(config.WS_PORT, {
      node,
      db,
      peerRegistry,
      executeSingleTransaction: executeFastPath,
      planOperations: planLocal,
      executeOperations: nodeExecuteOperations,
      auditRecentTransactions: (count) => auditRecentTransactions(db, count),
      chatWithAgent: chatMesh,
      chatWithAgentStream: chatStreamMesh,
      requestSync: triggerSync,
      proposals,
      activity,
      getSystemStats,
      syncEngine
    });
    wss = wsResult.wss;

    // ── Step 12: Check Ollama availability and prewarm model ─────────────────
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

    // ── Step 13: Start interactive CLI ──────────────────────────────────────
    logger.info('Starting interactive CLI...');
    startCLI({
      node,
      db,
      peerRegistry,
      wss: wsResult.wss,
      broadcastTransaction: broadcastTx,
      executeOperations: nodeExecuteOperations,
      planOperations: planLocal,
      auditRecentTransactions: (count) => auditRecentTransactions(db, count),
      routeTask: (input) => routeTask(input, db),
      executeSingleTransaction: executeFastPath,
      requestSync: triggerSync,
      proposals
    });

    // ── Step 14: Register graceful shutdown handlers ─────────────────────────
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
    logger.success('  All systems online. Node delta is ready.     ');
    logger.success('═══════════════════════════════════════════════');

  } catch (error) {
    logger.error(`Fatal error during boot: ${error.message}`);
    console.error(error);
    process.exit(1);
  }
}

// Boot the node
main();
