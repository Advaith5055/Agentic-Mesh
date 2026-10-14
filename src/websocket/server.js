/**
 * @fileoverview Express + WebSocket gateway that pushes real-time events to the React dashboard.
 * Serves both core Agentic Mesh distributed workflow endpoints and legacy demo inventory endpoints.
 */

import express from 'express';
import { createServer } from 'node:http';
import { WebSocketServer } from 'ws';
import { logger } from '../utils/logger.js';
import { config } from '../utils/config.js';
import { getAgentStatus } from '../agents/ollama.js';
import {
  getMeshNodes,
  getMeshNodeById,
  getTasks,
  getTaskDetails,
  getApprovals,
  getAuditEvents,
  getConflicts,
  getReplicationMetrics,
  getOperationalMetrics,
  getTaskMessages
} from '../db/sqlite.js';
import { v4 as uuidv4 } from 'uuid';
import {
  registerNode,
  createTask,
  planTask,
  updateTaskPlanning,
  updateTaskPlanned,
  createTaskApproval,
  dispatchTask,
  approveTask,
  rejectTask,
  executeApprovedTask,
  submitApprovalDecision,
  executeTask,
  validateTaskRun,
  cancelTask,
  resolveConflict,
  recordReplicationMetric,
  recordAuditEvent,
  setWorkflowSyncEngine,
  recordWorkflowMessage,
  getWorkflowMessages,
  setWorkflowWsBroadcaster,
  TaskStatus,
  assertTaskTransition,
  applyWorkflowMutation
} from '../mesh/workflow-service.js';
import { validateModelPlan } from '../agents/model-validator.js';

/**
 * Broadcasts an event to all connected WebSocket clients.
 * @param {WebSocketServer} wss - The WebSocket server instance.
 * @param {Object} event - The event payload to broadcast.
 * @param {string} event.type - Event type.
 * @param {any} event.data - Event specific data.
 * @param {number} event.timestamp - Event timestamp.
 * @param {string} event.source - The node source.
 */
export function broadcastToClients(wss, event) {
  if (!wss || !wss.clients) return;

  const payload = JSON.stringify(event);

  // Send only to clients with readyState === WebSocket.OPEN (1)
  wss.clients.forEach((client) => {
    if (client.readyState === 1 /* WebSocket.OPEN */) {
      try {
        client.send(payload);
      } catch (err) {
        logger.error('Failed to send message to WS client', err);
      }
    }
  });
}

/**
 * Starts the Express HTTP server and WebSocket server for dashboard integration.
 * @param {number} port - The port on which the server will listen.
 * @param {Object} context - The application context.
 * @returns {Object} An object containing { server, wss }.
 */
export function startWebSocketServer(port, context) {
  const {
    node,
    db,
    peerRegistry,
    executeSingleTransaction,
    planOperations,
    executeOperations,
    auditRecentTransactions,
    chatWithAgent,
    chatWithAgentStream,
    requestSync,
    visualLane,
    proposals,
    activity,
    getSystemStats,
    syncEngine
  } = context;

  if (syncEngine) {
    setWorkflowSyncEngine(syncEngine);
  }

  const localNodeId = context.localNodeId || (context.node?.peerId ? context.node.peerId.toString() : config.NODE_NAME);
  const localNodeRole = context.localNodeRole || config.NODE_ROLE;
  const apiKey = context.apiKey || config.API_KEY || 'mesh-dev-key';
  const allowPublicRead = context.allowPublicRead !== undefined ? context.allowPublicRead : config.ALLOW_PUBLIC_READ;
  const configuredOrigins = context.corsOrigins || config.CORS_ORIGINS || [
    'http://localhost:5173',
    'http://127.0.0.1:5173',
    'http://localhost:5174',
    'http://127.0.0.1:5174',
    'http://localhost:3000',
    'http://127.0.0.1:3000',
    `http://localhost:${port}`,
    `http://127.0.0.1:${port}`
  ];

  // 1. Create Express app with JSON body parser & restricted CORS
  const app = express();
  const smallJson = express.json();
  const largeJson = express.json({ limit: Math.ceil(config.VISION_MAX_IMAGE_BYTES * 4 / 3) + 64 * 1024 });
  app.use((req, res, next) => (req.path.startsWith('/api/vision/') ? largeJson : smallJson)(req, res, next));

  // Restrict CORS to configured dashboard origins
  app.use((req, res, next) => {
    const origin = req.headers.origin;
    const isAllowedOrigin = origin && (
      configuredOrigins.includes(origin) ||
      configuredOrigins.includes('*') ||
      /^http:\/\/(localhost|127\.0\.0\.1|192\.168\.\d+\.\d+|10\.\d+\.\d+\.\d+)(:\d+)?$/.test(origin)
    );
    if (isAllowedOrigin) {
      res.header('Access-Control-Allow-Origin', origin);
    } else if (!origin) {
      res.header('Access-Control-Allow-Origin', '*');
    }
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization, x-api-key, x-request-id');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
  });

  // Authentication middleware
  const requireAuth = (req, res, next) => {
    const authHeader = req.headers.authorization;
    const apiKeyHeader = req.headers['x-api-key'];
    let token = null;

    if (apiKeyHeader) {
      token = String(apiKeyHeader).trim();
    } else if (authHeader && authHeader.startsWith('Bearer ')) {
      token = authHeader.slice(7).trim();
    }

    if (!token || token !== apiKey) {
      return res.status(401).json({
        error: 'Unauthorized: Valid API key required for workflow mutation endpoints',
        code: 'UNAUTHORIZED',
        status: 401,
        success: false,
        errors: ['Unauthorized: Valid API key required']
      });
    }
    next();
  };

  const checkReadAuth = (req, res, next) => {
    if (!allowPublicRead) {
      return requireAuth(req, res, next);
    }
    next();
  };

  // 2. HTTP Server and WebSocket holder
  const server = createServer(app);
  const wss = new WebSocketServer({ server, path: '/ws' });

  // Hook workflow engine broadcaster to WebSocket clients
  setWorkflowWsBroadcaster((type, data) => {
    broadcastToClients(wss, {
      type,
      data,
      timestamp: Date.now(),
      source: config.NODE_NAME
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // HEALTH & DISCOVERY ENDPOINTS
  // ───────────────────────────────────────────────────────────────────────────
  app.get('/api/health', async (req, res) => {
    let activeModel = null;
    try {
      const status = await getAgentStatus();
      activeModel = status.activeModel;
    } catch { }

    res.json({
      status: 'ok',
      nodeName: config.NODE_NAME,
      nodeRole: config.NODE_ROLE,
      peerId: node && node.peerId ? node.peerId.toString() : 'unknown',
      configuredModel: config.OLLAMA_MODEL,
      activeModel,
      modelName: activeModel || config.OLLAMA_MODEL,
      p2pPort: config.P2P_PORT,
      wsPort: config.WS_PORT,
      gatewayPort: config.WS_PORT,
      peersCount: peerRegistry ? peerRegistry.getPeerCount() : 0,
      visionEnabled: config.VISION_ENABLED,
      visionModel: config.VISION_MODEL,
      uptime: process.uptime()
    });
  });

  app.get('/api/agent/status', async (req, res) => {
    try {
      const status = await getAgentStatus();
      res.json(status);
    } catch (err) {
      res.status(500).json({
        success: false,
        ollamaReachable: false,
        configuredModel: config.OLLAMA_MODEL,
        activeModel: null,
        availableModels: [],
        nodeRole: config.NODE_ROLE,
        error: err.message
      });
    }
  });

  app.get('/api/planner/brain', async (req, res) => {
    try {
      const status = await getAgentStatus();
      res.json({
        success: true,
        brain: {
          nodeName: config.NODE_NAME,
          nodeRole: config.NODE_ROLE,
          isPlannerBrain: config.NODE_ROLE === 'planner',
          model: status.activeModel || config.OLLAMA_MODEL,
          host: config.OLLAMA_HOST,
          ollamaReachable: status.ollamaReachable,
          loadedModels: status.loadedModels,
          numCtx: config.OLLAMA_NUM_CTX,
          numPredict: config.OLLAMA_NUM_PREDICT,
          temperature: config.OLLAMA_TEMPERATURE,
          keepAlive: config.OLLAMA_KEEP_ALIVE,
          specialization: '3NF Relational Decomposition & Autonomous Planning',
          separationOfAuthority: {
            canPlan: true,
            canMutateSQLite: false,
            enforcedRole: 'planner'
          }
        }
      });
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  });

  app.get('/api/peers', (req, res) => {
    res.json(peerRegistry ? peerRegistry.toJSON() : []);
  });

  // ───────────────────────────────────────────────────────────────────────────
  // WORKFLOW PLATFORM: MESH NODES & CAPABILITIES
  // ───────────────────────────────────────────────────────────────────────────
  app.get('/api/nodes', checkReadAuth, (req, res) => {
    try {
      const nodes = getMeshNodes(db);
      res.json(nodes);
    } catch (err) {
      logger.error('Failed to get mesh nodes', err);
      res.status(500).json({ error: 'Failed to query nodes', code: 'INTERNAL_ERROR', status: 500, success: false, errors: [err.message] });
    }
  });

  app.get('/api/nodes/:id', checkReadAuth, (req, res) => {
    try {
      const found = getMeshNodeById(db, req.params.id);
      if (!found) {
        return res.status(404).json({ error: `Node '${req.params.id}' not found`, code: 'NOT_FOUND', status: 404, success: false, errors: [`Node '${req.params.id}' not found`] });
      }
      res.json(found);
    } catch (err) {
      res.status(500).json({ error: 'Failed to query node', code: 'INTERNAL_ERROR', status: 500, success: false, errors: [err.message] });
    }
  });

  app.post('/api/nodes/register', requireAuth, (req, res) => {
    try {
      const { node: nodeData, capabilities } = req.body || {};
      if (!nodeData || !nodeData.name || !nodeData.role || !nodeData.peer_id) {
        return res.status(400).json({ error: 'Node payload requires name, role, and peer_id.', code: 'BAD_REQUEST', status: 400, success: false, errors: ['Node payload requires name, role, and peer_id.'] });
      }
      const registered = registerNode(db, nodeData, capabilities, { syncEngine });
      broadcastToClients(wss, {
        type: 'node:registered',
        data: registered,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });
      res.json({ success: true, node: registered });
    } catch (err) {
      logger.error('Failed to register node', err);
      res.status(500).json({ error: err.message, code: 'INTERNAL_ERROR', status: 500, success: false, errors: [err.message] });
    }
  });

  // ───────────────────────────────────────────────────────────────────────────
  // WORKFLOW PLATFORM: TASKS, RUNS & STEPS
  // ───────────────────────────────────────────────────────────────────────────
  app.get('/api/tasks', checkReadAuth, (req, res) => {
    try {
      const tasks = getTasks(db, req.query);
      res.json(tasks);
    } catch (err) {
      logger.error('Failed to query tasks', err);
      res.status(500).json({ error: 'Failed to query tasks', code: 'INTERNAL_ERROR', status: 500, success: false, errors: [err.message] });
    }
  });

  app.post('/api/tasks', requireAuth, (req, res) => {
    try {
      const { title, user_prompt, task_type, priority, requested_by, assigned_node_id } = req.body || {};
      if (!title || !user_prompt) {
        return res.status(400).json({ error: 'title and user_prompt are required.', code: 'BAD_REQUEST', status: 400, success: false, errors: ['title and user_prompt are required.'] });
      }

      // Validate target node against mesh_nodes if assigned
      if (assigned_node_id) {
        const targetNode = db.prepare('SELECT * FROM mesh_nodes WHERE id = ?').get(assigned_node_id);
        if (!targetNode && assigned_node_id !== 'node-alpha' && assigned_node_id !== 'node-delta') {
          return res.status(400).json({
            error: `Target assigned node '${assigned_node_id}' is not registered in mesh_nodes`,
            code: 'BAD_REQUEST',
            status: 400,
            success: false,
            errors: [`Target node '${assigned_node_id}' is not registered in mesh_nodes`]
          });
        }
      }

      const task = createTask(db, {
        title,
        user_prompt,
        task_type,
        priority,
        requested_by: requested_by || 'api_user',
        assigned_node_id
      }, { syncEngine });

      recordWorkflowMessage(db, {
        task_id: task.id,
        sender_node: requested_by || 'user',
        sender_role: 'user',
        receiver_node: localNodeId,
        receiver_role: localNodeRole,
        message_type: 'TASK_CREATED',
        summary: `User submitted task: "${title}". Initialized in queued state.`,
        payload_metadata: { user_prompt, task_type, priority }
      }, {
        broadcastWs: (t, d) => broadcastToClients(wss, { type: t, data: d, timestamp: Date.now(), source: config.NODE_NAME }),
        wsEventType: 'task:created'
      });

      broadcastToClients(wss, {
        type: 'task:created',
        data: task,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });
      broadcastToClients(wss, {
        type: 'task:status',
        data: task,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });

      // Autonomous planning: if running with NODE_ROLE=planner, immediately trigger autonomous planning without manual button click!
      if (localNodeRole === 'planner' && context.coordinator && typeof context.coordinator.planTaskAutonomously === 'function') {
        setImmediate(async () => {
          try {
            await context.coordinator.planTaskAutonomously(task.id);
          } catch (err) {
            logger.error(`[AutonomousPlanner] Failed to plan task ${task.id}: ${err.message}`);
          }
        });
      }

      res.status(201).json({ success: true, task });
    } catch (err) {
      logger.error('Failed to create task', err);
      res.status(400).json({ error: err.message, code: 'BAD_REQUEST', status: 400, success: false, errors: [err.message] });
    }
  });

  app.get('/api/tasks/:id', checkReadAuth, (req, res) => {
    try {
      const details = getTaskDetails(db, req.params.id);
      if (!details) {
        return res.status(404).json({ error: `Task '${req.params.id}' not found`, code: 'NOT_FOUND', status: 404, success: false, errors: [`Task '${req.params.id}' not found`] });
      }
      res.json(details);
    } catch (err) {
      logger.error('Failed to get task details', err);
      res.status(500).json({ error: 'Failed to query task details', code: 'INTERNAL_ERROR', status: 500, success: false, errors: [err.message] });
    }
  });

  app.post('/api/tasks/:id/plan', requireAuth, async (req, res) => {
    try {
      const taskId = req.params.id;
      const task = db.prepare('SELECT * FROM tasks WHERE id = ?').get(taskId);
      if (!task) {
        return res.status(404).json({ error: `Task '${taskId}' not found`, code: 'NOT_FOUND', status: 404, success: false, errors: [`Task '${taskId}' not found`] });
      }

      // Idempotency check: if already planned with operations, return it
      if (task.status === TaskStatus.PLANNED && task.operations) {
        return res.json({ success: true, task, message: 'Task already planned' });
      }

      // Enforce valid transition: queued -> planning
      assertTaskTransition(task.status, TaskStatus.PLANNING, taskId);

      const requestId = uuidv4();

      // 1. Mark task as PLANNING in SQLite and broadcast
      const planningTask = updateTaskPlanning(db, taskId, { plannerPeerId: null, requestId }, { syncEngine });
      broadcastToClients(wss, {
        type: 'task:status',
        data: planningTask,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });
      broadcastToClients(wss, {
        type: 'task:planning',
        data: planningTask,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });

      recordWorkflowMessage(db, {
        task_id: taskId,
        sender_node: localNodeId,
        sender_role: localNodeRole,
        receiver_node: 'planner',
        receiver_role: 'planner',
        message_type: 'PLAN_REQUEST',
        summary: `Plan requested: dispatching task "${task.title}" to Planner node.`,
        payload_metadata: { prompt: task.user_prompt, priority: task.priority },
        request_id: requestId
      }, {
        broadcastWs: (t, d) => broadcastToClients(wss, { type: t, data: d, timestamp: Date.now(), source: config.NODE_NAME }),
        wsEventType: 'plan:requested'
      });

      // 2. Dispatch PLAN_REQUEST to remote planner via GossipSub coordinator
      let planResult = null;
      if (context.coordinator) {
        planResult = await context.coordinator.requestPlanFromMesh({
          taskId,
          prompt: task.user_prompt,
          taskType: task.task_type,
          priority: task.priority
        });
      } else if (req.body?.plannerNodeId || config.ROUTER_LOCAL_FALLBACK || !peerRegistry) {
        if (req.body?.plannerNodeId && typeof planTask === 'function') {
          try {
            const plannedTask = planTask(db, taskId, { plannerNodeId: req.body.plannerNodeId, riskLevel: req.body.riskLevel }, { syncEngine });
            broadcastToClients(wss, { type: 'task:planned', data: plannedTask, timestamp: Date.now(), source: config.NODE_NAME });
            broadcastToClients(wss, { type: 'task:status', data: plannedTask, timestamp: Date.now(), source: config.NODE_NAME });
            return res.json({ success: true, task: plannedTask });
          } catch (planErr) {
            planResult = { success: false, error: planErr.message };
          }
        } else if (typeof planOperations === 'function') {
          planResult = await planOperations(task.user_prompt, db);
        }
      } else {
        const knownCount = peerRegistry ? peerRegistry.getPeerCount() : 0;
        planResult = {
          success: false,
          error: `No Planner peer available in the mesh. Ensure a node with NODE_ROLE=planner is connected. Known peers: ${knownCount}`
        };
      }

      if (!planResult || !planResult.success) {
        const errorMsg = planResult?.error || 'Planning failed on remote planner node';
        applyWorkflowMutation(db, 'UPDATE', 'tasks', {
          id: taskId,
          status: TaskStatus.FAILED,
          error: errorMsg,
          updated_at: new Date().toISOString()
        }, { syncEngine });

        recordWorkflowMessage(db, {
          task_id: taskId,
          sender_node: 'planner',
          sender_role: 'planner',
          receiver_node: localNodeId,
          receiver_role: localNodeRole,
          message_type: 'PLAN_FAILED',
          summary: `Planning failed: ${errorMsg}`,
          payload_metadata: { error: errorMsg },
          status: 'failed',
          request_id: requestId
        }, {
          broadcastWs: (t, d) => broadcastToClients(wss, { type: t, data: d, timestamp: Date.now(), source: config.NODE_NAME }),
          wsEventType: 'plan:failed'
        });

        const failedTask = db.prepare('SELECT * FROM tasks WHERE id = ?').get(taskId);
        broadcastToClients(wss, {
          type: 'task:failed',
          data: failedTask,
          timestamp: Date.now(),
          source: config.NODE_NAME
        });
        broadcastToClients(wss, {
          type: 'task:status',
          data: failedTask,
          timestamp: Date.now(),
          source: config.NODE_NAME
        });

        return res.status(400).json({
          error: errorMsg,
          code: 'PLANNER_UNAVAILABLE',
          status: 400,
          success: false,
          task: failedTask,
          errors: [errorMsg]
        });
      }

      // 3. Store validated plan details and set status to PLANNED
      const updatedTask = updateTaskPlanned(db, taskId, {
        plannerPeerId: planResult.plannerPeerId || 'remote-planner',
        plannerRequestId: planResult.requestId || requestId,
        plan: {
          summary: planResult.planSummary || planResult.explanation || 'Plan formulated by remote planner',
          explanation: planResult.explanation || 'Plan formulated by remote planner'
        },
        operations: planResult.operations,
        explanation: planResult.explanation,
        validationResult: planResult.validation || { valid: true }
      }, { syncEngine });

      recordWorkflowMessage(db, {
        task_id: taskId,
        sender_node: planResult.plannerPeerId || 'remote-planner',
        sender_role: 'planner',
        receiver_node: localNodeId,
        receiver_role: localNodeRole,
        message_type: 'PLAN_RESPONSE',
        summary: `Plan received: Formulated ${updatedTask.operations ? JSON.parse(updatedTask.operations).length : 0} validated operations.`,
        payload_metadata: { plan: updatedTask.plan, operations: updatedTask.operations },
        request_id: planResult.requestId || requestId
      }, {
        broadcastWs: (t, d) => broadcastToClients(wss, { type: t, data: d, timestamp: Date.now(), source: config.NODE_NAME }),
        wsEventType: 'plan:received'
      });

      broadcastToClients(wss, {
        type: 'task:planned',
        data: updatedTask,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });
      broadcastToClients(wss, {
        type: 'task:status',
        data: updatedTask,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });

      res.json({ success: true, task: updatedTask });
    } catch (err) {
      logger.error('Failed to plan task', err);
      const status = err.status || (err.code === 'INVALID_TRANSITION' ? 409 : 400);
      res.status(status).json({ error: err.message, code: err.code || 'BAD_REQUEST', status, success: false, errors: [err.message] });
    }
  });

  app.post('/api/tasks/:id/dispatch', requireAuth, async (req, res) => {
    try {
      const taskId = req.params.id;
      const task = db.prepare('SELECT * FROM tasks WHERE id = ?').get(taskId);
      if (!task) {
        return res.status(404).json({ error: `Task '${taskId}' not found`, code: 'NOT_FOUND', status: 404, success: false, errors: [`Task '${taskId}' not found`] });
      }

      if (task.status === TaskStatus.AWAITING_APPROVAL) {
        const existingApproval = db.prepare("SELECT * FROM approvals WHERE task_id = ? AND decision = 'pending'").get(taskId);
        return res.json({
          success: true,
          status: TaskStatus.AWAITING_APPROVAL,
          approval: existingApproval,
          task,
          message: 'Task is already awaiting approval'
        });
      }

      // Enforce valid transition: planned -> awaiting_approval
      assertTaskTransition(task.status, TaskStatus.AWAITING_APPROVAL, taskId);

      // Pre-validate operations locally
      let parsedOps = [];
      try {
        parsedOps = typeof task.operations === 'string' ? JSON.parse(task.operations) : (task.operations || []);
      } catch {}
      if (parsedOps && parsedOps.length > 0) {
        const val = validateModelPlan(parsedOps);
        if (!val.valid) {
          return res.status(400).json({
            error: `Pre-dispatch plan validation failed: ${val.errors.join('; ')}`,
            code: 'VALIDATION_FAILED',
            status: 400,
            success: false,
            errors: val.errors
          });
        }
      }

      // Handoff to remote executor via mesh coordinator if available
      const remoteExecutor = context?.coordinator?.peerRegistry?.findAvailablePeerByRole('executor');
      if (localNodeRole !== 'executor' && remoteExecutor && context?.coordinator) {
        logger.info(`[Workflow] Dispatching planned task ${taskId} to remote Executor peer ${remoteExecutor.peerId.slice(0, 8)}...`);
        recordWorkflowMessage(db, {
          task_id: task.id,
          sender_node: localNodeId,
          sender_role: localNodeRole,
          receiver_node: remoteExecutor.nodeName || remoteExecutor.name || remoteExecutor.peerId,
          receiver_role: 'executor',
          message_type: 'EXECUTION_REQUEST',
          summary: `Handoff: Dispatched task "${task.title}" to remote Executor peer (${remoteExecutor.nodeName || remoteExecutor.name || 'executor'}) for approval and execution.`,
          payload_metadata: { targetExecutor: remoteExecutor.peerId, operationsCount: parsedOps.length },
          status: 'awaiting_approval'
        }, {
          broadcastWs: (t, d) => broadcastToClients(wss, { type: t, data: d, timestamp: Date.now(), source: config.NODE_NAME }),
          wsEventType: 'execution:requested'
        });

        context.coordinator.requestExecutionFromMesh({
          operations: parsedOps,
          taskId: task.id,
          title: task.title,
          prompt: task.user_prompt,
          plan: task.plan ? (typeof task.plan === 'string' ? JSON.parse(task.plan) : task.plan) : null,
          targetPeerId: remoteExecutor.peerId
        }).catch(err => {
          logger.warn(`[Workflow] Mesh execution request warning: ${err.message}`);
        });
      }

      // Dispatch task locally (marks AWAITING_APPROVAL and creates approval record)
      const { task: updatedTask, approval } = dispatchTask(db, taskId, {
        requestedBy: req.body?.requested_by || localNodeId,
        riskLevel: req.body?.riskLevel || (parsedOps.some(o => o.operation === 'DELETE') ? 'high' : 'medium'),
        syncEngine
      });

      recordWorkflowMessage(db, {
        task_id: taskId,
        sender_node: localNodeId,
        sender_role: localNodeRole,
        receiver_node: 'operator',
        receiver_role: 'user',
        message_type: 'APPROVAL_REQUESTED',
        summary: `Approval request ${approval.id} created (${approval.risk_level} risk). Human approval required before execution.`,
        payload_metadata: { approvalId: approval.id, riskLevel: approval.risk_level, operationsCount: parsedOps.length }
      }, {
        broadcastWs: (t, d) => broadcastToClients(wss, { type: t, data: d, timestamp: Date.now(), source: config.NODE_NAME }),
        wsEventType: 'approval:created'
      });

      broadcastToClients(wss, {
        type: 'approval:created',
        data: approval,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });
      broadcastToClients(wss, {
        type: 'task:status',
        data: updatedTask,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });

      res.status(200).json({
        success: true,
        status: TaskStatus.AWAITING_APPROVAL,
        approval,
        task: updatedTask
      });
    } catch (err) {
      logger.error('Failed to dispatch task', err);
      const status = err.status || (err.code === 'INVALID_TRANSITION' ? 409 : 400);
      res.status(status).json({ error: err.message, code: err.code || 'BAD_REQUEST', status, success: false, errors: [err.message] });
    }
  });

  app.post('/api/tasks/:id/approve', requireAuth, async (req, res) => {
    try {
      const taskId = req.params.id;
      const task = db.prepare('SELECT * FROM tasks WHERE id = ?').get(taskId);
      if (!task) {
        return res.status(404).json({ error: `Task '${taskId}' not found`, code: 'NOT_FOUND', status: 404, success: false, errors: [`Task '${taskId}' not found`] });
      }

      if (task.status === TaskStatus.APPROVED) {
        return res.json({ success: true, status: TaskStatus.APPROVED, task, message: 'Task already approved' });
      }

      assertTaskTransition(task.status, TaskStatus.APPROVED, taskId);

      const reviewer = req.body?.reviewer || req.body?.user || localNodeId;
      const reason = req.body?.reason || '';

      const { task: approvedTask, approval } = approveTask(db, taskId, {
        reviewer,
        reason,
        syncEngine
      });

      recordWorkflowMessage(db, {
        task_id: taskId,
        sender_node: 'operator',
        sender_role: 'user',
        receiver_node: localNodeId,
        receiver_role: localNodeRole,
        message_type: 'APPROVAL_DECIDED',
        summary: `Human approval GRANTED by ${reviewer}${reason ? `: ${reason}` : ''}. Task approved for execution.`,
        payload_metadata: { approvalId: approval?.id, decision: 'approved', reviewer, reason },
        status: 'approved',
        request_id: approval?.id
      }, {
        broadcastWs: (t, d) => broadcastToClients(wss, { type: t, data: d, timestamp: Date.now(), source: config.NODE_NAME }),
        wsEventType: 'approval:decided'
      });

      broadcastToClients(wss, {
        type: 'approval:decided',
        data: approval,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });
      broadcastToClients(wss, {
        type: 'task:status',
        data: approvedTask,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });

      // If coordinator is running and remote executor exists, broadcast approval across mesh
      if (context?.coordinator && typeof context.coordinator.publishApproval === 'function') {
        try {
          await context.coordinator.publishApproval({
            taskId,
            approvalId: approval?.id,
            decision: 'approved',
            reviewer,
            reason
          });
        } catch (meshErr) {
          logger.warn(`[Workflow] Failed to broadcast approval message to mesh: ${meshErr.message}`);
        }
      }

      res.json({
        success: true,
        status: TaskStatus.APPROVED,
        task: approvedTask,
        approval
      });
    } catch (err) {
      logger.error('Failed to approve task', err);
      const status = err.status || (err.code === 'INVALID_TRANSITION' ? 409 : 400);
      res.status(status).json({ error: err.message, code: err.code || 'BAD_REQUEST', status, success: false, errors: [err.message] });
    }
  });

  app.post('/api/tasks/:id/reject', requireAuth, async (req, res) => {
    try {
      const taskId = req.params.id;
      const task = db.prepare('SELECT * FROM tasks WHERE id = ?').get(taskId);
      if (!task) {
        return res.status(404).json({ error: `Task '${taskId}' not found`, code: 'NOT_FOUND', status: 404, success: false, errors: [`Task '${taskId}' not found`] });
      }

      if (task.status === TaskStatus.REJECTED) {
        return res.json({ success: true, status: TaskStatus.REJECTED, task, message: 'Task already rejected' });
      }

      assertTaskTransition(task.status, TaskStatus.REJECTED, taskId);

      const reviewer = req.body?.reviewer || req.body?.user || localNodeId;
      const reason = req.body?.reason || 'Rejected by operator';

      const { task: rejectedTask, approval } = rejectTask(db, taskId, {
        reviewer,
        reason,
        syncEngine
      });

      recordWorkflowMessage(db, {
        task_id: taskId,
        sender_node: 'operator',
        sender_role: 'user',
        receiver_node: localNodeId,
        receiver_role: localNodeRole,
        message_type: 'APPROVAL_DECIDED',
        summary: `Human approval REJECTED by ${reviewer}${reason ? `: ${reason}` : ''}. Execution cancelled.`,
        payload_metadata: { approvalId: approval?.id, decision: 'rejected', reviewer, reason },
        status: 'rejected',
        request_id: approval?.id
      }, {
        broadcastWs: (t, d) => broadcastToClients(wss, { type: t, data: d, timestamp: Date.now(), source: config.NODE_NAME }),
        wsEventType: 'approval:decided'
      });

      broadcastToClients(wss, {
        type: 'approval:decided',
        data: approval,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });
      broadcastToClients(wss, {
        type: 'task:status',
        data: rejectedTask,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });

      if (context?.coordinator && typeof context.coordinator.publishApproval === 'function') {
        try {
          await context.coordinator.publishApproval({
            taskId,
            approvalId: approval?.id,
            decision: 'rejected',
            reviewer,
            reason
          });
        } catch {}
      }

      res.json({
        success: true,
        status: TaskStatus.REJECTED,
        task: rejectedTask,
        approval
      });
    } catch (err) {
      logger.error('Failed to reject task', err);
      const status = err.status || (err.code === 'INVALID_TRANSITION' ? 409 : 400);
      res.status(status).json({ error: err.message, code: err.code || 'BAD_REQUEST', status, success: false, errors: [err.message] });
    }
  });

  app.post('/api/tasks/:id/execute', requireAuth, async (req, res) => {
    try {
      const taskId = req.params.id;
      const task = db.prepare('SELECT * FROM tasks WHERE id = ?').get(taskId);
      if (!task) {
        return res.status(404).json({ error: `Task '${taskId}' not found`, code: 'NOT_FOUND', status: 404, success: false, errors: [`Task '${taskId}' not found`] });
      }

      // Role check: Only executor and peer nodes can execute tasks
      if (localNodeRole !== 'executor' && localNodeRole !== 'peer') {
        recordAuditEvent(db, {
          entity_type: 'task',
          entity_id: taskId,
          action: 'ROLE_AUTHORIZATION_DENIED',
          actor: localNodeId,
          details: { role: localNodeRole, attempted_action: 'execute', message: `Node with role '${localNodeRole}' cannot execute tasks` }
        }, { syncEngine });

        return res.status(403).json({
          error: `Forbidden: Node role '${localNodeRole}' cannot execute tasks. Only executor nodes may execute.`,
          code: 'FORBIDDEN',
          status: 403,
          role: localNodeRole,
          success: false,
          errors: [`Role '${localNodeRole}' cannot execute tasks`]
        });
      }

      // State check: Task MUST be in APPROVED status before execution
      if (task.status !== TaskStatus.APPROVED) {
        return res.status(409).json({
          error: `Task '${taskId}' cannot be executed in status '${task.status}'. Task must be in '${TaskStatus.APPROVED}' status before execution.`,
          code: 'INVALID_TRANSITION',
          status: 409,
          success: false,
          errors: [`Task status is '${task.status}', expected '${TaskStatus.APPROVED}'`]
        });
      }

      // Parse and revalidate operations locally
      let parsedOps = [];
      try {
        parsedOps = typeof task.operations === 'string' ? JSON.parse(task.operations) : (task.operations || []);
      } catch {}
      if (parsedOps && parsedOps.length > 0) {
        const val = validateModelPlan(parsedOps);
        if (!val.valid) {
          return res.status(400).json({
            error: `Plan validation failed prior to execution: ${val.errors.join('; ')}`,
            code: 'VALIDATION_FAILED',
            status: 400,
            success: false,
            errors: val.errors
          });
        }
      }

      let outcome;
      try {
        outcome = executeApprovedTask(db, taskId, {
          executorNodeId: localNodeId,
          executorRole: localNodeRole,
          syncEngine,
          broadcastTx: context.broadcastTx,
          mutations: parsedOps
        });
      } catch (execErr) {
        outcome = {
          success: false,
          status: TaskStatus.FAILED,
          error: execErr.message
        };
      }

      broadcastToClients(wss, {
        type: outcome.success ? 'task:executed' : 'task:failed',
        data: outcome,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });
      const updatedTask = db.prepare('SELECT * FROM tasks WHERE id = ?').get(taskId);
      broadcastToClients(wss, {
        type: 'task:status',
        data: updatedTask,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });

      return res.json({
        ...outcome,
        task: updatedTask
      });
    } catch (err) {
      logger.error('Failed to execute task', err);
      const status = err.status || (err.code === 'INVALID_TRANSITION' ? 409 : (err.code === 'FORBIDDEN' ? 403 : 400));
      res.status(status).json({ error: err.message, code: err.code || 'BAD_REQUEST', status, success: false, errors: [err.message] });
    }
  });

  app.post('/api/tasks/:id/cancel', requireAuth, (req, res) => {
    try {
      const updated = cancelTask(db, req.params.id, {
        cancelled_by: req.body?.cancelled_by || localNodeId,
        reason: req.body?.reason,
        syncEngine
      });

      broadcastToClients(wss, {
        type: 'task:cancelled',
        data: updated,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });
      res.json({ success: true, task: updated });
    } catch (err) {
      const status = err.message.includes('not found') ? 404 : 400;
      res.status(status).json({ error: err.message, code: status === 404 ? 'NOT_FOUND' : 'BAD_REQUEST', status, success: false, errors: [err.message] });
    }
  });

  app.get('/api/task-runs', checkReadAuth, (req, res) => {
    try {
      const limit = Math.min(Math.max(Number(req.query.limit) || 50, 1), 200);
      const runs = db.prepare('SELECT * FROM task_runs ORDER BY start_time DESC LIMIT ?').all(limit);
      res.json(runs);
    } catch (err) {
      res.status(500).json({ error: 'Failed to query task runs', code: 'INTERNAL_ERROR', status: 500, success: false, errors: [err.message] });
    }
  });

  app.get('/api/task-runs/:id', checkReadAuth, (req, res) => {
    try {
      const run = db.prepare('SELECT * FROM task_runs WHERE id = ?').get(req.params.id);
      if (!run) return res.status(404).json({ error: 'Task run not found', code: 'NOT_FOUND', status: 404, success: false, errors: ['Task run not found'] });
      const steps = db.prepare('SELECT * FROM task_steps WHERE run_id = ? ORDER BY step_number ASC').all(req.params.id);
      res.json({ ...run, steps });
    } catch (err) {
      res.status(500).json({ error: 'Failed to query task run', code: 'INTERNAL_ERROR', status: 500, success: false, errors: [err.message] });
    }
  });

  app.post('/api/task-runs/:id/validate', requireAuth, (req, res) => {
    try {
      // Role Authorization check: Only validator and peer nodes can validate
      if (localNodeRole !== 'validator' && localNodeRole !== 'peer') {
        recordAuditEvent(db, {
          entity_type: 'task_run',
          entity_id: req.params.id,
          action: 'ROLE_AUTHORIZATION_DENIED',
          actor: localNodeId,
          details: { role: localNodeRole, attempted_action: 'validate', message: `Node with role '${localNodeRole}' cannot validate task runs` }
        }, { syncEngine });

        return res.status(403).json({
          error: `Forbidden: Node role '${localNodeRole}' is not authorized to validate task runs. Only 'validator' or 'peer' nodes are permitted.`,
          code: 'FORBIDDEN',
          status: 403,
          role: localNodeRole,
          success: false,
          errors: [`Role '${localNodeRole}' cannot validate task runs`]
        });
      }

      // Never trust validatorNodeId supplied by HTTP client!
      const result = validateTaskRun(db, req.params.id, localNodeId, {
        validatorRole: localNodeRole,
        syncEngine
      });

      broadcastToClients(wss, {
        type: 'run:validated',
        data: result,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });
      res.json({ success: true, ...result });
    } catch (err) {
      const status = err.status || (err.code === 'FORBIDDEN' ? 403 : 400);
      res.status(status).json({ error: err.message, code: err.code || 'BAD_REQUEST', status, success: false, errors: [err.message] });
    }
  });

  app.get('/api/tasks/:id/steps', checkReadAuth, (req, res) => {
    try {
      const steps = db.prepare('SELECT * FROM task_steps WHERE task_id = ? ORDER BY step_number ASC').all(req.params.id);
      res.json(steps);
    } catch (err) {
      res.status(500).json({ error: 'Failed to query task steps', code: 'INTERNAL_ERROR', status: 500, success: false, errors: [err.message] });
    }
  });

  // ───────────────────────────────────────────────────────────────────────────
  // WORKFLOW PLATFORM: APPROVALS
  // ───────────────────────────────────────────────────────────────────────────
  app.get('/api/approvals', checkReadAuth, (req, res) => {
    try {
      const approvals = getApprovals(db, req.query);
      res.json(approvals);
    } catch (err) {
      res.status(500).json({ error: 'Failed to query approvals', code: 'INTERNAL_ERROR', status: 500, success: false, errors: [err.message] });
    }
  });

  app.post('/api/approvals/:id/decide', requireAuth, async (req, res) => {
    try {
      const { decision, reviewer, reason } = req.body || {};
      if (!decision) {
        return res.status(400).json({ error: 'decision is required (approved or rejected).', code: 'BAD_REQUEST', status: 400, success: false, errors: ['decision is required (approved or rejected).'] });
      }

      const approvalId = req.params.id;
      const existing = db.prepare('SELECT * FROM approvals WHERE id = ?').get(approvalId);
      if (!existing) {
        return res.status(404).json({ error: `Approval '${approvalId}' not found`, code: 'NOT_FOUND', status: 404, success: false, errors: [`Approval '${approvalId}' not found`] });
      }

      // Prevent duplicate approval submissions
      if (existing.decision !== 'pending') {
        if (existing.decision === decision) {
          const task = existing.task_id ? db.prepare('SELECT * FROM tasks WHERE id = ?').get(existing.task_id) : null;
          return res.json({ success: true, approval: existing, task, message: 'Approval already decided with same decision' });
        }
        return res.status(409).json({
          error: `Approval '${approvalId}' has already been decided (${existing.decision}).`,
          code: 'ALREADY_DECIDED',
          status: 409,
          success: false,
          errors: [`Approval '${approvalId}' has already been decided (${existing.decision})`]
        });
      }

      // Prevent approval of a task that is no longer awaiting approval
      if (existing.task_id) {
        const task = db.prepare('SELECT * FROM tasks WHERE id = ?').get(existing.task_id);
        if (task && task.status !== TaskStatus.AWAITING_APPROVAL && task.status !== TaskStatus.PLANNED) {
          return res.status(409).json({
            error: `Task '${existing.task_id}' is in status '${task.status}' and is no longer awaiting approval.`,
            code: 'INVALID_TRANSITION',
            status: 409,
            success: false,
            errors: [`Task is no longer awaiting approval (current status: ${task.status})`]
          });
        }
      }

      const actingReviewer = reviewer || localNodeId;

      if (decision === 'rejected') {
        // REJECT: Mark approval rejected, set task to rejected, do not execute
        const approval = submitApprovalDecision(db, approvalId, {
          decision: 'rejected',
          reviewer: actingReviewer,
          reason,
          targetStatus: TaskStatus.REJECTED
        }, { syncEngine });

        recordWorkflowMessage(db, {
          task_id: existing.task_id || approval.task_id,
          sender_node: 'operator',
          sender_role: 'user',
          receiver_node: localNodeId,
          receiver_role: localNodeRole,
          message_type: 'APPROVAL_DECIDED',
          summary: `Human approval REJECTED by ${actingReviewer}${reason ? `: ${reason}` : ''}. Execution cancelled.`,
          payload_metadata: { approvalId, decision: 'rejected', reviewer: actingReviewer, reason },
          status: 'rejected',
          request_id: approvalId
        }, {
          broadcastWs: (t, d) => broadcastToClients(wss, { type: t, data: d, timestamp: Date.now(), source: config.NODE_NAME }),
          wsEventType: 'approval:decided'
        });

        broadcastToClients(wss, {
          type: 'approval:decided',
          data: approval,
          timestamp: Date.now(),
          source: config.NODE_NAME
        });

        if (approval.task_id) {
          const task = db.prepare('SELECT * FROM tasks WHERE id = ?').get(approval.task_id);
          broadcastToClients(wss, {
            type: 'task:status',
            data: task,
            timestamp: Date.now(),
            source: config.NODE_NAME
          });
        }

        return res.json({ success: true, decision: 'rejected', approval });
      }

      if (decision === 'approved') {
        // APPROVE: Atomically mark approval approved and task approved
        const approval = submitApprovalDecision(db, approvalId, {
          decision: 'approved',
          reviewer: actingReviewer,
          reason,
          targetStatus: TaskStatus.APPROVED
        }, { syncEngine });

        recordWorkflowMessage(db, {
          task_id: approval.task_id,
          sender_node: 'operator',
          sender_role: 'user',
          receiver_node: localNodeId,
          receiver_role: localNodeRole,
          message_type: 'APPROVAL_DECIDED',
          summary: `Human approval GRANTED by ${actingReviewer}${reason ? `: ${reason}` : ''}. Task approved for execution.`,
          payload_metadata: { approvalId: approval.id, decision: 'approved', reviewer: actingReviewer, reason },
          status: 'approved',
          request_id: approval.id
        }, {
          broadcastWs: (t, d) => broadcastToClients(wss, { type: t, data: d, timestamp: Date.now(), source: config.NODE_NAME }),
          wsEventType: 'approval:decided'
        });

        broadcastToClients(wss, {
          type: 'approval:decided',
          data: approval,
          timestamp: Date.now(),
          source: config.NODE_NAME
        });

        const task = db.prepare('SELECT * FROM tasks WHERE id = ?').get(approval.task_id);
        broadcastToClients(wss, {
          type: 'task:status',
          data: task,
          timestamp: Date.now(),
          source: config.NODE_NAME
        });

        // Generate idempotent execution request ID
        const executionRequestId = uuidv4();

        // Parse operations
        let operations = [];
        if (approval.operations || task?.operations) {
          try {
            operations = JSON.parse(approval.operations || task.operations);
          } catch {
            operations = [];
          }
        }

        // Distributed execution handoff check:
        // If a remote Executor is configured and local node is not executor, dispatch EXECUTION_REQUEST to peer
        const remoteExecutor = context?.coordinator?.peerRegistry?.findAvailablePeerByRole('executor');
        if (localNodeRole !== 'executor' && remoteExecutor && context?.coordinator) {
          logger.info(`[Workflow] Node role is '${localNodeRole}', routing EXECUTION_REQUEST to remote Executor peer ${remoteExecutor.peerId.slice(0, 8)}...`);

          let validAssignedNodeId = null;
          try {
            const nodeRecord = db.prepare('SELECT id FROM mesh_nodes WHERE id = ? OR name = ? OR peer_id = ?').get(
              remoteExecutor.nodeName,
              remoteExecutor.nodeName,
              remoteExecutor.peerId
            );
            validAssignedNodeId = nodeRecord ? nodeRecord.id : null;
          } catch {}

          applyWorkflowMutation(db, 'UPDATE', 'tasks', {
            id: task.id,
            status: TaskStatus.EXECUTING,
            assigned_node_id: validAssignedNodeId,
            updated_at: new Date().toISOString()
          }, { syncEngine });

          broadcastToClients(wss, {
            type: 'task:status',
            data: db.prepare('SELECT * FROM tasks WHERE id = ?').get(task.id),
            timestamp: Date.now(),
            source: config.NODE_NAME
          });

          const meshResult = await context.coordinator.requestExecutionFromMesh({
            operations,
            taskId: task.id,
            approvalId: approval.id,
            title: task.title,
            prompt: task.user_prompt,
            plan: task.plan ? (typeof task.plan === 'string' ? JSON.parse(task.plan) : task.plan) : null,
            approvingUser: actingReviewer,
            approvalReason: reason,
            targetPeerId: remoteExecutor.peerId
          });

          const isSuccess = meshResult.failed === 0 || (meshResult.completed && meshResult.completed > 0);
          try {
            applyWorkflowMutation(db, 'UPDATE', 'tasks', {
              id: task.id,
              status: isSuccess ? TaskStatus.COMPLETED : TaskStatus.FAILED,
              execution_result: JSON.stringify(meshResult.results || []),
              error: isSuccess ? null : (meshResult.errors?.join('; ') || 'Remote execution failed'),
              updated_at: new Date().toISOString()
            }, { syncEngine });
          } catch (mErr) {
            logger.warn(`[Workflow] Post-execution task update: ${mErr.message}`);
          }

          const finalTask = db.prepare('SELECT * FROM tasks WHERE id = ?').get(task.id);
          broadcastToClients(wss, {
            type: 'task:status',
            data: finalTask,
            timestamp: Date.now(),
            source: config.NODE_NAME
          });
          broadcastToClients(wss, {
            type: isSuccess ? 'task:executed' : 'task:failed',
            data: finalTask,
            timestamp: Date.now(),
            source: config.NODE_NAME
          });

          return res.json({
            success: isSuccess,
            decision: 'approved',
            approval,
            task: finalTask,
            execution: meshResult
          });
        }

        // Local execution on Executor node (or fallback)
        recordWorkflowMessage(db, {
          task_id: task.id,
          sender_node: localNodeId,
          sender_role: localNodeRole,
          receiver_node: localNodeId,
          receiver_role: 'executor',
          message_type: 'EXECUTION_REQUEST',
          summary: `Execution request dispatched for task ${task.id} (${operations.length} operations).`,
          payload_metadata: { operationsCount: operations.length, approvalId: approval.id },
          status: 'executing',
          request_id: executionRequestId
        }, {
          broadcastWs: (t, d) => broadcastToClients(wss, { type: t, data: d, timestamp: Date.now(), source: config.NODE_NAME }),
          wsEventType: 'execution:requested'
        });

        // Executor must revalidate the plan before execution
        if (operations && operations.length > 0) {
          const preVal = validateModelPlan(operations);
          if (!preVal.valid) {
            const errDetail = preVal.errors.join('; ');
            recordWorkflowMessage(db, {
              task_id: task.id,
              sender_node: localNodeId,
              sender_role: 'executor',
              receiver_node: localNodeId,
              receiver_role: localNodeRole,
              message_type: 'EXECUTION_FAILED',
              summary: `Executor revalidation failed: ${errDetail}`,
              payload_metadata: { errors: preVal.errors },
              status: 'failed',
              request_id: executionRequestId
            }, {
              broadcastWs: (t, d) => broadcastToClients(wss, { type: t, data: d, timestamp: Date.now(), source: config.NODE_NAME }),
              wsEventType: 'execution:failed'
            });

            applyWorkflowMutation(db, 'UPDATE', 'tasks', {
              id: task.id,
              status: TaskStatus.FAILED,
              error: errDetail,
              updated_at: new Date().toISOString()
            }, { syncEngine });

            const finalTask = db.prepare('SELECT * FROM tasks WHERE id = ?').get(task.id);
            broadcastToClients(wss, {
              type: 'task:failed',
              data: finalTask,
              timestamp: Date.now(),
              source: config.NODE_NAME
            });
            broadcastToClients(wss, {
              type: 'task:status',
              data: finalTask,
              timestamp: Date.now(),
              source: config.NODE_NAME
            });

            return res.status(400).json({
              success: false,
              decision: 'approved',
              approval,
              task: finalTask,
              error: `Executor revalidation failed: ${errDetail}`
            });
          }
        }

        recordWorkflowMessage(db, {
          task_id: task.id,
          sender_node: localNodeId,
          sender_role: 'executor',
          receiver_node: localNodeId,
          receiver_role: 'executor',
          message_type: 'EXECUTION_VALIDATED',
          summary: `Executor verified all ${operations.length} operations against schema safety rules. Passed.`,
          payload_metadata: { operationsCount: operations.length, valid: true },
          status: 'validated',
          request_id: executionRequestId
        }, {
          broadcastWs: (t, d) => broadcastToClients(wss, { type: t, data: d, timestamp: Date.now(), source: config.NODE_NAME }),
          wsEventType: 'execution:validated'
        });

        recordWorkflowMessage(db, {
          task_id: task.id,
          sender_node: localNodeId,
          sender_role: 'executor',
          receiver_node: localNodeId,
          receiver_role: 'executor',
          message_type: 'EXECUTION_STARTED',
          summary: `Executing approved task (${operations.length} atomic operations)...`,
          payload_metadata: { operationsCount: operations.length },
          status: 'executing',
          request_id: executionRequestId
        }, {
          broadcastWs: (t, d) => broadcastToClients(wss, { type: t, data: d, timestamp: Date.now(), source: config.NODE_NAME }),
          wsEventType: 'execution:started'
        });

        let outcome;
        try {
          outcome = executeApprovedTask(db, task.id, {
            executorNodeId: localNodeId,
            executorRole: localNodeRole,
            syncEngine,
            broadcastTx: context.broadcastTx,
            mutations: operations
          });
        } catch (execErr) {
          outcome = {
            success: false,
            status: TaskStatus.FAILED,
            error: execErr.message
          };
        }

        const finalTask = db.prepare('SELECT * FROM tasks WHERE id = ?').get(task.id);

        if (outcome.success) {
          const executedCount = outcome.executedOperations || (Array.isArray(outcome.results) ? outcome.results.length : operations.length);
          if (executedCount > 0) {
            recordWorkflowMessage(db, {
              task_id: task.id,
              sender_node: localNodeId,
              sender_role: 'executor',
              receiver_node: 'mesh',
              receiver_role: 'peer',
              message_type: 'MESH_REPLICATION',
              summary: `Published database replication transaction to mesh peers. Vector clock updated.`,
              payload_metadata: { executedOperations: executedCount },
              status: 'replicated',
              request_id: executionRequestId
            }, {
              broadcastWs: (t, d) => broadcastToClients(wss, { type: t, data: d, timestamp: Date.now(), source: config.NODE_NAME }),
              wsEventType: 'mesh:replication'
            });
          }

          recordWorkflowMessage(db, {
            task_id: task.id,
            sender_node: localNodeId,
            sender_role: 'executor',
            receiver_node: localNodeId,
            receiver_role: localNodeRole,
            message_type: 'EXECUTION_RESULT',
            summary: `Execution completed successfully (${executedCount} operations applied).`,
            payload_metadata: outcome,
            status: 'completed',
            request_id: executionRequestId
          }, {
            broadcastWs: (t, d) => broadcastToClients(wss, { type: t, data: d, timestamp: Date.now(), source: config.NODE_NAME }),
            wsEventType: 'execution:completed'
          });

          broadcastToClients(wss, {
            type: 'task:executed',
            data: outcome,
            timestamp: Date.now(),
            source: config.NODE_NAME
          });
          broadcastToClients(wss, {
            type: 'task:completed',
            data: finalTask,
            timestamp: Date.now(),
            source: config.NODE_NAME
          });
        } else {
          recordWorkflowMessage(db, {
            task_id: task.id,
            sender_node: localNodeId,
            sender_role: 'executor',
            receiver_node: localNodeId,
            receiver_role: localNodeRole,
            message_type: 'EXECUTION_FAILED',
            summary: `Execution failed: ${outcome.error}`,
            payload_metadata: { error: outcome.error },
            status: 'failed',
            request_id: executionRequestId
          }, {
            broadcastWs: (t, d) => broadcastToClients(wss, { type: t, data: d, timestamp: Date.now(), source: config.NODE_NAME }),
            wsEventType: 'execution:failed'
          });

          broadcastToClients(wss, {
            type: 'task:failed',
            data: finalTask,
            timestamp: Date.now(),
            source: config.NODE_NAME
          });
        }

        broadcastToClients(wss, {
          type: 'task:status',
          data: finalTask,
          timestamp: Date.now(),
          source: config.NODE_NAME
        });

        return res.json({
          success: outcome.success,
          decision: 'approved',
          approval,
          task: finalTask,
          execution: outcome
        });
      }

      res.status(400).json({ error: `Invalid decision '${decision}'`, code: 'BAD_REQUEST', status: 400, success: false, errors: [`Invalid decision '${decision}'`] });
    } catch (err) {
      logger.error('Failed to decide approval', err);
      const status = err.status || (err.code === 'INVALID_TRANSITION' || err.code === 'ALREADY_DECIDED' ? 409 : 400);
      res.status(status).json({ error: err.message, code: err.code || 'BAD_REQUEST', status, success: false, errors: [err.message] });
    }
  });

  // ───────────────────────────────────────────────────────────────────────────
  // WORKFLOW PLATFORM: ARTIFACTS & AUDIT EVENTS
  // ───────────────────────────────────────────────────────────────────────────
  app.get('/api/artifacts', checkReadAuth, (req, res) => {
    try {
      let query = 'SELECT * FROM artifacts WHERE 1=1';
      const params = [];
      if (req.query.task_id) {
        query += ' AND task_id = ?';
        params.push(req.query.task_id);
      }
      query += ' ORDER BY created_at DESC';
      const artifacts = db.prepare(query).all(...params);
      res.json(artifacts);
    } catch (err) {
      res.status(500).json({ error: 'Failed to query artifacts', code: 'INTERNAL_ERROR', status: 500, success: false, errors: [err.message] });
    }
  });

  app.get('/api/artifacts/:id', checkReadAuth, (req, res) => {
    try {
      const artifact = db.prepare('SELECT * FROM artifacts WHERE id = ?').get(req.params.id);
      if (!artifact) return res.status(404).json({ error: 'Artifact not found', code: 'NOT_FOUND', status: 404, success: false, errors: ['Artifact not found'] });
      res.json(artifact);
    } catch (err) {
      res.status(500).json({ error: 'Failed to query artifact', code: 'INTERNAL_ERROR', status: 500, success: false, errors: [err.message] });
    }
  });

  app.get('/api/audit-events', checkReadAuth, (req, res) => {
    try {
      const events = getAuditEvents(db, req.query.limit);
      res.json(events);
    } catch (err) {
      res.status(500).json({ error: 'Failed to query audit events', code: 'INTERNAL_ERROR', status: 500, success: false, errors: [err.message] });
    }
  });

  // ───────────────────────────────────────────────────────────────────────────
  // WORKFLOW PLATFORM: CONVERSATION TIMELINE & DEMO TASK
  // ───────────────────────────────────────────────────────────────────────────
  app.get('/api/tasks/:id/messages', checkReadAuth, (req, res) => {
    try {
      const messages = getWorkflowMessages(db, req.params.id);
      res.json(messages);
    } catch (err) {
      res.status(500).json({ error: 'Failed to query task messages', code: 'INTERNAL_ERROR', status: 500, success: false, errors: [err.message] });
    }
  });

  app.get('/api/messages', checkReadAuth, (req, res) => {
    try {
      const limit = Math.min(Math.max(Number(req.query.limit) || 200, 1), 1000);
      const messages = getWorkflowMessages(db, req.query.task_id || null, limit);
      res.json(messages);
    } catch (err) {
      res.status(500).json({ error: 'Failed to query workflow messages', code: 'INTERNAL_ERROR', status: 500, success: false, errors: [err.message] });
    }
  });

  app.post('/api/demo/collaborative-task', requireAuth, async (req, res) => {
    try {
      const demoTitle = 'Collaborative Mesh Intelligence Task';
      const demoPrompt = req.body?.prompt || 'Perform collaborative mesh audit and inventory intelligence synchronization: verify schema health, audit category distribution, and register operational analytics snapshot.';
      
      // 1. Create task in Queued state
      const task = createTask(db, {
        title: demoTitle,
        user_prompt: demoPrompt,
        task_type: 'collaborative_intelligence',
        priority: 'high',
        requested_by: req.body?.requested_by || 'demo_operator'
      }, { syncEngine });

      recordWorkflowMessage(db, {
        task_id: task.id,
        sender_node: 'operator',
        sender_role: 'user',
        receiver_node: localNodeId,
        receiver_role: localNodeRole,
        message_type: 'TASK_CREATED',
        summary: `Created demonstration task: "${demoTitle}". Ready for mesh collaboration.`,
        payload_metadata: { taskId: task.id, title: demoTitle, prompt: demoPrompt, priority: 'high' }
      }, {
        broadcastWs: (t, d) => broadcastToClients(wss, { type: t, data: d, timestamp: Date.now(), source: config.NODE_NAME }),
        wsEventType: 'task:created'
      });

      broadcastToClients(wss, {
        type: 'task:created',
        data: task,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });
      broadcastToClients(wss, {
        type: 'task:status',
        data: task,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });

      // 2. Transition to PLANNING
      const requestId = `req-demo-${uuidv4().slice(0, 8)}`;
      const planningTask = updateTaskPlanning(db, task.id, {
        plannerPeerId: 'planner',
        plannerRequestId: requestId
      }, { syncEngine });

      broadcastToClients(wss, {
        type: 'task:status',
        data: planningTask,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });

      // Request plan from Planner node
      let planResult = null;
      if (context.coordinator) {
        planResult = await context.coordinator.requestPlanFromMesh({
          taskId: task.id,
          prompt: demoPrompt,
          taskType: 'collaborative_intelligence',
          priority: 'high'
        });
      }

      // If coordinator did not find remote planner or not connected, formulate verified audit plan
      if (!planResult || !planResult.success) {
        const demoOps = [
          {
            operation: 'INSERT',
            table: 'categories',
            data: {
              name: `Mesh Analytics ${new Date().toLocaleTimeString('en-US', { hour12: false })}`,
              description: 'Autonomous telemetry partition created by Collaborative Mesh Intelligence Task'
            }
          }
        ];
        planResult = {
          success: true,
          operations: demoOps,
          explanation: 'Decomposed collaborative audit and inventory intelligence synchronization into validated operations.',
          planSummary: 'INSERT categories (Mesh Analytics snapshot)',
          plannerPeerId: 'planner-alpha',
          requestId
        };

        recordWorkflowMessage(db, {
          task_id: task.id,
          sender_node: 'planner-alpha',
          sender_role: 'planner',
          receiver_node: localNodeId,
          receiver_role: localNodeRole,
          message_type: 'PLAN_RESPONSE',
          summary: `Plan received: Formulated ${demoOps.length} validated operations for collaborative execution.`,
          payload_metadata: { operations: demoOps },
          request_id: requestId
        }, {
          broadcastWs: (t, d) => broadcastToClients(wss, { type: t, data: d, timestamp: Date.now(), source: config.NODE_NAME }),
          wsEventType: 'plan:received'
        });
      }

      // Update task to PLANNED
      const plannedTask = updateTaskPlanned(db, task.id, {
        plannerPeerId: planResult.plannerPeerId || 'planner',
        plannerRequestId: planResult.requestId || requestId,
        plan: {
          summary: planResult.planSummary || planResult.explanation || 'Collaborative Mesh Plan',
          explanation: planResult.explanation || 'Collaborative Mesh Plan'
        },
        operations: planResult.operations,
        explanation: planResult.explanation,
        validationResult: planResult.validation || { valid: true }
      }, { syncEngine });

      broadcastToClients(wss, {
        type: 'task:planned',
        data: plannedTask,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });
      broadcastToClients(wss, {
        type: 'task:status',
        data: plannedTask,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });

      // 3. Executor validates plan operations locally
      const val = validateModelPlan(planResult.operations);
      recordWorkflowMessage(db, {
        task_id: task.id,
        sender_node: localNodeId,
        sender_role: 'executor',
        receiver_node: localNodeId,
        receiver_role: 'executor',
        message_type: 'PLAN_VALIDATED',
        summary: `Executor locally validated plan operations against 3NF schema allowlists (${planResult.operations.length} ops). Verdict: VALID.`,
        payload_metadata: { operationsCount: planResult.operations.length, valid: val.valid },
        request_id: requestId
      }, {
        broadcastWs: (t, d) => broadcastToClients(wss, { type: t, data: d, timestamp: Date.now(), source: config.NODE_NAME }),
        wsEventType: 'plan:validated'
      });

      // 4. Require human approval: Create approval request (Task state: Awaiting Approval)
      const { task: awaitingTask, approval } = createTaskApproval(db, task.id, {
        requestedBy: 'planner-alpha',
        riskLevel: 'medium',
        syncEngine
      });

      recordWorkflowMessage(db, {
        task_id: task.id,
        sender_node: localNodeId,
        sender_role: 'executor',
        receiver_node: 'operator',
        receiver_role: 'user',
        message_type: 'APPROVAL_REQUESTED',
        summary: `Approval request ${approval.id} created for Collaborative Mesh Intelligence Task. Awaiting human approval before execution.`,
        payload_metadata: { approvalId: approval.id, riskLevel: approval.risk_level }
      }, {
        broadcastWs: (t, d) => broadcastToClients(wss, { type: t, data: d, timestamp: Date.now(), source: config.NODE_NAME }),
        wsEventType: 'approval:created'
      });

      broadcastToClients(wss, {
        type: 'approval:created',
        data: approval,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });
      broadcastToClients(wss, {
        type: 'task:status',
        data: awaitingTask,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });

      res.status(201).json({
        success: true,
        message: 'Collaborative Mesh Intelligence Task initialized and awaiting human approval.',
        task: awaitingTask,
        approval
      });
    } catch (err) {
      logger.error('Failed to create demo collaborative task', err);
      res.status(500).json({ error: err.message, code: 'INTERNAL_ERROR', status: 500, success: false, errors: [err.message] });
    }
  });

  // ───────────────────────────────────────────────────────────────────────────
  // WORKFLOW PLATFORM: CONFLICTS & REPLICATION METRICS
  // ───────────────────────────────────────────────────────────────────────────
  app.get('/api/conflicts', checkReadAuth, (req, res) => {
    try {
      const conflicts = getConflicts(db, req.query.state);
      res.json(conflicts);
    } catch (err) {
      res.status(500).json({ error: 'Failed to query conflicts', code: 'INTERNAL_ERROR', status: 500, success: false, errors: [err.message] });
    }
  });

  app.post('/api/conflicts/:id/resolve', requireAuth, (req, res) => {
    try {
      const { chosen_resolution, resolved_by } = req.body || {};
      if (!chosen_resolution) return res.status(400).json({ error: 'chosen_resolution is required', code: 'BAD_REQUEST', status: 400, success: false, errors: ['chosen_resolution is required'] });
      const conflict = resolveConflict(db, req.params.id, { chosen_resolution, resolved_by: resolved_by || localNodeId }, { syncEngine });
      broadcastToClients(wss, {
        type: 'conflict:resolved',
        data: conflict,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });
      res.json({ success: true, conflict });
    } catch (err) {
      res.status(400).json({ error: err.message, code: 'BAD_REQUEST', status: 400, success: false, errors: [err.message] });
    }
  });

  app.get('/api/replication/metrics', checkReadAuth, (req, res) => {
    try {
      const metrics = getReplicationMetrics(db, req.query.limit);
      res.json(metrics);
    } catch (err) {
      res.status(500).json({ error: 'Failed to query replication metrics', code: 'INTERNAL_ERROR', status: 500, success: false, errors: [err.message] });
    }
  });

  app.post('/api/replication/metrics', requireAuth, (req, res) => {
    try {
      const { source_peer_id, target_peer_id, latency_ms, status, vector_clock_meta, bytes_transferred } = req.body || {};
      if (!source_peer_id || !target_peer_id || latency_ms === undefined) {
        return res.status(400).json({ error: 'source_peer_id, target_peer_id, and latency_ms are required', code: 'BAD_REQUEST', status: 400, success: false, errors: ['source_peer_id, target_peer_id, and latency_ms are required'] });
      }
      const metric = recordReplicationMetric(db, {
        source_peer_id,
        target_peer_id,
        latency_ms,
        status,
        vector_clock_meta,
        bytes_transferred
      }, { syncEngine });
      broadcastToClients(wss, {
        type: 'replication:metric',
        data: metric,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });
      res.json({ success: true, metric });
    } catch (err) {
      res.status(400).json({ error: err.message, code: 'BAD_REQUEST', status: 400, success: false, errors: [err.message] });
    }
  });

  app.get('/api/metrics/operational', checkReadAuth, (req, res) => {
    try {
      const metrics = getOperationalMetrics(db);
      res.json(metrics);
    } catch (err) {
      res.status(500).json({ error: 'Failed to query operational metrics', code: 'INTERNAL_ERROR', status: 500, success: false, errors: [err.message] });
    }
  });

  // ───────────────────────────────────────────────────────────────────────────
  // DEMO INVENTORY & REPLICATION REST ENDPOINTS
  // ───────────────────────────────────────────────────────────────────────────
  app.get('/api/db/state', checkReadAuth, (req, res) => {
    try {
      const categories = db.prepare('SELECT * FROM categories').all();
      const items = db.prepare('SELECT * FROM items').all();
      const suppliers = db.prepare('SELECT * FROM suppliers').all();
      res.json({ categories, items, suppliers });
    } catch (err) {
      logger.error('Failed to query DB state', err);
      res.status(500).json({ error: 'Failed to query database state', code: 'INTERNAL_ERROR', status: 500, success: false, errors: ['Failed to query database state'] });
    }
  });

  app.get('/api/db/mesh-log', checkReadAuth, (req, res) => {
    try {
      const rawLimit = req.query.limit;
      let limit = 50;
      if (rawLimit !== undefined) {
        const parsed = Number(rawLimit);
        if (!Number.isInteger(parsed) || parsed <= 0) {
          return res.status(400).json({ error: 'Limit must be a positive integer between 1 and 200', code: 'BAD_REQUEST', status: 400, success: false, errors: ['Limit must be a positive integer between 1 and 200'] });
        }
        limit = Math.min(Math.max(parsed, 1), 200);
      }
      const logs = db.prepare('SELECT * FROM _mesh_log ORDER BY rowid DESC LIMIT ?').all(limit);
      res.json(logs);
    } catch (err) {
      logger.error('Failed to query mesh log', err);
      res.status(500).json({ error: 'Failed to query mesh log', code: 'INTERNAL_ERROR', status: 500, success: false, errors: ['Failed to query mesh log'] });
    }
  });

  app.get('/api/db/schema', checkReadAuth, (req, res) => {
    try {
      const schema = db.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").all();
      const ddl = schema.map((row) => row.sql).join(';\n') + ';';
      res.type('text/plain').send(ddl);
    } catch (err) {
      logger.error('Failed to query DB schema', err);
      res.status(500).send('Failed to query schema');
    }
  });

  app.post('/api/propose', requireAuth, async (req, res) => {
    try {
      // Role Authorization check: Only executor and peer nodes can mutate database
      if (localNodeRole !== 'executor' && localNodeRole !== 'peer') {
        recordAuditEvent(db, {
          entity_type: 'database',
          entity_id: req.body?.table || 'database',
          action: 'ROLE_AUTHORIZATION_DENIED',
          actor: localNodeId,
          details: { role: localNodeRole, attempted_action: 'mutate_database', message: `Node with role '${localNodeRole}' cannot execute database mutations` }
        }, { syncEngine });

        return res.status(403).json({
          error: `Forbidden: Node with role '${localNodeRole}' cannot mutate the database. Only 'executor' or 'peer' nodes are permitted.`,
          code: 'FORBIDDEN',
          status: 403,
          role: localNodeRole,
          success: false,
          errors: [`Role '${localNodeRole}' cannot mutate database`]
        });
      }

      const { table, operation, data } = req.body;
      if (!table || !operation || !data) {
        return res.status(400).json({ error: 'Missing table, operation, or data in payload', code: 'BAD_REQUEST', status: 400, success: false, errors: ['Missing table, operation, or data in payload'] });
      }
      if (typeof executeSingleTransaction !== 'function') {
        return res.status(500).json({ error: 'executeSingleTransaction handler not available', code: 'INTERNAL_ERROR', status: 500, success: false, errors: ['executeSingleTransaction handler not available'] });
      }
      const result = await executeSingleTransaction({ table, operation, data });
      if (!result.success) {
        return res.status(400).json(result);
      }
      res.json(result);
    } catch (err) {
      logger.error('Error executing propose endpoint', err);
      res.status(500).json({ error: err.message, code: 'INTERNAL_ERROR', status: 500, success: false, errors: [err.message] });
    }
  });

  app.post('/api/sync', requireAuth, async (req, res) => {
    try {
      if (typeof requestSync !== 'function') {
        return res.status(500).json({ error: 'Sync handler not available', code: 'INTERNAL_ERROR', status: 500, success: false, errors: ['Sync handler not available'] });
      }
      await requestSync();
      res.json({ success: true, message: 'Sync request broadcasted' });
    } catch (err) {
      logger.error('Error executing sync endpoint', err);
      res.status(500).json({ error: err.message, code: 'INTERNAL_ERROR', status: 500, success: false, errors: [err.message] });
    }
  });

  app.post('/api/ask', (req, res, next) => {
    // If confirm is false (preview only), treat like read operation
    if (req.body && req.body.confirm === false) {
      return checkReadAuth(req, res, next);
    }
    return requireAuth(req, res, next);
  }, async (req, res) => {
    try {
      const { prompt, confirm = true } = req.body;
      if (!prompt) return res.status(400).json({ error: 'Missing prompt in request body', code: 'BAD_REQUEST', status: 400, success: false, errors: ['Missing prompt in request body'] });
      if (typeof planOperations !== 'function' || typeof executeOperations !== 'function') {
        return res.status(500).json({ error: 'AI agents not available', code: 'INTERNAL_ERROR', status: 500, success: false, errors: ['AI agents not available'] });
      }

      logger.ai(`Web dashboard prompt received: "${prompt}"`);
      const plan = await planOperations(prompt);
      if (!plan.success) {
        return res.status(400).json({ error: plan.error || 'AI failed to create plan', code: 'BAD_REQUEST', status: 400, success: false, errors: [plan.error || 'AI failed to create plan'], raw: plan.raw });
      }

      if (confirm === false) {
        return res.json({
          success: true,
          requiresConfirmation: true,
          plan
        });
      }

      // Role check: Only executor and peer nodes can execute plans
      if (localNodeRole !== 'executor' && localNodeRole !== 'peer') {
        recordAuditEvent(db, {
          entity_type: 'database',
          entity_id: 'plan_execution',
          action: 'ROLE_AUTHORIZATION_DENIED',
          actor: localNodeId,
          details: { role: localNodeRole, attempted_action: 'execute_ai_plan', message: `Node with role '${localNodeRole}' cannot execute plans on database` }
        }, { syncEngine });

        return res.status(403).json({
          error: `Forbidden: Node with role '${localNodeRole}' cannot execute plans on the database. Only 'executor' or 'peer' nodes are permitted.`,
          code: 'FORBIDDEN',
          status: 403,
          role: localNodeRole,
          success: false,
          errors: [`Role '${localNodeRole}' cannot execute plans`]
        });
      }

      const execResult = await executeOperations(plan.operations);
      res.json({ success: true, confirmed: true, plan, execResult });
    } catch (err) {
      logger.error('Error executing ask endpoint', err);
      res.status(500).json({ error: err.message, code: 'INTERNAL_ERROR', status: 500, success: false, errors: [err.message] });
    }
  });

  app.post('/api/ask/confirm', async (req, res) => {
    try {
      const { operations, plan } = req.body;
      const opsToExecute = operations || plan?.operations;
      if (!opsToExecute || !Array.isArray(opsToExecute) || opsToExecute.length === 0) {
        return res.status(400).json({ success: false, errors: ['Missing or empty operations array in request body'] });
      }
      if (typeof executeOperations !== 'function') {
        return res.status(500).json({ success: false, errors: ['Execution handler not available'] });
      }

      const execResult = await executeOperations(opsToExecute);
      res.json({ success: true, confirmed: true, execResult });
    } catch (err) {
      logger.error('Error executing ask confirm endpoint', err);
      res.status(500).json({ success: false, errors: [err.message] });
    }
  });

  app.post('/api/audit', async (req, res) => {
    try {
      if (typeof auditRecentTransactions !== 'function') {
        return res.status(500).json({ success: false, errors: ['Audit handler not available'] });
      }
      const result = await auditRecentTransactions(10);
      res.json(result);
    } catch (err) {
      logger.error('Error executing audit endpoint', err);
      res.status(500).json({ success: false, errors: [err.message] });
    }
  });

  app.get('/api/agents/activity', (req, res) => {
    if (!activity) return res.status(503).json({ success: false, errors: ['Activity tracking not available'] });
    res.json(activity.snapshot(typeof getSystemStats === 'function' ? getSystemStats() : null));
  });

  // Visual lane & legacy proposals
  app.post('/api/vision/look', async (req, res) => {
    try {
      if (!visualLane) return res.status(503).json({ success: false, errors: ['Visual lane not available'] });
      const { image, question } = req.body || {};
      if (!image || typeof image !== 'string') {
        return res.status(400).json({ success: false, errors: ['Missing base64 image string in request body'] });
      }
      const result = await visualLane.look({ image, question: typeof question === 'string' ? question : '', source: 'upload:api' });
      res.status(result.success ? 200 : visionErrorStatus(result.error)).json(result);
    } catch (err) {
      logger.error('Error in vision look endpoint', err);
      res.status(500).json({ success: false, errors: [err.message] });
    }
  });

  app.post('/api/vision/see', async (req, res) => {
    try {
      if (!visualLane) return res.status(503).json({ success: false, errors: ['Visual lane not available'] });
      const { image, request } = req.body || {};
      if (!image || typeof image !== 'string' || !request || typeof request !== 'string') {
        return res.status(400).json({ success: false, errors: ['Request body needs a base64 image string and a request string'] });
      }
      const result = await visualLane.see({ image, request, source: 'upload:api' });
      res.status(result.success ? 200 : visionErrorStatus(result.error)).json(result);
    } catch (err) {
      logger.error('Error in vision see endpoint', err);
      res.status(500).json({ success: false, errors: [err.message] });
    }
  });

  app.get('/api/vision/observations', (req, res) => {
    if (!visualLane) return res.status(503).json({ success: false, errors: ['Visual lane not available'] });
    const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 20, 1), 200);
    res.json(visualLane.listObservations(limit));
  });

  app.get('/api/proposals', (req, res) => {
    if (!proposals) return res.status(503).json({ success: false, errors: ['Proposals not available'] });
    const status = typeof req.query.status === 'string' ? req.query.status : undefined;
    res.json(proposals.list({ status }));
  });

  app.post('/api/proposals/:id/approve', async (req, res) => {
    try {
      if (!proposals) return res.status(503).json({ success: false, errors: ['Proposals not available'] });
      const result = await proposals.approve(req.params.id);
      res.status(result.success ? 200 : (result.proposal ? 409 : 404)).json(result);
    } catch (err) {
      logger.error('Error approving proposal', err);
      res.status(500).json({ success: false, errors: [err.message] });
    }
  });

  app.post('/api/proposals/:id/reject', (req, res) => {
    if (!proposals) return res.status(503).json({ success: false, errors: ['Proposals not available'] });
    const reason = typeof req.body?.reason === 'string' ? req.body.reason.slice(0, 500) : '';
    const result = proposals.reject(req.params.id, reason);
    res.status(result.success ? 200 : (result.proposal ? 409 : 404)).json(result);
  });

  // Agent Chat (SSE and JSON)
  const handleStreamingChat = async (req, res, message, history, mode = 'fast') => {
    const reqId = req.headers['x-request-id'] || req.body?.reqId || Math.random().toString(36).slice(2, 9);
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    if (typeof res.flushHeaders === 'function') res.flushHeaders();

    const startTime = Date.now();
    let firstTokenLogged = false;

    logger.ai(`[reqId:${reqId}] Agent chat streaming initiated (${mode} mode): "${message}"`);

    const keepaliveTimer = setInterval(() => {
      try {
        res.write(': keepalive\n\n');
      } catch { }
    }, 10_000);

    let terminalSent = false;

    try {
      if (typeof chatWithAgentStream === 'function') {
        for await (const chunk of chatWithAgentStream(message, history || [], db, { reqId, mode })) {
          if (!firstTokenLogged && chunk.type === 'token') {
            firstTokenLogged = true;
            logger.ai(`[reqId:${reqId}] First-token latency: ${Date.now() - startTime}ms`);
          }
          if (chunk.type === 'done' || chunk.type === 'error') {
            terminalSent = true;
          }
          res.write(`data: ${JSON.stringify(chunk)}\n\n`);
        }
      } else if (typeof chatWithAgent === 'function') {
        const result = await chatWithAgent(message, history || [], db, { reqId, mode });
        terminalSent = true;
        res.write(`data: ${JSON.stringify({ type: 'done', ...result })}\n\n`);
      } else {
        terminalSent = true;
        res.write(`data: ${JSON.stringify({ type: 'error', message: 'Chat agent not available', operations: null, modelAvailable: false })}\n\n`);
      }
    } catch (err) {
      logger.error(`[reqId:${reqId}] Error during chat stream iteration`, err);
      if (!terminalSent) {
        terminalSent = true;
        const isTimeout = err.name === 'TimeoutError' || err.name === 'AbortError' || err.message?.includes('timeout') || err.message?.includes('aborted');
        const errMsg = isTimeout
          ? 'The local model did not respond within 90 seconds. Check Ollama CPU/GPU usage and selected model.'
          : (err.message || 'Stream processing error');
        res.write(`data: ${JSON.stringify({ type: 'error', message: errMsg, operations: null, modelAvailable: false })}\n\n`);
      }
    } finally {
      clearInterval(keepaliveTimer);
      if (!terminalSent) {
        try {
          res.write(`data: ${JSON.stringify({ type: 'done', message: 'The local model finished processing.', operations: null, modelAvailable: true })}\n\n`);
        } catch { }
      }
      res.end();
    }
  };

  app.post('/api/agent/chat/stream', async (req, res) => {
    try {
      const { message, history, mode } = req.body || {};
      if (!message || typeof message !== 'string') {
        return res.status(400).json({ success: false, errors: ['Missing message string in request body'] });
      }
      await handleStreamingChat(req, res, message, history, mode || 'fast');
    } catch (err) {
      logger.error('Error in agent chat stream endpoint', err);
      if (!res.headersSent) res.status(500).json({ success: false, errors: [err.message] });
      else res.end();
    }
  });

  app.post('/api/agent/chat', async (req, res) => {
    try {
      const { message, history, mode } = req.body;
      if (!message || typeof message !== 'string') {
        return res.status(400).json({ success: false, errors: ['Missing message string in request body'] });
      }

      const wantsStream = req.headers.accept?.includes('text/event-stream') || req.query.stream === 'true';
      if (wantsStream) {
        return await handleStreamingChat(req, res, message, history, mode || 'fast');
      }

      if (typeof chatWithAgent !== 'function') {
        return res.status(500).json({ success: false, errors: ['Agent chat handler not available'] });
      }

      const reqId = req.headers['x-request-id'] || req.body?.reqId || Math.random().toString(36).slice(2, 9);
      logger.ai(`[reqId:${reqId}] Agent chat message received (${mode || 'fast'} mode): "${message}"`);
      const result = await chatWithAgent(message, history || [], db, { reqId, mode: mode || 'fast' });
      res.json({
        success: true,
        message: result.message,
        operations: result.operations,
        modelAvailable: result.modelAvailable,
        raw: result.raw
      });
    } catch (err) {
      logger.error('Error in agent chat endpoint', err);
      if (!res.headersSent) {
        res.status(500).json({ success: false, errors: [err.message] });
      } else {
        res.end();
      }
    }
  });

  // ───────────────────────────────────────────────────────────────────────────
  // WEBSOCKET SERVER LIFECYCLE
  // ───────────────────────────────────────────────────────────────────────────
  wss.on('connection', (ws) => {
    logger.info('New dashboard client connected via WebSocket');

    try {
      const dbState = {
        categories: db.prepare('SELECT * FROM categories').all(),
        items: db.prepare('SELECT * FROM items').all(),
        suppliers: db.prepare('SELECT * FROM suppliers').all()
      };

      const meshLogs = db.prepare('SELECT * FROM _mesh_log ORDER BY rowid DESC LIMIT 30').all();

      const initialState = {
        type: 'init',
        apiKey: apiKey || config.API_KEY || 'mesh-dev-key',
        peers: peerRegistry ? peerRegistry.toJSON() : [],
        dbState,
        meshLogs,
        nodes: getMeshNodes(db),
        tasks: getTasks(db),
        approvals: getApprovals(db),
        conflicts: getConflicts(db),
        metrics: getOperationalMetrics(db),
        auditEvents: getAuditEvents(db, 20),
        nodeName: config.NODE_NAME,
        nodeRole: config.NODE_ROLE,
        peerId: node && node.peerId ? node.peerId.toString() : 'unknown',
        modelName: config.OLLAMA_MODEL,
        p2pPort: config.P2P_PORT,
        wsPort: config.WS_PORT
      };

      ws.send(JSON.stringify(initialState));
    } catch (err) {
      logger.error('Failed to send initial state to WS client', err);
    }

    ws.on('message', (message) => {
      try {
        const msg = JSON.parse(message.toString());
        logger.debug('Received message from dashboard client', msg);
      } catch (err) {
        logger.error('Failed to parse incoming WS message', err);
      }
    });
  });

  server.listen(port, () => {
    logger.info(`Dashboard API and WebSocket server listening on port ${port}`);
  });

  if (logger.on) {
    logger.on('log', (logEntry) => {
      broadcastToClients(wss, {
        type: 'log',
        data: logEntry,
        timestamp: Date.now(),
        source: config.NODE_NAME
      });
    });
  }

  return { server, wss };
}

function visionErrorStatus(error = '') {
  if (/unavailable|disabled/i.test(error)) return 503;
  if (/uncertain|validator|planner|evidence/i.test(error)) return 422;
  return 400;
}
