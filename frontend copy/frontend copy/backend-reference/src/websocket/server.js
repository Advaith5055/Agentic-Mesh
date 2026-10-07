/**
 * @fileoverview Express + WebSocket gateway that pushes real-time events to the React dashboard.
 */

import express from 'express';
import { createServer } from 'node:http';
import { WebSocketServer } from 'ws';
import { logger } from '../utils/logger.js';
import { config } from '../utils/config.js';
import { getAgentStatus } from '../agents/ollama.js';

/**
 * Broadcasts an event to all connected WebSocket clients.
 * @param {WebSocketServer} wss - The WebSocket server instance.
 * @param {Object} event - The event payload to broadcast.
 * @param {string} event.type - Event type (e.g., 'log', 'peer:joined', 'tx:received').
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
 * @param {Object} context.node - The libp2p node instance.
 * @param {Object} context.db - The database instance (e.g., better-sqlite3).
 * @param {Object} context.peerRegistry - The peer registry tracking connected peers.
 * @param {Function} [context.executeSingleTransaction] - Fast path execution.
 * @param {Function} [context.planOperations] - Natural language planner.
 * @param {Function} [context.executeOperations] - Execute operation batch.
 * @param {Function} [context.auditRecentTransactions] - AI audit.
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
    getSystemStats
  } = context;

  // 1. Create Express app with JSON body parser & CORS
  // Vision routes carry base64 images, so only they get a larger body limit.
  const app = express();
  const smallJson = express.json();
  const largeJson = express.json({ limit: Math.ceil(config.VISION_MAX_IMAGE_BYTES * 4 / 3) + 64 * 1024 });
  app.use((req, res, next) => (req.path.startsWith('/api/vision/') ? largeJson : smallJson)(req, res, next));
  app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, x-request-id');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
  });

  // 2. REST endpoints
  app.get('/api/health', async (req, res) => {
    let activeModel = null;
    try {
      const status = await getAgentStatus();
      activeModel = status.activeModel;
    } catch {}

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

  app.get('/api/peers', (req, res) => {
    res.json(peerRegistry ? peerRegistry.toJSON() : []);
  });

  app.get('/api/db/state', (req, res) => {
    try {
      const categories = db.prepare('SELECT * FROM categories').all();
      const items = db.prepare('SELECT * FROM items').all();
      const suppliers = db.prepare('SELECT * FROM suppliers').all();
      res.json({ categories, items, suppliers });
    } catch (err) {
      logger.error('Failed to query DB state', err);
      res.status(500).json({ success: false, errors: ['Failed to query database state'] });
    }
  });

  app.get('/api/db/mesh-log', (req, res) => {
    try {
      const rawLimit = req.query.limit;
      let limit = 50;
      if (rawLimit !== undefined) {
        const parsed = Number(rawLimit);
        if (!Number.isInteger(parsed) || parsed <= 0) {
          return res.status(400).json({ success: false, errors: ['Limit must be a positive integer between 1 and 200'] });
        }
        limit = Math.min(Math.max(parsed, 1), 200);
      }
      const logs = db.prepare('SELECT * FROM _mesh_log ORDER BY rowid DESC LIMIT ?').all(limit);
      res.json(logs);
    } catch (err) {
      logger.error('Failed to query mesh log', err);
      res.status(500).json({ success: false, errors: ['Failed to query mesh log'] });
    }
  });

  app.get('/api/db/schema', (req, res) => {
    try {
      const schema = db.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").all();
      const ddl = schema.map((row) => row.sql).join(';\n') + ';';
      res.type('text/plain').send(ddl);
    } catch (err) {
      logger.error('Failed to query DB schema', err);
      res.status(500).send('Failed to query schema');
    }
  });

  app.post('/api/propose', async (req, res) => {
    try {
      const { table, operation, data } = req.body;
      if (!table || !operation || !data) {
        return res.status(400).json({ success: false, errors: ['Missing table, operation, or data in payload'] });
      }
      if (typeof executeSingleTransaction !== 'function') {
        return res.status(500).json({ success: false, errors: ['executeSingleTransaction handler not available'] });
      }
      const result = await executeSingleTransaction({ table, operation, data });
      if (!result.success) {
        return res.status(400).json(result);
      }
      res.json(result);
    } catch (err) {
      logger.error('Error executing propose endpoint', err);
      res.status(500).json({ success: false, errors: [err.message] });
    }
  });

  app.post('/api/sync', async (req, res) => {
    try {
      if (typeof requestSync !== 'function') {
        return res.status(500).json({ success: false, errors: ['Sync handler not available'] });
      }
      await requestSync();
      res.json({ success: true, message: 'Sync request broadcasted' });
    } catch (err) {
      logger.error('Error executing sync endpoint', err);
      res.status(500).json({ success: false, errors: [err.message] });
    }
  });

  app.post('/api/ask', async (req, res) => {
    try {
      const { prompt, confirm = true } = req.body;
      if (!prompt) return res.status(400).json({ success: false, errors: ['Missing prompt in request body'] });
      if (typeof planOperations !== 'function' || typeof executeOperations !== 'function') {
        return res.status(500).json({ success: false, errors: ['AI agents not available'] });
      }

      logger.ai(`Web dashboard prompt received: "${prompt}"`);
      const plan = await planOperations(prompt);
      if (!plan.success) {
        return res.status(400).json({ success: false, errors: [plan.error || 'AI failed to create plan'], raw: plan.raw });
      }

      if (confirm === false) {
        return res.json({
          success: true,
          requiresConfirmation: true,
          plan
        });
      }

      const execResult = await executeOperations(plan.operations);
      res.json({ success: true, confirmed: true, plan, execResult });
    } catch (err) {
      logger.error('Error executing ask endpoint', err);
      res.status(500).json({ success: false, errors: [err.message] });
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

  // ── Agent activity: communications, task progress, performance ───────────
  app.get('/api/agents/activity', (req, res) => {
    if (!activity) return res.status(503).json({ success: false, errors: ['Activity tracking not available'] });
    res.json(activity.snapshot(typeof getSystemStats === 'function' ? getSystemStats() : null));
  });

  // ── Visual lane: photo → observation → proposal → human approval ─────────
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

  const handleStreamingChat = async (req, res, message, history, mode = 'fast') => {
    const reqId = req.headers['x-request-id'] || req.body?.reqId || Math.random().toString(36).slice(2, 9);
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    if (typeof res.flushHeaders === 'function') res.flushHeaders();

    const startTime = Date.now();
    let firstTokenLogged = false;

    logger.ai(`[reqId:${reqId}] Agent chat streaming initiated (${mode} mode): "${message}"`);

    // Keepalive comment every 10s to prevent proxy/client timeouts
    const keepaliveTimer = setInterval(() => {
      try {
        res.write(': keepalive\n\n');
      } catch {}
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
        } catch {}
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

  // 3. Create HTTP server from Express app
  const server = createServer(app);

  // 4. Create WebSocketServer attached to HTTP server on path '/ws'
  const wss = new WebSocketServer({ server, path: '/ws' });

  // 5. On WebSocket connection
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
        peers: peerRegistry ? peerRegistry.toJSON() : [],
        dbState,
        meshLogs,
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

  // 6. Listen on port
  server.listen(port, () => {
    logger.info(`Dashboard API and WebSocket server listening on port ${port}`);
  });

  // Hook into logger's 'log' event to auto-push log entries to dashboard
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
