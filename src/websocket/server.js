/**
 * @fileoverview Express + WebSocket gateway that pushes real-time events to the React dashboard.
 */

import express from 'express';
import { createServer } from 'node:http';
import { WebSocketServer } from 'ws';
import { logger } from '../utils/logger.js';
import { config } from '../utils/config.js';

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
  const { node, db, peerRegistry, executeSingleTransaction, planOperations, executeOperations, auditRecentTransactions } = context;

  // 1. Create Express app with JSON body parser & CORS
  const app = express();
  app.use(express.json());
  app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
  });

  // 2. REST endpoints
  app.get('/api/health', (req, res) => {
    res.json({
      status: 'ok',
      nodeName: config.NODE_NAME,
      peerId: node && node.peerId ? node.peerId.toString() : 'unknown',
      uptime: process.uptime()
    });
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
      res.status(500).json({ error: 'Failed to query database state' });
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
        return res.status(400).json({ error: 'Missing table, operation, or data in payload' });
      }
      if (typeof executeSingleTransaction !== 'function') {
        return res.status(500).json({ error: 'executeSingleTransaction handler not available' });
      }
      const result = await executeSingleTransaction({ table, operation, data });
      res.json(result);
    } catch (err) {
      logger.error('Error executing propose endpoint', err);
      res.status(500).json({ error: err.message });
    }
  });

  app.post('/api/ask', async (req, res) => {
    try {
      const { prompt } = req.body;
      if (!prompt) return res.status(400).json({ error: 'Missing prompt in request body' });
      if (typeof planOperations !== 'function' || typeof executeOperations !== 'function') {
        return res.status(500).json({ error: 'AI agents not available' });
      }

      logger.ai(`Web dashboard prompt received: "${prompt}"`);
      const plan = await planOperations(prompt);
      if (!plan.success) {
        return res.status(400).json({ error: plan.error || 'AI failed to create plan', raw: plan.raw });
      }

      const execResult = await executeOperations(plan.operations);
      res.json({ plan, execResult });
    } catch (err) {
      logger.error('Error executing ask endpoint', err);
      res.status(500).json({ error: err.message });
    }
  });

  app.post('/api/audit', async (req, res) => {
    try {
      if (typeof auditRecentTransactions !== 'function') {
        return res.status(500).json({ error: 'Audit handler not available' });
      }
      const result = await auditRecentTransactions(10);
      res.json(result);
    } catch (err) {
      logger.error('Error executing audit endpoint', err);
      res.status(500).json({ error: err.message });
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

      const initialState = {
        type: 'init',
        peers: peerRegistry ? peerRegistry.toJSON() : [],
        dbState,
        nodeName: config.NODE_NAME,
        peerId: node && node.peerId ? node.peerId.toString() : 'unknown'
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
