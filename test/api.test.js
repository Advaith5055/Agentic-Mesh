import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { initDatabase } from '../src/db/sqlite.js';
import { createSyncEngine } from '../src/db/sync.js';
import { executeSingleTransaction, executeOperations } from '../src/agents/executor.js';
import { startWebSocketServer } from '../src/websocket/server.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

describe('Gateway REST API Endpoints', () => {
  let tmpDir;
  let db;
  let syncEngine;
  let serverInstance;
  let port;
  let baseUrl;

  beforeEach(async () => {
    tmpDir = mkdtempSync(join(tmpdir(), 'mesh-api-test-'));
    db = initDatabase(join(tmpDir, 'api-test.db'));
    syncEngine = createSyncEngine('node-api-test');

    // Pick dynamic port (avoid 11434 which is Ollama's port)
    port = Math.floor(20000 + Math.random() * 30000);
    baseUrl = `http://127.0.0.1:${port}`;

    serverInstance = startWebSocketServer(port, {
      node: { peerId: { toString: () => 'node-api-test' } },
      db,
      peerRegistry: { getPeerCount: () => 0, toJSON: () => [] },
      executeSingleTransaction: (payload) => executeSingleTransaction(payload.operation, payload.table, payload.data, db, syncEngine),
      planOperations: async (prompt) => {
        if (prompt.includes('fail')) {
          return { success: false, error: 'AI failed to plan' };
        }
        return {
          success: true,
          operations: [
            {
              operation: 'INSERT',
              table: 'categories',
              data: { name: 'API Test Category', description: 'Created via API' }
            }
          ]
        };
      },
      executeOperations: (ops) => executeOperations(ops, db, syncEngine),
      auditRecentTransactions: async () => ({ issues: [] }),
      chatWithAgent: async (message, _history, _db, _options) => {
        if (message === 'hi') {
          return {
            message: 'Hello! I am your node agent.',
            operations: null,
            modelAvailable: true
          };
        }
        if (message === 'timeout-test') {
          throw new Error('The local model did not respond within 90 seconds. Check Ollama CPU/GPU usage and selected model.');
        }
        return {
          message: 'Planned 1 item insertion.',
          operations: [{ operation: 'INSERT', table: 'items', data: { name: 'Chat Item', price: 29.99, sku: 'SKU-CHAT-1', category_id: 1 } }],
          modelAvailable: true
        };
      },
      chatWithAgentStream: async function* (message, _history, _db, _options) {
        if (message === 'stream-test') {
          yield { type: 'token', content: 'public class ' };
          yield { type: 'token', content: 'EvenOdd {}' };
          yield { type: 'done', message: 'public class EvenOdd {}', operations: null, modelAvailable: true };
          return;
        }
        if (message === 'timeout-stream') {
          yield { type: 'error', message: 'The local model did not respond within 90 seconds. Check Ollama CPU/GPU usage and selected model.', operations: null, modelAvailable: false };
          return;
        }
        if (message === 'model-unavailable-stream') {
          yield { type: 'error', message: 'Configured model "gemma4:e2b" is not installed in Ollama at http://127.0.0.1:11434. Available models: none.', operations: null, modelAvailable: false };
          return;
        }
        if (message === 'throw-stream') {
          throw new Error('Unexpected stream failure');
        }
        yield { type: 'done', message: 'Default response', operations: null, modelAvailable: true };
      },
      requestSync: async () => {}
    });

    // Wait a brief moment for server to listen
    await new Promise(r => setTimeout(r, 100));
  });

  afterEach(async () => {
    try {
      if (serverInstance?.server) {
        serverInstance.server.close();
      }
      db.close();
      rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  });

  it('validates mesh-log limit parameter correctly', async () => {
    // 1. Invalid limit (negative or NaN) returns 400
    const resBad = await fetch(`${baseUrl}/api/db/mesh-log?limit=-5`);
    assert.equal(resBad.status, 400);
    const dataBad = await resBad.json();
    assert.equal(dataBad.success, false);
    assert.ok(dataBad.errors[0].includes('Limit must be a positive integer'));

    const resNaN = await fetch(`${baseUrl}/api/db/mesh-log?limit=abc`);
    assert.equal(resNaN.status, 400);

    // 2. Default limit returns array
    const resDefault = await fetch(`${baseUrl}/api/db/mesh-log`);
    assert.equal(resDefault.status, 200);
    const logs = await resDefault.json();
    assert.ok(Array.isArray(logs));

    // 3. Clamped limit works
    const resLarge = await fetch(`${baseUrl}/api/db/mesh-log?limit=9999`);
    assert.equal(resLarge.status, 200);
  });

  it('handles /api/ask plan preview with confirm: false', async () => {
    const res = await fetch(`${baseUrl}/api/ask`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ prompt: 'Add API category', confirm: false })
    });

    assert.equal(res.status, 200);
    const data = await res.json();
    assert.equal(data.success, true);
    assert.equal(data.requiresConfirmation, true);
    assert.ok(data.plan);
    assert.equal(data.plan.operations.length, 1);

    // Verify operation was NOT executed yet
    const cat = db.prepare("SELECT * FROM categories WHERE name = 'API Test Category'").get();
    assert.equal(cat, undefined);
  });

  it('executes confirmed plan via /api/ask/confirm', async () => {
    const res = await fetch(`${baseUrl}/api/ask/confirm`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        operations: [
          {
            operation: 'INSERT',
            table: 'categories',
            data: { name: 'API Test Category', description: 'Created via API' }
          }
        ]
      })
    });

    assert.equal(res.status, 200);
    const data = await res.json();
    assert.equal(data.success, true);
    assert.equal(data.confirmed, true);
    assert.equal(data.execResult.completed, 1);

    // Verify operation committed to DB
    const cat = db.prepare("SELECT * FROM categories WHERE name = 'API Test Category'").get();
    assert.ok(cat);
  });

  it('handles conversational messages via /api/agent/chat', async () => {
    const res = await fetch(`${baseUrl}/api/agent/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: 'hi' })
    });

    assert.equal(res.status, 200);
    const data = await res.json();
    assert.equal(data.success, true);
    assert.equal(data.message, 'Hello! I am your node agent.');
    assert.equal(data.operations, null);
    assert.equal(data.modelAvailable, true);
  });

  it('handles planning messages with structured operations via /api/agent/chat', async () => {
    const res = await fetch(`${baseUrl}/api/agent/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: 'add mechanical keyboard' })
    });

    assert.equal(res.status, 200);
    const data = await res.json();
    assert.equal(data.success, true);
    assert.ok(Array.isArray(data.operations));
    assert.equal(data.operations.length, 1);
    assert.equal(data.operations[0].operation, 'INSERT');
  });

  it('returns 400 when message is missing in /api/agent/chat', async () => {
    const res = await fetch(`${baseUrl}/api/agent/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({})
    });

    assert.equal(res.status, 400);
    const data = await res.json();
    assert.equal(data.success, false);
    assert.ok(data.errors[0].includes('Missing message'));
  });

  it('streams response chunks via SSE when Accept: text/event-stream is requested', async () => {
    const res = await fetch(`${baseUrl}/api/agent/chat`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'text/event-stream'
      },
      body: JSON.stringify({ message: 'stream-test' })
    });

    assert.equal(res.status, 200);
    assert.ok(res.headers.get('content-type').includes('text/event-stream'));
    const bodyText = await res.text();
    assert.ok(bodyText.includes('"type":"token"'));
    assert.ok(bodyText.includes('EvenOdd'));
    assert.ok(bodyText.includes('"type":"done"'));
  });

  it('returns model-unavailable error in SSE stream', async () => {
    const res = await fetch(`${baseUrl}/api/agent/chat`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'text/event-stream'
      },
      body: JSON.stringify({ message: 'model-unavailable-stream' })
    });

    assert.equal(res.status, 200);
    const bodyText = await res.text();
    assert.ok(bodyText.includes('"type":"error"'));
    assert.ok(bodyText.includes('not installed in Ollama'));
  });

  it('returns timeout error in SSE stream', async () => {
    const res = await fetch(`${baseUrl}/api/agent/chat`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'text/event-stream'
      },
      body: JSON.stringify({ message: 'timeout-stream' })
    });

    assert.equal(res.status, 200);
    const bodyText = await res.text();
    assert.ok(bodyText.includes('"type":"error"'));
    assert.ok(bodyText.includes('90 seconds'));
  });

  it('sends terminal error event when stream throws unexpectedly', async () => {
    const res = await fetch(`${baseUrl}/api/agent/chat`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'text/event-stream'
      },
      body: JSON.stringify({ message: 'throw-stream' })
    });

    assert.equal(res.status, 200);
    const bodyText = await res.text();
    assert.ok(bodyText.includes('"type":"error"'));
    assert.ok(bodyText.includes('Unexpected stream failure'));
  });

  it('handles client abort cleanly on SSE stream', async () => {
    const controller = new AbortController();
    const fetchPromise = fetch(`${baseUrl}/api/agent/chat`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'text/event-stream'
      },
      body: JSON.stringify({ message: 'stream-test' }),
      signal: controller.signal
    });

    // Abort immediately
    controller.abort();

    await assert.rejects(fetchPromise, { name: 'AbortError' });
  });

  it('truthfully reports node and model status in /api/health and /api/agent/status', async () => {
    const resHealth = await fetch(`${baseUrl}/api/health`);
    assert.equal(resHealth.status, 200);
    const health = await resHealth.json();
    assert.equal(health.status, 'ok');
    assert.ok('configuredModel' in health);
    assert.ok('p2pPort' in health);
    assert.ok('wsPort' in health);
    assert.ok('nodeRole' in health);

    const resStatus = await fetch(`${baseUrl}/api/agent/status`);
    assert.equal(resStatus.status, 200);
    const status = await resStatus.json();
    assert.ok('configuredModel' in status);
    assert.ok('ollamaReachable' in status);
    assert.ok('availableModels' in status);
    assert.ok('nodeRole' in status);
  });
});
