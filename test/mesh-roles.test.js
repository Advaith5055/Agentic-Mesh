/**
 * @fileoverview Unit and Integration Tests for Mesh Roles (Router, Planner, Executor).
 * 
 * Verifies:
 *   1. NODE_ROLE validation and default behavior.
 *   2. Protocol encoding and decoding for all new message types.
 *   3. PeerRegistry role tracking and findAvailablePeerByRole.
 *   4. Planner handling of a valid PLAN_REQUEST without mutating DB.
 *   5. Planner refusal to execute mutations.
 *   6. Executor revalidation and atomic execution/rollback of remote plans.
 *   7. Request ID correlation and timeout cleanup without memory leaks.
 *   8. Multi-role flow: Router -> Planner -> Executor.
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

import { NodeRole, SUPPORTED_ROLES, validateNodeRole } from '../src/utils/config.js';
import {
  MessageType,
  createEnvelope,
  encode,
  decode,
  createPlanRequest,
  createPlanResponse,
  createExecutionRequest,
  createExecutionResponse,
  createPeerAnnounce
} from '../src/utils/protocol.js';
import { PeerRegistry } from '../src/p2p/discovery.js';
import { MeshCoordinator } from '../src/agents/coordinator.js';
import { initDatabase } from '../src/db/sqlite.js';
import { createSyncEngine } from '../src/db/sync.js';
import { executeOperations } from '../src/agents/executor.js';

describe('Mesh Roles & Architecture', () => {
  let tmpDir;
  let db;
  let syncEngine;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'mesh-roles-test-'));
    db = initDatabase(join(tmpDir, 'test.db'));
    syncEngine = createSyncEngine('test-peer');
  });

  afterEach(() => {
    try {
      db.close();
      rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 1. NODE_ROLE VALIDATION
  // ───────────────────────────────────────────────────────────────────────────
  describe('NODE_ROLE Validation & Defaults', () => {
    it('accepts all supported roles', () => {
      for (const role of SUPPORTED_ROLES) {
        assert.equal(validateNodeRole(role), role);
      }
    });

    it('defaults to peer when undefined or empty', () => {
      assert.equal(validateNodeRole(undefined), NodeRole.PEER);
      assert.equal(validateNodeRole(''), NodeRole.PEER);
      assert.equal(validateNodeRole(null), NodeRole.PEER);
    });

    it('normalizes case and whitespace', () => {
      assert.equal(validateNodeRole(' PLANNER '), NodeRole.PLANNER);
      assert.equal(validateNodeRole('Router'), NodeRole.ROUTER);
      assert.equal(validateNodeRole('EXECUTOR'), NodeRole.EXECUTOR);
    });

    it('gracefully handles legacy ai-agent alias by mapping to peer', () => {
      assert.equal(validateNodeRole('ai-agent'), NodeRole.PEER);
      assert.equal(validateNodeRole('AI-AGENT'), NodeRole.PEER);
    });

    it('rejects unsupported roles with a helpful error message', () => {
      assert.throws(
        () => validateNodeRole('super-agent'),
        /Invalid NODE_ROLE "super-agent"\. Supported roles are: peer, router, planner, executor, validator/
      );
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 2. PROTOCOL ENCODING & DECODING
  // ───────────────────────────────────────────────────────────────────────────
  describe('Protocol Message Encoding & Decoding', () => {
    it('encodes and decodes PLAN_REQUEST correctly', () => {
      const payload = createPlanRequest({
        requestId: 'req-123',
        prompt: 'Add mechanical keyboard',
        requesterPeerId: 'peer-router',
        targetPeerId: 'peer-planner',
        timestamp: 123456789
      });

      const envelope = createEnvelope(MessageType.PLAN_REQUEST, payload, 'peer-router');
      const bytes = encode(envelope);
      const decoded = decode(bytes);

      assert.equal(decoded.type, MessageType.PLAN_REQUEST);
      assert.equal(decoded.payload.requestId, 'req-123');
      assert.equal(decoded.payload.prompt, 'Add mechanical keyboard');
      assert.equal(decoded.payload.requesterPeerId, 'peer-router');
      assert.equal(decoded.payload.targetPeerId, 'peer-planner');
      assert.equal(decoded.payload.timestamp, 123456789);
    });

    it('rejects invalid PLAN_REQUEST missing prompt', () => {
      assert.throws(() => createPlanRequest({ prompt: '' }), /requires a valid string prompt/);
      assert.throws(() => createPlanRequest({ prompt: null }), /requires a valid string prompt/);
    });

    it('encodes and decodes PLAN_RESPONSE correctly with structured operations', () => {
      const operations = [
        { operation: 'INSERT', table: 'items', data: { category_id: 1, name: 'Keychron', price: 99, sku: 'SKU-K1' } }
      ];

      const payload = createPlanResponse({
        requestId: 'req-123',
        success: true,
        operations,
        error: null,
        plannerPeerId: 'peer-planner',
        targetPeerId: 'peer-router',
        raw: '[{"operation":"INSERT",...}]'
      });

      const envelope = createEnvelope(MessageType.PLAN_RESPONSE, payload, 'peer-planner');
      const bytes = encode(envelope);
      const decoded = decode(bytes);

      assert.equal(decoded.type, MessageType.PLAN_RESPONSE);
      assert.equal(decoded.payload.requestId, 'req-123');
      assert.equal(decoded.payload.success, true);
      assert.deepEqual(decoded.payload.operations, operations);
      assert.equal(decoded.payload.plannerPeerId, 'peer-planner');
      assert.equal(decoded.payload.targetPeerId, 'peer-router');
    });

    it('encodes and decodes EXECUTION_REQUEST correctly', () => {
      const operations = [
        { operation: 'INSERT', table: 'categories', data: { name: 'Audio', description: 'Headphones & Mics' } }
      ];

      const payload = createExecutionRequest({
        requestId: 'exec-456',
        operations,
        routerPeerId: 'peer-router',
        plannerPeerId: 'peer-planner',
        targetPeerId: 'peer-executor'
      });

      const envelope = createEnvelope(MessageType.EXECUTION_REQUEST, payload, 'peer-router');
      const decoded = decode(encode(envelope));

      assert.equal(decoded.type, MessageType.EXECUTION_REQUEST);
      assert.equal(decoded.payload.requestId, 'exec-456');
      assert.deepEqual(decoded.payload.operations, operations);
      assert.equal(decoded.payload.routerPeerId, 'peer-router');
      assert.equal(decoded.payload.plannerPeerId, 'peer-planner');
      assert.equal(decoded.payload.targetPeerId, 'peer-executor');
    });

    it('rejects EXECUTION_REQUEST with empty operations', () => {
      assert.throws(
        () => createExecutionRequest({ operations: [], routerPeerId: 'r1' }),
        /requires a non-empty operations array/
      );
    });

    it('encodes and decodes EXECUTION_RESPONSE correctly', () => {
      const payload = createExecutionResponse({
        requestId: 'exec-456',
        success: true,
        summary: { completed: 1, results: [{ rowId: 4 }] },
        error: null,
        executorPeerId: 'peer-executor',
        targetPeerId: 'peer-router'
      });

      const envelope = createEnvelope(MessageType.EXECUTION_RESPONSE, payload, 'peer-executor');
      const decoded = decode(encode(envelope));

      assert.equal(decoded.type, MessageType.EXECUTION_RESPONSE);
      assert.equal(decoded.payload.requestId, 'exec-456');
      assert.equal(decoded.payload.success, true);
      assert.equal(decoded.payload.summary.completed, 1);
      assert.equal(decoded.payload.executorPeerId, 'peer-executor');
      assert.equal(decoded.payload.targetPeerId, 'peer-router');
    });

    it('encodes and decodes PEER_ANNOUNCE correctly', () => {
      const payload = createPeerAnnounce({
        peerId: 'peer-planner-99',
        nodeName: 'planner-laptop',
        nodeRole: 'planner',
        status: 'connected'
      });

      const envelope = createEnvelope(MessageType.PEER_ANNOUNCE, payload, 'peer-planner-99');
      const decoded = decode(encode(envelope));

      assert.equal(decoded.type, MessageType.PEER_ANNOUNCE);
      assert.equal(decoded.payload.nodeName, 'planner-laptop');
      assert.equal(decoded.payload.nodeRole, 'planner');
      assert.equal(decoded.payload.status, 'connected');
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 3. PEER REGISTRY ROLE DISCOVERY
  // ───────────────────────────────────────────────────────────────────────────
  describe('PeerRegistry Role Management', () => {
    it('registers peer and updates role and nodeName', () => {
      const registry = new PeerRegistry();

      registry.registerOrUpdatePeer('peer-1', {
        nodeName: 'node-router',
        role: 'router',
        status: 'connected'
      });

      registry.registerOrUpdatePeer('peer-2', {
        nodeName: 'node-planner',
        role: 'planner',
        status: 'connected'
      });

      registry.registerOrUpdatePeer('peer-3', {
        nodeName: 'node-executor',
        role: 'executor',
        status: 'connected'
      });

      assert.equal(registry.getPeerCount(), 3);
      const planners = registry.getPeersByRole('planner');
      assert.equal(planners.length, 1);
      assert.equal(planners[0].nodeName, 'node-planner');

      const foundPlanner = registry.findAvailablePeerByRole('planner');
      assert.ok(foundPlanner);
      assert.equal(foundPlanner.peerId, 'peer-2');

      const foundValidator = registry.findAvailablePeerByRole('validator');
      assert.equal(foundValidator, null);

      const json = registry.toJSON();
      assert.equal(json.length, 3);
      assert.equal(json[1].role, 'planner');
      assert.equal(json[1].status, 'connected');
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 4. PLANNER NODE BEHAVIOR & IMMUTABILITY
  // ───────────────────────────────────────────────────────────────────────────
  describe('Planner Node Behavior', () => {
    it('handles valid PLAN_REQUEST and produces validated plan without mutating DB', async () => {
      let publishedControlEnvelope = null;
      let broadcastTxCalled = false;

      const mockNode = {
        peerId: { toString: () => 'planner-peer-id' },
        services: {
          pubsub: {
            publish: async (_topic, data) => {
              publishedControlEnvelope = decode(data);
            }
          }
        }
      };

      const registry = new PeerRegistry();

      const coordinator = new MeshCoordinator({
        node: mockNode,
        peerRegistry: registry,
        db,
        syncEngine,
        planOperations: async (prompt, _db) => {
          assert.equal(prompt, 'Add wireless mouse');
          return {
            success: true,
            operations: [
              {
                operation: 'INSERT',
                table: 'items',
                data: { category_id: 1, name: 'Wireless Mouse', price: 29.99, sku: 'SKU-MOUSE-1' }
              }
            ],
            raw: '[]'
          };
        },
        executeOperations: async () => {
          broadcastTxCalled = true;
          throw new Error('Planner must NEVER execute operations');
        },
        broadcastTx: async () => {
          broadcastTxCalled = true;
        }
      });

      const initialItemCount = db.prepare('SELECT COUNT(*) as c FROM items').get().c;

      const planReqEnvelope = createEnvelope(
        MessageType.PLAN_REQUEST,
        createPlanRequest({
          requestId: 'test-req-001',
          prompt: 'Add wireless mouse',
          requesterPeerId: 'router-peer-id'
        }),
        'router-peer-id'
      );

      await coordinator.handlePlanRequest(planReqEnvelope, 'router-peer-id');

      // Verify PLAN_RESPONSE was emitted
      assert.ok(publishedControlEnvelope);
      assert.equal(publishedControlEnvelope.type, MessageType.PLAN_RESPONSE);
      assert.equal(publishedControlEnvelope.payload.requestId, 'test-req-001');
      assert.equal(publishedControlEnvelope.payload.success, true);
      assert.equal(publishedControlEnvelope.payload.operations.length, 1);
      assert.equal(publishedControlEnvelope.payload.operations[0].data.name, 'Wireless Mouse');
      assert.equal(publishedControlEnvelope.payload.targetPeerId, 'router-peer-id');

      // Verify Planner did NOT execute operations, gossip transactions, or mutate DB
      assert.equal(broadcastTxCalled, false);
      const finalItemCount = db.prepare('SELECT COUNT(*) as c FROM items').get().c;
      assert.equal(finalItemCount, initialItemCount, 'Database must not be modified by planning');
    });

    it('rejects plan request if model output fails validateModelPlan', async () => {
      let publishedControlEnvelope = null;

      const mockNode = {
        peerId: { toString: () => 'planner-peer-id' },
        services: {
          pubsub: {
            publish: async (_topic, data) => {
              publishedControlEnvelope = decode(data);
            }
          }
        }
      };

      const coordinator = new MeshCoordinator({
        node: mockNode,
        peerRegistry: new PeerRegistry(),
        db,
        syncEngine,
        planOperations: async () => {
          return {
            success: true,
            operations: [
              // Disallowed operation and table
              { operation: 'DROP_DATABASE', table: 'security_credentials', data: {} }
            ]
          };
        },
        executeOperations: async () => {},
        broadcastTx: async () => {}
      });

      const planReqEnvelope = createEnvelope(
        MessageType.PLAN_REQUEST,
        createPlanRequest({
          requestId: 'bad-plan-001',
          prompt: 'Hack system',
          requesterPeerId: 'router-peer-id'
        }),
        'router-peer-id'
      );

      await coordinator.handlePlanRequest(planReqEnvelope, 'router-peer-id');

      assert.ok(publishedControlEnvelope);
      assert.equal(publishedControlEnvelope.payload.success, false);
      assert.match(publishedControlEnvelope.payload.error, /Plan validation failed/);
      assert.equal(publishedControlEnvelope.payload.operations.length, 0);
    });

    it('skips duplicate PLAN_REQUEST with the same requestId', async () => {
      let publishCount = 0;

      const mockNode = {
        peerId: { toString: () => 'planner-peer-id' },
        services: {
          pubsub: {
            publish: async () => { publishCount++; }
          }
        }
      };

      const coordinator = new MeshCoordinator({
        node: mockNode,
        peerRegistry: new PeerRegistry(),
        db,
        syncEngine,
        planOperations: async () => ({
          success: true,
          operations: [{ operation: 'INSERT', table: 'categories', data: { name: 'TestCat' } }]
        }),
        executeOperations: async () => {},
        broadcastTx: async () => {}
      });

      const req = createEnvelope(
        MessageType.PLAN_REQUEST,
        createPlanRequest({ requestId: 'dedup-001', prompt: 'test', requesterPeerId: 'r1' }),
        'r1'
      );

      await coordinator.handlePlanRequest(req, 'r1');
      assert.equal(publishCount, 1);

      // Send same requestId again
      await coordinator.handlePlanRequest(req, 'r1');
      assert.equal(publishCount, 1, 'Duplicate requestId must be skipped');
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 5. EXECUTOR NODE REVALIDATION & EXECUTION
  // ───────────────────────────────────────────────────────────────────────────
  describe('Executor Node Behavior', () => {
    it('revalidates remote plan and commits valid operations atomically', async () => {
      let responseEnvelope = null;
      let broadcastTxCount = 0;

      const mockNode = {
        peerId: { toString: () => 'executor-peer-id' },
        services: {
          pubsub: {
            publish: async (_topic, data) => {
              responseEnvelope = decode(data);
            }
          }
        }
      };

      const coordinator = new MeshCoordinator({
        node: mockNode,
        peerRegistry: new PeerRegistry(),
        db,
        syncEngine,
        planOperations: async () => {},
        executeOperations: (ops, database, engine, broadcastFn) => {
          return executeOperations(ops, database, engine, broadcastFn);
        },
        broadcastTx: async () => {
          broadcastTxCount++;
        }
      });

      const initialCount = db.prepare('SELECT COUNT(*) as c FROM items').get().c;

      const execReqEnvelope = createEnvelope(
        MessageType.EXECUTION_REQUEST,
        createExecutionRequest({
          requestId: 'exec-req-001',
          operations: [
            {
              operation: 'INSERT',
              table: 'items',
              data: { category_id: 1, name: 'USB-C Cable', price: 12.5, sku: 'SKU-CABLE-1' }
            }
          ],
          routerPeerId: 'router-peer-id',
          plannerPeerId: 'planner-peer-id'
        }),
        'router-peer-id'
      );

      await coordinator.handleExecutionRequest(execReqEnvelope, 'router-peer-id');

      assert.ok(responseEnvelope);
      assert.equal(responseEnvelope.type, MessageType.EXECUTION_RESPONSE);
      assert.equal(responseEnvelope.payload.success, true);
      assert.equal(responseEnvelope.payload.summary.completed, 1);
      assert.equal(responseEnvelope.payload.targetPeerId, 'router-peer-id');

      // Verify DB was modified and transaction was broadcasted
      const finalCount = db.prepare('SELECT COUNT(*) as c FROM items').get().c;
      assert.equal(finalCount, initialCount + 1);
      assert.equal(broadcastTxCount, 1);
    });

    it('rejects invalid remote operations and rolls back completely', async () => {
      let responseEnvelope = null;
      let broadcastTxCount = 0;

      const mockNode = {
        peerId: { toString: () => 'executor-peer-id' },
        services: {
          pubsub: {
            publish: async (_topic, data) => {
              responseEnvelope = decode(data);
            }
          }
        }
      };

      const coordinator = new MeshCoordinator({
        node: mockNode,
        peerRegistry: new PeerRegistry(),
        db,
        syncEngine,
        planOperations: async () => {},
        executeOperations: (ops, database, engine, broadcastFn) => {
          return executeOperations(ops, database, engine, broadcastFn);
        },
        broadcastTx: async () => {
          broadcastTxCount++;
        }
      });

      const initialCount = db.prepare('SELECT COUNT(*) as c FROM items').get().c;

      // Plan containing negative price (violates CHECK price > 0)
      const execReqEnvelope = createEnvelope(
        MessageType.EXECUTION_REQUEST,
        createExecutionRequest({
          requestId: 'exec-fail-001',
          operations: [
            {
              operation: 'INSERT',
              table: 'items',
              data: { category_id: 1, name: 'Invalid Item', price: -50, sku: 'SKU-BAD-1' }
            }
          ],
          routerPeerId: 'router-peer-id'
        }),
        'router-peer-id'
      );

      await coordinator.handleExecutionRequest(execReqEnvelope, 'router-peer-id');

      assert.ok(responseEnvelope);
      assert.equal(responseEnvelope.payload.success, false);
      assert.match(responseEnvelope.payload.error, /execution failed/i);

      // Verify no changes to DB and no gossip broadcast
      const finalCount = db.prepare('SELECT COUNT(*) as c FROM items').get().c;
      assert.equal(finalCount, initialCount);
      assert.equal(broadcastTxCount, 0);
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 6. REQUEST CORRELATION & TIMEOUT CLEANUP
  // ───────────────────────────────────────────────────────────────────────────
  describe('Router Request Correlation & Timeout Cleanup', () => {
    it('correlates concurrent PLAN_REQUESTs safely by requestId', async () => {
      const mockNode = {
        peerId: { toString: () => 'router-peer-id' },
        services: {
          pubsub: {
            publish: async () => {}
          }
        }
      };

      const registry = new PeerRegistry();
      registry.registerOrUpdatePeer('planner-1', { role: 'planner', status: 'connected' });

      const coordinator = new MeshCoordinator({
        node: mockNode,
        peerRegistry: registry,
        db,
        syncEngine,
        planOperations: async () => {},
        executeOperations: async () => {},
        broadcastTx: async () => {}
      });

      // Start two concurrent plan requests
      const p1 = coordinator.requestPlanFromMesh('Query 1', { timeoutMs: 2000 });
      const p2 = coordinator.requestPlanFromMesh('Query 2', { timeoutMs: 2000 });

      assert.equal(coordinator.pendingPlanRequests.size, 2);
      const [reqId1, reqId2] = Array.from(coordinator.pendingPlanRequests.keys());

      // Reply to reqId2 first
      coordinator.handlePlanResponse({
        payload: {
          requestId: reqId2,
          success: true,
          operations: [{ operation: 'INSERT', table: 'items', data: { name: 'Item 2' } }],
          plannerPeerId: 'planner-1'
        }
      }, 'planner-1');

      // Reply to reqId1 second
      coordinator.handlePlanResponse({
        payload: {
          requestId: reqId1,
          success: true,
          operations: [{ operation: 'INSERT', table: 'items', data: { name: 'Item 1' } }],
          plannerPeerId: 'planner-1'
        }
      }, 'planner-1');

      const [res1, res2] = await Promise.all([p1, p2]);

      assert.equal(res1.operations[0].data.name, 'Item 1');
      assert.equal(res2.operations[0].data.name, 'Item 2');
      assert.equal(coordinator.pendingPlanRequests.size, 0, 'Pending map must be cleaned up');
    });

    it('cleans up pending requests on timeout to prevent memory leaks', async () => {
      const mockNode = {
        peerId: { toString: () => 'router-peer-id' },
        services: { pubsub: { publish: async () => {} } }
      };

      const registry = new PeerRegistry();
      registry.registerOrUpdatePeer('planner-1', { role: 'planner', status: 'connected' });

      const coordinator = new MeshCoordinator({
        node: mockNode,
        peerRegistry: registry,
        db,
        syncEngine,
        planOperations: async () => {},
        executeOperations: async () => {},
        broadcastTx: async () => {}
      });

      // Set a short 50ms timeout
      const result = await coordinator.requestPlanFromMesh('Timeout query', { timeoutMs: 50 });

      assert.equal(result.success, false);
      assert.match(result.error, /Timeout/);
      assert.equal(coordinator.pendingPlanRequests.size, 0, 'Timeout must clean up pending map');
    });

    it('returns error when no Planner peer is available and local fallback is disabled', async () => {
      const mockNode = {
        peerId: { toString: () => 'router-peer-id' },
        services: { pubsub: { publish: async () => {} } }
      };

      const coordinator = new MeshCoordinator({
        node: mockNode,
        peerRegistry: new PeerRegistry(), // empty peer registry
        db,
        syncEngine,
        planOperations: async () => {},
        executeOperations: async () => {},
        broadcastTx: async () => {}
      });

      const result = await coordinator.requestPlanFromMesh('Query with no planner');
      assert.equal(result.success, false);
      assert.match(result.error, /No Planner peer available/);
    });
  });

  // ───────────────────────────────────────────────────────────────────────────
  // 7. FULL MULTI-ROLE WORKFLOW (ROUTER -> PLANNER -> EXECUTOR)
  // ───────────────────────────────────────────────────────────────────────────
  describe('Full Multi-Role Mesh Flow', () => {
    it('executes Router -> Planner -> Executor -> Transaction gossip successfully', async () => {
      // 1. Setup shared mock bus
      const messageBus = [];

      const createMockBusNode = (id) => ({
        peerId: { toString: () => id },
        services: {
          pubsub: {
            publish: async (_topic, data) => {
              messageBus.push(decode(data));
            }
          }
        }
      });

      const registryRouter = new PeerRegistry();
      registryRouter.registerOrUpdatePeer('planner-id', { role: 'planner', status: 'connected' });
      registryRouter.registerOrUpdatePeer('executor-id', { role: 'executor', status: 'connected' });

      const coordinatorRouter = new MeshCoordinator({
        node: createMockBusNode('router-id'),
        peerRegistry: registryRouter,
        db,
        syncEngine,
        planOperations: async () => {},
        executeOperations: async () => {},
        broadcastTx: async () => {}
      });

      const coordinatorPlanner = new MeshCoordinator({
        node: createMockBusNode('planner-id'),
        peerRegistry: new PeerRegistry(),
        db,
        syncEngine,
        planOperations: async (prompt) => ({
          success: true,
          operations: [
            {
              operation: 'INSERT',
              table: 'categories',
              data: { name: 'Gaming Gear', description: 'Consoles and accessories' }
            }
          ],
          raw: prompt
        }),
        executeOperations: async () => { throw new Error('Planner must not execute'); },
        broadcastTx: async () => {}
      });

      let executorGossipSent = false;
      const coordinatorExecutor = new MeshCoordinator({
        node: createMockBusNode('executor-id'),
        peerRegistry: new PeerRegistry(),
        db,
        syncEngine,
        planOperations: async () => {},
        executeOperations: (ops, database, engine, broadcastFn) => {
          return executeOperations(ops, database, engine, broadcastFn);
        },
        broadcastTx: async () => { executorGossipSent = true; }
      });

      // ── Step A: Router initiates plan request ─────────────────────────────
      const planPromise = coordinatorRouter.requestPlanFromMesh('Add Gaming Gear category');
      
      // Locate PLAN_REQUEST on bus
      const planReqEnvelope = messageBus.find(m => m.type === MessageType.PLAN_REQUEST);
      assert.ok(planReqEnvelope);
      assert.equal(planReqEnvelope.payload.targetPeerId, 'planner-id');

      // ── Step B: Planner handles request and replies ───────────────────────
      await coordinatorPlanner.handlePlanRequest(planReqEnvelope, 'router-id');

      // Locate PLAN_RESPONSE on bus
      const planRespEnvelope = messageBus.find(m => m.type === MessageType.PLAN_RESPONSE);
      assert.ok(planRespEnvelope);

      // Router receives PLAN_RESPONSE
      coordinatorRouter.handlePlanResponse(planRespEnvelope, 'planner-id');
      const planResult = await planPromise;
      assert.equal(planResult.success, true);
      assert.equal(planResult.operations.length, 1);

      // ── Step C: Router forwards approved plan to Executor ─────────────────
      const execPromise = coordinatorRouter.requestExecutionFromMesh(planResult.operations, {
        plannerPeerId: planResult.plannerPeerId
      });

      // Locate EXECUTION_REQUEST on bus
      const execReqEnvelope = messageBus.find(m => m.type === MessageType.EXECUTION_REQUEST);
      assert.ok(execReqEnvelope);
      assert.equal(execReqEnvelope.payload.targetPeerId, 'executor-id');

      // ── Step D: Executor validates, executes, broadcasts, and replies ─────
      await coordinatorExecutor.handleExecutionRequest(execReqEnvelope, 'router-id');

      // Locate EXECUTION_RESPONSE on bus
      const execRespEnvelope = messageBus.find(m => m.type === MessageType.EXECUTION_RESPONSE);
      assert.ok(execRespEnvelope);

      // Router receives EXECUTION_RESPONSE
      coordinatorRouter.handleExecutionResponse(execRespEnvelope, 'executor-id');
      const execResult = await execPromise;

      assert.equal(execResult.completed, 1);
      assert.equal(execResult.failed, 0);
      assert.equal(executorGossipSent, true, 'Executor must broadcast transaction gossip on commit');

      // Verify category is in DB
      const cat = db.prepare("SELECT * FROM categories WHERE name = 'Gaming Gear'").get();
      assert.ok(cat);
      assert.equal(cat.name, 'Gaming Gear');
    });
  });
});
