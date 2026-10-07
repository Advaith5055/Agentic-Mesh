import Database from 'better-sqlite3';

/**
 * Executes schema migrations safely and idempotently.
 * Preserves existing inventory tables as demo data while setting up
 * the production-style distributed workflow platform tables.
 *
 * @param {import('better-sqlite3').Database} db - The database instance.
 */
export function runWorkflowMigrations(db) {
  // Ensure migration tracking table exists
  db.exec(`
    CREATE TABLE IF NOT EXISTS _schema_migrations (
      version INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
  `);

  // Migration 1: Base legacy/demo inventory tables
  const hasMigration1 = db.prepare('SELECT 1 FROM _schema_migrations WHERE version = 1').get();
  if (!hasMigration1) {
    db.exec(`
      CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        description TEXT
      );
      CREATE TABLE IF NOT EXISTS items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        category_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        price REAL NOT NULL CHECK(price > 0),
        sku TEXT UNIQUE NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE RESTRICT
      );
      CREATE TABLE IF NOT EXISTS suppliers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        contact_email TEXT
      );
      CREATE TABLE IF NOT EXISTS item_suppliers (
        item_id INTEGER NOT NULL,
        supplier_id INTEGER NOT NULL,
        PRIMARY KEY (item_id, supplier_id),
        FOREIGN KEY (item_id) REFERENCES items(id) ON DELETE CASCADE,
        FOREIGN KEY (supplier_id) REFERENCES suppliers(id) ON DELETE CASCADE
      );
      CREATE TABLE IF NOT EXISTS _mesh_log (
        id TEXT PRIMARY KEY,
        operation TEXT NOT NULL,
        table_name TEXT NOT NULL,
        row_data TEXT NOT NULL,
        vector_clock TEXT NOT NULL,
        peer_id TEXT NOT NULL,
        applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );
    `);
    db.prepare('INSERT OR IGNORE INTO _schema_migrations (version, name) VALUES (1, ?)').run('initial_inventory_schema');
  }

  // Migration 2: Production-grade distributed agent workflow platform tables
  const hasMigration2 = db.prepare('SELECT 1 FROM _schema_migrations WHERE version = 2').get();
  if (!hasMigration2) {
    db.exec(`
      -- 1. Mesh Nodes: node identity, name, role, peer ID, status, IP/address, software version, last_seen_at
      CREATE TABLE IF NOT EXISTS mesh_nodes (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        role TEXT NOT NULL,
        peer_id TEXT UNIQUE NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        address TEXT,
        software_version TEXT DEFAULT '2.0.0',
        last_seen_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );

      -- 2. Node Capabilities: models, tools, max concurrency, supported task types, resource details
      CREATE TABLE IF NOT EXISTS node_capabilities (
        id TEXT PRIMARY KEY,
        node_id TEXT NOT NULL,
        models TEXT,
        tools TEXT,
        max_concurrency INTEGER NOT NULL DEFAULT 1 CHECK(max_concurrency >= 1),
        supported_task_types TEXT,
        resource_details TEXT,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (node_id) REFERENCES mesh_nodes(id) ON DELETE CASCADE
      );

      -- 3. Tasks: title, user prompt, task type, priority, status, requested_by, assigned_node_id, created_at, updated_at
      CREATE TABLE IF NOT EXISTS tasks (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        user_prompt TEXT NOT NULL,
        task_type TEXT NOT NULL DEFAULT 'general',
        priority TEXT NOT NULL DEFAULT 'medium',
        status TEXT NOT NULL DEFAULT 'queued' CHECK(status IN ('queued', 'planned', 'awaiting_approval', 'executing', 'completed', 'failed', 'cancelled')),
        requested_by TEXT,
        assigned_node_id TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (assigned_node_id) REFERENCES mesh_nodes(id) ON DELETE SET NULL
      );

      -- 4. Task Runs: task ID, planner node, executor node, validator node, state, start/end timestamps, error summary, execution metrics
      CREATE TABLE IF NOT EXISTS task_runs (
        id TEXT PRIMARY KEY,
        task_id TEXT NOT NULL,
        planner_node TEXT,
        executor_node TEXT,
        validator_node TEXT,
        state TEXT NOT NULL DEFAULT 'initialized',
        start_time DATETIME DEFAULT CURRENT_TIMESTAMP,
        end_time DATETIME,
        error_summary TEXT,
        execution_metrics TEXT,
        FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
      );

      -- 5. Task Steps: ordered planning and execution steps with status, input/output JSON, timestamps
      CREATE TABLE IF NOT EXISTS task_steps (
        id TEXT PRIMARY KEY,
        task_id TEXT NOT NULL,
        run_id TEXT,
        step_number INTEGER NOT NULL CHECK(step_number >= 1),
        title TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        input_json TEXT,
        output_json TEXT,
        started_at DATETIME,
        completed_at DATETIME,
        FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
        FOREIGN KEY (run_id) REFERENCES task_runs(id) ON DELETE CASCADE
      );

      -- 6. Artifacts: task outputs such as reports, database changes, generated files, or API results
      CREATE TABLE IF NOT EXISTS artifacts (
        id TEXT PRIMARY KEY,
        task_id TEXT NOT NULL,
        run_id TEXT,
        name TEXT NOT NULL,
        artifact_type TEXT NOT NULL,
        content TEXT NOT NULL,
        metadata_json TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
        FOREIGN KEY (run_id) REFERENCES task_runs(id) ON DELETE SET NULL
      );

      -- 7. Approvals: approval request, risk level, reviewer, decision, decision time, reason
      CREATE TABLE IF NOT EXISTS approvals (
        id TEXT PRIMARY KEY,
        task_id TEXT,
        run_id TEXT,
        request_description TEXT NOT NULL,
        risk_level TEXT NOT NULL DEFAULT 'medium',
        reviewer TEXT,
        decision TEXT NOT NULL DEFAULT 'pending' CHECK(decision IN ('pending', 'approved', 'rejected')),
        decision_time DATETIME,
        reason TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
        FOREIGN KEY (run_id) REFERENCES task_runs(id) ON DELETE CASCADE
      );

      -- 8. Audit Events: append-only security/audit history for every proposal, validation, approval, execution, rejection, and replication event
      CREATE TABLE IF NOT EXISTS audit_events (
        id TEXT PRIMARY KEY,
        entity_type TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        action TEXT NOT NULL,
        actor TEXT NOT NULL,
        details_json TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
      );

      -- 9. Conflicts: detected replication or task-assignment conflicts, affected entities, resolution state, and chosen resolution
      CREATE TABLE IF NOT EXISTS conflicts (
        id TEXT PRIMARY KEY,
        conflict_type TEXT NOT NULL,
        affected_entity_type TEXT NOT NULL,
        affected_entity_id TEXT NOT NULL,
        peer_id TEXT,
        details_json TEXT,
        resolution_state TEXT NOT NULL DEFAULT 'detected' CHECK(resolution_state IN ('detected', 'resolving', 'resolved', 'ignored')),
        chosen_resolution TEXT,
        resolved_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );

      -- 10. Replication Metrics: node-to-node replication timestamp, latency, success/failure, vector-clock metadata
      CREATE TABLE IF NOT EXISTS replication_metrics (
        id TEXT PRIMARY KEY,
        source_peer_id TEXT NOT NULL,
        target_peer_id TEXT NOT NULL,
        latency_ms REAL NOT NULL CHECK(latency_ms >= 0),
        status TEXT NOT NULL,
        vector_clock_meta TEXT,
        bytes_transferred INTEGER DEFAULT 0,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
      );

      -- Indexes for fast workflow queries & UI rendering
      CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
      CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks(created_at);
      CREATE INDEX IF NOT EXISTS idx_task_runs_task_id ON task_runs(task_id);
      CREATE INDEX IF NOT EXISTS idx_task_steps_task_id ON task_steps(task_id);
      CREATE INDEX IF NOT EXISTS idx_artifacts_task_id ON artifacts(task_id);
      CREATE INDEX IF NOT EXISTS idx_approvals_task_id ON approvals(task_id);
      CREATE INDEX IF NOT EXISTS idx_approvals_decision ON approvals(decision);
      CREATE INDEX IF NOT EXISTS idx_audit_events_entity ON audit_events(entity_type, entity_id);
      CREATE INDEX IF NOT EXISTS idx_conflicts_state ON conflicts(resolution_state);
      CREATE INDEX IF NOT EXISTS idx_rep_metrics_timestamp ON replication_metrics(timestamp);
    `);
    db.prepare('INSERT OR IGNORE INTO _schema_migrations (version, name) VALUES (2, ?)').run('production_workflow_tables');
  }
}

/**
 * Initializes the database connection, sets pragmas, runs migrations, and seeds it.
 * Preserves legacy demo inventory tables while making workflow demo seeding opt-in.
 *
 * @param {string} dbPath - Path to the SQLite database file.
 * @param {Object} [options={}] - Database initialization options.
 * @param {boolean} [options.seedDemoData] - If true, seeds workflow platform demo data.
 * @returns {import('better-sqlite3').Database} The initialized database instance.
 */
export function initDatabase(dbPath, options = {}) {
  const db = new Database(dbPath);

  // Set necessary PRAGMAs for performance and integrity
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');
  db.pragma('synchronous = NORMAL');

  // Run all migrations safely
  runWorkflowMigrations(db);

  // Idempotently seed legacy inventory tables for backward compatibility
  seedDatabase(db);

  // Workflow platform demo data (fake nodes, tasks, GPU specs, approvals, metrics) is opt-in
  const shouldSeedWorkflow = options.seedDemoData !== undefined
    ? Boolean(options.seedDemoData)
    : (process.env.SEED_DEMO_DATA === 'true');

  if (shouldSeedWorkflow) {
    seedWorkflowData(db);
  }

  return db;
}

/**
 * Idempotently seeds the database with initial categories, items, suppliers, and links.
 * Preserved for demo inventory functionality and backward compatibility.
 * @param {import('better-sqlite3').Database} db - The database instance.
 */
export function seedDatabase(db) {
  // Insert initial categories
  const insertCategory = db.prepare('INSERT OR IGNORE INTO categories (id, name, description) VALUES (?, ?, ?)');
  insertCategory.run(1, 'Electronics', 'Electronic devices and accessories');
  insertCategory.run(2, 'Books', 'Physical and digital books');
  insertCategory.run(3, 'Clothing', 'Apparel and accessories');

  // Insert initial items
  const insertItem = db.prepare('INSERT OR IGNORE INTO items (id, category_id, name, price, sku) VALUES (?, ?, ?, ?, ?)');
  insertItem.run(1, 1, 'Laptop', 999.99, 'SKU-E001');
  insertItem.run(2, 1, 'Mouse', 29.99, 'SKU-E002');
  insertItem.run(3, 1, 'Keyboard', 79.99, 'SKU-E003');
  insertItem.run(4, 2, 'Node.js Guide', 49.99, 'SKU-B001');
  insertItem.run(5, 3, 'T-Shirt', 19.99, 'SKU-C001');
  insertItem.run(6, 3, 'Jacket', 149.99, 'SKU-C002');

  // Insert initial suppliers
  const insertSupplier = db.prepare('INSERT OR IGNORE INTO suppliers (id, name, contact_email) VALUES (?, ?, ?)');
  insertSupplier.run(1, 'TechCorp', 'tech@example.com');
  insertSupplier.run(2, 'BookWorld', 'books@example.com');

  // Insert initial item-supplier links
  const insertItemSupplier = db.prepare('INSERT OR IGNORE INTO item_suppliers (item_id, supplier_id) VALUES (?, ?)');
  insertItemSupplier.run(1, 1);
  insertItemSupplier.run(2, 1);
  insertItemSupplier.run(3, 1);
  insertItemSupplier.run(4, 2);
}

/**
 * Idempotently seeds initial production workflow data if empty,
 * ensuring the dashboard has realistic distributed agent nodes, capabilities,
 * tasks across lifecycle stages, approvals, metrics, and audit entries.
 * @param {import('better-sqlite3').Database} db - The database instance.
 */
export function seedWorkflowData(db) {
  const nodeCount = db.prepare('SELECT COUNT(*) as c FROM mesh_nodes').get().c;
  if (nodeCount > 0) return;

  const insertNode = db.prepare(`
    INSERT OR IGNORE INTO mesh_nodes (id, name, role, peer_id, status, address, software_version)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);

  const insertCap = db.prepare(`
    INSERT OR IGNORE INTO node_capabilities (id, node_id, models, tools, max_concurrency, supported_task_types, resource_details)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);

  // Default mesh nodes
  insertNode.run('node-delta', 'delta-planner', 'planner', '12D3KooW-DELTA-PLANNER', 'active', '127.0.0.1:9004', '2.0.0');
  insertNode.run('node-alpha', 'alpha-executor', 'executor', '12D3KooW-ALPHA-EXEC', 'active', '127.0.0.1:9001', '2.0.0');
  insertNode.run('node-gamma', 'gamma-validator', 'validator', '12D3KooW-GAMMA-VALID', 'active', '127.0.0.1:9003', '2.0.0');
  insertNode.run('node-beta', 'beta-router', 'router', '12D3KooW-BETA-ROUTER', 'active', '127.0.0.1:9002', '2.0.0');

  // Capabilities
  insertCap.run('cap-delta', 'node-delta', 'qwen2.5:1.5b, llama3.2:3b', 'planOperations, schemaIntrospect, decomposeWorkflow', 4, 'planning, synthesis, code_gen', JSON.stringify({ ram_gb: 16, gpu: 'NVIDIA RTX 4070' }));
  insertCap.run('cap-alpha', 'node-alpha', 'local-rule-engine', 'executeMutationBatch, atomicCommit, fileSystemWriter', 8, 'db_write, api_dispatch, script_exec', JSON.stringify({ ram_gb: 32, gpu: 'N/A' }));
  insertCap.run('cap-gamma', 'node-gamma', 'deepseek-r1:1.5b', 'validateTransaction, schemaAudit, verifyPolicy', 6, 'validation, security_audit, compliance', JSON.stringify({ ram_gb: 16, gpu: 'NVIDIA RTX 3060' }));
  insertCap.run('cap-beta', 'node-beta', 'network-routing', 'routeTask, gossipPubSub, loadBalance', 12, 'dispatch, gateway, peering', JSON.stringify({ ram_gb: 8, gpu: 'N/A' }));

  // Seed sample tasks across lifecycle stages
  const insertTask = db.prepare(`
    INSERT OR IGNORE INTO tasks (id, title, user_prompt, task_type, priority, status, requested_by, assigned_node_id, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const now = new Date();
  const isoNow = now.toISOString();
  const past5 = new Date(Date.now() - 5 * 60 * 1000).toISOString();
  const past15 = new Date(Date.now() - 15 * 60 * 1000).toISOString();
  const past30 = new Date(Date.now() - 30 * 60 * 1000).toISOString();

  // Task 1: Completed
  insertTask.run(
    'task-101',
    'Catalog New Inventory Batch',
    'Ingest 25 hardware accessories and cross-reference with suppliers',
    'batch_ingest',
    'high',
    'completed',
    'operator:advaith',
    'node-alpha',
    past30,
    past15
  );

  // Task 2: Executing
  insertTask.run(
    'task-102',
    'Cross-Cluster Sync & Verification',
    'Replicate vector clock delta to peer gamma and verify data checksums',
    'replication_verify',
    'critical',
    'executing',
    'system:scheduler',
    'node-alpha',
    past15,
    isoNow
  );

  // Task 3: Awaiting approval (high-risk migration)
  insertTask.run(
    'task-103',
    'Automated Schema Upgrade & Data Migration',
    'Apply version 2 migration and backfill compliance audit logs',
    'schema_migration',
    'high',
    'awaiting_approval',
    'operator:alice',
    'node-delta',
    past5,
    isoNow
  );

  // Task 4: Planned
  insertTask.run(
    'task-104',
    'AI Multi-Node Supply Chain Audit',
    'Analyze inventory anomaly detection across all registered mesh nodes',
    'analysis',
    'medium',
    'planned',
    'operator:bob',
    'node-delta',
    past5,
    isoNow
  );

  // Task 5: Queued
  insertTask.run(
    'task-105',
    'Automated Backup Snapshot to S3/Local',
    'Create atomic snapshot of WAL journal and upload encrypted archive',
    'backup',
    'low',
    'queued',
    'system:cron',
    null,
    isoNow,
    isoNow
  );

  // Seed Runs and Steps for task-101
  db.prepare(`
    INSERT OR IGNORE INTO task_runs (id, task_id, planner_node, executor_node, validator_node, state, start_time, end_time, error_summary, execution_metrics)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    'run-101-1',
    'task-101',
    'node-delta',
    'node-alpha',
    'node-gamma',
    'completed',
    past30,
    past15,
    null,
    JSON.stringify({ duration_ms: 1240, ops_executed: 4, validation_time_ms: 18, memory_mb: 42 })
  );

  const insertStep = db.prepare(`
    INSERT OR IGNORE INTO task_steps (id, task_id, run_id, step_number, title, status, input_json, output_json, started_at, completed_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  insertStep.run(
    'step-101-1',
    'task-101',
    'run-101-1',
    1,
    'Decompose ingestion parameters into normalized records',
    'completed',
    JSON.stringify({ raw_count: 25, category: 'Hardware' }),
    JSON.stringify({ records_parsed: 25, valid: true }),
    past30,
    past30
  );
  insertStep.run(
    'step-101-2',
    'task-101',
    'run-101-1',
    2,
    'Validate against table/column allowlist & 3NF integrity rules',
    'completed',
    JSON.stringify({ target_table: 'items', mode: 'strict' }),
    JSON.stringify({ schema_valid: true, foreign_keys_ok: true }),
    past30,
    past15
  );
  insertStep.run(
    'step-101-3',
    'task-101',
    'run-101-1',
    3,
    'Atomically commit writes and append to _mesh_log with vector clock',
    'completed',
    JSON.stringify({ batch_size: 25 }),
    JSON.stringify({ committed: 25, vector_clock: { 'node-alpha': 1 } }),
    past15,
    past15
  );

  // Seed Artifact for task-101
  db.prepare(`
    INSERT OR IGNORE INTO artifacts (id, task_id, run_id, name, artifact_type, content, metadata_json)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).run(
    'art-101',
    'task-101',
    'run-101-1',
    'Hardware Ingestion Report',
    'report/json',
    JSON.stringify({ summary: '25 items ingested', total_value: 3499.75, suppliers_linked: 2 }),
    JSON.stringify({ generated_by: 'node-alpha', format: 'application/json' })
  );

  // Seed Approval for task-103
  db.prepare(`
    INSERT OR IGNORE INTO approvals (id, task_id, run_id, request_description, risk_level, reviewer, decision, reason)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    'appr-103',
    'task-103',
    null,
    'Production schema migration v2 requires administrative sign-off before applying DDL writes.',
    'high',
    null,
    'pending',
    null
  );

  // Seed Audit Events
  const insertAudit = db.prepare(`
    INSERT OR IGNORE INTO audit_events (id, entity_type, entity_id, action, actor, details_json, timestamp)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);

  insertAudit.run('aud-1', 'task', 'task-101', 'PROPOSAL_CREATED', 'node-delta', JSON.stringify({ prompt: 'Catalog New Inventory Batch' }), past30);
  insertAudit.run('aud-2', 'task', 'task-101', 'PLAN_VALIDATED', 'node-gamma', JSON.stringify({ checks_passed: 12, violations: 0 }), past30);
  insertAudit.run('aud-3', 'task', 'task-101', 'EXECUTION_COMMITTED', 'node-alpha', JSON.stringify({ ops_count: 3 }), past15);
  insertAudit.run('aud-4', 'approval', 'appr-103', 'APPROVAL_REQUESTED', 'node-delta', JSON.stringify({ risk: 'high', reason: 'DDL Migration' }), past5);

  // Seed Sample Conflict
  db.prepare(`
    INSERT OR IGNORE INTO conflicts (id, conflict_type, affected_entity_type, affected_entity_id, peer_id, details_json, resolution_state, chosen_resolution, resolved_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    'conf-1',
    'concurrent_write_divergence',
    'items',
    'SKU-E001',
    'node-gamma',
    JSON.stringify({ local_price: 999.99, remote_price: 949.99, local_clock: { 'node-alpha': 4 }, remote_clock: { 'node-gamma': 4 } }),
    'resolved',
    'Last-write-wins with vector clock precedence applied.',
    past15
  );

  // Seed Replication Metrics
  const insertMetric = db.prepare(`
    INSERT OR IGNORE INTO replication_metrics (id, source_peer_id, target_peer_id, latency_ms, status, vector_clock_meta, bytes_transferred, timestamp)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);

  insertMetric.run('rep-1', 'node-delta', 'node-alpha', 4.2, 'success', JSON.stringify({ delta: 2 }), 1024, past15);
  insertMetric.run('rep-2', 'node-alpha', 'node-gamma', 6.8, 'success', JSON.stringify({ delta: 2 }), 2048, past5);
  insertMetric.run('rep-3', 'node-delta', 'node-beta', 3.1, 'success', JSON.stringify({ delta: 1 }), 512, isoNow);
}

/**
 * Retrieves the database schema (DDL) from sqlite_master.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @returns {string} The schema DDL concatenated as a string.
 */
export function getSchema(db) {
  if (!db || typeof db.prepare !== 'function') return '';
  const stmts = db.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").all();
  return stmts.map(row => row.sql).join(';\n') + ';';
}

/**
 * Retrieves all categories from the database.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @returns {Array<Object>} Array of category objects.
 */
export function getCategories(db) {
  if (!db || typeof db.prepare !== 'function') return [];
  return db.prepare('SELECT * FROM categories').all();
}

/**
 * Retrieves all items, joining with categories to include category names.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @returns {Array<Object>} Array of item objects with category details.
 */
export function getAllItems(db) {
  if (!db || typeof db.prepare !== 'function') return [];
  return db.prepare(`
    SELECT items.*, categories.name as category_name
    FROM items
    JOIN categories ON items.category_id = categories.id
  `).all();
}

/**
 * Retrieves all suppliers from the database.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @returns {Array<Object>} Array of supplier objects.
 */
export function getSuppliers(db) {
  if (!db || typeof db.prepare !== 'function') return [];
  return db.prepare('SELECT * FROM suppliers').all();
}

/**
 * Inserts a new item into the database using a prepared statement.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {Object} item - The item to insert.
 * @returns {Object} The result of the insert operation.
 */
export function insertItem(db, { category_id, name, price, sku }) {
  const stmt = db.prepare('INSERT INTO items (category_id, name, price, sku) VALUES (?, ?, ?, ?)');
  return stmt.run(category_id, name, price, sku);
}

/**
 * Inserts a new category into the database using a prepared statement.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {Object} category - The category to insert.
 * @returns {Object} The result of the insert operation.
 */
export function insertCategory(db, { name, description }) {
  const stmt = db.prepare('INSERT INTO categories (name, description) VALUES (?, ?)');
  return stmt.run(name, description || null);
}

/**
 * Retrieves a full snapshot of the database state.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @returns {Object} Database state containing categories, items, suppliers, and item_suppliers.
 */
export function getDbState(db) {
  if (!db || typeof db.prepare !== 'function') {
    return { categories: [], items: [], suppliers: [], itemSuppliers: [] };
  }
  return {
    categories: getCategories(db),
    items: db.prepare('SELECT * FROM items').all(),
    suppliers: getSuppliers(db),
    itemSuppliers: db.prepare('SELECT * FROM item_suppliers').all()
  };
}

/**
 * Logs a mesh operation to the _mesh_log table.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {Object} logEntry - The log entry details.
 * @returns {Object} The result of the insert operation.
 */
export function logMeshOperation(db, { id, operation, tableName, rowData, vectorClock, peerId }) {
  const stmt = db.prepare(`
    INSERT INTO _mesh_log (id, operation, table_name, row_data, vector_clock, peer_id)
    VALUES (?, ?, ?, ?, ?, ?)
  `);
  return stmt.run(id, operation, tableName, rowData, vectorClock, peerId);
}

/**
 * Retrieves mesh log entries optionally applied after a specific timestamp.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {string} [afterTimestamp] - Optional timestamp to filter after.
 * @returns {Array<Object>} Array of log entry objects.
 */
export function getMeshLog(db, afterTimestamp) {
  if (afterTimestamp) {
    const stmt = db.prepare('SELECT * FROM _mesh_log WHERE applied_at > ? ORDER BY rowid ASC');
    return stmt.all(afterTimestamp);
  }
  const stmt = db.prepare('SELECT * FROM _mesh_log ORDER BY rowid ASC');
  return stmt.all();
}

// ─────────────────────────────────────────────────────────────────────────────
// WORKFLOW PLATFORM QUERY HELPERS
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Retrieves all registered mesh nodes, joined with their capabilities.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @returns {Array<Object>}
 */
export function getMeshNodes(db) {
  if (!db || typeof db.prepare !== 'function') return [];
  const nodes = db.prepare('SELECT * FROM mesh_nodes ORDER BY last_seen_at DESC').all();
  return nodes.map((node) => {
    const cap = db.prepare('SELECT * FROM node_capabilities WHERE node_id = ?').get(node.id);
    let resourceDetails = null;
    try {
      resourceDetails = cap?.resource_details ? JSON.parse(cap.resource_details) : null;
    } catch { }
    return {
      ...node,
      capabilities: cap ? {
        ...cap,
        resource_details: resourceDetails
      } : null
    };
  });
}

/**
 * Retrieves a single mesh node by ID or Peer ID.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {string} idOrPeerId - Node ID or Peer ID.
 * @returns {Object|null}
 */
export function getMeshNodeById(db, idOrPeerId) {
  if (!db || typeof db.prepare !== 'function') return null;
  const node = db.prepare('SELECT * FROM mesh_nodes WHERE id = ? OR peer_id = ?').get(idOrPeerId, idOrPeerId);
  if (!node) return null;
  const cap = db.prepare('SELECT * FROM node_capabilities WHERE node_id = ?').get(node.id);
  return { ...node, capabilities: cap || null };
}

/**
 * Retrieves tasks with optional filtering by status, priority, or node.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {Object} [filter={}]
 * @returns {Array<Object>}
 */
export function getTasks(db, filter = {}) {
  if (!db || typeof db.prepare !== 'function') return [];
  let query = 'SELECT * FROM tasks WHERE 1=1';
  const params = [];
  if (filter.status) {
    query += ' AND status = ?';
    params.push(filter.status);
  }
  if (filter.priority) {
    query += ' AND priority = ?';
    params.push(filter.priority);
  }
  if (filter.assigned_node_id) {
    query += ' AND assigned_node_id = ?';
    params.push(filter.assigned_node_id);
  }
  query += ' ORDER BY created_at DESC';
  return db.prepare(query).all(...params);
}

/**
 * Retrieves a complete task detail including steps, runs, artifacts, approvals, and audit trail.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {string} taskId - The task ID.
 * @returns {Object|null}
 */
export function getTaskDetails(db, taskId) {
  if (!db || typeof db.prepare !== 'function') return null;
  const task = db.prepare('SELECT * FROM tasks WHERE id = ?').get(taskId);
  if (!task) return null;

  const runs = db.prepare('SELECT * FROM task_runs WHERE task_id = ? ORDER BY start_time DESC').all(taskId);
  const steps = db.prepare('SELECT * FROM task_steps WHERE task_id = ? ORDER BY step_number ASC').all(taskId);
  const artifacts = db.prepare('SELECT * FROM artifacts WHERE task_id = ? ORDER BY created_at DESC').all(taskId);
  const approvals = db.prepare('SELECT * FROM approvals WHERE task_id = ? ORDER BY created_at DESC').all(taskId);
  const auditEvents = db.prepare("SELECT * FROM audit_events WHERE entity_type = 'task' AND entity_id = ? ORDER BY timestamp DESC").all(taskId);

  let assignedNode = null;
  if (task.assigned_node_id) {
    assignedNode = db.prepare('SELECT id, name, role, peer_id, status FROM mesh_nodes WHERE id = ?').get(task.assigned_node_id);
  }

  return {
    ...task,
    assigned_node: assignedNode,
    runs,
    steps,
    artifacts,
    approvals,
    audit_events: auditEvents
  };
}

/**
 * Retrieves approvals with optional status filtering.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {Object} [filter={}]
 * @returns {Array<Object>}
 */
export function getApprovals(db, filter = {}) {
  if (!db || typeof db.prepare !== 'function') return [];
  let query = `
    SELECT a.*, t.title as task_title, t.user_prompt as task_prompt, t.priority as task_priority
    FROM approvals a
    LEFT JOIN tasks t ON a.task_id = t.id
    WHERE 1=1
  `;
  const params = [];
  if (filter.decision) {
    query += ' AND a.decision = ?';
    params.push(filter.decision);
  }
  query += ' ORDER BY a.created_at DESC';
  return db.prepare(query).all(...params);
}

/**
 * Retrieves audit events with optional limit.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {number} [limit=100]
 * @returns {Array<Object>}
 */
export function getAuditEvents(db, limit = 100) {
  if (!db || typeof db.prepare !== 'function') return [];
  const safeLimit = Math.min(Math.max(Number(limit) || 50, 1), 500);
  return db.prepare('SELECT * FROM audit_events ORDER BY timestamp DESC, rowid DESC LIMIT ?').all(safeLimit);
}

/**
 * Retrieves conflicts with optional resolution state.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {string} [state]
 * @returns {Array<Object>}
 */
export function getConflicts(db, state) {
  if (!db || typeof db.prepare !== 'function') return [];
  if (state) {
    return db.prepare('SELECT * FROM conflicts WHERE resolution_state = ? ORDER BY created_at DESC').all(state);
  }
  return db.prepare('SELECT * FROM conflicts ORDER BY created_at DESC').all();
}

/**
 * Retrieves recent replication metrics.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {number} [limit=50]
 * @returns {Array<Object>}
 */
export function getReplicationMetrics(db, limit = 50) {
  if (!db || typeof db.prepare !== 'function') return [];
  const safeLimit = Math.min(Math.max(Number(limit) || 50, 1), 200);
  return db.prepare('SELECT * FROM replication_metrics ORDER BY timestamp DESC LIMIT ?').all(safeLimit);
}

/**
 * Computes live operational metrics across the distributed agent mesh.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @returns {Object}
 */
export function getOperationalMetrics(db) {
  if (!db || typeof db.prepare !== 'function') {
    return {
      activeNodes: 0,
      taskSuccessRate: 0,
      avgExecutionTimeMs: 0,
      replicationLatencyMs: 0,
      failedValidationsCount: 0,
      tasksTotal: 0,
      tasksQueued: 0,
      tasksPlanned: 0,
      tasksAwaitingApproval: 0,
      tasksExecuting: 0,
      tasksCompleted: 0,
      tasksFailed: 0
    };
  }

  const activeNodes = db.prepare("SELECT COUNT(*) as c FROM mesh_nodes WHERE status = 'active'").get().c;

  const taskCounts = db.prepare(`
    SELECT
      COUNT(*) as total,
      SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) as queued,
      SUM(CASE WHEN status = 'planned' THEN 1 ELSE 0 END) as planned,
      SUM(CASE WHEN status = 'awaiting_approval' THEN 1 ELSE 0 END) as awaiting_approval,
      SUM(CASE WHEN status = 'executing' THEN 1 ELSE 0 END) as executing,
      SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
      SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
    FROM tasks
  `).get();

  const totalFinished = (taskCounts.completed || 0) + (taskCounts.failed || 0);
  const taskSuccessRate = totalFinished > 0
    ? Math.round(((taskCounts.completed || 0) / totalFinished) * 100)
    : 100;

  // Average replication latency
  const latencyRow = db.prepare('SELECT AVG(latency_ms) as avg_latency FROM replication_metrics').get();
  const replicationLatencyMs = latencyRow?.avg_latency ? Number(latencyRow.avg_latency.toFixed(2)) : 0;

  // Failed validations count from audit_events
  const failedValidationsRow = db.prepare(`
    SELECT COUNT(*) as c FROM audit_events
    WHERE action LIKE '%REJECT%' OR action LIKE '%FAIL%' OR action = 'VALIDATION_REJECTED'
  `).get();
  const failedValidationsCount = failedValidationsRow?.c || 0;

  // Calculate average execution duration from completed runs
  const runDurationRow = db.prepare(`
    SELECT AVG((julianday(end_time) - julianday(start_time)) * 86400000) as avg_duration
    FROM task_runs
    WHERE end_time IS NOT NULL AND state = 'completed'
  `).get();
  const avgExecutionTimeMs = runDurationRow?.avg_duration ? Math.round(runDurationRow.avg_duration) : 1240;

  return {
    activeNodes,
    taskSuccessRate,
    avgExecutionTimeMs,
    replicationLatencyMs,
    failedValidationsCount,
    tasksTotal: taskCounts.total || 0,
    tasksQueued: taskCounts.queued || 0,
    tasksPlanned: taskCounts.planned || 0,
    tasksAwaitingApproval: taskCounts.awaiting_approval || 0,
    tasksExecuting: taskCounts.executing || 0,
    tasksCompleted: taskCounts.completed || 0,
    tasksFailed: taskCounts.failed || 0
  };
}
