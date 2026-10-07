import Database from 'better-sqlite3';

/**
 * Initializes the database connection, sets pragmas, creates the 3NF schema, and seeds it.
 * @param {string} dbPath - Path to the SQLite database file.
 * @returns {import('better-sqlite3').Database} The initialized database instance.
 */
export function initDatabase(dbPath) {
  // Open the database connection
  const db = new Database(dbPath);

  // Set necessary PRAGMAs for performance and integrity
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');
  db.pragma('synchronous = NORMAL');

  // Define and execute the normalized 3NF schema creation
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
    CREATE TABLE IF NOT EXISTS observations (
      id TEXT PRIMARY KEY,
      peer_id TEXT NOT NULL,
      source TEXT NOT NULL,
      image_sha256 TEXT NOT NULL,
      question TEXT,
      summary TEXT NOT NULL,
      fields TEXT,
      visible_text TEXT,
      confidence TEXT NOT NULL CHECK(confidence IN ('high', 'medium', 'low', 'uncertain')),
      model TEXT NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
  `);

  migrateMeshLog(db);

  // Idempotently seed the database with initial data
  seedDatabase(db);

  return db;
}

/**
 * Adds provenance columns to _mesh_log on databases created before they existed.
 * origin: api | nl | vision | observer; evidence_id: observation a vision write is based on.
 * @param {import('better-sqlite3').Database} db - The database instance.
 */
function migrateMeshLog(db) {
  const columns = db.prepare('PRAGMA table_info(_mesh_log)').all().map(c => c.name);
  if (!columns.includes('origin')) {
    db.exec("ALTER TABLE _mesh_log ADD COLUMN origin TEXT NOT NULL DEFAULT 'api'");
  }
  if (!columns.includes('evidence_id')) {
    db.exec('ALTER TABLE _mesh_log ADD COLUMN evidence_id TEXT');
  }
}

/**
 * Idempotently seeds the database with initial categories, items, suppliers, and links.
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
  insertItemSupplier.run(1, 1); // Laptop -> TechCorp
  insertItemSupplier.run(2, 1); // Mouse -> TechCorp
  insertItemSupplier.run(3, 1); // Keyboard -> TechCorp
  insertItemSupplier.run(4, 2); // Node.js Guide -> BookWorld
}

/**
 * Retrieves the database schema (DDL) from sqlite_master.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {Object} [options={}]
 * @param {string[]} [options.exclude=[]] - Table names to leave out (e.g. to keep LLM prompts short).
 * @returns {string} The schema DDL concatenated as a string.
 */
export function getSchema(db, { exclude = [] } = {}) {
  if (!db || typeof db.prepare !== 'function') return '';
  const stmts = db.prepare("SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").all();
  return stmts.filter(row => !exclude.includes(row.name)).map(row => row.sql).join(';\n') + ';';
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
 * @param {number} item.category_id - The category ID for the item.
 * @param {string} item.name - The name of the item.
 * @param {number} item.price - The price of the item.
 * @param {string} item.sku - The unique SKU of the item.
 * @returns {Object} The result of the insert operation containing lastInsertRowid.
 */
export function insertItem(db, { category_id, name, price, sku }) {
  const stmt = db.prepare('INSERT INTO items (category_id, name, price, sku) VALUES (?, ?, ?, ?)');
  return stmt.run(category_id, name, price, sku);
}

/**
 * Inserts a new category into the database using a prepared statement.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {Object} category - The category to insert.
 * @param {string} category.name - The unique name of the category.
 * @param {string} [category.description] - Optional description of the category.
 * @returns {Object} The result of the insert operation containing lastInsertRowid.
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
 * @param {string} logEntry.id - Unique ID for the operation.
 * @param {string} logEntry.operation - The type of operation (INSERT, UPDATE, DELETE).
 * @param {string} logEntry.tableName - The name of the affected table.
 * @param {string} logEntry.rowData - The serialized row data.
 * @param {string} logEntry.vectorClock - The serialized vector clock at the time of operation.
 * @param {string} logEntry.peerId - The ID of the peer originating the operation.
 * @param {string} [logEntry.origin='api'] - Where the write came from (api, nl, vision, observer).
 * @param {string|null} [logEntry.evidenceId=null] - Observation id a vision write is based on.
 * @returns {Object} The result of the insert operation.
 */
export function logMeshOperation(db, { id, operation, tableName, rowData, vectorClock, peerId, origin = 'api', evidenceId = null }) {
  const stmt = db.prepare(`
    INSERT INTO _mesh_log (id, operation, table_name, row_data, vector_clock, peer_id, origin, evidence_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);
  return stmt.run(id, operation, tableName, rowData, vectorClock, peerId, origin, evidenceId);
}

/**
 * Retrieves the most recent vision observations, newest first, with JSON columns parsed.
 * @param {import('better-sqlite3').Database} db - The database instance.
 * @param {number} [limit=20] - Maximum rows to return.
 * @returns {Array<Object>} Observation rows.
 */
export function getObservations(db, limit = 20) {
  if (!db || typeof db.prepare !== 'function') return [];
  return db.prepare('SELECT * FROM observations ORDER BY rowid DESC LIMIT ?').all(limit).map(row => ({
    ...row,
    fields: parseJsonOr(row.fields, {}),
    visible_text: parseJsonOr(row.visible_text, [])
  }));
}

function parseJsonOr(value, fallback) {
  try { return value ? JSON.parse(value) : fallback; } catch { return fallback; }
}

/**
 * Retrieves mesh log entries optionally applied after a specific timestamp.
 * Uses rowid ASC for deterministic FIFO insertion ordering.
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
