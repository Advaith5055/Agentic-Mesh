/**
 * Schema validation rules defining the shape, constraints, and relationships of tables.
 * Used for fast, sub-millisecond local validation before committing changes.
 */
export const SCHEMA_RULES = {
  items: {
    required: ['category_id', 'name', 'price', 'sku'],
    types: { category_id: 'number', name: 'string', price: 'number', sku: 'string' },
    checks: [{ field: 'price', check: v => v > 0, message: 'price must be greater than 0' }],
    foreignKeys: [{ field: 'category_id', refTable: 'categories', refField: 'id' }],
    uniques: ['sku']
  },
  categories: {
    required: ['name'],
    types: { name: 'string', description: 'string' }, // description is optional but typed
    checks: [],
    foreignKeys: [],
    uniques: ['name']
  },
  suppliers: {
    required: ['name'],
    types: { name: 'string', contact_email: 'string' },
    checks: [],
    foreignKeys: [],
    uniques: ['name']
  },
  item_suppliers: {
    required: ['item_id', 'supplier_id'],
    types: { item_id: 'number', supplier_id: 'number' },
    checks: [],
    foreignKeys: [
      { field: 'item_id', refTable: 'items', refField: 'id' },
      { field: 'supplier_id', refTable: 'suppliers', refField: 'id' }
    ],
    uniques: []
  },
  // Vision observations are append-only records produced by the vision model.
  observations: {
    operations: ['INSERT'],
    required: ['id', 'peer_id', 'source', 'image_sha256', 'summary', 'confidence', 'model'],
    types: {
      id: 'string', peer_id: 'string', source: 'string', image_sha256: 'string', question: 'string',
      summary: 'string', fields: 'string', visible_text: 'string', confidence: 'string', model: 'string'
    },
    checks: [
      { field: 'id', check: v => /^obs-[A-Za-z0-9-]{8,64}$/.test(v), message: "id must look like 'obs-<uuid>'" },
      { field: 'image_sha256', check: v => /^[a-f0-9]{64}$/.test(v), message: 'image_sha256 must be a hex SHA-256 digest' },
      { field: 'confidence', check: v => ['high', 'medium', 'low', 'uncertain'].includes(v), message: 'confidence must be high, medium, low or uncertain' },
      { field: 'summary', check: v => v.length > 0 && v.length <= 2000, message: 'summary must be 1-2000 characters' },
      { field: 'question', check: v => v.length <= 1000, message: 'question must be at most 1000 characters' },
      { field: 'source', check: v => v.length <= 300, message: 'source must be at most 300 characters' },
      { field: 'fields', check: isJsonUnder(8000), message: 'fields must be JSON under 8000 characters' },
      { field: 'visible_text', check: isJsonUnder(8000), message: 'visible_text must be JSON under 8000 characters' }
    ],
    foreignKeys: [],
    uniques: ['id']
  }
};

function isJsonUnder(maxLength) {
  return (v) => {
    if (v.length > maxLength) return false;
    try { JSON.parse(v); return true; } catch { return false; }
  };
}

/**
 * Strict allowlists to prevent SQL injection or unknown table/column manipulations.
 */
export const ALLOWED_OPERATIONS = new Set(['INSERT', 'UPDATE', 'DELETE']);

export const ALLOWED_TABLES = new Set(['categories', 'items', 'suppliers', 'item_suppliers', 'observations']);

export const ALLOWED_COLUMNS = {
  categories: new Set(['id', 'name', 'description']),
  items: new Set(['id', 'category_id', 'name', 'price', 'sku', 'created_at']),
  suppliers: new Set(['id', 'name', 'contact_email']),
  item_suppliers: new Set(['item_id', 'supplier_id']),
  observations: new Set([
    'id', 'peer_id', 'source', 'image_sha256', 'question', 'summary',
    'fields', 'visible_text', 'confidence', 'model'
  ])
};

/**
 * Validates dynamic table and column inputs against allowlists.
 * @param {string} tableName - Target table name.
 * @param {string[]} [columns=[]] - Array of column names to validate.
 * @returns {{ valid: boolean, error?: string }}
 */
export function validateAllowlist(tableName, columns = []) {
  if (!ALLOWED_TABLES.has(tableName)) {
    return { valid: false, error: `Table '${tableName}' is not in allowed tables list.` };
  }
  const allowed = ALLOWED_COLUMNS[tableName];
  for (const col of columns) {
    if (!allowed.has(col)) {
      return { valid: false, error: `Column '${col}' is not allowed on table '${tableName}'.` };
    }
  }
  return { valid: true };
}

/**
 * Validates a transaction operation (INSERT, UPDATE, DELETE) against predefined schema rules.
 * Performs fast JavaScript validation without AI, checking types, constraints, and local DB references.
 *
 * @param {string} operation - The operation type: 'INSERT', 'UPDATE', or 'DELETE'.
 * @param {string} tableName - The name of the table being modified.
 * @param {Object} data - The row data associated with the operation.
 * @param {import('better-sqlite3').Database} db - The database instance used to check foreign keys and uniques.
 * @returns {{ valid: boolean, errors: string[] }} Validation result with any accumulated errors.
 */
export function validateTransaction(operation, tableName, data, db) {
  const errors = [];

  if (!operation || !ALLOWED_OPERATIONS.has(operation)) {
    return {
      valid: false,
      errors: [`Operation '${operation}' is not supported. Must be INSERT, UPDATE, or DELETE.`]
    };
  }

  if (!data || typeof data !== 'object') {
    return { valid: false, errors: ['Transaction data must be an object.'] };
  }

  // Step 0: Validate table and column names against allowlists
  const allowlistCheck = validateAllowlist(tableName, Object.keys(data));
  if (!allowlistCheck.valid) {
    errors.push(allowlistCheck.error);
    return { valid: false, errors };
  }

  // Step 1: Check if the table exists in our schema rules
  const rules = SCHEMA_RULES[tableName];
  if (!rules) {
    errors.push(`Table '${tableName}' does not exist in schema rules.`);
    return { valid: false, errors };
  }

  if (rules.operations && !rules.operations.includes(operation)) {
    errors.push(`Operation '${operation}' is not permitted on table '${tableName}'.`);
    return { valid: false, errors };
  }

  // Handle DELETE operations
  if (operation === 'DELETE') {
    if (tableName === 'item_suppliers') {
      if (data.item_id === undefined || data.item_id === null || data.supplier_id === undefined || data.supplier_id === null) {
        errors.push("DELETE on item_suppliers requires both 'item_id' and 'supplier_id'.");
      } else {
        const stmt = db.prepare('SELECT 1 FROM item_suppliers WHERE item_id = ? AND supplier_id = ?');
        if (!stmt.get(data.item_id, data.supplier_id)) {
          errors.push('Row to delete not found in item_suppliers.');
        }
      }
    } else {
      if (data.id === undefined || data.id === null) {
        errors.push('DELETE requires an identifier (id).');
      } else {
        const stmt = db.prepare(`SELECT 1 FROM ${tableName} WHERE id = ?`);
        if (!stmt.get(data.id)) {
          errors.push(`Row to delete not found in ${tableName}.`);
        }
      }
    }
    return { valid: errors.length === 0, errors };
  }

  // Handle UPDATE identifier checks
  if (operation === 'UPDATE') {
    if (tableName === 'item_suppliers') {
      if (data.item_id === undefined || data.item_id === null || data.supplier_id === undefined || data.supplier_id === null) {
        errors.push("UPDATE on item_suppliers requires both 'item_id' and 'supplier_id'.");
      } else {
        const stmt = db.prepare('SELECT 1 FROM item_suppliers WHERE item_id = ? AND supplier_id = ?');
        if (!stmt.get(data.item_id, data.supplier_id)) {
          errors.push('Row to update not found in item_suppliers.');
        }
      }
    } else {
      if (data.id === undefined || data.id === null) {
        errors.push('UPDATE requires an id field.');
      } else {
        const stmt = db.prepare(`SELECT 1 FROM ${tableName} WHERE id = ?`);
        if (!stmt.get(data.id)) {
          errors.push(`Row to update not found in ${tableName}.`);
        }
      }
    }
    if (errors.length > 0) {
      return { valid: false, errors };
    }
  }

  // Determine fields to validate (all for INSERT, provided for UPDATE)
  const fieldsToValidate = operation === 'INSERT' 
    ? Object.keys(rules.types) 
    : Object.keys(data).filter(k => k !== 'id');

  // Step 2: Check all required fields present (only strict for INSERT)
  if (operation === 'INSERT') {
    for (const req of rules.required) {
      if (data[req] === undefined || data[req] === null) {
        errors.push(`Missing required field: '${req}'.`);
      }
    }
  }

  // Step 3 & 4: Check data types match and run CHECK constraints
  for (const field of fieldsToValidate) {
    const value = data[field];
    if (value === undefined || value === null) continue;

    // Type check
    const expectedType = rules.types[field];
    if (expectedType && typeof value !== expectedType) {
      errors.push(`Invalid type for field '${field}': expected ${expectedType}, got ${typeof value}.`);
    }

    // CHECK constraints
    const fieldChecks = rules.checks.filter(c => c.field === field);
    for (const checkObj of fieldChecks) {
      if (!checkObj.check(value)) {
        errors.push(`Constraint failed for '${field}': ${checkObj.message}.`);
      }
    }
  }

  // Step 5: Verify foreign keys exist in local DB (only if value is a valid SQL primitive)
  for (const fk of rules.foreignKeys) {
    const fkValue = data[fk.field];
    if (fkValue !== undefined && fkValue !== null && (typeof fkValue === 'number' || typeof fkValue === 'string' || typeof fkValue === 'bigint')) {
      const stmt = db.prepare(`SELECT 1 FROM ${fk.refTable} WHERE ${fk.refField} = ?`);
      const exists = stmt.get(fkValue);
      if (!exists) {
        errors.push(`Foreign key violation: ${fk.field} value '${fkValue}' does not exist in ${fk.refTable}.${fk.refField}.`);
      }
    }
  }

  // Step 6: Verify UNIQUE constraints won't be violated
  for (const uniqueField of rules.uniques) {
    const uniqueValue = data[uniqueField];
    if (uniqueValue !== undefined && uniqueValue !== null && (typeof uniqueValue === 'number' || typeof uniqueValue === 'string' || typeof uniqueValue === 'bigint')) {
      // Build the query to check uniqueness, excluding the current row for UPDATE operations
      let query = `SELECT 1 FROM ${tableName} WHERE ${uniqueField} = ?`;
      const queryParams = [uniqueValue];
      
      if (operation === 'UPDATE' && data.id) {
        query += ` AND id != ?`;
        queryParams.push(data.id);
      }

      const stmt = db.prepare(query);
      const conflict = stmt.get(...queryParams);
      if (conflict) {
        errors.push(`Unique constraint violation: ${uniqueField} value '${uniqueValue}' already exists.`);
      }
    }
  }

  return { valid: errors.length === 0, errors };
}
