/**
 * Schema validation rules defining the shape, constraints, and relationships of tables.
 * Used for fast, sub-millisecond local validation before committing changes.
 */
const SCHEMA_RULES = {
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
  }
};

/**
 * Validates a transaction operation (INSERT, UPDATE, DELETE) against the predefined SCHEMA_RULES.
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

  // Step 1: Check if the table exists in our schema rules
  const rules = SCHEMA_RULES[tableName];
  if (!rules) {
    errors.push(`Table '${tableName}' does not exist in schema rules.`);
    return { valid: false, errors };
  }

  // Handle DELETE operations
  if (operation === 'DELETE') {
    // For DELETE, we typically just need the primary key(s) to verify existence.
    // Assuming 'id' for most tables or composite keys.
    // Basic existence check could be performed here, but keeping it simple as requested.
    if (!data.id && (!data.item_id || !data.supplier_id)) {
      errors.push('DELETE requires an identifier (id or composite keys).');
    } else {
      let stmt;
      if (data.id) {
        stmt = db.prepare(`SELECT 1 FROM ${tableName} WHERE id = ?`);
        if (!stmt.get(data.id)) errors.push(`Row to delete not found in ${tableName}.`);
      }
    }
    return { valid: errors.length === 0, errors };
  }

  // Determine fields to validate (all for INSERT, provided for UPDATE)
  const fieldsToValidate = operation === 'INSERT' 
    ? Object.keys(rules.types) 
    : Object.keys(data);

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

  // Step 5: Verify foreign keys exist in local DB
  for (const fk of rules.foreignKeys) {
    const fkValue = data[fk.field];
    if (fkValue !== undefined && fkValue !== null) {
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
    if (uniqueValue !== undefined && uniqueValue !== null) {
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
