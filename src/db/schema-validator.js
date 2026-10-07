/**
 * Schema validation rules defining the shape, constraints, and relationships of tables.
 * Used for fast, sub-millisecond local validation before committing changes.
 */
export const SCHEMA_RULES = {
  // Legacy inventory tables (preserved for demo data)
  items: {
    required: ['category_id', 'name', 'price', 'sku'],
    types: { category_id: 'number', name: 'string', price: 'number', sku: 'string', id: 'number', created_at: 'string' },
    checks: [{ field: 'price', check: v => v > 0, message: 'price must be greater than 0' }],
    foreignKeys: [{ field: 'category_id', refTable: 'categories', refField: 'id' }],
    uniques: ['sku']
  },
  categories: {
    required: ['name'],
    types: { name: 'string', description: 'string', id: 'number' },
    checks: [],
    foreignKeys: [],
    uniques: ['name']
  },
  suppliers: {
    required: ['name'],
    types: { name: 'string', contact_email: 'string', id: 'number' },
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

  // Production Distributed Agent Workflow Platform Tables
  mesh_nodes: {
    required: ['id', 'name', 'role', 'peer_id'],
    types: {
      id: 'string',
      name: 'string',
      role: 'string',
      peer_id: 'string',
      status: 'string',
      address: 'string',
      software_version: 'string',
      last_seen_at: 'string'
    },
    checks: [
      {
        field: 'status',
        check: v => ['active', 'offline', 'drained', 'degraded'].includes(v),
        message: 'status must be one of: active, offline, drained, degraded'
      }
    ],
    foreignKeys: [],
    uniques: ['peer_id']
  },

  node_capabilities: {
    required: ['id', 'node_id'],
    types: {
      id: 'string',
      node_id: 'string',
      models: 'string',
      tools: 'string',
      max_concurrency: 'number',
      supported_task_types: 'string',
      resource_details: 'string',
      updated_at: 'string'
    },
    checks: [
      { field: 'max_concurrency', check: v => v >= 1, message: 'max_concurrency must be at least 1' }
    ],
    foreignKeys: [{ field: 'node_id', refTable: 'mesh_nodes', refField: 'id' }],
    uniques: []
  },

  tasks: {
    required: ['id', 'title', 'user_prompt'],
    types: {
      id: 'string',
      title: 'string',
      user_prompt: 'string',
      task_type: 'string',
      priority: 'string',
      status: 'string',
      requested_by: 'string',
      assigned_node_id: 'string',
      created_at: 'string',
      updated_at: 'string'
    },
    checks: [
      {
        field: 'status',
        check: v => ['queued', 'planned', 'awaiting_approval', 'executing', 'completed', 'failed', 'cancelled'].includes(v),
        message: 'status must be one of: queued, planned, awaiting_approval, executing, completed, failed, cancelled'
      }
    ],
    foreignKeys: [
      { field: 'assigned_node_id', refTable: 'mesh_nodes', refField: 'id' }
    ],
    uniques: []
  },

  task_runs: {
    required: ['id', 'task_id'],
    types: {
      id: 'string',
      task_id: 'string',
      planner_node: 'string',
      executor_node: 'string',
      validator_node: 'string',
      state: 'string',
      start_time: 'string',
      end_time: 'string',
      error_summary: 'string',
      execution_metrics: 'string'
    },
    checks: [
      {
        field: 'state',
        check: v => ['initialized', 'running', 'completed', 'failed', 'cancelled'].includes(v),
        message: 'state must be one of: initialized, running, completed, failed, cancelled'
      }
    ],
    foreignKeys: [{ field: 'task_id', refTable: 'tasks', refField: 'id' }],
    uniques: []
  },

  task_steps: {
    required: ['id', 'task_id', 'step_number', 'title'],
    types: {
      id: 'string',
      task_id: 'string',
      run_id: 'string',
      step_number: 'number',
      title: 'string',
      status: 'string',
      input_json: 'string',
      output_json: 'string',
      started_at: 'string',
      completed_at: 'string'
    },
    checks: [
      { field: 'step_number', check: v => v >= 1, message: 'step_number must be greater than or equal to 1' }
    ],
    foreignKeys: [{ field: 'task_id', refTable: 'tasks', refField: 'id' }],
    uniques: []
  },

  artifacts: {
    required: ['id', 'task_id', 'name', 'artifact_type', 'content'],
    types: {
      id: 'string',
      task_id: 'string',
      run_id: 'string',
      name: 'string',
      artifact_type: 'string',
      content: 'string',
      metadata_json: 'string',
      created_at: 'string'
    },
    checks: [],
    foreignKeys: [{ field: 'task_id', refTable: 'tasks', refField: 'id' }],
    uniques: []
  },

  approvals: {
    required: ['id', 'request_description'],
    types: {
      id: 'string',
      task_id: 'string',
      run_id: 'string',
      request_description: 'string',
      risk_level: 'string',
      reviewer: 'string',
      decision: 'string',
      decision_time: 'string',
      reason: 'string',
      created_at: 'string'
    },
    checks: [
      {
        field: 'decision',
        check: v => ['pending', 'approved', 'rejected'].includes(v),
        message: 'decision must be one of: pending, approved, rejected'
      }
    ],
    foreignKeys: [{ field: 'task_id', refTable: 'tasks', refField: 'id' }],
    uniques: []
  },

  audit_events: {
    required: ['id', 'entity_type', 'entity_id', 'action', 'actor'],
    types: {
      id: 'string',
      entity_type: 'string',
      entity_id: 'string',
      action: 'string',
      actor: 'string',
      details_json: 'string',
      timestamp: 'string'
    },
    checks: [],
    foreignKeys: [],
    uniques: []
  },

  conflicts: {
    required: ['id', 'conflict_type', 'affected_entity_type', 'affected_entity_id'],
    types: {
      id: 'string',
      conflict_type: 'string',
      affected_entity_type: 'string',
      affected_entity_id: 'string',
      peer_id: 'string',
      details_json: 'string',
      resolution_state: 'string',
      chosen_resolution: 'string',
      resolved_at: 'string',
      created_at: 'string'
    },
    checks: [
      {
        field: 'resolution_state',
        check: v => ['detected', 'resolving', 'resolved', 'ignored'].includes(v),
        message: 'resolution_state must be one of: detected, resolving, resolved, ignored'
      }
    ],
    foreignKeys: [],
    uniques: []
  },

  replication_metrics: {
    required: ['id', 'source_peer_id', 'target_peer_id', 'latency_ms', 'status'],
    types: {
      id: 'string',
      source_peer_id: 'string',
      target_peer_id: 'string',
      latency_ms: 'number',
      status: 'string',
      vector_clock_meta: 'string',
      bytes_transferred: 'number',
      timestamp: 'string'
    },
    checks: [
      { field: 'latency_ms', check: v => v >= 0, message: 'latency_ms must be non-negative' }
    ],
    foreignKeys: [],
    uniques: []
  }
};

/**
 * Strict allowlists to prevent SQL injection or unknown table/column manipulations.
 */
export const ALLOWED_OPERATIONS = new Set(['INSERT', 'UPDATE', 'DELETE']);

export const ALLOWED_TABLES = new Set([
  // Legacy inventory tables
  'categories',
  'items',
  'suppliers',
  'item_suppliers',

  // Workflow platform tables
  'mesh_nodes',
  'node_capabilities',
  'tasks',
  'task_runs',
  'task_steps',
  'artifacts',
  'approvals',
  'audit_events',
  'conflicts',
  'replication_metrics'
]);

export const ALLOWED_COLUMNS = {
  // Legacy inventory
  categories: new Set(['id', 'name', 'description']),
  items: new Set(['id', 'category_id', 'name', 'price', 'sku', 'created_at']),
  suppliers: new Set(['id', 'name', 'contact_email']),
  item_suppliers: new Set(['item_id', 'supplier_id']),

  // Workflow platform
  mesh_nodes: new Set(['id', 'name', 'role', 'peer_id', 'status', 'address', 'software_version', 'last_seen_at']),
  node_capabilities: new Set(['id', 'node_id', 'models', 'tools', 'max_concurrency', 'supported_task_types', 'resource_details', 'updated_at']),
  tasks: new Set(['id', 'title', 'user_prompt', 'task_type', 'priority', 'status', 'requested_by', 'assigned_node_id', 'created_at', 'updated_at']),
  task_runs: new Set(['id', 'task_id', 'planner_node', 'executor_node', 'validator_node', 'state', 'start_time', 'end_time', 'error_summary', 'execution_metrics']),
  task_steps: new Set(['id', 'task_id', 'run_id', 'step_number', 'title', 'status', 'input_json', 'output_json', 'started_at', 'completed_at']),
  artifacts: new Set(['id', 'task_id', 'run_id', 'name', 'artifact_type', 'content', 'metadata_json', 'created_at']),
  approvals: new Set(['id', 'task_id', 'run_id', 'request_description', 'risk_level', 'reviewer', 'decision', 'decision_time', 'reason', 'created_at']),
  audit_events: new Set(['id', 'entity_type', 'entity_id', 'action', 'actor', 'details_json', 'timestamp']),
  conflicts: new Set(['id', 'conflict_type', 'affected_entity_type', 'affected_entity_id', 'peer_id', 'details_json', 'resolution_state', 'chosen_resolution', 'resolved_at', 'created_at']),
  replication_metrics: new Set(['id', 'source_peer_id', 'target_peer_id', 'latency_ms', 'status', 'vector_clock_meta', 'bytes_transferred', 'timestamp'])
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
      // Build the query to check uniqueness, excluding current row for UPDATE operations
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
