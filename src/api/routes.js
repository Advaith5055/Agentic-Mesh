/**
 * @fileoverview Interactive CLI using Node.js readline for controlling the Agentic Mesh node.
 */

import * as readline from 'node:readline';
import chalk from 'chalk';
import { config } from '../utils/config.js';

/**
 * Starts the Interactive CLI using readline.
 * @param {Object} context - Application context providing methods to execute commands.
 * @param {Object} context.node - The libp2p node instance.
 * @param {Object} context.db - The database instance (e.g., better-sqlite3).
 * @param {Object} context.peerRegistry - The peer registry.
 * @param {Function} context.broadcastTransaction - Broadcasts tx to peers.
 * @param {Function} context.executeOperations - Executes planned operations.
 * @param {Function} context.planOperations - Plans ops from natural language via AI.
 * @param {Function} context.auditRecentTransactions - Runs AI audit on recent tx.
 * @param {Function} context.routeTask - Routes a task payload.
 * @param {Function} context.executeSingleTransaction - Executes a single tx locally.
 * @param {Object} [context.wss] - The WebSocket server instance.
 */
export function startCLI(context) {
  const {
    node,
    db,
    peerRegistry,
    executeOperations,
    planOperations,
    auditRecentTransactions,
    executeSingleTransaction,
    requestSync,
    wss // Passed to allow graceful shutdown
  } = context;

  // 1. Create readline interface on stdin/stdout
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });

  // 2. Print welcome banner with node name, peer ID, and available commands
  console.log(chalk.cyan(`
╔═══════════════════════════════════════════════╗
║                                               ║
║           AGENTIC MESH NODE CLI               ║
║                                               ║
╚═══════════════════════════════════════════════╝
`));
  console.log(chalk.bold.green(`➤ Node Name: ${config.NODE_NAME}`));
  console.log(chalk.bold.green(`➤ Node Role: ${config.NODE_ROLE}`));
  console.log(chalk.bold.green(`➤ Peer ID:   ${node && node.peerId ? node.peerId.toString() : 'N/A'}`));
  console.log('');
  console.log(chalk.yellow('Available Commands:'));
  console.log(chalk.gray('  propose <json>         ') + 'Fast path direct transaction');
  console.log(chalk.gray('  ask <natural language> ') + 'AI path');
  console.log(chalk.gray('  bad                    ') + 'Generate deliberately malformed payload');
  console.log(chalk.gray('  peers                  ') + 'List discovered peers');
  console.log(chalk.gray('  db                     ') + 'Show database state');
  console.log(chalk.gray('  sync                   ') + 'Force sync with peers');
  console.log(chalk.gray('  audit                  ') + 'Trigger background AI audit');
  console.log(chalk.gray('  status                 ') + 'Show node info');
  console.log(chalk.gray('  help                   ') + 'Show available commands');
  console.log(chalk.gray('  quit | exit            ') + 'Graceful shutdown');
  console.log('');

  // 3. Set prompt
  rl.setPrompt(chalk.blueBright(`[${config.NODE_NAME}:${config.NODE_ROLE}] > `));
  rl.prompt();

  // 4. Handle commands
  rl.on('line', async (line) => {
    const input = line.trim();
    if (!input) {
      rl.prompt();
      return;
    }

    // Split input into command and arguments
    const firstSpaceIdx = input.indexOf(' ');
    const command = firstSpaceIdx === -1 ? input.toLowerCase() : input.substring(0, firstSpaceIdx).toLowerCase();
    const argsString = firstSpaceIdx === -1 ? '' : input.substring(firstSpaceIdx + 1).trim();

    try {
      switch (command) {
        case 'propose':
          // Fast path direct transaction
          if (!argsString) {
            console.log(chalk.red('Error: Please provide a JSON payload.'));
            break;
          }
          try {
            const payload = JSON.parse(argsString);
            const txResult = await executeSingleTransaction(payload);
            if (txResult.success) {
              console.log(chalk.green('✔ Transaction proposed and executed successfully.'), txResult);
            } else {
              console.log(chalk.red('✘ Transaction rejected:'), txResult.errors?.join(' | '));
            }
          } catch (err) {
            console.log(chalk.red(`✘ Failed to propose transaction: ${err.message}`));
          }
          break;

        case 'ask':
          // AI path
          if (!argsString) {
            console.log(chalk.red('Error: Please provide natural language instructions.'));
            break;
          }
          console.log(chalk.yellow('⌛ Planning operations using AI...'));
          try {
            const plan = await planOperations(argsString);
            if (!plan.success) {
              console.log(chalk.red(`✘ AI planning failed: ${plan.error || 'Unknown error'}`));
              break;
            }
            console.log(chalk.cyan('Planned Operations:\n'), JSON.stringify(plan.operations, null, 2));
            
            console.log(chalk.yellow('⌛ Executing operations...'));
            const results = await executeOperations(plan.operations);
            console.log(chalk.green('✔ Execution Results:\n'), JSON.stringify(results, null, 2));
          } catch (err) {
            console.log(chalk.red(`✘ AI processing failed: ${err.message}`));
          }
          break;

        case 'bad': {
          // Generate deliberately malformed payload
          const badPayload = {
            table: 'items',
            operation: 'INSERT',
            data: { category_id: 999, name: 'Bad Item', price: -5, sku: 'INVALID' }
          };
          console.log(chalk.yellow('Executing bad payload with orphaned FK and invalid price...'));
          try {
            const badRes = await executeSingleTransaction(badPayload);
            if (!badRes.success) {
              console.log(chalk.green('✔ Error correctly caught by transaction execution:'), badRes.errors?.join(' | '));
            } else {
              console.log(chalk.red('✘ Unexpected success on bad payload'));
            }
          } catch (err) {
            console.log(chalk.green('✔ Error correctly caught:'), err.message);
          }
          break;
        }

        case 'peers':
          // List discovered peers
          if (peerRegistry) {
            const peers = peerRegistry.toJSON ? peerRegistry.toJSON() : peerRegistry.getPeers();
            console.log(chalk.cyan(`Discovered Peers (${peers.length}):`));
            if (peers.length > 0) {
              console.table(peers);
            }
          } else {
            console.log(chalk.yellow('Peer registry is not available.'));
          }
          break;

        case 'connect':
        case 'dial':
          if (!argsString) {
            console.log(chalk.red('Usage: connect <multiaddr or IP:port>'));
            console.log(chalk.gray('Example: connect /ip4/192.168.1.15/tcp/9003/p2p/12D3KooW...'));
            console.log(chalk.gray('Example: connect 192.168.1.15:9003'));
            break;
          }
          try {
            let targetAddr = argsString.trim();
            if (targetAddr.includes(':') && !targetAddr.startsWith('/')) {
              const [ip, port] = targetAddr.split(':');
              targetAddr = `/ip4/${ip}/tcp/${port}`;
            }
            console.log(chalk.yellow(`⌛ Dialing peer ${targetAddr}...`));
            if (node && typeof node.dial === 'function') {
              const { multiaddr } = await import('@multiformats/multiaddr');
              await node.dial(multiaddr(targetAddr));
              console.log(chalk.green(`✔ Connected successfully to ${targetAddr}!`));
            } else {
              console.log(chalk.red('✘ Node dial service not available'));
            }
          } catch (err) {
            console.log(chalk.red(`✘ Dial failed: ${err.message}`));
          }
          break;

        case 'db':
          // Show database state
          if (db) {
            try {
              const categories = db.prepare('SELECT * FROM categories').all();
              const items = db.prepare('SELECT * FROM items').all();
              console.log(chalk.cyan(`\n--- Categories (${categories.length}) ---`));
              console.table(categories);
              console.log(chalk.cyan(`\n--- Items (${items.length}) ---`));
              console.table(items);
            } catch (err) {
              console.log(chalk.red('✘ Failed to query database:'), err.message);
            }
          } else {
            console.log(chalk.yellow('Database instance is not available.'));
          }
          break;

        case 'sync':
          // Force sync with peers
          console.log(chalk.yellow('Broadcasting SYNC_REQUEST to mesh peers...'));
          try {
            if (typeof requestSync === 'function') {
              await requestSync();
              console.log(chalk.green('✔ Sync request broadcasted.'));
            } else {
              console.log(chalk.yellow('Sync function not available in CLI context.'));
            }
          } catch (err) {
            console.log(chalk.red(`✘ Failed to sync: ${err.message}`));
          }
          break;

        case 'audit':
          // Trigger background AI audit
          console.log(chalk.yellow('⌛ Triggering background AI audit on recent transactions...'));
          try {
            const auditResult = await auditRecentTransactions();
            console.log(chalk.green('✔ Audit Completed:'));
            console.log(JSON.stringify(auditResult, null, 2));
          } catch (err) {
            console.log(chalk.red('✘ Audit failed:'), err.message);
          }
          break;

        case 'status':
          // Show node info
          console.log(chalk.cyan('\n--- Node Status ---'));
          console.log(`Node Name:    ${config.NODE_NAME}`);
          console.log(`Node Role:    ${config.NODE_ROLE}`);
          console.log(`Peer ID:      ${node && node.peerId ? node.peerId.toString() : 'N/A'}`);
          
          if (node && typeof node.getMultiaddrs === 'function') {
            const addrs = node.getMultiaddrs().map((a) => a.toString());
            console.log(`Multiaddrs:   ${addrs.length > 0 ? addrs.join(', ') : 'None'}`);
          }
          
          if (peerRegistry && typeof peerRegistry.getPeers === 'function') {
            console.log(`Peer Count:   ${peerRegistry.getPeers().length}`);
          }
          
          if (db) {
            try {
              const count = db.prepare('SELECT COUNT(*) as c FROM items').get().c;
              console.log(`Items count:  ${count}`);
            } catch (_e) {}
          }
          console.log('');
          break;

        case 'help':
          // Show available commands
          console.log(chalk.yellow('Available Commands:'));
          console.log('  propose <json>         - Fast path direct transaction');
          console.log('  ask <natural language> - AI path');
          console.log('  bad                    - Generate deliberately malformed payload');
          console.log('  peers                  - List discovered peers');
          console.log('  db                     - Show database state');
          console.log('  sync                   - Force sync with peers');
          console.log('  audit                  - Trigger background AI audit');
          console.log('  status                 - Show node info');
          console.log('  help                   - Show available commands');
          console.log('  quit | exit            - Graceful shutdown');
          break;

        case 'quit':
        case 'exit':
          // Graceful shutdown
          console.log(chalk.yellow('Shutting down gracefully...'));
          try {
            if (node && typeof node.stop === 'function') {
              await node.stop();
            }
            if (db && typeof db.close === 'function') {
              db.close();
            }
            if (wss && typeof wss.close === 'function') {
              wss.close();
            }
          } catch (err) {
            console.error('Error during shutdown:', err);
          }
          process.exit(0);
          break;

        default:
          // Handle unknown commands gracefully
          console.log(chalk.red(`✘ Unknown command: '${command}'. Type 'help' to see available commands.`));
          break;
      }
    } catch (globalErr) {
      console.log(chalk.red('✘ Uncaught error during command execution:'), globalErr.message);
    }

    // Always re-prompt after command finishes
    rl.prompt();
  });
}
