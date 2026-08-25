/**
 * @fileoverview Structured logging utility with colorized output and WebSocket event forwarding.
 * @module utils/logger
 */

import chalk from 'chalk';
import { EventEmitter } from 'node:events';
import { config } from './config.js';

/**
 * Logger class extending EventEmitter.
 * Provides colorized console output and emits 'log' events for websocket broadcast.
 * @extends EventEmitter
 */
class Logger extends EventEmitter {
  /**
   * Internal method to format, print, and emit log messages.
   * 
   * @private
   * @param {string} level - The log level (e.g., INFO, P2P, DB).
   * @param {string} message - The message to log.
   * @param {Function} colorFn - The chalk color function for console output.
   */
  _log(level, message, colorFn) {
    const timestamp = new Date().toISOString();
    const nodeName = config.NODE_NAME;
    
    // 1. Print formatted and colored output to console
    const formattedConsoleMsg = `[${timestamp}] [${nodeName}] [${level}] ${message}`;
    console.log(colorFn(formattedConsoleMsg));
    
    // 2. Emit 'log' event for WebSocket forwarding
    this.emit('log', {
      timestamp,
      level,
      message,
      nodeName
    });
  }

  /**
   * Logs a debug message.
   * Output color: Gray
   * 
   * @param {string} msg - The debug message.
   */
  debug(msg) {
    this._log('DEBUG', msg, chalk.gray);
  }

  /**
   * Logs a warning message.
   * Output color: Yellow
   * 
   * @param {string} msg - The warning message.
   */
  warn(msg) {
    this._log('WARN', msg, chalk.yellow);
  }

  /**
   * Logs a general informational message.
   * Output color: Cyan
   * 
   * @param {string} msg - The informational message.
   */
  info(msg) {
    this._log('INFO', msg, chalk.cyan);
  }

  /**
   * Logs a P2P networking related message.
   * Output color: Yellow
   * 
   * @param {string} msg - The P2P network message.
   */
  p2p(msg) {
    this._log('P2P', msg, chalk.yellow);
  }

  /**
   * Logs a database operation related message.
   * Output color: Green
   * 
   * @param {string} msg - The database message.
   */
  db(msg) {
    this._log('DB', msg, chalk.green);
  }

  /**
   * Logs an AI/LLM operation related message.
   * Output color: Magenta
   * 
   * @param {string} msg - The AI process message.
   */
  ai(msg) {
    this._log('AI', msg, chalk.magenta);
  }

  /**
   * Logs an error message.
   * Output color: Red
   * 
   * @param {string} msg - The error message.
   */
  error(msg) {
    this._log('ERROR', msg, chalk.red);
  }

  /**
   * Logs a success message.
   * Output color: Bright Green
   * 
   * @param {string} msg - The success message.
   */
  success(msg) {
    this._log('SUCCESS', msg, chalk.greenBright);
  }
}

/**
 * Singleton instance of the Logger.
 * @type {Logger}
 */
export const logger = new Logger();
