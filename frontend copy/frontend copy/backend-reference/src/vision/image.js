/**
 * @fileoverview Safe image intake for the visual lane.
 * Accepts a file path, a Buffer, or a base64 / data-URL string; checks the size cap
 * and the file signature (JPEG, PNG, WEBP only) before anything reaches a model.
 * @module vision/image
 */

import { createHash } from 'node:crypto';
import { readFileSync, statSync } from 'node:fs';
import { homedir } from 'node:os';
import { basename, resolve } from 'node:path';
import { config } from '../utils/config.js';

/**
 * Detects the image type from its leading bytes.
 * @param {Buffer} buffer
 * @returns {'image/jpeg'|'image/png'|'image/webp'|null}
 */
export function detectImageType(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 12) return null;
  if (buffer[0] === 0xff && buffer[1] === 0xd8 && buffer[2] === 0xff) return 'image/jpeg';
  if (buffer.subarray(0, 8).equals(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]))) return 'image/png';
  if (buffer.toString('ascii', 0, 4) === 'RIFF' && buffer.toString('ascii', 8, 12) === 'WEBP') return 'image/webp';
  return null;
}

/**
 * Loads and checks an image.
 *
 * @param {string|Buffer} input - File path, Buffer, or base64 / data-URL string.
 * @param {Object} [options={}]
 * @param {number} [options.maxBytes] - Size cap (defaults to config.VISION_MAX_IMAGE_BYTES).
 * @param {string} [options.source] - Label stored with the observation (e.g. 'upload:dashboard').
 * @returns {{ buffer: Buffer, base64: string, mime: string, sha256: string, bytes: number, source: string }}
 * @throws {Error} If the input is missing, too large, or not a supported image.
 */
export function loadImage(input, { maxBytes = config.VISION_MAX_IMAGE_BYTES, source } = {}) {
  let buffer;
  let label = source;

  if (Buffer.isBuffer(input)) {
    buffer = input;
    label ||= 'buffer';
  } else if (typeof input === 'string' && input.startsWith('data:')) {
    buffer = decodeBase64(input.slice(input.indexOf(',') + 1), maxBytes);
    label ||= 'upload';
  } else if (typeof input === 'string' && looksLikePath(input)) {
    const path = resolve(input.replace(/^~(?=$|\/)/, homedir()));
    let stats;
    try {
      stats = statSync(path);
    } catch {
      throw new Error(`Image file not found: ${path}`);
    }
    if (!stats.isFile()) throw new Error(`Not a file: ${path}`);
    if (stats.size > maxBytes) throw new Error(`Image is ${stats.size} bytes; the limit is ${maxBytes}.`);
    buffer = readFileSync(path);
    label ||= `file:${basename(path)}`;
  } else if (typeof input === 'string' && input.length > 0) {
    buffer = decodeBase64(input, maxBytes);
    label ||= 'upload';
  } else {
    throw new Error('No image provided.');
  }

  if (buffer.length > maxBytes) {
    throw new Error(`Image is ${buffer.length} bytes; the limit is ${maxBytes}.`);
  }

  const mime = detectImageType(buffer);
  if (!mime) {
    throw new Error('Unsupported image format. Use JPEG, PNG or WEBP.');
  }

  return {
    buffer,
    base64: buffer.toString('base64'),
    mime,
    sha256: createHash('sha256').update(buffer).digest('hex'),
    bytes: buffer.length,
    source: label.slice(0, 300)
  };
}

function looksLikePath(value) {
  return value.length < 1024 && /^(~|\.{0,2}\/|[A-Za-z]:\\)|\.(jpe?g|png|webp)$/i.test(value);
}

function decodeBase64(value, maxBytes) {
  // Base64 inflates size by 4/3; reject oversized strings before decoding them.
  if (value.length > Math.ceil(maxBytes * 4 / 3) + 4) {
    throw new Error(`Image is larger than the ${maxBytes} byte limit.`);
  }
  return Buffer.from(value, 'base64');
}
