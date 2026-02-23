const redis = require('./redis-client');
const crypto = require('crypto');

/**
 * Acquires a sync lock for a specific row ID.
 * @param {string} rowId - The unique ID of the row.
 * @param {number} ttl - Lock duration in seconds (default 15).
 * @returns {Promise<boolean>} - True if lock was acquired, false otherwise.
 */
async function acquireLock(rowId, ttl = 15) {
  const lockKey = `lock:${rowId}`;
  const result = await redis.set(lockKey, 'locked', 'EX', ttl, 'NX');
  return result === 'OK';
}

/**
 * Releases a sync lock for a specific row ID.
 * @param {string} rowId - The unique ID of the row.
 */
async function releaseLock(rowId) {
  const lockKey = `lock:${rowId}`;
  await redis.del(lockKey);
}

/**
 * Checks if a webhook event has already been processed.
 * @param {string} eventId - The unique ID of the webhook event.
 * @returns {Promise<boolean>} - True if already processed.
 */
async function isDuplicateEvent(eventId) {
  const eventKey = `event:${eventId}`;
  const result = await redis.set(eventKey, 'processed', 'EX', 86400, 'NX'); // 24h TTL
  return result === null; // If result is null, it means the key already existed
}

/**
 * Verifies the Supabase webhook signature.
 * @param {string} payload - The raw request body.
 * @param {string} signature - The X-Supabase-Signature header.
 * @param {string} secret - The SUPABASE_WEBHOOK_SECRET.
 * @returns {boolean} - True if signature is valid.
 */
function verifySupabaseSignature(payload, signature, secret) {
  if (!signature || !secret) return false;
  const hmac = crypto.createHmac('sha256', secret);
  const digest = hmac.update(payload).digest('hex');
  return signature === digest;
}

/**
 * Maps Supabase record to Google Sheets row data.
 * @param {object} record - Supabase record.
 * @param {Array<string>} headers - The headers of the target sheet.
 * @returns {Array} - Array of values for the Google Sheet.
 */
function mapSupabaseToSheets(record, headers) {
  if (!headers || headers.length === 0) {
    // Fallback: If no headers, return all keys
    return Object.values(record);
  }
  
  // Map values exactly to the header positions
  return headers.map(header => {
    const val = record[header];
    if (val === undefined) return "";
    return typeof val === 'object' ? JSON.stringify(val) : val;
  });
}

/**
 * Maps Google Sheets row data to Supabase record.
 * @param {Array} row - Row data from Sheets API.
 * @param {Array<string>} headers - The headers of the target sheet.
 * @returns {object} - Supabase record.
 */
function mapSheetsToSupabase(row, headers) {
  const record = {};
  headers.forEach((header, index) => {
    let val = row[index];
    // Try to parse JSON if it's a stringified object
    if (typeof val === 'string' && (val.startsWith('{') || val.startsWith('['))) {
      try { val = JSON.parse(val); } catch (e) {}
    }
    record[header] = val;
  });
  return record;
}

module.exports = {
  acquireLock,
  releaseLock,
  isDuplicateEvent,
  verifySupabaseSignature,
  mapSupabaseToSheets,
  mapSheetsToSupabase
};
