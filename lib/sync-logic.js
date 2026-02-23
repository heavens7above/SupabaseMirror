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
 * @returns {Array} - Array of values for the Google Sheet.
 */
function mapSupabaseToSheets(record) {
  // Assuming Column A: id, Column B: synced_at, and then other columns
  return [
    record.id,
    record.synced_at || new Date().toISOString(),
    record.name,
    record.description,
    record.price,
    record.is_available,
    record.is_veg
  ];
}

/**
 * Maps Google Sheets row data to Supabase record.
 * @param {Array} row - Row data from Sheets API.
 * @returns {object} - Supabase record.
 */
function mapSheetsToSupabase(row) {
  return {
    id: row[0],
    synced_at: row[1],
    name: row[2],
    description: row[3],
    price: row[4],
    is_available: row[5],
    is_veg: row[6]
  };
}

module.exports = {
  acquireLock,
  releaseLock,
  isDuplicateEvent,
  verifySupabaseSignature,
  mapSupabaseToSheets,
  mapSheetsToSupabase
};
