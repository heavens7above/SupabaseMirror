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
  if (!eventId) return false;
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

const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const ISO_DATE_REGEX = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/;

/**
 * Maps Google Sheets row data to Supabase record with extreme type safety.
 * @param {Array} row - Row data from Sheets API.
 * @param {Array<string>} headers - The headers of the target sheet.
 * @returns {object} - Supabase record.
 */
function mapSheetsToSupabase(row, headers) {
  const record = {};
  
  // GREEDY ID IDENTIFICATION:
  // If the 'id' header exists but its value is empty or invalid, scan the row for a UUID.
  const idColIndex = headers.findIndex(h => h && h.trim().toLowerCase() === 'id');
  let rowId = idColIndex !== -1 ? row[idColIndex] : null;

  if (!rowId || !UUID_REGEX.test(String(rowId).trim())) {
    const foundUuid = row.find(v => v && UUID_REGEX.test(String(v).trim()));
    if (foundUuid) {
      console.log(`[TYPE SAFETY] Fuzzy Match! Found valid row ID elsewhere: ${foundUuid}`);
      rowId = foundUuid;
    }
  }

  headers.forEach((header, index) => {
    if (!header) return;
    
    let val = row[index];
    const key = header.trim().toLowerCase();
    
    // Use the potentially recovered rowId specifically for 'id' keys
    if (key === 'id') {
      val = rowId;
    }

    // Convert common empty/null representations to true null
    if (val === "" || val === undefined || (typeof val === 'string' && val.toLowerCase() === 'null')) {
      val = null;
    }
    
    // Try to parse JSON if it's a stringified object
    if (typeof val === 'string' && (val.startsWith('{') || val.startsWith('['))) {
      try { val = JSON.parse(val); } catch (e) {}
    }

    // TYPE SAFETY: UUID Validation (for foreign keys)
    if (val && key.endsWith('_id') && key !== 'id') {
      const stringVal = String(val).trim();
      if (!UUID_REGEX.test(stringVal)) {
        console.warn(`[TYPE SAFETY] Discarding invalid UUID for '${header}': "${stringVal}"`);
        val = null;
      }
    }

    // TYPE SAFETY: Timestamp Validation
    if (val && (key.endsWith('_at') || key === 'timestamp')) {
      const stringVal = String(val).trim();
      if (!ISO_DATE_REGEX.test(stringVal) && isNaN(Date.parse(stringVal))) {
        console.warn(`[TYPE SAFETY] Discarding invalid Timestamp for '${header}': "${stringVal}"`);
        val = null;
      }
    }

    // TYPE SAFETY: Numeric Validation
    const numericFields = ['price', 'amount', 'quantity', 'sort_order', 'total_amount'];
    if (val && numericFields.includes(key)) {
      const num = Number(val);
      if (isNaN(num)) {
        console.warn(`[TYPE SAFETY] Discarding invalid Numeric for '${header}': "${val}"`);
        val = null;
      } else {
        val = num;
      }
    }

    // TYPE SAFETY: Boolean Coercion
    if (key.startsWith('is_')) {
      if (typeof val === 'string') {
        val = val.toLowerCase() === 'true' || val === '1';
      } else {
        val = Boolean(val);
      }
    }

    record[header] = val;
  });
  return record;
}

/**
 * Calculates a stable MD5 fingerprint for a record.
 * @param {object} record - The record to hash.
 * @param {Array<string>} [includeKeys] - Optional keys to include.
 * @returns {string} - The hex digest.
 */
function calculateFingerprint(record, includeKeys = null) {
  if (!record) return "";
  const normalized = {};
  const keys = includeKeys || Object.keys(record);
  
  const metadataFields = ['synced_at', 'created_at', 'source', 'updated_at'];
  
  keys.sort().forEach(key => {
    // ALWAYS skip internal metadata fields, even if they are in the sheet headers
    if (metadataFields.includes(key)) {
      return;
    }
    
    if (record[key] !== undefined && record[key] !== null) {
      // Coerce all business values to strings for stable comparison across sources
      normalized[key] = typeof record[key] === 'object' 
        ? JSON.stringify(record[key]) 
        : String(record[key]);
    } else if (record[key] === null) {
      normalized[key] = null;
    }
  });

  const fingerprint = crypto.createHash('md5').update(JSON.stringify(normalized)).digest('hex');
  
  // Debug log to console for Railway inspection
  console.log(`[FINGERPRINT DEBUG] Content: ${JSON.stringify(normalized)} -> ${fingerprint}`);
  
  return fingerprint;
}

module.exports = {
  acquireLock,
  releaseLock,
  isDuplicateEvent,
  verifySupabaseSignature,
  mapSupabaseToSheets,
  mapSheetsToSupabase,
  calculateFingerprint
};
