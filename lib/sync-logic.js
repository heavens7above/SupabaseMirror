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
 * Maps Google Sheets row data to Supabase record with extreme type safety and 
 * greedy "Smart Column Recovery" for misaligned sheets.
 */
function mapSheetsToSupabase(row, headers) {
  const record = {};
  
  // 1. GREEDY ID RECOVERY:
  const idColIndex = headers.findIndex(h => h && h.trim().toLowerCase() === 'id');
  let rowId = idColIndex !== -1 ? row[idColIndex] : null;
  if (!rowId || !UUID_REGEX.test(String(rowId).trim())) {
    rowId = row.find(v => v && UUID_REGEX.test(String(v).trim())) || rowId;
  }

    // 2. PRIMARY MAPPING (With Aggressive Type Filtering)
    headers.forEach((header, index) => {
      if (!header) return;
      let val = row[index];
      const key = header.trim().toLowerCase();
      
      if (key === 'id') val = rowId;

      if (val === "" || val === undefined || (typeof val === 'string' && val.toLowerCase() === 'null')) {
        val = null;
      }

      // TYPE SAFETY: UUID Validation
      if (val && (key.endsWith('_id') || key === 'id')) {
        if (!UUID_REGEX.test(String(val).trim())) {
          console.warn(`[TYPE SAFETY] Discarding invalid UUID for '${header}': "${val}"`);
          val = null;
        }
      }

      // TYPE SAFETY: Timestamp Validation
      if (val && (key.endsWith('_at') || key === 'timestamp')) {
        const strVal = String(val).trim();
        const isActuallyADate = ISO_DATE_REGEX.test(strVal) || !isNaN(Date.parse(strVal));
        if (!isActuallyADate) {
          console.warn(`[TYPE SAFETY] Discarding invalid Timestamp for '${header}': "${val}"`);
          val = null;
        }
      }

      // TYPE SAFETY: Name/Text Validation (Names shouldn't be UUIDs or Dates)
      if (val && (key === 'name' || key === 'title')) {
        const strVal = String(val).trim();
        if (UUID_REGEX.test(strVal) || ISO_DATE_REGEX.test(strVal) || (!isNaN(Date.parse(strVal)) && strVal.includes(':'))) {
          console.warn(`[TYPE SAFETY] Discarding misaligned Name for '${header}': "${val}"`);
          val = null;
        }
      }

      // TYPE SAFETY: Numeric Validation
      const numericFields = ['price', 'amount', 'quantity', 'sort_order', 'total_amount'];
      if (val && numericFields.includes(key)) {
        const num = Number(val);
        if (isNaN(num)) {
          console.warn(`[TYPE SAFETY] Discarding invalid Number for '${header}': "${val}"`);
          val = null;
        } else {
          val = num;
        }
      }

      // TYPE SAFETY: Boolean Validation
      if (key.startsWith('is_')) {
        if (typeof val === 'string') {
          const lv = val.toLowerCase().trim();
          if (lv === 'true' || lv === '1') val = true;
          else if (lv === 'false' || lv === '0') val = false;
          else {
            console.warn(`[TYPE SAFETY] Discarding invalid Boolean for '${header}': "${val}"`);
            val = null;
          }
        } else if (typeof val !== 'boolean') {
          val = null;
        }
      }

      record[header] = val;
    });

  // 3. SMART RECOVERY: If critical fields are MISSING due to misalignment, scan the row greedily
  
  // Find valid Name (Not a UUID, Not a Date, Not a Number, Not "sheets/supabase", NOT "null")
  if (!record.name) {
    const nameCandidate = row.find(v => {
      if (!v || typeof v !== 'string' || v.length < 2) return false;
      const lv = v.toLowerCase().trim();
      if (lv === 'sheets' || lv === 'supabase' || lv === 'null' || lv === 'undefined' || lv === 'true' || lv === 'false') return false;
      if (UUID_REGEX.test(v)) return false;
      if (ISO_DATE_REGEX.test(v)) return false;
      if (!isNaN(Date.parse(v)) && v.includes(':')) return false;
      if (!isNaN(Number(v))) return false;
      return true;
    });
    if (nameCandidate) {
      console.log(`[RECOVERY] Recovered 'name' from misaligned column: "${nameCandidate}"`);
      record.name = nameCandidate;
    }
  }

  // Find valid Price (The first column that is a valid number > 0 and NOT a boolean)
  if (!record.price && record.price !== 0) {
    const priceCandidate = row.find(v => {
      if (v === null || v === undefined || v === "" || typeof v === 'boolean') return false;
      if (String(v).toLowerCase() === 'true' || String(v).toLowerCase() === 'false') return false;
      const num = Number(v);
      return !isNaN(num) && num > 0;
    });
    if (priceCandidate) {
      console.log(`[RECOVERY] Recovered 'price' from misaligned column: ${priceCandidate}`);
      record.price = Number(priceCandidate);
    }
  }

  // Find Category ID (The first UUID that isn't the primary ID)
  if (!record.category_id) {
    const catIdCandidate = row.find(v => v && UUID_REGEX.test(String(v).trim()) && v !== rowId);
    if (catIdCandidate) {
      console.log(`[RECOVERY] Recovered 'category_id' from misaligned column: ${catIdCandidate}`);
      record.category_id = catIdCandidate;
    }
  }

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
  calculateFingerprint,
  UUID_REGEX
};
