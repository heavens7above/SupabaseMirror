const express = require("express");
const bodyParser = require("body-parser");
const helmet = require("helmet");
const { default: pRetry } = require("p-retry");
const supabase = require("./lib/supabase-client");
const { sheets, sheetId } = require("./lib/sheets-client");
const redis = require("./lib/redis-client");
const syncLogic = require("./lib/sync-logic");
const logger = require("./lib/logger");
const { default: PQueue } = require("p-queue");
const { createWebhookMiddleware } = require("./lib/webhook-middleware");
const { registry } = require("./lib/webhook-dispatcher");

const app = express();
const port = process.env.PORT || 3000;

// Local Header Cache (TTL: 1 minute)
const headerCache = new Map();
const HEADER_CACHE_TTL = 60000;

// Local Memory Fallback for Concurrency and Loops
const localLocks = new Set();
const localFingerprints = new Map();

// Security Middleware
app.use(helmet());

// Rate limit Sheets API writes (300 req/min -> 5 req/sec is conservative)
const sheetsQueue = new PQueue({ intervalCap: 5, interval: 1000 });

app.use(
  bodyParser.json({
    verify: (req, res, buf) => {
      req.rawBody = buf.toString();
    },
  }),
);

// Health Check Endpoint
app.get("/health", async (req, res) => {
  try {
    await redis.ping();
    const catCount = await redis.scard('valid_categories');
    res.status(200).json({ status: "ok", redis: "connected", categories_cached: catCount });
  } catch (error) {
    logger.error("Health check failed", { error: error.message });
    res.status(503).json({ status: "error", redis: "disconnected" });
  }
});

/**
 * Periodically syncs valid category IDs from Supabase to Redis for fast validation.
 */
async function syncCategories() {
  try {
    const { data: categories } = await supabase.from('menu_categories').select('id');
    if (categories && categories.length > 0) {
      const ids = categories.map(c => c.id);
      await redis.del('valid_categories');
      await redis.sadd('valid_categories', ...ids);
      logger.info(`Synced ${ids.length} valid category IDs to cache.`);
    }
  } catch (error) {
    logger.error("Failed to sync categories to cache", { error: error.message });
  }
}

// Initial sync and every 5 minutes
syncCategories();
setInterval(syncCategories, 300000);

// Helper: Convert column index to Google Sheets letter (0=A, 1=B, 26=AA)
function getColumnLetter(index) {
  let tempIndex = index;
  let letter = "";
  while (tempIndex >= 0) {
    letter = String.fromCharCode((tempIndex % 26) + 65) + letter;
    tempIndex = Math.floor(tempIndex / 26) - 1;
  }
  return letter || "A";
}

// Helper: Log Sync Error to Supabase (DLQ)
async function logSyncError(source, payload, error) {
  // CRITICAL: Log full error to console for Railway debugging
  console.error(`[CRITICAL ERROR] Source: ${source}, Message: ${error.message}`);
  if (error.stack) console.error(error.stack);
  
  try {
    await pRetry(
      () =>
        supabase.from("sync_errors").insert({
          source,
          payload,
          error_message: error.message,
          stack: error.stack,
          created_at: new Date().toISOString(),
        }),
      { retries: 3 },
    );
  } catch (dbError) {
    console.error(`[META ERROR] Failed to log to Supabase: ${dbError.message}`);
  }
}

// Supabase Webhook Endpoint
const supabaseHandler = async (req, res) => {
  const eventId = req.headers["x-supabase-event-id"];
  const signature = req.headers["x-supabase-signature"];
  const tableName = req.body.table;

  logger.info(`Webhook content received: ${tableName}`);

  logger.info("Received Supabase webhook", {
    eventId,
    table: tableName,
    type: req.body.type,
  });

  // Basic Payload Validation
  if (!req.body.type || (!req.body.record && !req.body.old_record) || !tableName) {
    return res.status(400).send("Invalid payload structure or missing table name");
  }

  if (eventId && (await syncLogic.isDuplicateEvent(eventId))) {
    logger.warn("Duplicate event dropped", { eventId });
    return res.status(200).send("Duplicate");
  }

  const { record, old_record, type } = req.body;
  const rowId = record ? record.id : old_record ? old_record.id : null;

  if (!rowId) return res.status(400).send("No row ID found");

  const webhookSecret = (process.env.SUPABASE_WEBHOOK_SECRET || "").replace(/^"|"$/g, '');

  if (
    !syncLogic.verifySupabaseSignature(
      req.rawBody,
      signature,
      webhookSecret,
    ) && 
    req.headers["x-webhook-secret"] !== webhookSecret
  ) {
    logger.error("Invalid Supabase signature or secret header", { eventId });
    return res.status(401).send("Unauthorized: Invalid Signature or Secret");
  }

  try {
    const lockKey = `${tableName}:${rowId}`;
    
    // BURST PROTECTION: Try Redis first, fallback to local memory
    let lockAcquired = false;
    try {
      const redisLockKey = `supabase_processing:${lockKey}`;
      lockAcquired = await redis.set(redisLockKey, "true", "EX", 10, "NX");
    } catch (e) {
      logger.warn(`Redis lock failed, falling back to local memory for ${lockKey}`);
      if (!localLocks.has(lockKey)) {
        localLocks.add(lockKey);
        lockAcquired = true;
      }
    }

    if (!lockAcquired) {
      logger.info(`Burst Protection: Already processing ${lockKey}. skipping.`);
      return res.status(200).send("OK (Burst Protected)");
    }

    // Step 1: Get Headers (with local caching)
    let headers = null;
    const cached = headerCache.get(tableName);
    if (cached && Date.now() - cached.timestamp < HEADER_CACHE_TTL) {
      headers = cached.headers;
    } else {
      logger.info(`[Step 1] Fetching fresh headers for ${tableName}`);
      const headerResponse = await pRetry(
        () =>
          sheets.spreadsheets.values.get({
            spreadsheetId: sheetId,
            range: `${tableName}!1:1`,
          }),
        { retries: 3 },
      );
      headers = headerResponse.data.values ? headerResponse.data.values[0] : [];
      if (headers.length > 0) {
        headerCache.set(tableName, { headers, timestamp: Date.now() });
      }
    }

    if (!headers || headers.length === 0) {
      throw new Error(`Target sheet '${tableName}' has no headers.`);
    }

    logger.info(`[Step 2] Calculating fingerprint for ${rowId}`);
    const incomingFingerprint = syncLogic.calculateFingerprint(record, headers);
    
    let storedFingerprint = null;
    try {
      storedFingerprint = await redis.get(`lastfingerprint:${tableName}:${rowId}`);
    } catch (e) {
      logger.warn(`Redis fingerprint lookup failed, falling back to local for ${lockKey}`);
    }
    
    // Explicitly check local map if Redis returned null or failed
    if (!storedFingerprint) {
      storedFingerprint = localFingerprints.get(lockKey);
    }
    
    const isShifted = await redis.get(`shifted:${tableName}:${rowId}`).catch(() => null);

    if (req.body.record && req.body.record.source === "sheets") {
      logger.info(`Loop Check (Local Echo): rowId=${rowId} match=${incomingFingerprint === storedFingerprint} shifted=${!!isShifted}`);

      if (incomingFingerprint === storedFingerprint && !isShifted) {
        logger.info(`Loop Check: rowId=${rowId} match=true. Skipping local echo.`);
        localLocks.delete(lockKey);
        return res.status(200).send("Skipped loop");
      }
      
      if (isShifted) {
        logger.info(`Heal Sync: rowId=${rowId} misalignment detected. Overwriting sheet...`);
        await redis.del(`shifted:${tableName}:${rowId}`);
      }
    } else {
      logger.info(`Supabase Source Update: rowId=${rowId} source=${req.body.record ? req.body.record.source : 'unknown'}`);
    }
    if (type === "INSERT" || type === "UPDATE") {
      logger.info(`[Step 3] Adding to sheetsQueue for ${rowId}`);
      await sheetsQueue.add(async () => {
        logger.info(`[Step 4] Queue started for ${rowId}`);
        const idColIndex = headers.findIndex(h => h && h.trim().toLowerCase() === 'id');
        const actualIdIndex = idColIndex !== -1 ? idColIndex : 0;
        const idColLetter = getColumnLetter(actualIdIndex);

        logger.info(`Processing Supabase sync for ${tableName}:${rowId}`, {
          idColIndex: actualIdIndex,
          idColLetter,
          headers: headers.slice(0, 10)
        });

        const rowData = syncLogic.mapSupabaseToSheets(record, headers);
        let rowIndex = await redis.get(`rowindex:${tableName}:${rowId}`).catch(() => null);

        if (rowIndex) {
          // VERIFY: Check if the row at this index still contains the correct ID
          const verifyResponse = await pRetry(
            () =>
              sheets.spreadsheets.values.get({
                spreadsheetId: sheetId,
                range: `${tableName}!${idColLetter}${rowIndex}`,
              }),
            { retries: 3 },
          );
          
          const foundId = verifyResponse.data.values ? verifyResponse.data.values[0][0] : null;
          
          if (foundId !== rowId) {
            logger.warn(`Stale rowIndex detected for ${rowId} at row ${rowIndex}. Found ID: "${foundId}". Re-scanning column ${idColLetter}...`, {
              headers: headers.slice(0, 5)
            });
            rowIndex = null; // Force re-scan
            await redis.del(`rowindex:${tableName}:${rowId}`).catch(() => {});
          }
        }

        if (!rowIndex) {
          const response = await pRetry(
            () =>
              sheets.spreadsheets.values.get({
                spreadsheetId: sheetId,
                range: `${tableName}!${idColLetter}:${idColLetter}`,
              }),
            { retries: 3 },
          );
          const rows = response.data.values || [];
          // CRITICAL FIX: The range is a single column, so the ID is always at index 0
          const index = rows.findIndex((r) => r[0] === rowId);
          if (index !== -1) {
            rowIndex = index + 1;
            await redis.set(`rowindex:${tableName}:${rowId}`, rowIndex).catch(() => {});
            logger.info(`Discovered existing rowIndex for ${rowId}: ${rowIndex}`);
          }
        }

        if (rowIndex) {
          await pRetry(
            () =>
              sheets.spreadsheets.values.update({
                spreadsheetId: sheetId,
                range: `${tableName}!A${rowIndex}`,
                valueInputOption: "USER_ENTERED",
                resource: { values: [rowData] },
              }),
            { retries: 3 },
          );
          logger.info(`Updated Sheets row ${rowIndex} for ${rowId}`);
        } else {
          const appendResponse = await pRetry(
            () =>
              sheets.spreadsheets.values.append({
                spreadsheetId: sheetId,
                range: `${tableName}!1:1`,
                valueInputOption: "USER_ENTERED",
                resource: { values: [rowData] },
              }),
            { retries: 3 },
          );
          
          // Capture new row index from append response
          const updatedRange = appendResponse.data.updates.updatedRange; // e.g. "menu_items!A15:F15"
          const match = updatedRange.match(/!A(\d+):/);
          if (match) {
            rowIndex = match[1];
            await redis.set(`rowindex:${tableName}:${rowId}`, rowIndex).catch(() => {});
          }
          logger.info("Appended new Sheets row", { table: tableName, rowId, rowIndex });
        }

        // CRITICAL: Update the state fingerprint in Redis after successful sync (with fallback)
        await redis.set(`lastfingerprint:${tableName}:${rowId}`, incomingFingerprint, "EX", 86400).catch(() => {});
        localFingerprints.set(`${tableName}:${rowId}`, incomingFingerprint);
      }).finally(() => {
        // Release locks ONLY after queue task is done
        localLocks.delete(`${tableName}:${rowId}`);
      });
    } else {
      // Release lock for non-sync events
      localLocks.delete(`${tableName}:${rowId}`);
    }

    res.status(200).send("OK");
  } catch (error) {
    await logSyncError("supabase", req.body, error);
    res.status(500).send("Internal Server Error");
  } finally {
    // Release lock (cleanup is automatic by TTL, but let's be proactive)
    await redis.del(`supabase_processing:${tableName}:${rowId}`).catch(() => {});
  }
};

registry.register('supabase', supabaseHandler);
app.post("/supabase-webhook", createWebhookMiddleware('supabase'), registry.dispatch('supabase'));

// Google Sheets Webhook Endpoint
const sheetsHandler = async (req, res) => {
  const { row, timestamp, table } = req.body;
  if (!row || !table) return res.status(400).send("Invalid row data or missing table name");
  const sheetsSyncedAt = new Date(timestamp);

  try {
    // Fetch headers first to find the ID column dynamically
    const headerResponse = await pRetry(
      () =>
        sheets.spreadsheets.values.get({
          spreadsheetId: sheetId,
          range: `${table}!1:1`,
        }),
      { retries: 3 },
    );
    const headers = headerResponse.data.values ? headerResponse.data.values[0] : [];
    
    // Structural Alignment log (only if data changed or for the first request in a burst)
    // We already log "Processing Sheets Update" below.

    const idIndex = headers.findIndex(h => h && h.trim().toLowerCase() === 'id');
    if (idIndex === -1) {
      logger.error(`Missing 'id' column in sheet headers`, { headers });
      throw new Error(`Sheet '${table}' is missing an 'id' column.`);
    }

    const rowId = row[idIndex];
    if (!rowId) return res.status(400).send("No row ID found in the detected column");

    // BURST PROTECTION: Stop Sheet firehose (multiple onEdit events for one change)
    // Hybrid: try Redis lock, fall back to local memory if Redis is down
    const lockKey = `${table}:${rowId}`;
    let sheetsLockAcquired = false;
    try {
      sheetsLockAcquired = await redis.set(`sheets_processing:${lockKey}`, 'true', 'EX', 5, 'NX');
    } catch (e) {
      logger.warn(`Redis sheets lock failed, falling back to local memory for ${lockKey}`);
      if (!localLocks.has(`sheets:${lockKey}`)) {
        localLocks.add(`sheets:${lockKey}`);
        sheetsLockAcquired = true;
        setTimeout(() => localLocks.delete(`sheets:${lockKey}`), 5000);
      }
    }
    if (!sheetsLockAcquired) {
      logger.info(`Sheets Burst Protection: Already processing ${table}:${rowId}. skipping.`);
      return res.status(200).send("OK (Burst Protected)");
    }

    try {
      logger.info(`Processing Sheets Update: table=${table} rowId=${rowId}`);

      // PRE-UPSERT DEDUPLICATION:
      const incomingRecord = syncLogic.mapSheetsToSupabase(row, headers);
      
      // STRUCTURAL DIAGNOSTIC: Log full alignment if misalignment is detected
      const isMisaligned = row.some((v, i) => v && headers[i] && headers[i].toLowerCase().includes('id') && !syncLogic.UUID_REGEX.test(v));
      if (isMisaligned) {
        const diagMap = headers.slice(0, 10).map((h, i) => `${h || 'COL'+i}: ${String(row[i]).substring(0, 15)}`).join(' | ');
        logger.info(`[DIAGNOSTIC] Structural Shift Detected on ${table}:${rowId}: ${diagMap}`);
        // Flag for "Structural Healing" - forces a sync back even if fingerprints match
        await redis.set(`shifted:${table}:${rowId}`, 'true', 'EX', 60).catch(() => {});
      }

      // FK VALIDATION: Only discard category_id when Redis CONFIRMS it's invalid.
      // If Redis is down, always preserve the category_id to avoid FK violations.
      if (incomingRecord.category_id) {
        let fkCheckResult = null;
        try {
          fkCheckResult = await redis.sismember('valid_categories', incomingRecord.category_id);
        } catch (e) {
          // Redis unavailable — skip check, preserve category_id
          fkCheckResult = 1;
        }
        if (!fkCheckResult) {
          logger.warn(`Discarding invalid category_id: ${incomingRecord.category_id} (Not found in Supabase)`);
          incomingRecord.category_id = null;
        }
      }

      const incomingFingerprint = syncLogic.calculateFingerprint(incomingRecord, headers);
      const lastFingerprint = await redis.get(`lastfingerprint:${table}:${rowId}`).catch(() => null);
      const localFingerprint = localFingerprints.get(`${table}:${rowId}`);
      const effectiveLastFingerprint = lastFingerprint || localFingerprint;

      if (incomingFingerprint === effectiveLastFingerprint) {
        logger.info(`Dropping duplicate/local-echo Sheets update for ${rowId}`);
        localLocks.delete(lockKey);
        return res.status(200).send("Dropped (Duplicate)");
      }

      // CONFLICT RESOLUTION:
      const { data: currentRecord } = await pRetry(
        () =>
          supabase
            .from(table)
            .select("synced_at")
            .eq("id", rowId)
            .single(),
        { retries: 3 },
      );

    if (currentRecord && currentRecord.synced_at) {
      const supabaseSyncedAt = new Date(currentRecord.synced_at);
      if (supabaseSyncedAt > sheetsSyncedAt) {
        logger.info(
          `Conflict Detected! Supabase record is newer for ${rowId}. Dropping.`,
          { rowId },
        );
        return res.status(200).send("Dropped due to conflict");
      }
    }

    // Prepare final payload for Supabase
    const supabaseRecord = { ...incomingRecord };
    const fieldsToExclude = ['created_at', 'updated_at', 'synced_at', 'source'];
    fieldsToExclude.forEach(field => delete supabaseRecord[field]);

    supabaseRecord.source = "sheets"; // Tag origin for loop prevention
    supabaseRecord.synced_at = sheetsSyncedAt.toISOString();

    const { error } = await pRetry(
      () =>
        supabase
          .from(table)
          .upsert(supabaseRecord, { onConflict: "id" }),
      { retries: 3 },
    );

    if (error) throw error;

    logger.info("Upserted Supabase record from Sheets", { rowId });
    
    // Store fingerprint to prevent infinite loops — must have .catch() since Redis may be down
    await redis.set(`lastfingerprint:${table}:${rowId}`, incomingFingerprint, 'EX', 3600).catch(() => {});
    localFingerprints.set(`${table}:${rowId}`, incomingFingerprint);
    
    res.status(200).send("OK");
    } finally {
      // Clear burst lock slowly to allow firehose to drain
      setTimeout(() => redis.del(lockKey).catch(() => {}), 2000);
    }
  } catch (error) {
    await logSyncError("sheets", req.body, error);
    res.status(500).send("Internal Server Error");
  }
};

registry.register('sheets', sheetsHandler);
app.post("/sheets-webhook", createWebhookMiddleware('sheets'), registry.dispatch('sheets'));

const server = app.listen(port, () => {
  logger.info(`Middleware server listening on port ${port}`, {
    env: process.env.NODE_ENV,
  });
});

// Graceful Shutdown
async function shutdown(signal) {
  logger.info(`${signal} received. Starting graceful shutdown...`);

  server.close(() => {
    logger.info("HTTP server closed.");
  });

  try {
    logger.info("Waiting for pending queue items...");
    await sheetsQueue.onIdle();
    logger.info("Queue idle.");

    await redis.quit();
    logger.info("Redis connection closed.");

    process.exit(0);
  } catch (err) {
    logger.error("Error during shutdown", { error: err.message });
    process.exit(1);
  }
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
