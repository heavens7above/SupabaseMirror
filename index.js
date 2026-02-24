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

const app = express();
const port = process.env.PORT || 3000;

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
    res.status(200).json({ status: "ok", redis: "connected" });
  } catch (error) {
    logger.error("Health check failed", { error: error.message });
    res.status(503).json({ status: "error", redis: "disconnected" });
  }
});

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
app.post("/supabase-webhook", async (req, res) => {
  const eventId = req.headers["x-supabase-event-id"];
  const signature = req.headers["x-supabase-signature"];
  const tableName = req.body.table;

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
    // Fetch headers upfront to use for fingerprinting AND mapping
    const headerResponse = await pRetry(
      () =>
        sheets.spreadsheets.values.get({
          spreadsheetId: sheetId,
          range: `${tableName}!1:1`,
        }),
      { retries: 3 },
    );
    const headers = headerResponse.data.values ? headerResponse.data.values[0] : [];
    if (headers.length === 0) {
      throw new Error(`Target sheet '${tableName}' has no headers.`);
    }

    // Refined Loop Prevention: Only fingerprint keys that exist in the SHEET
    if (req.body.record && req.body.record.source === "sheets") {
      const incomingFingerprint = syncLogic.calculateFingerprint(record, headers);
      const lastFingerprint = await redis.get(`lastfingerprint:${tableName}:${rowId}`);

      logger.info(`Loop Check: rowId=${rowId} match=${incomingFingerprint === lastFingerprint}`, { 
        rowId, 
        isMatch: incomingFingerprint === lastFingerprint,
        incoming: incomingFingerprint,
        stored: lastFingerprint
      });

      if (incomingFingerprint === lastFingerprint) {
        logger.info(`Loop Check: rowId=${rowId} match=true. Skipping local echo.`);
        return res.status(200).send("Skipped loop");
      }
      logger.info(`Loop Check: rowId=${rowId} match=false. Proceeding with sync.`);
    }
    if (type === "INSERT" || type === "UPDATE") {
      await sheetsQueue.add(async () => {
        const idColIndex = headers.findIndex(h => h.toLowerCase() === 'id');
        const idColLetter = getColumnLetter(idColIndex !== -1 ? idColIndex : 0);

        const rowData = syncLogic.mapSupabaseToSheets(record, headers);
        let rowIndex = await redis.get(`rowindex:${tableName}:${rowId}`);

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
          const index = rows.findIndex((r) => r[0] === rowId);
          if (index !== -1) {
            rowIndex = index + 1;
            await redis.set(`rowindex:${tableName}:${rowId}`, rowIndex);
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
          logger.info("Updated Sheets row", { table: tableName, rowId, rowIndex });
        } else {
          await pRetry(
            () =>
              sheets.spreadsheets.values.append({
                spreadsheetId: sheetId,
                range: `${tableName}!1:1`,
                valueInputOption: "USER_ENTERED",
                resource: { values: [rowData] },
              }),
            { retries: 3 },
          );
          logger.info("Appended new Sheets row", { table: tableName, rowId });
        }
      });
    }

    res.status(200).send("OK");
  } catch (error) {
    await logSyncError("supabase", req.body, error);
    res.status(500).send("Internal Server Error");
  }
});

// Google Sheets Webhook Endpoint
app.post("/sheets-webhook", async (req, res) => {
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
    const lockKey = `sheets_processing:${table}:${rowId}`;
    const acquired = await redis.set(lockKey, 'true', 'EX', 5, 'NX');
    if (!acquired) {
      logger.info(`Sheets Burst Protection: Already processing ${table}:${rowId}. skipping.`);
      return res.status(200).send("OK (Burst Protected)");
    }

    try {
      logger.info(`Processing Sheets Update: table=${table} rowId=${rowId}`);

      // PRE-UPSERT DEDUPLICATION:
      const incomingRecord = syncLogic.mapSheetsToSupabase(row, headers);
      const incomingFingerprint = syncLogic.calculateFingerprint(incomingRecord, headers);
      const lastFingerprint = await redis.get(`lastfingerprint:${table}:${rowId}`);

      if (incomingFingerprint === lastFingerprint) {
        logger.info(`Dropping duplicate/local-echo Sheets update for ${rowId}`);
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
    
    // Store fingerprint to prevent infinite loops (using incoming fingerprint for stability)
    await redis.set(`lastfingerprint:${table}:${rowId}`, incomingFingerprint, 'EX', 3600);
    
    res.status(200).send("OK");
    } finally {
      // Clear burst lock slowly to allow firehose to drain
      setTimeout(() => redis.del(lockKey).catch(() => {}), 2000);
    }
  } catch (error) {
    await logSyncError("sheets", req.body, error);
    res.status(500).send("Internal Server Error");
  }
});

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
