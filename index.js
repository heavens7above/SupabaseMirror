const express = require("express");
const bodyParser = require("body-parser");
const helmet = require("helmet");
const pRetry = require("p-retry");
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

// Helper: Log Sync Error to Supabase (DLQ)
async function logSyncError(source, payload, error) {
  logger.error(`Sync error from ${source}`, { error: error.message, payload });
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
    logger.error("Failed to log sync error to Supabase", {
      error: dbError.message,
    });
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

  if (await syncLogic.isDuplicateEvent(eventId)) {
    logger.warn("Duplicate event dropped", { eventId });
    return res.status(200).send("Duplicate");
  }

  if (
    !syncLogic.verifySupabaseSignature(
      req.rawBody,
      signature,
      process.env.SUPABASE_WEBHOOK_SECRET,
    ) && 
    req.headers["x-webhook-secret"] !== process.env.SUPABASE_WEBHOOK_SECRET
  ) {
    logger.error("Invalid Supabase signature or secret header", { eventId });
    return res.status(401).send("Unauthorized: Invalid Signature or Secret");
  }

  const { record, old_record, type } = req.body;
  const rowId = record ? record.id : old_record ? old_record.id : null;

  if (!rowId) return res.status(400).send("No row ID found");

  if (!(await syncLogic.acquireLock(rowId))) {
    logger.info("Lock active, skipping supabase-webhook", { rowId, eventId });
    return res.status(200).send("Locked");
  }

  try {
    if (type === "INSERT" || type === "UPDATE") {
      await sheetsQueue.add(async () => {
        // Fetch headers from the sheet to ensure correct column mapping
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

        const rowData = syncLogic.mapSupabaseToSheets(record, headers);
        let rowIndex = await redis.get(`rowindex:${tableName}:${rowId}`);

        if (!rowIndex) {
          const response = await pRetry(
            () =>
              sheets.spreadsheets.values.get({
                spreadsheetId: sheetId,
                range: `${tableName}!A:A`,
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
  } finally {
    await syncLogic.releaseLock(rowId);
  }
});

// Google Sheets Webhook Endpoint
app.post("/sheets-webhook", async (req, res) => {
  const { row, timestamp, table } = req.body;
  if (!row || !row[0] || !table) return res.status(400).send("Invalid row data or missing table name");

  const rowId = row[0];
  const sheetsSyncedAt = new Date(timestamp);

  logger.info("Received Sheets webhook", { table, rowId, timestamp });

  if (!(await syncLogic.acquireLock(rowId))) {
    logger.info("Lock active, skipping sheets-webhook", { rowId });
    return res.status(200).send("Locked");
  }

  try {
    // Fetch headers to perform correct reverse mapping
    const headerResponse = await pRetry(
      () =>
        sheets.spreadsheets.values.get({
          spreadsheetId: sheetId,
          range: `${table}!1:1`,
        }),
      { retries: 3 },
    );
    const headers = headerResponse.data.values ? headerResponse.data.values[0] : [];

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
          "Conflict: Supabase record is newer. Dropping Sheets update.",
          { rowId },
        );
        return res.status(200).send("Dropped due to conflict");
      }
    }

    const supabaseRecord = syncLogic.mapSheetsToSupabase(row, headers);
    supabaseRecord.synced_at = sheetsSyncedAt.toISOString();
    supabaseRecord.source = "sheets";

    const { error } = await pRetry(
      () =>
        supabase
          .from(table)
          .upsert(supabaseRecord, { onConflict: "id" }),
      { retries: 3 },
    );

    if (error) throw error;

    logger.info("Upserted Supabase record from Sheets", { rowId });
    res.status(200).send("OK");
  } catch (error) {
    await logSyncError("sheets", req.body, error);
    res.status(500).send("Internal Server Error");
  } finally {
    await syncLogic.releaseLock(rowId);
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
