const crypto = require("crypto");
const logger = require("./logger");

// In-Memory Stores
const fingerprintCache = new Map(); // key -> expiry timestamp
const sourceHitCounters = new Map(); // source -> { hits, firstHitTime, pauseUntil }
const idempotencyCache = new Map(); // key -> expiry timestamp

const FINGERPRINT_TTL_MS = 30 * 1000; // 30s
const LOOP_WINDOW_MS = 60 * 1000; // 60s
const LOOP_THRESHOLD = 10; // Max events before loop trigger
const LOOP_PAUSE_DURATION_MS = 90 * 1000; // 90s pause block
const IDEMPOTENCY_TTL_MS = 10 * 60 * 1000; // 10m TTL

function cleanupCache(cache) {
  const now = Date.now();
  for (const [key, expiry] of cache.entries()) {
    if (now > expiry) cache.delete(key);
  }
}

function createWebhookMiddleware(source) {
  return function webhookMiddleware(req, res, next) {
    const now = Date.now();

    // 1. Idempotency Key Check
    const idempotencyKey = req.headers["x-idempotency-key"];
    if (idempotencyKey) {
      cleanupCache(idempotencyCache);
      if (idempotencyCache.has(idempotencyKey)) {
        logger.info(
          `[SYSTEM ALERT] Idempotency key matched. Suppressed duplicate.`,
          { idempotencyKey, alertLevel: "amber" },
        );
        return res.status(200).send("Idempotent OK");
      }
      idempotencyCache.set(idempotencyKey, now + IDEMPOTENCY_TTL_MS);
    }

    // 2. Fingerprinting Feature
    cleanupCache(fingerprintCache);

    const timeWindow5s = Math.floor(now / 5000) * 5000;
    const bodyString = req.rawBody || JSON.stringify(req.body) || "";
    const hashData = `${source}:${bodyString}:${timeWindow5s}`;
    const fingerprint = crypto.createHash("md5").update(hashData).digest("hex");

    if (fingerprintCache.has(fingerprint)) {
      logger.warn(
        `[SYSTEM ALERT] Duplicate event suppressed (fingerprint match: ${fingerprint})`,
        { source, alertLevel: "amber" },
      );
      return res.status(200).send("Duplicate Suppressed");
    }

    fingerprintCache.set(fingerprint, now + FINGERPRINT_TTL_MS);

    // 3. Loop Detection (Rate Limiting per Source)
    let counter = sourceHitCounters.get(source) || {
      hits: 0,
      firstHitTime: now,
      pauseUntil: 0,
    };

    if (counter.pauseUntil > now) {
      logger.error(
        `[CRITICAL ALERT] Loop breaker active for source '${source}'. Suppressed event.`,
        { source, alertLevel: "red" },
      );
      return res.status(429).send("Too Many Requests (Loop Breaker Pause)");
    }

    if (now - counter.firstHitTime > LOOP_WINDOW_MS) {
      // Reset window
      counter = { hits: 1, firstHitTime: now, pauseUntil: 0 };
    } else {
      counter.hits++;
      if (counter.hits > LOOP_THRESHOLD) {
        counter.pauseUntil = now + LOOP_PAUSE_DURATION_MS;
        logger.error(
          `[CRITICAL ALERT] Potential loop detected for source '${source}'. ${counter.hits} events in < 60s. Pausing ingestion for 90s.`,
          { source, alertLevel: "red" },
        );
        sourceHitCounters.set(source, counter);
        return res
          .status(429)
          .send("Too Many Requests (Loop Breaker Triggered)");
      }
    }
    sourceHitCounters.set(source, counter);

    // Continue to next middleware / route handler
    next();
  };
}

module.exports = {
  createWebhookMiddleware,
  // Exported for internal testing overrides if needed
  _resetState() {
    fingerprintCache.clear();
    sourceHitCounters.clear();
    idempotencyCache.clear();
  },
};
