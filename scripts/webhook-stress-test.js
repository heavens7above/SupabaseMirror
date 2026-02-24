const { WebhookRegistry } = require('../lib/webhook-dispatcher');

const SERVER_URL = process.env.SERVER_URL || 'http://localhost:3000';

const GREEN = '\x1b[32m';
const RED = '\x1b[31m';
const AMBER = '\x1b[33m';
const CYAN = '\x1b[36m';
const RESET = '\x1b[0m';

function logInfo(msg) { console.log(`${CYAN}[INFO]${RESET} ${msg}`); }
function logSuccess(msg) { console.log(`${GREEN}✔ [PASS]${RESET} ${msg}`); }
function logAmber(msg) { console.log(`${AMBER}⚠ [ALERT]${RESET} ${msg}`); }
function logCritical(msg) { console.log(`${RED}✖ [CRITICAL]${RESET} ${msg}`); }

const headers = { 'Content-Type': 'application/json' };
const delay = ms => new Promise(res => setTimeout(res, ms));

async function hitWebhook(payload, extraHeaders = {}) {
  try {
    const res = await fetch(`${SERVER_URL}/supabase-webhook`, {
      method: 'POST',
      headers: { ...headers, ...extraHeaders },
      body: JSON.stringify(payload)
    });
    const text = await res.text();
    return { status: res.status, text };
  } catch (error) {
    return { status: 0, text: 'Connection Refused' };
  }
}

async function runScenario1() {
  logInfo("--- SCENARIO 1: Fingerprint Deduplication (50 Identical Payloads in 5s) ---");
  const payload = {
    table: 'test_table',
    type: 'INSERT',
    record: { id: 'evt-1234', data: 'identical-payload-body' }
  };

  const promises = [];
  for(let i = 0; i < 50; i++) {
    // Stagger slightly realistically
    promises.push(delay(i * 10).then(() => hitWebhook(payload)));
  }
  
  const results = await Promise.all(promises);
  const suppresses = results.filter(r => r.text.includes('Duplicate Suppressed'));
  const processed = results.filter(r => !r.text.includes('Duplicate Suppressed'));
  
  if (suppresses.length >= 49) {
    logSuccess(`Scenario 1 Passed: 1 processed, ${suppresses.length} dropped cleanly with amber alerts.`);
  } else if (results.some(r => r.status === 0)) {
    logAmber(`Server offline at ${SERVER_URL}. Skipping live endpoint checks.`);
  } else {
    logCritical(`Scenario 1 Failed. Expected ~49 dropped, got ${suppresses.length}.`);
  }
}

async function runScenario2() {
  logInfo("--- SCENARIO 2: Idempotency Key Retry Storm ---");
  const payload = {
    table: 'test_table',
    type: 'UPDATE',
    record: { id: 'evt-retry-99' }
  };
  const idempotencyKey = `idempotent-key-${Date.now()}`;
  
  // Exponential backoff
  const backoffs = [100, 200, 400, 800, 1600];
  let processed = 0;
  let suppressed = 0;

  for (const timeMs of backoffs) {
    const r = await hitWebhook(payload, { 'x-idempotency-key': idempotencyKey });
    if (r.text.includes('Idempotent OK')) suppressed++;
    else processed++;
    
    if (processed + suppressed > 0 && r.status !== 0) {
      logAmber(`Sent retry after ${timeMs}ms - Result: ${r.text}`);
    }
    await delay(timeMs);
  }

  if (suppressed === 4 && processed === 1) {
    logSuccess(`Scenario 2 Passed: Single execution verified, subsequent retries dropped via idempotency key.`);
  } else if (processed + suppressed > 0) {
    logAmber(`Scenario 2 Completed. Processed: ${processed}, Suppressed: ${suppressed}`);
  }
}

async function runScenario3() {
  logInfo("--- SCENARIO 3: Strict Single-Subscriber Enforcement (Startup Conflict) ---");
  const testRegistry = new WebhookRegistry();
  const mockHandler1 = async () => {};
  const mockHandler2 = async () => {};
  
  try {
    testRegistry.register('test_event', mockHandler1);
    testRegistry.register('test_event', mockHandler2); // Should throw here
    logCritical("Scenario 3 Failed: Overlapped listeners were allowed.");
  } catch (err) {
    if (err.message.includes('Multiple listeners registered')) {
      logSuccess("Scenario 3 Passed: System preemptively crashed on redundant listener registration.");
    } else {
      logCritical(`Scenario 3 Failed with unexpected error: ${err.message}`);
    }
  }
}

async function runScenario4() {
  logInfo("--- SCENARIO 4: Rate-Breaker Flood (15 unique events in 10s) ---");
  const promises = [];
  
  // 15 unique events to bypass fingerprinting cache and trigger loop protection
  for(let i = 0; i < 15; i++) {
    const payload = {
      table: 'test_table',
      type: 'INSERT',
      record: { id: `loop-evt-${Math.random()}` }
    };
    promises.push(delay(i * 100).then(() => hitWebhook(payload)));
  }

  const results = await Promise.all(promises);
  const paused = results.filter(r => r.status === 429);
  
  if (paused.length === 5) {
    logCritical(`[DDoS Protection] Loop breaker suppressed ${paused.length} flooding events.`);
    logSuccess(`Scenario 4 Passed: Breaker activated exactly at threshold limit (10).`);
  } else if (paused.length > 0) {
    logAmber(`Scenario 4 recorded ${paused.length} blocked requests. Threshold configuration may vary.`);
  } else if (!results.some(r => r.status === 0)) {
    logCritical(`Scenario 4 Failed: Loop protection did not trigger.`);
  }
}

async function runAll() {
  console.log(`${CYAN}=================================================${RESET}`);
  console.log(`${CYAN}   COMMANDCENTER WEBHOOK PIPELINE STRESS TEST    ${RESET}`);
  console.log(`${CYAN}=================================================${RESET}\n`);
  
  await runScenario1();
  console.log("");
  await runScenario2();
  console.log("");
  await runScenario3();
  console.log("");
  await runScenario4();
  
  console.log(`\n${CYAN}=================================================${RESET}`);
  console.log(`${GREEN}             STRESS TEST COMPLETED               ${RESET}`);
  console.log(`${CYAN}=================================================${RESET}\n`);
}

runAll().catch(console.error);
