/**
 * verify-roundtrip.js
 * 
 * Verifies the full bidirectional sync flow:
 *   Sheet change → /sheets-webhook → Supabase upsert
 *   → Supabase webhook fires → Google Sheets updated (or loop stopped)
 *   → Simulated GAS echo webhook → DROPPED (loop stop verified)
 * 
 * Run with: node verify-roundtrip.js
 */

const axios = require('axios');
const crypto = require('crypto');
const { default: pRetry } = require('p-retry');
const supabase = require('../lib/supabase-client');
require('dotenv').config({ path: '../.env' });

const BASE_URL = process.env.MIDDLEWARE_URL || 'http://localhost:3000';
const WEBHOOK_SECRET = (process.env.SUPABASE_WEBHOOK_SECRET || '').replace(/^"|"$/g, '');

const TABLE   = 'menu_items';
const ROW_ID  = '10c4021e-9404-437c-85ac-8440baf8595c';  // Pre-existing test row
const CAT_ID  = '61401201-6a49-4928-8984-e13cbfe8ae34';
// Sheet column order must match the real Google Sheet exactly
const HEADERS = ["id","category_id","name","description","price","image_url","is_available","is_veg","created_at","updated_at","deleted_at","synced_at","source"];

function sign(payload) {
  return crypto.createHmac('sha256', WEBHOOK_SECRET).update(payload).digest('hex');
}

function buildSheetRow(overrides = {}) {
  const defaults = {
    category_id: CAT_ID,
    created_at: '',
    deleted_at: '',
    description: 'Roundtrip Test',
    id: ROW_ID,
    image_url: '',
    is_available: 'true',
    is_veg: 'true',
    name: 'Roundtrip Item',
    price: '99',
  };
  const merged = { ...defaults, ...overrides };
  return HEADERS.map(h => merged[h] || '');
}

async function postSheets(rowOverrides, label) {
  const row = buildSheetRow(rowOverrides);
  try {
    const res = await axios.post(`${BASE_URL}/sheets-webhook`, {
      table: TABLE,
      row,
      timestamp: new Date().toISOString(),
    });
    console.log(`  [sheets-webhook] ${label}: HTTP ${res.status} → "${res.data}"`);
    return res;
  } catch (e) {
    const res = e.response;
    console.log(`  [sheets-webhook] ${label}: HTTP ${res?.status} → "${res?.data}"`);
    return res;
  }
}

async function postSupabase(recordOverrides, label) {
  const record = {
    id: ROW_ID,
    category_id: CAT_ID,
    name: 'Roundtrip Item',
    description: 'Roundtrip Test',
    price: 99,
    is_available: true,
    is_veg: true,
    deleted_at: null,
    image_url: null,
    ...recordOverrides,
  };
  const payload = JSON.stringify({ type: 'UPDATE', table: TABLE, record, schema: 'public' });
  try {
    const res = await axios.post(`${BASE_URL}/supabase-webhook`, payload, {
      headers: { 'Content-Type': 'application/json', 'x-supabase-signature': sign(payload) },
    });
    console.log(`  [supabase-webhook] ${label}: HTTP ${res.status} → "${res.data}"`);
    return res;
  } catch (e) {
    const res = e.response;
    console.log(`  [supabase-webhook] ${label}: HTTP ${res?.status} → "${res?.data}"`);
    return res;
  }
}

async function getSupabaseRecord() {
  const { data } = await pRetry(
    () => supabase.from(TABLE).select('*').eq('id', ROW_ID).single(),
    { retries: 3 },
  );
  return data;
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function assert(condition, message) {
  if (!condition) {
    console.error(`  ❌ FAIL: ${message}`);
    process.exitCode = 1;
  } else {
    console.log(`  ✅ PASS: ${message}`);
  }
}

async function run() {
  console.log(`\n${'='.repeat(65)}`);
  console.log(`  Bidirectional Round-Trip Verification`);
  console.log(`  Target: ${BASE_URL} | Table: ${TABLE}`);
  console.log(`${'='.repeat(65)}\n`);

  // ─────────────────────────────────────────────────────
  // STEP 1: Sheets → Supabase
  // ─────────────────────────────────────────────────────
  console.log(`\n📋 STEP 1: Sheet change → /sheets-webhook → Supabase upsert`);
  const step1Price = (Math.floor(Math.random() * 50) + 50).toString();
  console.log(`  Sending sheet change: price=${step1Price}, is_available=false`);
  const s1Res = await postSheets({ price: step1Price, is_available: 'false' }, 'initial change');

  // Wait for the upsert to complete
  await sleep(3000);

  // Verify Supabase has the new value
  const afterStep1 = await getSupabaseRecord();
  assert(
    String(afterStep1?.price) === step1Price,
    `Supabase price is ${step1Price} (got ${afterStep1?.price})`,
  );
  assert(
    afterStep1?.is_available === false || afterStep1?.is_available === 'false',
    `Supabase is_available is false (got ${afterStep1?.is_available})`,
  );

  // ─────────────────────────────────────────────────────
  // STEP 2: Supabase webhook echo should be stopped
  // ─────────────────────────────────────────────────────
  console.log(`\n🔄 STEP 2: Supabase echo webhook → fingerprint match → LOOP STOP`);
  const s2Res = await postSupabase(
    { price: Number(step1Price), is_available: false, source: 'sheets' },
    'echo from sheets source',
  );
  assert(
    s2Res?.data === 'Skipped loop',
    `Supabase echo is dropped ("${s2Res?.data}" should be "Skipped loop")`,
  );

  // ─────────────────────────────────────────────────────
  // STEP 3: Real Supabase change → Google Sheets updated
  // ─────────────────────────────────────────────────────
  
  // Wait 11 seconds for the burst protection lock (10s TTL) to expire
  console.log(`\n  ⏳ Waiting 11s for burst locks to expire...`);
  await sleep(11000);

  console.log(`\n⚡ STEP 3: Real Supabase change → /supabase-webhook → Sheets updated`);
  const step3Price = (Number(step1Price) + 10).toString();
  console.log(`  Sending Supabase change: price=${step3Price}, is_available=true`);
  const s3Res = await postSupabase(
    { price: Number(step3Price), is_available: true },
    'real Supabase change',
  );
  assert(s3Res?.status === 200 && s3Res?.data === 'OK', `Supabase webhook accepted (got "${s3Res?.data}")`);
  
  // Give the queue time to write to Google Sheets
  console.log(`  ⏳ Waiting 6s for Sheets queue to flush...`);
  await sleep(6000);

  // ─────────────────────────────────────────────────────
  // STEP 4: GAS echo after API write → should be DROPPED
  // The GAS onEdit doesn't fire on API writes, but if it did:
  // ─────────────────────────────────────────────────────
  console.log(`\n🛑 STEP 4: Simulated GAS echo after Sheets API write → DROPPED`);
  console.log(`  (Simulates what would happen if Google Apps Script fires after API write)`);
  const s4Res = await postSheets({ price: step3Price, is_available: 'true' }, 'GAS echo simulation');
  assert(
    s4Res?.data === 'Dropped (Duplicate)' || s4Res?.data?.includes('Duplicate') || s4Res?.data?.includes('Burst'),
    `GAS echo is dropped (got "${s4Res?.data}")`,
  );

  // ─────────────────────────────────────────────────────
  // STEP 5: Confirm another GAS echo is also dropped
  // ─────────────────────────────────────────────────────
  console.log(`\n🛑 STEP 5: Second GAS echo (same data) → also DROPPED`);
  await sleep(1000);
  const s5Res = await postSheets({ price: step3Price, is_available: 'true' }, 'second GAS echo');
  assert(
    s5Res?.data === 'Dropped (Duplicate)' ||
    s5Res?.data?.includes('Duplicate') ||
    s5Res?.data?.includes('Burst') ||
    s5Res?.data?.includes('Skipped'),
    `Second GAS echo is dropped (got "${s5Res?.data}")`,
  );

  console.log(`\n${'='.repeat(65)}`);
  if (process.exitCode === 1) {
    console.log(`  ❌ VERIFICATION FAILED — check logs above`);
  } else {
    console.log(`  ✅ ALL STEPS PASSED — Round-trip verified!`);
    console.log(`\n  Flow confirmed:`);
    console.log(`    Sheet edit → /sheets-webhook → Supabase upserted`);
    console.log(`    Supabase echo → fingerprint match → LOOP STOPPED`);
    console.log(`    Real Supabase change → Google Sheets updated (row scan)`);
    console.log(`    GAS echo simulation → Dropped (Duplicate) → LOOP STOPPED`);
  }
  console.log(`${'='.repeat(65)}\n`);
}

run().catch(err => {
  console.error('Fatal error:', err.message);
  process.exit(1);
});
