const axios = require('axios');
const crypto = require('crypto');
const logger = require('./lib/logger');
require('dotenv').config();

const BASE_URL = 'http://localhost:3000';
const WEBHOOK_SECRET = (process.env.SUPABASE_WEBHOOK_SECRET || "").replace(/^"|"$/g, '');

function calculateSignature(payload, secret) {
  return crypto.createHmac('sha256', secret).update(payload).digest('hex');
}

async function simulateSupabaseWebhook(table, record, type = 'UPDATE') {
  const payload = JSON.stringify({
    type,
    table,
    record,
    schema: 'public'
  });
  const signature = calculateSignature(payload, WEBHOOK_SECRET);
  
  try {
    return await axios.post(`${BASE_URL}/supabase-webhook`, payload, {
      headers: {
        'Content-Type': 'application/json',
        'x-supabase-signature': signature
      }
    });
  } catch (error) {
    return error.response;
  }
}

async function simulateSheetsWebhook(table, row, headers) {
  const payload = {
    table,
    row,
    timestamp: new Date().toISOString()
  };
  
  try {
    return await axios.post(`${BASE_URL}/sheets-webhook`, payload);
  } catch (error) {
    return error.response;
  }
}

async function runStressTest() {
  console.log("🚀 Starting Heavy Stress Test...");
  
  const testId = "10c4021e-9404-437c-85ac-8440baf8595c"; // Use an existing ID
  const table = "menu_items";
  const headers = ["id", "category_id", "name", "description", "price", "image_url", "is_available", "is_veg", "created_at", "updated_at", "deleted_at", "synced_at", "source"];
  
  const baseRecord = {
    id: testId,
    category_id: "61401201-6a49-4928-8984-e13cbfe8ae34",
    name: "Stress Test Item",
    price: "100",
    is_available: "true",
    is_veg: "true"
  };

  // 1. Concurrent Bursts from Supabase
  console.log("\n--- Stage 1: Supabase Burst (Concurrency Check) ---");
  const supabasePromises = Array(5).fill(null).map(() => simulateSupabaseWebhook(table, { ...baseRecord, price: "101" }));
  const supabaseResults = await Promise.all(supabasePromises);
  const successCount = supabaseResults.filter(r => r && r && r.status === 200).length;
  const burstCount = supabaseResults.filter(r => r && r.data === "OK (Burst Protected)").length;
  const failCount = supabaseResults.filter(r => !r || (r.status !== 200 && r.data !== "OK (Burst Protected)")).length;
  console.log(`Supabase Burst: ${successCount} succeeded, ${burstCount} burst protected, ${failCount} failed.`);

  // 2. Concurrent Bursts from Sheets
  console.log("\n--- Stage 2: Sheets Burst (Concurrency Check) ---");
  const sheetRow = [testId, baseRecord.category_id, "Sheets Stress", "Desc", "110", "", "TRUE", "TRUE", "", "", "", "", "sheets"];
  const sheetsPromises = Array(5).fill(null).map(() => simulateSheetsWebhook(table, sheetRow, headers));
  const sheetsResults = await Promise.all(sheetsPromises);
  const sSuccessCount = sheetsResults.filter(r => r && r && r.status === 200).length;
  const sBurstCount = sheetsResults.filter(r => r && r && r.status === 200 && typeof r.data === 'string' && r.data.includes("Burst Protected")).length;
  console.log(`Sheets Burst: ${sSuccessCount} succeeded, ${sBurstCount} burst protected.`);

  // 3. Alternating Rapid Fire (Loop Check)
  console.log("\n--- Stage 3: Alternating Rapid Fire (Bidirectional Loop Check) ---");
  for (let i = 0; i < 3; i++) {
    console.log(`Iteration ${i+1}...`);
    const sRes = await simulateSupabaseWebhook(table, { ...baseRecord, price: (120 + i).toString() });
    const hRes = await simulateSheetsWebhook(table, [testId, baseRecord.category_id, "Iter " + i, "Desc", (120 + i).toString(), "", "TRUE", "TRUE", "", "", "", "", "sheets"], headers);
    console.log(`  Supabase: ${sRes.status}, Sheets: ${hRes.status}`);
  }

  // 4. Local Echo Simulation
  console.log("\n--- Stage 4: Local Echo Simulation ---");
  const echoRecord = { ...baseRecord, source: "sheets", price: "122" };
  const echoRes = await simulateSupabaseWebhook(table, echoRecord);
  console.log(`Local Echo Result: ${echoRes.data} (Expected: Skipped loop)`);

  console.log("\n✅ Stress Test Completed. Check logs for details.");
}

runStressTest().catch(console.error);
