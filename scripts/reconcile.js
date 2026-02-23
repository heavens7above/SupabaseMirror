const supabase = require('../lib/supabase-client');
const { sheets, sheetId } = require('../lib/sheets-client');
const syncLogic = require('../lib/sync-logic');
const logger = require('../lib/logger');
const { default: pRetry } = require('p-retry');

async function reconcile() {
  const tableName = process.argv[2] || process.env.SUPABASE_TABLE_NAME || 'menu_items';
  logger.info(`Running reconciliation job for table: ${tableName}...`);

  try {
    const { data: supabaseRecords, error } = await pRetry(() => supabase
      .from(tableName)
      .select('*'), { retries: 3 });
    if (error) throw error;

    const sheetsResponse = await pRetry(() => sheets.spreadsheets.values.get({
      spreadsheetId: sheetId,
      range: `${tableName}!1:5000`, // Support bulk fetch
    }), { retries: 3 });
    const sheetsRows = sheetsResponse.data.values || [];
    const headers = sheetsRows[0] || [];
    const sheetsData = sheetsRows.slice(1).map(row => syncLogic.mapSheetsToSupabase(row, headers));

    const supabaseMap = new Map(supabaseRecords.map(r => [r.id, r]));
    const sheetsMap = new Map(sheetsData.map(r => [r.id, r]));
    const allIds = new Set([...supabaseMap.keys(), ...sheetsMap.keys()]);

    let sheetUpdates = [];

    for (const id of allIds) {
      const supabaseRecord = supabaseMap.get(id);
      const sheetsRecord = sheetsMap.get(id);

      if (!supabaseRecord && sheetsRecord) {
        logger.info(`Row ${id} missing in Supabase. Adding...`);
        const record = syncLogic.mapSheetsToSupabase(sheetsRecord, headers);
        await pRetry(() => supabase.from(tableName).upsert(record), { retries: 3 });
      } else if (supabaseRecord && !sheetsRecord) {
        logger.info(`Row ${id} missing in Sheets. Queuing add...`);
        sheetUpdates.push(syncLogic.mapSupabaseToSheets(supabaseRecord, headers));
      } else if (supabaseRecord && sheetsRecord) {
        const sTime = new Date(supabaseRecord.synced_at).getTime();
        const shTime = new Date(sheetsRecord.synced_at).getTime();

        if (Math.abs(sTime - shTime) > 5000) {
          logger.info(`Discrepancy found for row ${id}. Resolving...`);
          if (sTime > shTime) {
            sheetUpdates.push(syncLogic.mapSupabaseToSheets(supabaseRecord, headers));
          } else {
            const record = syncLogic.mapSheetsToSupabase(sheetsRecord, headers);
            await pRetry(() => supabase.from(tableName).upsert(record), { retries: 3 });
          }
        }
      }
    }

    if (sheetUpdates.length > 0) {
      logger.info(`Pushing ${sheetUpdates.length} updates to Sheets...`);
      await pRetry(() => sheets.spreadsheets.values.append({
        spreadsheetId: sheetId,
        range: `${tableName}!1:1`,
        valueInputOption: 'USER_ENTERED',
        resource: { values: sheetUpdates },
      }), { retries: 3 });
    }

    logger.info('Reconciliation completed.');
  } catch (error) {
    logger.error('Reconciliation failed', { error: error.message });
  } finally {
    process.exit();
  }
}

reconcile();
