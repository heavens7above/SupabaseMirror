const supabase = require('../lib/supabase-client');
const { sheets, sheetId } = require('../lib/sheets-client');
const redis = require('../lib/redis-client');
const syncLogic = require('../lib/sync-logic');

async function backfill() {
  const tableName = process.argv[2] || process.env.SUPABASE_TABLE_NAME || 'menu_items';
  console.log(`Starting backfill for table: ${tableName}...`);
  
  try {
    // 1. Fetch all rows from Supabase
    // Using pagination for large tables
    let hasMore = true;
    let offset = 0;
    const limit = 500;
    let allRecords = [];

    while (hasMore) {
      const { data, error } = await supabase
        .from(tableName)
        .select('*')
        .range(offset, offset + limit - 1)
        .order('id', { ascending: true });

      if (error) throw error;
      
      allRecords = allRecords.concat(data);
      offset += limit;
      hasMore = data.length === limit;
    }

    console.log(`Fetched ${allRecords.length} records from Supabase.`);

    // 2. Fetch headers from the target sheet
    const headerResponse = await sheets.spreadsheets.values.get({
      spreadsheetId: sheetId,
      range: `${tableName}!1:1`,
    });
    const headers = headerResponse.data.values ? headerResponse.data.values[0] : [];
    if (headers.length === 0) throw new Error(`Sheet '${tableName}' has no headers.`);

    // 3. Clear Sheet (keep headers) and write in batches
    console.log(`Clearing existing data from Google Sheet...`);
    await sheets.spreadsheets.values.clear({
      spreadsheetId: sheetId,
      range: `${tableName}!A2:ZZ`,
    });
    
    console.log(`Writing batches to Google Sheet...`);
    const batchSize = 500;
    let currentRow = 2; // start after header
    
    for (let i = 0; i < allRecords.length; i += batchSize) {
      const batch = allRecords.slice(i, i + batchSize);
      const values = batch.map(record => syncLogic.mapSupabaseToSheets(record, headers));
      
      const endRowText = currentRow + values.length - 1;
      await sheets.spreadsheets.values.update({
        spreadsheetId: sheetId,
        range: `${tableName}!A${currentRow}:Z${endRowText}`,
        valueInputOption: 'USER_ENTERED',
        resource: { values },
      });

      console.log(`Processed batch ${Math.floor(i / batchSize) + 1} (Rows ${currentRow}-${endRowText})`);
      currentRow += values.length;
      
      // Sleep to respect rate limits if needed
      await new Promise(resolve => setTimeout(resolve, 1000));
    }

    // 4. Update Redis row index cache for tableName (Optional)
    try {
      console.log(`Attempting to update Redis row index cache for ${tableName}...`);
      const response = await sheets.spreadsheets.values.get({
        spreadsheetId: sheetId,
        range: `${tableName}!A:A`,
      });
      const rows = response.data.values || [];
      const multi = redis.multi();
      rows.forEach((row, index) => {
        if (index === 0) return; // Skip header
        if (row[0]) {
          multi.set(`rowindex:${tableName}:${row[0]}`, index + 1);
        }
      });
      await multi.exec();
      console.log('Redis cache updated successfully.');
    } catch (redisError) {
      console.warn(`Skipping Redis cache update: ${redisError.message}`);
    }

    console.log('Backfill completed successfully.');
  } catch (error) {
    console.error('Backfill failed:', error);
  } finally {
    process.exit();
  }
}

backfill();
