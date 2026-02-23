const supabase = require('../lib/supabase-client');
const { sheets, sheetId } = require('../lib/sheets-client');
const redis = require('../lib/redis-client');
const syncLogic = require('../lib/sync-logic');

async function backfill() {
  console.log('Starting backfill...');
  
  try {
    // 1. Fetch all rows from Supabase
    // Using pagination for large tables
    let hasMore = true;
    let offset = 0;
    const limit = 500;
    let allRecords = [];

    while (hasMore) {
      const { data, error } = await supabase
        .from('synced_table') // Replace with your table name
        .select('*')
        .range(offset, offset + limit - 1)
        .order('id', { ascending: true });

      if (error) throw error;
      
      allRecords = allRecords.concat(data);
      offset += limit;
      hasMore = data.length === limit;
    }

    console.log(`Fetched ${allRecords.length} records from Supabase.`);

    // 2. Clear Sheet (optional/be careful) and write in batches
    // Assuming headers are already there
    const batchSize = 500;
    for (let i = 0; i < allRecords.length; i += batchSize) {
      const batch = allRecords.slice(i, i + batchSize);
      const values = batch.map(record => syncLogic.mapSupabaseToSheets(record));
      
      await sheets.spreadsheets.values.append({
        spreadsheetId: sheetId,
        range: 'Sheet1!A2',
        valueInputOption: 'USER_ENTERED',
        resource: { values },
      });

      // 3. Populate Redis cache (id -> rowIndex)
      // Note: append doesn't return exact row indexes for each item easily.
      // A full read after append might be needed to get accurate row indexes if the sheet wasn't empty.
      
      console.log(`Processed batch ${Math.floor(i / batchSize) + 1}`);
      // Sleep to respect rate limits if needed
      await new Promise(resolve => setTimeout(resolve, 1000));
    }

    // 4. Update Redis id -> rowIndex mapping
    console.log('Updating Redis row index cache...');
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: sheetId,
      range: 'Sheet1!A:A',
    });
    const rows = response.data.values || [];
    const multi = redis.multi();
    rows.forEach((row, index) => {
      if (index === 0) return; // Skip header
      if (row[0]) {
        multi.set(`rowindex:${row[0]}`, index + 1);
      }
    });
    await multi.exec();

    console.log('Backfill completed successfully.');
  } catch (error) {
    console.error('Backfill failed:', error);
  } finally {
    process.exit();
  }
}

backfill();
