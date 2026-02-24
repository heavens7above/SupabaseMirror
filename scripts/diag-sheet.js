require('dotenv').config({ path: __dirname + '/../.env' });
const { sheets, sheetId } = require('../lib/sheets-client');

async function run() {
  const table = process.argv[2] || 'menu_items';
  console.log('Fetching', table, 'from sheet', sheetId);
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: sheetId,
    range: `${table}!A1:Z5`
  });
  console.log(JSON.stringify(res.data.values, null, 2));
}
run().catch(console.error);
