require('dotenv').config();
const { sheets, sheetId } = require('./lib/sheets-client');

async function getHeaders() {
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: sheetId,
      range: 'menu_items!1:1'
    });
    console.log(JSON.stringify(res.data.values[0]));
    process.exit(0);
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

getHeaders();
