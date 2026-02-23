const { google } = require('googleapis');
require('dotenv').config();

const auth = new google.auth.JWT(
  process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
  null,
  process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n'),
  ['https://www.googleapis.com/auth/spreadsheets']
);

const sheets = google.sheets({ version: 'v4', auth });

module.exports = {
  sheets,
  sheetId: process.env.GOOGLE_SHEET_ID
};
