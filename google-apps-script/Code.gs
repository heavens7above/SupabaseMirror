/**
 * Google Apps Script for bidirectional sync.
 * To be installed as a "Bound" script in the Google Sheet.
 */

const MIDDLEWARE_URL =
  "https://supabasemirror-production.up.railway.app/sheets-webhook"; // Update with your deployed URL

/**
 * Triggered on any edit in the spreadsheet.
 */
function onEdit(e) {
  const sheet = e.source.getActiveSheet();
  const range = e.range;
  const startRow = range.getRow();
  const numRows = range.getNumRows();

  // Skip if entirely in header row
  if (startRow === 1 && numRows === 1) return;

  // Handle single row or bulk edit
  // Note: Apps Script onEdit is limited for massive pastes.
  // For safety, we process each row but limit total rows in one event.
  const limit = Math.min(numRows, 100); // Caps at 100 rows to prevent execution timeout

  for (let i = 0; i < limit; i++) {
    const currentRow = startRow + i;
    if (currentRow === 1) continue; // Skip header

    const rowData = sheet
      .getRange(currentRow, 1, 1, sheet.getLastColumn())
      .getValues()[0];
    const id = rowData[0];
    if (!id) continue;

    const payload = {
      row: rowData,
      table: sheet.getName(),
      timestamp: new Date().toISOString(),
    };

    const options = {
      method: "post",
      contentType: "application/json",
      payload: JSON.stringify(payload),
      muteHttpExceptions: true,
      headers: {
        "X-Sheets-Source": "GoogleAppsScript",
      },
    };

    try {
      UrlFetchApp.fetch(MIDDLEWARE_URL, options);
    } catch (err) {
      console.error("Sync Error on row " + currentRow + ":", err.message);
    }
  }
}

/**
 * One-time setup: adds the ON_EDIT trigger programmatically if needed.
 * This is safer than relying on simple triggers for external requests.
 */
function setupTrigger() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ScriptApp.newTrigger("onEdit").forSpreadsheet(ss).onEdit().create();
}
