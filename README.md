# Supabase ↔ Google Sheets Bidirectional Sync

A production-ready, real-time synchronization system that keeps a Supabase database and a Google Sheet in sync. This project handles bidirectional updates, prevents synchronization loops using Redis-based locking, and ensures data integrity with a self-healing reconciliation script.

## 🏗 Architecture

The system consists of three main parts:

1.  **Node.js Middleware**: An Express server that acts as a bridge between Supabase and Google Sheets.
2.  **Supabase Webhooks**: Triggered on database changes (`INSERT`, `UPDATE`, `DELETE`) to notify the middleware.
3.  **Google Apps Script**: An `onEdit` trigger in the Google Sheet that notifies the middleware of user changes.
4.  **Redis (Upstash)**: Used for row-level locking (to prevent sync loops) and idempotency (to prevent duplicate processing).

### 🔄 Sync Flow

- **Supabase → Sheets**: Supabase Webhook → Middleware (verifies signature & acquires lock) → Google Sheets API.
- **Sheets → Supabase**: Google Sheet Edit → Apps Script → Middleware (acquires lock) → Supabase Upsert.

## 🚀 Features

- **Loop Prevention**: Redis-based synchronization locks (15s TTL) prevent events from bouncing back and forth infinitely.
- **Rate Limiting**: Intelligent queuing via `p-queue` ensures Google Sheets API limits (300 requests/min) are respected.
- **Reliability**: External API calls use automatic retries with exponential backoff.
- **Dead-Letter Queue (DLQ)**: Failed sync attempts are logged to a `sync_errors` table in Supabase for manual review.
- **Security**: Mandatory Supabase webhook signature verification and `helmet` for secure HTTP headers.
- **Self-Healing**: A reconciliation script compares data bidirectional and fixes inconsistencies.

## 🛠 Setup Instructions

### 1. Supabase Database Setup

Ensure your Supabase table has the following columns:

- `id` (Primary Key, e.g., uuid or int)
- `synced_at` (timestamptz): Last time the row was synced.
- `source` (text): Tracks if the update came from `supabase` or `sheets`.

Create a **Dead-Letter Queue Table** (`sync_errors`):

```sql
create table sync_errors (
  id bigint generated always as identity primary key,
  source text not null, -- 'supabase' or 'sheets'
  payload jsonb,
  error_message text,
  stack text,
  created_at timestamptz default now()
);
```

### 2. Middleware Deployment

Deploy this folder to a service like Railway, Fly.io, or Render.
Set the following environment variables:

```env
SUPABASE_URL=...
SUPABASE_SERVICE_ROLE_KEY=...
SUPABASE_WEBHOOK_SECRET=...
GOOGLE_SERVICE_ACCOUNT_EMAIL=...
GOOGLE_PRIVATE_KEY="..."
GOOGLE_SHEET_ID=...
REDIS_URL=...
PORT=3000
NODE_ENV=production
```

### 3. Google Sheets Integration

1.  Open your Google Sheet.
2.  Go to **Extensions > Apps Script**.
3.  Delete any existing code and paste the content of `google-apps-script/Code.gs`.
4.  Update the `MIDDLEWARE_URL` in the script.
5.  Run the `setupTrigger` function once to authorize.
6.  Ensure **Column A** is `id` and **Column B** is `synced_at`.

### 4. Supabase Webhook Configuration

1.  In Supabase Dashboard, go to **Database > Webhooks**.
2.  Create a webhook:
    - **Table**: Your target table.
    - **Events**: `INSERT`, `UPDATE`, `DELETE`.
    - **URL**: `https://your-middleware.com/supabase-webhook`.
    - **Headers**: Add `X-Supabase-Signature` using your `SUPABASE_WEBHOOK_SECRET`.

## ⚙️ Maintenance & Scripts

- **Initial Backfill**: `npm run backfill` reads all Supabase rows and seeds the Google Sheet.
- **Reconciliation**: `npm run reconcile` performs a bidirectional comparison and fixes any drifted data. It is recommended to run this as a cron job every 15-30 minutes.
- **Health Check**: `GET /health` monitors the service status and Redis connectivity.
