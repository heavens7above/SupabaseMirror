# Setup Guide: Supabase Mirror

This guide will help you set up bidirectional synchronization between your Supabase database and Google Sheets.

## 1. Supabase Preparation

### Schema Updates

Run this SQL in your Supabase SQL Editor to add the required columns to your tables and create an error log table.

```sql
-- Create error log table
CREATE TABLE IF NOT EXISTS sync_errors (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  source text NOT NULL,
  payload jsonb,
  error_message text,
  stack text,
  created_at timestamptz DEFAULT now()
);

-- Add sync columns to ALL public tables automatically
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT table_name
              FROM information_schema.tables
              WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE'
                AND table_name != 'sync_errors')
    LOOP
        EXECUTE format('ALTER TABLE public.%I ADD COLUMN IF NOT EXISTS synced_at timestamptz', r.table_name);
        EXECUTE format('ALTER TABLE public.%I ADD COLUMN IF NOT EXISTS source text DEFAULT ''supabase''', r.table_name);
    END LOOP;
END $$;
```

### Webhooks

1. Go to **Database > Webhooks** in your Supabase Dashboard.
2. Create a webhook for each table you want to sync (e.g., `users`, `orders`).
3. Set the **Method** to `POST`.
4. Set the **URL** to `https://your-deployment-url.com/supabase-webhook`.
5. Add a header: `x-webhook-secret: YOUR_SECRET_HERE`.

## 2. Google Sheets Setup

### Sheet Configuration

1. Create a tab for each database table (the name must match exactly).
2. Set the headers in Row 1 to match your Supabase column names (e.g., `id`, `name`, `synced_at`).

### Apps Script

1. Open your sheet and go to **Extensions > Apps Script**.
2. Copy the contents of `google-apps-script/Code.gs` into the editor.
3. Replace the `MIDDLEWARE_URL` with your deployment URL.
4. Click the **Run** button for the `setupTrigger` function once to activate "onEdit" events.

## 3. Deployment

### Environment Variables

Set these variables in your hosting provider (e.g., Railway):

| Variable                       | Description                                        |
| :----------------------------- | :------------------------------------------------- |
| `SUPABASE_URL`                 | Your Supabase Project URL                          |
| `SUPABASE_SERVICE_ROLE_KEY`    | Your Service Role Key (Keep it secret!)            |
| `SUPABASE_WEBHOOK_SECRET`      | Matches the secret you set in Supabase Webhooks    |
| `GOOGLE_SERVICE_ACCOUNT_EMAIL` | Google Service Account Email                       |
| `GOOGLE_PRIVATE_KEY`           | Multi-line Private Key (Include BEGIN/END markers) |
| `GOOGLE_SHEET_ID`              | The ID of your Spreadsheet                         |
| `REDIS_URL`                    | Redis connection string (required for locking)     |

### Start the Server

```bash
npm install
npm start
```
