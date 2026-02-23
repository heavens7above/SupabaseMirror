# 🪞 Supabase Mirror

[![Railway Deployment](https://img.shields.io/badge/Deploy%20on-Railway-0b0d0e?style=for-the-badge&logo=railway)](https://railway.app/new/template?template=https://github.com/heavens7above/SupabaseMirror)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](https://opensource.org/licenses/MIT)

**Supabase Mirror** is a lightweight, high-performance middleware that creates a seamless **bidirectional mirror** between your Supabase database and Google Sheets.

Whether you're building an internal tool, a shared inventory system, or just want a "no-code" interface for your database, Supabase Mirror keeps your data in sync with millisecond precision.

---

## ✨ Features

- 🔄 **Bidirectional Sync**: Edits in Google Sheets sync to Supabase, and database changes sync back to Sheets.
- 📂 **Multi-Table Support**: Sync any number of tables by simply adding matching tabs to your Google Sheet.
- 🛡️ **Conflict Resolution**: Smart timestamp-based locking prevents data overwrites and infinite sync loops.
- 🚀 **Generic Mapping**: No hardcoding! Add or remove columns in your sheet, and the middleware adapts automatically.
- 🔐 **Secure & Robust**: Signed webhooks, Redis-based locking, and p-retry integration for high availability.

---

## 🚀 Quick Start

1. **Deploy the Middleware**: Click the "Deploy on Railway" button above or follow the [Manual Setup Guide](./docs/SETUP.md).
2. **Configure Supabase**: Add required columns and webhooks. See [Supabase Setup](./docs/SETUP.md#1-supabase-preparation).
3. **Connect Google Sheets**: Install the Apps Script from `google-apps-script/Code.gs`.

---

## 🛠️ Maintenance Scripts

The project includes utility scripts for state management:

- **Backfill**: Populate a sheet tab with existing database data.
  ```bash
  npm run backfill <table_name>
  ```
- **Reconcile**: Fix minor data discrepancies and ensure symmetry.
  ```bash
  npm run reconcile <table_name>
  ```

---

## 📖 Learn More

- [Detailed Setup Guide](./docs/SETUP.md)
- [System Architecture](./docs/ARCHITECTURE.md)
- [Contributing Guidelines](./CONTRIBUTING.md)

---

## 📝 License

Distributed under the MIT License. See `LICENSE` for more information.

Built with ❤️ for the open-source community.
