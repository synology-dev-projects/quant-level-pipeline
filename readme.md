# 📊 Quant Level Pipeline

Automated ETL (Extract, Transform, Load) pipeline for parsing, structuring, and storing **TradingEdge Quant Price Levels** into an Oracle Cloud Database, with automated push notifications via **nfty**.

---

## 🌟 Overview

The **Quant Level Pipeline** processes market analysis posts from the TradingEdge platform to extract key price boundaries (Start Levels, End Levels, Buy/Sell indicators, and notes). It structures the raw HTML/JSON data into a clean relational format and syncs it with Oracle Database.

### Key Features
* **Dual Execution Modes:** Supports both full **Historical Ingestion** (overwrite/rebuild) and automated **Daily Incremental Ingestion** (upsert new posts since max database timestamp).
* **Automated Data Cleaning & Deduplication:** Parses HTML markup, extracts price targets, normalizes timestamps, and merges duplicate intra-day post updates.
* **Oracle Cloud Integration:** Direct upsert and merge logic into `QUANT_LVL_DATA_TE` database table with transaction rollbacks on failure.
* **Real-time Notifications:** Sends formatted execution summaries and data alerts to **nfty** push channels.

---

## 📁 Repository Structure

```text
quant-level-pipeline/
├── src/
│   ├── extract.py         # TradingEdge API fetcher & HTML body scraper
│   ├── transform.py       # Regex parsing, price level extraction & DataFrame normalization
│   ├── load.py            # Oracle DB connection, table upsert/overwrite & nfty notifications
│   └── scripts/           # Execution entry points
│       ├── manual_historical.py   # Full historical ETL runner
│       └── daily_incremental.py   # Daily incremental ETL runner
├── tests/
│   ├── conftest.py                # Pytest fixtures & environment setup
│   ├── test_extract_functions.py  # Extraction module tests
│   ├── test_transform_functions.py# HTML & regex parser tests
│   └── test_running_scripts.py    # Pipeline integration & DB contract tests
├── pyproject.toml / requirements.txt
├── verify.sh                      # CI/CD verification script
└── README.md
```

---

## ⚙️ Architecture & Data Workflows

### 1. Historical Ingestion Workflow (`manual_historical.py`)

Runs a complete backfill of all available historical quant level posts.

```text
User / Cron
  │ (Runs manual_historical.py)
  ▼
[Config Module] ──► Reads secrets & Oracle credentials
  │
  ▼
[Extract Module]
  ├── Loop (Page 1...N):
  │     <-- Fetch JSON feed from TradingEdge API
  │     --> Collect Post Links
  └── Loop (Each Link):
        <-- Scrape HTML content from Post Link
        --> Append raw JSON/HTML payload to list
  │
  ▼
[Transform Module]
  ├── Convert raw payload list to Pandas DataFrame
  ├── Parse HTML tags & extract price bounds (Start/End Prices, Buy/Sell indicator)
  ├── Validate schema & datatypes
  └── Merge duplicate intraday records (combining notes)
  │
  ▼
[Load Module]
  ├── Connect to Oracle DB
  ├── Overwrite / Rebuild `QUANT_LVL_DATA_TE` table
  └── Commit Transaction & trigger nfty alert
```

---

### 2. Daily Incremental Workflow (`daily_incremental.py`)

Queries the database for the max recorded timestamp and only extracts new posts created since that date.

```text
User / Cron / CI-CD
  │ (Runs daily_incremental.py)
  ▼
[Config Module] ──► Reads secrets & Oracle credentials
  │
  ▼
[Load Module] ──► Queries `SELECT MAX(DATETIME) FROM QUANT_LVL_DATA_TE`
  │                Returns: `cutoff_date` (e.g. Latest stored date)
  ▼
[Extract Module]
  ├── Loop (Page 1...N):
  │     <-- Fetch JSON from TradingEdge API
  │     └── Check: Is post timestamp <= cutoff_date?
  │           [YES] ──► STOP Pagination Loop immediately (Early Exit)
  │           [NO]  ──► Append Link for extraction
  └── Loop (New Links only):
        <-- Scrape HTML content
  │
  ▼
[Transform Module] ──► Clean & validate new incoming posts
  │
  ▼
[Load Module]
  ├── Upsert/Merge new records into Oracle DB
  ├── Check Commit Status:
  │     [Success] ──► COMMIT transaction & send nfty notification
  │     [Error]   ──► ROLLBACK transaction & alert failure
```

---

## 🚀 Getting Started

### Prerequisites
* **Python 3.12+**
* Access to shared `common-lib` (contains shared config & Oracle connectors)
* Active `.env` file in `common_config` directory containing:
  ```env
  ORACLE_USER=your_oracle_user
  ORACLE_PASS=your_oracle_password
  ORACLE_SERVICE=your_oracle_service_name
  TE_COOKIE=your_tradingedge_cookie
  NTFY_ENDPOINT=https://richntfynotifier.synology.me
  ```

### Running the Pipeline Locally

1. **Incremental Daily Run:**
   ```bash
   python src/scripts/daily_incremental.py
   ```

2. **Full Historical Backfill:**
   ```bash
   python src/scripts/manual_historical.py
   ```

---

## 🧪 Running Unit & Integration Tests

Execute the full test suite using `pytest`:

```bash
# Run all tests with common-lib included in pythonpath
python -m pytest -o "pythonpath=src ../common-lib" tests/ -v
```

*Note: Database integration tests include automatic availability checks (`is_oracle_available`), gracefully skipping DB assertions if Oracle Cloud is offline.*