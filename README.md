# OLX Used Car ETL Pipeline

<!-- Uncomment this badge after you add a CI workflow (e.g. .github/workflows/ci.yml)
[![CI](https://github.com/CodeSavesMe/Car-ETL-Pipeline-pacmann-introde-3/actions/workflows/ci.yml/badge.svg)](https://github.com/CodeSavesMe/Car-ETL-Pipeline-pacmann-introde-3/actions/workflows/ci.yml)
-->
[![Python](https://img.shields.io/badge/Python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![Luigi](https://img.shields.io/badge/Orchestrator-Luigi-0d6efd.svg)](https://luigi.readthedocs.io/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

An end-to-end ETL pipeline that scrapes used car listings from **OLX Indonesia**, parses and cleans the data, transforms it into an analytics-ready format, and loads it into **PostgreSQL**, while also producing a JSON snapshot of inserted records.

---

## Architecture

```mermaid
flowchart LR
    %% Section 1: User and Pipeline Initiation
    subgraph "User"
        U[run_pipeline.sh / Luigi CLI]
    end

    %% Section 2: Luigi Pipeline Tasks (ETL Flow)
    subgraph "Luigi Pipeline (ETL Flow)"
        S["Scrape Task\nPlaywright"] --> P["Parse Task\nBeautifulSoup"]
        P --> T["Transform Task\nPandas + Business Rules"]
        T --> L["Load Task\nSQLAlchemy"]
    end

    %% Connection: User triggers the Scrape Task
    U --> S

    %% Section 3: Data Targets (Outputs)
    %% Scrape Task output: Raw HTML
    S --> HTML["Raw HTML\n(data/raw_html/*.html)"]
    
    %% Parse Task output: Parsed CSV
    P --> CSV_PARSED["Parsed CSV\n(data/parsed/*.csv)"]
    
    %% Transform Task output: Transformed CSV
    T --> CSV_TRANS["Transformed CSV\n(data/transformed/*.csv)"]

    %% Load Task outputs: Final Database and Audit Snapshot
    L --> DB[(PostgreSQL\nscrape_data table)]
    L --> JSON_SNAP["JSON Snapshot\n(data/inserted/*.json)"]
````

---

## Features

* Automated ETL pipeline orchestrated with **Luigi**

  * Tasks: `Scrape` → `Parse` → `Transform` → `Load` with explicit dependencies.
* Robust web scraping with **Playwright**

  * Handles dynamic content, infinite scrolling, and pop-up dialogs.
* HTML parsing to structured data with **BeautifulSoup**

  * Extracts title, raw price, listing URL, location, posted time, installment, and year/mileage summary.
* Data cleaning and enrichment using **Pandas**

  * Normalizes prices and installments to numeric forms.
  * Splits year/mileage into `year`, `lower_km`, and `upper_km`.
  * Standardizes location format and posted-time representation.
  * Imputes missing installment values using simple business rules.
* Database loading with **SQLAlchemy**

  * Inserts into a PostgreSQL table (`scrape_data`).
  * Writes a JSON snapshot of inserted records for auditing.
* Configuration via `.env` file

  * Supports either a single `DB_URL` or separate `POSTGRES_*` environment variables.
* Centralized logging with **loguru**

  * Console and rotating log files for both pipeline and transformation stages.

---

## Technology Stack

* Language: Python 3.11+
* Workflow Orchestration: Luigi
* Scraping: Playwright (async)
* Parsing: BeautifulSoup4
* Data Processing: Pandas, NumPy
* Database: PostgreSQL
* Database Access: SQLAlchemy, psycopg2
* Configuration: python-dotenv
* Logging: loguru
* Auxiliary: Bash helper script (`run_pipeline.sh`)
* Developer tooling: Makefile, pytest

---

## Project Structure

```text
.
├── scraps.py                     # Luigi tasks (Scrape, Parse, Transform, Load)
├── run_pipeline.sh               # Helper script to run full pipeline
├── requirements.txt
├── README.md
├── Makefile                      # Lint/format/test helpers
├── logs/
│   ├── pipeline.log              # High-level pipeline logs
│   └── etl_log_*.log             # Detailed transform logs
├── data/
│   ├── raw_html/                 # Scraped HTML from OLX
│   ├── parsed/                   # Parsed CSV (raw structured data)
│   ├── transformed/              # Cleaned CSV ready for DB
│   └── inserted/                 # JSON snapshots of inserted records
├── tests/                        # Unit tests for parser/transformer
└── source/
    └── etl/
        ├── etl_scraper.py        # Playwright-based scraper
        ├── etl_parser.py         # BeautifulSoup HTML parser
        ├── etl_transformer.py    # Data cleaning and enrichment
        ├── db_loader.py          # PostgreSQL loader (SQLAlchemy)
        └── utils/
            └── etl_selector.py   # CSS/XPath selectors and constants
```

---

## Pipeline Overview

### 1. Scrape (Playwright)

**File:** `source/etl/etl_scraper.py`
**Luigi Task:** `Scrape` (defined in `scraps.py`)

Responsibilities:

* Builds a search URL based on the keyword, for example:
  `https://www.olx.co.id/mobil-bekas_c198/q-{keyword}`
* Uses Playwright (Chromium) with a custom User-Agent.
* Handles:

  * Navigation timeouts (logs a warning and continues with partially loaded content).
  * Standard pop-ups such as notifications and modals.
  * Location selection (e.g., `"Indonesia"`).
* Implements an infinite-scroll style loading:

  * Attempts to click the `"Load more"` button.
  * Falls back to scrolling to page bottom if the button is not available.
  * Stops after several iterations without new items.
* Outputs:

  * Full-page screenshot in `screenshots/<keyword>.png`.
  * Raw HTML in `data/raw_html/<slug>.html`.

---

### 2. Parse (BeautifulSoup)

**File:** `source/etl/etl_parser.py`
**Luigi Task:** `Parse`

Responsibilities:

* Converts raw HTML into a structured dataset with the following columns:

  * `title`
  * `price`
  * `listing_url` (normalized with `BASE_URL`)
  * `location`
  * `posted_time`
  * `installment`
  * `year_mileage`
* Handles both:

  * Relative posted times (`"hari ini"`, `"kemarin"`, `"N hari yang lalu"`), converting them into a `DD Mon` format.
  * Absolute dates already in `DD Mon` format (e.g., `"18 Nov"`).
* Writes the parsed CSV to `data/parsed/<slug>.csv`.

---

### 3. Transform (Pandas)

**File:** `source/etl/etl_transformer.py`
**Luigi Task:** `Transform`

Responsibilities:

* Cleans and enriches parsed data to make it suitable for analytical and database usage.

Key transformations:

1. **Price normalization**

   * Strips non-digit characters and converts to `float`, e.g.
     `"Rp 450.000.000"` → `450000000.0`.

2. **Year and mileage parsing**

   * Parses `year_mileage` strings such as `"2018 - 70.000-75.000 km"` into:

     * `year`
     * `lower_km`
     * `upper_km`

3. **URL enrichment**

   * Ensures `listing_url` values are absolute URLs by prefixing `BASE_URL` if required.

4. **Location normalization**

   * Cleans location strings by retaining only the first segment before `"."`, `" | "`, or `" - "`.

5. **Installment normalization**

   * Converts textual installment information (e.g., `"8,9jt-an/bln"`) to numeric monthly amount in IDR:

     * `"8,9jt-an/bln"` → `8_900_000.0`.

6. **Installment imputation**

   * For rows with missing installment values, estimates the monthly installment based on:

     * 30% down payment.
     * 11% additional costs.
     * 20% interest on the loan.
     * 36-month tenor.
   * Stores the estimate in `installment` and flags such rows via `installment_imputed = True`.

7. **Posted time validation**

   * Retains short tokens (e.g. `"26 Nov"`).
   * Marks suspiciously long or malformed values as missing.

The task outputs a cleaned and enriched CSV in `data/transformed/<slug>.csv`.

---

### 4. Load (PostgreSQL and JSON Snapshot)

**File:** `source/etl/db_loader.py`
**Luigi Task:** `Load`

Responsibilities:

* Reads the transformed dataset from CSV or an in-memory DataFrame.
* Drops technical helper columns, such as `installment_imputed`, before inserting into the database.
* Converts `NaN` and `pd.NA` to `None` so PostgreSQL receives `NULL`.
* Safely casts the `year` column to a nullable integer type.
* Connects to PostgreSQL using SQLAlchemy and reflects the target table `scrape_data`.
* Inserts all records as a batch.
* Writes a JSON snapshot of all inserted rows to `data/inserted/<slug>_inserted.json`.

Example table schema:

```sql
CREATE TABLE IF NOT EXISTS scrape_data (
    uuid         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title        VARCHAR(255),
    price        NUMERIC(15,2),
    listing_url  VARCHAR(255),
    location     VARCHAR(255),
    installment  NUMERIC(15,2),
    posted_time  VARCHAR(255),
    year         INT,
    lower_km     DOUBLE PRECISION,
    upper_km     DOUBLE PRECISION,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## Installation

1. Clone the repository:

```bash
git clone git@github.com:CodeSavesMe/Car-ETL-Pipeline-pacmann-introde-3.git
cd Car-ETL-Pipeline-pacmann-introde-3
```

2. Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
# On Windows:
# .venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Install Playwright browsers:

```bash
playwright install
```

---

## Configuration

Create a `.env` file in the project root:

```env
# Option 1: Single URL (recommended)
DB_URL=postgresql+psycopg2://user:password@localhost:5435/scrape-olx

# Option 2: Components (fallback)
POSTGRES_USER=db_mentoring_w8
POSTGRES_PASSWORD=db_mentoring_w8
POSTGRES_DB=scrape-olx
POSTGRES_HOST=localhost
POSTGRES_PORT=5435
```

Ensure your PostgreSQL instance (or Docker container) is configured with matching credentials and ports.

---

## Running the Pipeline

### 1. Using the helper script

Example:

```bash
bash run_pipeline.sh
```

The script typically performs:

1. Virtual environment activation.
2. Loading environment variables from `.env`.
3. Starting the PostgreSQL Docker container (for example, `db_mentoring_w8`).
4. Running the full Luigi pipeline up to the `Load` task.

### 2. Using Luigi directly

Example command:

```bash
python scraps.py Load \
  --local-scheduler \
  --keyword "Mitsubishi Pajero Sport" \
  --html-path data/raw_html/pajero.html \
  --parsed-path data/parsed/pajero.csv \
  --transformed-path data/transformed/pajero_transformed.csv \
  --inserted-path data/inserted/pajero_inserted.json
```

---

## Logging

### Pipeline-level logging (`scraps.py`)

Configured via loguru to:

* Print informative messages to the console during Luigi task execution.
* Write rotating log files to `logs/pipeline.log` with:

  * Maximum size: 5 MB per file.
  * Retention: 7 days.
  * Compression: zip.
  * Minimum log level: DEBUG.

### Transformation-level logging (`olx_transformer.py`)

* Writes detailed transformation logs to `logs/etl_log_{time}.log`.
* Logs include:

  * Failed or invalid price, year, and mileage parsing.
  * Suspicious posted-time values.
  * Number of rows whose installment was estimated.

---

## Development & Testing

Basic development helpers are provided via the `Makefile`:

```bash
# Run static analysis (ruff)
make lint

# Auto-format code (black)
make format

# Run unit tests (pytest)
make test
```

Unit tests live under the `tests/` directory and focus on:

* HTML parsing behaviour (`parse_html`).
* Transformation logic (price/mileage parsing, installment estimation, etc.).

---

## License

This project is licensed under the MIT License.
Refer to the [`LICENSE`](LICENSE) file for full details.
