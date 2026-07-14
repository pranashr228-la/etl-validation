# ETL Validation Utility

A universal, fully automated Python utility that validates whether source data correctly reached a target database after ETL/dbt transformation.

Works with any domain — AdventureWorks, healthcare, finance, retail, or any other dataset. No hardcoding. No manual intervention.

---

## How It Works

```
You set .env  -->  Run python run.py  -->  See HTML report
```

That is it. The tool does everything else automatically:
- Connects to source and target databases
- Discovers all tables and columns
- Auto-matches source tables to target tables
- Auto-detects primary keys
- Auto-maps columns
- Runs all validation layers
- Generates color-coded HTML + CSV report
- Opens the report in your browser

---

## Requirements

- Python 3.10 or higher
- DuckDB file with your source and target tables
- DBeaver (optional — for creating/viewing tables)

---

## Setup — One Time Only

### Step 1 — Install dependencies

```bash
pip install -r requirements.txt
```

### Step 2 — Create your `.env` file

```bash
copy .env.example .env
```

Open `.env` and set your DuckDB file path:

```
SOURCE_DUCKDB_PATH=C:/Users/yourname/data/mydata.duckdb
TARGET_DUCKDB_PATH=C:/Users/yourname/data/mydata.duckdb
```

> If your source and target tables are in the same DuckDB file, both paths are the same.
> Never commit `.env` to version control.

---

## Running the Validation

### Step 1 — Create your tables in DBeaver

- Open DBeaver and connect to your DuckDB file
- Create your source table (e.g. `SalesOrderHeader`)
- Create your target table (e.g. `FactInternetSales`)
- Insert your data

### Step 2 — Disconnect DBeaver

> **IMPORTANT**: DuckDB allows only one process to open the file at a time.
> Before running the script, you must disconnect DBeaver:
>
> 1. In DBeaver, right-click your DuckDB connection
> 2. Click **Disconnect**

### Step 3 — Run the script

```bash
python run.py
```

That is it. The script runs automatically and opens the HTML report in your browser when done.

---

## What Happens When You Run

```
[1/6] Reading database connections from .env
[2/6] Discovering tables, columns and data types
[3/6] Auto-matching source tables to target tables
[4/6] Detecting primary keys and mapping columns
[5/6] Generating validation config
[6/6] Running validation (Layer 2 + Layer 3)
      --> Report opened in browser automatically
```

---

## Validation Layers

| Layer | What it does | When it runs |
|-------|-------------|-------------|
| Layer 2 | Hash comparison + cell drill-down — finds exactly which row and column differs | Only when primary key is unique and reliable |
| Layer 3 | Statistical comparison — row count, null count, min, max, sum, avg, stddev | Always runs for all tables |

---

## Understanding the Report

The HTML report opens automatically. It shows:

### Summary Section
- Total tables checked
- Tables passed / failed
- Total checks passed / failed / warnings

### Detail Section (one row per check)

| Column | Meaning |
|--------|---------|
| table_name | Which table was checked |
| layer | Which layer detected it (2 or 3) |
| column_name | Which column has the issue |
| check_type | What was checked (hash_match, row_count, sum, avg, etc.) |
| source_result | Value from source |
| target_result | Value from target |
| status | PASS / FAIL / WARNING |
| difference | How different the values are |
| issue_type | Specific issue (missing rows, decimal precision, case mismatch, etc.) |
| severity | HIGH / MEDIUM / LOW |
| remarks | Human-readable explanation |

### Issue Types

| Issue | Meaning |
|-------|---------|
| row count mismatch | Source and target have different number of rows |
| missing rows | A row exists in source but not in target |
| extra rows | A row exists in target but not in source |
| hash mismatch | Row content differs between source and target |
| null mismatch | One side has NULL, other has a value |
| value mismatch | Column value is different |
| case mismatch | Same value but different case (e.g. `ACTIVE` vs `active`) |
| truncation detected | Target value is shorter than source (e.g. `Christophe` vs `Christopher`) |
| extra spaces detected | Trailing or leading spaces |
| date mismatch | Date value differs |
| decimal precision issue | Numeric value differs beyond tolerance |
| decimal scale issue | Number of decimal places differs |
| missing category | A distinct value exists in source but not in target |
| extra category | A distinct value exists in target but not in source |
| duplicate key in source | Primary key is not unique in source |
| unexpected duplicates | Table has duplicate rows |

---

## Output Files

All files are written to the `output/` folder:

| File | Description |
|------|-------------|
| `generated_validation_config.yaml` | Auto-generated config (you can review it) |
| `validation_report_<run_id>.html` | Color-coded HTML report (opens automatically) |
| `validation_report_<run_id>.csv` | CSV report for Excel or further analysis |

---

## Advanced: Running Specific Layers or Tables

If you want more control, you can also run using the config directly:

```bash
# Run only Layer 3 (stats)
python -m src.main --config output/generated_validation_config.yaml --layers 3

# Run only Layer 2 (hash)
python -m src.main --config output/generated_validation_config.yaml --layers 2

# Run all layers
python -m src.main --config output/generated_validation_config.yaml --layers 1,2,3

# Run specific table only
python -m src.main --config output/generated_validation_config.yaml --tables SalesOrderHeader
```

---

## Advanced: Adding Business Filters

If your ETL/dbt logic filters source data (e.g. only online orders), open `output/generated_validation_config.yaml` and add:

```yaml
- source_table: oltp.SalesOrderHeader
  target_table: dwh.FactInternetSales
  source_where: "OnlineOrderFlag = true"   # <-- add your dbt filter here
```

Then re-run:
```bash
python run.py
```

---

## Troubleshooting

### "Cannot open file — used by another process"
DBeaver has the DuckDB file open. Disconnect DBeaver first:
1. Right-click DuckDB connection in DBeaver
2. Click **Disconnect**
3. Run `python run.py` again

### "Source DuckDB file not found"
Check your `.env` file. Make sure `SOURCE_DUCKDB_PATH` points to the correct file path.

### "No tables found"
Make sure you have created and populated tables in DBeaver before running.

### Row count mismatch is expected
If your ETL filters out some rows (e.g. terminated employees, offline orders), add `source_where` in the generated config to match your dbt filter. This tells the tool which rows were intentionally excluded.

---

## Project Structure

```
etl-validation-utility/
├── run.py                            # MAIN ENTRY POINT — run this
├── .env                              # Your DB connection (never commit)
├── .env.example                      # Template for .env
├── requirements.txt                  # Python dependencies
├── config/
│   └── validation_config.yaml        # Manual config (optional)
├── src/
│   ├── main.py                       # Validation runner
│   ├── discover.py                   # Discovery mode runner
│   ├── run.py                        # (same as root run.py)
│   ├── config_loader.py              # Config loader
│   ├── config_generator.py           # Auto config generator
│   ├── db_connector.py               # Database connector (DuckDB, Postgres, etc.)
│   ├── filter_engine.py              # SQL WHERE builder
│   ├── metadata_discovery.py         # Table/column discovery
│   ├── key_detector.py               # Primary key detection
│   ├── mapping_suggester.py          # Table and column matching
│   ├── discovery_report_generator.py # Discovery HTML report
│   ├── report_generator.py           # Validation HTML + CSV report
│   ├── utils.py                      # Shared utilities
│   └── validators/
│       ├── row_validator.py          # Layer 1: row-by-row
│       ├── hash_validator.py         # Layer 2: hash + drill-down
│       └── stats_validator.py        # Layer 3: statistics
├── tests/
│   └── test_validators.py            # 28 automated tests
└── output/                           # Reports and generated config land here
```

---

## Supported Databases

| Database | Status |
|----------|--------|
| DuckDB | Supported |
| PostgreSQL | Supported (add `psycopg2-binary` to requirements) |
| SQL Server | Supported (add `pyodbc` to requirements) |
| MySQL | Supported (add `pymysql` to requirements) |
| Snowflake | Supported (add `snowflake-connector-python` to requirements) |
