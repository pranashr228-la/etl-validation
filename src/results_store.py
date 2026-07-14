"""
results_store.py
================
Saves validation results into output/results.db (DuckDB) after every run.

Two tables are created/updated:
  1. validation_results      — every single check row
  2. validation_run_summary  — one summary row per run

Each run gets a unique meaningful name like:
    etl_validation_20260711_162759

Grafana reads from results.db for the dashboard.
"""

import os
import time
import duckdb
from datetime import datetime
from src.utils import logger

RESULTS_DB_PATH = os.path.join("output", "etl_validation.duckdb")


def generate_meaningful_run_name(tables: list[str] = None) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if tables:
        clean = [t.split(".")[-1].lower() for t in tables[:2]]
        label = "_vs_".join(clean)
        return f"etl_{label}_{ts}"
    return f"etl_validation_{ts}"


def _get_connection() -> duckdb.DuckDBPyConnection:
    os.makedirs("output", exist_ok=True)
    retries = 10
    for attempt in range(retries):
        try:
            return duckdb.connect(RESULTS_DB_PATH)
        except Exception as e:
            if attempt < retries - 1:
                logger.warning(f"[results_store] File locked by Windows, retrying in 3s... ({attempt + 1}/{retries})")
                time.sleep(3)
            else:
                raise e


def _create_tables(conn: duckdb.DuckDBPyConnection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS validation_results (
            run_id           VARCHAR,
            run_timestamp    VARCHAR,
            table_name       VARCHAR,
            layer            INTEGER,
            column_name      VARCHAR,
            key_value        VARCHAR,
            check_type       VARCHAR,
            source_result    VARCHAR,
            target_result    VARCHAR,
            status           VARCHAR,
            difference       VARCHAR,
            issue_type       VARCHAR,
            severity         VARCHAR,
            remarks          VARCHAR,
            source_query     VARCHAR,
            target_query     VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS validation_run_summary (
            run_id               VARCHAR,
            run_name             VARCHAR,
            run_timestamp        VARCHAR,
            tables_checked       INTEGER,
            tables_passed        INTEGER,
            tables_failed        INTEGER,
            total_checks         INTEGER,
            passed_checks        INTEGER,
            failed_checks        INTEGER,
            warnings             INTEGER,
            overall_status       VARCHAR,
            pass_rate            DOUBLE
        )
    """)


def save_results(results: list[dict], summary: dict, run_id: str, run_timestamp: str, run_name: str = None):
    if not results:
        logger.warning("[results_store] No results to save. Skipping.")
        return

    conn = _get_connection()
    try:
        _create_tables(conn)

        # ── Insert all check rows ──────────────────────────────────────────
        for row in results:
            conn.execute("""
                INSERT INTO validation_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                str(row.get("run_id", "")),
                str(row.get("run_timestamp", "")),
                str(row.get("table_name", "")),
                int(row.get("layer", 0)),
                str(row.get("column_name", "")),
                str(row.get("key_value", "")),
                str(row.get("check_type", "")),
                str(row.get("source_result", "")),
                str(row.get("target_result", "")),
                str(row.get("status", "")),
                str(row.get("difference", "")),
                str(row.get("issue_type", "")),
                str(row.get("severity", "")),
                str(row.get("remarks", "")),
                str(row.get("source_query", "")),
                str(row.get("target_query", "")),
            ])

        # ── Insert one summary row ─────────────────────────────────────────
        total   = summary.get("total_checks", 0)
        passed  = summary.get("passed_checks", 0)
        failed  = summary.get("failed_checks", 0)
        overall = "PASS" if failed == 0 else "FAIL"
        rate    = round(passed / total * 100, 2) if total > 0 else 0.0

        meaningful_name = run_name or f"etl_validation_{run_timestamp.replace(' ', '_').replace(':', '').replace('-', '')}"

        conn.execute("""
            INSERT INTO validation_run_summary VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            run_id,
            meaningful_name,
            run_timestamp,
            summary.get("total_tables_checked", 0),
            summary.get("tables_passed", 0),
            summary.get("tables_failed", 0),
            total,
            passed,
            failed,
            summary.get("warning_checks", 0),
            overall,
            rate,
        ])

        conn.commit()
        logger.info(f"[results_store] Saved {len(results)} rows + summary to {RESULTS_DB_PATH}")

    except Exception as e:
        logger.error(f"[results_store] Failed to save results: {e}")
        raise
    finally:
        conn.close()


def get_all_runs() -> list[dict]:
    if not os.path.exists(RESULTS_DB_PATH):
        return []
    conn = _get_connection()
    try:
        df = conn.execute("SELECT * FROM validation_run_summary ORDER BY run_timestamp DESC").df()
        return df.to_dict(orient="records")
    finally:
        conn.close()
