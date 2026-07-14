"""
ETL Validation Utility — Single Entry Point
============================================
User runs: python run.py

This script does everything automatically:
1. Reads DB connections from .env
2. Connects to source and target databases
3. Discovers all tables, columns, data types, row counts
4. Auto-matches source tables to target tables
5. Auto-detects primary keys
6. Auto-maps columns (exact match + pattern match)
7. Generates validation config
8. Runs all validation layers (stats for all, hash for unique-key tables)
9. Generates CSV + HTML report
10. Opens HTML report in browser

The user never touches any config file.
"""

import argparse
import os
import sys
import webbrowser
import yaml
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
from src.db_connector import DBConnector
from src.metadata_discovery import discover_metadata, get_columns
from src.key_detector import detect_candidate_keys
from src.mapping_suggester import match_tables, match_columns
from src.config_generator import generate_config
from src.config_loader import load_config, get_settings
from src.source_builder import load_mappings, expand_mappings, build_source_query, get_source_label
from src.validators.hash_validator import run_hash_validator
from src.validators.stats_validator import run_stats_validator
from src.report_generator import generate_report
from src.results_store import save_results, generate_meaningful_run_name
from src.utils import generate_run_id, logger

load_dotenv()

OUTPUT_DIR = "output"
CONFIG_PATH = os.path.join(OUTPUT_DIR, "generated_validation_config.yaml")
MAPPINGS_FILE = "table_mappings.yaml"


def _load_user_mappings() -> list | None:
    mappings = load_mappings(MAPPINGS_FILE)
    return mappings if mappings else None


def _resolve_env(key: str) -> str | None:
    val = os.environ.get(key, "")
    if val and not val.startswith("${"):
        return val
    return None


def _build_db_config(prefix: str, db_type: str) -> dict | None:
    """
    Build a db_config dict from environment variables for the given prefix
    (SOURCE or TARGET) and db_type (duckdb, postgres, snowflake).

    Returns None if the required variables are not set.
    """
    if db_type == "duckdb":
        path = _resolve_env(f"{prefix}_DUCKDB_PATH")
        if not path:
            return None
        return {"db_type": "duckdb", "path": path}

    if db_type == "postgres":
        host = _resolve_env(f"{prefix}_DB_HOST")
        port = _resolve_env(f"{prefix}_DB_PORT") or "5432"
        database = _resolve_env(f"{prefix}_DB_NAME")
        schema = _resolve_env(f"{prefix}_DB_SCHEMA") or "public"
        username = _resolve_env(f"{prefix}_DB_USER")
        password = _resolve_env(f"{prefix}_DB_PASSWORD")
        if not all([host, database, username, password]):
            return None
        return {
            "db_type": "postgres",
            "host": host,
            "port": port,
            "database": database,
            "schema": schema,
            "username": username,
            "password": password,
        }

    if db_type == "snowflake":
        account = _resolve_env(f"{prefix}_SNOWFLAKE_ACCOUNT")
        user = _resolve_env(f"{prefix}_SNOWFLAKE_USER")
        password = _resolve_env(f"{prefix}_SNOWFLAKE_PASSWORD")
        warehouse = _resolve_env(f"{prefix}_SNOWFLAKE_WAREHOUSE")
        database = _resolve_env(f"{prefix}_SNOWFLAKE_DATABASE")
        schema = _resolve_env(f"{prefix}_SNOWFLAKE_SCHEMA") or "PUBLIC"
        role = _resolve_env(f"{prefix}_SNOWFLAKE_ROLE")
        if not all([account, user, password, database]):
            return None
        cfg = {
            "db_type": "snowflake",
            "host": account,
            "username": user,
            "password": password,
            "database": database,
            "schema": schema,
        }
        if warehouse:
            cfg["warehouse"] = warehouse
        if role:
            cfg["role"] = role
        return cfg

    return None


def _detect_db_config(prefix: str) -> dict | None:
    """
    Auto-detect the DB type for a given prefix (SOURCE or TARGET) by checking
    which set of environment variables is populated.
    Priority: duckdb > postgres > snowflake.
    """
    for db_type in ("duckdb", "postgres", "snowflake"):
        cfg = _build_db_config(prefix, db_type)
        if cfg:
            return cfg
    return None


def _print_banner(title: str):
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def _print_step(step: int, total: int, msg: str):
    print(f"\n[{step}/{total}] {msg}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="ETL Validation Utility — run discovery and validation automatically."
    )
    parser.add_argument(
        "--layers",
        default=None,
        nargs="*",
        help="Layers to run. e.g. --layers 1 2 3  or  --layers 2 3  Default: 2 3",
    )
    parser.add_argument(
        "--table", "--tables",
        default=None,
        nargs="*",
        dest="table",
        help="Mapping name(s) to validate. e.g. --table dim_product_category",
    )
    return parser.parse_args()


def run():
    args = parse_args()

    # --layers 1 2 3  →  {1, 2, 3}
    if not args.layers:
        requested_layers = {2, 3}
    else:
        requested_layers = set(int(l) for l in args.layers)

    # --table / --tables dim_product_category  →  {"dim_product_category"}
    requested_tables = set(t.strip() for t in args.table) if args.table else None

    _print_banner("ETL VALIDATION UTILITY")
    print(f"  Started : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Layers  : {sorted(requested_layers)}")
    if requested_tables:
        print(f"  Tables  : {sorted(requested_tables)}")

    # ------------------------------------------------------------------ #
    # Step 1 — Read DB connections from .env
    # ------------------------------------------------------------------ #
    _print_step(1, 6, "Reading database connections from .env ...")

    source_db_config = _detect_db_config("SOURCE")
    target_db_config = _detect_db_config("TARGET")

    if not source_db_config:
        print("\nERROR: Source database connection not configured in .env file.")
        print("Set one of the following in .env:")
        print("  DuckDB   : SOURCE_DUCKDB_PATH")
        print("  Postgres : SOURCE_DB_HOST, SOURCE_DB_NAME, SOURCE_DB_USER, SOURCE_DB_PASSWORD")
        print("  Snowflake: SOURCE_SNOWFLAKE_ACCOUNT, SOURCE_SNOWFLAKE_USER, SOURCE_SNOWFLAKE_PASSWORD, SOURCE_SNOWFLAKE_DATABASE")
        sys.exit(1)

    if not target_db_config:
        print("\nERROR: Target database connection not configured in .env file.")
        print("Set one of the following in .env:")
        print("  DuckDB   : TARGET_DUCKDB_PATH")
        print("  Postgres : TARGET_DB_HOST, TARGET_DB_NAME, TARGET_DB_USER, TARGET_DB_PASSWORD")
        print("  Snowflake: TARGET_SNOWFLAKE_ACCOUNT, TARGET_SNOWFLAKE_USER, TARGET_SNOWFLAKE_PASSWORD, TARGET_SNOWFLAKE_DATABASE")
        sys.exit(1)

    # Extra validation for DuckDB file existence
    if source_db_config["db_type"] == "duckdb" and not os.path.exists(source_db_config["path"]):
        print(f"\nERROR: Source DuckDB file not found: {source_db_config['path']}")
        sys.exit(1)

    if target_db_config["db_type"] == "duckdb" and not os.path.exists(target_db_config["path"]):
        print(f"\nERROR: Target DuckDB file not found: {target_db_config['path']}")
        sys.exit(1)

    def _db_label(cfg: dict) -> str:
        if cfg["db_type"] == "duckdb":
            return f"duckdb:{cfg['path']}"
        if cfg["db_type"] == "snowflake":
            return f"snowflake:{cfg['host']}/{cfg['database']}/{cfg.get('schema', 'PUBLIC')}"
        return f"{cfg['db_type']}:{cfg.get('host', '')}:{cfg.get('port', '')}/{cfg.get('database', '')}"

    print(f"  Source DB : {_db_label(source_db_config)}")
    print(f"  Target DB : {_db_label(target_db_config)}")

    source_conn = None
    target_conn = None
    try:
        source_conn = DBConnector(source_db_config, label="source").connect()
    except ConnectionError as e:
        print(f"\n{'='*60}")
        print("  CONNECTION ERROR")
        print(f"{'='*60}")
        print(str(e))
        if source_db_config["db_type"] == "duckdb":
            print("\n  HOW TO FIX:")
            print("  1. Open DBeaver")
            print("  2. Right-click your DuckDB connection")
            print("  3. Click 'Disconnect'")
            print("  4. Run this script again: python run.py")
        print(f"{'='*60}")
        sys.exit(1)

    try:
        target_conn = DBConnector(target_db_config, label="target").connect()
    except ConnectionError as e:
        source_conn.close()
        print(f"\n{'='*60}")
        print("  CONNECTION ERROR")
        print(f"{'='*60}")
        print(str(e))
        print(f"{'='*60}")
        sys.exit(1)

    try:
        # -------------------------------------------------------------- #
        # Step 2 — Discover tables and metadata
        # -------------------------------------------------------------- #
        _print_step(2, 6, "Discovering tables, columns and data types ...")

        # If a table filter is given and all requested mappings are in table_mappings.yaml,
        # only fetch metadata for those specific tables — skip full DB scan
        user_mappings_for_filter = _load_user_mappings() if requested_tables else None
        scoped_source_tables = None
        scoped_target_tables = None

        if requested_tables and user_mappings_for_filter:
            expanded_all = expand_mappings(user_mappings_for_filter)
            matched_pairs = [
                p for p in expanded_all
                if p["name"].lower() in {t.lower() for t in requested_tables}
            ]
            if matched_pairs:
                scoped_source_tables = []
                scoped_target_tables = []
                for p in matched_pairs:
                    for src in p["sources"]:
                        scoped_source_tables.append(src["table"])
                    tgt = p["target_table"].split(".")[-1]  # short name for target lookup
                    scoped_target_tables.append(p["target_table"])
                    scoped_target_tables.append(tgt)

        from src.metadata_discovery import get_columns, get_row_count

        def _scoped_metadata(conn, db_type, tables):
            from src.metadata_discovery import map_to_standard_type
            metadata = {}
            for table in tables:
                columns = get_columns(conn, table, db_type)
                row_count = get_row_count(conn, table)
                metadata[table] = {
                    "columns": columns,
                    "row_count": row_count,
                    "column_map": {
                        col["column_name"]: map_to_standard_type(col["column_type"])
                        for col in columns
                    },
                }
                logger.info(f"  Scoped: {table} ({len(columns)} columns, {row_count} rows)")
            return metadata

        if scoped_source_tables:
            source_metadata = _scoped_metadata(source_conn, source_db_config["db_type"], scoped_source_tables)
            # For target, also discover by short name for lookup
            tgt_set = list(dict.fromkeys(scoped_target_tables))  # preserve order, deduplicate
            target_metadata = {}
            for tbl in tgt_set:
                cols = get_columns(target_conn, tbl, target_db_config["db_type"])
                rc = get_row_count(target_conn, tbl)
                from src.metadata_discovery import map_to_standard_type
                target_metadata[tbl] = {
                    "columns": cols,
                    "row_count": rc,
                    "column_map": {c["column_name"]: map_to_standard_type(c["column_type"]) for c in cols},
                }
                logger.info(f"  Scoped: {tbl} ({len(cols)} columns, {rc} rows)")
        else:
            source_metadata = discover_metadata(source_conn, source_db_config["db_type"])
            target_metadata = discover_metadata(target_conn, target_db_config["db_type"])

        source_tables = list(source_metadata.keys())
        target_tables = list(target_metadata.keys())

        print(f"  Source tables found : {len(source_tables)}")
        print(f"  Target tables found : {len(target_tables)}")

        for t in source_tables:
            rc = source_metadata[t]["row_count"]
            print(f"    Source: {t} ({rc:,} rows)")
        for t in target_tables:
            rc = target_metadata[t]["row_count"]
            print(f"    Target: {t} ({rc:,} rows)")

        # -------------------------------------------------------------- #
        # Step 3 — Match source tables to target tables
        # -------------------------------------------------------------- #
        _print_step(3, 6, "Matching source tables to target tables ...")

        user_mappings = _load_user_mappings()
        table_matches = []

        # ── Step A: Load user-defined mappings from YAML ──────────────────
        yaml_source_tables = set()
        yaml_target_tables = set()

        if user_mappings:
            print(f"  Using user-defined mappings from '{MAPPINGS_FILE}':")
            expanded = expand_mappings(user_mappings)
            for pair in expanded:
                src_label = pair["source_label"]
                tgt_table = pair["target_table"]
                is_multi = len(pair["sources"]) > 1
                join_info = " (multi-source join)" if is_multi else ""
                print(f"  [YAML] {src_label} -> {tgt_table}{join_info}")
                table_matches.append({
                    "name": pair["name"],
                    "source_table": pair["sources"][0]["table"],
                    "source_label": src_label,
                    "sources": pair["sources"],
                    "target_table": tgt_table,
                    "confidence": "high",
                    "match_reason": "user-defined mapping",
                    "status": "AUTO",
                    "source_where": pair["source_where"],
                    "target_where": pair["target_where"],
                    "is_multi_source": is_multi,
                    "compare_columns": pair.get("compare_columns"),
                })
                for src in pair["sources"]:
                    yaml_source_tables.add(src["table"].lower())
                yaml_target_tables.add(tgt_table.lower())

        # ── Step B: Auto-discover remaining unmatched tables ──────────────
        # Skip auto-matching when a --tables filter is active — only run the requested mappings
        if requested_tables:
            print(f"  Table filter active — skipping auto-matching for unspecified tables.")
        else:
            remaining_sources = [t for t in source_tables if t.lower() not in yaml_source_tables]
            remaining_targets = [t for t in target_tables if t.lower() not in yaml_target_tables]

            if remaining_sources:
                print(f"  Auto-matching remaining tables not in '{MAPPINGS_FILE}'...")
                auto_matches = match_tables(remaining_sources, remaining_targets)
                for m in auto_matches:
                    m["is_multi_source"] = False
                    m["sources"] = [{"table": m["source_table"]}]
                    m["source_label"] = m["source_table"]
                    m["source_where"] = None
                    m["target_where"] = None
                    m["compare_columns"] = None
                    status_icon = "[AUTO]" if m["status"] == "AUTO" else "[?]"
                    print(f"  {status_icon} {m['source_table']} -> {m['target_table'] or 'NO MATCH'} [{m['confidence']}]")
                    table_matches.append(m)

        # -------------------------------------------------------------- #
        # Step 4 — Detect keys and map columns
        # -------------------------------------------------------------- #
        _print_step(4, 6, "Detecting primary keys and mapping columns ...")

        key_suggestions = {}
        column_matches = {}

        for match in table_matches:
            src_table = match["source_table"]
            tgt_table = match["target_table"]
            sources = match.get("sources", [{"table": src_table}])
            is_multi = match.get("is_multi_source", False)

            # For multi-source: combine columns from all source tables
            if is_multi:
                src_cols = []
                for src in sources:
                    tbl = src["table"]
                    tbl_cols = source_metadata.get(tbl, {}).get("columns", [])
                    # prefix alias to avoid ambiguity in display only
                    alias = src.get("alias", "")
                    for col in tbl_cols:
                        src_cols.append({
                            "column_name": col["column_name"],
                            "column_type": col["column_type"],
                            "_source_table": tbl,
                            "_alias": alias,
                        })
                # Use primary table for key detection
                key_info = detect_candidate_keys(source_conn, src_table,
                    source_metadata.get(src_table, {}).get("columns", []))
            else:
                src_cols = source_metadata.get(src_table, {}).get("columns", [])
                key_info = detect_candidate_keys(source_conn, src_table, src_cols)

            key_suggestions[src_table] = key_info
            print(f"  Key for {match['source_label']}: {key_info['suggested_key']} [{key_info['confidence']}]")

            if tgt_table:
                # Resolve target table — try fully qualified name first, then short name
                tgt_short = tgt_table.split(".")[-1]
                tgt_cols = (
                    target_metadata.get(tgt_table, {}).get("columns")
                    or target_metadata.get(tgt_short, {}).get("columns")
                    or target_metadata.get(tgt_short.upper(), {}).get("columns")
                    or source_metadata.get(tgt_table, {}).get("columns", [])
                )
                if not tgt_cols:
                    logger.warning(f"  No columns found for target table '{tgt_table}'. Skipping column mapping.")
                    continue
                col_map = match_columns(src_cols, tgt_cols)
                column_matches[src_table] = col_map
                matched = sum(1 for c in col_map if c["target_col"])
                print(f"  Columns: {match['source_label']} -> {tgt_table}: {matched}/{len(col_map)} matched")

        # -------------------------------------------------------------- #
        # Step 5 — Generate validation config automatically
        # -------------------------------------------------------------- #
        _print_step(5, 6, "Generating validation config ...")

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        config_path = generate_config(
            source_db_config, target_db_config,
            table_matches, source_metadata, target_metadata,
            key_suggestions, column_matches,
            output_path=CONFIG_PATH,
        )
        print(f"  Config written : {config_path}")

        # -------------------------------------------------------------- #
        # Step 6 — Run validation for all tables
        # -------------------------------------------------------------- #
        _print_step(6, 6, "Running validation ...")

        config = load_config(config_path)
        settings = get_settings(config)
        run_id = generate_run_id()
        run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        all_results = []

        for table_config in config["tables"]:
            src_tbl = table_config["source_table"]
            tgt_tbl = table_config["target_table"]
            sources = table_config.get("sources", [{"table": src_tbl}])
            validation_layers = table_config.get("validation_layers", {})

            # Skip if user specified specific tables and none of them match
            # Match against: source_table, all source tables in join, target_table, mapping name
            if requested_tables:
                all_tables_in_mapping = set()
                all_tables_in_mapping.add(src_tbl.lower())
                all_tables_in_mapping.add(tgt_tbl.lower())
                all_tables_in_mapping.add(table_config.get("source_label", "").lower())
                all_tables_in_mapping.add(table_config.get("name", "").lower())
                for src in sources:
                    all_tables_in_mapping.add(src["table"].lower())

                requested_lower = set(t.lower().strip() for t in requested_tables)
                if not requested_lower & all_tables_in_mapping:
                    print(f"\n  Skipping: {src_tbl} (not in --table filter)")
                    continue

            print(f"\n  Validating: {src_tbl} -> {tgt_tbl}")

            # Determine which layers to run
            key_info = key_suggestions.get(src_tbl, {})
            key_is_reliable = key_info.get("confidence") in ("high", "medium")

            run_l1 = 1 in requested_layers
            run_l2 = 2 in requested_layers and key_is_reliable
            run_l3 = 3 in requested_layers

            if run_l1:
                print(f"    Layer 1 (Row-by-row) ...")
                from src.validators.row_validator import run_row_validator
                results = run_row_validator(
                    source_conn, target_conn, table_config, settings, run_id, run_timestamp
                )
                all_results.extend(results)
                passed = sum(1 for r in results if r["status"] == "PASS")
                failed = sum(1 for r in results if r["status"] == "FAIL")
                print(f"    Layer 1: {passed} passed, {failed} failed")

            if run_l2:
                print(f"    Layer 2 (Hash + drill-down) ...")
                results = run_hash_validator(
                    source_conn, target_conn, table_config, settings, run_id, run_timestamp
                )
                all_results.extend(results)
                passed = sum(1 for r in results if r["status"] == "PASS")
                failed = sum(1 for r in results if r["status"] == "FAIL")
                print(f"    Layer 2: {passed} passed, {failed} failed")
            else:
                if 2 not in requested_layers:
                    print(f"    Layer 2 skipped (not requested)")
                elif not key_is_reliable:
                    print(f"    Layer 2 skipped (primary key not unique — use Layer 3 for stats only)")

            if run_l3:
                print(f"    Layer 3 (Statistics) ...")
                results = run_stats_validator(
                    source_conn, target_conn, table_config, settings, run_id, run_timestamp
                )
                all_results.extend(results)
                passed = sum(1 for r in results if r["status"] == "PASS")
                failed = sum(1 for r in results if r["status"] == "FAIL")
                print(f"    Layer 3: {passed} passed, {failed} failed")

            # ---------------------------------------------------------- #
            # Unmatched columns — WARNING for source cols with no target
            # match and target cols with no source match
            # ---------------------------------------------------------- #
            matched_src_cols = {c["source_col"] for c in table_config.get("columns", [])}
            matched_tgt_cols = {c["target_col"] for c in table_config.get("columns", [])}

            all_src_cols = [c["column_name"] for c in source_metadata.get(src_tbl, {}).get("columns", [])]
            all_tgt_cols = [c["column_name"] for c in target_metadata.get(tgt_tbl, {}).get("columns", [])]

            for col in all_src_cols:
                if col not in matched_src_cols:
                    all_results.append({
                        "run_id": run_id, "run_timestamp": run_timestamp,
                        "table_name": src_tbl, "layer": 0,
                        "column_name": col, "key_value": "",
                        "check_type": "unmatched_column",
                        "source_result": "exists in source",
                        "target_result": "no target match",
                        "status": "WARNING",
                        "difference": "",
                        "issue_type": "unmatched source column",
                        "severity": "LOW",
                        "remarks": f"Source column '{col}' has no matching target column in '{tgt_tbl}'. It was not validated.",
                        "source_query": "", "target_query": "",
                    })

            for col in all_tgt_cols:
                if col not in matched_tgt_cols:
                    all_results.append({
                        "run_id": run_id, "run_timestamp": run_timestamp,
                        "table_name": src_tbl, "layer": 0,
                        "column_name": col, "key_value": "",
                        "check_type": "unmatched_column",
                        "source_result": "no source match",
                        "target_result": "exists in target",
                        "status": "WARNING",
                        "difference": "",
                        "issue_type": "unmatched target column",
                        "severity": "LOW",
                        "remarks": f"Target column '{col}' in '{tgt_tbl}' has no matching source column in '{src_tbl}'. It was not validated.",
                        "source_query": "", "target_query": "",
                    })

            unmatched = sum(1 for r in all_results if r.get("check_type") == "unmatched_column" and r.get("table_name") == src_tbl)
            if unmatched:
                print(f"    Unmatched columns: {unmatched} warnings")

    finally:
        source_conn.close()
        target_conn.close()

    # ------------------------------------------------------------------ #
    # Generate report
    # ------------------------------------------------------------------ #
    summary = generate_report(all_results, config, run_id, run_timestamp)

    # Save results to output/results.db for Grafana dashboard
    validated_tables = [t["source_table"] for t in config.get("tables", [])]
    run_name = generate_meaningful_run_name(validated_tables)
    save_results(all_results, summary, run_id, run_timestamp, run_name=run_name)

    report_html = os.path.join(OUTPUT_DIR, f"validation_report_{run_id}.html")
    report_csv  = os.path.join(OUTPUT_DIR, f"validation_report_{run_id}.csv")

    _print_banner("VALIDATION COMPLETE")
    print(f"  Run ID         : {run_id}")
    print(f"  Tables Checked : {summary['total_tables_checked']}")
    print(f"  Tables Passed  : {summary['tables_passed']}")
    print(f"  Tables Failed  : {summary['tables_failed']}")
    print(f"  Total Checks   : {summary['total_checks']}")
    print(f"  Passed         : {summary['passed_checks']}")
    print(f"  Failed         : {summary['failed_checks']}")
    print(f"  Warnings       : {summary['warning_checks']}")
    print(f"\n  HTML Report    : {report_html}")
    print(f"  CSV Report     : {report_csv}")
    print("=" * 60)

    # Auto-open HTML report in browser
    try:
        abs_path = os.path.abspath(report_html)
        webbrowser.open(f"file:///{abs_path}")
        print("\n  Report opened in browser automatically.")
    except Exception:
        print(f"\n  Open manually: {report_html}")

    if summary["failed_checks"] > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    run()
