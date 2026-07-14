import argparse
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
from src.db_connector import DBConnector
from src.metadata_discovery import discover_metadata
from src.key_detector import detect_candidate_keys
from src.mapping_suggester import match_tables, match_columns
from src.config_generator import generate_config
from src.discovery_report_generator import generate_discovery_report
from src.utils import logger

load_dotenv()


def parse_args():
    parser = argparse.ArgumentParser(
        description="ETL Validator — Discover mode: auto-detect tables, columns, keys and generate config."
    )
    parser.add_argument(
        "--source-db",
        default=None,
        help="Path to source DuckDB file. Overrides SOURCE_DUCKDB_PATH in .env",
    )
    parser.add_argument(
        "--target-db",
        default=None,
        help="Path to target DuckDB file. Overrides TARGET_DUCKDB_PATH in .env",
    )
    parser.add_argument(
        "--output-config",
        default="output/generated_validation_config.yaml",
        help="Where to write the generated config. Default: output/generated_validation_config.yaml",
    )
    parser.add_argument(
        "--output-report",
        default="output/discovery_report.html",
        help="Where to write the discovery HTML report. Default: output/discovery_report.html",
    )
    return parser.parse_args()


def _resolve_db_path(arg_val, env_key):
    if arg_val:
        return arg_val
    val = os.environ.get(env_key, "")
    if val and not val.startswith("${"):
        return val
    return None


def main():
    args = parse_args()

    source_path = _resolve_db_path(args.source_db, "SOURCE_DUCKDB_PATH")
    target_path = _resolve_db_path(args.target_db, "TARGET_DUCKDB_PATH")

    if not source_path:
        print("\nERROR: Source DB path not provided.")
        print("Set SOURCE_DUCKDB_PATH in .env or use --source-db flag.")
        sys.exit(1)

    if not target_path:
        print("\nERROR: Target DB path not provided.")
        print("Set TARGET_DUCKDB_PATH in .env or use --target-db flag.")
        sys.exit(1)

    if not os.path.exists(source_path):
        print(f"\nERROR: Source DuckDB file not found: {source_path}")
        sys.exit(1)

    if not os.path.exists(target_path):
        print(f"\nERROR: Target DuckDB file not found: {target_path}")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("  ETL VALIDATOR — DISCOVER MODE")
    print("=" * 60)
    print(f"  Source DB : {source_path}")
    print(f"  Target DB : {target_path}")
    print("=" * 60)

    source_db_config = {"db_type": "duckdb", "path": source_path}
    target_db_config = {"db_type": "duckdb", "path": target_path}

    source_conn = DBConnector(source_db_config, label="source").connect()
    target_conn = DBConnector(target_db_config, label="target").connect()

    try:
        print("\n[1/5] Discovering source metadata...")
        source_metadata = discover_metadata(source_conn, "duckdb")

        print(f"\n[2/5] Discovering target metadata...")
        target_metadata = discover_metadata(target_conn, "duckdb")

        print(f"\n[3/5] Matching tables...")
        source_tables = list(source_metadata.keys())
        target_tables = list(target_metadata.keys())
        table_matches = match_tables(source_tables, target_tables)

        for m in table_matches:
            status_icon = "[OK]" if m["status"] == "AUTO" else "[?]"
            print(f"  {status_icon} {m['source_table']} -> {m['target_table'] or 'NO MATCH'} [{m['confidence']}] {m['match_reason']}")

        print(f"\n[4/5] Detecting keys and matching columns...")
        key_suggestions = {}
        column_matches = {}

        for match in table_matches:
            src_table = match["source_table"]
            tgt_table = match["target_table"]

            src_cols = source_metadata[src_table]["columns"]
            key_suggestions[src_table] = detect_candidate_keys(source_conn, src_table, src_cols)
            print(f"  Key for '{src_table}': {key_suggestions[src_table]['suggested_key']} [{key_suggestions[src_table]['confidence']}]")

            if tgt_table:
                tgt_cols = target_metadata[tgt_table]["columns"]
                column_matches[src_table] = match_columns(src_cols, tgt_cols)
                matched = sum(1 for c in column_matches[src_table] if c["target_col"])
                total = len(column_matches[src_table])
                print(f"  Columns for '{src_table}' -> '{tgt_table}': {matched}/{total} matched")

        print(f"\n[5/5] Generating config and discovery report...")
        config_path = generate_config(
            source_db_config, target_db_config,
            table_matches, source_metadata, target_metadata,
            key_suggestions, column_matches,
            output_path=args.output_config,
        )

        report_path = generate_discovery_report(
            table_matches, source_metadata, target_metadata,
            key_suggestions, column_matches,
            source_db=source_path, target_db=target_path,
            output_path=args.output_report,
        )

    finally:
        source_conn.close()
        target_conn.close()

    print("\n" + "=" * 60)
    print("  DISCOVERY COMPLETE")
    print("=" * 60)
    print(f"  Config      : {config_path}")
    print(f"  HTML Report : {report_path}")
    print("\n  NEXT STEPS:")
    print(f"  1. Open {report_path} in your browser to review mappings")
    print(f"  2. Edit {config_path} if any NEEDS_REVIEW mappings are wrong")
    print(f"  3. Add source_where filter if your dbt logic has one")
    print(f"  4. Run validation:")
    print(f"     python -m src.main --config {config_path}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
