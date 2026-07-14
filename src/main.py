import argparse
import sys
from datetime import datetime

from src.config_loader import load_config, get_settings
from src.db_connector import get_connectors
from src.validators.row_validator import run_row_validator
from src.validators.hash_validator import run_hash_validator
from src.validators.stats_validator import run_stats_validator
from src.report_generator import generate_report
from src.utils import generate_run_id, logger


def parse_args():
    parser = argparse.ArgumentParser(
        description="ETL Validation Utility — validates data migration from source to target."
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to validation_config.yaml",
    )
    parser.add_argument(
        "--layers",
        default="2,3",
        help="Comma-separated layers to run (e.g. 1,2,3). Default: 2,3",
    )
    parser.add_argument(
        "--tables",
        default=None,
        help="Comma-separated table names to validate. Default: all tables in config.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    requested_layers = set(int(l.strip()) for l in args.layers.split(","))
    requested_tables = set(t.strip() for t in args.tables.split(",")) if args.tables else None

    run_id = generate_run_id()
    run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logger.info(f"Starting ETL Validation — Run ID: {run_id}")
    logger.info(f"Config: {args.config}")
    logger.info(f"Layers: {sorted(requested_layers)}")

    config = load_config(args.config)
    settings = get_settings(config)

    logger.info(f"Settings: {settings}")

    source_conn, target_conn = get_connectors(config)

    all_results = []

    try:
        for table_config in config["tables"]:
            table_name = table_config["source_table"]

            if requested_tables and table_name not in requested_tables:
                logger.info(f"Skipping table '{table_name}' (not in --tables filter).")
                continue

            logger.info(f"{'='*60}")
            logger.info(f"Validating table: {table_name} -> {table_config['target_table']}")

            validation_layers = table_config.get("validation_layers", {})
            run_l1 = validation_layers.get("run_layer1", False) and 1 in requested_layers
            run_l2 = validation_layers.get("run_layer2", True) and 2 in requested_layers
            run_l3 = validation_layers.get("run_layer3", True) and 3 in requested_layers

            if run_l1:
                logger.info(f"Running Layer 1 (Row-by-row) for '{table_name}'...")
                results = run_row_validator(
                    source_conn, target_conn, table_config, settings, run_id, run_timestamp
                )
                all_results.extend(results)

            if run_l2:
                logger.info(f"Running Layer 2 (Hash + drill-down) for '{table_name}'...")
                results = run_hash_validator(
                    source_conn, target_conn, table_config, settings, run_id, run_timestamp
                )
                all_results.extend(results)

            if run_l3:
                logger.info(f"Running Layer 3 (Stats) for '{table_name}'...")
                results = run_stats_validator(
                    source_conn, target_conn, table_config, settings, run_id, run_timestamp
                )
                all_results.extend(results)

    finally:
        source_conn.close()
        target_conn.close()

    logger.info(f"{'='*60}")
    logger.info(f"All validations complete. Total results: {len(all_results)}")
    logger.info(f"Generating report...")

    summary = generate_report(all_results, config, run_id, run_timestamp)

    print("\n" + "="*60)
    print(f"  ETL VALIDATION SUMMARY — {run_id}")
    print("="*60)
    print(f"  Tables Checked : {summary['total_tables_checked']}")
    print(f"  Tables Passed  : {summary['tables_passed']}")
    print(f"  Tables Failed  : {summary['tables_failed']}")
    print(f"  Total Checks   : {summary['total_checks']}")
    print(f"  Passed         : {summary['passed_checks']}")
    print(f"  Failed         : {summary['failed_checks']}")
    print(f"  Warnings       : {summary['warning_checks']}")
    print("="*60)

    if summary["failed_checks"] > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
