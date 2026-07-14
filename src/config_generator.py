import os
import yaml
from src.metadata_discovery import map_to_standard_type
from src.utils import logger


class _NoAliasDumper(yaml.Dumper):
    def ignore_aliases(self, data):
        return True


def _strip_meta(obj):
    if isinstance(obj, dict):
        return {k: _strip_meta(v) for k, v in obj.items() if not k.startswith("_")}
    if isinstance(obj, list):
        return [_strip_meta(i) for i in obj]
    return obj


DEFAULT_CHECKS_BY_TYPE = {
    "string":  ["null_count", "distinct_count", "min_length", "max_length", "avg_length", "exact_match", "hash"],
    "date":    ["null_count", "min", "max", "exact_match", "hash"],
    "decimal": ["null_count", "min", "max", "sum", "avg", "exact_match", "hash"],
    "integer": ["null_count", "min", "max", "sum", "exact_match", "hash"],
}


def _checks_for_type(data_type: str) -> list:
    return DEFAULT_CHECKS_BY_TYPE.get(data_type, ["null_count", "exact_match", "hash"])


def generate_config(
    source_db_config: dict,
    target_db_config: dict,
    table_matches: list,
    source_metadata: dict,
    target_metadata: dict,
    key_suggestions: dict,
    column_matches: dict,
    output_path: str = "output/generated_validation_config.yaml",
) -> str:

    config = {
        "source": source_db_config,
        "target": target_db_config,
        "settings": {
            "batch_size": 10000,
            "default_tolerance": 0.01,
            "null_token": "__NULL__",
            "date_format": "%Y-%m-%d",
            "decimal_places": 10,
            "hash_algorithm": "md5",
            "normalize_strings": False,
            "date_numeric_epoch": "1900-01-01",
            "valid_date_formats": ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y%m%d"],
            "default_currency_tolerance": 0.05,
        },
        "tables": [],
        "report": {
            "output_format": ["html"],
            "output_path": "./output/",
        },
    }

    for match in table_matches:
        src_table = match["source_table"]
        tgt_table = match["target_table"]
        confidence = match["confidence"]
        status = match["status"]

        if tgt_table is None:
            logger.warning(f"Skipping '{src_table}' — no target table match found.")
            continue

        key_info = key_suggestions.get(src_table, {})
        suggested_key = key_info.get("suggested_key", ["NEEDS_REVIEW_primary_key"])
        key_confidence = key_info.get("confidence", "low")

        col_mappings = column_matches.get(src_table, [])

        # If user explicitly defined compare_columns in mapping, use those directly
        user_columns = match.get("compare_columns")
        if user_columns:
            columns = user_columns
        else:
            columns = []
            for cm in col_mappings:
                if cm["target_col"] is None:
                    continue
                data_type = map_to_standard_type(cm["source_type"] or "VARCHAR")
                col_entry = {
                    "source_col": cm["source_col"],
                    "target_col": cm["target_col"],
                    "data_type": data_type,
                    "checks": _checks_for_type(data_type),
                    "_confidence": cm["confidence"],
                    "_match_reason": cm["match_reason"],
                    "_status": cm["status"],
                }
                columns.append(col_entry)

        if not columns:
            logger.warning(f"No column mappings for '{src_table}' → '{tgt_table}'. Skipping.")
            continue

        src_row_count = source_metadata.get(src_table, {}).get("row_count", -1)
        tgt_row_count = target_metadata.get(tgt_table, {}).get("row_count", -1)

        table_entry = {
            "_discovery_confidence": confidence,
            "_discovery_status": status,
            "_discovery_reason": match.get("match_reason", ""),
            "_source_row_count": src_row_count,
            "_target_row_count": tgt_row_count,
            "_key_confidence": key_confidence,
            "name": match.get("name", src_table),
            "source_table": src_table,
            "source_label": match.get("source_label", src_table),
            "sources": match.get("sources", [{"table": src_table}]),
            "target_table": tgt_table,
            "primary_key": suggested_key,
            "source_where": match.get("source_where", None),
            "target_where": match.get("target_where", None),
            "columns": columns,
            "validation_layers": {
                "run_layer1": False,
                "run_layer2": confidence in ("high", "medium") and not match.get("is_multi_source", False),
                "run_layer3": True,
            },
        }
        config["tables"].append(table_entry)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    clean_config = _strip_meta(config)

    with open(output_path, "w") as f:
        yaml.dump(clean_config, f, Dumper=_NoAliasDumper, default_flow_style=False, sort_keys=False, allow_unicode=True)

    logger.info(f"Generated config written to: {output_path}")
    return output_path
