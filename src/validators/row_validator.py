from src.filter_engine import build_source_select_expressions, build_target_select_expressions
from src.utils import detect_string_issues, values_within_tolerance, logger


def _compare_cell(source_val, target_val, col_config: dict, settings: dict) -> tuple:
    data_type = col_config.get("data_type", "string")
    tolerance = col_config.get("tolerance", settings.get("default_tolerance", 0.01))

    if source_val is None and target_val is None:
        return True, ""
    if source_val is None and target_val is not None:
        return False, "null mismatch"
    if source_val is not None and target_val is None:
        return False, "null mismatch"

    if data_type in ("decimal", "float", "numeric"):
        if values_within_tolerance(source_val, target_val, tolerance):
            return True, ""
        return False, "decimal precision issue"

    if data_type in ("integer", "int", "bigint"):
        if str(source_val) == str(target_val):
            return True, ""
        return False, "value mismatch"

    if data_type == "date":
        if str(source_val) == str(target_val):
            return True, ""
        return False, "date mismatch"

    if data_type == "string":
        issues = detect_string_issues(str(source_val), str(target_val))
        if not issues:
            return True, ""
        return False, issues[0]

    if str(source_val) == str(target_val):
        return True, ""
    return False, "value mismatch"


def run_row_validator(
    source_conn,
    target_conn,
    table_config: dict,
    settings: dict,
    run_id: str,
    run_timestamp: str,
) -> list:
    results = []
    source_table = table_config["source_table"]
    target_table = table_config["target_table"]
    primary_keys = table_config["primary_key"]
    columns = table_config["columns"]
    source_where = table_config.get("source_where")
    target_where = table_config.get("target_where")
    batch_size = settings.get("batch_size", 10000)

    # Build column mapping: source_col -> target_col
    col_pk_map = {col["source_col"]: col["target_col"] for col in columns}
    # Primary keys expressed as target column names (used to index both DataFrames)
    tgt_primary_keys = [col_pk_map.get(pk, pk) for pk in primary_keys]

    # Source query: alias source columns to target column names so both DFs align
    src_exprs = build_source_select_expressions(table_config)
    src_pk_aliases = [
        f"{pk} AS {col_pk_map.get(pk, pk)}" if pk != col_pk_map.get(pk, pk) else pk
        for pk in primary_keys
    ]
    src_pk_list = ", ".join(src_pk_aliases)

    # Exclude PK columns from src_col_list to avoid duplicate columns in SELECT
    pk_target_names_lower = {col_pk_map.get(pk, pk).lower() for pk in primary_keys}
    src_exprs_no_pk = [
        expr for expr, col in zip(src_exprs, table_config["columns"])
        if col["target_col"].lower() not in pk_target_names_lower
    ]
    src_col_list = ", ".join(src_exprs_no_pk) if src_exprs_no_pk else ""
    src_where_clause = f"WHERE {source_where}" if source_where else ""
    if src_col_list:
        source_query = f"SELECT {src_pk_list}, {src_col_list} FROM {source_table} {src_where_clause}".strip()
    else:
        source_query = f"SELECT {src_pk_list} FROM {source_table} {src_where_clause}".strip()

    # Target query: use target column names directly
    tgt_exprs = build_target_select_expressions(table_config)
    tgt_pk_list = ", ".join(tgt_primary_keys)
    # Exclude PK columns from tgt_col_list too
    tgt_exprs_no_pk = [
        expr for expr, col in zip(tgt_exprs, table_config["columns"])
        if col["target_col"].lower() not in pk_target_names_lower
    ]
    tgt_col_list = ", ".join(tgt_exprs_no_pk) if tgt_exprs_no_pk else ""
    tgt_where_clause = f"WHERE {target_where}" if target_where else ""
    if tgt_col_list:
        target_query = f"SELECT {tgt_pk_list}, {tgt_col_list} FROM {target_table} {tgt_where_clause}".strip()
    else:
        target_query = f"SELECT {tgt_pk_list} FROM {target_table} {tgt_where_clause}".strip()

    logger.info(f"[Layer 1] Fetching source data from '{source_table}' in batches of {batch_size}...")
    source_df = source_conn.fetch_df(source_query)
    logger.info(f"[Layer 1] Fetching target data from '{target_table}'...")
    target_df = target_conn.fetch_df(target_query)

    # Normalize column names to lowercase — Snowflake returns uppercase column names
    source_df.columns = [c.lower() for c in source_df.columns]
    target_df.columns = [c.lower() for c in target_df.columns]
    tgt_primary_keys_norm = [pk.lower() for pk in tgt_primary_keys]

    # Both DataFrames now use target column names — index by target PK names
    source_indexed = source_df.set_index(tgt_primary_keys_norm)
    target_indexed = target_df.set_index(tgt_primary_keys_norm)

    source_keys = set(source_indexed.index.tolist()) if len(tgt_primary_keys_norm) == 1 else set(map(tuple, source_indexed.index.tolist()))
    target_keys = set(target_indexed.index.tolist()) if len(tgt_primary_keys_norm) == 1 else set(map(tuple, target_indexed.index.tolist()))

    missing_keys = source_keys - target_keys
    extra_keys = target_keys - source_keys

    for key in missing_keys:
        key_str = str(key) if len(primary_keys) == 1 else "|".join(str(k) for k in key)
        results.append({
            "run_id": run_id, "run_timestamp": run_timestamp,
            "table_name": source_table, "layer": 1, "column_name": "ROW",
            "key_value": key_str, "check_type": "row_existence",
            "source_result": "exists", "target_result": "missing",
            "status": "FAIL", "difference": "row missing in target",
            "issue_type": "missing rows", "severity": "HIGH",
            "remarks": f"Row with key {key_str} exists in source but not in target.",
            "source_query": source_query, "target_query": target_query,
        })

    for key in extra_keys:
        key_str = str(key) if len(primary_keys) == 1 else "|".join(str(k) for k in key)
        results.append({
            "run_id": run_id, "run_timestamp": run_timestamp,
            "table_name": source_table, "layer": 1, "column_name": "ROW",
            "key_value": key_str, "check_type": "row_existence",
            "source_result": "missing", "target_result": "exists",
            "status": "FAIL", "difference": "extra row in target",
            "issue_type": "extra rows", "severity": "HIGH",
            "remarks": f"Row with key {key_str} exists in target but not in source.",
            "source_query": source_query, "target_query": target_query,
        })

    common_keys = source_keys & target_keys

    for key in common_keys:
        src_row = source_indexed.loc[key].to_dict() if len(tgt_primary_keys_norm) == 1 else source_indexed.loc[[key]].iloc[0].to_dict()
        tgt_row = target_indexed.loc[key].to_dict() if len(tgt_primary_keys_norm) == 1 else target_indexed.loc[[key]].iloc[0].to_dict()
        key_str = str(key) if len(tgt_primary_keys_norm) == 1 else "|".join(str(k) for k in key)

        # Add primary key values back into row dicts (removed by set_index)
        if len(tgt_primary_keys_norm) == 1:
            src_row[tgt_primary_keys_norm[0]] = key
            tgt_row[tgt_primary_keys_norm[0]] = key
        else:
            for pk, kv in zip(tgt_primary_keys_norm, key):
                src_row[pk] = kv
                tgt_row[pk] = kv

        for col_config in columns:
            target_col = col_config["target_col"]
            target_col_lower = target_col.lower()
            checks = col_config.get("checks", [])

            if "exact_match" not in checks:
                continue

            # Both DataFrames use lowercase column names now
            src_val = src_row.get(target_col_lower)
            tgt_val = tgt_row.get(target_col_lower)
            passed, issue = _compare_cell(src_val, tgt_val, col_config, settings)

            results.append({
                "run_id": run_id, "run_timestamp": run_timestamp,
                "table_name": source_table, "layer": 1,
                "column_name": target_col, "key_value": key_str,
                "check_type": "exact_match",
                "source_result": str(src_val) if src_val is not None else "NULL",
                "target_result": str(tgt_val) if tgt_val is not None else "NULL",
                "status": "PASS" if passed else "FAIL",
                "difference": "" if passed else f"source={src_val}, target={tgt_val}",
                "issue_type": "" if passed else issue,
                "severity": "" if passed else "HIGH",
                "remarks": "" if passed else f"Column '{target_col}' mismatch for key {key_str}.",
                "source_query": source_query, "target_query": target_query,
            })

    logger.info(f"[Layer 1] Completed. {len(results)} result(s) recorded.")
    return results
