from src.filter_engine import build_source_select_expressions, build_target_select_expressions
from src.utils import compute_row_hash, detect_string_issues, values_within_tolerance, logger


def _drill_down_row(src_row: dict, tgt_row: dict, columns: list, key_str: str,
                    settings: dict, run_id: str, run_timestamp: str,
                    source_table: str, source_query: str, target_query: str) -> list:
    results = []
    for col_config in columns:
        source_col = col_config["source_col"]
        target_col = col_config["target_col"]
        data_type = col_config.get("data_type", "string")
        tolerance = col_config.get("tolerance", settings.get("default_tolerance", 0.01))

        # Both src_row and tgt_row use lowercase target column names (aliased in source query)
        src_val = src_row.get(target_col.lower())
        tgt_val = tgt_row.get(target_col.lower())

        passed = False
        issue = ""

        if src_val is None and tgt_val is None:
            passed = True
        elif src_val is None or tgt_val is None:
            issue = "null mismatch"
        elif data_type in ("decimal", "float", "numeric"):
            if values_within_tolerance(src_val, tgt_val, tolerance):
                passed = True
            else:
                issue = "decimal precision issue"
        elif data_type == "date":
            passed = str(src_val) == str(tgt_val)
            if not passed:
                issue = "date mismatch"
        elif data_type == "string":
            issues = detect_string_issues(str(src_val), str(tgt_val))
            passed = not issues
            issue = issues[0] if issues else ""
        else:
            passed = str(src_val) == str(tgt_val)
            if not passed:
                issue = "value mismatch"

        results.append({
            "run_id": run_id, "run_timestamp": run_timestamp,
            "table_name": source_table, "layer": 2,
            "column_name": target_col, "key_value": key_str,
            "check_type": "cell_drill_down",
            "source_result": str(src_val) if src_val is not None else "NULL",
            "target_result": str(tgt_val) if tgt_val is not None else "NULL",
            "status": "PASS" if passed else "FAIL",
            "difference": "" if passed else f"source={src_val}, target={tgt_val}",
            "issue_type": "" if passed else issue,
            "severity": "" if passed else "HIGH",
            "remarks": "" if passed else f"Column '{target_col}' mismatch detected via hash drill-down for key {key_str}.",
            "source_query": source_query, "target_query": target_query,
        })

    return results


def run_hash_validator(
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

    # Build column mapping: source_col -> target_col
    col_pk_map = {col["source_col"]: col["target_col"] for col in columns}
    tgt_primary_keys = [col_pk_map.get(pk, pk) for pk in primary_keys]
    pk_target_names_lower = {col_pk_map.get(pk, pk).lower() for pk in primary_keys}

    # Source query: alias source columns to target column names so both DFs align
    src_exprs = build_source_select_expressions(table_config)
    src_pk_aliases = [
        f"{pk} AS {col_pk_map.get(pk, pk)}" if pk != col_pk_map.get(pk, pk) else pk
        for pk in primary_keys
    ]
    src_pk_list = ", ".join(src_pk_aliases)
    # Exclude PK columns to avoid duplicate columns in SELECT
    src_exprs_no_pk = [
        expr for expr, col in zip(src_exprs, columns)
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
    tgt_exprs_no_pk = [
        expr for expr, col in zip(tgt_exprs, columns)
        if col["target_col"].lower() not in pk_target_names_lower
    ]
    tgt_col_list = ", ".join(tgt_exprs_no_pk) if tgt_exprs_no_pk else ""
    tgt_where_clause = f"WHERE {target_where}" if target_where else ""
    if tgt_col_list:
        target_query = f"SELECT {tgt_pk_list}, {tgt_col_list} FROM {target_table} {tgt_where_clause}".strip()
    else:
        target_query = f"SELECT {tgt_pk_list} FROM {target_table} {tgt_where_clause}".strip()

    logger.info(f"[Layer 2] Fetching source data from '{source_table}'...")
    source_df = source_conn.fetch_df(source_query)
    logger.info(f"[Layer 2] Fetching target data from '{target_table}'...")
    target_df = target_conn.fetch_df(target_query)

    # Normalize column names to lowercase — Snowflake returns uppercase column names
    source_df.columns = [c.lower() for c in source_df.columns]
    target_df.columns = [c.lower() for c in target_df.columns]
    tgt_primary_keys_norm = [pk.lower() for pk in tgt_primary_keys]

    # Both DataFrames now use lowercase target column names
    src_col_order = [col["target_col"].lower() for col in columns]
    tgt_col_order = [col["target_col"].lower() for col in columns]

    # --- Duplicate key check ---
    src_dup_mask = source_df.duplicated(subset=tgt_primary_keys_norm, keep=False)
    tgt_dup_mask = target_df.duplicated(subset=tgt_primary_keys_norm, keep=False)

    src_duplicates = source_df[src_dup_mask][tgt_primary_keys_norm].drop_duplicates()
    tgt_duplicates = target_df[tgt_dup_mask][tgt_primary_keys_norm].drop_duplicates()

    for _, row in src_duplicates.iterrows():
        key_str = "|".join(str(row[pk]) for pk in tgt_primary_keys_norm)
        count = int(src_dup_mask[source_df[tgt_primary_keys_norm].eq(row[tgt_primary_keys_norm]).all(axis=1)].sum())
        results.append({
            "run_id": run_id, "run_timestamp": run_timestamp,
            "table_name": source_table, "layer": 2, "column_name": "ROW",
            "key_value": key_str, "check_type": "duplicate_key_check",
            "source_result": f"{count} duplicate rows",
            "target_result": "",
            "status": "WARNING", "difference": f"{count} rows share the same key",
            "issue_type": "duplicate key in source", "severity": "HIGH",
            "remarks": f"Primary key {key_str} appears {count} times in source '{source_table}'. Hash comparison for this key is unreliable.",
            "source_query": source_query, "target_query": target_query,
        })

    for _, row in tgt_duplicates.iterrows():
        key_str = "|".join(str(row[pk]) for pk in tgt_primary_keys_norm)
        count = int(tgt_dup_mask[target_df[tgt_primary_keys_norm].eq(row[tgt_primary_keys_norm]).all(axis=1)].sum())
        results.append({
            "run_id": run_id, "run_timestamp": run_timestamp,
            "table_name": source_table, "layer": 2, "column_name": "ROW",
            "key_value": key_str, "check_type": "duplicate_key_check",
            "source_result": "",
            "target_result": f"{count} duplicate rows",
            "status": "WARNING", "difference": f"{count} rows share the same key",
            "issue_type": "duplicate key in target", "severity": "HIGH",
            "remarks": f"Primary key {key_str} appears {count} times in target '{target_table}'. Hash comparison for this key is unreliable.",
            "source_query": source_query, "target_query": target_query,
        })

    # Collect duplicate keys to exclude from hash comparison
    src_dup_keys = set(tuple(row) for _, row in src_duplicates.iterrows()) if not src_duplicates.empty else set()
    tgt_dup_keys = set(tuple(row) for _, row in tgt_duplicates.iterrows()) if not tgt_duplicates.empty else set()
    all_dup_keys = src_dup_keys | tgt_dup_keys

    if src_duplicates.shape[0] > 0 or tgt_duplicates.shape[0] > 0:
        logger.warning(f"[Layer 2] Duplicate keys found — source: {src_duplicates.shape[0]}, target: {tgt_duplicates.shape[0]}. These keys will be skipped from hash comparison.")

    # Both DataFrames use tgt_primary_keys_norm as index
    source_indexed = source_df.set_index(tgt_primary_keys_norm)
    target_indexed = target_df.set_index(tgt_primary_keys_norm)

    source_keys = set(source_indexed.index.tolist()) if len(tgt_primary_keys_norm) == 1 else set(map(tuple, source_indexed.index.tolist()))
    target_keys = set(target_indexed.index.tolist()) if len(tgt_primary_keys_norm) == 1 else set(map(tuple, target_indexed.index.tolist()))

    # Exclude duplicate keys from further comparison
    if all_dup_keys:
        if len(tgt_primary_keys_norm) == 1:
            single_dup_keys = set(k[0] for k in all_dup_keys)
            source_keys -= single_dup_keys
            target_keys -= single_dup_keys
        else:
            source_keys -= all_dup_keys
            target_keys -= all_dup_keys

    missing_keys = source_keys - target_keys
    extra_keys = target_keys - source_keys
    common_keys = source_keys & target_keys

    for key in missing_keys:
        key_str = str(key) if len(tgt_primary_keys_norm) == 1 else "|".join(str(k) for k in key)
        results.append({
            "run_id": run_id, "run_timestamp": run_timestamp,
            "table_name": source_table, "layer": 2, "column_name": "ROW",
            "key_value": key_str, "check_type": "row_existence",
            "source_result": "exists", "target_result": "missing",
            "status": "FAIL", "difference": "row missing in target",
            "issue_type": "missing rows", "severity": "HIGH",
            "remarks": f"Row with key {key_str} exists in source but not in target.",
            "source_query": source_query, "target_query": target_query,
        })

    for key in extra_keys:
        key_str = str(key) if len(tgt_primary_keys_norm) == 1 else "|".join(str(k) for k in key)
        results.append({
            "run_id": run_id, "run_timestamp": run_timestamp,
            "table_name": source_table, "layer": 2, "column_name": "ROW",
            "key_value": key_str, "check_type": "row_existence",
            "source_result": "missing", "target_result": "exists",
            "status": "FAIL", "difference": "extra row in target",
            "issue_type": "extra rows", "severity": "HIGH",
            "remarks": f"Row with key {key_str} exists in target but not in source.",
            "source_query": source_query, "target_query": target_query,
        })

    hash_pass = 0
    hash_fail = 0

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

        src_hash = compute_row_hash(src_row, src_col_order, settings)
        tgt_hash = compute_row_hash(tgt_row, tgt_col_order, settings)

        if src_hash == tgt_hash:
            hash_pass += 1
            results.append({
                "run_id": run_id, "run_timestamp": run_timestamp,
                "table_name": source_table, "layer": 2, "column_name": "ROW",
                "key_value": key_str, "check_type": "hash_match",
                "source_result": src_hash, "target_result": tgt_hash,
                "status": "PASS", "difference": "", "issue_type": "",
                "severity": "", "remarks": "",
                "source_query": source_query, "target_query": target_query,
            })
        else:
            hash_fail += 1
            results.append({
                "run_id": run_id, "run_timestamp": run_timestamp,
                "table_name": source_table, "layer": 2, "column_name": "ROW",
                "key_value": key_str, "check_type": "hash_match",
                "source_result": src_hash, "target_result": tgt_hash,
                "status": "FAIL", "difference": "hash mismatch",
                "issue_type": "hash mismatch", "severity": "HIGH",
                "remarks": f"Row hash mismatch for key {key_str}. Drilling down to cell level.",
                "source_query": source_query, "target_query": target_query,
            })
            drill_results = _drill_down_row(
                src_row, tgt_row, columns, key_str,
                settings, run_id, run_timestamp,
                source_table, source_query, target_query,
            )
            results.extend(drill_results)

    logger.info(f"[Layer 2] Hash check complete. Passed: {hash_pass}, Failed: {hash_fail}, Missing: {len(missing_keys)}, Extra: {len(extra_keys)}.")
    return results
