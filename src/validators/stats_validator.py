from src.utils import logger
from src.source_builder import build_source_query as _build_joined_source_query


# ---------------------------------------------------------------------------
# SQL dialect helpers
# ---------------------------------------------------------------------------

def _datediff_days_expr(db_type: str, epoch_str: str, col: str) -> str:
    """
    Return a SQL expression that computes integer days between epoch_str and col.
    Handles DuckDB, PostgreSQL, and Snowflake syntax differences.
    """
    if db_type == "postgres":
        return f"({col}::date - DATE '{epoch_str}')"
    if db_type == "snowflake":
        return f"DATEDIFF('day', '{epoch_str}'::DATE, {col})"
    # DuckDB (default)
    return f"DATEDIFF('day', DATE '{epoch_str}', {col})"


def _year_expr(db_type: str, col: str) -> str:
    """
    Return a SQL expression extracting the year from a date column.
    PostgreSQL requires EXTRACT(YEAR FROM col); DuckDB and Snowflake support YEAR(col).
    """
    if db_type == "postgres":
        return f"EXTRACT(YEAR FROM {col})::INTEGER"
    return f"YEAR({col})"


def _get_conn_db_type(conn) -> str:
    """
    Return the db_type string for a DBConnector instance.
    Falls back to 'duckdb' for in-memory/test connections.
    """
    return getattr(conn, "db_type", "duckdb")


def _make_result(run_id, run_timestamp, table_name, column_name, check_type,
                 source_result, target_result, source_query, target_query,
                 tolerance=None) -> dict:
    if source_result is None and target_result is None:
        status, diff, issue, severity, remarks = "PASS", "", "", "", ""
    elif source_result is None or target_result is None:
        status = "FAIL"
        diff = f"source={source_result}, target={target_result}"
        issue = "null count mismatch"
        severity = "HIGH"
        remarks = f"One side returned NULL for {check_type} on column '{column_name}'."
    else:
        try:
            from decimal import Decimal
            s = Decimal(str(source_result))
            t = Decimal(str(target_result))
            tol = Decimal(str(tolerance if tolerance is not None else 0.01))
            if abs(s - t) <= tol:
                status, diff, issue, severity, remarks = "PASS", "", "", "", ""
            else:
                status = "FAIL"
                diff = str(round(float(s - t), 10))
                issue = f"{check_type} mismatch"
                severity = "MEDIUM"
                remarks = f"{check_type} differs: source={source_result}, target={target_result}."
        except (TypeError, ValueError, Exception):
            if str(source_result) == str(target_result):
                status, diff, issue, severity, remarks = "PASS", "", "", "", ""
            else:
                status = "FAIL"
                diff = f"source={source_result}, target={target_result}"
                issue = f"{check_type} mismatch"
                severity = "MEDIUM"
                remarks = f"{check_type} differs."

    return {
        "run_id": run_id, "run_timestamp": run_timestamp,
        "table_name": table_name, "layer": 3,
        "column_name": column_name, "key_value": "",
        "check_type": check_type,
        "source_result": str(source_result) if source_result is not None else "NULL",
        "target_result": str(target_result) if target_result is not None else "NULL",
        "status": status, "difference": diff,
        "issue_type": issue, "severity": severity, "remarks": remarks,
        "source_query": source_query, "target_query": target_query,
    }


def _run_stat_query(conn, table: str, col_expr: str, data_type: str, where: str | None) -> dict:
    where_clause = f"WHERE {where}" if where else ""

    if data_type == "string":
        q = f"""
            SELECT
                COUNT(*) AS total_count,
                COUNT({col_expr}) AS non_null_count,
                SUM(CASE WHEN {col_expr} IS NULL THEN 1 ELSE 0 END) AS null_count,
                MIN(LENGTH(CAST({col_expr} AS VARCHAR))) AS min_length,
                MAX(LENGTH(CAST({col_expr} AS VARCHAR))) AS max_length,
                AVG(LENGTH(CAST({col_expr} AS VARCHAR))) AS avg_length,
                STDDEV(LENGTH(CAST({col_expr} AS VARCHAR))) AS stddev_length,
                COUNT(DISTINCT {col_expr}) AS distinct_count,
                SUM(CASE WHEN CAST({col_expr} AS VARCHAR) = '' THEN 1 ELSE 0 END) AS empty_string_count,
                SUM(CASE WHEN LEFT(CAST({col_expr} AS VARCHAR), 1) = ' ' THEN 1 ELSE 0 END) AS leading_space_count,
                SUM(CASE WHEN RIGHT(CAST({col_expr} AS VARCHAR), 1) = ' ' THEN 1 ELSE 0 END) AS trailing_space_count
            FROM {table} {where_clause}
        """
    elif data_type == "date":
        q = f"""
            SELECT
                COUNT(*) AS total_count,
                SUM(CASE WHEN {col_expr} IS NULL THEN 1 ELSE 0 END) AS null_count,
                MIN({col_expr}) AS min_date,
                MAX({col_expr}) AS max_date,
                COUNT(DISTINCT {col_expr}) AS distinct_count
            FROM {table} {where_clause}
        """
    elif data_type in ("decimal", "float", "numeric", "integer", "int", "bigint"):
        q = f"""
            SELECT
                COUNT(*) AS total_count,
                SUM(CASE WHEN {col_expr} IS NULL THEN 1 ELSE 0 END) AS null_count,
                MIN({col_expr}) AS min_val,
                MAX({col_expr}) AS max_val,
                SUM({col_expr}) AS sum_val,
                AVG({col_expr}) AS avg_val,
                STDDEV({col_expr}) AS stddev_val
            FROM {table} {where_clause}
        """
    else:
        q = f"""
            SELECT
                COUNT(*) AS total_count,
                SUM(CASE WHEN {col_expr} IS NULL THEN 1 ELSE 0 END) AS null_count,
                COUNT(DISTINCT {col_expr}) AS distinct_count
            FROM {table} {where_clause}
        """

    df = conn.fetch_df(q.strip())
    if df.empty:
        return {}
    # Normalize keys to lowercase so lookups work across all DB dialects (Snowflake returns uppercase)
    return {k.lower(): v for k, v in df.iloc[0].to_dict().items()}


def _check_truncation(run_id, run_timestamp, table_name, source_col,
                      src_stats, tgt_stats, source_query, target_query) -> dict | None:
    src_max = src_stats.get("max_length")
    tgt_max = tgt_stats.get("max_length")
    if src_max is None or tgt_max is None:
        return None
    try:
        src_max = int(src_max)
        tgt_max = int(tgt_max)
    except (TypeError, ValueError):
        return None

    if tgt_max < src_max:
        return {
            "run_id": run_id, "run_timestamp": run_timestamp,
            "table_name": table_name, "layer": 3,
            "column_name": source_col, "key_value": "",
            "check_type": "truncation_indicator",
            "source_result": str(src_max), "target_result": str(tgt_max),
            "status": "WARNING",
            "difference": str(src_max - tgt_max),
            "issue_type": "truncation detected",
            "severity": "HIGH",
            "remarks": f"Target max string length ({tgt_max}) is shorter than source ({src_max}). Possible truncation.",
            "source_query": source_query, "target_query": target_query,
        }
    return None


def _check_decimal_scale(conn_src, conn_tgt, table_src, table_tgt,
                         src_expr, tgt_expr, where_src, where_tgt,
                         run_id, run_timestamp, table_name, source_col,
                         source_query, target_query) -> dict | None:
    src_where = f"WHERE {where_src}" if where_src else ""
    tgt_where = f"WHERE {where_tgt}" if where_tgt else ""

    src_null_filter = f"{src_expr} IS NOT NULL AND {src_expr} <> FLOOR({src_expr})"
    tgt_null_filter = f"{tgt_expr} IS NOT NULL AND {tgt_expr} <> FLOOR({tgt_expr})"
    src_combined = f"{where_src} AND {src_null_filter}" if where_src else src_null_filter
    tgt_combined = f"{where_tgt} AND {tgt_null_filter}" if where_tgt else tgt_null_filter

    src_q = f"""
        SELECT MAX(
            LENGTH(CAST(ABS({src_expr} - FLOOR(ABS({src_expr}))) AS VARCHAR)) - 2
        ) AS max_decimal_scale
        FROM {table_src}
        WHERE {src_combined}
    """.strip()

    tgt_q = f"""
        SELECT MAX(
            LENGTH(CAST(ABS({tgt_expr} - FLOOR(ABS({tgt_expr}))) AS VARCHAR)) - 2
        ) AS max_decimal_scale
        FROM {table_tgt}
        WHERE {tgt_combined}
    """.strip()

    try:
        import pandas as pd
        src_df = conn_src.fetch_df(src_q)
        tgt_df = conn_tgt.fetch_df(tgt_q)
        src_scale = src_df.iloc[0].iloc[0] if not src_df.empty else None
        tgt_scale = tgt_df.iloc[0].iloc[0] if not tgt_df.empty else None
        if pd.isna(src_scale): src_scale = None
        if pd.isna(tgt_scale): tgt_scale = None
    except Exception:
        return None

    if src_scale is None and tgt_scale is None:
        return None

    src_scale = int(src_scale) if src_scale is not None else 0
    tgt_scale = int(tgt_scale) if tgt_scale is not None else 0

    status = "PASS" if src_scale == tgt_scale else "FAIL"
    return {
        "run_id": run_id, "run_timestamp": run_timestamp,
        "table_name": table_name, "layer": 3,
        "column_name": source_col, "key_value": "",
        "check_type": "decimal_scale_check",
        "source_result": str(src_scale), "target_result": str(tgt_scale),
        "status": status,
        "difference": str(src_scale - tgt_scale) if status == "FAIL" else "",
        "issue_type": "decimal scale issue" if status == "FAIL" else "",
        "severity": "MEDIUM" if status == "FAIL" else "",
        "remarks": f"Max decimal scale differs: source={src_scale}, target={tgt_scale}." if status == "FAIL" else "",
        "source_query": source_query, "target_query": target_query,
    }


def _check_date_distribution(conn_src, conn_tgt, table_src, table_tgt,
                              col_src, col_tgt, where_src, where_tgt,
                              run_id, run_timestamp, table_name, source_col,
                              source_query, target_query) -> list:
    results = []

    src_db_type = _get_conn_db_type(conn_src)
    tgt_db_type = _get_conn_db_type(conn_tgt)
    src_year = _year_expr(src_db_type, col_src)
    tgt_year = _year_expr(tgt_db_type, col_tgt)

    src_not_null = f"{col_src} IS NOT NULL"
    tgt_not_null = f"{col_tgt} IS NOT NULL"
    src_combined = f"{where_src} AND {src_not_null}" if where_src else src_not_null
    tgt_combined = f"{where_tgt} AND {tgt_not_null}" if where_tgt else tgt_not_null

    src_q = f"""
        SELECT {src_year} AS yr, COUNT(*) AS cnt
        FROM {table_src}
        WHERE {src_combined}
        GROUP BY {src_year}
        ORDER BY yr
    """.strip()

    tgt_q = f"""
        SELECT {tgt_year} AS yr, COUNT(*) AS cnt
        FROM {table_tgt}
        WHERE {tgt_combined}
        GROUP BY {tgt_year}
        ORDER BY yr
    """.strip()

    try:
        src_df = conn_src.fetch_df(src_q)
        tgt_df = conn_tgt.fetch_df(tgt_q)
    except Exception:
        return results

    src_dist = {int(row.iloc[0]): int(row.iloc[1]) for _, row in src_df.iterrows()}
    tgt_dist = {int(row.iloc[0]): int(row.iloc[1]) for _, row in tgt_df.iterrows()}
    # tgt_dist already set above via positional access

    all_years = sorted(set(src_dist.keys()) | set(tgt_dist.keys()))

    for yr in all_years:
        src_cnt = src_dist.get(yr, 0)
        tgt_cnt = tgt_dist.get(yr, 0)
        status = "PASS" if src_cnt == tgt_cnt else "FAIL"
        results.append({
            "run_id": run_id, "run_timestamp": run_timestamp,
            "table_name": table_name, "layer": 3,
            "column_name": source_col, "key_value": str(yr),
            "check_type": "date_distribution",
            "source_result": str(src_cnt), "target_result": str(tgt_cnt),
            "status": status,
            "difference": str(src_cnt - tgt_cnt) if status == "FAIL" else "",
            "issue_type": "date distribution mismatch" if status == "FAIL" else "",
            "severity": "MEDIUM" if status == "FAIL" else "",
            "remarks": f"Year {yr}: source has {src_cnt} rows, target has {tgt_cnt} rows." if status == "FAIL" else "",
            "source_query": source_query, "target_query": target_query,
        })

    return results


def _check_duplicates(conn_src, conn_tgt, table_src, table_tgt,
                      primary_keys, where_src, where_tgt,
                      run_id, run_timestamp, table_name,
                      source_query, target_query) -> list:
    results = []
    src_where = f"WHERE {where_src}" if where_src else ""
    tgt_where = f"WHERE {where_tgt}" if where_tgt else ""
    pk_list = ", ".join(primary_keys)

    src_q = f"""
        SELECT COUNT(*) AS total_rows, COUNT(DISTINCT {pk_list}) AS distinct_rows
        FROM {table_src} {src_where}
    """.strip()

    tgt_q = f"""
        SELECT COUNT(*) AS total_rows, COUNT(DISTINCT {pk_list}) AS distinct_rows
        FROM {table_tgt} {tgt_where}
    """.strip()

    try:
        src_df = conn_src.fetch_df(src_q)
        tgt_df = conn_tgt.fetch_df(tgt_q)
    except Exception:
        return results

    src_total = int(src_df.iloc[0].iloc[0])
    src_distinct = int(src_df.iloc[0].iloc[1])
    tgt_total = int(tgt_df.iloc[0].iloc[0])
    tgt_distinct = int(tgt_df.iloc[0].iloc[1])

    src_dups = src_total - src_distinct
    tgt_dups = tgt_total - tgt_distinct

    for label, total, distinct, dups, side in [
        ("source", src_total, src_distinct, src_dups, "source"),
        ("target", tgt_total, tgt_distinct, tgt_dups, "target"),
    ]:
        status = "PASS" if dups == 0 else "WARNING"
        results.append({
            "run_id": run_id, "run_timestamp": run_timestamp,
            "table_name": table_name, "layer": 3,
            "column_name": "ROW", "key_value": "",
            "check_type": f"duplicate_row_check_{side}",
            "source_result": str(total), "target_result": str(distinct),
            "status": status,
            "difference": str(dups) if dups > 0 else "",
            "issue_type": f"unexpected duplicates in {side}" if dups > 0 else "",
            "severity": "HIGH" if dups > 0 else "",
            "remarks": f"{side.capitalize()} has {dups} duplicate primary key row(s) ({total} total vs {distinct} distinct)." if dups > 0 else "",
            "source_query": source_query, "target_query": target_query,
        })

    return results


def _check_categories(conn_src, conn_tgt, table_src, table_tgt,
                      col_src, col_tgt, where_src, where_tgt,
                      run_id, run_timestamp, table_name, source_col,
                      source_query, target_query) -> list:
    results = []

    src_not_null = f"{col_src} IS NOT NULL"
    tgt_not_null = f"{col_tgt} IS NOT NULL"
    src_combined = f"{where_src} AND {src_not_null}" if where_src else src_not_null
    tgt_combined = f"{where_tgt} AND {tgt_not_null}" if where_tgt else tgt_not_null

    src_q = f"SELECT DISTINCT CAST({col_src} AS VARCHAR) AS val FROM {table_src} WHERE {src_combined}".strip()
    tgt_q = f"SELECT DISTINCT CAST({col_tgt} AS VARCHAR) AS val FROM {table_tgt} WHERE {tgt_combined}".strip()

    try:
        src_df = conn_src.fetch_df(src_q)
        tgt_df = conn_tgt.fetch_df(tgt_q)
    except Exception:
        return results

    src_cats = set(src_df.iloc[:, 0].tolist())
    tgt_cats = set(tgt_df.iloc[:, 0].tolist())

    missing_in_target = src_cats - tgt_cats
    extra_in_target = tgt_cats - src_cats

    if not missing_in_target and not extra_in_target:
        results.append({
            "run_id": run_id, "run_timestamp": run_timestamp,
            "table_name": table_name, "layer": 3,
            "column_name": source_col, "key_value": "",
            "check_type": "category_check",
            "source_result": str(sorted(src_cats)),
            "target_result": str(sorted(tgt_cats)),
            "status": "PASS", "difference": "",
            "issue_type": "", "severity": "", "remarks": "",
            "source_query": source_query, "target_query": target_query,
        })

    for val in sorted(missing_in_target):
        results.append({
            "run_id": run_id, "run_timestamp": run_timestamp,
            "table_name": table_name, "layer": 3,
            "column_name": source_col, "key_value": val,
            "check_type": "category_check",
            "source_result": val, "target_result": "missing",
            "status": "FAIL", "difference": "category missing in target",
            "issue_type": "missing category", "severity": "HIGH",
            "remarks": f"Category '{val}' exists in source column '{source_col}' but not in target.",
            "source_query": source_query, "target_query": target_query,
        })

    for val in sorted(extra_in_target):
        results.append({
            "run_id": run_id, "run_timestamp": run_timestamp,
            "table_name": table_name, "layer": 3,
            "column_name": source_col, "key_value": val,
            "check_type": "category_check",
            "source_result": "missing", "target_result": val,
            "status": "FAIL", "difference": "extra category in target",
            "issue_type": "extra category", "severity": "MEDIUM",
            "remarks": f"Category '{val}' exists in target column '{source_col}' but not in source.",
            "source_query": source_query, "target_query": target_query,
        })

    return results


def _check_date_stats_extended(conn_src, conn_tgt, table_src, table_tgt,
                                col_src, col_tgt, where_src, where_tgt,
                                settings, run_id, run_timestamp,
                                table_name, source_col,
                                source_query, target_query) -> list:
    """
    Extended date checks:
    - Date stddev using configurable epoch (no hardcoded 1970)
    - Date format issue count (values not matching any valid_date_formats)
    - Day/month swap detection (checks if swapped date is valid and different)
    - Invalid date count
    """
    results = []
    epoch_str = settings.get("date_numeric_epoch", "1900-01-01")
    valid_formats = settings.get("valid_date_formats", ["%Y-%m-%d"])

    src_db_type = _get_conn_db_type(conn_src)
    tgt_db_type = _get_conn_db_type(conn_tgt)

    src_filter = f"{where_src} AND {col_src} IS NOT NULL" if where_src else f"{col_src} IS NOT NULL"
    tgt_filter = f"{where_tgt} AND {col_tgt} IS NOT NULL" if where_tgt else f"{col_tgt} IS NOT NULL"

    # --- Date stddev using epoch from settings ---
    # Convert dates to integer days from epoch, then compute stddev
    src_datediff = _datediff_days_expr(src_db_type, epoch_str, col_src)
    tgt_datediff = _datediff_days_expr(tgt_db_type, epoch_str, col_tgt)

    src_stddev_q = f"""
        SELECT STDDEV(
            {src_datediff}
        ) AS date_stddev
        FROM {table_src}
        WHERE {src_filter}
    """.strip()

    tgt_stddev_q = f"""
        SELECT STDDEV(
            {tgt_datediff}
        ) AS date_stddev
        FROM {table_tgt}
        WHERE {tgt_filter}
    """.strip()

    try:
        import pandas as pd
        src_df = conn_src.fetch_df(src_stddev_q)
        tgt_df = conn_tgt.fetch_df(tgt_stddev_q)
        src_stddev = src_df.iloc[0].iloc[0] if not src_df.empty else None
        tgt_stddev = tgt_df.iloc[0].iloc[0] if not tgt_df.empty else None
        if pd.isna(src_stddev): src_stddev = None
        if pd.isna(tgt_stddev): tgt_stddev = None

        results.append(_make_result(
            run_id, run_timestamp, table_name, source_col,
            "date_stddev",
            src_stddev, tgt_stddev,
            source_query, target_query,
        ))
    except Exception:
        pass

    # --- Date format issue count ---
    # Fetch raw string values and check against valid_date_formats
    src_raw_q = f"SELECT CAST({col_src} AS VARCHAR) AS raw_val FROM {table_src} WHERE {src_filter}".strip()
    tgt_raw_q = f"SELECT CAST({col_tgt} AS VARCHAR) AS raw_val FROM {table_tgt} WHERE {tgt_filter}".strip()

    def count_format_issues(df, formats):
        from datetime import datetime as dt
        count = 0
        for val in df.iloc[:, 0].tolist():
            if val is None:
                continue
            matched = False
            for fmt in formats:
                try:
                    dt.strptime(str(val), fmt)
                    matched = True
                    break
                except ValueError:
                    continue
            if not matched:
                count += 1
        return count

    try:
        src_raw_df = conn_src.fetch_df(src_raw_q)
        tgt_raw_df = conn_tgt.fetch_df(tgt_raw_q)
        src_fmt_issues = count_format_issues(src_raw_df, valid_formats)
        tgt_fmt_issues = count_format_issues(tgt_raw_df, valid_formats)

        status = "PASS" if src_fmt_issues == 0 and tgt_fmt_issues == 0 else "FAIL"
        results.append({
            "run_id": run_id, "run_timestamp": run_timestamp,
            "table_name": table_name, "layer": 3,
            "column_name": source_col, "key_value": "",
            "check_type": "date_format_issue",
            "source_result": str(src_fmt_issues), "target_result": str(tgt_fmt_issues),
            "status": status,
            "difference": f"source={src_fmt_issues}, target={tgt_fmt_issues}" if status == "FAIL" else "",
            "issue_type": "date format issue" if status == "FAIL" else "",
            "severity": "HIGH" if status == "FAIL" else "",
            "remarks": f"Values not matching any of {valid_formats}. source={src_fmt_issues}, target={tgt_fmt_issues}." if status == "FAIL" else "",
            "source_query": source_query, "target_query": target_query,
        })
    except Exception:
        pass

    # --- Day/month swap detection ---
    # For each date, try swapping day and month — if result is different and valid → possible swap
    def count_day_month_swaps(df):
        from datetime import datetime as dt
        count = 0
        for val in df["raw_val"].tolist():
            if val is None:
                continue
            try:
                d = dt.strptime(str(val).split(" ")[0], "%Y-%m-%d")
                if d.day != d.month:
                    swapped = dt(d.year, d.day, d.month)
                    if swapped != d:
                        count += 1
            except (ValueError, TypeError):
                continue
        return count

    try:
        src_swaps = count_day_month_swaps(src_raw_df)
        tgt_swaps = count_day_month_swaps(tgt_raw_df)
        status = "PASS" if src_swaps == tgt_swaps else "WARNING"
        results.append({
            "run_id": run_id, "run_timestamp": run_timestamp,
            "table_name": table_name, "layer": 3,
            "column_name": source_col, "key_value": "",
            "check_type": "day_month_swap_risk",
            "source_result": str(src_swaps), "target_result": str(tgt_swaps),
            "status": status,
            "difference": str(abs(src_swaps - tgt_swaps)) if status == "WARNING" else "",
            "issue_type": "day/month swap risk" if status == "WARNING" else "",
            "severity": "MEDIUM" if status == "WARNING" else "",
            "remarks": f"Rows where day and month could be swapped differ: source={src_swaps}, target={tgt_swaps}." if status == "WARNING" else "",
            "source_query": source_query, "target_query": target_query,
        })
    except Exception:
        pass

    return results


def _check_string_extended(conn_src, conn_tgt, table_src, table_tgt,
                            col_src, col_tgt, where_src, where_tgt,
                            settings, run_id, run_timestamp,
                            table_name, source_col,
                            source_query, target_query) -> list:
    """
    Extended string checks:
    - Case mismatch count (rows where values differ only by case)
    - Missing characters count (target has fewer unique chars than source)
    """
    results = []

    src_filter = f"{where_src} AND {col_src} IS NOT NULL" if where_src else f"{col_src} IS NOT NULL"
    tgt_filter = f"{where_tgt} AND {col_tgt} IS NOT NULL" if where_tgt else f"{col_tgt} IS NOT NULL"

    src_q = f"SELECT CAST({col_src} AS VARCHAR) AS val FROM {table_src} WHERE {src_filter}".strip()
    tgt_q = f"SELECT CAST({col_tgt} AS VARCHAR) AS val FROM {table_tgt} WHERE {tgt_filter}".strip()

    try:
        src_df = conn_src.fetch_df(src_q)
        tgt_df = conn_tgt.fetch_df(tgt_q)
    except Exception:
        return results

    src_vals = src_df.iloc[:, 0].tolist()
    tgt_vals = tgt_df.iloc[:, 0].tolist()

    # Case mismatch count — values that match when uppercased but not as-is
    src_upper = set(v.upper() for v in src_vals if v)
    tgt_upper = set(v.upper() for v in tgt_vals if v)
    src_exact = set(src_vals)
    tgt_exact = set(tgt_vals)

    case_mismatches = len((src_upper & tgt_upper) - (src_exact & tgt_exact))
    status = "PASS" if case_mismatches == 0 else "WARNING"
    results.append({
        "run_id": run_id, "run_timestamp": run_timestamp,
        "table_name": table_name, "layer": 3,
        "column_name": source_col, "key_value": "",
        "check_type": "case_mismatch_count",
        "source_result": str(len(src_vals)), "target_result": str(case_mismatches),
        "status": status,
        "difference": str(case_mismatches) if case_mismatches > 0 else "",
        "issue_type": "case mismatch" if case_mismatches > 0 else "",
        "severity": "MEDIUM" if case_mismatches > 0 else "",
        "remarks": f"{case_mismatches} distinct value(s) differ only by case between source and target." if case_mismatches > 0 else "",
        "source_query": source_query, "target_query": target_query,
    })

    # Missing characters — characters present in source values but absent in target
    src_chars = set("".join(v for v in src_vals if v))
    tgt_chars = set("".join(v for v in tgt_vals if v))
    missing_chars = src_chars - tgt_chars

    status = "PASS" if not missing_chars else "WARNING"
    results.append({
        "run_id": run_id, "run_timestamp": run_timestamp,
        "table_name": table_name, "layer": 3,
        "column_name": source_col, "key_value": "",
        "check_type": "missing_characters",
        "source_result": str(sorted(src_chars)), "target_result": str(sorted(missing_chars)) if missing_chars else "none",
        "status": status,
        "difference": str(sorted(missing_chars)) if missing_chars else "",
        "issue_type": "missing characters detected" if missing_chars else "",
        "severity": "MEDIUM" if missing_chars else "",
        "remarks": f"Characters present in source but absent in target: {sorted(missing_chars)}." if missing_chars else "",
        "source_query": source_query, "target_query": target_query,
    })

    return results


def _check_currency_conversion(conn_src, conn_tgt, table_src, table_tgt,
                                src_expr, tgt_expr, where_src, where_tgt,
                                exchange_rate, currency_tolerance,
                                run_id, run_timestamp, table_name, source_col,
                                source_query, target_query) -> list:
    """
    Currency conversion check:
    - Applies exchange_rate to source values
    - Compares sum and avg of converted source vs target
    - exchange_rate and tolerance come from column config — never hardcoded
    """
    results = []

    src_filter = f"WHERE {where_src}" if where_src else ""
    tgt_filter = f"WHERE {where_tgt}" if where_tgt else ""

    src_q = f"""
        SELECT
            SUM({src_expr} * {exchange_rate}) AS converted_sum,
            AVG({src_expr} * {exchange_rate}) AS converted_avg
        FROM {table_src} {src_filter}
        WHERE {src_expr} IS NOT NULL
    """.strip()

    tgt_q = f"""
        SELECT
            SUM({tgt_expr}) AS converted_sum,
            AVG({tgt_expr}) AS converted_avg
        FROM {table_tgt} {tgt_filter}
        WHERE {tgt_expr} IS NOT NULL
    """.strip()

    # Fix double WHERE if src_filter already present
    if src_filter:
        src_q = f"""
            SELECT
                SUM({src_expr} * {exchange_rate}) AS converted_sum,
                AVG({src_expr} * {exchange_rate}) AS converted_avg
            FROM {table_src}
            WHERE {where_src} AND {src_expr} IS NOT NULL
        """.strip()

    if tgt_filter:
        tgt_q = f"""
            SELECT
                SUM({tgt_expr}) AS converted_sum,
                AVG({tgt_expr}) AS converted_avg
            FROM {table_tgt}
            WHERE {where_tgt} AND {tgt_expr} IS NOT NULL
        """.strip()

    try:
        from decimal import Decimal
        import pandas as pd
        src_df = conn_src.fetch_df(src_q)
        tgt_df = conn_tgt.fetch_df(tgt_q)

        for metric in ["converted_sum", "converted_avg"]:
            src_val = src_df.iloc[0][metric] if not src_df.empty else None
            tgt_val = tgt_df.iloc[0][metric] if not tgt_df.empty else None
            if pd.isna(src_val): src_val = None
            if pd.isna(tgt_val): tgt_val = None

            if src_val is None and tgt_val is None:
                continue

            passed = False
            if src_val is not None and tgt_val is not None:
                try:
                    s = Decimal(str(src_val))
                    t = Decimal(str(tgt_val))
                    tol = Decimal(str(currency_tolerance))
                    passed = abs(s - t) <= tol
                except Exception:
                    passed = str(src_val) == str(tgt_val)

            check_label = "currency_sum_check" if "sum" in metric else "currency_avg_check"
            results.append({
                "run_id": run_id, "run_timestamp": run_timestamp,
                "table_name": table_name, "layer": 3,
                "column_name": source_col, "key_value": "",
                "check_type": check_label,
                "source_result": str(src_val), "target_result": str(tgt_val),
                "status": "PASS" if passed else "FAIL",
                "difference": "" if passed else f"source={src_val}, target={tgt_val}",
                "issue_type": "" if passed else "currency conversion issue",
                "severity": "" if passed else "HIGH",
                "remarks": "" if passed else f"Currency conversion mismatch (rate={exchange_rate}, tolerance={currency_tolerance}).",
                "source_query": source_query, "target_query": target_query,
            })
    except Exception:
        pass

    return results


def run_stats_validator(
    source_conn,
    target_conn,
    table_config: dict,
    settings: dict,
    run_id: str,
    run_timestamp: str,
) -> list:
    results = []
    source_table = table_config["source_table"]
    source_label = table_config.get("source_label", source_table)
    target_table = table_config["target_table"]
    primary_keys = table_config["primary_key"]
    columns = table_config["columns"]
    source_where = table_config.get("source_where")
    target_where = table_config.get("target_where")
    default_tolerance = settings.get("default_tolerance", 0.01)
    sources = table_config.get("sources", [{"table": source_table}])

    # Build effective source FROM clause — single table or JOIN subquery
    if len(sources) > 1:
        join_sql = _build_joined_source_query(
            {"sources": sources, "source_where": source_where},
            select_cols="*"
        )
        src_from = f"({join_sql}) AS _src"
        # WHERE already baked into the join subquery; pass None to helpers
        effective_source_where = None
    else:
        src_from = source_table
        effective_source_where = source_where

    # Build source row count query
    if len(sources) > 1:
        src_count_query = _build_joined_source_query(
            {"sources": sources, "source_where": source_where},
            select_cols="COUNT(*) AS row_count"
        )
    else:
        src_where_clause = f"WHERE {source_where}" if source_where else ""
        src_count_query = f"SELECT COUNT(*) AS row_count FROM {source_table} {src_where_clause}".strip()

    tgt_where_clause = f"WHERE {target_where}" if target_where else ""
    tgt_count_query = f"SELECT COUNT(*) AS row_count FROM {target_table} {tgt_where_clause}".strip()

    src_count = source_conn.fetch_df(src_count_query).iloc[0].iloc[0]
    tgt_count = target_conn.fetch_df(tgt_count_query).iloc[0].iloc[0]

    row_count_result = _make_result(
        run_id, run_timestamp, source_table,
        "ROW", "row_count",
        src_count, tgt_count,
        src_count_query, tgt_count_query,
        tolerance=0,
    )
    if src_count != tgt_count:
        row_count_result["issue_type"] = "row count mismatch"
        row_count_result["severity"] = "HIGH"
    results.append(row_count_result)
    logger.info(f"[Layer 3] Row count — source: {src_count}, target: {tgt_count}.")

    # Duplicate row check (table level)
    dup_results = _check_duplicates(
        source_conn, target_conn,
        src_from, target_table,
        primary_keys, effective_source_where, target_where,
        run_id, run_timestamp, source_table,
        src_count_query, tgt_count_query,
    )
    results.extend(dup_results)
    logger.info(f"[Layer 3] Duplicate row check complete.")

    for col_config in columns:
        source_col = col_config["source_col"]
        target_col = col_config["target_col"]
        data_type = col_config.get("data_type", "string")
        tolerance = col_config.get("tolerance", default_tolerance)
        checks = col_config.get("checks", [])
        src_expr = col_config.get("source_expression", source_col)
        tgt_expr = col_config.get("target_expression", target_col)

        logger.info(f"[Layer 3] Computing stats for column '{source_col}' -> '{target_col}' (type={data_type})...")

        src_stats = _run_stat_query(source_conn, src_from, src_expr, data_type, effective_source_where)
        src_where_clause2 = f"WHERE {source_where}" if source_where else ""
        src_stat_query = f"SELECT stats({src_expr}) FROM {src_from} {src_where_clause2}".strip()

        tgt_stats = _run_stat_query(target_conn, target_table, tgt_expr, data_type, target_where)
        tgt_where_clause_stat = f"WHERE {target_where}" if target_where else ""
        tgt_stat_query = f"SELECT stats({tgt_expr}) FROM {target_table} {tgt_where_clause_stat}".strip()

        def add(check_key, src_key, tgt_key, tol=None):
            if check_key not in checks:
                return
            results.append(_make_result(
                run_id, run_timestamp, source_table,
                target_col, check_key,
                src_stats.get(src_key), tgt_stats.get(tgt_key),
                src_stat_query, tgt_stat_query,
                tolerance=tol if tol is not None else tolerance,
            ))

        if data_type == "string":
            add("null_count", "null_count", "null_count", tol=0)
            add("min_length", "min_length", "min_length", tol=0)
            add("max_length", "max_length", "max_length", tol=0)
            add("avg_length", "avg_length", "avg_length")
            add("stddev_length", "stddev_length", "stddev_length")
            add("distinct_count", "distinct_count", "distinct_count", tol=0)
            add("empty_string_count", "empty_string_count", "empty_string_count", tol=0)
            add("leading_space_count", "leading_space_count", "leading_space_count", tol=0)
            add("trailing_space_count", "trailing_space_count", "trailing_space_count", tol=0)

            # Truncation indicator
            if "truncation_indicator" in checks:
                trunc = _check_truncation(
                    run_id, run_timestamp, source_table, source_col,
                    src_stats, tgt_stats, src_count_query, tgt_count_query,
                )
                if trunc:
                    results.append(trunc)
                else:
                    results.append(_make_result(
                        run_id, run_timestamp, source_table, source_col,
                        "truncation_indicator",
                        src_stats.get("max_length"), tgt_stats.get("max_length"),
                        src_count_query, tgt_count_query, tolerance=0,
                    ))

            # Category check
            if "category_check" in checks:
                cat_results = _check_categories(
                    source_conn, target_conn,
                    src_from, target_table,
                    src_expr, tgt_expr,
                    effective_source_where, target_where,
                    run_id, run_timestamp, source_table, source_col,
                    src_count_query, tgt_count_query,
                )
                results.extend(cat_results)

            # Extended string checks: case mismatch count + missing characters
            if "case_mismatch_count" in checks or "missing_characters" in checks:
                str_ext = _check_string_extended(
                    source_conn, target_conn,
                    src_from, target_table,
                    src_expr, tgt_expr,
                    effective_source_where, target_where,
                    settings, run_id, run_timestamp, source_table, source_col,
                    src_count_query, tgt_count_query,
                )
                for r in str_ext:
                    if r["check_type"] in checks:
                        results.append(r)

        elif data_type == "date":
            add("null_count", "null_count", "null_count", tol=0)
            add("min", "min_date", "min_date", tol=0)
            add("max", "max_date", "max_date", tol=0)
            add("distinct_count", "distinct_count", "distinct_count", tol=0)

            # Date distribution check
            if "date_distribution" in checks:
                dist_results = _check_date_distribution(
                    source_conn, target_conn,
                    src_from, target_table,
                    src_expr, tgt_expr,
                    effective_source_where, target_where,
                    run_id, run_timestamp, source_table, source_col,
                    src_count_query, tgt_count_query,
                )
                results.extend(dist_results)

            # Extended date checks: stddev, format issues, day/month swap
            date_ext_checks = {"date_stddev", "date_format_issue", "day_month_swap_risk"}
            if date_ext_checks & set(checks):
                date_ext = _check_date_stats_extended(
                    source_conn, target_conn,
                    src_from, target_table,
                    src_expr, tgt_expr,
                    effective_source_where, target_where,
                    settings, run_id, run_timestamp, source_table, source_col,
                    src_count_query, tgt_count_query,
                )
                for r in date_ext:
                    if r["check_type"] in checks:
                        results.append(r)

        elif data_type in ("decimal", "float", "numeric", "integer", "int", "bigint"):
            add("null_count", "null_count", "null_count", tol=0)
            add("min", "min_val", "min_val")
            add("max", "max_val", "max_val")
            add("sum", "sum_val", "sum_val")
            add("avg", "avg_val", "avg_val")
            add("stddev", "stddev_val", "stddev_val")

            # Decimal scale check
            if "decimal_scale_check" in checks:
                scale_result = _check_decimal_scale(
                    source_conn, target_conn,
                    src_from, target_table,
                    src_expr, tgt_expr,
                    effective_source_where, target_where,
                    run_id, run_timestamp, source_table, source_col,
                    src_count_query, tgt_count_query,
                )
                if scale_result:
                    results.append(scale_result)

            # Currency conversion check
            if "currency_conversion" in checks:
                exchange_rate = col_config.get("exchange_rate")
                currency_tolerance = col_config.get(
                    "currency_tolerance",
                    settings.get("default_currency_tolerance", 0.05)
                )
                if exchange_rate is not None:
                    curr_results = _check_currency_conversion(
                        source_conn, target_conn,
                        src_from, target_table,
                        src_expr, tgt_expr,
                        effective_source_where, target_where,
                        exchange_rate, currency_tolerance,
                        run_id, run_timestamp, source_table, source_col,
                        src_count_query, tgt_count_query,
                    )
                    results.extend(curr_results)
                else:
                    logger.warning(f"[Layer 3] 'currency_conversion' check requested for column '{source_col}' but 'exchange_rate' is not set in config. Skipping.")

    logger.info(f"[Layer 3] Completed. {len(results)} result(s) recorded.")
    return results
