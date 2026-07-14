from src.utils import logger


KEY_SUFFIXES = ["id", "key", "number", "code", "num", "no"]
KEY_PREFIXES = ["id", "key"]


def _is_candidate_by_name(col_name: str) -> bool:
    lower = col_name.lower()
    for suffix in KEY_SUFFIXES:
        if lower.endswith(suffix):
            return True
    for prefix in KEY_PREFIXES:
        if lower.startswith(prefix):
            return True
    return False


def _test_uniqueness(conn, table: str, columns: list) -> bool:
    try:
        if len(columns) == 1:
            col_expr = columns[0]
        else:
            col_expr = "CONCAT(" + ", '|', ".join(f"CAST({c} AS VARCHAR)" for c in columns) + ")"
        df = conn.fetch_df(
            f"SELECT COUNT(*) AS total, COUNT(DISTINCT {col_expr}) AS distinct_count "
            f"FROM {table}"
        )
        total = int(df.iloc[0]["total"])
        distinct = int(df.iloc[0]["distinct_count"])
        return total > 0 and total == distinct
    except Exception as e:
        logger.warning(f"Uniqueness test failed for {table}.{columns}: {e}")
        return False


def detect_candidate_keys(conn, table: str, columns: list) -> dict:
    candidates = []

    # Single column candidates by name pattern
    name_candidates = [
        col["column_name"] for col in columns
        if _is_candidate_by_name(col["column_name"])
        and col["column_name"].upper() not in ("UPDATEDBY", "CREATEDBY", "MODIFIEDBY")
    ]

    for col_name in name_candidates:
        is_unique = _test_uniqueness(conn, table, [col_name])
        candidates.append({
            "columns": [col_name],
            "unique": is_unique,
            "confidence": "high" if is_unique else "low",
        })

    # Composite key: first two ID-like columns
    if len(name_candidates) >= 2:
        pair = name_candidates[:2]
        is_unique = _test_uniqueness(conn, table, pair)
        if is_unique:
            candidates.append({
                "columns": pair,
                "unique": True,
                "confidence": "medium",
            })

    # Pick best candidate — prefer unique single column
    best = None
    for c in candidates:
        if c["unique"] and len(c["columns"]) == 1:
            best = c
            break
    if not best:
        for c in candidates:
            if c["unique"]:
                best = c
                break
    if not best and candidates:
        best = candidates[0]

    return {
        "suggested_key": best["columns"] if best else [columns[0]["column_name"]] if columns else ["id"],
        "confidence": best["confidence"] if best else "low",
        "all_candidates": candidates,
    }
