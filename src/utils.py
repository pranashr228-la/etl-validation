import hashlib
import logging
from datetime import date, datetime
from decimal import Decimal


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("etl_validator")


logger = setup_logging()


def format_value_for_hash(value, settings: dict) -> str:
    import pandas as pd
    null_token = settings.get("null_token", "__NULL__")
    date_format = settings.get("date_format", "%Y-%m-%d")
    decimal_places = settings.get("decimal_places", 10)
    normalize = settings.get("normalize_strings", False)

    if value is None:
        return null_token

    # Handle pandas NaT and NA
    try:
        if pd.isna(value):
            return null_token
    except (TypeError, ValueError):
        pass

    if isinstance(value, bool):
        return str(value).upper()

    if isinstance(value, datetime):
        return value.strftime(date_format + " %H:%M:%S")

    if isinstance(value, date):
        return value.strftime(date_format)

    if isinstance(value, (float, Decimal)):
        return f"{float(value):.{decimal_places}f}"

    if isinstance(value, int):
        return str(value)

    if isinstance(value, str):
        if normalize:
            return value.strip().upper()
        return value

    return str(value)


def compute_row_hash(row: dict, col_order: list, settings: dict) -> str:
    parts = []
    for col in col_order:
        val = row.get(col)
        parts.append(format_value_for_hash(val, settings))
    combined = "|".join(parts)

    algorithm = settings.get("hash_algorithm", "md5")
    if algorithm == "sha256":
        return hashlib.sha256(combined.encode("utf-8")).hexdigest()
    return hashlib.md5(combined.encode("utf-8")).hexdigest()


def date_to_numeric(d, epoch_str: str = "1900-01-01") -> int | None:
    if d is None:
        return None
    try:
        epoch = datetime.strptime(epoch_str, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        epoch = date(1900, 1, 1)
    if isinstance(d, datetime):
        d = d.date()
    if isinstance(d, date):
        return (d - epoch).days
    if isinstance(d, str):
        for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y%m%d"]:
            try:
                return (datetime.strptime(d, fmt).date() - epoch).days
            except ValueError:
                continue
    return None


def detect_missing_characters(source_val: str | None, target_val: str | None) -> list:
    issues = []
    if source_val is None or target_val is None:
        return issues
    if len(target_val) < len(source_val):
        missing = set(source_val) - set(target_val)
        if missing:
            issues.append(f"missing characters detected: {sorted(missing)}")
    return issues


def detect_string_issues(source_val: str | None, target_val: str | None) -> list:
    issues = []
    if source_val is None or target_val is None:
        return issues

    if source_val != target_val:
        if source_val.strip() == target_val.strip():
            issues.append("extra spaces detected")
        elif source_val.upper() == target_val.upper():
            issues.append("case mismatch")
        elif target_val in source_val and len(target_val) < len(source_val):
            issues.append("truncation detected")
        else:
            issues.append("value mismatch")

    if source_val.startswith(" ") or target_val.startswith(" "):
        if "extra spaces detected" not in issues:
            issues.append("extra spaces detected")

    if source_val.endswith(" ") or target_val.endswith(" "):
        if "extra spaces detected" not in issues:
            issues.append("extra spaces detected")

    return issues


def values_within_tolerance(source_val, target_val, tolerance) -> bool:
    try:
        import pandas as pd
        if source_val is None or target_val is None:
            return False
        try:
            if pd.isna(source_val) or pd.isna(target_val):
                return False
        except (TypeError, ValueError):
            pass
        from decimal import Decimal
        s = Decimal(str(source_val))
        t = Decimal(str(target_val))
        tol = Decimal(str(tolerance))
        return abs(s - t) <= tol
    except (TypeError, ValueError, Exception):
        return False


def generate_run_id() -> str:
    from datetime import datetime
    import uuid
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    short_id = str(uuid.uuid4())[:8]
    return f"run_{ts}_{short_id}"


def get_primary_key_value(row: dict, primary_keys: list) -> str:
    null_token = "__NULL__"
    parts = [str(row.get(pk, null_token)) for pk in primary_keys]
    return "|".join(parts)
