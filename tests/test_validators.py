import pytest
import duckdb
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.validators.hash_validator import run_hash_validator
from src.validators.row_validator import run_row_validator
from src.validators.stats_validator import run_stats_validator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeConn:
    def __init__(self, conn):
        self._conn = conn

    def fetch_df(self, query):
        return self._conn.execute(query).df()

    def execute(self, query):
        return self._conn.execute(query)

    def close(self):
        pass


def make_duckdb_pair(source_ddl, source_data, target_ddl, target_data):
    src_db = duckdb.connect(":memory:")
    tgt_db = duckdb.connect(":memory:")
    src_db.execute(source_ddl)
    for row in source_data:
        src_db.execute(f"INSERT INTO source_employees VALUES {row}")
    tgt_db.execute(target_ddl)
    for row in target_data:
        tgt_db.execute(f"INSERT INTO target_employees VALUES {row}")
    return FakeConn(src_db), FakeConn(tgt_db)


SOURCE_DDL = """
CREATE TABLE source_employees (
    employee_id INTEGER,
    first_name VARCHAR,
    last_name VARCHAR,
    hire_date DATE,
    salary DECIMAL(10,2),
    status VARCHAR
)
"""

TARGET_DDL = """
CREATE TABLE target_employees (
    employee_id INTEGER,
    first_name VARCHAR,
    last_name VARCHAR,
    hiring_date DATE,
    adjusted_salary DECIMAL(10,2)
)
"""

BASE_TABLE_CONFIG = {
    "source_table": "source_employees",
    "target_table": "target_employees",
    "primary_key": ["employee_id"],
    "source_where": "hire_date IS NOT NULL AND status <> 'TERMINATED'",
    "target_where": None,
    "normalize_strings": False,
    "columns": [
        {"source_col": "employee_id", "target_col": "employee_id", "data_type": "integer",
         "checks": ["null_count", "exact_match", "hash"]},
        {"source_col": "first_name", "target_col": "first_name", "data_type": "string",
         "checks": ["null_count", "exact_match", "hash"]},
        {"source_col": "last_name", "target_col": "last_name", "data_type": "string",
         "checks": ["null_count", "exact_match", "hash"]},
        {"source_col": "hire_date", "target_col": "hiring_date", "data_type": "date",
         "checks": ["null_count", "min", "max", "exact_match", "hash"]},
        {"source_col": "salary", "target_col": "adjusted_salary", "data_type": "decimal",
         "source_expression": "salary * 1.1", "target_expression": "adjusted_salary",
         "tolerance": 0.01,
         "checks": ["null_count", "min", "max", "sum", "avg", "exact_match", "hash"]},
    ],
    "validation_layers": {"run_layer1": True, "run_layer2": True, "run_layer3": True},
}

RUN_ID = "test_run"
RUN_TS = "2026-01-01 00:00:00"

SETTINGS = {
    "batch_size": 10000,
    "default_tolerance": 0.01,
    "null_token": "__NULL__",
    "date_format": "%Y-%m-%d",
    "decimal_places": 10,
    "hash_algorithm": "md5",
    "normalize_strings": False,
}


# ---------------------------------------------------------------------------
# Test 1: Perfect match
# ---------------------------------------------------------------------------
def test_perfect_match_hash():
    src_data = [
        "(1, 'John', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'John', 'Doe', '2020-01-01', 55000.00)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_hash_validator(src, tgt, BASE_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    hash_results = [r for r in results if r["check_type"] == "hash_match"]
    assert all(r["status"] == "PASS" for r in hash_results), "Expected all hashes to pass for perfect match."


# ---------------------------------------------------------------------------
# Test 2: Source row excluded by filter (NULL hire_date) should not appear
# ---------------------------------------------------------------------------
def test_excluded_row_not_flagged():
    src_data = [
        "(1, 'John', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
        "(2, 'Jane', 'Smith', NULL, 60000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'John', 'Doe', '2020-01-01', 55000.00)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_hash_validator(src, tgt, BASE_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    missing = [r for r in results if r["issue_type"] == "missing rows"]
    assert len(missing) == 0, "Excluded rows should not be flagged as missing."


# ---------------------------------------------------------------------------
# Test 3: Missing target row
# ---------------------------------------------------------------------------
def test_missing_target_row():
    src_data = [
        "(1, 'John', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
        "(2, 'Jane', 'Smith', '2021-03-15', 60000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'John', 'Doe', '2020-01-01', 55000.00)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_hash_validator(src, tgt, BASE_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    missing = [r for r in results if r["issue_type"] == "missing rows"]
    assert len(missing) == 1
    assert missing[0]["key_value"] == "2"


# ---------------------------------------------------------------------------
# Test 4: Extra target row
# ---------------------------------------------------------------------------
def test_extra_target_row():
    src_data = [
        "(1, 'John', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'John', 'Doe', '2020-01-01', 55000.00)",
        "(99, 'Ghost', 'Row', '2022-01-01', 10000.00)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_hash_validator(src, tgt, BASE_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    extra = [r for r in results if r["issue_type"] == "extra rows"]
    assert len(extra) == 1
    assert extra[0]["key_value"] == "99"


# ---------------------------------------------------------------------------
# Test 5: Value mismatch
# ---------------------------------------------------------------------------
def test_value_mismatch():
    src_data = [
        "(1, 'John', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'John', 'Wrong', '2020-01-01', 55000.00)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_hash_validator(src, tgt, BASE_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    fails = [r for r in results if r["status"] == "FAIL" and r["column_name"] == "last_name"]
    assert len(fails) > 0, "Expected last_name mismatch to be detected."


# ---------------------------------------------------------------------------
# Test 6: Null mismatch
# ---------------------------------------------------------------------------
def test_null_mismatch():
    src_data = [
        "(1, 'John', NULL, '2020-01-01', 50000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'John', 'Doe', '2020-01-01', 55000.00)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_hash_validator(src, tgt, BASE_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    fails = [r for r in results if r["status"] == "FAIL" and r["column_name"] == "last_name"]
    assert len(fails) > 0


# ---------------------------------------------------------------------------
# Test 7: Trailing space
# ---------------------------------------------------------------------------
def test_trailing_space():
    src_data = [
        "(1, 'John', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'John ', 'Doe', '2020-01-01', 55000.00)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_hash_validator(src, tgt, BASE_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    fails = [r for r in results if r["status"] == "FAIL" and r["column_name"] == "first_name"]
    assert len(fails) > 0


# ---------------------------------------------------------------------------
# Test 8: Leading space
# ---------------------------------------------------------------------------
def test_leading_space():
    src_data = [
        "(1, 'John', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, ' John', 'Doe', '2020-01-01', 55000.00)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_hash_validator(src, tgt, BASE_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    fails = [r for r in results if r["status"] == "FAIL" and r["column_name"] == "first_name"]
    assert len(fails) > 0


# ---------------------------------------------------------------------------
# Test 9: Case mismatch
# ---------------------------------------------------------------------------
def test_case_mismatch():
    src_data = [
        "(1, 'john', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'JOHN', 'Doe', '2020-01-01', 55000.00)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_hash_validator(src, tgt, BASE_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    fails = [r for r in results if r["status"] == "FAIL" and r["column_name"] == "first_name"]
    assert any(r["issue_type"] == "case mismatch" for r in fails)


# ---------------------------------------------------------------------------
# Test 10: String truncation
# ---------------------------------------------------------------------------
def test_string_truncation():
    src_data = [
        "(1, 'Christopher', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'Christophe', 'Doe', '2020-01-01', 55000.00)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_hash_validator(src, tgt, BASE_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    fails = [r for r in results if r["status"] == "FAIL" and r["column_name"] == "first_name"]
    assert any(r["issue_type"] == "truncation detected" for r in fails)


# ---------------------------------------------------------------------------
# Test 11: Date mismatch — different date values (day/month swapped in value)
# ---------------------------------------------------------------------------
def test_date_mismatch():
    src_data = [
        "(1, 'John', 'Doe', '2020-01-15', 50000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'John', 'Doe', '2020-03-20', 55000.00)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_hash_validator(src, tgt, BASE_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    fails = [r for r in results if r["status"] == "FAIL" and r["column_name"] == "hiring_date"]
    assert len(fails) > 0, "Expected date mismatch to be detected."


# ---------------------------------------------------------------------------
# Test 12: Decimal rounding mismatch (within tolerance → cell drill-down PASS)
# Source: salary=50000 → expression salary*1.1 = 55000.0
# Target: adjusted_salary=55000.005 → diff=0.005 which is within tolerance=0.01
# ---------------------------------------------------------------------------
def test_decimal_within_tolerance():
    src_data = [
        "(1, 'John', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'John', 'Doe', '2020-01-01', 55000.005)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_hash_validator(src, tgt, BASE_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    drill = [r for r in results if r["check_type"] == "cell_drill_down" and r["column_name"] == "adjusted_salary"]
    # Hash will differ (hashing uses raw formatted values), but cell drill-down must report PASS
    # because the numeric diff (0.005) is within tolerance (0.01)
    assert len(drill) > 0, "Expected cell drill-down to be triggered for salary."
    assert drill[0]["status"] == "PASS", (
        f"Expected PASS within tolerance, got FAIL. "
        f"source={drill[0]['source_result']}, target={drill[0]['target_result']}"
    )


# ---------------------------------------------------------------------------
# Test 13: Numeric scale mismatch (beyond tolerance → FAIL)
# ---------------------------------------------------------------------------
def test_decimal_beyond_tolerance():
    src_data = [
        "(1, 'John', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'John', 'Doe', '2020-01-01', 60000.00)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_hash_validator(src, tgt, BASE_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    drill = [r for r in results if r["check_type"] == "cell_drill_down" and r["column_name"] == "adjusted_salary"]
    assert len(drill) > 0
    assert drill[0]["status"] == "FAIL"


# ---------------------------------------------------------------------------
# Test 14: Stats mismatch
# ---------------------------------------------------------------------------
def test_stats_mismatch():
    src_data = [
        "(1, 'John', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
        "(2, 'Jane', 'Smith', '2021-03-15', 60000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'John', 'Doe', '2020-01-01', 55000.00)",
        "(2, 'Jane', 'Smith', '2021-03-15', 55000.00)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_stats_validator(src, tgt, BASE_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    fails = [r for r in results if r["status"] == "FAIL"]
    assert len(fails) > 0, "Expected stats mismatch on salary."


# ---------------------------------------------------------------------------
# Test 15: Hash mismatch confirmed by drill-down
# ---------------------------------------------------------------------------
def test_hash_mismatch_with_drilldown():
    src_data = [
        "(1, 'John', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'John', 'WRONG', '2020-01-01', 55000.00)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_hash_validator(src, tgt, BASE_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)

    hash_fail = [r for r in results if r["check_type"] == "hash_match" and r["status"] == "FAIL"]
    drill_fail = [r for r in results if r["check_type"] == "cell_drill_down" and r["status"] == "FAIL"]

    assert len(hash_fail) == 1, "Expected exactly one hash mismatch."
    assert len(drill_fail) > 0, "Expected drill-down to find the bad column."
    assert any(r["column_name"] == "last_name" for r in drill_fail)


# ---------------------------------------------------------------------------
# Test 16: Duplicate key in source — should report WARNING and skip hash
# ---------------------------------------------------------------------------
def test_duplicate_key_in_source():
    src_data = [
        "(1, 'John', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
        "(1, 'John', 'Duplicate', '2020-01-01', 50000.00, 'ACTIVE')",  # duplicate key
    ]
    tgt_data = [
        "(1, 'John', 'Doe', '2020-01-01', 55000.00)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_hash_validator(src, tgt, BASE_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)

    dup_warnings = [r for r in results if r["check_type"] == "duplicate_key_check" and "source" in r["issue_type"]]
    assert len(dup_warnings) == 1, "Expected one duplicate key warning for source."
    assert dup_warnings[0]["status"] == "WARNING"
    assert dup_warnings[0]["key_value"] == "1"

    # Duplicate key must be excluded from hash comparison
    hash_results = [r for r in results if r["check_type"] == "hash_match" and r["key_value"] == "1"]
    assert len(hash_results) == 0, "Duplicate key should be excluded from hash comparison."


# ---------------------------------------------------------------------------
# Test 17: Duplicate key in target — should report WARNING and skip hash
# ---------------------------------------------------------------------------
def test_duplicate_key_in_target():
    src_data = [
        "(1, 'John', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'John', 'Doe', '2020-01-01', 55000.00)",
        "(1, 'John', 'Duplicate', '2020-01-01', 55000.00)",  # duplicate key
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_hash_validator(src, tgt, BASE_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)

    dup_warnings = [r for r in results if r["check_type"] == "duplicate_key_check" and "target" in r["issue_type"]]
    assert len(dup_warnings) == 1, "Expected one duplicate key warning for target."
    assert dup_warnings[0]["status"] == "WARNING"
    assert dup_warnings[0]["key_value"] == "1"

    # Duplicate key must be excluded from hash comparison
    hash_results = [r for r in results if r["check_type"] == "hash_match" and r["key_value"] == "1"]
    assert len(hash_results) == 0, "Duplicate key should be excluded from hash comparison."


# ---------------------------------------------------------------------------
# Stats Layer 3 new checks
# ---------------------------------------------------------------------------

STATS_TABLE_CONFIG = {
    "source_table": "source_employees",
    "target_table": "target_employees",
    "primary_key": ["employee_id"],
    "source_where": "hire_date IS NOT NULL AND status <> 'TERMINATED'",
    "target_where": None,
    "columns": [
        {"source_col": "employee_id", "target_col": "employee_id", "data_type": "integer",
         "checks": ["null_count"]},
        {"source_col": "first_name", "target_col": "first_name", "data_type": "string",
         "checks": ["null_count", "truncation_indicator", "category_check"]},
        {"source_col": "hire_date", "target_col": "hiring_date", "data_type": "date",
         "checks": ["null_count", "min", "max", "date_distribution"]},
        {"source_col": "salary", "target_col": "adjusted_salary", "data_type": "decimal",
         "source_expression": "salary * 1.1", "target_expression": "adjusted_salary",
         "tolerance": 0.01,
         "checks": ["null_count", "sum", "avg", "decimal_scale_check"]},
    ],
    "validation_layers": {"run_layer1": False, "run_layer2": False, "run_layer3": True},
}


# ---------------------------------------------------------------------------
# Test 18: Truncation indicator — target max_length shorter than source
# ---------------------------------------------------------------------------
def test_truncation_indicator():
    src_data = [
        "(1, 'Christopher', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'Christophe', 'Doe', '2020-01-01', 55000.00)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_stats_validator(src, tgt, STATS_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    trunc = [r for r in results if r["check_type"] == "truncation_indicator"]
    assert len(trunc) == 1
    assert trunc[0]["status"] == "WARNING"
    assert trunc[0]["issue_type"] == "truncation detected"


# ---------------------------------------------------------------------------
# Test 19: Decimal scale check — scale differs between source and target
# ---------------------------------------------------------------------------
def test_decimal_scale_check():
    src_data = [
        "(1, 'John', 'Doe', '2020-01-01', 50000.005, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'John', 'Doe', '2020-01-01', 55000.01)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_stats_validator(src, tgt, STATS_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    scale = [r for r in results if r["check_type"] == "decimal_scale_check"]
    assert len(scale) == 1


# ---------------------------------------------------------------------------
# Test 20: Date distribution — year counts differ
# ---------------------------------------------------------------------------
def test_date_distribution():
    src_data = [
        "(1, 'John', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
        "(2, 'Jane', 'Smith', '2021-03-15', 60000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'John', 'Doe', '2020-01-01', 55000.00)",
        # emp 2 missing → year 2021 count will differ
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_stats_validator(src, tgt, STATS_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    dist = [r for r in results if r["check_type"] == "date_distribution" and r["status"] == "FAIL"]
    assert len(dist) > 0
    assert any(r["key_value"] == "2021" for r in dist)


# ---------------------------------------------------------------------------
# Test 21: Duplicate row check — source has duplicates
# ---------------------------------------------------------------------------
def test_unexpected_duplicates():
    src_data = [
        "(1, 'John', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
        "(1, 'John', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",  # duplicate
    ]
    tgt_data = [
        "(1, 'John', 'Doe', '2020-01-01', 55000.00)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_stats_validator(src, tgt, STATS_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    dups = [r for r in results if r["check_type"] == "duplicate_row_check_source" and r["status"] == "WARNING"]
    assert len(dups) == 1
    assert "duplicate" in dups[0]["remarks"].lower()


# ---------------------------------------------------------------------------
# Test 22: Missing category — value in source not in target
# ---------------------------------------------------------------------------
def test_missing_category():
    src_data = [
        "(1, 'John', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
        "(2, 'Jane', 'Smith', '2021-03-15', 60000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'John', 'Doe', '2020-01-01', 55000.00)",
        # Jane missing → 'Jane' category missing in target first_name
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_stats_validator(src, tgt, STATS_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    missing_cat = [r for r in results if r["check_type"] == "category_check" and r["issue_type"] == "missing category"]
    assert len(missing_cat) > 0
    assert any(r["key_value"] == "Jane" for r in missing_cat)


# ---------------------------------------------------------------------------
# Test 23: Extra category — value in target not in source
# ---------------------------------------------------------------------------
def test_extra_category():
    src_data = [
        "(1, 'John', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'John', 'Doe', '2020-01-01', 55000.00)",
        "(99, 'Ghost', 'Row', '2024-01-01', 10000.00)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_stats_validator(src, tgt, STATS_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    extra_cat = [r for r in results if r["check_type"] == "category_check" and r["issue_type"] == "extra category"]
    assert len(extra_cat) > 0
    assert any(r["key_value"] == "Ghost" for r in extra_cat)


# ---------------------------------------------------------------------------
# Extended config for new checks
# ---------------------------------------------------------------------------

EXT_TABLE_CONFIG = {
    "source_table": "source_employees",
    "target_table": "target_employees",
    "primary_key": ["employee_id"],
    "source_where": "hire_date IS NOT NULL AND status <> 'TERMINATED'",
    "target_where": None,
    "columns": [
        {"source_col": "first_name", "target_col": "first_name", "data_type": "string",
         "checks": ["case_mismatch_count", "missing_characters"]},
        {"source_col": "hire_date", "target_col": "hiring_date", "data_type": "date",
         "checks": ["date_stddev", "date_format_issue", "day_month_swap_risk"]},
        {"source_col": "salary", "target_col": "adjusted_salary", "data_type": "decimal",
         "source_expression": "salary * 1.1", "target_expression": "adjusted_salary",
         "tolerance": 0.01,
         "exchange_rate": 0.92,
         "currency_tolerance": 500.0,
         "checks": ["currency_conversion"]},
    ],
    "validation_layers": {"run_layer1": False, "run_layer2": False, "run_layer3": True},
}


# ---------------------------------------------------------------------------
# Test 24: Case mismatch count in stats
# ---------------------------------------------------------------------------
def test_case_mismatch_count_stats():
    src_data = [
        "(1, 'john', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'JOHN', 'Doe', '2020-01-01', 55000.00)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_stats_validator(src, tgt, EXT_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    case_results = [r for r in results if r["check_type"] == "case_mismatch_count"]
    assert len(case_results) == 1
    assert case_results[0]["status"] == "WARNING"


# ---------------------------------------------------------------------------
# Test 25: Missing characters detection in stats
# ---------------------------------------------------------------------------
def test_missing_characters_stats():
    src_data = [
        "(1, 'Christopher', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'Christoph', 'Doe', '2020-01-01', 55000.00)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_stats_validator(src, tgt, EXT_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    mc = [r for r in results if r["check_type"] == "missing_characters"]
    assert len(mc) == 1
    assert mc[0]["status"] == "WARNING"


# ---------------------------------------------------------------------------
# Test 26: Date stddev using configurable epoch
# ---------------------------------------------------------------------------
def test_date_stddev_configurable_epoch():
    src_data = [
        "(1, 'John', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
        "(2, 'Jane', 'Smith', '2021-06-15', 60000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'John', 'Doe', '2020-01-01', 55000.00)",
        "(2, 'Jane', 'Smith', '2021-06-15', 66000.00)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    custom_settings = {**SETTINGS, "date_numeric_epoch": "2000-01-01"}
    results = run_stats_validator(src, tgt, EXT_TABLE_CONFIG, custom_settings, RUN_ID, RUN_TS)
    stddev_results = [r for r in results if r["check_type"] == "date_stddev"]
    assert len(stddev_results) == 1
    assert stddev_results[0]["status"] == "PASS"


# ---------------------------------------------------------------------------
# Test 27: Date format issue detection
# ---------------------------------------------------------------------------
def test_date_format_issue():
    src_data = [
        "(1, 'John', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'John', 'Doe', '2020-01-01', 55000.00)",
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_stats_validator(src, tgt, EXT_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    fmt_results = [r for r in results if r["check_type"] == "date_format_issue"]
    assert len(fmt_results) == 1
    # Valid dates → PASS
    assert fmt_results[0]["status"] == "PASS"


# ---------------------------------------------------------------------------
# Test 28: Currency conversion check
# ---------------------------------------------------------------------------
def test_currency_conversion():
    src_data = [
        "(1, 'John', 'Doe', '2020-01-01', 50000.00, 'ACTIVE')",
    ]
    tgt_data = [
        "(1, 'John', 'Doe', '2020-01-01', 46000.00)",  # ~50000 * 1.1 * 0.92 = 50600 — intentionally wrong
    ]
    src, tgt = make_duckdb_pair(SOURCE_DDL, src_data, TARGET_DDL, tgt_data)
    results = run_stats_validator(src, tgt, EXT_TABLE_CONFIG, SETTINGS, RUN_ID, RUN_TS)
    curr = [r for r in results if r["check_type"] in ("currency_sum_check", "currency_avg_check")]
    assert len(curr) == 2
