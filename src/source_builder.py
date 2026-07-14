"""
source_builder.py
=================
Reads table_mappings.yaml and builds:
  1. A unified source SQL query (handles single or multi-table JOINs)
  2. A list of target tables with their filters

Supports:
  - Single source table
  - Multiple source tables with INNER/LEFT/RIGHT JOIN and join_on condition
  - source_where filter applied after join
  - Multiple target tables per mapping
"""

import os
import yaml
from src.utils import logger


MAPPINGS_FILE = "table_mappings.yaml"


def load_mappings(path: str = MAPPINGS_FILE) -> list:
    if not os.path.exists(path):
        return []
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    return data.get("mappings", []) if data else []


def build_source_query(mapping: dict, select_cols: str = "*") -> str:
    """
    Build the unified source SQL query from a mapping definition.

    Single source:
        SELECT * FROM oltp.ProductCategory WHERE ...

    Multi source (join):
        SELECT * FROM oltp.SalesOrderHeader h
        INNER JOIN oltp.SalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID
        WHERE h.OnlineOrderFlag = true
    """
    sources = mapping.get("sources", [])
    source_where = mapping.get("source_where")

    if not sources:
        raise ValueError(f"Mapping '{mapping.get('name')}' has no sources defined.")

    if len(sources) == 1:
        # Single source — simple SELECT
        table = sources[0]["table"]
        where_clause = f"WHERE {source_where}" if source_where else ""
        return f"SELECT {select_cols} FROM {table} {where_clause}".strip()

    # Multi source — build JOIN query
    # First table is the base (FROM)
    base = sources[0]
    base_table = base["table"]
    base_alias = base.get("alias", "")
    from_clause = f"{base_table} {base_alias}".strip()

    join_clauses = []
    for src in sources[1:]:
        join_table = src["table"]
        join_alias = src.get("alias", "")
        join_type = src.get("join_type", "INNER JOIN").upper()
        join_on = src.get("join_on", "")
        if not join_on:
            raise ValueError(
                f"Mapping '{mapping.get('name')}': source '{join_table}' "
                f"is missing 'join_on' condition."
            )
        join_clauses.append(f"{join_type} {join_table} {join_alias} ON {join_on}".strip())

    joins = "\n".join(join_clauses)
    where_clause = f"WHERE {source_where}" if source_where else ""

    return f"SELECT {select_cols} FROM {from_clause}\n{joins}\n{where_clause}".strip()


def get_source_label(mapping: dict) -> str:
    sources = mapping.get("sources", [])
    if len(sources) == 1:
        return sources[0]["table"]
    return " + ".join(s["table"] for s in sources)


def get_targets(mapping: dict) -> list:
    return mapping.get("targets", [])


def expand_mappings(mappings: list) -> list:
    """
    Expand each mapping into flat source→target pairs.
    A mapping with multiple targets produces one pair per target.

    Returns list of dicts:
      {
        name, source_label, source_query_base,
        source_where, sources,
        target_table, target_where
      }
    """
    pairs = []
    for mapping in mappings:
        name = mapping.get("name", "unnamed")
        source_label = get_source_label(mapping)
        source_where = mapping.get("source_where")
        sources = mapping.get("sources", [])
        targets = get_targets(mapping)

        if not targets:
            logger.warning(f"Mapping '{name}' has no targets. Skipping.")
            continue

        for tgt in targets:
            pairs.append({
                "name": name,
                "source_label": source_label,
                "sources": sources,
                "source_where": source_where,
                "target_table": tgt["table"],
                "target_where": tgt.get("target_where"),
                "compare_columns": mapping.get("compare_columns"),
            })

    return pairs
