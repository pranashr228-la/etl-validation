def build_where_clause(where_expr: str | None) -> str:
    if not where_expr:
        return ""
    return f"WHERE {where_expr}"


def build_source_query(table_config: dict, columns: list[str] | None = None) -> str:
    source_table = table_config["source_table"]
    source_where = table_config.get("source_where")
    where_clause = build_where_clause(source_where)

    if columns:
        col_list = ", ".join(columns)
    else:
        col_list = "*"

    return f"SELECT {col_list} FROM {source_table} {where_clause}".strip()


def build_target_query(table_config: dict, columns: list[str] | None = None) -> str:
    target_table = table_config["target_table"]
    target_where = table_config.get("target_where")
    where_clause = build_where_clause(target_where)

    if columns:
        col_list = ", ".join(columns)
    else:
        col_list = "*"

    return f"SELECT {col_list} FROM {target_table} {where_clause}".strip()


def build_source_select_expressions(table_config: dict) -> list[str]:
    expressions = []
    for col in table_config["columns"]:
        source_col = col["source_col"]
        target_col = col["target_col"]
        expr = col.get("source_expression")
        if expr:
            # Apply expression and alias to target column name
            expressions.append(f"({expr}) AS {target_col}")
        elif source_col != target_col:
            # Alias source column to target column name so both DataFrames align
            expressions.append(f"{source_col} AS {target_col}")
        else:
            expressions.append(source_col)
    return expressions


def build_target_select_expressions(table_config: dict) -> list[str]:
    expressions = []
    for col in table_config["columns"]:
        target_col = col["target_col"]
        expr = col.get("target_expression")
        if expr:
            expressions.append(f"({expr}) AS {target_col}")
        else:
            expressions.append(target_col)
    return expressions
