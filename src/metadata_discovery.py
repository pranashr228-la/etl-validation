from src.utils import logger


def get_all_schemas(conn, db_type: str) -> list:
    try:
        if db_type == "duckdb":
            df = conn.fetch_df("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'pg_catalog')")
            return df["schema_name"].tolist()
        elif db_type in ("postgres", "mysql"):
            df = conn.fetch_df("SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')")
            return df["schema_name"].tolist()
        elif db_type == "mssql":
            df = conn.fetch_df("SELECT name FROM sys.schemas WHERE name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest')")
            return df["name"].tolist()
        elif db_type == "snowflake":
            df = conn.fetch_df("SHOW SCHEMAS")
            return df["name"].tolist()
        return ["main"]
    except Exception as e:
        logger.warning(f"Could not list schemas: {e}")
        return ["main"]


def get_tables(conn, db_type: str, schema: str = None) -> list:
    try:
        if db_type == "duckdb":
            if schema:
                df = conn.fetch_df(
                    f"SELECT table_name FROM information_schema.tables "
                    f"WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE'"
                )
                return df["table_name"].tolist()
            else:
                df = conn.fetch_df(
                    "SELECT table_schema, table_name FROM information_schema.tables "
                    "WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('information_schema', 'pg_catalog')"
                )
                return [f"{row['table_schema']}.{row['table_name']}" for _, row in df.iterrows()]
        elif db_type in ("postgres", "mysql"):
            df = conn.fetch_df(
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')"
            )
            return [f"{row['table_schema']}.{row['table_name']}" for _, row in df.iterrows()]
        elif db_type == "mssql":
            df = conn.fetch_df(
                "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
            )
            return [f"{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}" for _, row in df.iterrows()]
        elif db_type == "snowflake":
            df = conn.fetch_df(
                "SELECT TABLE_NAME FROM information_schema.tables "
                "WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME"
            )
            name_col = "TABLE_NAME" if "TABLE_NAME" in df.columns else "table_name"
            return df[name_col].tolist()
        else:
            df = conn.fetch_df("SHOW TABLES")
            return df.iloc[:, 0].tolist()
    except Exception as e:
        logger.warning(f"Could not list tables: {e}")
        return []


def get_columns(conn, table: str, db_type: str) -> list:
    try:
        # Handle fully-qualified table names: db.schema.table or schema.table
        parts = table.split(".")
        if len(parts) == 3:
            schema, tbl = parts[1], parts[2]
        elif len(parts) == 2:
            schema, tbl = parts[0], parts[1]
        else:
            schema, tbl = None, table

        if db_type == "duckdb":
            if schema:
                df = conn.fetch_df(
                    f"SELECT column_name, data_type AS column_type "
                    f"FROM information_schema.columns "
                    f"WHERE table_schema = '{schema}' AND table_name = '{tbl}' "
                    f"ORDER BY ordinal_position"
                )
            else:
                df = conn.fetch_df(f"DESCRIBE {tbl}")
                df = df.rename(columns={"column_type": "column_type"})
            return [
                {"column_name": row["column_name"], "column_type": row["column_type"]}
                for _, row in df.iterrows()
            ]
        elif db_type in ("postgres", "mysql"):
            where_schema = f"AND table_schema = '{schema}'" if schema else ""
            df = conn.fetch_df(
                f"SELECT column_name, data_type FROM information_schema.columns "
                f"WHERE table_name = '{tbl}' {where_schema} ORDER BY ordinal_position"
            )
            return [
                {"column_name": row["column_name"], "column_type": row["data_type"]}
                for _, row in df.iterrows()
            ]
        elif db_type == "mssql":
            where_schema = f"AND TABLE_SCHEMA = '{schema}'" if schema else ""
            df = conn.fetch_df(
                f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_NAME = '{tbl}' {where_schema} ORDER BY ORDINAL_POSITION"
            )
            return [
                {"column_name": row["COLUMN_NAME"], "column_type": row["DATA_TYPE"]}
                for _, row in df.iterrows()
            ]
        elif db_type == "snowflake":
            if schema:
                df = conn.fetch_df(
                    f"SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns "
                    f"WHERE TABLE_SCHEMA = '{schema.upper()}' AND TABLE_NAME = '{tbl.upper()}' "
                    f"ORDER BY ORDINAL_POSITION"
                )
            else:
                df = conn.fetch_df(
                    f"SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns "
                    f"WHERE TABLE_NAME = '{tbl.upper()}' ORDER BY ORDINAL_POSITION"
                )
            # Snowflake returns uppercase column names
            name_col = "COLUMN_NAME" if "COLUMN_NAME" in df.columns else "column_name"
            type_col = "DATA_TYPE" if "DATA_TYPE" in df.columns else "data_type"
            return [
                {"column_name": row[name_col], "column_type": row[type_col]}
                for _, row in df.iterrows()
            ]
        else:
            df = conn.fetch_df(f"DESCRIBE {table}")
            return [
                {"column_name": row.iloc[0], "column_type": row.iloc[1]}
                for _, row in df.iterrows()
            ]
    except Exception as e:
        logger.warning(f"Could not describe table '{table}': {e}")
        return []


def get_row_count(conn, table: str, where: str = None) -> int:
    try:
        where_clause = f"WHERE {where}" if where else ""
        df = conn.fetch_df(f"SELECT COUNT(*) AS cnt FROM {table} {where_clause}".strip())
        # Snowflake returns uppercase column names
        col = "CNT" if "CNT" in df.columns else "cnt"
        return int(df.iloc[0][col])
    except Exception as e:
        logger.warning(f"Could not count rows for '{table}': {e}")
        return -1


def map_to_standard_type(col_type: str) -> str:
    col_type = col_type.upper()
    if any(t in col_type for t in ["VARCHAR", "TEXT", "CHAR", "STRING", "NVARCHAR", "NCHAR", "UUID"]):
        return "string"
    if "TIMESTAMP" in col_type or "DATETIME" in col_type:
        return "date"
    if "DATE" in col_type:
        return "date"
    if any(t in col_type for t in ["DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL", "MONEY", "HUGEINT"]):
        return "decimal"
    if any(t in col_type for t in ["INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT", "UBIGINT", "BYTEINT", "UTINYINT"]):
        return "integer"
    if "BOOL" in col_type or "BIT" in col_type:
        return "string"
    return "string"


def discover_metadata(conn, db_type: str) -> dict:
    tables = get_tables(conn, db_type)
    metadata = {}
    for table in tables:
        columns = get_columns(conn, table, db_type)
        row_count = get_row_count(conn, table)
        metadata[table] = {
            "columns": columns,
            "row_count": row_count,
            "column_map": {
                col["column_name"]: map_to_standard_type(col["column_type"])
                for col in columns
            },
        }
        logger.info(f"  Discovered: {table} ({len(columns)} columns, {row_count} rows)")
    return metadata
