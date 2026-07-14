from src.utils import logger


def _build_duckdb_connection(db_config: dict, label: str):
    import duckdb
    path = db_config.get("path")
    if not path:
        raise ValueError(f"[{label}] 'path' is required for DuckDB connection.")
    # Try read_only first (preferred — allows DBeaver to stay open)
    # Fall back to read_write if file is locked exclusively
    try:
        return duckdb.connect(database=path, read_only=True)
    except Exception:
        pass
    try:
        return duckdb.connect(database=path, read_only=False)
    except Exception as e:
        raise ConnectionError(
            f"[{label}] Failed to connect to DuckDB at '{path}'.\n"
            f"If DBeaver has the file open, please disconnect DBeaver first, then retry.\n"
            f"Error: {e}"
        )


def _build_postgres_connection(db_config: dict, label: str):
    try:
        import psycopg2
    except ImportError:
        raise ImportError(f"[{label}] psycopg2 is not installed. Run: pip install psycopg2-binary")
    try:
        return psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            dbname=db_config["database"],
            user=db_config["username"],
            password=db_config["password"],
        )
    except Exception as e:
        raise ConnectionError(f"[{label}] Failed to connect to PostgreSQL: {e}")


def _build_mssql_connection(db_config: dict, label: str):
    try:
        import pyodbc
    except ImportError:
        raise ImportError(f"[{label}] pyodbc is not installed. Run: pip install pyodbc")
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={db_config['host']},{db_config['port']};"
            f"DATABASE={db_config['database']};"
            f"UID={db_config['username']};"
            f"PWD={db_config['password']}"
        )
        return pyodbc.connect(conn_str)
    except Exception as e:
        raise ConnectionError(f"[{label}] Failed to connect to SQL Server: {e}")


def _build_mysql_connection(db_config: dict, label: str):
    try:
        import pymysql
    except ImportError:
        raise ImportError(f"[{label}] pymysql is not installed. Run: pip install pymysql")
    try:
        return pymysql.connect(
            host=db_config["host"],
            port=int(db_config["port"]),
            database=db_config["database"],
            user=db_config["username"],
            password=db_config["password"],
        )
    except Exception as e:
        raise ConnectionError(f"[{label}] Failed to connect to MySQL: {e}")


def _build_snowflake_connection(db_config: dict, label: str):
    try:
        import snowflake.connector
    except ImportError:
        raise ImportError(
            f"[{label}] snowflake-connector-python is not installed. "
            f"Run: pip install snowflake-connector-python"
        )
    try:
        connect_kwargs = {
            "account": db_config["host"],
            "user": db_config["username"],
            "password": db_config["password"],
            "database": db_config["database"],
            "schema": db_config.get("schema", "PUBLIC"),
        }
        if db_config.get("warehouse"):
            connect_kwargs["warehouse"] = db_config["warehouse"]
        if db_config.get("role"):
            connect_kwargs["role"] = db_config["role"]

        # Use corporate CA bundle if set (needed behind SSL-inspecting proxies)
        import os
        ca_bundle = os.environ.get("SNOWFLAKE_CA_BUNDLE", "")
        if ca_bundle and os.path.exists(ca_bundle):
            os.environ["REQUESTS_CA_BUNDLE"] = ca_bundle
            os.environ["SSL_CERT_FILE"] = ca_bundle
            logger.info(f"[{label}] Using CA bundle: {ca_bundle}")

        return snowflake.connector.connect(**connect_kwargs)
    except Exception as e:
        raise ConnectionError(f"[{label}] Failed to connect to Snowflake: {e}")


DB_BUILDERS = {
    "duckdb": _build_duckdb_connection,
    "postgres": _build_postgres_connection,
    "mssql": _build_mssql_connection,
    "mysql": _build_mysql_connection,
    "snowflake": _build_snowflake_connection,
}


class DBConnector:
    def __init__(self, db_config: dict, label: str = "db"):
        self.label = label
        self.db_type = db_config.get("db_type", "duckdb")
        self.db_config = db_config
        self.conn = None

    def connect(self):
        builder = DB_BUILDERS.get(self.db_type)
        if not builder:
            raise ValueError(
                f"[{self.label}] Unsupported db_type '{self.db_type}'. "
                f"Supported: {list(DB_BUILDERS.keys())}"
            )
        logger.info(f"[{self.label}] Connecting to {self.db_type}...")
        self.conn = builder(self.db_config, self.label)
        logger.info(f"[{self.label}] Connected successfully.")
        return self

    def execute(self, query: str):
        if self.conn is None:
            raise RuntimeError(f"[{self.label}] Not connected. Call connect() first.")
        try:
            return self.conn.execute(query)
        except Exception as e:
            raise RuntimeError(f"[{self.label}] Query failed:\n{query}\nError: {e}")

    def fetch_df(self, query: str):
        import pandas as pd
        if self.conn is None:
            raise RuntimeError(f"[{self.label}] Not connected. Call connect() first.")
        try:
            if self.db_type == "duckdb":
                return self.conn.execute(query).df()
            else:
                return pd.read_sql(query, self.conn)
        except Exception as e:
            raise RuntimeError(f"[{self.label}] Query failed:\n{query}\nError: {e}")

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info(f"[{self.label}] Connection closed.")

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def get_connectors(config: dict):
    source_conn = DBConnector(config["source"], label="source")
    target_conn = DBConnector(config["target"], label="target")
    source_conn.connect()
    target_conn.connect()
    return source_conn, target_conn
