import os
import yaml
from dotenv import load_dotenv


load_dotenv()

DEFAULT_SETTINGS = {
    "batch_size": 10000,
    "default_tolerance": 0.01,
    "null_token": "__NULL__",
    "date_format": "%Y-%m-%d",
    "decimal_places": 10,
    "hash_algorithm": "md5",
    "normalize_strings": False,
    "date_numeric_epoch": "1900-01-01",
    "valid_date_formats": ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y%m%d"],
    "default_currency_tolerance": 0.05,
}


def _resolve_env_vars(value):
    if isinstance(value, str):
        for key, env_val in os.environ.items():
            value = value.replace(f"${{{key}}}", env_val)
        return value
    elif isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_resolve_env_vars(i) for i in value]
    return value


def _validate_db_config(db_config: dict, label: str):
    supported = {"duckdb", "postgres", "mssql", "mysql", "snowflake"}
    db_type = db_config.get("db_type")

    if not db_type:
        raise ValueError(f"{label}: 'db_type' is required.")
    if db_type not in supported:
        raise ValueError(f"{label}: unsupported db_type '{db_type}'. Supported: {supported}")

    if db_type == "duckdb":
        if not db_config.get("path"):
            raise ValueError(f"{label}: 'path' is required for db_type 'duckdb'.")
    elif db_type == "snowflake":
        # For Snowflake: host = account identifier; warehouse and role are optional
        required = ["host", "database", "username", "password"]
        for field in required:
            val = db_config.get(field)
            if not val or str(val).startswith("${"):
                raise ValueError(
                    f"{label}: '{field}' is required for db_type 'snowflake'. "
                    f"Make sure the environment variable is set in your .env file."
                )
    else:
        required = ["host", "port", "database", "username", "password"]
        for field in required:
            val = db_config.get(field)
            if not val or str(val).startswith("${"):
                raise ValueError(
                    f"{label}: '{field}' is required for db_type '{db_type}'. "
                    f"Make sure the environment variable is set in your .env file."
                )


def _validate_table_config(table: dict):
    name = table.get("source_table", "<unknown>")

    if "source_table" not in table:
        raise ValueError("A table entry is missing 'source_table'.")
    if "target_table" not in table:
        raise ValueError(f"Table '{name}': 'target_table' is required.")
    if "primary_key" not in table or not table["primary_key"]:
        raise ValueError(f"Table '{name}': 'primary_key' is required and must not be empty.")
    if "columns" not in table or not table["columns"]:
        raise ValueError(f"Table '{name}': 'columns' is required and must not be empty.")

    for col in table["columns"]:
        if "source_col" not in col:
            raise ValueError(f"Table '{name}': a column entry is missing 'source_col'.")
        if "target_col" not in col:
            raise ValueError(f"Table '{name}': column '{col.get('source_col')}' is missing 'target_col'.")
        if "data_type" not in col:
            raise ValueError(f"Table '{name}': column '{col.get('source_col')}' is missing 'data_type'.")


def _validate_settings(settings: dict):
    valid_algorithms = {"md5", "sha256"}
    algo = settings.get("hash_algorithm", "md5")
    if algo not in valid_algorithms:
        raise ValueError(f"settings.hash_algorithm '{algo}' is not valid. Choose from: {valid_algorithms}")

    batch_size = settings.get("batch_size", 10000)
    if not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError(f"settings.batch_size must be a positive integer, got: {batch_size}")

    decimal_places = settings.get("decimal_places", 10)
    if not isinstance(decimal_places, int) or decimal_places < 0:
        raise ValueError(f"settings.decimal_places must be a non-negative integer, got: {decimal_places}")


def load_config(config_path: str) -> dict:
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)

    config = _resolve_env_vars(raw)

    if "source" not in config:
        raise ValueError("Config is missing 'source' section.")
    if "target" not in config:
        raise ValueError("Config is missing 'target' section.")
    if "tables" not in config or not config["tables"]:
        raise ValueError("Config is missing 'tables' section or it is empty.")

    _validate_db_config(config["source"], "source")
    _validate_db_config(config["target"], "target")

    # Merge user settings with defaults — user values override defaults
    user_settings = config.get("settings", {})
    config["settings"] = {**DEFAULT_SETTINGS, **user_settings}
    _validate_settings(config["settings"])

    for table in config["tables"]:
        _validate_table_config(table)

    return config


def get_settings(config: dict) -> dict:
    return config.get("settings", DEFAULT_SETTINGS.copy())
