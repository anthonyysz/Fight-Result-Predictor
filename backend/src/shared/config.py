from __future__ import annotations

import os
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[2]
ENV_PATH = BACKEND_DIR / ".env"
DB_REQUIRED_KEYS = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]


def read_dotenv(path: str | Path) -> dict[str, str]:
    values: dict[str, str] = {}
    env_path = Path(path)

    if not env_path.exists():
        return values

    with env_path.open("r", encoding="utf-8") as handle:
        raw_lines = handle.read().splitlines()

    for raw_line in raw_lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")

    return values


def get_file_env_values() -> dict[str, str]:
    return read_dotenv(ENV_PATH)


def get_setting(name: str, default: str | None = None) -> str | None:
    runtime_value = os.environ.get(name)
    if runtime_value:
        return runtime_value

    file_value = get_file_env_values().get(name)
    if file_value:
        return file_value

    return default


def parse_csv_env(value: str | None) -> list[str]:
    if not value:
        return []

    parsed: list[str] = []
    for item in value.split(","):
        cleaned = item.strip()
        if not cleaned:
            continue
        if cleaned.startswith(("http://", "https://")):
            cleaned = cleaned.rstrip("/")
        parsed.append(cleaned)

    return parsed


def get_csv_setting(name: str, default: list[str] | None = None) -> list[str]:
    parsed = parse_csv_env(get_setting(name))
    if parsed:
        return parsed
    return default or []


def build_conninfo(values: dict[str, str]) -> str:
    return (
        f"host={values['PGHOST']} "
        f"port={values['PGPORT']} "
        f"dbname={values['PGDATABASE']} "
        f"user={values['PGUSER']} "
        f"password={values['PGPASSWORD']}"
    )


def get_database_conninfo(required: bool = True) -> str | None:
    runtime_database_url = os.environ.get("DATABASE_URL")
    if runtime_database_url:
        return runtime_database_url

    runtime_values = {key: os.environ.get(key) for key in DB_REQUIRED_KEYS}
    if all(runtime_values.values()):
        return build_conninfo(runtime_values)

    file_values = get_file_env_values()
    file_database_url = file_values.get("DATABASE_URL")
    if file_database_url:
        return file_database_url

    file_split_values = {key: file_values.get(key) for key in DB_REQUIRED_KEYS}
    if all(file_split_values.values()):
        return build_conninfo(file_split_values)  # type: ignore[arg-type]

    if required:
        raise ValueError(
            "Missing database settings. Set DATABASE_URL or "
            "PGHOST, PGPORT, PGDATABASE, PGUSER, and PGPASSWORD."
        )

    return None
