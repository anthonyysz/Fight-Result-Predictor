from __future__ import annotations

import argparse
from pathlib import Path

from historical_scraper.core.csv_manager import (
    RECENT_FIGHTS_CSV_PATH,
    create_empty_recent_dataframe,
    finalize_recent_dataframe,
    save_missing_odds_report,
    save_missing_reports,
    save_recent_dataframe,
)
from historical_scraper.core.utils import parse_us_date
from historical_scraper.sources.odds_scraper import apply_odds
from historical_scraper.sources.rankings_scraper import apply_rankings
from historical_scraper.sources.ufcstats_scraper import apply_ufcstats_data, initialize_recent_rows


DEFAULT_START_DATE = "2024-12-14"
SCRAPER_DIR = Path(__file__).resolve().parent
REPO_DIR = SCRAPER_DIR.parent
DATA_DIR = SCRAPER_DIR / "data"
SQL_ENV_PATH = REPO_DIR / "sql_data" / ".env"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build historical_scraper/recent_fights.csv from 2024-12-14 to present.")
    parser.add_argument(
        "--start-date",
        default=DEFAULT_START_DATE,
        help="Inclusive start date in YYYY-MM-DD format. Defaults to 2024-12-14.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start_date = parse_us_date(args.start_date)
    database_latest_fight_date = lookup_database_latest_fight_date()

    initial_rows = initialize_recent_rows(start_date)
    df_internal = create_empty_recent_dataframe(initial_rows)
    df_internal = apply_ufcstats_data(df_internal)
    df_internal = apply_odds(df_internal, DATA_DIR / "fighter_aliases.csv")
    df_internal = apply_rankings(df_internal, DATA_DIR, DATA_DIR / "fighter_aliases.csv")

    recent_df = finalize_recent_dataframe(df_internal)
    save_recent_dataframe(recent_df)
    missing_report_path, missing_summary_path = save_missing_reports(recent_df)
    missing_odds_report_path = save_missing_odds_report(df_internal)

    print(f"start_date: {start_date.isoformat()}")
    print(f"database_latest_fight_date: {database_latest_fight_date or 'unavailable'}")
    print(f"recent_fights_csv: {RECENT_FIGHTS_CSV_PATH}")
    print(f"missing_data_report: {missing_report_path}")
    print(f"missing_columns_summary: {missing_summary_path}")
    print(f"missing_odds_report: {missing_odds_report_path}")
    print(f"fight_rows: {len(recent_df)}")


def lookup_database_latest_fight_date() -> str | None:
    if not SQL_ENV_PATH.exists():
        return None

    try:
        import psycopg
    except ImportError:
        return None

    env_values = read_dotenv(SQL_ENV_PATH)
    database_url = env_values.get("DATABASE_URL")
    if database_url:
        conninfo = database_url
    else:
        required_keys = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]
        if any(not env_values.get(key) for key in required_keys):
            return None
        conninfo = (
            f"host={env_values['PGHOST']} "
            f"port={env_values['PGPORT']} "
            f"dbname={env_values['PGDATABASE']} "
            f"user={env_values['PGUSER']} "
            f"password={env_values['PGPASSWORD']}"
        )

    try:
        with psycopg.connect(conninfo) as conn:
            with conn.cursor() as cur:
                cur.execute("select max(fight_date) from public.all_fights")
                row = cur.fetchone()
                if not row or row[0] is None:
                    return None
                return row[0].isoformat()
    except Exception:
        return None


def read_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


if __name__ == "__main__":
    main()
