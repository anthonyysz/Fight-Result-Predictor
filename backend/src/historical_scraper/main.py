from __future__ import annotations

import argparse
import os

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
SCRAPER_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.normpath(os.path.join(SCRAPER_DIR, "..", ".."))
REFERENCE_DATA_DIR = os.path.join(BACKEND_DIR, "data", "reference", "historical_scraper")
SQL_ENV_PATH = os.path.join(BACKEND_DIR, ".env")

# Adding a start date argument
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build backend/data/generated/historical_scraper/recent_fights.csv from 2024-12-14 to present."
    )
    parser.add_argument(
        "--start-date",
        default=DEFAULT_START_DATE,
        help="Inclusive start date in YYYY-MM-DD format. Defaults to 2024-12-14.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start_date = parse_us_date(args.start_date)
    # Checking when we need to start scraping from
    database_latest_fight_date = lookup_database_latest_fight_date()

    # Creating the dataframe using apply odds, apply rankings, and apply stats from the respective source files
    initial_rows = initialize_recent_rows(start_date)
    df_internal = create_empty_recent_dataframe(initial_rows)
    df_internal = apply_ufcstats_data(df_internal)
    alias_csv_path = os.path.join(REFERENCE_DATA_DIR, "fighter_aliases.csv")
    df_internal = apply_odds(df_internal, alias_csv_path)
    df_internal = apply_rankings(df_internal, REFERENCE_DATA_DIR, alias_csv_path)

    # Saving the dataframe and the missing data reports
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

# Checking for the most recent fight that's been added to the SQL database
def lookup_database_latest_fight_date() -> str | None:
    if not os.path.exists(SQL_ENV_PATH):
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

# Used to read the environment file
def read_dotenv(path: str) -> dict[str, str]:
    values: dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as handle:
        raw_lines = handle.read().splitlines()
    for raw_line in raw_lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


if __name__ == "__main__":
    main()
