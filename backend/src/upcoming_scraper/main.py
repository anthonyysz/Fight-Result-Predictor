from __future__ import annotations

import argparse
import os
from datetime import date
from typing import Any

from historical_scraper.core.csv_manager import create_empty_recent_dataframe
from historical_scraper.sources.odds_scraper import apply_odds
from historical_scraper.sources.rankings_scraper import apply_rankings
from upcoming_scraper.core.csv_manager import (
    UPCOMING_FIGHTS_CSV_PATH,
    finalize_upcoming_dataframe,
    save_missing_odds_report,
    save_missing_reports,
    save_upcoming_dataframe,
)
from upcoming_scraper.loaders import build_upcoming_metadata
from upcoming_scraper.sources.ufcstats_scraper import apply_upcoming_ufcstats_data, initialize_upcoming_rows


SCRAPER_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.normpath(os.path.join(SCRAPER_DIR, "..", ".."))
REFERENCE_DATA_DIR = os.path.join(BACKEND_DIR, "data", "reference")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build backend/data/generated/upcoming_scraper/upcoming_fights.csv for the nearest upcoming UFC event."
    )
    return parser.parse_args()


def run_upcoming_scrape(today: date | None = None) -> dict[str, Any]:
    # Creating the dataframe using apply odds, apply rankings, and apply stats from the respective source files
    initial_rows = initialize_upcoming_rows(today)
    df_internal = create_empty_recent_dataframe(initial_rows)
    df_internal = apply_upcoming_ufcstats_data(df_internal)
    alias_csv_path = os.path.join(REFERENCE_DATA_DIR, "fighter_aliases.csv")
    df_internal = apply_odds(df_internal, alias_csv_path)
    df_internal = apply_rankings(df_internal, REFERENCE_DATA_DIR, alias_csv_path)

    # Saving the dataframe and the missing data reports
    upcoming_df = finalize_upcoming_dataframe(df_internal)
    upcoming_csv_path = save_upcoming_dataframe(upcoming_df)
    missing_report_path, missing_summary_path = save_missing_reports(upcoming_df)
    missing_odds_report_path = save_missing_odds_report(df_internal)
    metadata_path = build_upcoming_metadata(initial_rows)

    event_name = initial_rows[0]["event_name"] if initial_rows else "unavailable"
    event_date = initial_rows[0]["fight_date"].isoformat() if initial_rows else "unavailable"

    return {
        "event_name": event_name,
        "event_date": event_date,
        "upcoming_fights_csv": upcoming_csv_path,
        "metadata_path": metadata_path,
        "missing_data_report": missing_report_path,
        "missing_columns_summary": missing_summary_path,
        "missing_odds_report": missing_odds_report_path,
        "fight_rows": len(upcoming_df),
        "initial_rows": initial_rows,
    }


def main() -> None:
    parse_args()
    summary = run_upcoming_scrape()

    print(f"event_name: {summary['event_name']}")
    print(f"event_date: {summary['event_date']}")
    print(f"upcoming_fights_csv: {summary['upcoming_fights_csv']}")
    print(f"metadata_path: {summary['metadata_path']}")
    print(f"missing_data_report: {summary['missing_data_report']}")
    print(f"missing_columns_summary: {summary['missing_columns_summary']}")
    print(f"missing_odds_report: {summary['missing_odds_report']}")
    print(f"fight_rows: {summary['fight_rows']}")


if __name__ == "__main__":
    main()
