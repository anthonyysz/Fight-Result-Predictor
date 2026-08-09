from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Any

import psycopg

from reports.fight_week import generate_fight_week_report
from shared.config import get_database_conninfo


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scrape this week's upcoming UFC event, load it into Postgres, "
            "generate predictions, and build the local fight-week report files."
        )
    )
    parser.add_argument(
        "--reference-date",
        type=date.fromisoformat,
        default=None,
        help="Optional YYYY-MM-DD date for selecting that week's fight weekend.",
    )
    return parser.parse_args()


def public_summary(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": summary.get("status"),
        "event_name": summary.get("event_name"),
        "event_date": summary.get("event_date"),
        "generated_at": summary.get("generated_at"),
        "report_title": summary.get("report_title"),
        "prediction_count": summary.get("prediction_count"),
        "latest_html": (summary.get("report_paths") or {}).get("latest_html"),
        "latest_csv": (summary.get("report_paths") or {}).get("latest_csv"),
        "history_path": summary.get("history_path"),
        "reason": summary.get("reason"),
    }


def main() -> None:
    args = parse_args()
    with psycopg.connect(get_database_conninfo(required=True)) as conn:
        summary = generate_fight_week_report(
            conn,
            reference_date=args.reference_date,
        )
    print(json.dumps(public_summary(summary), indent=2))


if __name__ == "__main__":
    main()
