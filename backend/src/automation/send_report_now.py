from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Any

import psycopg

from automation.fight_week import run_fight_week_report_now
from shared.config import get_database_conninfo


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scrape this week's upcoming UFC event, load it into Postgres, "
            "generate predictions, build the email report, and send it through Mailchimp immediately. "
            "MAILCHIMP_DRY_RUN=true prevents the live send."
        )
    )
    parser.add_argument(
        "--run-type",
        choices=["early", "late"],
        default="late",
        help="Label to use in the report subject and archive file names.",
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
        "run_type": summary.get("run_type"),
        "event_name": summary.get("event_name"),
        "event_date": summary.get("event_date"),
        "generated_at": summary.get("generated_at"),
        "sent_at": summary.get("sent_at"),
        "dry_run": summary.get("dry_run"),
        "mailchimp_campaign_id": summary.get("mailchimp_campaign_id"),
        "mailchimp_subject": summary.get("mailchimp_subject"),
        "audience_member_count": summary.get("audience_member_count"),
        "prediction_count": summary.get("prediction_count"),
        "latest_html": (summary.get("report_paths") or {}).get("latest_html"),
        "latest_csv": (summary.get("report_paths") or {}).get("latest_csv"),
        "queue_path": summary.get("queue_path"),
        "reason": summary.get("reason"),
    }


def main() -> None:
    args = parse_args()
    with psycopg.connect(get_database_conninfo(required=True)) as conn:
        summary = run_fight_week_report_now(
            conn,
            run_type=args.run_type,
            reference_date=args.reference_date,
        )
    print(json.dumps(public_summary(summary), indent=2))


if __name__ == "__main__":
    main()
