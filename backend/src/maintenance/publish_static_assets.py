from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg

from api.stats import render_average_return_chart, render_suggested_bet_confidence_chart, render_top_betting_events_chart
from shared.config import get_database_conninfo


REPO_ROOT = Path(__file__).resolve().parents[3]
FRONTEND_GENERATED_DIR = REPO_ROOT / "frontend" / "public" / "generated"
AVERAGE_RETURN_CHART_PATH = FRONTEND_GENERATED_DIR / "average-return-chart.png"
SUGGESTED_BET_CONFIDENCE_CHART_PATH = FRONTEND_GENERATED_DIR / "suggested-bet-confidence-chart.png"
TOP_EVENTS_CHART_PATH = FRONTEND_GENERATED_DIR / "top-betting-events-chart.png"
SITE_METADATA_PATH = FRONTEND_GENERATED_DIR / "site-metadata.json"
LOCAL_TIMEZONE = ZoneInfo("America/New_York")


def fetch_site_metadata(conn) -> dict[str, str | int | None]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) AS row_count,
                MAX(fight_date) AS latest_fight_date,
                MAX(event_name) FILTER (WHERE fight_date = (SELECT MAX(fight_date) FROM public.historical_predictions))
                    AS latest_event_name
            FROM public.historical_predictions
            """
        )
        row = cur.fetchone()

    latest_fight_date = row[1].isoformat() if row and row[1] is not None else None
    latest_event_name = row[2] if row and row[2] is not None else None
    row_count = int(row[0]) if row and row[0] is not None else 0

    return {
        "last_updated": datetime.now(LOCAL_TIMEZONE).isoformat(),
        "latest_fight_date": latest_fight_date,
        "latest_event_name": latest_event_name,
        "historical_prediction_rows": row_count,
    }


def ensure_generated_dir() -> None:
    FRONTEND_GENERATED_DIR.mkdir(parents=True, exist_ok=True)


def write_bytes(path: Path, payload: bytes) -> None:
    path.write_bytes(payload)


def write_json(path: Path, payload: dict[str, str | int | None]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def publish_static_assets() -> dict[str, str]:
    ensure_generated_dir()

    conninfo = get_database_conninfo(required=True)
    with psycopg.connect(conninfo) as conn:
        average_return_chart = render_average_return_chart(conn)
        suggested_bet_confidence_chart = render_suggested_bet_confidence_chart(conn)
        top_events_chart = render_top_betting_events_chart(conn)
        site_metadata = fetch_site_metadata(conn)

    write_bytes(AVERAGE_RETURN_CHART_PATH, average_return_chart)
    write_bytes(SUGGESTED_BET_CONFIDENCE_CHART_PATH, suggested_bet_confidence_chart)
    write_bytes(TOP_EVENTS_CHART_PATH, top_events_chart)
    write_json(SITE_METADATA_PATH, site_metadata)

    return {
        "average_return_chart": os.fspath(AVERAGE_RETURN_CHART_PATH),
        "suggested_bet_confidence_chart": os.fspath(SUGGESTED_BET_CONFIDENCE_CHART_PATH),
        "top_events_chart": os.fspath(TOP_EVENTS_CHART_PATH),
        "site_metadata": os.fspath(SITE_METADATA_PATH),
    }


def main() -> None:
    outputs = publish_static_assets()
    print(f"average_return_chart: {outputs['average_return_chart']}")
    print(f"suggested_bet_confidence_chart: {outputs['suggested_bet_confidence_chart']}")
    print(f"top_events_chart: {outputs['top_events_chart']}")
    print(f"site_metadata: {outputs['site_metadata']}")


if __name__ == "__main__":
    main()
