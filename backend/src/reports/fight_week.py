from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from html import escape
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from upcoming_scraper.main import run_upcoming_scrape
from upcoming_scraper.predictions import generate_upcoming_predictions
from upcoming_scraper.sources.ufcstats_scraper import initialize_upcoming_rows
from upcoming_scraper.loaders import (
    UPCOMING_CSV_TO_DB_COLUMNS,
    UPCOMING_METADATA_CSV_PATH,
    UPCOMING_METADATA_CSV_TO_DB_COLUMNS,
    UPCOMING_FIGHTS_CSV_PATH,
    UPSERT_UPCOMING,
    UPSERT_UPCOMING_METADATA,
)

EASTERN = ZoneInfo("America/New_York")
BACKEND_DIR = Path(__file__).resolve().parents[2]
GENERATED_DIR = BACKEND_DIR / "data" / "generated"
REPORT_DIR = GENERATED_DIR / "reports"
ARCHIVE_DIR = REPORT_DIR / "archive"
HISTORY_PATH = REPORT_DIR / "history.json"


def is_fight_weekend(fight_date: date, reference_date: date) -> bool:
    week_start = reference_date - timedelta(days=reference_date.weekday())
    week_end = week_start + timedelta(days=6)
    return week_start <= fight_date <= week_end and fight_date.weekday() in {4, 5, 6}


def check_nearest_event(reference_date: date) -> dict[str, Any]:
    rows = initialize_upcoming_rows(reference_date)
    if not rows:
        raise RuntimeError("No upcoming fights were found for the nearest UFC event.")
    first = rows[0]
    return {
        "event_name": first["event_name"],
        "event_date": first["fight_date"],
        "fight_count": len(rows),
    }


def to_python_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date()
    item = getattr(value, "item", None)
    return item() if callable(item) else value


def load_generated_csv(
    conn,
    csv_path: str,
    column_mapping: list[tuple[str, str]],
    upsert_sql: str,
) -> dict[str, Any]:
    df = pd.read_csv(csv_path)
    expected_columns = [csv_column for csv_column, _ in column_mapping]
    if list(df.columns) != expected_columns:
        raise RuntimeError(
            f"{Path(csv_path).name} has unexpected columns. "
            f"Expected {expected_columns}, got {list(df.columns)}."
        )

    incomplete_rows: list[str] = []
    for _, row in df.iterrows():
        missing_columns = [column for column in expected_columns if pd.isna(row[column])]
        if not missing_columns:
            continue
        date_value = pd.to_datetime(row["Date"]).date().isoformat()
        incomplete_rows.append(
            f"{date_value} | {row['RedFighter']} vs {row['BlueFighter']} | "
            f"missing: {', '.join(missing_columns)}"
        )

    complete_df = df.loc[df[expected_columns].notna().all(axis=1)].copy()
    records: list[tuple[Any, ...]] = []
    for _, row in complete_df.iterrows():
        values = [to_python_value(row[csv_column]) for csv_column, _ in column_mapping]
        values.append(Path(csv_path).name)
        records.append(tuple(values))

    with conn.cursor() as cur:
        if records:
            cur.executemany(upsert_sql, records)
    conn.commit()

    return {
        "csv_path": csv_path,
        "loaded_count": len(records),
        "skipped_incomplete_count": len(incomplete_rows),
        "incomplete_rows": incomplete_rows,
    }


def fetch_report_rows(conn, event_date: date, event_name: str) -> list[dict[str, Any]]:
    query = """
        SELECT
            p.fight_date,
            m.event_name,
            p.red_fighter,
            p.blue_fighter,
            f.red_odds,
            f.blue_odds,
            p.weight_class,
            p.predicted_winner,
            p.confidence,
            p.expected_value_red,
            p.expected_value_blue,
            p.recommended_bet
        FROM public.upcoming_predictions p
        INNER JOIN public.upcoming_fights f
            ON p.fight_date = f.fight_date
            AND p.red_fighter = f.red_fighter
            AND p.blue_fighter = f.blue_fighter
            AND p.weight_class = f.weight_class
        INNER JOIN public.upcoming_metadata m
            ON p.fight_date = m.fight_date
            AND p.red_fighter = m.red_fighter
            AND p.blue_fighter = m.blue_fighter
        WHERE p.fight_date = %s
          AND m.event_name = %s
        ORDER BY p.fight_date, p.red_fighter, p.blue_fighter
    """
    with conn.cursor() as cur:
        cur.execute(query, (event_date, event_name))
        rows = cur.fetchall()
        columns = [column.name for column in cur.description]
    return [dict(zip(columns, row)) for row in rows]


def format_odds(value: Any) -> str:
    odds = int(value)
    return f"+{odds}" if odds > 0 else str(odds)


def format_confidence(value: Any) -> str:
    return f"{round(float(value) * 100)}%"


def format_expected_value(value: Any) -> str:
    return f"{float(value):.2f}"


def build_report_html(event_name: str, event_date: date, rows: list[dict[str, Any]]) -> str:
    font = "Raleway, Arial, sans-serif"
    bg_color = "#2c2f33"
    header_color = "#222529"
    panel_color = "#34393f"
    panel_color_2 = "#3b4047"
    text_color = "#e7e9ec"
    paragraph_color = "#e3e5e5"
    accent_color = "#f08a24"
    red_color = "#ec5a5a"
    blue_color = "#6aa8ff"
    border_color = "#4e545c"

    row_html_parts: list[str] = []
    for index, row in enumerate(rows):
        row_bg = panel_color if index % 2 == 0 else panel_color_2
        row_html_parts.append(
            f"""
            <tr>
              <td style="padding:18px 20px;background-color:{row_bg};border-top:1px solid #42474e;">
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
                  <tr>
                    <td width="250" style="width:250px;padding:0 16px 0 0;vertical-align:middle;">
                      <div style="margin:0 0 10px;">
                        <div style="font-family:{font};font-size:12px;line-height:16px;text-transform:uppercase;letter-spacing:0.08em;color:{red_color};">Red Corner</div>
                        <div style="font-family:{font};font-size:16px;line-height:24px;color:{text_color};">{escape(row['red_fighter'])}</div>
                      </div>
                      <div>
                        <div style="font-family:{font};font-size:12px;line-height:16px;text-transform:uppercase;letter-spacing:0.08em;color:{blue_color};">Blue Corner</div>
                        <div style="font-family:{font};font-size:16px;line-height:24px;color:{text_color};">{escape(row['blue_fighter'])}</div>
                      </div>
                    </td>
                    <td width="120" style="width:120px;padding:0 16px 0 0;vertical-align:middle;font-family:{font};font-size:16px;line-height:24px;color:{paragraph_color};">
                      {escape(format_odds(row['red_odds']))}<br>{escape(format_odds(row['blue_odds']))}
                    </td>
                    <td width="130" style="width:130px;padding:0 16px 0 0;vertical-align:middle;font-family:{font};font-size:16px;line-height:24px;color:{paragraph_color};">
                      {escape(row['weight_class'])}
                    </td>
                    <td width="130" style="width:130px;padding:0 16px 0 0;vertical-align:middle;font-family:{font};font-size:16px;line-height:24px;color:{paragraph_color};">
                      {escape(row['predicted_winner'])}
                    </td>
                    <td width="120" style="width:120px;padding:0 16px 0 0;vertical-align:middle;font-family:{font};font-size:16px;line-height:24px;color:{paragraph_color};">
                      {escape(format_confidence(row['confidence']))}
                    </td>
                    <td width="110" style="width:110px;padding:0;vertical-align:middle;font-family:{font};font-size:16px;line-height:24px;color:{paragraph_color};">
                      {escape(row['recommended_bet'])}
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
            """
        )

    row_html = "\n".join(row_html_parts)
    return f"""<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{escape(event_name)}</title>
  </head>
  <body style="margin:0;padding:0;background-color:{bg_color};color:{text_color};">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse;background-color:{bg_color};">
      <tr>
        <td align="center" style="padding:112px 24px 32px;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="width:100%;max-width:1100px;border-collapse:collapse;">
            <tr>
              <td style="padding:0 0 24px;">
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse;">
                  <tr>
                    <td style="vertical-align:bottom;padding:0 24px 0 0;">
                      <div style="font-family:{font};font-size:14px;line-height:20px;letter-spacing:0.16em;text-transform:uppercase;color:{accent_color};margin:0 0 8px;">Upcoming predictions</div>
                      <div style="font-family:{font};font-size:34px;line-height:40px;font-weight:700;color:{text_color};margin:0;">{escape(event_name or "Upcoming UFC Event")}</div>
                    </td>
                    <td width="460" style="width:460px;vertical-align:bottom;text-align:right;font-family:{font};font-size:18px;line-height:28px;color:{paragraph_color};">
                      Odds will, of course, change throughout fight week
                    </td>
                  </tr>
                </table>
              </td>
            </tr>

            <tr>
              <td style="border:1px solid {border_color};border-radius:20px;overflow:hidden;">
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse;border-radius:20px;overflow:hidden;">
                  <tr>
                    <td style="background-color:{header_color};padding:16px 20px;">
                      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
                        <tr>
                          <td width="250" style="width:250px;padding:0 16px 0 0;font-family:{font};font-size:14px;line-height:20px;font-weight:700;text-transform:uppercase;color:{text_color};">Fighter</td>
                          <td width="120" style="width:120px;padding:0 16px 0 0;font-family:{font};font-size:14px;line-height:20px;font-weight:700;text-transform:uppercase;color:{text_color};">Odds</td>
                          <td width="130" style="width:130px;padding:0 16px 0 0;font-family:{font};font-size:14px;line-height:20px;font-weight:700;text-transform:uppercase;color:{text_color};">Weight</td>
                          <td width="130" style="width:130px;padding:0 16px 0 0;font-family:{font};font-size:14px;line-height:20px;font-weight:700;text-transform:uppercase;color:{text_color};">Winner?</td>
                          <td width="120" style="width:120px;padding:0 16px 0 0;font-family:{font};font-size:14px;line-height:20px;font-weight:700;text-transform:uppercase;color:{text_color};">Confidence</td>
                          <td width="110" style="width:110px;padding:0;font-family:{font};font-size:14px;line-height:20px;font-weight:700;text-transform:uppercase;color:{text_color};">Pick/Pass</td>
                        </tr>
                      </table>
                    </td>
                  </tr>
                  {row_html}
                </table>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>
"""


def build_report_text(event_name: str, event_date: date, rows: list[dict[str, Any]]) -> str:
    lines = ["Fight Week Predictions", event_name, event_date.isoformat(), ""]
    for row in rows:
        lines.append(
            f"{row['red_fighter']} vs {row['blue_fighter']} | "
            f"{row['weight_class']} | Winner: {row['predicted_winner']} | "
            f"Confidence: {format_confidence(row['confidence'])} | "
            f"Pick/Pass: {row['recommended_bet']}"
        )
    return "\n".join(lines) + "\n"


def save_report_files(
    event_name: str,
    event_date: date,
    rows: list[dict[str, Any]],
    generated_at: datetime,
) -> dict[str, str]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = generated_at.strftime("%Y%m%d_%H%M%S")

    csv_df = pd.DataFrame(rows)
    latest_csv = REPORT_DIR / "latest.csv"
    archive_csv = ARCHIVE_DIR / f"{timestamp}.csv"
    csv_df.to_csv(latest_csv, index=False)
    csv_df.to_csv(archive_csv, index=False)

    html = build_report_html(event_name, event_date, rows)
    text = build_report_text(event_name, event_date, rows)

    latest_html = REPORT_DIR / "latest.html"
    latest_text = REPORT_DIR / "latest.txt"
    archive_html = ARCHIVE_DIR / f"{timestamp}.html"
    archive_text = ARCHIVE_DIR / f"{timestamp}.txt"

    latest_html.write_text(html, encoding="utf-8")
    latest_text.write_text(text, encoding="utf-8")
    archive_html.write_text(html, encoding="utf-8")
    archive_text.write_text(text, encoding="utf-8")

    return {
        "latest_csv": str(latest_csv),
        "latest_html": str(latest_html),
        "latest_text": str(latest_text),
        "archive_csv": str(archive_csv),
        "archive_html": str(archive_html),
        "archive_text": str(archive_text),
    }


def build_report_title(event_name: str) -> str:
    return f"UFC picks: {event_name}"


def append_report_history(record: dict[str, Any]) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    if HISTORY_PATH.exists():
        history = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
    else:
        history = []
    history.append(record)
    HISTORY_PATH.write_text(json.dumps(history, indent=2, default=str), encoding="utf-8")


def build_skip_record(
    status: str,
    reason: str,
    event: dict[str, Any] | None,
    generated_at: datetime,
) -> dict[str, Any]:
    return {
        "status": status,
        "reason": reason,
        "error_message": None,
        "event_name": event.get("event_name") if event else None,
        "event_date": event.get("event_date").isoformat() if event else None,
        "generated_at": generated_at.isoformat(),
    }


def generate_fight_week_report(
    conn,
    reference_date: date | None = None,
) -> dict[str, Any]:
    generated_at = datetime.now(EASTERN)
    report_reference_date = reference_date or generated_at.date()

    event = check_nearest_event(report_reference_date)
    event_date = event["event_date"]
    event_name = event["event_name"]

    if not is_fight_weekend(event_date, report_reference_date):
        record = build_skip_record(
            "skipped_no_fight_weekend",
            "The nearest UFC event is not on Friday, Saturday, or Sunday of this run week.",
            event,
            generated_at,
        )
        append_report_history(record)
        return record

    scrape_summary = run_upcoming_scrape(report_reference_date)
    fights_load = load_generated_csv(conn, UPCOMING_FIGHTS_CSV_PATH, UPCOMING_CSV_TO_DB_COLUMNS, UPSERT_UPCOMING)
    metadata_load = load_generated_csv(
        conn,
        UPCOMING_METADATA_CSV_PATH,
        UPCOMING_METADATA_CSV_TO_DB_COLUMNS,
        UPSERT_UPCOMING_METADATA,
    )
    prediction_count, predicted_fights = generate_upcoming_predictions(conn)
    report_rows = fetch_report_rows(conn, event_date, event_name)
    if not report_rows:
        raise RuntimeError(f"No report rows were available for {event_name} on {event_date.isoformat()}.")

    paths = save_report_files(event_name, event_date, report_rows, generated_at)

    record = {
        "status": "report_generated",
        "event_name": event_name,
        "event_date": event_date.isoformat(),
        "generated_at": generated_at.isoformat(),
        "report_title": build_report_title(event_name),
        "error_message": None,
        "report_paths": paths,
        "scrape_summary": scrape_summary,
        "fights_load": fights_load,
        "metadata_load": metadata_load,
        "prediction_count": prediction_count,
        "predicted_fights": predicted_fights,
        "history_path": str(HISTORY_PATH),
    }
    append_report_history(record)
    return record
