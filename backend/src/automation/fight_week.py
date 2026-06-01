from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from html import escape
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from shared.config import get_setting
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
MAILING_DIR = GENERATED_DIR / "mailings"
ARCHIVE_DIR = MAILING_DIR / "archive"
QUEUE_PATH = MAILING_DIR / "queue.json"
SEND_WINDOW_GRACE_MINUTES = 5

ReportRunType = Literal["early", "late"]


@dataclass(frozen=True)
class MailchimpConfig:
    api_key: str | None
    server_prefix: str | None
    list_id: str | None
    from_name: str
    reply_to: str | None
    report_tag: str
    dry_run: bool


def parse_bool_setting(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def get_mailchimp_config() -> MailchimpConfig:
    return MailchimpConfig(
        api_key=get_setting("MAILCHIMP_API_KEY"),
        server_prefix=get_setting("MAILCHIMP_SERVER_PREFIX"),
        list_id=get_setting("MAILCHIMP_LIST_ID"),
        from_name=get_setting("MAILCHIMP_FROM_NAME", "Fight Result Predictor") or "Fight Result Predictor",
        reply_to=get_setting("MAILCHIMP_REPLY_TO"),
        report_tag=get_setting("MAILCHIMP_REPORT_TAG", "fight-week-report") or "fight-week-report",
        dry_run=parse_bool_setting(get_setting("MAILCHIMP_DRY_RUN"), default=True),
    )


def coerce_eastern(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(EASTERN)
    if value.tzinfo is None:
        return value.replace(tzinfo=EASTERN)
    return value.astimezone(EASTERN)


def build_default_send_time(now_eastern: datetime) -> datetime:
    return datetime.combine(now_eastern.date(), time(12, 0), tzinfo=EASTERN)


def is_missed_send_window(target_send_at: datetime, now_eastern: datetime) -> bool:
    return now_eastern > target_send_at + timedelta(minutes=SEND_WINDOW_GRACE_MINUTES)


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


def build_report_html(event_name: str, event_date: date, run_type: ReportRunType, rows: list[dict[str, Any]]) -> str:
    row_html = "\n".join(
        f"""
        <div class="fight-table-row">
          <div class="fight-table-item fighter-item">
            <div class="fighter-row">
              <span class="corner-label red-corner">Red Corner</span>
              <span class="fighter-name">{escape(row['red_fighter'])}</span>
            </div>
            <div class="fighter-row">
              <span class="corner-label blue-corner">Blue Corner</span>
              <span class="fighter-name">{escape(row['blue_fighter'])}</span>
            </div>
          </div>

          <div class="fight-table-item odds-item">
            <span>{escape(format_odds(row['red_odds']))}</span>
            <span>{escape(format_odds(row['blue_odds']))}</span>
          </div>

          <div class="fight-table-item data-item" data-label="Weight">
            {escape(row['weight_class'])}
          </div>

          <div class="fight-table-item data-item" data-label="Winner?">
            {escape(row['predicted_winner'])}
          </div>

          <div class="fight-table-item data-item" data-label="Confidence">
            {escape(format_confidence(row['confidence']))}
          </div>

          <div class="fight-table-item data-item" data-label="Pick/Pass">
            {escape(row['recommended_bet'])}
          </div>
        </div>
        """
        for row in rows
    )

    return f"""<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{escape(event_name)}</title>
    <style>
      @import url("https://fonts.googleapis.com/css2?family=Raleway:wght@200;400;500;600;700&display=swap");

      :root {{
        --bg-color: #2c2f33;
        --header-color: #222529;
        --panel-color: #34393f;
        --panel-color-2: #3b4047;
        --border-color: #4e545c;
        --text-color: #e7e9ec;
        --paragraph-color: #e3e5e5;
        --accent-color: #f08a24;
        --red-color: #ec5a5a;
        --blue-color: #6aa8ff;
        --shadow-color: 0 12px 24px rgba(0, 0, 0, 0.22);
      }}

      * {{
        box-sizing: border-box;
      }}

      body {{
        margin: 0;
        background-color: var(--bg-color);
        color: var(--text-color);
        font-family: Raleway, Arial, sans-serif;
      }}

      .home-screen {{
        width: 100%;
        min-height: 100vh;
        padding-top: 112px;
        padding-bottom: 32px;
      }}

      .home-container {{
        max-width: 1100px;
        margin-left: auto;
        margin-right: auto;
        padding-left: 24px;
        padding-right: 24px;
        display: flex;
        flex-direction: column;
        gap: 24px;
      }}

      .home-heading-row {{
        display: flex;
        justify-content: space-between;
        align-items: flex-end;
        gap: 24px;
      }}

      .home-greeting-text {{
        font-size: 14px;
        line-height: 20px;
        letter-spacing: 0.16em;
        text-transform: uppercase;
        color: var(--accent-color);
        margin: 0 0 8px;
      }}

      .home-title-text {{
        margin: 0;
        font-size: 34px;
        line-height: 40px;
        font-weight: bold;
      }}

      .home-bio-text {{
        margin: 0;
        font-size: 18px;
        line-height: 28px;
        color: var(--paragraph-color);
        max-width: 460px;
        text-align: right;
      }}

      .fight-table-shell {{
        width: 100%;
        border-radius: 20px;
        overflow: hidden;
        border: 1px solid rgba(255, 255, 255, 0.08);
        box-shadow: var(--shadow-color);
      }}

      .fight-table-header {{
        display: grid;
        grid-template-columns: minmax(250px, 2fr) minmax(120px, 0.9fr) minmax(130px, 1fr) minmax(130px, 1fr) minmax(120px, 0.9fr) minmax(110px, 0.9fr);
        gap: 16px;
        padding: 16px 20px;
        background-color: var(--header-color);
        font-size: 14px;
        line-height: 20px;
        font-weight: bold;
        text-transform: uppercase;
      }}

      .fight-table-body {{
        display: flex;
        flex-direction: column;
      }}

      .fight-table-row {{
        display: grid;
        grid-template-columns: minmax(250px, 2fr) minmax(120px, 0.9fr) minmax(130px, 1fr) minmax(130px, 1fr) minmax(120px, 0.9fr) minmax(110px, 0.9fr);
        gap: 16px;
        padding: 18px 20px;
        align-items: center;
        background-color: var(--panel-color);
        border-top: 1px solid rgba(255, 255, 255, 0.05);
      }}

      .fight-table-row:nth-child(even) {{
        background-color: var(--panel-color-2);
      }}

      .fight-table-item {{
        min-width: 0;
      }}

      .fighter-item {{
        display: flex;
        flex-direction: column;
        gap: 10px;
      }}

      .fighter-row,
      .odds-item {{
        display: flex;
        flex-direction: column;
        gap: 2px;
      }}

      .corner-label {{
        font-size: 12px;
        line-height: 16px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }}

      .red-corner {{
        color: var(--red-color);
      }}

      .blue-corner {{
        color: var(--blue-color);
      }}

      .fighter-name {{
        font-size: 16px;
        line-height: 24px;
      }}

      .odds-item,
      .data-item {{
        color: var(--paragraph-color);
      }}

      @media (max-width: 900px) {{
        .home-heading-row {{
          flex-direction: column;
          align-items: flex-start;
        }}

        .home-bio-text {{
          max-width: none;
          text-align: left;
        }}

        .fight-table-header {{
          display: none;
        }}

        .fight-table-row {{
          grid-template-columns: repeat(2, minmax(0, 1fr));
        }}

        .fight-table-item::before {{
          display: block;
          margin-bottom: 6px;
          font-size: 12px;
          line-height: 16px;
          text-transform: uppercase;
          letter-spacing: 0.08em;
          color: var(--text-color);
        }}

        .fighter-item::before {{
          content: "Fighter";
        }}

        .odds-item::before {{
          content: "Odds";
        }}

        .data-item::before {{
          content: attr(data-label);
        }}
      }}

      @media (max-width: 640px) {{
        .home-screen {{
          padding-top: 130px;
        }}

        .home-container {{
          padding-left: 16px;
          padding-right: 16px;
        }}

        .home-title-text {{
          font-size: 28px;
          line-height: 34px;
        }}

        .home-bio-text {{
          font-size: 16px;
          line-height: 24px;
        }}

        .fight-table-row {{
          grid-template-columns: 1fr;
        }}
      }}
    </style>
  </head>
  <body>
    <div class="home-screen">
      <div class="home-container">
        <div class="home-heading-row">
          <div>
            <p class="home-greeting-text">Upcoming predictions</p>
            <h1 class="home-title-text">{escape(event_name or "Upcoming UFC Event")}</h1>
          </div>

          <p class="home-bio-text">Odds will, of course, change throughout fight week</p>
        </div>

        <div class="fight-table-shell">
          <div class="fight-table-header">
            <div class="fight-table-header-item">Fighter</div>
            <div class="fight-table-header-item">Odds</div>
            <div class="fight-table-header-item">Weight</div>
            <div class="fight-table-header-item">Winner?</div>
            <div class="fight-table-header-item">Confidence</div>
            <div class="fight-table-header-item">Pick/Pass</div>
          </div>

          <div class="fight-table-body">{row_html}</div>
        </div>
      </div>
    </div>
  </body>
</html>
"""


def build_report_text(event_name: str, event_date: date, run_type: ReportRunType, rows: list[dict[str, Any]]) -> str:
    report_label = "Early" if run_type == "early" else "Late"
    lines = [f"{report_label} Fight Week Predictions", event_name, event_date.isoformat(), ""]
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
    run_type: ReportRunType,
    rows: list[dict[str, Any]],
    generated_at: datetime,
) -> dict[str, str]:
    MAILING_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = generated_at.strftime("%Y%m%d_%H%M%S")
    archive_stem = f"{timestamp}_{run_type}"

    csv_df = pd.DataFrame(rows)
    latest_csv = MAILING_DIR / "latest.csv"
    archive_csv = ARCHIVE_DIR / f"{archive_stem}.csv"
    csv_df.to_csv(latest_csv, index=False)
    csv_df.to_csv(archive_csv, index=False)

    html = build_report_html(event_name, event_date, run_type, rows)
    text = build_report_text(event_name, event_date, run_type, rows)

    latest_html = MAILING_DIR / "latest.html"
    latest_text = MAILING_DIR / "latest.txt"
    archive_html = ARCHIVE_DIR / f"{archive_stem}.html"
    archive_text = ARCHIVE_DIR / f"{archive_stem}.txt"

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


def mailchimp_request(config: MailchimpConfig, method: str, path: str, payload: dict[str, Any]) -> dict[str, Any]:
    if not config.api_key or not config.server_prefix:
        raise RuntimeError("MAILCHIMP_API_KEY and MAILCHIMP_SERVER_PREFIX are required when dry run is disabled.")

    url = f"https://{config.server_prefix}.api.mailchimp.com/3.0{path}"
    response = requests.request(
        method,
        url,
        auth=("fight-result-predictor", config.api_key),
        json=payload,
        timeout=30,
    )
    if response.status_code >= 400:
        try:
            detail = response.json()
        except ValueError:
            detail = response.text
        raise RuntimeError(f"Mailchimp API request failed: {response.status_code} {detail}")
    return response.json() if response.content else {}


def create_or_schedule_mailchimp_campaign(
    config: MailchimpConfig,
    event_name: str,
    run_type: ReportRunType,
    html: str,
    text: str,
    send_at: datetime,
) -> dict[str, Any]:
    report_label = "Early" if run_type == "early" else "Late"
    subject = f"{report_label} UFC picks: {event_name}"
    if config.dry_run:
        return {
            "campaign_id": f"dry-run-{send_at.strftime('%Y%m%d%H%M%S')}-{run_type}",
            "status": "dry_run",
            "subject": subject,
            "scheduled_for": send_at.isoformat(),
        }

    if not config.list_id or not config.reply_to:
        raise RuntimeError("MAILCHIMP_LIST_ID and MAILCHIMP_REPLY_TO are required when dry run is disabled.")

    campaign = mailchimp_request(
        config,
        "POST",
        "/campaigns",
        {
            "type": "regular",
            "recipients": {"list_id": config.list_id},
            "settings": {
                "subject_line": subject,
                "preview_text": "Model picks for this weekend's UFC card.",
                "from_name": config.from_name,
                "reply_to": config.reply_to,
                "title": f"{config.report_tag} {run_type} {send_at.date().isoformat()}",
            },
        },
    )
    campaign_id = campaign["id"]
    mailchimp_request(config, "PUT", f"/campaigns/{campaign_id}/content", {"html": html, "plain_text": text})
    schedule_time = send_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    mailchimp_request(config, "POST", f"/campaigns/{campaign_id}/actions/schedule", {"schedule_time": schedule_time})
    return {
        "campaign_id": campaign_id,
        "status": "scheduled",
        "subject": subject,
        "scheduled_for": send_at.isoformat(),
    }


def append_queue_record(record: dict[str, Any]) -> None:
    MAILING_DIR.mkdir(parents=True, exist_ok=True)
    if QUEUE_PATH.exists():
        queue = json.loads(QUEUE_PATH.read_text(encoding="utf-8"))
    else:
        queue = []
    queue.append(record)
    QUEUE_PATH.write_text(json.dumps(queue, indent=2, default=str), encoding="utf-8")


def build_skip_record(
    run_type: ReportRunType,
    status: str,
    reason: str,
    event: dict[str, Any] | None,
    generated_at: datetime,
    send_at: datetime,
) -> dict[str, Any]:
    return {
        "run_type": run_type,
        "status": status,
        "reason": reason,
        "error_message": None,
        "event_name": event.get("event_name") if event else None,
        "event_date": event.get("event_date").isoformat() if event else None,
        "generated_at": generated_at.isoformat(),
        "scheduled_for": send_at.isoformat(),
    }


def run_fight_week_report(
    conn,
    run_type: ReportRunType,
    scheduled_for: datetime | None = None,
) -> dict[str, Any]:
    generated_at = datetime.now(EASTERN)
    send_at = coerce_eastern(scheduled_for) if scheduled_for else build_default_send_time(generated_at)
    config = get_mailchimp_config()

    if is_missed_send_window(send_at, generated_at) and not config.dry_run:
        record = build_skip_record(
            run_type,
            "skipped_missed_send_window",
            "The local job ran after the scheduled send window.",
            None,
            generated_at,
            send_at,
        )
        append_queue_record(record)
        return record

    event = check_nearest_event(send_at.date())
    event_date = event["event_date"]
    event_name = event["event_name"]

    if not is_fight_weekend(event_date, send_at.date()):
        record = build_skip_record(
            run_type,
            "skipped_no_fight_weekend",
            "The nearest UFC event is not on Friday, Saturday, or Sunday of this run week.",
            event,
            generated_at,
            send_at,
        )
        append_queue_record(record)
        return record

    scrape_summary = run_upcoming_scrape(send_at.date())
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

    paths = save_report_files(event_name, event_date, run_type, report_rows, generated_at)
    html = Path(paths["latest_html"]).read_text(encoding="utf-8")
    text = Path(paths["latest_text"]).read_text(encoding="utf-8")
    campaign = create_or_schedule_mailchimp_campaign(config, event_name, run_type, html, text, send_at)

    record = {
        "run_type": run_type,
        "status": campaign["status"],
        "event_name": event_name,
        "event_date": event_date.isoformat(),
        "generated_at": generated_at.isoformat(),
        "scheduled_for": send_at.isoformat(),
        "mailchimp_campaign_id": campaign["campaign_id"],
        "mailchimp_subject": campaign["subject"],
        "error_message": None,
        "dry_run": config.dry_run,
        "report_paths": paths,
        "scrape_summary": scrape_summary,
        "fights_load": fights_load,
        "metadata_load": metadata_load,
        "prediction_count": prediction_count,
        "predicted_fights": predicted_fights,
        "queue_path": str(QUEUE_PATH),
    }
    append_queue_record(record)
    return record
