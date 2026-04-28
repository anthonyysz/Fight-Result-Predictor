from __future__ import annotations

import os
from datetime import date
from typing import Any, Literal

import pandas as pd
import psycopg
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel

from historical_scraper.core.csv_manager import RECENT_COLUMNS
from historical_scraper.core.utils import parse_us_date
from historical_scraper.main import DEFAULT_START_DATE, lookup_database_latest_fight_date, read_dotenv, run_recent_scrape
from upcoming_scraper.main import run_upcoming_scrape
from upcoming_scraper.loaders import (
    UPCOMING_CSV_TO_DB_COLUMNS,
    UPCOMING_METADATA_COLUMNS,
    UPCOMING_METADATA_CSV_PATH,
    UPCOMING_METADATA_CSV_TO_DB_COLUMNS,
    UPCOMING_FIGHTS_CSV_PATH,
    UPSERT_UPCOMING,
    UPSERT_UPCOMING_METADATA,
    build_upcoming_metadata,
    finish_upcoming_fights,
)


APP_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.normpath(os.path.join(APP_DIR, "..", ".."))
SQL_ENV_PATH = os.path.join(BACKEND_DIR, ".env")
RECENT_FIGHTS_CSV_PATH = os.path.join(BACKEND_DIR, "data", "generated", "historical_scraper", "recent_fights.csv")
TESTING_CSV_PATH = os.path.normpath(os.path.join(BACKEND_DIR, "..", "notebooks", "data", "testing.csv"))
CSV_TO_DB_COLUMNS = [
    ("RedFighter", "red_fighter"),
    ("BlueFighter", "blue_fighter"),
    ("RedOdds", "red_odds"),
    ("BlueOdds", "blue_odds"),
    ("RedWinner", "red_winner"),
    ("RedReturn", "red_return"),
    ("BlueReturn", "blue_return"),
    ("OddsDiff", "odds_diff"),
    ("AgeDiff", "age_diff"),
    ("ReachDiff", "reach_diff"),
    ("HeightDiff", "height_diff"),
    ("WinsDiff", "wins_diff"),
    ("LossesDiff", "losses_diff"),
    ("RoundsDiff", "rounds_diff"),
    ("TitleBoutDiff", "title_bout_diff"),
    ("KODiff", "ko_diff"),
    ("SubmissionDiff", "submission_diff"),
    ("WinStreakDiff", "win_streak_diff"),
    ("LoseStreakDiff", "lose_streak_diff"),
    ("LongestWinStreakDiff", "longest_win_streak_diff"),
    ("SigStrDiff", "sig_str_diff"),
    ("SubAttDiff", "sub_att_diff"),
    ("TDDiff", "td_diff"),
    ("RankDiff", "rank_diff"),
    ("Date", "fight_date"),
    ("TitleBout", "title_bout"),
    ("WeightClass", "weight_class"),
    ("Gender", "gender"),
    ("NumberOfRounds", "number_of_rounds"),
    ("BlueCurrentLoseStreak", "blue_current_lose_streak"),
    ("BlueCurrentWinStreak", "blue_current_win_streak"),
    ("BlueLongestWinStreak", "blue_longest_win_streak"),
    ("BlueLosses", "blue_losses"),
    ("BlueTotalRoundsFought", "blue_total_rounds_fought"),
    ("BlueTotalTitleBouts", "blue_total_title_bouts"),
    ("BlueWinsByKO", "blue_wins_by_ko"),
    ("BlueWinsBySubmission", "blue_wins_by_submission"),
    ("BlueWins", "blue_wins"),
    ("BlueStance", "blue_stance"),
    ("BlueHeightCms", "blue_height_cms"),
    ("BlueReachCms", "blue_reach_cms"),
    ("RedCurrentLoseStreak", "red_current_lose_streak"),
    ("RedCurrentWinStreak", "red_current_win_streak"),
    ("RedLongestWinStreak", "red_longest_win_streak"),
    ("RedLosses", "red_losses"),
    ("RedTotalRoundsFought", "red_total_rounds_fought"),
    ("RedTotalTitleBouts", "red_total_title_bouts"),
    ("RedWinsByKO", "red_wins_by_ko"),
    ("RedWinsBySubmission", "red_wins_by_submission"),
    ("RedWins", "red_wins"),
    ("RedStance", "red_stance"),
    ("RedHeightCms", "red_height_cms"),
    ("RedReachCms", "red_reach_cms"),
    ("RedAge", "red_age"),
    ("BlueAge", "blue_age"),
    ("BMatchWCRank", "b_match_wc_rank"),
    ("RMatchWCRank", "r_match_wc_rank"),
]

DB_COLUMNS = [db_column for _, db_column in CSV_TO_DB_COLUMNS] + ["source_name"]
UPSERT_SQL = (
    f"INSERT INTO public.all_fights ({', '.join(DB_COLUMNS)}) "
    f"VALUES ({', '.join(['%s'] * len(DB_COLUMNS))}) "
    "ON CONFLICT ON CONSTRAINT all_fights_unique_fight DO NOTHING"
)

LOAD_CONFIG = {
    "recent": {
        "csv_path": RECENT_FIGHTS_CSV_PATH,
        "expected_columns": [csv for csv, _ in CSV_TO_DB_COLUMNS],
        "column_mapping": CSV_TO_DB_COLUMNS,
        "upsert_sql": UPSERT_SQL,
        "table_kind": "all_fights",
    },
    "testing": {
        "csv_path": TESTING_CSV_PATH,
        "expected_columns": RECENT_COLUMNS,
        "column_mapping": CSV_TO_DB_COLUMNS,
        "upsert_sql": UPSERT_SQL,
        "table_kind": "all_fights",
    },
    "upcoming_fights": {
        "csv_path": UPCOMING_FIGHTS_CSV_PATH,
        "expected_columns": [csv for csv, _ in UPCOMING_CSV_TO_DB_COLUMNS],
        "column_mapping": UPCOMING_CSV_TO_DB_COLUMNS,
        "upsert_sql": UPSERT_UPCOMING,
        "table_kind": "upcoming_fights",
    },
    "upcoming_metadata": {
        "csv_path": UPCOMING_METADATA_CSV_PATH,
        "expected_columns": [csv for csv, _ in UPCOMING_METADATA_CSV_TO_DB_COLUMNS],
        "column_mapping": UPCOMING_METADATA_CSV_TO_DB_COLUMNS,
        "upsert_sql": UPSERT_UPCOMING_METADATA,
        "table_kind": "upcoming_metadata",
    },
}

app = FastAPI(title="Fight Result Predictor Admin API", version="0.1.0")


class HealthResponse(BaseModel):
    status: str


class RecentScrapeRequest(BaseModel):
    start_date: date | None = None


class SourceLoadRequest(BaseModel):
    source: Literal["recent", "upcoming_fights", "testing", "upcoming_metadata"]


class ScrapeResponse(BaseModel):
    message: str
    output_path: str
    row_count: int
    metadata_path: str | None = None
    missing_data_report: str | None = None
    missing_columns_summary: str | None = None
    missing_odds_report: str | None = None
    event_name: str | None = None
    event_date: str | None = None
    start_date: str | None = None
    database_latest_fight_date: str | None = None


class FinishResponse(BaseModel):
    message: str
    output_path: str
    row_count: int
    completed_fights: list[str]


class LoadResponse(BaseModel):
    message: str
    source: str
    csv_path: str
    inserted_count: int
    skipped_duplicate_count: int
    skipped_incomplete_count: int
    duplicate_fights: list[str]
    incomplete_fights: list[str]


def get_conninfo() -> str:
    if not os.path.exists(SQL_ENV_PATH):
        raise HTTPException(status_code=500, detail=f"Missing backend env file at {SQL_ENV_PATH}")

    env_values = read_dotenv(SQL_ENV_PATH)
    database_url = env_values.get("DATABASE_URL")
    if database_url:
        return database_url

    required_keys = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]
    missing = [key for key in required_keys if not env_values.get(key)]
    if missing:
        raise HTTPException(status_code=500, detail=f"Missing database settings: {', '.join(missing)}")

    return (
        f"host={env_values['PGHOST']} "
        f"port={env_values['PGPORT']} "
        f"dbname={env_values['PGDATABASE']} "
        f"user={env_values['PGUSER']} "
        f"password={env_values['PGPASSWORD']}"
    )


def get_db_connection() -> Any:
    try:
        with psycopg.connect(get_conninfo()) as conn:
            yield conn
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {exc}") from exc


def ensure_file_exists(path: str, label: str) -> None:
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail=f"Missing {label}: {path}")


REQUIRED_DB_CSV_COLUMNS = [csv_column for csv_column, _ in CSV_TO_DB_COLUMNS]

def validate_exact_columns(df: pd.DataFrame, expected_columns: list[str], label: str) -> pd.DataFrame:
    actual_columns = list(df.columns)
    if actual_columns != expected_columns:
        raise HTTPException(
            status_code=400,
            detail={
                "message": f"{label} must match the exact expected schema",
                "expected_columns": expected_columns,
                "actual_columns": actual_columns,
            },
        )
    return df.copy()

def build_db_records(
    df: pd.DataFrame,
    column_mapping: list[tuple[str, str]],
    source_name: str,
) -> list[tuple[Any, ...]]:
    records: list[tuple[Any, ...]] = []

    for _, row in df.iterrows():
        values = [to_python_value(row[csv_column]) for csv_column, _ in column_mapping]
        values.append(source_name)
        records.append(tuple(values))

    return records

def build_incomplete_fight_messages(df: pd.DataFrame) -> list[str]:
    incomplete_fights: list[str] = []

    for _, row in df.iterrows():
        missing_columns = [column for column in REQUIRED_DB_CSV_COLUMNS if pd.isna(row[column])]
        if not missing_columns:
            continue

        fight_date = pd.to_datetime(row["Date"]).date().isoformat()
        incomplete_fights.append(
            f"{fight_date} | {row['RedFighter']} vs {row['BlueFighter']} | "
            f"{row['WeightClass']} | missing: {', '.join(missing_columns)}"
        )

    return incomplete_fights


def to_python_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date()
    item = getattr(value, "item", None)
    return item() if callable(item) else value


def build_duplicate_keys(df: pd.DataFrame) -> set[tuple[Any, Any, Any, Any]]:
    key_rows = df[["Date", "RedFighter", "BlueFighter", "WeightClass"]].copy()
    key_rows["Date"] = pd.to_datetime(key_rows["Date"]).dt.date
    return {
        (row.Date, row.RedFighter, row.BlueFighter, row.WeightClass)
        for row in key_rows.itertuples(index=False)
    }


def fetch_existing_fight_keys(conn, dates: list[date]) -> set[tuple[Any, Any, Any, Any]]:
    if not dates:
        return set()

    query = """
        SELECT fight_date, red_fighter, blue_fighter, weight_class
        FROM public.all_fights
        WHERE fight_date = ANY(%s)
    """
    with conn.cursor() as cur:
        cur.execute(query, (dates,))
        return {(row[0], row[1], row[2], row[3]) for row in cur.fetchall()}


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok")


@app.post("/admin/recent-fights/scrape", response_model=ScrapeResponse)
def scrape_recent_fights(payload: RecentScrapeRequest) -> ScrapeResponse:
    start_date = payload.start_date
    if start_date is None:
        latest_fight_date = lookup_database_latest_fight_date()
        start_date = parse_us_date(latest_fight_date) if latest_fight_date else parse_us_date(DEFAULT_START_DATE)

    summary = run_recent_scrape(start_date)
    return ScrapeResponse(
        message="Recent fights CSV refreshed",
        output_path=summary["recent_fights_csv"],
        row_count=summary["fight_rows"],
        missing_data_report=summary["missing_data_report"],
        missing_columns_summary=summary["missing_columns_summary"],
        missing_odds_report=summary["missing_odds_report"],
        start_date=summary["start_date"],
        database_latest_fight_date=summary["database_latest_fight_date"],
    )


@app.post("/admin/upcoming-fights/scrape", response_model=ScrapeResponse)
def scrape_upcoming_fights() -> ScrapeResponse:
    summary = run_upcoming_scrape()
    metadata_path = build_upcoming_metadata(summary["initial_rows"])
    return ScrapeResponse(
        message="Upcoming fights CSV refreshed",
        output_path=summary["upcoming_fights_csv"],
        row_count=summary["fight_rows"],
        metadata_path=metadata_path,
        missing_data_report=summary["missing_data_report"],
        missing_columns_summary=summary["missing_columns_summary"],
        missing_odds_report=summary["missing_odds_report"],
        event_name=summary["event_name"],
        event_date=summary["event_date"],
    )


@app.post("/admin/upcoming-fights/finish", response_model=FinishResponse)
def finish_upcoming_fights_route() -> FinishResponse:
    completed_df, completed_fights = finish_upcoming_fights()
    return FinishResponse(
        message="Upcoming fights CSV finished with results and returns",
        output_path=UPCOMING_FIGHTS_CSV_PATH,
        row_count=len(completed_df),
        completed_fights=completed_fights,
    )


@app.post("/admin/fights/load", response_model=LoadResponse)
def load_fights(payload: SourceLoadRequest, conn=Depends(get_db_connection)) -> LoadResponse:
    config = LOAD_CONFIG[payload.source]
    csv_path = config["csv_path"]
    ensure_file_exists(csv_path, f"{payload.source} CSV")

    df = pd.read_csv(csv_path)

    if config["table_kind"] == "all_fights":
        df = validate_exact_columns(df, config["expected_columns"], f"{payload.source} CSV")
        incomplete_fights = build_incomplete_fight_messages(df)
        complete_mask = df[REQUIRED_DB_CSV_COLUMNS].notna().all(axis=1)
        df = df.loc[complete_mask].copy()

        candidate_keys = build_duplicate_keys(df)
        existing_keys = fetch_existing_fight_keys(conn, sorted({key[0] for key in candidate_keys}))
        duplicate_keys = candidate_keys & existing_keys

        duplicate_fights = [
            f"{fight_date.isoformat()} | {red_fighter} vs {blue_fighter} | {weight_class}"
            for fight_date, red_fighter, blue_fighter, weight_class in sorted(duplicate_keys)
        ]

        non_duplicate_mask = [
            (
                pd.to_datetime(row["Date"]).date(),
                row["RedFighter"],
                row["BlueFighter"],
                row["WeightClass"],
            )
            not in duplicate_keys
            for _, row in df.iterrows()
        ]
        df = df.loc[non_duplicate_mask].copy()
        records = build_db_records(df, config["column_mapping"], os.path.basename(csv_path))

        with conn.cursor() as cur:
            cur.executemany(config["upsert_sql"], records)
            conn.commit()

        return LoadResponse(
            message="Fight CSV load complete",
            source=payload.source,
            csv_path=csv_path,
            inserted_count=len(records),
            skipped_duplicate_count=len(duplicate_fights),
            skipped_incomplete_count=len(incomplete_fights),
            duplicate_fights=duplicate_fights,
            incomplete_fights=incomplete_fights,
        )

    df = validate_exact_columns(df, config["expected_columns"], f"{payload.source} CSV")
    records = build_db_records(df, config["column_mapping"], os.path.basename(csv_path))

    with conn.cursor() as cur:
        cur.executemany(config["upsert_sql"], records)
        conn.commit()

    return LoadResponse(
        message="Fight CSV load complete",
        source=payload.source,
        csv_path=csv_path,
        inserted_count=len(records),
        skipped_duplicate_count=0,
        skipped_incomplete_count=0,
        duplicate_fights=[],
        incomplete_fights=[],
    )
