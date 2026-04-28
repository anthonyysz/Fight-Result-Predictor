from __future__ import annotations

import os
from typing import Any

import pandas as pd
from fastapi import HTTPException

from historical_scraper.core.csv_manager import RECENT_COLUMNS
from historical_scraper.core.utils import american_profit_multiple
from historical_scraper.sources.ufcstats_scraper import parse_fight_detail
from upcoming_scraper.core.csv_manager import UPCOMING_FIGHTS_CSV_PATH
from upcoming_scraper.sources.ufcstats_scraper import create_session


BACKEND_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
UPCOMING_METADATA_CSV_PATH = os.path.join(
    BACKEND_DIR, "data", "generated", "upcoming_scraper", "upcoming_fights_metadata.csv"
)

UPCOMING_CSV_TO_DB_COLUMNS = [
    ("RedFighter", "red_fighter"),
    ("BlueFighter", "blue_fighter"),
    ("RedOdds", "red_odds"),
    ("BlueOdds", "blue_odds"),
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

UPCOMING_METADATA_CSV_TO_DB_COLUMNS = [
    ("Date", "fight_date"),
    ("RedFighter", "red_fighter"),
    ("BlueFighter", "blue_fighter"),
    ("event_name", "event_name"),
    ("event_url", "event_url"),
    ("fight_url", "fight_url"),
]

UPCOMING_METADATA_COLUMNS = [csv_column for csv_column, _ in UPCOMING_METADATA_CSV_TO_DB_COLUMNS]

UPCOMING_DB_COLUMNS = [db_column for _, db_column in UPCOMING_CSV_TO_DB_COLUMNS] + ["source_name"]
UPCOMING_METADATA_DB_COLUMNS = [db_column for _, db_column in UPCOMING_METADATA_CSV_TO_DB_COLUMNS] + ["source_name"]


def build_upsert_sql(table_name: str, columns: list[str], constraint_name: str) -> str:
    return (
        f"INSERT INTO public.{table_name} ({', '.join(columns)}) "
        f"VALUES ({', '.join(['%s'] * len(columns))}) "
        f"ON CONFLICT ON CONSTRAINT {constraint_name} DO NOTHING"
    )


UPSERT_UPCOMING = build_upsert_sql(
    table_name="upcoming_fights",
    columns=UPCOMING_DB_COLUMNS,
    constraint_name="upcoming_fights_unique_fight",
)

UPSERT_UPCOMING_METADATA = build_upsert_sql(
    table_name="upcoming_metadata",
    columns=UPCOMING_METADATA_DB_COLUMNS,
    constraint_name="upcoming_metadata_unique_fight",
)


def build_upcoming_metadata(initial_rows: list[dict[str, Any]]) -> str:
    metadata_rows = [
        {
            "Date": row["fight_date"],
            "RedFighter": row["red_fighter"],
            "BlueFighter": row["blue_fighter"],
            "event_name": row["event_name"],
            "event_url": row["event_url"],
            "fight_url": row["fight_url"],
        }
        for row in initial_rows
    ]
    metadata_df = pd.DataFrame(metadata_rows, columns=UPCOMING_METADATA_COLUMNS)
    metadata_df.to_csv(UPCOMING_METADATA_CSV_PATH, index=False)
    return UPCOMING_METADATA_CSV_PATH


def load_upcoming_with_metadata() -> pd.DataFrame:
    if not os.path.exists(UPCOMING_FIGHTS_CSV_PATH):
        raise HTTPException(status_code=404, detail=f"Missing upcoming fights CSV: {UPCOMING_FIGHTS_CSV_PATH}")
    if not os.path.exists(UPCOMING_METADATA_CSV_PATH):
        raise HTTPException(status_code=404, detail=f"Missing upcoming fight metadata CSV: {UPCOMING_METADATA_CSV_PATH}")

    upcoming_df = pd.read_csv(UPCOMING_FIGHTS_CSV_PATH)
    metadata_df = pd.read_csv(UPCOMING_METADATA_CSV_PATH)

    upcoming_df = upcoming_df.copy()
    upcoming_df["Date"] = pd.to_datetime(upcoming_df["Date"]).dt.date
    metadata_df["Date"] = pd.to_datetime(metadata_df["Date"]).dt.date

    merged_df = upcoming_df.merge(
        metadata_df,
        on=["Date", "RedFighter", "BlueFighter"],
        how="left",
        validate="one_to_one",
    )

    if merged_df["fight_url"].isna().any():
        missing_rows = merged_df.loc[merged_df["fight_url"].isna(), ["Date", "RedFighter", "BlueFighter"]]
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Upcoming metadata is missing one or more fight URLs",
                "missing_fights": [
                    f"{row.Date.isoformat()} | {row.RedFighter} vs {row.BlueFighter}" for row in missing_rows.itertuples()
                ],
            },
        )

    return merged_df


def finish_upcoming_fights() -> tuple[pd.DataFrame, list[str]]:
    merged_df = load_upcoming_with_metadata()
    session = create_session()

    completed_rows: list[dict[str, Any]] = []
    completed_fights: list[str] = []
    pending_fights: list[str] = []
    unsupported_results: list[str] = []

    for row in merged_df.itertuples(index=False):
        fight_detail = parse_fight_detail(session, row.fight_url)
        identifier = f"{row.Date.isoformat()} | {row.RedFighter} vs {row.BlueFighter}"
        red_status = fight_detail.get("red_status", "")
        blue_status = fight_detail.get("blue_status", "")

        if not red_status and not blue_status and not fight_detail.get("method"):
            pending_fights.append(identifier)
            continue
        if red_status != "W" and blue_status != "W":
            unsupported_results.append(f"{identifier} ({red_status or 'unknown'} / {blue_status or 'unknown'})")
            continue

        row_dict = row._asdict()
        red_winner = red_status == "W"
        row_dict["RedWinner"] = red_winner
        row_dict["RedReturn"] = american_profit_multiple(row_dict["RedOdds"], red_winner)
        row_dict["BlueReturn"] = american_profit_multiple(row_dict["BlueOdds"], not red_winner)
        completed_rows.append({column: row_dict[column] for column in RECENT_COLUMNS})
        completed_fights.append(identifier)

    if pending_fights:
        raise HTTPException(
            status_code=409,
            detail={"message": "One or more upcoming fights are not finished on UFC Stats yet", "pending_fights": pending_fights},
        )
    if unsupported_results:
        raise HTTPException(
            status_code=409,
            detail={"message": "One or more fights do not have a supported winner outcome", "unsupported_fights": unsupported_results},
        )

    completed_df = pd.DataFrame(completed_rows, columns=RECENT_COLUMNS)
    completed_df.to_csv(UPCOMING_FIGHTS_CSV_PATH, index=False)
    return completed_df, completed_fights
