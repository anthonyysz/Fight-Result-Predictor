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

UPCOMING_FIGHT_DB_COLUMNS = UPCOMING_DB_COLUMNS + ["loaded_at"]

ALL_FIGHTS_INSERT_COLUMNS = [
    "red_fighter",
    "blue_fighter",
    "red_odds",
    "blue_odds",
    "red_winner",
    "red_return",
    "blue_return",
    *[
        db_column
        for _, db_column in UPCOMING_CSV_TO_DB_COLUMNS
        if db_column not in {"red_fighter", "blue_fighter", "red_odds", "blue_odds"}
    ],
    "source_name",
]

HISTORICAL_PREDICTION_COLUMNS = [
    *UPCOMING_FIGHT_DB_COLUMNS,
    "model_picked_red_winner",
    "confidence",
    "expected_value_red",
    "expected_value_blue",
    "model_picked_red_bet",
    "weight_class_model_used",
    "estimator",
    "model_params",
    "bet_threshold",
]

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

INSERT_ALL_FIGHTS = build_upsert_sql(
    table_name="all_fights",
    columns=ALL_FIGHTS_INSERT_COLUMNS,
    constraint_name="all_fights_unique_fight",
)

UPSERT_HISTORICAL_PREDICTIONS = (
    f"INSERT INTO public.historical_predictions ({', '.join(HISTORICAL_PREDICTION_COLUMNS)}) "
    f"VALUES ({', '.join(['%s'] * len(HISTORICAL_PREDICTION_COLUMNS))}) "
    "ON CONFLICT ON CONSTRAINT historical_predictions_unique_fight DO NOTHING"
)

DELETE_UPCOMING_FIGHT = """
    DELETE FROM public.upcoming_fights
    WHERE fight_date = %s
      AND red_fighter = %s
      AND blue_fighter = %s
      AND weight_class = %s
"""

DELETE_UPCOMING_METADATA = """
    DELETE FROM public.upcoming_metadata
    WHERE fight_date = %s
      AND red_fighter = %s
      AND blue_fighter = %s
"""

DELETE_UPCOMING_PREDICTION = """
    DELETE FROM public.upcoming_predictions
    WHERE fight_date = %s
      AND red_fighter = %s
      AND blue_fighter = %s
      AND weight_class = %s
"""

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

def to_python_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    item = getattr(value, "item", None)
    return item() if callable(item) else value


def fetch_upcoming_fights_with_metadata(conn) -> pd.DataFrame:
    query = f"""
        SELECT
            {', '.join(f'uf.{column}' for column in UPCOMING_FIGHT_DB_COLUMNS)},
            um.fight_url,
            up.predicted_winner,
            up.confidence,
            up.expected_value_red,
            up.expected_value_blue,
            up.recommended_bet,
            up.weight_class_model_used,
            up.estimator,
            up.model_params,
            up.bet_threshold
        FROM public.upcoming_fights uf
        LEFT JOIN public.upcoming_metadata um
            ON uf.fight_date = um.fight_date
            AND uf.red_fighter = um.red_fighter
            AND uf.blue_fighter = um.blue_fighter
        LEFT JOIN public.upcoming_predictions up
            ON uf.fight_date = up.fight_date
            AND uf.red_fighter = up.red_fighter
            AND uf.blue_fighter = up.blue_fighter
            AND uf.weight_class = up.weight_class
        ORDER BY uf.fight_date, uf.red_fighter, uf.blue_fighter
    """

    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        columns = [column.name for column in cur.description]

    if not rows:
        raise HTTPException(status_code=409, detail="public.upcoming_fights has no rows to finish.")

    merged_df = pd.DataFrame(rows, columns=columns)

    if merged_df["fight_url"].isna().any():
        missing_rows = merged_df.loc[merged_df["fight_url"].isna(), ["fight_date", "red_fighter", "blue_fighter"]]
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Upcoming metadata is missing one or more fight URLs",
                "missing_fights": [
                    f"{row.fight_date.isoformat()} | {row.red_fighter} vs {row.blue_fighter}"
                    for row in missing_rows.itertuples(index=False)
                ],
            },
        )

    return merged_df


def build_fight_identifier(row: dict[str, Any]) -> str:
    return f"{row['fight_date'].isoformat()} | {row['red_fighter']} vs {row['blue_fighter']} | {row['weight_class']}"


def build_upcoming_fight_key(row: dict[str, Any]) -> tuple[Any, Any, Any, Any]:
    return (row["fight_date"], row["red_fighter"], row["blue_fighter"], row["weight_class"])


def build_upcoming_metadata_key(row: dict[str, Any]) -> tuple[Any, Any, Any]:
    return (row["fight_date"], row["red_fighter"], row["blue_fighter"])


def fight_is_pending(fight_detail: dict[str, Any]) -> bool:
    red_status = fight_detail.get("red_status", "")
    blue_status = fight_detail.get("blue_status", "")
    method = fight_detail.get("method", "")
    return not red_status and not blue_status and not method


def fight_has_supported_winner(fight_detail: dict[str, Any]) -> bool:
    red_status = fight_detail.get("red_status", "")
    blue_status = fight_detail.get("blue_status", "")
    return (red_status == "W") ^ (blue_status == "W")


def has_prediction_row(row: dict[str, Any]) -> bool:
    return pd.notna(row.get("predicted_winner"))


def build_all_fights_record(row: dict[str, Any], red_winner: bool) -> tuple[Any, ...]:
    record = {
        "red_fighter": row["red_fighter"],
        "blue_fighter": row["blue_fighter"],
        "red_odds": row["red_odds"],
        "blue_odds": row["blue_odds"],
        "red_winner": red_winner,
        "red_return": american_profit_multiple(row["red_odds"], red_winner),
        "blue_return": american_profit_multiple(row["blue_odds"], not red_winner),
        **{
            db_column: row[db_column]
            for _, db_column in UPCOMING_CSV_TO_DB_COLUMNS
            if db_column not in {"red_fighter", "blue_fighter", "red_odds", "blue_odds"}
        },
        "source_name": row["source_name"],
    }

    return tuple(to_python_value(record[column]) for column in ALL_FIGHTS_INSERT_COLUMNS)


def build_historical_prediction_record(row: dict[str, Any]) -> tuple[Any, ...]:
    record = {
        **{column: row[column] for column in UPCOMING_FIGHT_DB_COLUMNS},
        "model_picked_red_winner": row["predicted_winner"] == row["red_fighter"],
        "confidence": row["confidence"],
        "expected_value_red": row["expected_value_red"],
        "expected_value_blue": row["expected_value_blue"],
        "model_picked_red_bet": row["recommended_bet"] == row["red_fighter"],
        "weight_class_model_used": row["weight_class_model_used"],
        "estimator": row["estimator"],
        "model_params": row["model_params"],
        "bet_threshold": row["bet_threshold"],
    }

    return tuple(to_python_value(record[column]) for column in HISTORICAL_PREDICTION_COLUMNS)


def finish_upcoming_fights(conn) -> dict[str, Any]:
    merged_df = fetch_upcoming_fights_with_metadata(conn)
    session = create_session()

    all_fights_records: list[tuple[Any, ...]] = []
    historical_prediction_records: list[tuple[Any, ...]] = []
    upcoming_fight_deletes: list[tuple[Any, Any, Any, Any]] = []
    upcoming_metadata_deletes: list[tuple[Any, Any, Any]] = []
    upcoming_prediction_deletes: list[tuple[Any, Any, Any, Any]] = []

    moved_fights: list[str] = []
    discarded_fights: list[str] = []
    pending_fights: list[str] = []

    for row in merged_df.itertuples(index=False):
        row_dict = row._asdict()
        fight_detail = parse_fight_detail(session, row_dict["fight_url"])
        identifier = build_fight_identifier(row_dict)

        if fight_is_pending(fight_detail):
            pending_fights.append(identifier)
            continue

        if fight_has_supported_winner(fight_detail):
            red_winner = fight_detail.get("red_status", "") == "W"
            all_fights_records.append(build_all_fights_record(row_dict, red_winner))

            if has_prediction_row(row_dict):
                historical_prediction_records.append(build_historical_prediction_record(row_dict))

            upcoming_fight_deletes.append(build_upcoming_fight_key(row_dict))
            upcoming_metadata_deletes.append(build_upcoming_metadata_key(row_dict))
            upcoming_prediction_deletes.append(build_upcoming_fight_key(row_dict))
            moved_fights.append(identifier)
            continue

        red_status = fight_detail.get("red_status", "") or "unknown"
        blue_status = fight_detail.get("blue_status", "") or "unknown"
        method = fight_detail.get("method", "") or "no method"
        discarded_fights.append(f"{identifier} ({red_status} / {blue_status} / {method})")

        upcoming_fight_deletes.append(build_upcoming_fight_key(row_dict))
        upcoming_metadata_deletes.append(build_upcoming_metadata_key(row_dict))
        upcoming_prediction_deletes.append(build_upcoming_fight_key(row_dict))

    with conn.cursor() as cur:
        if all_fights_records:
            cur.executemany(INSERT_ALL_FIGHTS, all_fights_records)

        if historical_prediction_records:
            cur.executemany(UPSERT_HISTORICAL_PREDICTIONS, historical_prediction_records)

        if upcoming_fight_deletes:
            cur.executemany(DELETE_UPCOMING_FIGHT, upcoming_fight_deletes)

        if upcoming_metadata_deletes:
            cur.executemany(DELETE_UPCOMING_METADATA, upcoming_metadata_deletes)

        if upcoming_prediction_deletes:
            cur.executemany(DELETE_UPCOMING_PREDICTION, upcoming_prediction_deletes)

    conn.commit()

    return {
        "moved_fights": moved_fights,
        "discarded_fights": discarded_fights,
        "pending_fights": pending_fights,
        "historical_prediction_rows_inserted": len(historical_prediction_records),
    }