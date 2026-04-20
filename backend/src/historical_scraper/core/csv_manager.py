from __future__ import annotations

import os

import pandas as pd

from historical_scraper.core.utils import american_profit_multiple


BACKEND_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))
GENERATED_DATA_DIR = os.path.join(BACKEND_DIR, "data", "generated", "historical_scraper")
MISSING_DATA_DIR = os.path.join(GENERATED_DATA_DIR, "missing_data")
RECENT_FIGHTS_CSV_PATH = os.path.join(GENERATED_DATA_DIR, "recent_fights.csv")
MISSING_DATA_REPORT_PATH = os.path.join(MISSING_DATA_DIR, "missing_data_report.csv")
MISSING_COLUMNS_SUMMARY_PATH = os.path.join(MISSING_DATA_DIR, "missing_columns_summary.csv")
MISSING_ODDS_REPORT_PATH = os.path.join(MISSING_DATA_DIR, "missing_odds_report.csv")
# The columns used while getting the data for the recent_fights dataframe
INTERNAL_COLUMNS = [
    "fight_date",
    "event_name",
    "location",
    "event_url",
    "fight_url",
    "red_fighter",
    "blue_fighter",
    "red_fighter_url",
    "blue_fighter_url",
    "red_winner",
    "method",
    "finish_details",
    "finish_round",
    "finish_time",
    "title_bout",
    "weight_class",
    "gender",
    "number_of_rounds",
    "red_height_cms",
    "blue_height_cms",
    "red_reach_cms",
    "blue_reach_cms",
    "red_stance",
    "blue_stance",
    "red_age",
    "blue_age",
    "red_odds",
    "blue_odds",
    "odds_source",
    "odds_note",
    "r_match_wc_rank",
    "b_match_wc_rank",
    "red_current_lose_streak",
    "red_current_win_streak",
    "red_longest_win_streak",
    "red_losses",
    "red_total_rounds_fought",
    "red_total_title_bouts",
    "red_wins_by_ko",
    "red_wins_by_submission",
    "red_wins",
    "red_sig_str_landed_per_min",
    "red_avg_sub_att",
    "red_avg_td_landed",
    "blue_current_lose_streak",
    "blue_current_win_streak",
    "blue_longest_win_streak",
    "blue_losses",
    "blue_total_rounds_fought",
    "blue_total_title_bouts",
    "blue_wins_by_ko",
    "blue_wins_by_submission",
    "blue_wins",
    "blue_sig_str_landed_per_min",
    "blue_avg_sub_att",
    "blue_avg_td_landed",
]
# The columns used in the recent_fights dataframe
RECENT_COLUMNS = [
    "RedFighter",
    "BlueFighter",
    "RedOdds",
    "BlueOdds",
    "RedWinner",
    "RedReturn",
    "BlueReturn",
    "OddsDiff",
    "AgeDiff",
    "ReachDiff",
    "HeightDiff",
    "WinsDiff",
    "LossesDiff",
    "RoundsDiff",
    "TitleBoutDiff",
    "KODiff",
    "SubmissionDiff",
    "WinStreakDiff",
    "LoseStreakDiff",
    "LongestWinStreakDiff",
    "SigStrDiff",
    "SubAttDiff",
    "TDDiff",
    "RankDiff",
    "Date",
    "TitleBout",
    "WeightClass",
    "Gender",
    "NumberOfRounds",
    "BlueCurrentLoseStreak",
    "BlueCurrentWinStreak",
    "BlueLongestWinStreak",
    "BlueLosses",
    "BlueTotalRoundsFought",
    "BlueTotalTitleBouts",
    "BlueWinsByKO",
    "BlueWinsBySubmission",
    "BlueWins",
    "BlueStance",
    "BlueHeightCms",
    "BlueReachCms",
    "RedCurrentLoseStreak",
    "RedCurrentWinStreak",
    "RedLongestWinStreak",
    "RedLosses",
    "RedTotalRoundsFought",
    "RedTotalTitleBouts",
    "RedWinsByKO",
    "RedWinsBySubmission",
    "RedWins",
    "RedStance",
    "RedHeightCms",
    "RedReachCms",
    "RedAge",
    "BlueAge",
    "BMatchWCRank",
    "RMatchWCRank",
]
RESULT_COLUMNS = ["RedWinner", "RedReturn", "BlueReturn"]
NON_RESULT_COLUMNS = [column for column in RECENT_COLUMNS if column not in RESULT_COLUMNS]

# Creating an empty dataframe with all of the internal columns
def create_empty_recent_dataframe(fight_rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(fight_rows)
    for column in INTERNAL_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA
    return df[INTERNAL_COLUMNS].copy()

# Returning the dataframe with the columns used by our modeling csvs
def build_feature_dataframe(df: pd.DataFrame, include_results: bool = True) -> pd.DataFrame:
    feature_columns = {
        "RedFighter": df["red_fighter"],
        "BlueFighter": df["blue_fighter"],
        "RedOdds": df["red_odds"],
        "BlueOdds": df["blue_odds"],
        "OddsDiff": df["red_odds"] - df["blue_odds"],
        "AgeDiff": df["red_age"] - df["blue_age"],
        "ReachDiff": df["red_reach_cms"] - df["blue_reach_cms"],
        "HeightDiff": df["red_height_cms"] - df["blue_height_cms"],
        "WinsDiff": df["red_wins"] - df["blue_wins"],
        "LossesDiff": df["red_losses"] - df["blue_losses"],
        "RoundsDiff": df["red_total_rounds_fought"] - df["blue_total_rounds_fought"],
        "TitleBoutDiff": df["red_total_title_bouts"] - df["blue_total_title_bouts"],
        "KODiff": df["red_wins_by_ko"] - df["blue_wins_by_ko"],
        "SubmissionDiff": df["red_wins_by_submission"] - df["blue_wins_by_submission"],
        "WinStreakDiff": df["red_current_win_streak"] - df["blue_current_win_streak"],
        "LoseStreakDiff": df["red_current_lose_streak"] - df["blue_current_lose_streak"],
        "LongestWinStreakDiff": df["red_longest_win_streak"] - df["blue_longest_win_streak"],
        "SigStrDiff": df["red_sig_str_landed_per_min"] - df["blue_sig_str_landed_per_min"],
        "SubAttDiff": df["red_avg_sub_att"] - df["blue_avg_sub_att"],
        "TDDiff": df["red_avg_td_landed"] - df["blue_avg_td_landed"],
        "RankDiff": df["r_match_wc_rank"] - df["b_match_wc_rank"],
        "Date": df["fight_date"],
        "TitleBout": df["title_bout"],
        "WeightClass": df["weight_class"],
        "Gender": df["gender"],
        "NumberOfRounds": df["number_of_rounds"],
        "BlueCurrentLoseStreak": df["blue_current_lose_streak"],
        "BlueCurrentWinStreak": df["blue_current_win_streak"],
        "BlueLongestWinStreak": df["blue_longest_win_streak"],
        "BlueLosses": df["blue_losses"],
        "BlueTotalRoundsFought": df["blue_total_rounds_fought"],
        "BlueTotalTitleBouts": df["blue_total_title_bouts"],
        "BlueWinsByKO": df["blue_wins_by_ko"],
        "BlueWinsBySubmission": df["blue_wins_by_submission"],
        "BlueWins": df["blue_wins"],
        "BlueStance": df["blue_stance"],
        "BlueHeightCms": df["blue_height_cms"],
        "BlueReachCms": df["blue_reach_cms"],
        "RedCurrentLoseStreak": df["red_current_lose_streak"],
        "RedCurrentWinStreak": df["red_current_win_streak"],
        "RedLongestWinStreak": df["red_longest_win_streak"],
        "RedLosses": df["red_losses"],
        "RedTotalRoundsFought": df["red_total_rounds_fought"],
        "RedTotalTitleBouts": df["red_total_title_bouts"],
        "RedWinsByKO": df["red_wins_by_ko"],
        "RedWinsBySubmission": df["red_wins_by_submission"],
        "RedWins": df["red_wins"],
        "RedStance": df["red_stance"],
        "RedHeightCms": df["red_height_cms"],
        "RedReachCms": df["red_reach_cms"],
        "RedAge": df["red_age"],
        "BlueAge": df["blue_age"],
        "BMatchWCRank": df["b_match_wc_rank"],
        "RMatchWCRank": df["r_match_wc_rank"],
    }
    if include_results:
        feature_columns["RedWinner"] = df["red_winner"]
        feature_columns["RedReturn"] = [
            american_profit_multiple(odds, bool(winner))
            for odds, winner in zip(df["red_odds"], df["red_winner"], strict=False)
        ]
        feature_columns["BlueReturn"] = [
            american_profit_multiple(odds, not bool(winner))
            for odds, winner in zip(df["blue_odds"], df["red_winner"], strict=False)
        ]

    recent = pd.DataFrame(feature_columns)
    rounded_two_columns = [
        "ReachDiff",
        "HeightDiff",
        "SigStrDiff",
        "SubAttDiff",
        "TDDiff",
        "BlueHeightCms",
        "BlueReachCms",
        "RedHeightCms",
        "RedReachCms",
    ]
    for column in rounded_two_columns:
        recent[column] = recent[column].round(2)
    if include_results:
        for column in ["RedReturn", "BlueReturn"]:
            recent[column] = recent[column].round(4)
    ordered_columns = RECENT_COLUMNS if include_results else NON_RESULT_COLUMNS
    return recent[ordered_columns].copy()


# Returning the dataframe with only the columns used in recent_fights.csv
def finalize_recent_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    return build_feature_dataframe(df, include_results=True)

# Saving recent_fights.csv
def save_recent_dataframe(df: pd.DataFrame) -> str:
    os.makedirs(GENERATED_DATA_DIR, exist_ok=True)
    df[RECENT_COLUMNS].to_csv(RECENT_FIGHTS_CSV_PATH, index=False)
    return RECENT_FIGHTS_CSV_PATH

# Building the missing data report for rows with missing data
def build_missing_data_report(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for _, row in df.iterrows():
        missing_columns = [column for column in RECENT_COLUMNS if pd.isna(row[column])]
        if not missing_columns:
            continue
        rows.append(
            {
                "Date": row["Date"],
                "RedFighter": row["RedFighter"],
                "BlueFighter": row["BlueFighter"],
                "MissingColumnCount": len(missing_columns),
                "MissingColumns": ", ".join(missing_columns),
            }
        )
    return pd.DataFrame(
        rows,
        columns=["Date", "RedFighter", "BlueFighter", "MissingColumnCount", "MissingColumns"],
    )

# Building the report for how many missing data points there are in each column
def build_missing_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary_rows = []
    for column in RECENT_COLUMNS:
        missing_count = int(df[column].isna().sum())
        if missing_count == 0:
            continue
        summary_rows.append({"ColumnName": column, "MissingCount": missing_count})
    summary = pd.DataFrame(summary_rows, columns=["ColumnName", "MissingCount"])
    if not summary.empty:
        summary = summary.sort_values(["MissingCount", "ColumnName"], ascending=[False, True]).reset_index(drop=True)
    return summary

# Saving the missing reports in their folder
def save_missing_reports(df: pd.DataFrame) -> tuple[str, str]:
    os.makedirs(MISSING_DATA_DIR, exist_ok=True)
    missing_report = build_missing_data_report(df)
    missing_summary = build_missing_summary(df)
    missing_report.to_csv(MISSING_DATA_REPORT_PATH, index=False)
    missing_summary.to_csv(MISSING_COLUMNS_SUMMARY_PATH, index=False)
    return MISSING_DATA_REPORT_PATH, MISSING_COLUMNS_SUMMARY_PATH

# Building and saving the report for rows with odds missing
def save_missing_odds_report(df: pd.DataFrame) -> str:
    os.makedirs(MISSING_DATA_DIR, exist_ok=True)
    rows = []
    for index, row in df.reset_index(drop=True).iterrows():
        if pd.isna(row["red_odds"]) or pd.isna(row["blue_odds"]):
            rows.append(
                {
                    "Date": row["fight_date"],
                    "RedFighter": row["red_fighter"],
                    "BlueFighter": row["blue_fighter"],
                    "RecentFightsLineNumber": index + 2,
                }
            )
    missing_odds = pd.DataFrame(
        rows,
        columns=["Date", "RedFighter", "BlueFighter", "RecentFightsLineNumber"],
    )
    missing_odds.to_csv(MISSING_ODDS_REPORT_PATH, index=False)
    return MISSING_ODDS_REPORT_PATH
