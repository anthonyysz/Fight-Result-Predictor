from __future__ import annotations

import os

import pandas as pd

from historical_scraper.core.csv_manager import NON_RESULT_COLUMNS, build_feature_dataframe


BACKEND_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))
GENERATED_DATA_DIR = os.path.join(BACKEND_DIR, "data", "generated", "upcoming_scraper")
MISSING_DATA_DIR = os.path.join(GENERATED_DATA_DIR, "missing_data")
UPCOMING_FIGHTS_CSV_PATH = os.path.join(GENERATED_DATA_DIR, "upcoming_fights.csv")
MISSING_DATA_REPORT_PATH = os.path.join(MISSING_DATA_DIR, "missing_data_report.csv")
MISSING_COLUMNS_SUMMARY_PATH = os.path.join(MISSING_DATA_DIR, "missing_columns_summary.csv")
MISSING_ODDS_REPORT_PATH = os.path.join(MISSING_DATA_DIR, "missing_odds_report.csv")


# Returning the dataframe with only the columns used in upcoming_fights.csv
def finalize_upcoming_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    return build_feature_dataframe(df, include_results=False)


# Saving upcoming_fights.csv
def save_upcoming_dataframe(df: pd.DataFrame) -> str:
    os.makedirs(GENERATED_DATA_DIR, exist_ok=True)
    df[NON_RESULT_COLUMNS].to_csv(UPCOMING_FIGHTS_CSV_PATH, index=False)
    return UPCOMING_FIGHTS_CSV_PATH


# Building the missing data report for rows with missing data
def build_missing_data_report(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for _, row in df.iterrows():
        missing_columns = [column for column in NON_RESULT_COLUMNS if pd.isna(row[column])]
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
    for column in NON_RESULT_COLUMNS:
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
                    "UpcomingFightsLineNumber": index + 2,
                }
            )
    missing_odds = pd.DataFrame(
        rows,
        columns=["Date", "RedFighter", "BlueFighter", "UpcomingFightsLineNumber"],
    )
    missing_odds.to_csv(MISSING_ODDS_REPORT_PATH, index=False)
    return MISSING_ODDS_REPORT_PATH
