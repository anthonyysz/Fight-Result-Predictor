# IMPORTS

from pathlib import Path
import warnings
import os
import boto3

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import (
    AdaBoostClassifier,
    ExtraTreesClassifier,
    GradientBoostingClassifier,
    HistGradientBoostingClassifier,
    RandomForestClassifier,
)
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from xgboost import XGBClassifier
from joblib import dump
import psycopg

# SECTION 1: ENVIRONMENT CONSTANTS

MODEL_BUCKET_ENV = "MODEL_BUCKET" # Cloud
MODEL_PREFIX_ENV = "MODEL_PREFIX"
ROOT = Path(__file__).resolve().parents[3] #Local
BACKEND_DIR = ROOT / "backend"
MODELS_PATH = ROOT / "models"
ENV_PATH = BACKEND_DIR / ".env"

def read_dotenv(path: str) -> dict[str, str]:
    values: dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as handle:
        raw_lines = handle.read().splitlines()
    for raw_line in raw_lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values

def get_conninfo() -> str:
    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        return database_url

    if not ENV_PATH.exists():
        raise ValueError(f"Missing backend env file at {ENV_PATH}")

    env_values = read_dotenv(str(ENV_PATH))
    database_url = env_values.get("DATABASE_URL")
    if database_url:
        return database_url

    required_keys = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]
    missing = [key for key in required_keys if not env_values.get(key)]
    if missing:
        raise ValueError(f"Missing database settings: {', '.join(missing)}")

    return (
        f"host={env_values['PGHOST']} "
        f"port={env_values['PGPORT']} "
        f"dbname={env_values['PGDATABASE']} "
        f"user={env_values['PGUSER']} "
        f"password={env_values['PGPASSWORD']}"
    )
# SECTION 2: VARIABLES AND ESTIMATORS

LEAKAGE_COLS = ["RedWinner", "RedReturn", "BlueReturn"]
IDENTITY_COLS = ["RedFighter", "BlueFighter"]
RETURN_COLS = ["RedReturn", "BlueReturn", "RedOdds", "BlueOdds"]
CATEGORICAL_COLS = ['RedStance', 'BlueStance', 'Gender']
GLOBAL_START_DATES = ["2010-01-01", "2014-01-01", "2016-01-01", "2018-01-01"]
TEST_YEARS = [2021, 2022, 2023, 2024]
EXCLUDED_WEIGHT_CLASSES = {"Catch Weight", "Women's Featherweight"}

DEFAULT_MODEL_PARAMS = {
    "logreg": {"C": 0.5},
    "rf": {"n_estimators": 400, "min_samples_leaf": 3, "max_depth": None},
    "extra_trees": {"n_estimators": 400, "min_samples_leaf": 3, "max_depth": None},
    "gb": {"n_estimators": 250, "learning_rate": 0.04, "max_depth": 2},
    "hist_gb": {"max_depth": 5, "learning_rate": 0.03, "max_iter": 350},
    "ada": {"n_estimators": 400, "learning_rate": 0.03},
    "xgb": {"n_estimators": 300, "max_depth": 3, "learning_rate": 0.03},
}

ESTIMATOR_PARAM_GRIDS = {
    "logreg": [{"C": c} for c in [0.05, 0.10, 0.50, 1.50]],
    "rf": [
        {"n_estimators": 300, "min_samples_leaf": 2, "max_depth": None},
        {"n_estimators": 500, "min_samples_leaf": 3, "max_depth": None},
    ],
    "extra_trees": [
        {"n_estimators": 300, "min_samples_leaf": 2, "max_depth": None},
        {"n_estimators": 500, "min_samples_leaf": 3, "max_depth": None},
    ],
    "gb": [
        {"n_estimators": 200, "learning_rate": 0.05, "max_depth": 2},
        {"n_estimators": 300, "learning_rate": 0.03, "max_depth": 2},
        {"n_estimators": 250, "learning_rate": 0.04, "max_depth": 3},
    ],
    "hist_gb": [
        {"max_depth": 3, "learning_rate": 0.05, "max_iter": 250},
        {"max_depth": 5, "learning_rate": 0.03, "max_iter": 350},
        {"max_depth": 6, "learning_rate": 0.02, "max_iter": 500},
    ],
    "ada": [
        {"n_estimators": 200, "learning_rate": 0.05},
        {"n_estimators": 400, "learning_rate": 0.03},
        {"n_estimators": 600, "learning_rate": 0.02},
    ],
    "xgb": [
        {"n_estimators": n, "max_depth": d, "learning_rate": lr, **booster}
        for booster in [{}, {"booster": "dart"}]
        for n, d, lr in [(250, 2, 0.04), (350, 3, 0.03), (300, 3, 0.03)]
    ],
}

MODEL_BUILDERS = {
    "logreg": lambda params: LogisticRegression(max_iter=3000, random_state=42, **params),
    "rf": lambda params: RandomForestClassifier(random_state=42, n_jobs=-1, **params),
    "extra_trees": lambda params: ExtraTreesClassifier(random_state=42, n_jobs=-1, **params),
    "gb": lambda params: GradientBoostingClassifier(random_state=42, **params),
    "hist_gb": lambda params: HistGradientBoostingClassifier(random_state=42, **params),
    "ada": lambda params: AdaBoostClassifier(random_state=42, **params),
    "xgb": lambda params: XGBClassifier(
        random_state=42,
        eval_metric="logloss",
        n_jobs=4,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=1.0,
        min_child_weight=2,
        **params,
    ),
}

# SECTION 3: GETTING OUR DATAFRAME FROM ALL_FIGHTS

def fetch_training_frame() -> pd.DataFrame:
    query = """
        SELECT
            red_fighter,
            blue_fighter,
            red_odds,
            blue_odds,
            red_winner,
            red_return,
            blue_return,
            odds_diff,
            age_diff,
            reach_diff,
            height_diff,
            wins_diff,
            losses_diff,
            rounds_diff,
            title_bout_diff,
            ko_diff,
            submission_diff,
            win_streak_diff,
            lose_streak_diff,
            longest_win_streak_diff,
            sig_str_diff,
            sub_att_diff,
            td_diff,
            rank_diff,
            fight_date,
            title_bout,
            weight_class,
            gender,
            number_of_rounds,
            blue_current_lose_streak,
            blue_current_win_streak,
            blue_longest_win_streak,
            blue_losses,
            blue_total_rounds_fought,
            blue_total_title_bouts,
            blue_wins_by_ko,
            blue_wins_by_submission,
            blue_wins,
            blue_stance,
            blue_height_cms,
            blue_reach_cms,
            red_current_lose_streak,
            red_current_win_streak,
            red_longest_win_streak,
            red_losses,
            red_total_rounds_fought,
            red_total_title_bouts,
            red_wins_by_ko,
            red_wins_by_submission,
            red_wins,
            red_stance,
            red_height_cms,
            red_reach_cms,
            red_age,
            blue_age,
            b_match_wc_rank,
            r_match_wc_rank,
            source_name,
            loaded_at
        FROM public.all_fights
    """

    with psycopg.connect(get_conninfo()) as conn:
        df = pd.read_sql(query, conn)

    return normalize_training_frame(df)

DB_TO_NOTEBOOK_COLUMNS = {
    "red_fighter": "RedFighter",
    "blue_fighter": "BlueFighter",
    "red_odds": "RedOdds",
    "blue_odds": "BlueOdds",
    "red_winner": "RedWinner",
    "red_return": "RedReturn",
    "blue_return": "BlueReturn",
    "odds_diff": "OddsDiff",
    "age_diff": "AgeDiff",
    "reach_diff": "ReachDiff",
    "height_diff": "HeightDiff",
    "wins_diff": "WinsDiff",
    "losses_diff": "LossesDiff",
    "rounds_diff": "RoundsDiff",
    "title_bout_diff": "TitleBoutDiff",
    "ko_diff": "KODiff",
    "submission_diff": "SubmissionDiff",
    "win_streak_diff": "WinStreakDiff",
    "lose_streak_diff": "LoseStreakDiff",
    "longest_win_streak_diff": "LongestWinStreakDiff",
    "sig_str_diff": "SigStrDiff",
    "sub_att_diff": "SubAttDiff",
    "td_diff": "TDDiff",
    "rank_diff": "RankDiff",
    "fight_date": "Date",
    "title_bout": "TitleBout",
    "weight_class": "WeightClass",
    "gender": "Gender",
    "number_of_rounds": "NumberOfRounds",
    "blue_current_lose_streak": "BlueCurrentLoseStreak",
    "blue_current_win_streak": "BlueCurrentWinStreak",
    "blue_longest_win_streak": "BlueLongestWinStreak",
    "blue_losses": "BlueLosses",
    "blue_total_rounds_fought": "BlueTotalRoundsFought",
    "blue_total_title_bouts": "BlueTotalTitleBouts",
    "blue_wins_by_ko": "BlueWinsByKO",
    "blue_wins_by_submission": "BlueWinsBySubmission",
    "blue_wins": "BlueWins",
    "blue_stance": "BlueStance",
    "blue_height_cms": "BlueHeightCms",
    "blue_reach_cms": "BlueReachCms",
    "red_current_lose_streak": "RedCurrentLoseStreak",
    "red_current_win_streak": "RedCurrentWinStreak",
    "red_longest_win_streak": "RedLongestWinStreak",
    "red_losses": "RedLosses",
    "red_total_rounds_fought": "RedTotalRoundsFought",
    "red_total_title_bouts": "RedTotalTitleBouts",
    "red_wins_by_ko": "RedWinsByKO",
    "red_wins_by_submission": "RedWinsBySubmission",
    "red_wins": "RedWins",
    "red_stance": "RedStance",
    "red_height_cms": "RedHeightCms",
    "red_reach_cms": "RedReachCms",
    "red_age": "RedAge",
    "blue_age": "BlueAge",
    "b_match_wc_rank": "BMatchWCRank",
    "r_match_wc_rank": "RMatchWCRank",
}

TRAINING_COLUMNS = [
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

def normalize_training_frame(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.rename(columns=DB_TO_NOTEBOOK_COLUMNS).copy()

    normalized["Date"] = pd.to_datetime(normalized["Date"])
    normalized = normalized.sort_values("Date").reset_index(drop=True)

    for column in ["source_name", "loaded_at"]:
        if column in normalized.columns:
            normalized = normalized.drop(columns=column)

    missing_columns = [column for column in TRAINING_COLUMNS if column not in normalized.columns]
    if missing_columns:
        raise ValueError(f"Missing expected training columns: {missing_columns}")

    normalized = normalized[TRAINING_COLUMNS].copy()
    return normalized

raw_df = fetch_training_frame()

# SECTION 4: THRESHOLD GRID AND BETTING ODDS

THRESHOLD_GRID = np.round(np.linspace(-0.02, 0.20, 45), 3)

def odds_to_decimal(odds):
    """Converts sportsbook odds to decimal odds"""
    odds = np.asarray(odds, dtype=float)
    return np.where(odds > 0, 1 + odds / 100.0, 1 + 100.0 / np.abs(odds))

def evaluate_betting_strategy(proba_red, odds_frame, threshold):
    """Scores model bets against the sportsbook odds"""
    red_dec = odds_to_decimal(odds_frame["RedOdds"])
    blue_dec = odds_to_decimal(odds_frame["BlueOdds"])
    ev_red = proba_red * red_dec - 1
    ev_blue = (1 - proba_red) * blue_dec - 1

    choose_red = ev_red >= ev_blue
    bet_mask = np.maximum(ev_red, ev_blue) > threshold
    realized_return = np.where(choose_red, odds_frame["RedReturn"].to_numpy(), odds_frame["BlueReturn"].to_numpy())
    placed_returns = realized_return[bet_mask]

    return {
        "bets": int(placed_returns.size),
        "returns": placed_returns,
        "return_rate": float(placed_returns.mean()) if placed_returns.size else np.nan,
        "threshold": float(threshold),
    }

def tune_threshold(proba_red, odds_frame, min_bets):
    """Picks the validation threshold with the best return rate"""
    results = [evaluate_betting_strategy(proba_red, odds_frame, threshold) for threshold in THRESHOLD_GRID]
    valid_results = [result for result in results if result["bets"] >= min_bets]
    best = max(valid_results or [evaluate_betting_strategy(proba_red, odds_frame, 0.0)], key=lambda x: (x["return_rate"], x["bets"]))
    return best["threshold"], best

# SECTION 5: PREPARING FOR PREPROCESSING

def prepare_feature_frame(frame):
    """Creates model features and sportsbook implied probability columns"""
    DIFF_FEATURE_COLS = [c for c in raw_df.columns if c.endswith("Diff")]
    BASE_FEATURE_COLS = ["RedOdds", "BlueOdds", *DIFF_FEATURE_COLS, "TitleBout", "NumberOfRounds"]
    X = frame.reindex(columns=BASE_FEATURE_COLS).copy()

    red_dec = odds_to_decimal(X["RedOdds"])
    blue_dec = odds_to_decimal(X["BlueOdds"])
    red_imp = 1 / red_dec
    blue_imp = 1 / blue_dec

    X["RedDecimalOdds"] = red_dec
    X["BlueDecimalOdds"] = blue_dec
    X["RedImpliedProb"] = red_imp
    X["BlueImpliedProb"] = blue_imp
    return X


def make_one_hot_encoder():
    """Processing categorical data into binary with OneHotEncoder"""
    try:
        return OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    except TypeError:
        return OneHotEncoder(handle_unknown="ignore", sparse=False)


def build_preprocessor(feature_cols):
    """Builds all preprocessing steps"""
    cat_cols = [c for c in CATEGORICAL_COLS if c in feature_cols]
    num_cols = [c for c in feature_cols if c not in cat_cols]

    return ColumnTransformer([
        ("num", Pipeline([("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler())]), num_cols),
        ("cat", Pipeline([("imputer", SimpleImputer(strategy="most_frequent")), ("onehot", make_one_hot_encoder())]), cat_cols),
    ], remainder="drop")

# SECTION 6: ESTIMATOR AND PIPELINE BUILDING

def build_estimator(model_name, model_params=None):
    """Creates an unfitted estimator from the chosen model family and parameters"""
    params = {**DEFAULT_MODEL_PARAMS[model_name], **(model_params or {})}
    return MODEL_BUILDERS[model_name](params)


def build_pipeline(feature_cols, model_name, model_params=None):
    """Builds the pipeline for preprocessing and modeling"""
    return Pipeline([
        ("prep", build_preprocessor(feature_cols)),
        ("model", build_estimator(model_name, model_params)),
    ])


def format_params(model_params):
    """Formats parameters so that they can be displayed in a table"""
    if not model_params:
        return "default"
    return ", ".join(f"{key}={value}" for key, value in sorted(model_params.items()))

# SECTION 7: PREPARING FOR THE BACKTEST

def choose_weight_class_backtest_config(frame, weight_class):
    """Uses looser split requirements for weight classes with fewer fights"""
    total_fights = int((frame["WeightClass"] == weight_class).sum())
    for floor, config in [
        (300, {"min_train": 120, "min_val": 20, "min_test": 20, "min_val_bets": 8}),
        (150, {"min_train": 80, "min_val": 12, "min_test": 12, "min_val_bets": 5}),
        (60, {"min_train": 30, "min_val": 6, "min_test": 6, "min_val_bets": 3}),
    ]:
        if total_fights >= floor:
            return config
    return {"min_train": 15, "min_val": 3, "min_test": 3, "min_val_bets": 1}

def split_year(data, test_year):
    """Creates train, validation, and test masks for one test year"""
    val_start = pd.Timestamp(f"{test_year - 1}-01-01")
    test_start = pd.Timestamp(f"{test_year}-01-01")
    test_end = pd.Timestamp(f"{test_year + 1}-01-01")

    return (
        data["Date"] < val_start,
        (data["Date"] >= val_start) & (data["Date"] < test_start),
        (data["Date"] >= test_start) & (data["Date"] < test_end),
    )

def get_backtest_data(frame, start_date=None, weight_class=None):
    """Filters the dataframe and prepares X, y, and feature columns for a backtest"""
    data = frame.copy()
    if start_date is not None:
        data = data[data["Date"] >= pd.Timestamp(start_date)]
    if weight_class is not None:
        data = data[data["WeightClass"] == weight_class]
    data = data.sort_values("Date").reset_index(drop=True)

    X = prepare_feature_frame(data)
    y = data["RedWinner"].astype(int)
    feature_cols = [c for c in X.columns if c != "Date"]
    return data, X, y, feature_cols

# SECTION 8: THE BACKTEST

def rolling_backtest(frame, model_name, model_params=None, start_date=None, weight_class=None, min_train=500, min_val=100, min_test=100, min_val_bets=30):
    """Runs a backtest for each model"""
    data, X, y, feature_cols = get_backtest_data(frame, start_date=start_date, weight_class=weight_class)
    if data.empty:
        return None

    returns, truth, proba, yearly_details = [], [], [], []

    for test_year in TEST_YEARS:
        train_mask, val_mask, test_mask = split_year(data, test_year)
        if train_mask.sum() < min_train or val_mask.sum() < min_val or test_mask.sum() < min_test:
            continue

        pipeline = build_pipeline(feature_cols, model_name, model_params=model_params)
        pipeline.fit(X.loc[train_mask, feature_cols], y.loc[train_mask])

        val_proba = pipeline.predict_proba(X.loc[val_mask, feature_cols])[:, 1]
        test_proba = pipeline.predict_proba(X.loc[test_mask, feature_cols])[:, 1]
        threshold, _ = tune_threshold(val_proba, data.loc[val_mask, RETURN_COLS], min_bets=min_val_bets)
        test_stats = evaluate_betting_strategy(test_proba, data.loc[test_mask, RETURN_COLS], threshold)

        if test_stats["bets"]:
            returns.append(test_stats["returns"])
        truth.append(y.loc[test_mask].to_numpy())
        proba.append(test_proba)
        yearly_details.append({"test_year": test_year, "threshold": threshold, "bets": test_stats["bets"], "return_rate": test_stats["return_rate"]})

    if not returns:
        return None

    flat_returns = np.concatenate(returns)
    flat_truth = np.concatenate(truth)
    flat_proba = np.concatenate(proba)

    return {
        "model": model_name,
        "model_params": model_params or {},
        "params_label": format_params(model_params or {}),
        "start_date": start_date,
        "weight_class": weight_class,
        "bets": int(flat_returns.size),
        "return_rate": float(flat_returns.mean()),
        "accuracy": float(accuracy_score(flat_truth, flat_proba >= 0.5)),
        "brier": float(brier_score_loss(flat_truth, flat_proba)),
        "logloss": float(log_loss(flat_truth, flat_proba, labels=[0, 1])),
        "splits": len(yearly_details),
        "yearly_details": yearly_details,
    }


def favorite_baseline(frame, weight_class=None, start_date="2021-01-01", end_date="2025-01-01"):
    """Shows the results of a plain favorite or underdog approach"""
    data = frame.copy()
    if weight_class is not None:
        data = data[data["WeightClass"] == weight_class]
    data = data[(data["Date"] >= pd.Timestamp(start_date)) & (data["Date"] < pd.Timestamp(end_date))]

    favorite_returns = np.where(data["RedOdds"] < data["BlueOdds"], data["RedReturn"], data["BlueReturn"])
    underdog_returns = np.where(data["RedOdds"] >= data["BlueOdds"], data["RedReturn"], data["BlueReturn"])

    return {
        "fights": int(len(data)),
        "favorite_return_rate": float(favorite_returns.mean()),
        "underdog_return_rate": float(underdog_returns.mean()),
    }

# SECTION 9: VALIDATION YEAR AND FITTING

def choose_deployment_validation_year(data, preferred_year=2024):
    """Pick the latest year that still leaves earlier fights available for training and enough fights for threshold tuning"""
    available_years = sorted(data["Date"].dt.year.dropna().unique().tolist(), reverse=True)
    ordered_years = []
    if preferred_year in available_years:
        ordered_years.append(preferred_year)
    ordered_years.extend(year for year in available_years if year != preferred_year)

    for year in ordered_years:
        year_start = pd.Timestamp(f"{year}-01-01")
        year_end = pd.Timestamp(f"{year + 1}-01-01")
        train_count = int((data["Date"] < year_start).sum())
        valid_count = int(((data["Date"] >= year_start) & (data["Date"] < year_end)).sum())
        min_valid = max(1, min(8, valid_count))
        if train_count >= max(10, min(50, len(data) // 4)) and valid_count >= min_valid:
            return int(year)
    return None

#Fitting the model
def fit_deployment_model(frame, model_name, model_params=None, weight_class=None, start_date=None, validation_year=2024):
    data = frame.copy()
    #Fitting to the preferred start date
    if start_date is not None:
        data = data[data["Date"] >= pd.Timestamp(start_date)]
    #Choosing the proper weight class
    if weight_class is not None:
        data = data[data["WeightClass"] == weight_class]
    data = data.sort_values("Date").reset_index(drop=True)

    #Getting implied odds from sportsbook
    X = prepare_feature_frame(data)
    y = data["RedWinner"].astype(int)
    feature_cols = [c for c in X.columns if c != "Date"]
    
    #Choosing the validation year and EV threshold
    chosen_validation_year = choose_deployment_validation_year(data, preferred_year=validation_year)
    if chosen_validation_year is None:
        threshold = 0.0
    else:
        valid_start = pd.Timestamp(f"{chosen_validation_year}-01-01")
        valid_end = pd.Timestamp(f"{chosen_validation_year + 1}-01-01")
        tune_train_mask = data["Date"] < valid_start
        tune_valid_mask = (data["Date"] >= valid_start) & (data["Date"] < valid_end)
        
        tuning_pipeline = build_pipeline(feature_cols, model_name, model_params=model_params)
        tuning_pipeline.fit(X.loc[tune_train_mask, feature_cols], y.loc[tune_train_mask])
        valid_proba = tuning_pipeline.predict_proba(X.loc[tune_valid_mask, feature_cols])[:, 1]
        min_bets = max(1, min(10, int(np.ceil(tune_valid_mask.sum() / 3))))
        threshold, _ = tune_threshold(valid_proba, data.loc[tune_valid_mask, RETURN_COLS], min_bets=min_bets)

    #Building and fitting the pipeline
    final_pipeline = build_pipeline(feature_cols, model_name, model_params=model_params)
    final_pipeline.fit(X.loc[:, feature_cols], y)

    return {
        "model": final_pipeline,
        "model_name": model_name,
        "model_params": model_params or {},
        "params_label": format_params(model_params or {}),
        "weight_class": weight_class,
        "start_date": start_date,
        "feature_columns": feature_cols,
        "threshold": float(threshold),
        "validation_year_used": chosen_validation_year,
    }

# SECTION 10: PREDICTING

CORNER_DIFF_SPECS = {
    "OddsDiff": ("RedOdds", "BlueOdds"),
    "AgeDiff": ("RedAge", "BlueAge"),
    "ReachDiff": ("RedReachCms", "BlueReachCms"),
    "HeightDiff": ("RedHeightCms", "BlueHeightCms"),
    "WinsDiff": ("RedWins", "BlueWins"),
    "LossesDiff": ("RedLosses", "BlueLosses"),
    "RoundsDiff": ("RedTotalRoundsFought", "BlueTotalRoundsFought"),
    "TitleBoutDiff": ("RedTotalTitleBouts", "BlueTotalTitleBouts"),
    "KODiff": ("RedWinsByKO", "BlueWinsByKO"),
    "SubmissionDiff": ("RedWinsBySubmission", "BlueWinsBySubmission"),
    "WinStreakDiff": ("RedCurrentWinStreak", "BlueCurrentWinStreak"),
    "LoseStreakDiff": ("RedCurrentLoseStreak", "BlueCurrentLoseStreak"),
    "LongestWinStreakDiff": ("RedLongestWinStreak", "BlueLongestWinStreak"),
    "SigStrDiff": ("RedAvgSigStrLanded", "BlueAvgSigStrLanded"),
    "SubAttDiff": ("RedAvgSubAtt", "BlueAvgSubAtt"),
    "TDDiff": ("RedAvgTDLanded", "BlueAvgTDLanded"),
    "RankDiff": ("RMatchWCRank", "BMatchWCRank"),
}


def build_prediction_row(red_corner, blue_corner, fight_details):
    """Builds a one-row prediction dataframe and automatically fills the difference columns from red and blue inputs."""
    row = {
        "RedFighter": red_corner["Fighter"],
        "BlueFighter": blue_corner["Fighter"],
        "RedOdds": red_corner["Odds"],
        "BlueOdds": blue_corner["Odds"],
        "RedAge": red_corner["Age"],
        "BlueAge": blue_corner["Age"],
        "RedReachCms": red_corner["ReachCms"],
        "BlueReachCms": blue_corner["ReachCms"],
        "RedHeightCms": red_corner["HeightCms"],
        "BlueHeightCms": blue_corner["HeightCms"],
        "RedWins": red_corner["Wins"],
        "BlueWins": blue_corner["Wins"],
        "RedLosses": red_corner["Losses"],
        "BlueLosses": blue_corner["Losses"],
        "RedTotalRoundsFought": red_corner["TotalRoundsFought"],
        "BlueTotalRoundsFought": blue_corner["TotalRoundsFought"],
        "RedTotalTitleBouts": red_corner["TotalTitleBouts"],
        "BlueTotalTitleBouts": blue_corner["TotalTitleBouts"],
        "RedWinsByKO": red_corner["WinsByKO"],
        "BlueWinsByKO": blue_corner["WinsByKO"],
        "RedWinsBySubmission": red_corner["WinsBySubmission"],
        "BlueWinsBySubmission": blue_corner["WinsBySubmission"],
        "RedCurrentWinStreak": red_corner["CurrentWinStreak"],
        "BlueCurrentWinStreak": blue_corner["CurrentWinStreak"],
        "RedCurrentLoseStreak": red_corner["CurrentLoseStreak"],
        "BlueCurrentLoseStreak": blue_corner["CurrentLoseStreak"],
        "RedLongestWinStreak": red_corner["LongestWinStreak"],
        "BlueLongestWinStreak": blue_corner["LongestWinStreak"],
        "RedAvgSigStrLanded": red_corner["AvgSigStrLanded"],
        "BlueAvgSigStrLanded": blue_corner["AvgSigStrLanded"],
        "RedAvgSubAtt": red_corner["AvgSubAtt"],
        "BlueAvgSubAtt": blue_corner["AvgSubAtt"],
        "RedAvgTDLanded": red_corner["AvgTDLanded"],
        "BlueAvgTDLanded": blue_corner["AvgTDLanded"],
        "RMatchWCRank": red_corner["MatchWCRank"],
        "BMatchWCRank": blue_corner["MatchWCRank"],
        **fight_details,
    }

    for diff_col, (red_col, blue_col) in CORNER_DIFF_SPECS.items():
        row[diff_col] = row[red_col] - row[blue_col]

    return pd.DataFrame([row])


def predict_fight(fight_row, registry=None):
    if registry is None:
        raise ValueError("A deployment registry must be provided to predict_fight().")
    #Copying the input to a dataframe, extra instances there for error prevention
    if isinstance(fight_row, dict):
        fight_df = pd.DataFrame([fight_row])
    elif isinstance(fight_row, pd.Series):
        fight_df = fight_row.to_frame().T
    else:
        fight_df = fight_row.copy()

    if len(fight_df) != 1:
        raise ValueError("predict_fight expects exactly one fight row.")

    #Getting the weight class
    weight_class = fight_df.iloc[0]["WeightClass"]
    #Getting our model and parameters, returning global if that weight class doesn't exist
    bundle = registry.get(weight_class, registry["__global__"])
    #Preparing our features for predicting
    feature_frame = prepare_feature_frame(fight_df)
    X_pred = feature_frame.reindex(columns=bundle["feature_columns"], fill_value=np.nan)

    #Getting probability for both red and blue
    prob_red = float(bundle["model"].predict_proba(X_pred)[0, 1])
    prob_blue = 1 - prob_red
    red_name = fight_df.iloc[0].get("RedFighter", "Red")
    blue_name = fight_df.iloc[0].get("BlueFighter", "Blue")

    #Showing our model's predicted winner
    predicted_corner = "Red" if prob_red >= prob_blue else "Blue"
    predicted_winner = red_name if predicted_corner == "Red" else blue_name
    confidence = prob_red if predicted_corner == "Red" else prob_blue

    #Deciding whether or not to bet on the fight based on the probability vs the return.
    red_decimal = float(odds_to_decimal([fight_df.iloc[0]["RedOdds"]])[0])
    blue_decimal = float(odds_to_decimal([fight_df.iloc[0]["BlueOdds"]])[0])
    ev_red = prob_red * red_decimal - 1
    ev_blue = prob_blue * blue_decimal - 1

    if ev_red >= ev_blue:
        bet_corner = "Red"
        bet_name = red_name
        best_ev = ev_red
    else:
        bet_corner = "Blue"
        bet_name = blue_name
        best_ev = ev_blue

    recommended_bet = bet_name if best_ev > bundle["threshold"] else "Pass"

    return pd.Series(
        {
            "weight_class_model_used": bundle["weight_class"] or "GLOBAL_FALLBACK",
            "estimator": bundle["model_name"],
            "model_params": bundle["params_label"],
            "predicted_winner": predicted_winner,
            "confidence": confidence,
            "expected_value_red": ev_red,
            "expected_value_blue": ev_blue,
            "bet_threshold": bundle["threshold"],
            "recommended_bet": recommended_bet,
        }
    )

# SECTION 11: RUNNING THE BACKTESTS

def run_global_backtests(raw_df: pd.DataFrame) -> pd.DataFrame:
    global_results = []

    for start_date in GLOBAL_START_DATES:
        for model_name in ESTIMATOR_PARAM_GRIDS:
            result = rolling_backtest(
                raw_df,
                model_name,
                model_params=DEFAULT_MODEL_PARAMS[model_name],
                start_date=start_date,
            )
            if result is not None:
                global_results.append(result)

    if not global_results:
        return pd.DataFrame(
            columns=[
                "model",
                "model_params",
                "params_label",
                "start_date",
                "weight_class",
                "bets",
                "return_rate",
                "accuracy",
                "brier",
                "logloss",
                "splits",
                "yearly_details",
            ]
        )

    return (
        pd.DataFrame(global_results)
        .sort_values("return_rate", ascending=False)
        .reset_index(drop=True)
    )

def run_weight_class_backtests(raw_df: pd.DataFrame) -> pd.DataFrame:
    MODELED_WEIGHT_CLASSES = [wc for wc in raw_df["WeightClass"].value_counts().index if wc not in EXCLUDED_WEIGHT_CLASSES]
    all_weight_classes = MODELED_WEIGHT_CLASSES

    excluded_weight_classes = sorted(EXCLUDED_WEIGHT_CLASSES)
    weight_results_start_date = "2016-01-01"

    weight_results = []
    for weight_class in all_weight_classes:
        class_config = choose_weight_class_backtest_config(raw_df, weight_class)
        fight_count = int(
            (
                (raw_df["WeightClass"] == weight_class)
                & (raw_df["Date"] >= pd.Timestamp(weight_results_start_date))
            ).sum()
        )

        for model_name, param_grid in ESTIMATOR_PARAM_GRIDS.items():
            for model_params in param_grid:
                result = rolling_backtest(
                    raw_df,
                    model_name,
                    model_params=model_params,
                    weight_class=weight_class,
                    start_date=weight_results_start_date,
                    **class_config,
                )
                if result is not None:
                    result["fight_count"] = fight_count
                    result["bet_rate"] = result["bets"] / fight_count if fight_count else np.nan
                    result["config_label"] = format_params(class_config)
                    weight_results.append(result)

    if not weight_results:
        return pd.DataFrame(
            columns=[
                "model",
                "model_params",
                "params_label",
                "start_date",
                "weight_class",
                "bets",
                "return_rate",
                "accuracy",
                "brier",
                "logloss",
                "splits",
                "yearly_details",
                "fight_count",
                "bet_rate",
                "config_label",
            ]
        )

    weight_results_df = (
        pd.DataFrame(weight_results)
        .sort_values(["weight_class", "return_rate"], ascending=[True, False])
        .reset_index(drop=True)
    )

    return weight_results_df

# SECTION 12: GETTING THE BEST MODELS

def select_best_weight_class_models(raw_df: pd.DataFrame, weight_results: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if weight_results.empty:
        raise ValueError("weight_results is empty, so no weight-class models can be selected.")

    best_weight_class_models = (
        weight_results.assign(is_profitable=weight_results["return_rate"] > 1)
        .sort_values(
            ["weight_class", "is_profitable", "return_rate", "bets", "accuracy", "brier", "splits"],
            ascending=[True, False, False, False, False, True, False],
        )
        .groupby("weight_class", as_index=False)
        .head(1)
        .sort_values("weight_class")
        .reset_index(drop=True)
    )

    final_specs = best_weight_class_models[
        [
            "weight_class",
            "model",
            "model_params",
            "params_label",
            "fight_count",
            "splits",
            "bets",
            "bet_rate",
            "return_rate",
            "accuracy",
            "brier",
            "logloss",
        ]
    ].copy()

    baseline_rows = []
    for weight_class in final_specs["weight_class"]:
        baseline = favorite_baseline(raw_df, weight_class=weight_class)
        baseline_rows.append({"weight_class": weight_class, **baseline})

    baseline_df = pd.DataFrame(baseline_rows)

    final_summary = final_specs.merge(baseline_df, on="weight_class", how="left")

    return final_specs, final_summary

def build_deployment_registry(raw_df: pd.DataFrame, final_specs: pd.DataFrame) -> tuple[dict[str, dict], pd.DataFrame]:
    deployment_registry: dict[str, dict] = {}
    deployment_rows: list[dict] = []

    for row in final_specs.to_dict("records"):
        fitted = fit_deployment_model(
            raw_df,
            row["model"],
            model_params=row["model_params"],
            weight_class=row["weight_class"],
        )
        deployment_registry[row["weight_class"]] = fitted
        deployment_rows.append(
            {
                "segment": row["weight_class"],
                "model": row["model"],
                "params_label": row["params_label"],
                "tuning_year": fitted["validation_year_used"],
                "deployment_threshold": fitted["threshold"],
            }
        )

    global_fallback = fit_deployment_model(
        raw_df,
        "logreg",
        model_params=DEFAULT_MODEL_PARAMS["logreg"],
        start_date="2016-01-01",
    )
    deployment_registry["__global__"] = global_fallback
    deployment_rows.append(
        {
            "segment": "__global__",
            "model": "logreg",
            "params_label": format_params(DEFAULT_MODEL_PARAMS["logreg"]),
            "tuning_year": global_fallback["validation_year_used"],
            "deployment_threshold": global_fallback["threshold"],
        }
    )

    deployment_df = pd.DataFrame(deployment_rows)
    return deployment_registry, deployment_df

# SECTION 13: SAVING THE MODELS

model_file_names = {
    "Women's Strawweight": "wstraw.joblib",
    "Women's Flyweight": "wfly.joblib",
    "Women's Bantamweight": "wbantam.joblib",
    "Flyweight": "fly.joblib",
    "Bantamweight": "bantam.joblib",
    "Featherweight": "feather.joblib",
    "Lightweight": "light.joblib",
    "Welterweight": "welter.joblib",
    "Middleweight": "middle.joblib",
    "Light Heavyweight": "lhw.joblib",
    "Heavyweight": "heavy.joblib",
}

# Local
def save_models_locally(deployment_registry: dict[str, dict]) -> list[dict]:
    MODELS_PATH.mkdir(exist_ok=True)

    saved_model_rows: list[dict] = []
    for weight_class, file_name in model_file_names.items():
        if weight_class not in deployment_registry:
            raise KeyError(f"Missing deployment bundle for weight class: {weight_class}")

        bundle = deployment_registry[weight_class]
        save_path = MODELS_PATH / file_name

        dump(bundle, save_path)

        saved_model_rows.append(
            {
                "weight_class": weight_class,
                "file_name": file_name,
                "local_path": str(save_path),
                "start_date": bundle["start_date"],
                "model": bundle["model_name"],
                "threshold": bundle["threshold"],
            }
        )

    return saved_model_rows

#Cloud
def get_model_storage_settings() -> tuple[str, str]:
    bucket = os.environ.get(MODEL_BUCKET_ENV)
    prefix = os.environ.get(MODEL_PREFIX_ENV)

    if bucket:
        return bucket, (prefix or "models").strip("/")

    if ENV_PATH.exists():
        env_values = read_dotenv(str(ENV_PATH))
        bucket = env_values.get(MODEL_BUCKET_ENV)
        prefix = env_values.get(MODEL_PREFIX_ENV, "models")
        if bucket:
            return bucket, prefix.strip("/")

    raise ValueError(
        f"Missing model storage configuration. Set {MODEL_BUCKET_ENV} and {MODEL_PREFIX_ENV}."
    )

def upload_models_to_s3(saved_model_rows: list[dict]) -> list[dict]:
    bucket, prefix = get_model_storage_settings()
    s3 = boto3.client("s3")

    uploaded_rows: list[dict] = []
    for row in saved_model_rows:
        local_path = row["local_path"]
        file_name = row["file_name"]
        s3_key = f"{prefix}/{file_name}"

        s3.upload_file(local_path, bucket, s3_key)

        uploaded_rows.append(
            {
                **row,
                "s3_bucket": bucket,
                "s3_key": s3_key,
                "s3_uri": f"s3://{bucket}/{s3_key}",
            }
        )

    return uploaded_rows

def main() -> None:
    raw_df = fetch_training_frame()

    print(f"training_rows: {len(raw_df)}")
    print(f"training_columns: {list(raw_df.columns)}")
    print(f"min_date: {raw_df['Date'].min()}")
    print(f"max_date: {raw_df['Date'].max()}")

    global_results = run_global_backtests(raw_df)
    print(f"global_backtest_rows: {len(global_results)}")

    weight_results = run_weight_class_backtests(raw_df)
    print(f"weight_backtest_rows: {len(weight_results)}")

    final_specs, final_summary = select_best_weight_class_models(raw_df, weight_results)
    print(final_summary)

    deployment_registry, deployment_df = build_deployment_registry(raw_df, final_specs)
    print(deployment_df)

    saved_model_rows = save_models_locally(deployment_registry)
    print(pd.DataFrame(saved_model_rows))

    uploaded_model_rows = upload_models_to_s3(saved_model_rows)
    print(pd.DataFrame(uploaded_model_rows))

    print("Retraining complete.")
    print(f"local_models_replaced: {len(saved_model_rows)}")
    print(f"s3_models_replaced: {len(uploaded_model_rows)}")


if __name__ == "__main__":
    main()