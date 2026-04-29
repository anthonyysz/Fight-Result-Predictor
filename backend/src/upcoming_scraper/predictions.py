from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from fastapi import HTTPException
from joblib import load

from model_training.retrain_models import get_model_storage_settings, model_file_names, predict_fight
from upcoming_scraper.loaders import UPCOMING_CSV_TO_DB_COLUMNS

MODEL_CACHE_DIR = Path(tempfile.gettempdir()) / "fight_result_predictor_models"

UPCOMING_SOURCE_DB_COLUMNS = [db_column for _, db_column in UPCOMING_CSV_TO_DB_COLUMNS]
DB_TO_MODEL_COLUMNS = {db_column: csv_column for csv_column, db_column in UPCOMING_CSV_TO_DB_COLUMNS}

UPCOMING_PREDICTION_COLUMNS = [
    "fight_date",
    "red_fighter",
    "blue_fighter",
    "weight_class",
    "predicted_winner",
    "confidence",
    "expected_value_red",
    "expected_value_blue",
    "recommended_bet",
    "weight_class_model_used",
    "estimator",
    "model_params",
    "bet_threshold",
]

UPSERT_UPCOMING_PREDICTIONS = (
    f"INSERT INTO public.upcoming_predictions ({', '.join(UPCOMING_PREDICTION_COLUMNS)}) "
    f"VALUES ({', '.join(['%s'] * len(UPCOMING_PREDICTION_COLUMNS))}) "
    "ON CONFLICT ON CONSTRAINT upcoming_predictions_unique_fight DO UPDATE SET "
    "predicted_winner = EXCLUDED.predicted_winner, "
    "confidence = EXCLUDED.confidence, "
    "expected_value_red = EXCLUDED.expected_value_red, "
    "expected_value_blue = EXCLUDED.expected_value_blue, "
    "recommended_bet = EXCLUDED.recommended_bet, "
    "weight_class_model_used = EXCLUDED.weight_class_model_used, "
    "estimator = EXCLUDED.estimator, "
    "model_params = EXCLUDED.model_params, "
    "bet_threshold = EXCLUDED.bet_threshold, "
    "generated_at = now()"
)

MODEL_BUNDLE_CACHE: dict[str, dict[str, Any]] = {}


def to_python_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    item = getattr(value, "item", None)
    return item() if callable(item) else value

def resolve_model_file_name(weight_class: str) -> tuple[str, str]:
    if weight_class in model_file_names:
        return weight_class, model_file_names[weight_class]
    return "__global__", model_file_names["__global__"]


def is_missing_s3_object(exc: ClientError) -> bool:
    error_code = str(exc.response.get("Error", {}).get("Code", ""))
    return error_code in {"404", "NoSuchKey", "NotFound"}


def download_model_from_s3(file_name: str) -> Path:
    try:
        bucket, prefix = get_model_storage_settings()
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    MODEL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    local_path = MODEL_CACHE_DIR / file_name

    if local_path.exists():
        return local_path

    s3 = boto3.client("s3")
    s3_key = f"{prefix}/{file_name}"

    try:
        s3.download_file(bucket, s3_key, str(local_path))
    except ClientError:
        if local_path.exists():
            local_path.unlink()
        raise

    return local_path

def get_prediction_bundle(weight_class: str) -> dict[str, Any]:
    if weight_class in MODEL_BUNDLE_CACHE:
        return MODEL_BUNDLE_CACHE[weight_class]

    resolved_weight_class, file_name = resolve_model_file_name(weight_class)

    try:
        local_path = download_model_from_s3(file_name)
    except ClientError as exc:
        if resolved_weight_class != "__global__" and is_missing_s3_object(exc):
            file_name = model_file_names["__global__"]
            try:
                local_path = download_model_from_s3(file_name)
            except ClientError as fallback_exc:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to download global fallback model from S3: {fallback_exc}",
                ) from fallback_exc
        else:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to download model bundle from S3: {exc}",
            ) from exc

    try:
        bundle = load(local_path)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load model bundle {file_name}: {exc}",
        ) from exc

    MODEL_BUNDLE_CACHE[weight_class] = bundle
    if bundle.get("weight_class") is None:
        MODEL_BUNDLE_CACHE.setdefault("__global__", bundle)

    return bundle

def fetch_upcoming_fights_frame(conn) -> pd.DataFrame:
    query = f"""
        SELECT {', '.join(UPCOMING_SOURCE_DB_COLUMNS)}
        FROM public.upcoming_fights
        ORDER BY fight_date, red_fighter, blue_fighter
    """

    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        columns = [column.name for column in cur.description]

    if not rows:
        raise HTTPException(
            status_code=409,
            detail="public.upcoming_fights has no rows to predict.",
        )

    return pd.DataFrame(rows, columns=columns)


def prepare_prediction_frame(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.rename(columns=DB_TO_MODEL_COLUMNS).copy()
    prepared["Date"] = pd.to_datetime(prepared["Date"])
    return prepared


def build_fight_identifier(source_row: pd.Series) -> str:
    fight_date = pd.to_datetime(source_row["fight_date"]).date().isoformat()
    return (
        f"{fight_date} | "
        f"{source_row['red_fighter']} vs {source_row['blue_fighter']} | "
        f"{source_row['weight_class']}"
    )


def build_prediction_record(source_row: pd.Series, prediction: pd.Series) -> tuple[Any, ...]:
    record = {
        "fight_date": pd.to_datetime(source_row["fight_date"]).date(),
        "red_fighter": source_row["red_fighter"],
        "blue_fighter": source_row["blue_fighter"],
        "weight_class": source_row["weight_class"],
        "predicted_winner": prediction["predicted_winner"],
        "confidence": prediction["confidence"],
        "expected_value_red": prediction["expected_value_red"],
        "expected_value_blue": prediction["expected_value_blue"],
        "recommended_bet": prediction["recommended_bet"],
        "weight_class_model_used": prediction["weight_class_model_used"],
        "estimator": prediction["estimator"],
        "model_params": prediction["model_params"],
        "bet_threshold": prediction["bet_threshold"],
    }

    return tuple(to_python_value(record[column]) for column in UPCOMING_PREDICTION_COLUMNS)

def generate_upcoming_predictions(conn) -> tuple[int, list[str]]:
    source_df = fetch_upcoming_fights_frame(conn)
    prediction_df = prepare_prediction_frame(source_df)

    global_bundle = get_prediction_bundle("__global__")

    records: list[tuple[Any, ...]] = []
    predicted_fights: list[str] = []

    for idx in range(len(prediction_df)):
        fight_row = prediction_df.iloc[idx]
        source_row = source_df.iloc[idx]

        weight_class = fight_row["WeightClass"]
        bundle = get_prediction_bundle(weight_class)
        registry = {
            weight_class: bundle,
            "__global__": global_bundle,
        }

        prediction = predict_fight(fight_row, registry=registry)

        records.append(build_prediction_record(source_row, prediction))
        predicted_fights.append(build_fight_identifier(source_row))

    with conn.cursor() as cur:
        cur.executemany(UPSERT_UPCOMING_PREDICTIONS, records)

    conn.commit()
    return len(records), predicted_fights