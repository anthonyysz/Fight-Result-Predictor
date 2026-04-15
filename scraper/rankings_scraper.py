from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import pandas as pd
import requests

from scraper.aliases import load_alias_registry


RANKINGS_HISTORY_URL = "https://raw.githubusercontent.com/martj42/ufc_rankings_history/master/rankings_history.csv"
DEFAULT_TIMEOUT = 45
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/135.0.0.0 Safari/537.36"
)


def ensure_rankings_history(data_dir: Path) -> Path:
    data_dir.mkdir(parents=True, exist_ok=True)
    destination = data_dir / "rankings_history.csv"
    if destination.exists():
        return destination

    response = requests.get(RANKINGS_HISTORY_URL, timeout=DEFAULT_TIMEOUT, headers={"User-Agent": DEFAULT_UA})
    response.raise_for_status()
    destination.write_bytes(response.content)
    return destination


def apply_rankings(df: pd.DataFrame, data_dir: Path, alias_csv_path: Path) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    rankings_path = ensure_rankings_history(data_dir)
    rankings_df = pd.read_csv(rankings_path)
    rankings_df["date"] = pd.to_datetime(rankings_df["date"]).dt.date

    aliases = load_alias_registry(alias_csv_path)
    rankings_df["fighter_key"] = rankings_df["fighter"].map(lambda value: aliases.canonicalize(value))
    rankings_df = rankings_df.sort_values(["date", "weightclass", "rank"]).reset_index(drop=True)

    @lru_cache(maxsize=8192)
    def rank_for(fighter_name: str, fight_date, weight_class: str) -> int:
        fighter_key = aliases.canonicalize(fighter_name)
        filtered = rankings_df[
            (rankings_df["fighter_key"] == fighter_key)
            & (rankings_df["weightclass"] == weight_class)
            & (rankings_df["date"] <= fight_date)
        ]
        if filtered.empty:
            return 20
        latest_date = filtered["date"].max()
        latest = filtered[filtered["date"] == latest_date].sort_values("rank").iloc[0]
        return int(latest["rank"])

    updated_rows = []
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        fight_date = pd.to_datetime(row_dict["fight_date"]).date()
        row_dict["r_match_wc_rank"] = rank_for(row_dict["red_fighter"], fight_date, row_dict["weight_class"])
        row_dict["b_match_wc_rank"] = rank_for(row_dict["blue_fighter"], fight_date, row_dict["weight_class"])
        updated_rows.append(row_dict)

    return pd.DataFrame(updated_rows)
