from __future__ import annotations

import os

import pandas as pd
from historical_scraper.core.utils import normalize_name


BACKEND_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))
DEFAULT_ALIAS_CSV_PATH = os.path.join(
    BACKEND_DIR,
    "data",
    "reference",
    "fighter_aliases.csv",
)

# Creating the Alias registry class with some functions
class AliasRegistry:
    def __init__(self) -> None:
        self._canonicals: dict[str, str] = {}
    # Adding an alias
    def add(self, fighter: str, alias: str) -> None:
        normalized_fighter = normalize_name(fighter)
        normalized_alias = normalize_name(alias)
        if normalized_fighter:
            self._canonicals[normalized_fighter] = normalized_fighter
        if normalized_alias:
            self._canonicals[normalized_alias] = normalized_fighter

    def canonicalize(self, name: str) -> str:
        normalized = normalize_name(name)
        return self._canonicals.get(normalized, normalized)

# Loading the alias registry
def load_alias_registry(alias_csv_path: str | None = None) -> AliasRegistry:
    registry = AliasRegistry()
    csv_path = alias_csv_path or DEFAULT_ALIAS_CSV_PATH
    if os.path.exists(csv_path):
        aliases_df = pd.read_csv(csv_path, encoding="utf-8-sig")
        for _, row in aliases_df.iterrows():
            fighter = row.get("fighter")
            alias = row.get("alias")
            if pd.notna(fighter) and pd.notna(alias):
                registry.add(str(fighter), str(alias))
    return registry
