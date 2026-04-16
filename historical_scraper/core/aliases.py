from __future__ import annotations

import csv
from pathlib import Path

from historical_scraper.core.utils import normalize_name


DEFAULT_ALIAS_CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "fighter_aliases.csv"


class AliasRegistry:
    def __init__(self) -> None:
        self._canonicals: dict[str, str] = {}

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


def load_alias_registry(alias_csv_path: Path | None = None) -> AliasRegistry:
    registry = AliasRegistry()
    csv_path = alias_csv_path or DEFAULT_ALIAS_CSV_PATH
    if csv_path.exists():
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                fighter = row.get("fighter")
                alias = row.get("alias")
                if fighter and alias:
                    registry.add(fighter, alias)
    return registry
