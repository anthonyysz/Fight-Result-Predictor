from __future__ import annotations

import csv
from pathlib import Path

from scraper.utils import normalize_name


BUILTIN_ALIAS_ROWS = [
    ("Francisco Figueiredo", "Francisco Figueredo"),
    ("Montserrat Conejo", "Montserrat Ruiz"),
    ("TJ Dillashaw", "T.J. Dillashaw"),
    ("Punahele Soriano", "Puna Soriano"),
    ("Wu Yanan", "Yanan Wu"),
    ("Philip Rowe", "Phillip Rowe"),
    ("Phil Rowe", "Phillip Rowe"),
    ("Ode Osbourne", "Oder Osbourne"),
    ("Saidyokub Kakhramonov", "Saidyokub Kakharamonov"),
    ("JJ Aldrich", "Jj Aldrich"),
    ("Serghei Spivac", "Sergey Spivak"),
    ("Ion Cutelaba", "Ion Curelaba"),
    ("JP Buys", "Jp Buys"),
    ("Alatengheili", "Heili Alateng"),
    ("Dan Hooker", "Daniel Hooker"),
    ("Alexander Hernandez", "Alex Hernandez"),
    ("Alexandr Romanov", "Alexander Romanov"),
    ("Loopy Godinez", "Lupita Godinez"),
    ("Tuco Tokkos", "George Tokkos"),
    ("Ian Machado Garry", "Ian Garry"),
]


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
    for fighter, alias in BUILTIN_ALIAS_ROWS:
        registry.add(fighter, alias)

    if alias_csv_path and alias_csv_path.exists():
        with alias_csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                fighter = row.get("fighter")
                alias = row.get("alias")
                if fighter and alias:
                    registry.add(fighter, alias)
    return registry

