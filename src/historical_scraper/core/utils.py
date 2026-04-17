from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime
from typing import Iterable

# Going from string to date object
def parse_us_date(value: str) -> date:
    value = value.strip()
    for fmt in ("%B %d, %Y", "%b. %d, %Y", "%b %d, %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unsupported date format: {value!r}")

# Getting the date from a string
def parse_optional_date(value: str | None) -> date | None:
    if not value:
        return None
    value = value.strip()
    if not value or value == "--":
        return None
    return parse_us_date(value)

# Clearing extra whitespaces
def clean_text(value: str | None) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value != value:
        return ""
    if not isinstance(value, str):
        value = str(value)
    return re.sub(r"\s+", " ", value).strip()

# Removes accents, removes non-ASCII characters, makes it lowercase, replaces non-letters/numbers with spaces, and removes extra spaces
def normalize_name(value: str | None) -> str:
    value = clean_text(value)
    normalized = unicodedata.normalize("NFKD", value)
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = normalized.lower()
    normalized = re.sub(r"[^a-z0-9 ]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized

# Getting the inch value from a couple of different spots
def parse_inches(raw_value: str | None) -> float | None:
    if raw_value is None:
        return None
    value = clean_text(raw_value)
    if not value or value == "--":
        return None

    feet_match = re.match(r"(?P<feet>\d+)'\s*(?P<inches>\d+)\"", value)
    if feet_match:
        feet = int(feet_match.group("feet"))
        inches = int(feet_match.group("inches"))
        return float((feet * 12) + inches)

    reach_match = re.match(r"(?P<inches>\d+(?:\.\d+)?)\"", value)
    if reach_match:
        return float(reach_match.group("inches"))

    raise ValueError(f"Unsupported measurement: {raw_value!r}")

# Inches to centimeters
def inches_to_cm(inches: float | None) -> float | None:
    if inches is None:
        return None
    return round(inches * 2.54, 2)

# Using inches to cm after getting just the inches value
def parse_height_to_cm(raw_value: str | None) -> float | None:
    return inches_to_cm(parse_inches(raw_value))

# Using inches to cm after getting just the inches value
def parse_reach_to_cm(raw_value: str | None) -> float | None:
    return inches_to_cm(parse_inches(raw_value))


def parse_float(raw_value: str | None) -> float | None:
    if raw_value is None:
        return None
    value = clean_text(raw_value)
    if not value or value == "--":
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", value.replace(",", ""))
    if not match:
        return None
    return float(match.group(0))

# Getting the (integer) number of rounds from a string
def parse_scheduled_rounds(raw_value: str | None) -> int | None:
    if not raw_value:
        return None
    match = re.search(r"(\d+)\s*Rnd", raw_value)
    return int(match.group(1)) if match else None

# Getting gender from weight class name
def infer_gender(weight_class: str) -> str:
    return "FEMALE" if weight_class.startswith("Women's ") else "MALE"

# Making our weight classes the ones we know
def normalize_weight_class(weight_class: str) -> str:
    weight_class = clean_text(weight_class)
    weight_class = weight_class.replace("Womenâ€™s ", "Women's ")
    for prefix in ("Interim UFC ", "UFC ", "Interim "):
        if weight_class.startswith(prefix):
            weight_class = weight_class[len(prefix) :]
    for suffix in (" Title Bout", " Title", " Bout"):
        if weight_class.endswith(suffix):
            weight_class = weight_class[: -len(suffix)]
            break
    known_weight_classes = [
        "Women's Strawweight",
        "Women's Flyweight",
        "Women's Bantamweight",
        "Women's Featherweight",
        "Strawweight",
        "Flyweight",
        "Bantamweight",
        "Featherweight",
        "Lightweight",
        "Welterweight",
        "Middleweight",
        "Light Heavyweight",
        "Heavyweight",
        "Catch Weight",
        "Open Weight",
    ]
    for known_weight_class in known_weight_classes:
        if known_weight_class in weight_class:
            return known_weight_class
    return weight_class

# Getting the fighter's age on fight day
def age_on_fight_date(dob: date | None, fight_date: date) -> int | None:
    if dob is None:
        return None
    years = fight_date.year - dob.year
    had_birthday = (fight_date.month, fight_date.day) >= (dob.month, dob.day)
    return years if had_birthday else years - 1

# Getting the profit amount based on whether the fighter won or lost
def american_profit_multiple(odds: int | None, won: bool) -> float | None:
    if odds is None:
        return None
    try:
        if odds != odds:
            return None
    except Exception:
        return None
    if not won:
        return 0.0
    try:
        if odds > 0:
            return round(1 + (odds / 100.0), 4)
        return round(1 + (100.0 / abs(odds)), 4)
    except Exception:
        return None


def parse_of_stat(raw_value: str | None) -> tuple[int | None, int | None]:
    if raw_value is None:
        return None, None
    value = clean_text(raw_value)
    if not value or value == "--":
        return None, None
    match = re.match(r"(\d+)\s+of\s+(\d+)", value)
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))

# Getting the number of seconds into a round
def parse_clock_to_seconds(raw_value: str | None) -> int | None:
    if raw_value is None:
        return None
    value = clean_text(raw_value)
    if not value or ":" not in value:
        return None
    minutes, seconds = value.split(":", 1)
    return int(minutes) * 60 + int(seconds)

# Getting the total fight time in seconds
def compute_total_fight_time_seconds(finish_round: int | None, finish_time: str | None) -> int | None:
    if finish_round is None or finish_round < 1:
        return None
    clock = parse_clock_to_seconds(finish_time)
    if clock is None:
        return None
    return ((finish_round - 1) * 300) + clock


def ensure_dir(path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def unique_preserving_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        if item not in seen:
            ordered.append(item)
            seen.add(item)
    return ordered
