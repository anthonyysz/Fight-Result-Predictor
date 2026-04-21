from __future__ import annotations

from functools import lru_cache
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup

from historical_scraper.core.utils import (
    age_on_fight_date,
    clean_text,
    infer_gender,
    normalize_weight_class,
    parse_float,
    parse_height_to_cm,
    parse_optional_date,
    parse_reach_to_cm,
    parse_scheduled_rounds,
    parse_us_date,
)


UFCSTATS_EVENTS_URL = "http://ufcstats.com/statistics/events/completed?page=all"
DEFAULT_TIMEOUT = 45
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/135.0.0.0 Safari/537.36"
)

# Starting a session
def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": DEFAULT_UA})
    return session

# Getting the text from a page
def get_soup(session: requests.Session, url: str) -> BeautifulSoup:
    response = session.get(url, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")

# Getting all of our completed events
def list_completed_events(session: requests.Session, start_date) -> list[dict[str, Any]]:
    soup = get_soup(session, UFCSTATS_EVENTS_URL)
    events: list[dict[str, Any]] = []
    for row in soup.select("tr.b-statistics__table-row"):
        link = row.select_one("a")
        cells = row.select("td")
        if not link or len(cells) < 2:
            continue
        first_parts = list(cells[0].stripped_strings)
        if len(first_parts) < 2:
            continue
        event_name = clean_text(first_parts[0])
        event_date = parse_us_date(first_parts[-1])
        if event_date < start_date:
            continue
        events.append(
            {
                "event_name": event_name,
                "event_url": link["href"],
                "fight_date": event_date,
                "location": clean_text(cells[1].get_text(" ", strip=True)),
            }
        )
    events.sort(key=lambda event: event["fight_date"])
    return events

# Getting the basic info from our rows to help with getting stats
def initialize_recent_rows(start_date) -> list[dict[str, Any]]:
    session = create_session()
    rows: list[dict[str, Any]] = []
    for event in list_completed_events(session, start_date):
        event_soup = get_soup(session, event["event_url"])
        for fight_row in event_soup.select("tr.b-fight-details__table-row.js-fight-details-click"):
            fight_url = fight_row.get("data-link")
            if not fight_url:
                continue
            corners = parse_fight_corners(session, fight_url)
            rows.append(
                {
                    "fight_date": event["fight_date"],
                    "event_name": event["event_name"],
                    "location": event["location"],
                    "event_url": event["event_url"],
                    "fight_url": fight_url,
                    "red_fighter": corners["red_fighter"],
                    "blue_fighter": corners["blue_fighter"],
                    "red_fighter_url": corners["red_fighter_url"],
                    "blue_fighter_url": corners["blue_fighter_url"],
                }
            )
    return rows

# Finding who is in which corner
def parse_fight_corners(session: requests.Session, fight_url: str) -> dict[str, str]:
    soup = get_soup(session, fight_url)
    totals_row = soup.select_one("section.b-fight-details__section table tbody tr")
    if totals_row is None:
        raise ValueError(f"Unable to find totals table for fight {fight_url}")
    fighter_links = totals_row.select("td.l-page_align_left a")
    if len(fighter_links) < 2:
        raise ValueError(f"Unable to find two fighters in totals table for fight {fight_url}")

    # Getting the link and fighter name of each fighter
    unique_links: list[tuple[str, str]] = []
    seen_urls: set[str] = set()
    for fighter_link in fighter_links:
        fighter_url = fighter_link.get("href", "")
        if not fighter_url or fighter_url in seen_urls:
            continue
        unique_links.append((clean_text(fighter_link.get_text(" ", strip=True)), fighter_url))
        seen_urls.add(fighter_url)
        if len(unique_links) == 2:
            break

    if len(unique_links) != 2:
        raise ValueError(f"Unable to resolve red/blue fighters for fight {fight_url}")

    return {
        "red_fighter": unique_links[0][0],
        "blue_fighter": unique_links[1][0],
        "red_fighter_url": unique_links[0][1],
        "blue_fighter_url": unique_links[1][1],
    }


# Getting the shared fighter profile details from the fighter url
def build_fighter_profile_lookup(session: requests.Session):
    @lru_cache(maxsize=2048)
    def fighter_profile(fighter_url: str) -> dict[str, Any]:
        soup = get_soup(session, fighter_url)
        stats: dict[str, str] = {}
        for item in soup.select("li.b-list__box-list-item"):
            text = clean_text(item.get_text(" ", strip=True))
            if ":" not in text:
                continue
            key, value = text.split(":", 1)
            stats[clean_text(key)] = clean_text(value)

        # Getting their stance
        stance = stats.get("STANCE")
        if not stance or stance == "--":
            stance = "Unknown"

        history_rows = []
        for row in soup.select("tr.b-fight-details__table-row.b-fight-details__table-row__hover"):
            cells = row.select("td")
            if len(cells) < 10:
                continue

            event_texts = [clean_text(text) for text in cells[6].stripped_strings]
            if len(event_texts) < 2:
                continue

            result = clean_text(cells[0].get_text(" ", strip=True)).lower()
            method = clean_text(cells[7].select_one("p").get_text(" ", strip=True) if cells[7].select_one("p") else "")
            fight_date = parse_us_date(event_texts[-1])
            round_value = clean_text(cells[8].get_text(" ", strip=True))
            history_rows.append(
                {
                    "fight_url": row.get("data-link", ""),
                    "fight_date": fight_date,
                    "event_name": event_texts[0],
                    "result": result,
                    "method": method,
                    "finish_round": int(round_value) if round_value.isdigit() else 0,
                    "title_bout": "belt.png" in str(cells[6]),
                }
            )

        # Parsing the stats to be saved to the df
        history_rows.sort(key=lambda bout: bout["fight_date"])
        return {
            "height_cms": parse_height_to_cm(stats.get("Height")),
            "reach_cms": parse_reach_to_cm(stats.get("Reach")),
            "stance": stance,
            "dob": parse_optional_date(stats.get("DOB")),
            "sig_str_landed_per_min": parse_float(stats.get("SLpM")) or 0.0,
            "avg_td_landed": parse_float(stats.get("TD Avg.")) or 0.0,
            "avg_sub_att": parse_float(stats.get("Sub. Avg.")) or 0.0,
            "history": history_rows,
        }

    return fighter_profile


def apply_ufcstats_data(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    session = create_session()
    fighter_profile = build_fighter_profile_lookup(session)

    # Adding this fight data to the dataframe
    enriched_rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        fight_date = pd.to_datetime(row_dict["fight_date"]).date()
        fight_data = parse_fight_detail(session, row_dict["fight_url"])

        red_profile = fighter_profile(row_dict["red_fighter_url"])
        blue_profile = fighter_profile(row_dict["blue_fighter_url"])
        red_prefight = summarize_prefight_stats(red_profile["history"], fight_date)
        blue_prefight = summarize_prefight_stats(blue_profile["history"], fight_date)

        row_dict.update(
            {
                "red_winner": fight_data["red_winner"],
                "method": fight_data["method"],
                "finish_details": fight_data["finish_details"],
                "finish_round": fight_data["finish_round"],
                "finish_time": fight_data["finish_time"],
                "title_bout": fight_data["title_bout"],
                "weight_class": fight_data["weight_class"],
                "gender": infer_gender(fight_data["weight_class"]),
                "number_of_rounds": fight_data["number_of_rounds"],
                "red_height_cms": red_profile["height_cms"],
                "blue_height_cms": blue_profile["height_cms"],
                "red_reach_cms": red_profile["reach_cms"],
                "blue_reach_cms": blue_profile["reach_cms"],
                "red_stance": red_profile["stance"],
                "blue_stance": blue_profile["stance"],
                "red_age": age_on_fight_date(red_profile["dob"], fight_date),
                "blue_age": age_on_fight_date(blue_profile["dob"], fight_date),
                "red_sig_str_landed_per_min": red_profile["sig_str_landed_per_min"],
                "blue_sig_str_landed_per_min": blue_profile["sig_str_landed_per_min"],
                "red_avg_sub_att": red_profile["avg_sub_att"],
                "blue_avg_sub_att": blue_profile["avg_sub_att"],
                "red_avg_td_landed": red_profile["avg_td_landed"],
                "blue_avg_td_landed": blue_profile["avg_td_landed"],
                "red_current_lose_streak": red_prefight["current_lose_streak"],
                "blue_current_lose_streak": blue_prefight["current_lose_streak"],
                "red_current_win_streak": red_prefight["current_win_streak"],
                "blue_current_win_streak": blue_prefight["current_win_streak"],
                "red_longest_win_streak": red_prefight["longest_win_streak"],
                "blue_longest_win_streak": blue_prefight["longest_win_streak"],
                "red_losses": red_prefight["losses"],
                "blue_losses": blue_prefight["losses"],
                "red_total_rounds_fought": red_prefight["total_rounds_fought"],
                "blue_total_rounds_fought": blue_prefight["total_rounds_fought"],
                "red_total_title_bouts": red_prefight["total_title_bouts"],
                "blue_total_title_bouts": blue_prefight["total_title_bouts"],
                "red_wins_by_ko": red_prefight["wins_by_ko"],
                "blue_wins_by_ko": blue_prefight["wins_by_ko"],
                "red_wins_by_submission": red_prefight["wins_by_submission"],
                "blue_wins_by_submission": blue_prefight["wins_by_submission"],
                "red_wins": red_prefight["wins"],
                "blue_wins": blue_prefight["wins"],
            }
        )
        enriched_rows.append(row_dict)

    return pd.DataFrame(enriched_rows)

# Getting fight details
def parse_fight_detail(session: requests.Session, fight_url: str) -> dict[str, Any]:
    soup = get_soup(session, fight_url)
    detail_block = soup.select_one("div.b-fight-details__fight")
    detail_items = []
    if detail_block is not None:
        detail_items = [
            item.get_text(" ", strip=True)
            for item in detail_block.select("i.b-fight-details__text-item_first, i.b-fight-details__text-item")
        ]
    detail_map: dict[str, str] = {}
    for item in detail_items:
        if ":" not in item:
            continue
        key, value = item.split(":", 1)
        detail_map[clean_text(key)] = clean_text(value)

    details_blocks = detail_block.select("p.b-fight-details__text") if detail_block is not None else []
    finish_details = ""
    if len(details_blocks) > 1:
        finish_details = details_blocks[1].get_text(" ", strip=True).replace("Details:", "", 1).strip()

    title_tag = soup.select_one("i.b-fight-details__fight-title")
    title_html = str(title_tag or "")
    weight_class = normalize_weight_class(clean_text(title_tag.get_text(" ", strip=True) if title_tag else ""))
    title_bout = "belt.png" in title_html or "Interim" in weight_class

    persons = soup.select("div.b-fight-details__person")
    red_status = ""
    blue_status = ""
    if len(persons) == 2:
        red_status_node = persons[0].select_one("i.b-fight-details__person-status")
        red_status = clean_text(red_status_node.get_text(" ", strip=True) if red_status_node else "")
        blue_status_node = persons[1].select_one("i.b-fight-details__person-status")
        blue_status = clean_text(blue_status_node.get_text(" ", strip=True) if blue_status_node else "")

    return {
        "red_winner": red_status == "W",
        "red_status": red_status,
        "blue_status": blue_status,
        "method": detail_map.get("Method", ""),
        "finish_details": finish_details,
        "finish_round": int(detail_map.get("Round", "0") or 0),
        "finish_time": detail_map.get("Time", ""),
        "number_of_rounds": parse_scheduled_rounds(detail_map.get("Time format")),
        "title_bout": title_bout,
        "weight_class": weight_class,
    }

# Adding up each of their win/loss stats
def summarize_prefight_stats(history: list[dict[str, Any]], fight_date) -> dict[str, int]:
    prior_bouts = [bout for bout in history if bout["fight_date"] < fight_date]

    wins = sum(1 for bout in prior_bouts if bout["result"] == "win")
    losses = sum(1 for bout in prior_bouts if bout["result"] == "loss")
    total_rounds_fought = sum(int(bout["finish_round"]) for bout in prior_bouts)
    total_title_bouts = sum(1 for bout in prior_bouts if bout["title_bout"])
    wins_by_ko = sum(1 for bout in prior_bouts if bout["result"] == "win" and is_ko_method(bout["method"]))
    wins_by_submission = sum(
        1 for bout in prior_bouts if bout["result"] == "win" and is_submission_method(bout["method"])
    )

    descending = sorted(prior_bouts, key=lambda bout: bout["fight_date"], reverse=True)
    current_win_streak = 0
    current_lose_streak = 0
    for bout in descending:
        if bout["result"] == "win" and current_lose_streak == 0:
            current_win_streak += 1
            continue
        if bout["result"] == "loss" and current_win_streak == 0:
            current_lose_streak += 1
            continue
        break

    longest_win_streak = 0
    running_win_streak = 0
    for bout in prior_bouts:
        if bout["result"] == "win":
            running_win_streak += 1
            longest_win_streak = max(longest_win_streak, running_win_streak)
        else:
            running_win_streak = 0

    return {
        "wins": wins,
        "losses": losses,
        "total_rounds_fought": total_rounds_fought,
        "total_title_bouts": total_title_bouts,
        "wins_by_ko": wins_by_ko,
        "wins_by_submission": wins_by_submission,
        "current_win_streak": current_win_streak,
        "current_lose_streak": current_lose_streak,
        "longest_win_streak": longest_win_streak,
    }

# Checking for KO
def is_ko_method(method: str) -> bool:
    upper = clean_text(method).upper()
    return "KO" in upper

# Checking for submission
def is_submission_method(method: str) -> bool:
    upper = clean_text(method).upper()
    return upper.startswith("SUB")
