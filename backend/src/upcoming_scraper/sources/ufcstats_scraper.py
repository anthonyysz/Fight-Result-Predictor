from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd

from historical_scraper.core.utils import age_on_fight_date, clean_text, infer_gender, normalize_weight_class, parse_us_date
from historical_scraper.sources.ufcstats_scraper import (
    build_fighter_profile_lookup,
    create_session,
    get_soup,
    summarize_prefight_stats,
)


UFCSTATS_UPCOMING_EVENTS_URL = "http://ufcstats.com/statistics/events/upcoming?page=all"


# Getting all of our upcoming events
def list_upcoming_events(session, start_date: date) -> list[dict[str, Any]]:
    soup = get_soup(session, UFCSTATS_UPCOMING_EVENTS_URL)
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


# Getting the nearest upcoming event
def get_nearest_upcoming_event(session, start_date: date) -> dict[str, Any]:
    events = list_upcoming_events(session, start_date)
    if not events:
        raise ValueError("Unable to find an upcoming UFC event on UFC Stats")
    return events[0]


# Finding who is in which corner from the event page row
def parse_upcoming_event_row(fight_row, fight_index: int) -> dict[str, Any]:
    cells = fight_row.select("td")
    fighter_cell = cells[1] if len(cells) > 1 else None
    fighter_links: list[tuple[str, str]] = []
    seen_urls: set[str] = set()
    # Upcoming matchups should use the visible names from the event table itself.
    for fighter_link in fighter_cell.select("a[href*='fighter-details']") if fighter_cell is not None else []:
        fighter_url = fighter_link.get("href", "")
        fighter_name = clean_text(fighter_link.get_text(" ", strip=True))
        if not fighter_url or not fighter_name or fighter_url in seen_urls:
            continue
        fighter_links.append((fighter_name, fighter_url))
        seen_urls.add(fighter_url)
        if len(fighter_links) == 2:
            break

    if len(fighter_links) != 2:
        raise ValueError("Unable to resolve red and blue fighters from upcoming event row")

    weight_class_cell = cells[6] if len(cells) > 6 else None
    weight_class_text = clean_text(weight_class_cell.get_text(" ", strip=True) if weight_class_cell else "")
    weight_class = normalize_weight_class(weight_class_text)
    title_bout = "belt.png" in str(weight_class_cell or "") or "Interim" in weight_class
    matchup_link = cells[4].select_one("a") if len(cells) > 4 else None
    fight_url = fight_row.get("data-link") or (matchup_link.get("data-link", "") if matchup_link else "")

    return {
        "fight_url": fight_url,
        "red_fighter": fighter_links[0][0],
        "blue_fighter": fighter_links[1][0],
        "red_fighter_url": fighter_links[0][1],
        "blue_fighter_url": fighter_links[1][1],
        "title_bout": title_bout,
        "weight_class": weight_class,
        "number_of_rounds": 5 if title_bout or fight_index == 0 else 3,
    }


# Getting the basic info from our rows to help with getting stats
def initialize_upcoming_rows(today: date | None = None) -> list[dict[str, Any]]:
    start_date = today or date.today()
    session = create_session()
    event = get_nearest_upcoming_event(session, start_date)
    event_soup = get_soup(session, event["event_url"])

    rows: list[dict[str, Any]] = []
    for fight_index, fight_row in enumerate(event_soup.select("tr.b-fight-details__table-row.js-fight-details-click")):
        row_data = parse_upcoming_event_row(fight_row, fight_index)
        rows.append(
            {
                "fight_date": event["fight_date"],
                "event_name": event["event_name"],
                "location": event["location"],
                "event_url": event["event_url"],
                "fight_url": row_data["fight_url"],
                "red_fighter": row_data["red_fighter"],
                "blue_fighter": row_data["blue_fighter"],
                "red_fighter_url": row_data["red_fighter_url"],
                "blue_fighter_url": row_data["blue_fighter_url"],
                "title_bout": row_data["title_bout"],
                "weight_class": row_data["weight_class"],
                "number_of_rounds": row_data["number_of_rounds"],
            }
        )
    return rows


# Adding this fight data to the dataframe
def apply_upcoming_ufcstats_data(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    session = create_session()
    fighter_profile = build_fighter_profile_lookup(session)

    enriched_rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        fight_date = pd.to_datetime(row_dict["fight_date"]).date()

        red_profile = fighter_profile(row_dict["red_fighter_url"])
        blue_profile = fighter_profile(row_dict["blue_fighter_url"])
        red_prefight = summarize_prefight_stats(red_profile["history"], fight_date)
        blue_prefight = summarize_prefight_stats(blue_profile["history"], fight_date)

        title_bout = bool(row_dict["title_bout"]) if pd.notna(row_dict["title_bout"]) else False
        weight_class = clean_text(row_dict["weight_class"])
        number_of_rounds = row_dict["number_of_rounds"] or (5 if title_bout else 3)

        row_dict.update(
            {
                "title_bout": title_bout,
                "weight_class": weight_class,
                "gender": infer_gender(weight_class),
                "number_of_rounds": number_of_rounds,
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
