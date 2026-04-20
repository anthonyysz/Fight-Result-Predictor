from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from difflib import SequenceMatcher

import pandas as pd
import requests

from historical_scraper.core.aliases import AliasRegistry, load_alias_registry
from historical_scraper.core.utils import clean_text, normalize_name


FIGHTODDS_GQL_URL = "https://api.fightodds.io/gql"
DEFAULT_TIMEOUT = 45
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/135.0.0.0 Safari/537.36"
)
SPORTSBOOK_PRIORITY = ["FanDuel", "BetUS", "MyBookieAG", "MyBookie"]


# Finding events in the date frame listed
EVENTS_QUERY = """
query EventsPromotionRecentQuery(
  $promotionSlug: String
  $dateLt: Date
  $dateGte: Date
  $after: String
  $first: Int
  $orderBy: String
) {
  promotion: promotionBySlug(slug: $promotionSlug) {
    events(first: $first, after: $after, date_Gte: $dateGte, date_Lt: $dateLt, orderBy: $orderBy) {
      edges {
        node {
          name
          pk
          slug
          date
        }
        cursor
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}
"""

# Finding odds for each fight in the selected event
EVENT_ODDS_QUERY = """
query EventOddsQuery($eventPk: Int!) {
  eventOfferTable(pk: $eventPk) {
    name
    pk
    fightOffers {
      edges {
        node {
          fighter1 { firstName lastName }
          fighter2 { firstName lastName }
          isCancelled
          straightOffers {
            edges {
              node {
                sportsbook { shortName }
                outcome1 { odds }
                outcome2 { odds }
              }
            }
          }
        }
      }
    }
  }
}
"""

# Out FightOddsEvent class, with name, date, and primary key
@dataclass
class FightOddsEvent:
    name: str
    event_date: date
    pk: int

# Starting the session
def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": DEFAULT_UA, "Content-Type": "application/json"})
    return session

# Runs adds odds to the dataframe; the big 'do everything' function for getting odds
def apply_odds(df: pd.DataFrame, alias_csv_path) -> pd.DataFrame:
    # If the dataframe is empty, theres nothing to do
    if df.empty:
        return df.copy()
    # Loading the aliases and starting the session
    aliases = load_alias_registry(alias_csv_path)
    session = create_session()
    # Start date is the earliest end data is most recent, makes sure we're not looking at events outside of our date range
    start_date = pd.to_datetime(df["fight_date"]).dt.date.min()
    end_date = pd.to_datetime(df["fight_date"]).dt.date.max()
    # Matching events from the UFC stats page to FightOdds page
    events_by_date = map_events_by_date(session, start_date, end_date)
    event_offer_cache: dict[int, dict] = {}

    # Making a list for our new fights
    updated_rows = []
    for _, row in df.iterrows():
        # Making each row a dictionary with the column header as the key and the row value as the value
        row_dict = row.to_dict()
        fight_date = pd.to_datetime(row_dict["fight_date"]).date()
        # Getting our FightOddsEvent
        event = resolve_event(row_dict["event_name"], fight_date, events_by_date)
        # If there is no event, return nothing and say so
        if event is None:
            row_dict["red_odds"] = pd.NA
            row_dict["blue_odds"] = pd.NA
            row_dict["odds_source"] = "missing"
            row_dict["odds_note"] = "could not find odds for fight"
            updated_rows.append(row_dict)
            continue
        # Finding the event by primary key if it isn't in the cache and putting it in the cache
        if event.pk not in event_offer_cache:
            event_offer_cache[event.pk] = get_event_offer_table(session, event.pk)

        # Matching the fight to the fightodds site, getting the odds
        matched = match_fight(
            event_offer_cache[event.pk],
            row_dict["red_fighter"],
            row_dict["blue_fighter"],
            aliases,
        )
        row_dict["red_odds"] = matched["red_odds"]
        row_dict["blue_odds"] = matched["blue_odds"]
        row_dict["odds_source"] = matched["odds_source"]
        row_dict["odds_note"] = matched["odds_note"]
        updated_rows.append(row_dict)
    # Returning a dataframe of our updated rows
    return pd.DataFrame(updated_rows)

# Initializing a session for GraphQL to navigate FightOdds website
def graphql(session: requests.Session, query: str, variables: dict) -> dict:
    response = session.post(
        FIGHTODDS_GQL_URL,
        json={"query": query, "variables": variables},
        timeout=DEFAULT_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("errors"):
        raise RuntimeError(payload["errors"][0]["message"])
    return payload["data"]

# Mapping each event date on UFC website to a FightOdds event
def map_events_by_date(session: requests.Session, start_date: date, end_date: date) -> dict[date, list[FightOddsEvent]]:
    events: dict[date, list[FightOddsEvent]] = {}
    cursor = None
    while True:
        data = graphql(
            session,
            # Finding events within 1 day of the date on the UFC website
            EVENTS_QUERY,
            {
                "promotionSlug": "ufc",
                "dateGte": start_date.isoformat(),
                "dateLt": (end_date + timedelta(days=1)).isoformat(),
                "after": cursor,
                "first": 100,
                "orderBy": "date",
            },
        )
        # Recording the event found with event name, date, and primary key
        connection = data["promotion"]["events"]
        for edge in connection["edges"]:
            node = edge["node"]
            event = FightOddsEvent(
                name=clean_text(node["name"]),
                event_date=date.fromisoformat(node["date"]),
                pk=int(node["pk"]),
            )
            events.setdefault(event.event_date, []).append(event)
        if not connection["pageInfo"]["hasNextPage"]:
            break
        cursor = connection["pageInfo"]["endCursor"]
    return events

# Getting the matching FightOddsEvent item
def resolve_event(event_name: str, fight_date: date, events_by_date: dict[date, list[FightOddsEvent]]) -> FightOddsEvent | None:
    candidates = events_by_date.get(fight_date, [])
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]

    normalized_target = normalize_name(event_name)
    scored = []
    for candidate in candidates:
        candidate_name = normalize_name(candidate.name)
        overlap = len(set(normalized_target.split()) & set(candidate_name.split()))
        scored.append((overlap, candidate))
    scored.sort(key=lambda item: item[0], reverse=True)
    return scored[0][1]

# Returning a dictionary with the event odds query based on a primary key given
def get_event_offer_table(session: requests.Session, event_pk: int) -> dict:
    data = graphql(session, EVENT_ODDS_QUERY, {"eventPk": event_pk})
    return data["eventOfferTable"]

# Matching names to aliases and getting the odds for that fight
def match_fight(event_offer_table: dict, red_fighter: str, blue_fighter: str, aliases: AliasRegistry) -> dict[str, object]:
    red_canonical = aliases.canonicalize(red_fighter)
    blue_canonical = aliases.canonicalize(blue_fighter)
    best_match = None
    best_score = -1.0

    # Looking at all the fights, skipping this if the fight was cancelled
    for edge in event_offer_table["fightOffers"]["edges"]:
        node = edge["node"]
        if node["isCancelled"]:
            continue
        #Clearing extra whitespaces
        fighter1 = clean_text(f"{node['fighter1']['firstName']} {node['fighter1']['lastName']}")
        fighter2 = clean_text(f"{node['fighter2']['firstName']} {node['fighter2']['lastName']}")
        # Seeing how similar the names are, the same and with reversed red/blue corners
        same_red_score = name_score(red_fighter, fighter1, aliases, red_canonical)
        same_blue_score = name_score(blue_fighter, fighter2, aliases, blue_canonical)
        reverse_red_score = name_score(red_fighter, fighter2, aliases, red_canonical)
        reverse_blue_score = name_score(blue_fighter, fighter1, aliases, blue_canonical)
        # Keeping track of how each scored
        same_orientation_score = same_red_score + same_blue_score
        reversed_orientation_score = reverse_red_score + reverse_blue_score
        # Seeing if either of the scores is high enough
        same_orientation = same_orientation_score >= 1.7 and min(same_red_score, same_blue_score) >= 0.8
        reversed_orientation = reversed_orientation_score >= 1.7 and min(reverse_red_score, reverse_blue_score) >= 0.8
        if not same_orientation and not reversed_orientation:
            continue
        # Choosing the sportsbook to get odds from
        preferred_offer = pick_preferred_offer(node["straightOffers"]["edges"])
        # If the scraper couldn't find the fight or the odds
        if preferred_offer is None:
            candidate = {
                "red_odds": pd.NA,
                "blue_odds": pd.NA,
                "odds_source": "missing",
                "odds_note": "could not find odds for fight",
            }
        # If the odds site has red and blue in the same order as the UFC page
        elif same_orientation:
            candidate = {
                "red_odds": int(preferred_offer["outcome1"]["odds"]),
                "blue_odds": int(preferred_offer["outcome2"]["odds"]),
                "odds_source": preferred_offer["sportsbook"]["shortName"],
                "odds_note": f"matched {preferred_offer['sportsbook']['shortName']} odds",
            }
        # If the odds site has the fighters in reversed order
        else:
            candidate = {
                "red_odds": int(preferred_offer["outcome2"]["odds"]),
                "blue_odds": int(preferred_offer["outcome1"]["odds"]),
                "odds_source": preferred_offer["sportsbook"]["shortName"],
                "odds_note": f"matched {preferred_offer['sportsbook']['shortName']} odds after reversing site order",
            }
        # If this is the best pick for that fight, we change this fight to the best match
        score = max(same_orientation_score, reversed_orientation_score)
        if score > best_score:
            best_score = score
            best_match = candidate
    # If there is a best_match, we return it
    if best_match is not None:
        return best_match
    # If not, we return it with missing
    return {
        "red_odds": pd.NA,
        "blue_odds": pd.NA,
        "odds_source": "missing",
        "odds_note": "could not find odds for fight",
    }

# Choosing which sportsbook to get odds from
def pick_preferred_offer(offer_edges: list[dict]) -> dict | None:
    offers_by_name = {}
    for offer_edge in offer_edges:
        offer_node = offer_edge["node"]
        offers_by_name[offer_node["sportsbook"]["shortName"]] = offer_node
    for sportsbook_name in SPORTSBOOK_PRIORITY:
        if sportsbook_name in offers_by_name:
            return offers_by_name[sportsbook_name]
    return None

# Scoring the name, using aliases and potential typos
def name_score(source_name: str, candidate_name: str, aliases: AliasRegistry, source_canonical: str | None = None) -> float:
    source_normalized = source_canonical or aliases.canonicalize(source_name)
    candidate_normalized = aliases.canonicalize(candidate_name)
    # If the source and candidate are the same, it's perfect
    if source_normalized == candidate_normalized:
        return 1.0
    # If either the source or candidate is empty, we return zero
    source_tokens = source_normalized.split()
    candidate_tokens = candidate_normalized.split()
    if not source_tokens or not candidate_tokens:
        return 0.0
    # If either is a subset of the other one
    source_set = set(source_tokens)
    candidate_set = set(candidate_tokens)
    if source_set <= candidate_set or candidate_set <= source_set:
        return 0.95
    # If the last name and first character of the first name are the same
    source_last = source_tokens[-1]
    candidate_last = candidate_tokens[-1]
    if source_last == candidate_last and source_tokens[0][:1] == candidate_tokens[0][:1]:
        return 0.93
    # Gives us how much of the names are similar using SequenceMatcher
    full_ratio = SequenceMatcher(None, source_normalized, candidate_normalized).ratio()
    if full_ratio >= 0.88:
        return full_ratio
    # If not a lot of the full name is similar, check just the last name
    last_ratio = SequenceMatcher(None, source_last, candidate_last).ratio()
    if last_ratio >= 0.88 and source_tokens[0][:1] == candidate_tokens[0][:1]:
        return 0.9
    # If none of the thresholds are met, let's just return the full SequenceMatcher ratio
    return full_ratio
