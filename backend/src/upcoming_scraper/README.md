# Upcoming Scraper

This scraper builds `backend/data/generated/upcoming_scraper/upcoming_fights.csv` for the nearest upcoming UFC event by applying source data in stages:

1. `ufcstats_scraper.py`
   - Initializes the fight rows from the nearest UFC Stats upcoming event
   - Fills weight class, scheduled rounds, title bout, height, reach, stance, age, current profile rates, and pre-fight UFC history totals
2. `odds_scraper.py`
   - Fills odds from FightOdds using sportsbook priority:
   - `FanDuel`, then `BetUS`, then `MyBookieAG`, then `MyBookie`
   - If none exist, the odds stay null and the fight is listed in `backend/data/generated/upcoming_scraper/missing_data/missing_odds_report.csv`
3. `rankings_scraper.py`
   - Fills rankings from `backend/data/reference/rankings_history.csv`
   - Unranked fighters are filled as `20`

The scraper does not write to SQL and mirrors the historical scraper's dataframe shape, except that result-derived columns are omitted.

## Main Files

- `main.py`
- `core/csv_manager.py`
- `sources/ufcstats_scraper.py`

## Run

From the repo root:

```powershell
.venv\Scripts\pip install -e .\backend
.venv\Scripts\python -m upcoming_scraper
```

## Outputs

- `backend/data/generated/upcoming_scraper/upcoming_fights.csv`
- `backend/data/generated/upcoming_scraper/missing_data/missing_data_report.csv`
- `backend/data/generated/upcoming_scraper/missing_data/missing_columns_summary.csv`
- `backend/data/generated/upcoming_scraper/missing_data/missing_odds_report.csv`
