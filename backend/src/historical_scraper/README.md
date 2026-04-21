# Historical Scraper

This scraper builds `backend/data/generated/historical_scraper/recent_fights.csv` from `2024-12-14` to the present by applying source data in stages:

1. `ufcstats_scraper.py`
   - Initializes the fight rows from UFC Stats completed events
   - Fills result, method, weight class, scheduled rounds, title bout, height, reach, stance, age, current profile rates, and pre-fight UFC history totals
2. `odds_scraper.py`
   - Fills odds from FightOdds using sportsbook priority:
   - `FanDuel`, then `BetUS`, then `MyBookieAG`, then `MyBookie`
   - If none exist, the odds stay null and the fight is listed in `backend/data/generated/historical_scraper/missing_data/missing_odds_report.csv`
3. `rankings_scraper.py`
   - Fills historical rankings from `backend/data/reference/rankings_history.csv`
   - Unranked fighters are filled as `20`

The scraper does not write to SQL and does not use the Kaggle reference dataset as an input source.

## Main Files

- `main.py`
- `core/csv_manager.py`
- `core/aliases.py`
- `core/utils.py`
- `sources/ufcstats_scraper.py`
- `sources/odds_scraper.py`
- `sources/rankings_scraper.py`

## Run

From the repo root:

```powershell
.venv\Scripts\pip install -e .\backend
.venv\Scripts\python -m historical_scraper
```

Optional:

```powershell
.venv\Scripts\pip install -e .\backend
.venv\Scripts\python -m historical_scraper --start-date 2024-12-14
```

## Outputs

- `backend/data/generated/historical_scraper/recent_fights.csv`
- `backend/data/generated/historical_scraper/missing_data/missing_data_report.csv`
- `backend/data/generated/historical_scraper/missing_data/missing_columns_summary.csv`
- `backend/data/generated/historical_scraper/missing_data/missing_odds_report.csv`

## Notes

- UFC Stats fight-detail pages are the source of truth for red and blue corners.
- Rankings are downloaded from `martj42/ufc_rankings_history`.
- Current `SLpM`, `TD Avg.`, and `Sub. Avg.` come from the fighter's current UFC Stats profile page because UFC Stats does not expose historical snapshots of those profile rates by fight date.
