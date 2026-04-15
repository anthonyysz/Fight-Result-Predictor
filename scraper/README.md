# Scraper

This scraper builds `scraper/recent.csv` from `2024-12-14` to the present by applying source data in stages:

1. `ufcstats_scraper.py`
   - Initializes the fight rows from UFC Stats completed events
   - Fills result, method, weight class, scheduled rounds, title bout, height, reach, stance, and age
2. `odds_scraper.py`
   - Fills odds from FightOdds using sportsbook priority:
   - `FanDuel`, then `BetUS`, then `MyBookieAG`
   - If none exist, both fighters get `-10000`
3. `rankings_scraper.py`
   - Fills historical rankings from `rankings_history.csv`

The scraper does not write to SQL and does not use the Kaggle reference dataset as an input source.

## Main Files

- `main.py`
- `csv_manager.py`
- `ufcstats_scraper.py`
- `odds_scraper.py`
- `rankings_scraper.py`
- `aliases.py`
- `utils.py`

## Run

From the repo root:

```powershell
.venv\Scripts\python -m scraper
```

Optional:

```powershell
.venv\Scripts\python -m scraper --start-date 2024-12-14
```

## Outputs

- `scraper/recent.csv`
- `scraper/missing_data_report.csv`
- `scraper/missing_columns_summary.csv`

## Notes

- UFC Stats fight-detail pages are the source of truth for red and blue corners.
- Rankings are downloaded from `martj42/ufc_rankings_history`.
- Historical prefight stats like win streaks, sig strike averages, and takedown averages are not currently scraped into `recent.csv` because I did not find a direct source path for them that avoids relying on the Kaggle dataset.
