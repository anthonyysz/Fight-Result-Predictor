-- Confirming no duplicate fights
SELECT
    fight_date,
    red_fighter,
    blue_fighter,
    weight_class,
    COUNT(*) AS duplicate_count
FROM public.all_fights
GROUP BY fight_date, red_fighter, blue_fighter, weight_class
HAVING COUNT(*) > 1;

-- Checking number of fights and most recent fight date
SELECT COUNT(*) AS total_rows, MAX(fight_date) AS newest_fight_date
FROM public.all_fights;

-- Looking at most recent fights
SELECT
    fight_date,
    red_fighter,
    blue_fighter,
    red_odds,
    blue_odds,
    red_winner,
    weight_class
FROM public.all_fights
ORDER BY fight_date DESC, red_fighter, blue_fighter
LIMIT 10;

-- Checking number of fights per weight class
SELECT weight_class, COUNT(*) AS fights
FROM public.all_fights
GROUP BY weight_class
ORDER BY fights DESC, weight_class;
