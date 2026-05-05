# SQL Connection

## 1. Why?

Instead of running the programs locally and uploading what I'd found to the cloud, I wanted to store the data in the cloud. I hoped this cloud experience would widen my abilities as a programmer and increase my knowledge of databases, which it did.

## 2. To test the connection

```powershell
psql --host=<rds-endpoint> --port=5432 --username=<username> --dbname=fight_data
```

## 3. Verify RDS

Set the database URL:

```powershell
$env:DATABASE_URL="postgresql://username:password@your-rds-endpoint:5432/fight_data"
```

Then run:

```powershell
psql $env:DATABASE_URL -c "SELECT COUNT(*) FROM public.all_fights;"
psql $env:DATABASE_URL -c "SELECT MAX(fight_date) FROM public.all_fights;"
```
