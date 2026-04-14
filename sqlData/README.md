# SQL Connection

## 1. Why?

When I first began this project, I wanted to be able to visualize what I'd already seen. I downloaded a database and did some data exploration and modeling as a jumping off point. Now, since I want my model to live on a live server, I need my own database. Without jeopardizing the accuracy of the comments I made in those notebooks, df.csv will live separately from the constantly updating SQL database. The main table for our SQL workflow in Postgres will be 'public.all_fights'.

## 2. Day-to-day workflow

When looking at the notebooks and seeing a file df.csv, keep in mind

- Keep notebooks pointed at `data/df.csv`
- Keep new scraped and updated data in PostgreSQL
- Scraper writes directly into `public.all_fights`

## Moving the same workflow to AWS RDS

Once you are ready to move from local PostgreSQL to AWS RDS, keep the same table shape and point your future scraper or any later refresh scripts at the RDS connection instead.

### 1. Create the RDS database

In the AWS Console:

1. Open Amazon RDS
2. Choose the region you want to use
3. Click `Create database`
4. Choose `Standard create`
5. Choose engine `PostgreSQL`
6. Choose the smallest dev-friendly template available
7. Set:
   - DB instance identifier: `fight-data-pg`
   - initial database name: `fight_data`
   - master username: your chosen username
   - master password: your chosen password
8. Set connectivity:
   - `Public access = Yes` for your first development setup
   - security group inbound rule for port `5432`
   - source should be your public IP only
   - do not allow `0.0.0.0/0`
9. Create the database and wait for status `Available`

### 2. Get the endpoint

After creation, copy:

- endpoint
- port
- database name
- username

### 3. Test the RDS connection

```powershell
psql --host=<rds-endpoint> --port=5432 --username=<username> --dbname=fight_data
```

### 4. Point your app or scraper at RDS

Update `sqlData/.env`:

```text
DATABASE_URL=postgresql://username:password@your-rds-endpoint:5432/fight_data
CSV_PATH=data/df.csv
```

Then have your future scraper or update workflow write to RDS instead of local PostgreSQL:

```powershell
# future scraper command goes here
```

### 5. Verify RDS

Set the session variable first:

```powershell
$env:DATABASE_URL="postgresql://username:password@your-rds-endpoint:5432/fight_data"
```

Then run:

```powershell
psql $env:DATABASE_URL -c "SELECT COUNT(*) FROM public.all_fights;"
psql $env:DATABASE_URL -c "SELECT MAX(fight_date) FROM public.all_fights;"
```

## Common beginner issues

### `psql` asks for a password and fails

- make sure you are using the right username
- make sure the password in `sqlData/.env` matches the database user
- if you used `DATABASE_URL`, remember that special characters in the password must be URL-encoded

### `DATABASE_URL is not set`

- make sure `sqlData/.env` exists
- make sure the file contains `DATABASE_URL=...`

### Connection refused

- make sure PostgreSQL is running locally
- make sure the host and port in `DATABASE_URL` are correct

### RDS timeout

- make sure the RDS instance is `Available`
- make sure `Public access` is enabled for this dev setup
- make sure the security group allows your current public IP on port `5432`

## Why `schema.sql` stays

`schema.sql` is still worth keeping because it defines the shape of the database table. If you ever recreate the database locally, move to AWS RDS, or need to rebuild the table from scratch, that file is the clean source of truth for the table structure.
