# FHCharts Worker (DigitalOcean App Platform)

This repository contains a worker that streams Binance aggTrade events and computes higher-timeframe (HTF) orderflow/footprint data, then writes HTF rows to a PostgreSQL table `timeframe_data`.

What's included:
- `worker_of.py` — main worker (already present in the repo)
- `db_postgres.py` — small Postgres helper to create table and upsert HTF rows
- `Dockerfile` — container image to run the worker
- `requirements.txt` — Python dependencies
- `.env.example` — example env vars (do not commit real credentials)

Deployment (DigitalOcean App Platform)
1. Push this repository to GitHub.
2. In DigitalOcean App Platform, create a new app and connect your GitHub repository.
3. Choose the service as a "Worker" and set the build to use the repository's Dockerfile (or let DO detect Python). The start command will be:

   python worker_of.py

4. Provide the following environment variables in the App Platform UI (or via DO secrets):

   - PGHOST
   - PGPORT
   - PGUSER
   - PGPASSWORD
   - PGDATABASE
   - SYMBOL (optional, defaults to `xmrusdt` in code)

Local testing
1. Create a Python virtualenv and install deps:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Set env vars (or copy `.env.example` to `.env` and edit). Then run:

```bash
python worker_of.py
```

Notes
- `db_postgres.py` will create the `timeframe_data` table in `public` schema if it does not exist.
- The worker calls `get_db()` at import/init time to resume buckets. Make sure DB creds are available when the process starts.
