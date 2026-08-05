# Lakehouse Analytics — Flask / MinIO / PostgreSQL / Spark

Production-oriented rebuild of the previous browser-only build. The browser
(HTML/CSS/JS) now **only** calls this Flask REST API — it never talks to
MinIO, PostgreSQL, or Spark directly, and holds no business logic.

```
HTML / CSS / JavaScript  --fetch()-->  Flask REST API
                                          ├── routes/auth.py     (login/session)
                                          ├── routes/datasets.py (catalog, preview, download, viz, merge)
                                          ├── routes/upload.py   (validate + upload to Bronze)
                                          ├── routes/etl.py      (Spark cleaning -> Silver)
                                          └── routes/admin.py    (row edit/delete, dataset delete, audit log)
                                          |
                                    services/
                                          ├── minio_service.py    (only module that imports boto3)
                                          ├── postgres_service.py (only module that imports psycopg2)
                                          ├── spark_service.py    (all cleaning/merge/aggregate logic)
                                          └── catalog_service.py  (combines MinIO listings + Postgres metadata)
```

## Running with Docker Compose (easiest)

This spins up PostgreSQL, MinIO, and the app together, applies `db/schema.sql`
automatically on first boot, and creates your first Administrator account —
no manual `psql` or `pip install` needed.

```
cp .env.example .env
# edit .env: at minimum set SECRET_KEY and SEED_ADMIN_PASSWORD to real values
docker compose up --build
```

Then open **http://localhost:5000** and sign in with `SEED_ADMIN_USERNAME` /
`SEED_ADMIN_PASSWORD` from your `.env` (defaults: `admin` / `changeme123` —
change that password before using this for anything real).

Notes:
- The Postgres schema only auto-applies the **first** time (an empty
  `pg_data` volume). If you change `db/schema.sql` later, apply it manually
  with `psql`, or run `docker compose down -v` to wipe and re-init (this
  deletes all data).
- MinIO's console is at http://localhost:9001, login with
  `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` from `.env`.
- Inside the compose network the app reaches Postgres/MinIO by service name
  (`postgres`, `minio`) — `docker-compose.yml` overrides `PG_HOST` and
  `MINIO_ENDPOINT` for you, so the `localhost` values in `.env` are only
  used when you run the app outside Docker (see below).
- `SPARK_MODE` stays `local` (pandas, in the app container) unless you point
  `SPARK_MASTER_URL` at a real cluster — Spark itself isn't part of this
  compose file.

## Manual Setup (without Docker)

1. **Python deps**
   ```
   python -m venv venv && source venv/bin/activate
   pip install -r requirements.txt
   # If you'll run Spark in cluster mode:
   # pip install pyspark==3.5.1
   ```

2. **Environment** — copy `.env.example` to `.env` and fill in your real
   MinIO / PostgreSQL endpoints and credentials. Nothing here is sent to the
   browser.

3. **Database** — create the schema, then seed the first admin account:
   ```
   psql -h $PG_HOST -U $PG_USER -d $PG_DB -f db/schema.sql
   python db/seed.py admin "a-strong-password" "Workspace Admin"
   ```

4. **Run**
   ```
   python app.py                 # dev server, http://localhost:5000
   # or, in production:
   gunicorn -w 4 -b 0.0.0.0:8000 "app:create_app()"
   ```

## Spark modes

`SPARK_MODE=local` (default) runs every cleaning/merge/aggregation operation
in-process with pandas — no cluster required, good for dev and for datasets
that fit comfortably in memory.

`SPARK_MODE=cluster` runs the same operations through a real PySpark
`SparkSession` against `SPARK_MASTER_URL`. Requires a reachable Spark cluster
and `pip install pyspark`. Both modes expose identical methods
(`SparkETLService.clean/merge/aggregate/validate_upload`), so routes never
need to know which is active.

## What moved out of the browser

The previous build ran directly against MinIO with the AWS SDK, embedding
MinIO credentials in a "Connect" panel in the UI, and did all cleaning/merge
logic in JavaScript. In this build:

- MinIO credentials live only in `.env` / `services/minio_service.py`.
- Postgres now owns dataset display names, division overrides, archived
  flags, audit log, and dataset version history (previously stored as JSON
  files in MinIO's `metadata_v3/` prefix).
- All cleaning, joining, and chart-aggregation logic moved to
  `services/spark_service.py`.
- Authentication is now real (Postgres-backed users + Flask sessions)
  instead of a client-side "Administrator / Employee" toggle button.
- Only Administrators can rename/archive/delete datasets, edit or delete
  rows, or view the audit log — enforced server-side by
  `routes/_common.py::admin_required`, not just hidden in the UI.

## Notes / next steps

- The existing `bronze_v3/` / `silver_v3/` / `gold_v3/` object layout in
  MinIO is unchanged, so any data from the previous build is picked up as-is.
- "Other Format" uploads skip automatic merge, per the original spec — the
  Prepare/Merge pages still work on them individually.
- Row-level edit/delete in the Prepare page is wired up for Administrators
  as a thin admin tool; it edits the CSV in place in MinIO and logs every
  change to `audit_log`.
- `reporting_metrics` is a starting point for the "PostgreSQL Reporting
  Tables -> Tableau" leg of the architecture — populate it from
  `SparkETLService` as your reporting needs grow.
