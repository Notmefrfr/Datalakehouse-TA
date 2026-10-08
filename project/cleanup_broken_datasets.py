"""
Deletes the leftover Postgres rows for datasets that show "Error" / 0 rows
on the Datasets page (their Delta table in MinIO never actually got
created, so the normal Delete button fails — it tries to remove Delta
storage that isn't really there and bails out before cleaning up Postgres).

This does NOT touch MinIO (nothing to touch — those folders are empty) and
does NOT touch any dataset that's actually working. It only removes the
catalog bookkeeping rows for the dataset_id(s) listed in BROKEN_DATASET_IDS
below, so they stop showing up at all and you can upload those names fresh.

Run from the project root (same folder as app.py / config.py), same venv:

    python cleanup_broken_datasets.py
"""
from config import Config
import psycopg2

# Edit this list if you have other broken/empty "Error" datasets to remove.
BROKEN_DATASET_IDS = ["employee", "nfl_coba"]

conn = psycopg2.connect(
    host=Config.PG_HOST, port=Config.PG_PORT, dbname=Config.PG_DB,
    user=Config.PG_USER, password=Config.PG_PASSWORD,
)
conn.autocommit = True

with conn.cursor() as cur:
    for dataset_id in BROKEN_DATASET_IDS:
        print(f"--- {dataset_id} ---")
        cur.execute("DELETE FROM dataset_schema WHERE dataset_id = %s", (dataset_id,))
        print(f"  dataset_schema: {cur.rowcount} row(s) deleted")
        cur.execute("DELETE FROM dataset_metadata WHERE layer = 'Master' AND name = %s", (dataset_id,))
        print(f"  dataset_metadata: {cur.rowcount} row(s) deleted")
        cur.execute("DELETE FROM dataset_versions WHERE layer = 'Master' AND name = %s", (dataset_id,))
        print(f"  dataset_versions: {cur.rowcount} row(s) deleted")
        cur.execute("DELETE FROM delta_compaction_log WHERE format_key = %s", (dataset_id,))
        print(f"  delta_compaction_log: {cur.rowcount} row(s) deleted")
        cur.execute("DELETE FROM pending_changes WHERE dataset_id = %s", (dataset_id,))
        print(f"  pending_changes: {cur.rowcount} row(s) deleted")

conn.close()
print("\nDone. Refresh the Datasets page — these should be gone now.")