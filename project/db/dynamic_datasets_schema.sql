-- Additive schema for the "no more division/dataset_type" feature.
-- Run this the SAME way you ran db/schema.sql (it's separate on purpose —
-- so a mistake here can't accidentally re-truncate the file that already
-- gave you trouble once):
--
--   Get-Content db/dynamic_datasets_schema.sql | docker exec -i telecom_postgres psql -U admin -d lakehouse
--
-- Everything here uses IF NOT EXISTS, so it's safe to run more than once.

-- Every dataset is now identified purely by its own dataset_id (a slug
-- generated from the name given at creation), not by division+dataset_type.
-- This table is the fast lookup used at upload time to decide which
-- existing dataset (if any) a new file's columns match, WITHOUT having to
-- open every dataset's Delta table just to read its schema on every single
-- upload. services/dataset_service.py keeps this in sync with reality:
-- it's updated every time a dataset is created or a schema-change request
-- (new column) is approved.
CREATE TABLE IF NOT EXISTS dataset_schema (
    dataset_id   TEXT PRIMARY KEY,
    columns      JSONB NOT NULL,  -- ordered list of column names, e.g. ["outage_date", "region", "duration_minutes"]
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Anything that needs an Administrator's sign-off before it takes effect —
-- today that's exactly two things: a schema-change (an upload introduces a
-- column the matched dataset doesn't have yet) and a dataset rename request
-- from a non-admin. Nothing in `payload` is applied to real data until a
-- row here is 'approved' (see services/dataset_service.py for
-- schema_change and routes/datasets.py for rename).
--
-- change_type = 'schema_change': payload is
--   {"staged_key": "<MinIO key of the cleaned, not-yet-written upload>",
--    "new_columns": ["col_a", "col_b"],
--    "all_upload_columns": ["existing_col", "col_a", "col_b"]}
-- change_type = 'rename': payload is {"new_display_name": "..."}
CREATE TABLE IF NOT EXISTS pending_changes (
    id                     SERIAL PRIMARY KEY,
    change_type            TEXT NOT NULL CHECK (change_type IN ('schema_change', 'rename')),
    dataset_id             TEXT NOT NULL,
    requested_by           INTEGER REFERENCES users(id),
    requested_by_username  TEXT NOT NULL,
    status                 TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected')),
    payload                JSONB NOT NULL,
    reviewed_by            INTEGER REFERENCES users(id),
    reviewed_at            TIMESTAMPTZ,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pending_changes_status ON pending_changes (status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_pending_changes_dataset ON pending_changes (dataset_id);
