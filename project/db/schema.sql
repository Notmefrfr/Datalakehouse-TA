-- PostgreSQL schema for the Lakehouse Analytics backend.
-- Run once against your PG_DB before starting the Flask app:
--   psql -h $PG_HOST -U $PG_USER -d $PG_DB -f db/schema.sql

CREATE TABLE IF NOT EXISTS users (
    id            SERIAL PRIMARY KEY,
    username      TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role          TEXT NOT NULL CHECK (role IN ('Administrator', 'Employee')),
    full_name     TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dataset_metadata (
    layer              TEXT NOT NULL,
    name               TEXT NOT NULL,
    display_name       TEXT,
    owner              TEXT,
    division_override  TEXT,
    archived           BOOLEAN NOT NULL DEFAULT false,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (layer, name)
);

CREATE TABLE IF NOT EXISTS dataset_versions (
    id          SERIAL PRIMARY KEY,
    layer       TEXT NOT NULL,
    name        TEXT NOT NULL,
    row_count   INTEGER NOT NULL,
    size_bytes  BIGINT NOT NULL,
    created_by  INTEGER REFERENCES users(id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    details     JSONB
);

-- Added for automatic merge: each row in dataset_versions where layer='Master'
-- is one upload-and-merge event for that format. 'details' carries the
-- Upload Summary stats (rows_uploaded, rows_added, duplicates_handled,
-- dedupe_mode) — this table doubles as the "Upload History" the spec asks
-- for, so no separate history table was needed.
ALTER TABLE dataset_versions ADD COLUMN IF NOT EXISTS details JSONB;

CREATE TABLE IF NOT EXISTS audit_log (
    id            SERIAL PRIMARY KEY,
    user_id       INTEGER REFERENCES users(id),
    username      TEXT NOT NULL,
    action        TEXT NOT NULL,
    dataset_name  TEXT,
    details        JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Populated by the Spark ETL service; this is what Tableau (or any BI tool)
-- reads for reporting, per the architecture's "PostgreSQL Reporting Tables" layer.
CREATE TABLE IF NOT EXISTS reporting_metrics (
    dataset_name  TEXT NOT NULL,
    metric_name   TEXT NOT NULL,
    metric_value  DOUBLE PRECISION NOT NULL,
    computed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset_name, metric_name)
);

-- Tracks when each Master format's Delta table was last small-file
-- compacted (see services/compaction_job.py). One row per format_key;
-- the background job compacts a format again once COMPACT_INTERVAL_DAYS
-- has passed since last_compacted_at (or immediately, if there's no row
-- yet for that format).
CREATE TABLE IF NOT EXISTS delta_compaction_log (
    format_key        TEXT PRIMARY KEY,
    last_compacted_at TIMESTAMPTZ NOT NULL,
    files_before      INTEGER,
    files_after       INTEGER,
    bytes_before      BIGINT,
    bytes_after       BIGINT,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_audit_log_created_at ON audit_log (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dataset_versions_layer_name ON dataset_versions (layer, name);
