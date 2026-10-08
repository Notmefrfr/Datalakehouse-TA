-- Additive follow-up to db/dynamic_datasets_schema.sql — adds row-identity
-- ("key column") support so uploads that share the same identity (e.g. the
-- same employee_id) update that row in place instead of creating a
-- duplicate. Run the SAME way as before:
--
--   Get-Content db/dynamic_datasets_schema_v2.sql | docker exec -i telecom_postgres psql -U admin -d lakehouse
--
-- Safe to run more than once (IF NOT EXISTS).

-- The column(s) that identify "the same row" for a dataset — a single
-- column (["employee_id"]) or a compound key (["employee_id", "site"]).
-- NULL until the dataset's first upload sets it (defaults to that upload's
-- first column unless the uploader picks different ones explicitly — see
-- services/dataset_service.py). Kept in sync by
-- set_dataset_columns()/handle_upload() whenever a key is (re)chosen.
ALTER TABLE dataset_schema ADD COLUMN IF NOT EXISTS key_columns JSONB;
