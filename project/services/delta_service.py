"""
Delta Lake service.

Owns every dataset as a real Delta Lake table stored on MinIO (S3-compatible),
via delta-rs (the `deltalake` package) — no Spark/JVM required, consistent
with SPARK_MODE=local's pandas-in-process design.

  - Every upload lands as its own small Parquet file, either appended
    (dedupe_mode "keep"/"append_raw") or written via a real Delta MERGE
    upsert (dedupe_mode "remove"/"replace") — see append()/upsert() below.
  - A background job (services/compaction_job.py) periodically rewrites a
    dataset's accumulated small files into fewer, larger ones, capped at
    roughly Config.COMPACT_TARGET_FILE_SIZE_MB — see compact().
  - Every read (Visualize, Prepare, download, etc.) transparently sees the
    union of every part-file for that dataset as one table — see
    read_df() — so nothing downstream needs to know the data is split
    across files at all.

Since the "no more division/dataset_type" feature, every dataset is keyed
purely by its own dataset_id (a slug, e.g. "outage_report_2026"), not by
"division__dtype" — nothing in this file actually depended on that naming
convention, so no change was needed here beyond adding get_columns()
(services/dataset_service.py uses it to compare an upload's columns against
an existing dataset's real, current schema).

Routes and other services never import deltalake or touch S3 paths
directly — everything about "the dataset with this id" goes through this
service.
"""
import io
import re

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

# Only alphanumeric/underscore column names are accepted as dedupe key
# columns. They get interpolated into a MERGE predicate string handed to
# delta-rs's SQL engine (datafusion) — this is what keeps that string safe
# to build from user-supplied form input without a real SQL-escaping layer.
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")


class DeltaService:
    def __init__(self, app_config, minio):
        self.config = app_config
        self.minio = minio  # only used for the one-time legacy-CSV migration below
        self.bucket = app_config.MINIO_BUCKET
        self.prefix = app_config.MASTER_DELTA_PREFIX
        self._storage_options = {
            "AWS_ENDPOINT_URL": app_config.MINIO_ENDPOINT,
            "AWS_ACCESS_KEY_ID": app_config.MINIO_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": app_config.MINIO_SECRET_KEY,
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "false" if app_config.MINIO_SECURE else "true",
            # MinIO doesn't implement the conditional-PUT dance delta-rs
            # normally uses to arbitrate concurrent writers on real S3.
            # Safe here because postgres().advisory_lock() in
            # routes/upload.py already guarantees only one writer at a
            # time per dataset_id.
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }

    def table_uri(self, dataset_id):
        return f"s3://{self.bucket}/{self.prefix}{dataset_id}/"

    def exists(self, dataset_id):
        return DeltaTable.is_deltatable(self.table_uri(dataset_id), storage_options=self._storage_options)

    def _open(self, dataset_id):
        return DeltaTable(self.table_uri(dataset_id), storage_options=self._storage_options)

    @staticmethod
    def validate_key_columns(key_columns):
        bad = [c for c in key_columns if not _SAFE_IDENTIFIER_RE.match(c)]
        if bad:
            raise ValueError(
                "Duplicate-key column name(s) must be letters, numbers or "
                "underscores only: " + ", ".join(bad)
            )

    # -- schema introspection (services/dataset_service.py matching) -----------

    def get_columns(self, dataset_id):
        """The dataset's actual current column names, straight from the
        Delta table's own schema (the source of truth) — used as a
        fallback/verification alongside Postgres' dataset_schema table
        (which exists purely so upload-time matching doesn't have to open
        every dataset's Delta table on every upload). Returns None if the
        dataset doesn't exist yet."""
        if not self.exists(dataset_id):
            return None
        return [f.name for f in self._open(dataset_id).schema().fields]

    # -- legacy migration ----------------------------------------------------

    def ensure_table(self, format_key):
        """Called before every read/write against a dataset's table. If the
        Delta table doesn't exist yet but an old (pre-Parquet) master CSV
        does — from before this migration ran — seed the Delta table from
        it once, so existing production data keeps working instead of
        silently disappearing the first time this deploys. A no-op on every
        call after that, for both new and migrated datasets."""
        if self.exists(format_key):
            return
        legacy_key = f"{self.config.MASTER_PREFIX}{format_key}/data.csv"
        if self.minio.object_exists(legacy_key):
            text = self.minio.get_object_text(legacy_key)
            df = pd.read_csv(io.StringIO(text))
            self.overwrite(format_key, df)

    # -- write -----------------------------------------------------------------

    def append(self, dataset_id, df):
        """Plain append, no dedup — dedupe_mode 'keep'/'append_raw', and the
        primary write path for services/dataset_service.py's dynamic
        matching (both the "no new columns" case and, on admin approval,
        the "new columns" case). schema_mode="merge" is what lets a
        DataFrame carrying columns the table doesn't have yet through
        without erroring — Delta records the wider schema going forward and
        transparently reads null for any older file that predates a given
        column. Always lands as a brand-new Parquet file; never rewrites
        existing ones."""
        write_deltalake(
            self.table_uri(dataset_id), pa.Table.from_pandas(df, preserve_index=False),
            mode="append", schema_mode="merge", storage_options=self._storage_options,
            target_file_size=self.config.COMPACT_TARGET_FILE_SIZE_MB * 1024 * 1024,
        )

    def overwrite(self, dataset_id, df):
        """Replaces the table's entire contents with df. Used to seed a
        brand-new dataset's first upload, for the legacy-CSV migration
        above, and for admin row edit/delete (routes/admin.py) — all
        infrequent, whole-dataset operations, unlike the per-upload
        append()/upsert() path uploads normally take."""
        write_deltalake(
            self.table_uri(dataset_id), pa.Table.from_pandas(df, preserve_index=False),
            mode="overwrite", schema_mode="overwrite", storage_options=self._storage_options,
            target_file_size=self.config.COMPACT_TARGET_FILE_SIZE_MB * 1024 * 1024,
        )

    def upsert(self, dataset_id, df, key_columns, replace_matches):
        """Real Delta MERGE against the dataset's table, keyed on
        key_columns. replace_matches=True is dedupe_mode 'replace' (new
        rows overwrite matching existing ones); False is dedupe_mode
        'remove' (existing rows win, new duplicates are simply dropped).

        Returns stats: rows_uploaded, rows_added, duplicates_handled,
        total_rows — the exact fields the Upload Summary panel displays,
        read straight back from delta-rs's own merge execution metrics
        rather than recomputed by hand.
        """
        self.validate_key_columns(key_columns)
        dt = self._open(dataset_id)
        predicate = " AND ".join(f'target."{c}" = source."{c}"' for c in key_columns)

        merger = dt.merge(
            source=pa.Table.from_pandas(df, preserve_index=False),
            predicate=predicate,
            source_alias="source",
            target_alias="target",
            merge_schema=True,
        )
        if replace_matches:
            merger = merger.when_matched_update_all()
        merger = merger.when_not_matched_insert_all()
        metrics = merger.execute()

        rows_uploaded = int(metrics["num_source_rows"])
        rows_added = int(metrics["num_target_rows_inserted"])
        duplicates_handled = int(metrics["num_target_rows_updated"]) if replace_matches \
            else rows_uploaded - rows_added
        return {
            "rows_uploaded": rows_uploaded,
            "rows_added": rows_added,
            "duplicates_handled": duplicates_handled,
            "total_rows": self.count_rows(dataset_id),
        }

    # -- read --------------------------------------------------------------------

    def read_df(self, dataset_id):
        """Every part-file for this dataset, unioned into one DataFrame —
        this is what makes Visualize/Prepare/download keep seeing 'one
        dataset' exactly like before, live, no matter how many small
        Parquet files it's actually split across underneath, and no matter
        how many times its schema has grown via approved column additions."""
        if not self.exists(dataset_id):
            return None
        return self._open(dataset_id).to_pandas()

    def count_rows(self, dataset_id):
        if not self.exists(dataset_id):
            return 0
        return self._open(dataset_id).to_pyarrow_dataset().count_rows()

    def read_dataset_arrow(self, dataset_id):
        """A lazy pyarrow.dataset.Dataset over this table's Parquet files — unlike
        to_pandas() (used by read_dataset_df), this does NOT read any row data yet.
        DuckDB can register and query it directly (services/sql_sandbox.py), pushing
        column/predicate pruning down to the Parquet files and streaming through data
        larger than RAM, which is what makes services/chat_agent.py's run_query safe
        on big datasets. Returns None if the dataset doesn't exist."""
        if not self.exists(dataset_id):
            return None
        return self._open(dataset_id).to_pyarrow_dataset()

    def table_size_bytes(self, dataset_id):
        """Sum of actual stored Parquet file sizes (metadata-only — doesn't
        read any data), for catalog listings/stats."""
        if not self.exists(dataset_id):
            return 0
        actions = pa.table(self._open(dataset_id).get_add_actions())
        return int(sum(actions.column("size_bytes").to_pylist()))

    def file_count(self, dataset_id):
        if not self.exists(dataset_id):
            return 0
        return len(self._open(dataset_id).file_uris())

    # -- delete ------------------------------------------------------------------

    def drop_table(self, dataset_id):
        """Deletes every object under this dataset's Delta table prefix
        (data files + _delta_log) — used when a dataset is deleted
        outright, unlike compact()/vacuum() which only ever rewrite files,
        never remove the dataset itself."""
        self.minio.delete_prefix(f"{self.prefix}{dataset_id}/")

    # -- compaction ----------------------------------------------------------------

    def compact(self, dataset_id):
        """Rewrites this dataset's small Parquet files into fewer, larger
        ones (up to Config.COMPACT_TARGET_FILE_SIZE_MB each) — the fix for
        the small-file problem every-upload-is-its-own-file otherwise
        causes. Purely a file-layout rewrite: row content and history are
        unaffected, and it only touches files already under the target
        size, so a file that has already reached the target is left alone
        going forward (it's effectively sealed history at that point).
        Returns delta-rs's own metrics dict (files added/removed, bytes).
        """
        if not self.exists(dataset_id):
            return None
        dt = self._open(dataset_id)
        return dt.optimize.compact(target_size=self.config.COMPACT_TARGET_FILE_SIZE_MB * 1024 * 1024)
