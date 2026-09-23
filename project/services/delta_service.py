"""
Delta Lake service.

Owns every Master-layer dataset as a real Delta Lake table stored on MinIO
(S3-compatible), via delta-rs (the `deltalake` package) — no Spark/JVM
required, consistent with SPARK_MODE=local's pandas-in-process design.

This is what replaced the old "one giant master CSV, read + merged +
rewritten in full on every upload" scheme. Now:

  - Every upload lands as its own small Parquet file, either appended
    (dedupe_mode "keep"/"append_raw") or written via a real Delta MERGE
    upsert (dedupe_mode "remove"/"replace") — see append()/upsert() below.
  - A background job (services/compaction_job.py) periodically rewrites a
    format's accumulated small files into fewer, larger ones, capped at
    roughly Config.COMPACT_TARGET_FILE_SIZE_MB — see compact().
  - Every read (Visualize, Prepare, download, etc.) transparently sees the
    union of every part-file for that format as one table — see
    read_df() — so nothing downstream needs to know the data is split
    across files at all.

Routes and other services never import deltalake or touch S3 paths
directly — everything about "the master dataset for format X" goes
through this service, keyed by format_key ("division__dtype").
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
            # time per format_key.
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }

    def table_uri(self, format_key):
        return f"s3://{self.bucket}/{self.prefix}{format_key}/"

    def exists(self, format_key):
        return DeltaTable.is_deltatable(self.table_uri(format_key), storage_options=self._storage_options)

    def _open(self, format_key):
        return DeltaTable(self.table_uri(format_key), storage_options=self._storage_options)

    @staticmethod
    def validate_key_columns(key_columns):
        bad = [c for c in key_columns if not _SAFE_IDENTIFIER_RE.match(c)]
        if bad:
            raise ValueError(
                "Duplicate-key column name(s) must be letters, numbers or "
                "underscores only: " + ", ".join(bad)
            )

    # -- legacy migration ----------------------------------------------------

    def ensure_table(self, format_key):
        """Called before every read/write against a format's table. If the
        Delta table doesn't exist yet but an old (pre-Parquet) master CSV
        does — from before this migration ran — seed the Delta table from
        it once, so existing production data keeps working instead of
        silently disappearing the first time this deploys. A no-op on every
        call after that, for both new and migrated formats."""
        if self.exists(format_key):
            return
        legacy_key = f"{self.config.MASTER_PREFIX}{format_key}/data.csv"
        if self.minio.object_exists(legacy_key):
            text = self.minio.get_object_text(legacy_key)
            df = pd.read_csv(io.StringIO(text))
            self.overwrite(format_key, df)

    # -- write -----------------------------------------------------------------

    def append(self, format_key, df):
        """Plain append, no dedup — dedupe_mode 'keep'/'append_raw'. Always
        lands as a brand-new Parquet file; never rewrites existing ones."""
        write_deltalake(
            self.table_uri(format_key), pa.Table.from_pandas(df, preserve_index=False),
            mode="append", schema_mode="merge", storage_options=self._storage_options,
            target_file_size=self.config.COMPACT_TARGET_FILE_SIZE_MB * 1024 * 1024,
        )

    def overwrite(self, format_key, df):
        """Replaces the table's entire contents with df. Used to seed a
        brand-new format's first upload, for the legacy-CSV migration
        above, and for admin row edit/delete (routes/admin.py) — all
        infrequent, whole-dataset operations, unlike the per-upload
        append()/upsert() path uploads normally take."""
        write_deltalake(
            self.table_uri(format_key), pa.Table.from_pandas(df, preserve_index=False),
            mode="overwrite", schema_mode="overwrite", storage_options=self._storage_options,
            target_file_size=self.config.COMPACT_TARGET_FILE_SIZE_MB * 1024 * 1024,
        )

    def upsert(self, format_key, df, key_columns, replace_matches):
        """Real Delta MERGE against the format's table, keyed on
        key_columns. replace_matches=True is dedupe_mode 'replace' (new
        rows overwrite matching existing ones); False is dedupe_mode
        'remove' (existing rows win, new duplicates are simply dropped).

        Returns stats: rows_uploaded, rows_added, duplicates_handled,
        total_rows — the exact fields the Upload Summary panel displays,
        read straight back from delta-rs's own merge execution metrics
        rather than recomputed by hand.
        """
        self.validate_key_columns(key_columns)
        dt = self._open(format_key)
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
            "total_rows": self.count_rows(format_key),
        }

    # -- read --------------------------------------------------------------------

    def read_df(self, format_key):
        """Every part-file for this format, unioned into one DataFrame —
        this is what makes Visualize/Prepare/download keep seeing 'one
        dataset per category' exactly like before, live, no matter how many
        small Parquet files it's actually split across underneath."""
        if not self.exists(format_key):
            return None
        return self._open(format_key).to_pandas()

    def count_rows(self, format_key):
        if not self.exists(format_key):
            return 0
        return self._open(format_key).to_pyarrow_dataset().count_rows()

    def table_size_bytes(self, format_key):
        """Sum of actual stored Parquet file sizes (metadata-only — doesn't
        read any data), for catalog listings/stats."""
        if not self.exists(format_key):
            return 0
        actions = pa.table(self._open(format_key).get_add_actions())
        return int(sum(actions.column("size_bytes").to_pylist()))

    def file_count(self, format_key):
        if not self.exists(format_key):
            return 0
        return len(self._open(format_key).file_uris())

    # -- delete ------------------------------------------------------------------

    def drop_table(self, format_key):
        """Deletes every object under this format's Delta table prefix
        (data files + _delta_log) — used when a Master dataset is deleted
        outright, unlike compact()/vacuum() which only ever rewrite files,
        never remove the dataset itself."""
        self.minio.delete_prefix(f"{self.prefix}{format_key}/")

    # -- compaction ----------------------------------------------------------------

    def compact(self, format_key):
        """Rewrites this format's small Parquet files into fewer, larger
        ones (up to Config.COMPACT_TARGET_FILE_SIZE_MB each) — the fix for
        the small-file problem every-upload-is-its-own-file otherwise
        causes. Purely a file-layout rewrite: row content and history are
        unaffected, and it only touches files already under the target
        size, so a file that has already reached the target is left alone
        going forward (it's effectively sealed history at that point).
        Returns delta-rs's own metrics dict (files added/removed, bytes).
        """
        if not self.exists(format_key):
            return None
        dt = self._open(format_key)
        return dt.optimize.compact(target_size=self.config.COMPACT_TARGET_FILE_SIZE_MB * 1024 * 1024)
