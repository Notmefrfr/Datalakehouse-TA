"""
Catalog service: everything about *which datasets exist and what they're
named* — combining Delta/MinIO storage with Postgres metadata.

CHANGED (dynamic-dataset feature, 2026-09-24): there is no more
division/Bronze-Silver-Gold-per-format catalog to scan. Every dataset is
now exactly one real Delta Lake table, identified purely by its own
dataset_id — postgres().list_all_dataset_schemas() (backed by the new
dataset_schema table) is the source of truth for which datasets exist,
same role dataset_metadata used to play for "Master" layer names alone.
This file no longer has a `layer` concept: Bronze (skip_merge, one file
per upload) and the division/dtype-keyed Master tables are both gone,
replaced by dataset_service.py's single append-with-null-fill rule for
every dataset. Every route that used to pass `layer` now just passes
`dataset_id` — see routes/datasets.py and routes/admin.py.
"""
from datetime import datetime, timezone


class CatalogService:
    def __init__(self, config, minio, postgres, delta):
        self.config = config
        self.minio = minio
        self.postgres = postgres
        self.delta = delta

    # -- read / write ------------------------------------------------------------

    def read_dataset_df(self, dataset_id):
        """The one place that knows how to actually fetch a dataset's data,
        as a DataFrame. Every dataset is a Delta table now, so this is a
        thin wrapper — kept as its own method (rather than inlining
        delta().read_df() everywhere) so callers don't need to know that."""
        df = self.delta.read_df(dataset_id)
        if df is None:
            raise ValueError(f"Unknown dataset '{dataset_id}'.")
        return df

    def read_dataset_csv(self, dataset_id):
        return self.read_dataset_df(dataset_id).to_csv(index=False)

    def read_dataset_arrow(self, dataset_id):
        """Lazy pyarrow.dataset.Dataset — see delta.read_dataset_arrow()'s docstring.
        Used only by the AI assistant's run_query (services/chat_agent.py) so a big
        dataset's full table never has to be materialized into pandas just to answer
        one aggregate query."""
        ds = self.delta.read_dataset_arrow(dataset_id)
        if ds is None:
            raise ValueError(f"Unknown dataset '{dataset_id}'.")
        return ds

    def true_row_count(self, dataset_id):
        """Exact row count straight from Parquet/Delta metadata — no data read, safe
        to call even on a huge dataset. Used to caption sampled results honestly
        (services/chat_agent.py) rather than reporting a capped/sampled size as if
        it were the whole dataset."""
        return self.delta.count_rows(dataset_id)

    def write_dataset_df(self, dataset_id, df):
        """Persists a full-dataset replacement — used by admin row
        edit/delete (routes/admin.py). Infrequent, unlike the per-upload
        append path normal uploads take (services/dataset_service.py)."""
        if not self.delta.exists(dataset_id):
            raise ValueError(f"Unknown dataset '{dataset_id}'.")
        self.delta.overwrite(dataset_id, df)
        self.postgres.set_dataset_columns(dataset_id, list(df.columns))

    def delete_dataset_storage(self, dataset_id):
        if not self.delta.exists(dataset_id):
            raise ValueError(f"Unknown dataset '{dataset_id}'.")
        self.delta.drop_table(dataset_id)
        self.postgres.delete_compaction_state(dataset_id)
        self.postgres.delete_dataset_schema(dataset_id)

    # -- catalog listing -----------------------------------------------------

    @staticmethod
    def _time_ago(dt):
        if dt is None:
            return "—"
        seconds = (datetime.now(timezone.utc) - dt).total_seconds()
        if seconds < 60:
            return "just now"
        minutes = int(seconds // 60)
        if minutes < 60:
            return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        hours = int(minutes // 60)
        if hours < 24:
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        days = int(hours // 24)
        return f"{days} day{'s' if days != 1 else ''} ago"

    @staticmethod
    def _format_date(dt):
        return dt.strftime("%Y-%m-%d %H:%M") if dt else "—"

    def list_catalog(self, include_archived=False):
        """One entry per dataset_id in dataset_schema. Row/column counts and
        size come straight from the Delta table's own metadata (count_rows/
        get_columns/table_size_bytes) — none of these read the actual data,
        so listing the catalog stays cheap even as datasets grow."""
        entries = []
        all_schemas = self.postgres.list_all_dataset_schemas()  # {dataset_id: [columns...]}

        for dataset_id, columns in all_schemas.items():
            meta = self.postgres.get_metadata("Master", dataset_id)
            if meta.get("archived") and not include_archived:
                continue

            try:
                rows = self.delta.count_rows(dataset_id)
                status = "Active" if rows > 0 else "Error"
            except Exception:
                rows, status = 0, "Error"

            display_name = meta.get("display_name") or dataset_id
            entries.append({
                "dataset_id": dataset_id,
                "display_name": display_name,
                "owner": meta.get("owner") or "Workspace",
                "rows": rows,
                "columns": len(columns),
                "status": status,
                "upload_count": self.postgres.count_versions("Master", dataset_id),
                "last_updated": self._format_date(meta.get("updated_at")),
                "_last_modified_raw": meta.get("updated_at"),
                "_size_bytes": self.delta.table_size_bytes(dataset_id),
                "archived": bool(meta.get("archived")),
            })
        return entries

    def compute_stats(self, user_count):
        catalog = self.list_catalog(include_archived=False)
        total_size_mb = sum(e["_size_bytes"] for e in catalog) / 1024 / 1024

        # Replaces the old "storage by division" breakdown (there are no
        # divisions anymore) with per-dataset storage share — the frontend
        # widget that rendered this list needs updating to read
        # "dataset"/"mb" instead of "division"/"mb"; flagged separately.
        storage_list = sorted(
            ({"dataset": e["display_name"], "mb": e["_size_bytes"] / 1024 / 1024} for e in catalog),
            key=lambda x: x["mb"], reverse=True,
        )[:10]

        recent = sorted(
            (e for e in catalog if e["_last_modified_raw"]),
            key=lambda e: e["_last_modified_raw"], reverse=True,
        )[:6]
        activity = [
            {
                "dataset": e["display_name"],
                "action": "was updated",
                "time_ago": self._time_ago(e["_last_modified_raw"]),
            }
            for e in recent
        ]

        return {
            "total_datasets": len(catalog),
            "storage_used_mb": total_size_mb,
            "total_files": len(catalog),
            "active_jobs": 0,
            "user_count": user_count,
            "storage_by_dataset": storage_list,
            "recent_activity": activity,
        }
