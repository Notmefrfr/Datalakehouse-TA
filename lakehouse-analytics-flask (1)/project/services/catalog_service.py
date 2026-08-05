"""
Catalog service: everything about *which datasets exist, where, and what
they're named* — combining raw MinIO object listings with Postgres metadata.
This used to run in the browser against MinIO directly; it now runs here.
"""
import re
import time
from datetime import datetime, timezone


def sanitize_name(raw):
    stub = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(raw)).strip("_")
    return stub or "dataset"


class CatalogService:
    def __init__(self, config, minio, postgres):
        self.config = config
        self.minio = minio
        self.postgres = postgres

    # -- object key helpers ---------------------------------------------------

    def bronze_key(self, division, dtype, stub):
        return f"{self.config.BRONZE_PREFIX}{division}/{dtype}/{stub}.csv"

    @staticmethod
    def format_key(division, dtype):
        """The one stable identifier for a predefined format's master dataset —
        same division+dtype always resolves to the same master file."""
        return f"{division}__{dtype}"

    def master_key(self, format_key):
        return f"{self.config.MASTER_PREFIX}{format_key}/data.csv"

    def parse_bronze_name(self, name):
        parts = name.split("__")
        return parts if len(parts) == 3 else None

    def object_key_for(self, layer, name):
        if layer == "Bronze":
            parsed = self.parse_bronze_name(name)
            if not parsed:
                return None
            division, dtype, stub = parsed
            return self.bronze_key(division, dtype, stub)
        if layer == "Silver":
            return f"{self.config.SILVER_PREFIX}{name}/data.csv"
        if layer == "Gold":
            return f"{self.config.GOLD_PREFIX}{name}/data.csv"
        if layer == "Master":
            return self.master_key(name)
        return None

    def new_stub(self, original_filename):
        base = sanitize_name(original_filename.rsplit(".", 1)[0])
        return f"{base}_{int(time.time() * 1000)}"

    # -- read / write ------------------------------------------------------------

    def read_dataset_csv(self, layer, name):
        key = self.object_key_for(layer, name)
        if not key:
            raise ValueError(f"Unknown dataset '{name}' in layer '{layer}'")
        return self.minio.get_object_text(key)

    def division_label(self, division_id):
        d = self.config.DIVISION_LOOKUP.get(division_id)
        return d["label"] if d else division_id

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
        entries = []

        for obj in self.minio.list_all_objects(self.config.BRONZE_PREFIX):
            key = obj["Key"]
            if not key.endswith(".csv"):
                continue
            rel = key[len(self.config.BRONZE_PREFIX):-len(".csv")]
            pieces = rel.split("/")
            if len(pieces) != 3:
                continue
            division, dtype, stub = pieces
            entries.append({
                "layer": "Bronze", "name": f"{division}__{dtype}__{stub}",
                "division_id": division, "last_modified": obj["LastModified"], "size_bytes": obj["Size"],
            })

        for layer, prefix in (("Silver", self.config.SILVER_PREFIX), ("Gold", self.config.GOLD_PREFIX), ("Master", self.config.MASTER_PREFIX)):
            seen = set()
            for obj in self.minio.list_all_objects(prefix):
                key = obj["Key"]
                if not key.endswith("/data.csv"):
                    continue
                name = key[len(prefix):-len("/data.csv")]
                if name in seen:
                    continue
                seen.add(name)
                entries.append({
                    "layer": layer, "name": name, "division_id": None,
                    "last_modified": obj["LastModified"], "size_bytes": obj["Size"],
                })

        enriched = []
        for e in entries:
            meta = self.postgres.get_metadata(e["layer"], e["name"])
            if meta.get("archived") and not include_archived:
                continue

            rows, cols, status = 0, 0, "Error"
            try:
                text = self.read_dataset_csv(e["layer"], e["name"])
                lines = [l for l in text.splitlines() if l.strip()]
                cols = len(lines[0].split(",")) if lines else 0
                rows = max(len(lines) - 1, 0)
                status = "Active" if rows > 0 else "Error"
            except Exception:
                pass

            division = (
                meta.get("division_override")
                or (self.division_label(e["division_id"]) if e["division_id"] else None)
                or "General"
            )

            display_name = meta.get("display_name") or e["name"].split("__")[-1]
            upload_count = None

            if e["layer"] == "Master":
                # Master names are format_keys ("division__dtype"), not the
                # 3-part Bronze naming — resolve division/label from config.
                parts = e["name"].split("__", 1)
                if len(parts) == 2:
                    division_id, dtype_id = parts
                    division = self.division_label(division_id)
                    dtype_conf = self.config.dataset_type_lookup(division_id, dtype_id)
                    if dtype_conf and not meta.get("display_name"):
                        display_name = dtype_conf["label"]
                upload_count = self.postgres.count_versions("Master", e["name"])

            enriched.append({
                "layer": e["layer"],
                "name": e["name"],
                "display_name": display_name,
                "division": division,
                "owner": meta.get("owner") or "Workspace",
                "rows": rows,
                "columns": cols,
                "status": status,
                "upload_count": upload_count,
                "last_updated": self._format_date(e["last_modified"]),
                "_last_modified_raw": e["last_modified"],
                "_size_bytes": e["size_bytes"],
                "archived": bool(meta.get("archived")),
            })
        return enriched

    def compute_stats(self, user_count):
        catalog = self.list_catalog(include_archived=False)
        total_size_mb = sum(e["_size_bytes"] for e in catalog) / 1024 / 1024

        by_division = {}
        for e in catalog:
            if e["layer"] not in ("Bronze", "Master"):
                continue
            by_division[e["division"]] = by_division.get(e["division"], 0) + e["_size_bytes"] / 1024 / 1024
        storage_list = sorted(
            ({"division": k, "mb": v} for k, v in by_division.items()),
            key=lambda x: x["mb"], reverse=True,
        )

        recent = sorted(
            (e for e in catalog if e["_last_modified_raw"]),
            key=lambda e: e["_last_modified_raw"], reverse=True,
        )[:6]
        action_by_layer = {"Bronze": "was uploaded", "Silver": "was cleaned in Prepare", "Gold": "was created in Merge", "Master": "was updated (automatic merge)"}
        activity = [
            {
                "dataset": e["display_name"],
                "action": action_by_layer.get(e["layer"], "was updated"),
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
            "storage_by_division": storage_list,
            "recent_activity": activity,
        }
