"""
Dataset service — the "no more division/dataset_type" matching brain.

This replaces the old fixed-catalog model (Config.DIVISIONS, a required
division+dataset_type picked on every upload) with dynamic, column-based
matching: every upload is compared against every dataset that already
exists, and the system decides for itself whether it's the same dataset,
a schema change to an existing dataset, or a brand-new dataset — nothing
about the file's shape is declared up front by the user.

The rule (agreed 2026-09-24), applied uniformly regardless of how much the
upload's columns overlap an existing dataset's:

  1. Score every existing dataset by how many columns it shares with the
     upload. The dataset with the highest nonzero overlap wins. Zero
     overlap with everything -> this is a brand-new dataset (needs a name
     from the uploader).
  2. If the upload introduces ANY column the matched dataset doesn't
     already have -> this is a schema change. It needs an Administrator's
     approval before anything is written: the cleaned upload is staged in
     MinIO and a pending_changes row is created. An Administrator
     uploading it themselves is auto-approved immediately (see
     handle_upload's admin_is_requester short-circuit) since asking them
     to approve their own request would be pointless.
  3. Otherwise (every column in the upload is already part of the matched
     dataset's schema, however small the overlap) -> written via a real
     Delta MERGE (see below), no approval needed.

Row identity (added 2026-09-24, second round): a write into a matched
dataset is never a blind append — it's a MERGE keyed on that dataset's
key_columns (services/delta_service.py's upsert(), replace_matches=True):
  - A row whose key already exists gets UPDATED: every column the new
    upload actually carries overwrites the old value (new upload wins on
    conflict); a column the matched dataset has that this specific upload
    doesn't carry is left exactly as it was — never blanked out, since the
    upload simply didn't say anything about it this time.
  - A row whose key doesn't exist yet gets INSERTED, with any column it
    doesn't carry filled null (Delta's per-file schema tracking, same as
    before).
key_columns defaults to the first column of a dataset's very first upload,
but the uploader can pick a different single column or a compound key
explicitly (routes/upload.py's `key_columns` form field) — whatever was
last chosen is remembered per dataset (postgres().get_dataset_key_columns)
and reused as the default on the next upload to that same dataset.

routes/upload.py is the only caller of handle_upload(); routes/admin.py
calls approve_schema_change()/reject_schema_change() from the approval
queue UI.
"""
import os
import re
import tempfile
import time

import pandas as pd


def slugify(raw):
    """Turns a user-typed dataset name into a stable, URL/S3-key-safe
    dataset_id. Deliberately similar to catalog_service.sanitize_name (same
    job), but kept local here so this service has no import-time dependency
    on catalog_service — dataset_service is meant to be the lower-level
    piece catalog_service and routes both build on, not the other way
    around."""
    stub = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(raw)).strip("_").lower()
    return stub or "dataset"


class DatasetService:
    def __init__(self, app_config, minio, postgres, delta, spark):
        self.config = app_config
        self.minio = minio
        self.postgres = postgres
        self.delta = delta
        self.spark = spark
        # Staged (not-yet-approved) uploads live here — never under the
        # Delta prefix, so a rejected/abandoned request can never
        # accidentally show up as part of a real dataset's data.
        self.staging_prefix = "pending_uploads/"

    # -- matching --------------------------------------------------------------

    @staticmethod
    def _normalize_columns(columns):
        # Whitespace-trimmed, but case-sensitive and otherwise verbatim —
        # matching is about "is this genuinely the same column", and
        # silently folding case could merge two columns a real dataset
        # actually intends to keep distinct (e.g. "ID" vs "Id" from two
        # different source systems). Trimming is the one normalization
        # applied everywhere else in this codebase's column handling too.
        return [c.strip() for c in columns]

    def find_matching_dataset(self, upload_columns):
        """Returns (dataset_id, existing_columns, overlap_count) for the
        best-matching existing dataset, or (None, None, 0) if the upload's
        columns don't overlap ANY existing dataset at all — meaning it's a
        brand-new dataset. Ties (two datasets with equal overlap) resolve
        to whichever dataset_schema happened to sort first out of
        Postgres — genuinely ambiguous ties should be rare in practice
        (it needs two datasets sharing the exact same overlap COUNT with
        this specific upload), and when it happens either candidate is by
        definition an equally reasonable match."""
        upload_set = set(self._normalize_columns(upload_columns))
        all_schemas = self.postgres.list_all_dataset_schemas()  # {dataset_id: [columns...]}

        best_id, best_columns, best_overlap = None, None, 0
        for dataset_id, columns in all_schemas.items():
            overlap = len(upload_set & set(self._normalize_columns(columns)))
            if overlap > best_overlap:
                best_id, best_columns, best_overlap = dataset_id, columns, overlap
        return best_id, best_columns, best_overlap

    # -- upload entry point (streaming, big-data-safe) --------------------------

    def _iter_chunks(self, file_path, delimiter):
        """Yields the uploaded file as a sequence of DataFrames of at most
        Config.UPLOAD_CHUNK_ROWS rows each, instead of parsing it into one
        DataFrame — this is the one thing that makes a 50GB upload cost
        50GB of disk (the temp file routes/upload.py already saved it to)
        rather than 50GB of RAM.

        dtype=str is deliberate: pandas' C parser otherwise infers each
        column's dtype PER CHUNK independently (a column with no blanks in
        one chunk reads as int64, the very same column with one blank cell
        in the next chunk reads as float64), which corrupts a big upload's
        Delta table the moment a later chunk's schema doesn't match the
        first. Reading everything as a plain string here and letting
        _build_type_plan()'s single whole-file decision (passed into every
        clean_chunk() call below as type_plan) do the actual numeric/date
        conversion means every chunk ends up with the exact same dtype for
        a given column, no matter what that specific chunk happens to
        contain."""
        return pd.read_csv(file_path, sep=delimiter, dtype=str, keep_default_na=True,
                           chunksize=self.config.UPLOAD_CHUNK_ROWS)

    def _build_type_plan(self, file_path, delimiter, auto_clean_operations):
        """One up-front decision (see services/spark_service.py's
        infer_column_types()) every chunk's "fix_types" step must use —
        see _iter_chunks()'s docstring for why per-chunk decisions are
        unsafe. Reads only Config.TYPE_SAMPLE_ROWS rows, not the whole
        file. Returns None when "fix_types" wasn't requested at all."""
        if "fix_types" not in auto_clean_operations:
            return None
        sample_df = pd.read_csv(file_path, sep=delimiter, dtype=str, keep_default_na=True,
                                nrows=self.config.TYPE_SAMPLE_ROWS)
        return self.spark.infer_column_types(sample_df)

    def handle_upload_stream(self, file_path, delimiter, auto_clean_operations, new_dataset_name, user, key_columns=None):
        """The single entry point routes/upload.py calls for every upload.
        `file_path` is a temp file on disk (routes/upload.py streamed the
        raw upload there instead of holding it as one string) and is read
        and cleaned in chunks (see _iter_chunks) rather than all at once —
        see Config.MAX_UPLOAD_SIZE_GB / UPLOAD_CHUNK_ROWS / DEDUPE_GLOBAL_MAX_ROWS
        in config.py for the knobs this is built around.

        Every decision below — new dataset vs. merge vs. needs-approval,
        which key column(s) to match rows on — only ever depended on this
        file's COLUMN NAMES, never on the row data itself (true of the old
        whole-file-in-memory handle_upload() too), so that decision is made
        once up front from just the header; only the chunk loop touches
        actual rows. `key_columns`, if given, is the uploader's explicit
        choice of which column(s) identify "the same row" (a single column
        or a compound key) — see the row-identity section of this file's
        docstring. Returns a dict describing what happened — one of:

          {"status": "created", "dataset_id": ..., ...}
            Brand-new dataset, written immediately.
          {"status": "merged", "dataset_id": ..., ...}
            Matched an existing dataset, no new columns, merged immediately.
          {"status": "pending_approval", "pending_id": ..., "dataset_id": ...}
            Matched an existing dataset but introduced new column(s) — staged,
            waiting on an Administrator (unless the uploader already IS one,
            in which case this method applies it immediately instead and
            still returns "merged", never "pending_approval").

        Raises ValueError for anything the caller should turn into a 400
        (e.g. no match found and no new_dataset_name given; a key column
        that isn't actually present).
        """
        header_columns = list(pd.read_csv(file_path, sep=delimiter, nrows=0).columns)
        upload_set = set(self._normalize_columns(header_columns))
        requested_key = [c.strip() for c in key_columns] if key_columns else None
        dedupe_requested = "dedupe" in auto_clean_operations
        type_plan = self._build_type_plan(file_path, delimiter, auto_clean_operations)

        dataset_id, existing_columns, overlap = self.find_matching_dataset(header_columns)

        # -- Case 1: no existing dataset shares even one column -----------------
        if dataset_id is None:
            name = (new_dataset_name or "").strip()
            if not name:
                raise ValueError(
                    "This doesn't match any existing dataset's columns — "
                    "give it a name to create it as a new dataset."
                )
            new_id = slugify(name)
            if self.postgres.get_dataset_columns(new_id) is not None:
                # Slug collision (e.g. two different names that sanitize to
                # the same slug, or genuinely re-creating one that still
                # exists) — surfacing this beats silently overwriting
                # another dataset's schema record.
                raise ValueError(f"A dataset named '{name}' already exists. Pick a different name.")

            # Postgres has no record of this dataset_id (we just checked
            # above) — but a Delta table can still physically exist at its
            # MinIO path from an EARLIER attempt that crashed partway
            # through (e.g. chunk 0's overwrite() below succeeded, then a
            # later chunk's append() raised before this method ever reached
            # set_dataset_columns() to record it in Postgres — so a retry
            # sees "no match" and takes this exact brand-new-dataset branch
            # again). write_deltalake(mode="overwrite", schema_mode=
            # "overwrite") does NOT reliably reset a column's type in that
            # case: if this run's chunk 0 happens to produce an all-null
            # Arrow column for some field (a categorical column this chunk
            # has no data for yet), delta-rs silently KEEPS that column's
            # OLD type from the leftover table instead of adopting the new
            # (null) one — so a later chunk's real data for that same
            # column, typed however THIS run's data actually warrants, can
            # come out a different Arrow type than that inherited leftover
            # type and get rejected as a schema-mismatch "Cast error" by
            # append() below, on every retry, for a reason that has nothing
            # to do with the new upload's own data. A "new" dataset should
            # never inherit a stale table's schema, so drop whatever
            # storage is sitting there first — it's unreachable garbage
            # Postgres never pointed at anyway.
            if self.delta.exists(new_id):
                self.delta.drop_table(new_id)

            effective_key = requested_key or [header_columns[0]]
            bad = [c for c in effective_key if c not in upload_set]
            if bad:
                raise ValueError(f"Key column(s) not found in this file: {', '.join(bad)}")

            row_count = 0
            seen_hashes = set() if dedupe_requested else None
            for i, chunk in enumerate(self._iter_chunks(file_path, delimiter)):
                if seen_hashes is not None and row_count > self.config.DEDUPE_GLOBAL_MAX_ROWS:
                    seen_hashes = None  # too big to cross-chunk-dedupe in RAM; each chunk still dedupes itself
                cleaned = self.spark.clean_chunk(chunk, auto_clean_operations, seen_hashes=seen_hashes, type_plan=type_plan)
                if i == 0:
                    self.delta.overwrite(new_id, cleaned)  # creates the table even if this first chunk is empty
                elif not cleaned.empty:
                    self.delta.append(new_id, cleaned)
                row_count += len(cleaned)

            self.postgres.set_dataset_columns(new_id, header_columns, key_columns=effective_key)
            self.postgres.upsert_metadata("Master", new_id, display_name=name, owner=user["full_name"] or user["username"], archived=False)
            self.postgres.record_version("Master", new_id, row_count=row_count, size_bytes=self.delta.table_size_bytes(new_id), created_by=user["id"], details={"event": "dataset_created", "key_columns": effective_key})
            self.postgres.log_action(user["id"], user["username"], "create_dataset", new_id, {"columns": header_columns, "rows": row_count, "key_columns": effective_key})
            return {"status": "created", "dataset_id": new_id, "display_name": name, "rows_added": row_count, "total_rows": row_count, "key_columns": effective_key}

        # -- Case 2/3: matched an existing dataset -------------------------------
        existing_set = set(self._normalize_columns(existing_columns))
        new_columns = [c for c in self._normalize_columns(header_columns) if c not in existing_set]

        # Row identity for the MERGE below: whatever the uploader explicitly
        # picked this time, else whatever was chosen last time for this
        # dataset, else that dataset's first-ever column. Either way it
        # must actually be usable to match rows — present on BOTH sides,
        # not just one.
        effective_key = requested_key or self.postgres.get_dataset_key_columns(dataset_id) or [existing_columns[0]]
        bad = [c for c in effective_key if c not in upload_set or c not in existing_set]
        if bad:
            raise ValueError(
                f"Key column(s) {', '.join(bad)} must exist in both this file and "
                f"the matched dataset ('{dataset_id}') to identify matching rows — "
                "pick different key column(s)."
            )

        if new_columns and user["role"] != "Administrator":
            # Needs approval: stream the cleaned upload into a local temp
            # CSV one chunk at a time, then hand that temp FILE to MinIO
            # (minio.put_object_file — boto3's managed multipart upload)
            # instead of cleaned_df.to_csv(index=False), which used to
            # materialize the entire cleaned file as one Python string
            # before it ever reached MinIO. Nothing about the real dataset
            # changes until an admin acts on it (approve_schema_change
            # below, which streams it right back down the same way).
            staged_key = f"{self.staging_prefix}{dataset_id}_{int(time.time() * 1000)}.csv"
            all_columns = list(existing_columns) + new_columns
            tmp_fd, tmp_path = tempfile.mkstemp(prefix="staged_upload_", suffix=".csv")
            os.close(tmp_fd)
            try:
                seen_hashes = set() if dedupe_requested else None
                rows_written = 0
                with open(tmp_path, "w", newline="", encoding="utf-8") as out:
                    for i, chunk in enumerate(self._iter_chunks(file_path, delimiter)):
                        if seen_hashes is not None and rows_written > self.config.DEDUPE_GLOBAL_MAX_ROWS:
                            seen_hashes = None
                        cleaned = self.spark.clean_chunk(chunk, auto_clean_operations, seen_hashes=seen_hashes, type_plan=type_plan)
                        cleaned.to_csv(out, index=False, header=(i == 0))
                        rows_written += len(cleaned)
                self.minio.put_object_file(staged_key, tmp_path, content_type="text/csv")
            finally:
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

            pending_id = self.postgres.create_pending_change(
                "schema_change", dataset_id, user["id"], user["username"],
                {"staged_key": staged_key, "new_columns": new_columns, "all_upload_columns": all_columns, "key_columns": effective_key},
            )
            self.postgres.log_action(
                user["id"], user["username"], "request_schema_change", dataset_id,
                {"pending_id": pending_id, "new_columns": new_columns},
            )
            return {"status": "pending_approval", "pending_id": pending_id, "dataset_id": dataset_id, "new_columns": new_columns}

        # No new columns, OR the uploader is an Administrator introducing
        # new columns themselves (auto-approved — asking them to approve
        # their own upload would be circular). Either way this is a real
        # Delta MERGE keyed on effective_key, one chunk at a time: a
        # matching row is UPDATED (new upload's values win on any shared
        # column; a column this upload doesn't carry is left untouched on
        # that row), a non-matching row is INSERTED (missing columns
        # null-filled). merge_schema=True inside delta.upsert() lets
        # new_columns through without needing to pre-pad every chunk to a
        # wider column list.
        target_columns = list(existing_columns) + new_columns if new_columns else existing_columns
        rows_added_total = 0
        duplicates_handled_total = 0
        total_rows = self.delta.count_rows(dataset_id)
        seen_hashes = set() if dedupe_requested else None
        rows_seen = 0
        for chunk in self._iter_chunks(file_path, delimiter):
            if seen_hashes is not None and rows_seen > self.config.DEDUPE_GLOBAL_MAX_ROWS:
                seen_hashes = None
            cleaned = self.spark.clean_chunk(chunk, auto_clean_operations, seen_hashes=seen_hashes, type_plan=type_plan)
            rows_seen += len(cleaned)
            if cleaned.empty:
                continue
            stats = self.delta.upsert(dataset_id, cleaned, effective_key, replace_matches=True)
            rows_added_total += stats["rows_added"]
            duplicates_handled_total += stats["duplicates_handled"]
            total_rows = stats["total_rows"]

        self.postgres.set_dataset_columns(dataset_id, target_columns, key_columns=effective_key)
        self.postgres.record_version(
            "Master", dataset_id, row_count=total_rows, size_bytes=self.delta.table_size_bytes(dataset_id),
            created_by=user["id"], details={
                "event": "schema_change_self_approved" if new_columns else "merge_upsert",
                "new_columns": new_columns, "key_columns": effective_key,
                "rows_added": rows_added_total, "rows_updated": duplicates_handled_total,
            },
        )
        self.postgres.log_action(
            user["id"], user["username"], "merge_upload", dataset_id,
            {"rows_added": rows_added_total, "rows_updated": duplicates_handled_total, "new_columns": new_columns, "key_columns": effective_key},
        )
        return {
            "status": "merged", "dataset_id": dataset_id, "new_columns": new_columns, "key_columns": effective_key,
            "rows_added": rows_added_total, "rows_updated": duplicates_handled_total, "total_rows": total_rows,
        }

    # -- admin approval queue ----------------------------------------------------

    def approve_schema_change(self, pending_id, admin_user):
        change = self.postgres.get_pending_change(pending_id)
        if not change or change["change_type"] != "schema_change":
            raise ValueError("No such pending schema-change request.")
        if change["status"] != "pending":
            raise ValueError(f"This request was already {change['status']}.")

        payload = change["payload"]
        staged_key = payload["staged_key"]
        if not self.minio.object_exists(staged_key):
            raise ValueError("The staged upload for this request is missing — it may have already been applied or cleaned up. Ask the requester to upload again.")

        target_columns = payload["all_upload_columns"]
        # key_columns was stored on the payload by handle_upload_stream() at
        # request time specifically so this approval path can MERGE the
        # same way an auto-approved (no-new-column, or admin-requested)
        # upload does — a matching row is updated in place (new upload's
        # values win, untouched columns stay as they were) instead of the
        # old blind reindex()+append(), which is what caused duplicate rows
        # for uploads that re-share an existing row's identity. Falls back
        # to the dataset's first column for requests staged before this
        # field existed.
        key_columns = payload.get("key_columns") or [target_columns[0]]
        dataset_id = change["dataset_id"]

        # The staged file was already cleaned (trimmed/deduped/filled) when
        # it was uploaded (see handle_upload_stream) — this just streams it
        # back down (never as one in-memory string: minio.download_object_to_file()
        # writes straight to a temp file) and MERGEs it in chunks, exactly
        # like a live upload does, so a schema-change approval is just as
        # big-data-safe as the upload that created it. The one cleaning
        # step still needed here is "fix_types" — numbers/dates were
        # already coerced before staging, but they're sitting in the
        # staged CSV as plain text again, and re-reading that text in
        # per-chunk-inferred dtype would risk the exact same cross-chunk
        # dtype mismatch _iter_chunks()/_build_type_plan() exist to avoid,
        # so this re-derives one whole-file type plan for the staged file
        # too instead of letting each chunk decide for itself.
        rows_added_total = 0
        duplicates_handled_total = 0
        total_rows = self.delta.count_rows(dataset_id)
        tmp_fd, tmp_path = tempfile.mkstemp(prefix="approve_schema_change_", suffix=".csv")
        os.close(tmp_fd)
        try:
            self.minio.download_object_to_file(staged_key, tmp_path)
            type_plan = self._build_type_plan(tmp_path, ",", ["fix_types"])
            for chunk in pd.read_csv(tmp_path, dtype=str, keep_default_na=True, chunksize=self.config.UPLOAD_CHUNK_ROWS):
                if chunk.empty:
                    continue
                chunk = self.spark.clean_chunk(chunk, ["fix_types"], type_plan=type_plan)
                stats = self.delta.upsert(dataset_id, chunk, key_columns, replace_matches=True)
                rows_added_total += stats["rows_added"]
                duplicates_handled_total += stats["duplicates_handled"]
                total_rows = stats["total_rows"]
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass

        self.postgres.set_dataset_columns(dataset_id, target_columns, key_columns=key_columns)
        self.postgres.record_version(
            "Master", dataset_id, row_count=total_rows, size_bytes=self.delta.table_size_bytes(dataset_id),
            created_by=change["requested_by"], details={
                "event": "schema_change_approved", "new_columns": payload["new_columns"], "key_columns": key_columns,
                "rows_added": rows_added_total, "rows_updated": duplicates_handled_total, "approved_by": admin_user["username"],
            },
        )
        resolved = self.postgres.resolve_pending_change(pending_id, "approved", admin_user["id"])
        if not resolved:
            # Someone else (another admin tab, a second request) resolved it
            # between our read and here — the data write above already
            # happened, which is a harmless double-apply of the same rows at
            # worst (Delta just gets an extra small file), but flag it
            # rather than pretend this call uniquely owned the approval.
            raise ValueError("This request was resolved by someone else at the same moment — the data was still applied, but double-check the audit log.")
        self.minio.delete_object(staged_key)
        self.postgres.log_action(
            admin_user["id"], admin_user["username"], "approve_schema_change", dataset_id,
            {"pending_id": pending_id, "new_columns": payload["new_columns"], "key_columns": key_columns},
        )
        return {
            "dataset_id": dataset_id, "total_rows": total_rows, "new_columns": payload["new_columns"], "key_columns": key_columns,
            "rows_added": rows_added_total, "rows_updated": duplicates_handled_total,
        }

    def reject_schema_change(self, pending_id, admin_user):
        change = self.postgres.get_pending_change(pending_id)
        if not change or change["change_type"] != "schema_change":
            raise ValueError("No such pending schema-change request.")
        if change["status"] != "pending":
            raise ValueError(f"This request was already {change['status']}.")

        resolved = self.postgres.resolve_pending_change(pending_id, "rejected", admin_user["id"])
        if not resolved:
            raise ValueError("This request was already resolved by someone else.")

        staged_key = change["payload"].get("staged_key")
        if staged_key and self.minio.object_exists(staged_key):
            self.minio.delete_object(staged_key)
        self.postgres.log_action(admin_user["id"], admin_user["username"], "reject_schema_change", change["dataset_id"], {"pending_id": pending_id})
        return {"dataset_id": change["dataset_id"]}

    # -- rename (routes/datasets.py's rename endpoint) ----------------------
    #
    # Symmetric with the schema-change flow above, just carrying a
    # display_name instead of a staged upload: an Administrator's own
    # rename applies immediately (asking them to approve their own request
    # would be circular, same reasoning as handle_upload's admin
    # short-circuit); anyone else's rename is queued in pending_changes and
    # only takes effect once an Administrator approves it.

    def request_rename(self, dataset_id, new_display_name, user):
        if not self.postgres.get_dataset_columns(dataset_id):
            raise ValueError(f"Unknown dataset '{dataset_id}'.")
        new_display_name = (new_display_name or "").strip()
        if not new_display_name:
            raise ValueError("A new name is required.")

        if user["role"] == "Administrator":
            self.postgres.upsert_metadata("Master", dataset_id, display_name=new_display_name)
            self.postgres.log_action(user["id"], user["username"], "rename_dataset", dataset_id, {"new_display_name": new_display_name})
            return {"status": "renamed", "dataset_id": dataset_id, "display_name": new_display_name}

        pending_id = self.postgres.create_pending_change(
            "rename", dataset_id, user["id"], user["username"],
            {"new_display_name": new_display_name},
        )
        self.postgres.log_action(user["id"], user["username"], "request_rename", dataset_id, {"pending_id": pending_id, "new_display_name": new_display_name})
        return {"status": "pending_approval", "pending_id": pending_id, "dataset_id": dataset_id}

    def approve_rename(self, pending_id, admin_user):
        change = self.postgres.get_pending_change(pending_id)
        if not change or change["change_type"] != "rename":
            raise ValueError("No such pending rename request.")
        if change["status"] != "pending":
            raise ValueError(f"This request was already {change['status']}.")

        new_display_name = change["payload"]["new_display_name"]
        self.postgres.upsert_metadata("Master", change["dataset_id"], display_name=new_display_name)
        resolved = self.postgres.resolve_pending_change(pending_id, "approved", admin_user["id"])
        if not resolved:
            raise ValueError("This request was resolved by someone else at the same moment — the rename was still applied, but double-check the audit log.")
        self.postgres.log_action(admin_user["id"], admin_user["username"], "approve_rename", change["dataset_id"], {"pending_id": pending_id, "new_display_name": new_display_name})
        return {"dataset_id": change["dataset_id"], "display_name": new_display_name}

    def reject_rename(self, pending_id, admin_user):
        change = self.postgres.get_pending_change(pending_id)
        if not change or change["change_type"] != "rename":
            raise ValueError("No such pending rename request.")
        if change["status"] != "pending":
            raise ValueError(f"This request was already {change['status']}.")

        resolved = self.postgres.resolve_pending_change(pending_id, "rejected", admin_user["id"])
        if not resolved:
            raise ValueError("This request was already resolved by someone else.")
        self.postgres.log_action(admin_user["id"], admin_user["username"], "reject_rename", change["dataset_id"], {"pending_id": pending_id})
        return {"dataset_id": change["dataset_id"]}
