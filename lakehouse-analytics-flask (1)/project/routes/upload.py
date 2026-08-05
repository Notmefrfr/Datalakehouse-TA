from flask import Blueprint, jsonify, request

from routes._common import catalog, config, login_required, minio, postgres, spark

bp = Blueprint("upload", __name__)

# Client sends a short code instead of the literal character — avoids
# whitespace-in-HTML-attribute headaches for "tab" specifically.
DELIMITER_MAP = {"comma": ",", "semicolon": ";", "tab": "\t", "pipe": "|"}


def _read_upload_file():
    file = request.files.get("file")
    division = (request.form.get("division") or "").strip()
    dtype = (request.form.get("dataset_type") or "").strip()
    if not file or not division or not dtype:
        return None, None, None, None, ("division, dataset_type and file are required.", 400)

    # Security: only .csv is accepted, checked on the filename before we even
    # read the content — the frontend also restricts the file picker/dropzone
    # to .csv, but that's a UI convenience only, not a real boundary; this is.
    if not file.filename.lower().endswith(".csv"):
        return None, None, None, None, ("Only .csv files are accepted.", 400)

    delimiter_code = (request.form.get("delimiter") or "comma").strip().lower()
    if delimiter_code not in DELIMITER_MAP:
        return None, None, None, None, (f"Unknown delimiter '{delimiter_code}'.", 400)
    delimiter = DELIMITER_MAP[delimiter_code]

    dtype_config = config().dataset_type_lookup(division, dtype)
    if not dtype_config:
        return None, None, None, None, ("Unknown division / dataset type.", 400)

    text = file.read().decode("utf-8", errors="replace")
    file.stream.seek(0)
    return file, dtype_config, text, delimiter, None


@bp.get("/upload/format-info")
@login_required
def format_info(user):
    """Powers the Upload page's 'About this format' panel: current master
    dataset stats + recent upload history for the selected division/type."""
    division = (request.args.get("division") or "").strip()
    dtype = (request.args.get("dataset_type") or "").strip()
    dtype_config = config().dataset_type_lookup(division, dtype)
    if not dtype_config:
        return jsonify({"error": "Unknown division / dataset type."}), 400

    if dtype_config.get("skip_merge"):
        return jsonify({"has_master": False, "skip_merge": True})

    format_key = catalog().format_key(division, dtype)
    meta = postgres().get_metadata("Master", format_key)
    if not meta:
        return jsonify({"has_master": False, "skip_merge": False})

    versions = postgres().list_versions("Master", format_key, limit=6)
    return jsonify({
        "has_master": True,
        "skip_merge": False,
        "total_rows": versions[0]["row_count"] if versions else 0,
        "upload_count": postgres().count_versions("Master", format_key),
        "last_updated": versions[0]["created_at"].isoformat() if versions else None,
        "history": [
            {
                "timestamp": v["created_at"].isoformat(),
                "rows_added": (v["details"] or {}).get("rows_added", 0),
                "uploaded_by": v["created_by_username"],
            }
            for v in versions
        ],
    })


@bp.post("/upload/validate")
@login_required
def validate_upload(user):
    file, dtype_config, text, delimiter, error = _read_upload_file()
    if error:
        message, status = error
        return jsonify({"valid": False, "checks": [{"ok": False, "message": message}]}), status

    result = spark().validate_upload(text, dtype_config["required_columns"], delimiter=delimiter)
    return jsonify(result)


@bp.post("/upload")
@login_required
def upload(user):
    file, dtype_config, text, delimiter, error = _read_upload_file()
    if error:
        message, status = error
        return jsonify({"error": message}), status

    division = request.form.get("division").strip()
    dtype = request.form.get("dataset_type").strip()

    result = spark().validate_upload(text, dtype_config["required_columns"], delimiter=delimiter)
    if not result["valid"]:
        return jsonify({"error": "File failed validation.", "checks": result["checks"]}), 400

    # "Other Format" (skip_merge) — unchanged behavior: every upload is its
    # own standalone Bronze dataset, never merged. Still canonicalized to
    # comma on the way in, same as everything else, so every other page
    # (Prepare/Visualize/Merge) can keep assuming comma-separated storage.
    if dtype_config.get("skip_merge"):
        stored_text = text if delimiter == "," else spark().to_csv_text(spark().read_csv_text(text, delimiter=delimiter))

        stub = catalog().new_stub(file.filename)
        key = catalog().bronze_key(division, dtype, stub)
        minio().put_object_text(key, stored_text, content_type="text/csv")

        name = f"{division}__{dtype}__{stub}"
        postgres().upsert_metadata(
            "Bronze", name,
            display_name=f"{dtype_config['label']} — {file.filename}",
            owner=user["full_name"] or user["username"],
            archived=False,
        )
        row_count = max(len([l for l in stored_text.splitlines() if l.strip()]) - 1, 0)
        postgres().record_version("Bronze", name, row_count=row_count, size_bytes=len(stored_text.encode("utf-8")), created_by=user["id"])
        postgres().log_action(user["id"], user["username"], "upload_dataset", name, {"division": division, "dataset_type": dtype})
        return jsonify({"ok": True, "layer": "Bronze", "name": name, "merged": False})

    # Predefined format — automatic merge into the one master dataset for
    # this division+dataset_type, per the "same format -> same file" spec.
    dedupe_mode = (request.form.get("dedupe_mode") or "remove").strip()
    if dedupe_mode not in ("remove", "replace", "keep", "append_raw"):
        return jsonify({"error": "Invalid dedupe_mode."}), 400

    key_columns_raw = (request.form.get("key_columns") or "").strip()
    key_columns = [c.strip() for c in key_columns_raw.split(",") if c.strip()]
    if dedupe_mode in ("remove", "replace") and not key_columns:
        key_columns = dtype_config["required_columns"][:1]  # sensible default
    if dedupe_mode in ("remove", "replace") and not key_columns:
        return jsonify({"error": "Pick at least one duplicate-key column."}), 400

    format_key = catalog().format_key(division, dtype)
    master_key = catalog().master_key(format_key)

    existing_text = None
    if minio().object_exists(master_key):
        existing_text = minio().get_object_text(master_key)

    try:
        merged_csv, stats = spark().merge_into_master(existing_text, text, dedupe_mode, key_columns, delimiter=delimiter)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    minio().put_object_text(master_key, merged_csv, content_type="text/csv")

    postgres().upsert_metadata(
        "Master", format_key,
        display_name=dtype_config["label"],
        owner=user["full_name"] or user["username"],
        division_override=config().DIVISION_LOOKUP.get(division, {}).get("label"),
        archived=False,
    )
    postgres().record_version(
        "Master", format_key,
        row_count=stats["total_rows"],
        size_bytes=len(merged_csv.encode("utf-8")),
        created_by=user["id"],
        details={
            "dedupe_mode": dedupe_mode,
            "key_columns": key_columns,
            "rows_uploaded": stats["rows_uploaded"],
            "rows_added": stats["rows_added"],
            "duplicates_handled": stats["duplicates_handled"],
        },
    )
    postgres().log_action(
        user["id"], user["username"], "merge_upload", format_key,
        {"division": division, "dataset_type": dtype, "dedupe_mode": dedupe_mode, **stats},
    )

    dup_label = "duplicates_replaced" if dedupe_mode == "replace" else "duplicates_removed"
    return jsonify({
        "ok": True,
        "layer": "Master",
        "name": format_key,
        "merged": True,
        "format_label": dtype_config["label"],
        "rows_uploaded": stats["rows_uploaded"],
        "rows_added": stats["rows_added"],
        "duplicates_handled": stats["duplicates_handled"],
        dup_label: stats["duplicates_handled"],
        "total_rows": stats["total_rows"],
        "status": "Merge Completed",
    })
