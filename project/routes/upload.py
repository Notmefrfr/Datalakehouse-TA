import csv
import io
import re
from contextlib import contextmanager, nullcontext

from flask import Blueprint, current_app, jsonify, request

from routes._common import catalog, config, login_required, minio, postgres, spark

bp = Blueprint("upload", __name__)


@contextmanager
def _upload_lock(lock_key):
    """Wraps postgres().advisory_lock(lock_key) — but degrades to a no-op if
    that method isn't defined yet (e.g. postgres_service.py hasn't been
    updated to add it). TEMPORARY: without the real lock, two uploads to the
    same master dataset at nearly the same time can still silently overwrite
    each other (the exact race condition advisory_lock was added to fix).
    This fallback only exists so upload doesn't hard-crash with
    AttributeError while that file catches up — remove this once
    postgres_service.py genuinely has advisory_lock."""
    pg = postgres()
    if hasattr(pg, "advisory_lock"):
        with pg.advisory_lock(lock_key):
            yield
    else:
        current_app.logger.warning(
            "postgres().advisory_lock() is not implemented yet — uploading "
            "without the race-condition lock. See routes/upload.py's "
            "_upload_lock() for what's missing."
        )
        with nullcontext():
            yield


# The only delimiters we know how to canonicalize down to comma. Detection
# below never returns anything outside this set.
DELIMITER_MAP = {"comma": ",", "semicolon": ";", "tab": "\t", "pipe": "|"}
DELIMITER_LABELS = {",": "Comma ( , )", ";": "Semicolon ( ; )", "\t": "Tab", "|": "Pipe ( | )"}

# Applied automatically to every upload, right after validation and before
# anything is written to storage — reuses spark().clean(), the exact same
# Silver Transformation logic the (now-removed) manual Prepare step used to
# call. This is what makes "upload → already clean" true: nothing raw is
# ever stored for the rest of the app to see. All five known operations
# run every time; there's no per-upload choice to make anymore, matching
# "no more manual cleaning UI."
AUTO_CLEAN_OPERATIONS = ["trim", "normalize_case", "fix_types", "dedupe", "fill_missing"]

# Any HTML tag (<img ...>, <script>, </script>, <svg ...>, etc.) or a
# javascript: URI in a header or cell gets the whole file rejected outright.
# This is a defense-in-depth control on top of (not instead of) escaping the
# data on output — it stops the payload from ever being stored at all,
# rather than relying solely on every future render path staying safe.
_HTML_TAG_RE = re.compile(r"<\s*/?\s*[a-zA-Z!][^>]*>")
_JS_URI_RE = re.compile(r"javascript\s*:", re.IGNORECASE)


def _find_markup(text, delimiter):
    """Scan every header and cell for HTML/script-like content.
    Returns a list of {"location": ..., "column": ...} dicts; empty if clean.
    Deliberately does NOT include the offending text itself — the client
    only needs to know where the problem is, not see the payload again."""
    findings = []
    try:
        rows = list(csv.reader(io.StringIO(text), delimiter=delimiter))
    except csv.Error:
        return findings  # let the normal CSV validation report the parse error instead

    if not rows:
        return findings

    header = rows[0]
    for col_index, cell in enumerate(header):
        if _HTML_TAG_RE.search(cell) or _JS_URI_RE.search(cell):
            findings.append({"location": "Header row", "column": f"column {col_index + 1}"})

    for row_index, row in enumerate(rows[1:], start=1):
        for col_index, cell in enumerate(row):
            if _HTML_TAG_RE.search(cell) or _JS_URI_RE.search(cell):
                col_name = header[col_index] if col_index < len(header) else f"column {col_index + 1}"
                findings.append({"location": f"Row {row_index}", "column": col_name})

    return findings


def _detect_delimiter(text):
    """Auto-detect which of comma/semicolon/tab/pipe a freshly-uploaded file
    uses, so the user never has to pick it themselves — the file is then
    canonicalized to comma on the way into storage, same as before.

    Tries csv.Sniffer on the first few non-empty lines first (it correctly
    ignores delimiter-look-alike characters that show up inside quoted
    fields); falls back to counting each candidate's occurrences on the
    header line if the sniffer can't decide. Defaults to comma — which is
    also correct for a genuinely single-column file.
    """
    sample_lines = [l for l in text.splitlines() if l.strip()][:10]
    if not sample_lines:
        return ","
    sample = "\n".join(sample_lines)

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters="".join(DELIMITER_MAP.values()))
        if dialect.delimiter in DELIMITER_MAP.values():
            return dialect.delimiter
    except csv.Error:
        pass  # fall through to the frequency-count heuristic below

    header = sample_lines[0]
    counts = {d: header.count(d) for d in DELIMITER_MAP.values()}
    best = max(counts, key=counts.get)
    return best if counts[best] > 0 else ","


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

    dtype_config = config().dataset_type_lookup(division, dtype)
    if not dtype_config:
        return None, None, None, None, ("Unknown division / dataset type.", 400)

    # Safety-net override: the frontend only ever sends this after auto-detection
    # failed AND the user explicitly picked a delimiter from the fallback picker
    # that appears in that situation — normal uploads never set this.
    override_code = (request.form.get("delimiter_override") or "").strip().lower()
    if override_code:
        if override_code not in DELIMITER_MAP:
            return None, None, None, None, (f"Unknown delimiter '{override_code}'.", 400)

    text = file.read().decode("utf-8", errors="replace")
    file.stream.seek(0)
    delimiter = DELIMITER_MAP[override_code] if override_code else _detect_delimiter(text)
    return file, dtype_config, text, delimiter, None


def _suggest_alternative_delimiter(text, required_columns, current_delimiter):
    """Called only when validation has already failed. Splits the header line
    with every candidate delimiter and scores each by (required columns
    matched, total columns found). If some other candidate scores strictly
    better than the one we actually used, that's a real signal the auto-detect
    (or the user's own override) picked the wrong one — return it so the
    frontend can offer it as a one-click fix. Returns None if nothing beats
    the current delimiter, meaning the failure is a genuine data problem, not
    a delimiter problem, so no fallback UI should appear.
    """
    lines = [l for l in text.splitlines() if l.strip()]
    if not lines:
        return None
    header_line = lines[0]

    def score(delimiter):
        cols = [c.strip() for c in header_line.split(delimiter)]
        matched = sum(1 for c in required_columns if c in cols)
        return (matched, len(cols))

    current_score = score(current_delimiter)
    best_delimiter, best_score = current_delimiter, current_score
    for delimiter in DELIMITER_MAP.values():
        candidate_score = score(delimiter)
        if candidate_score > best_score:
            best_delimiter, best_score = delimiter, candidate_score

    return best_delimiter if best_delimiter != current_delimiter else None


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

    is_override = bool((request.form.get("delimiter_override") or "").strip())

    result = spark().validate_upload(text, dtype_config["required_columns"], delimiter=delimiter)
    # Surface what we detected in the same checks list the UI already renders —
    # no separate "delimiter" field/control for the user to think about in the
    # normal case.
    result["checks"].insert(0, {
        "ok": True,
        "message": (
            f"Using {DELIMITER_LABELS.get(delimiter, delimiter)} as selected."
            if is_override else
            f"Detected file format: {DELIMITER_LABELS.get(delimiter, delimiter)}-separated."
        ),
    })
    result["detected_delimiter"] = delimiter

    markup_findings = _find_markup(text, delimiter)
    if markup_findings:
        count = len(markup_findings)
        result["checks"].append({
            "ok": False,
            "message": f"{count} cell{'s' if count != 1 else ''} contain HTML or script-like content and were rejected.",
        })
        result["valid"] = False
        result["markup_findings"] = markup_findings
    else:
        result["checks"].append({"ok": True, "message": "No HTML or script-like content in headers or cells."})

    # Safety net: only when validation actually failed, check whether a
    # different delimiter would have parsed this file better. If so, offer
    # it as a one-click fallback instead of leaving the user stuck.
    if not result["valid"]:
        suggestion = _suggest_alternative_delimiter(text, dtype_config["required_columns"], delimiter)
        if suggestion:
            result["delimiter_issue"] = True
            result["suggested_delimiter"] = suggestion

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

    markup_findings = _find_markup(text, delimiter)
    if markup_findings:
        count = len(markup_findings)
        result["checks"].append({
            "ok": False,
            "message": f"{count} cell{'s' if count != 1 else ''} contain HTML or script-like content and were rejected.",
        })
        result["valid"] = False

    if not result["valid"]:
        response = {"error": "File failed validation.", "checks": result["checks"]}
        if markup_findings:
            response["markup_findings"] = markup_findings
        suggestion = _suggest_alternative_delimiter(text, dtype_config["required_columns"], delimiter)
        if suggestion:
            response["delimiter_issue"] = True
            response["suggested_delimiter"] = suggestion
        return jsonify(response), 400

    # "Other Format" (skip_merge) — unchanged behavior: every upload is its
    # own standalone Bronze dataset, never merged. Still canonicalized to
    # comma on the way in, same as everything else, so every other page
    # (Prepare/Visualize/Merge) can keep assuming comma-separated storage.
    if dtype_config.get("skip_merge"):
        canonical_text = text if delimiter == "," else spark().to_csv_text(spark().read_csv_text(text, delimiter=delimiter))
        stored_text = spark().clean(canonical_text, AUTO_CLEAN_OPERATIONS)

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
        postgres().log_action(user["id"], user["username"], "upload_dataset", name, {"division": division, "dataset_type": dtype, "detected_delimiter": delimiter})
        return jsonify({"ok": True, "layer": "Bronze", "name": name, "merged": False, "detected_delimiter": delimiter})

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

    # Clean the incoming batch BEFORE it's merged — same reasoning as the
    # skip_merge branch above. Deliberately only the new batch, not a
    # re-clean of the whole existing master: master rows were already
    # cleaned when THEY were originally uploaded (going forward from this
    # change), so the master stays consistently clean over time without
    # having to reprocess everything on every single upload.
    canonical_text = text if delimiter == "," else spark().to_csv_text(spark().read_csv_text(text, delimiter=delimiter))
    cleaned_text = spark().clean(canonical_text, AUTO_CLEAN_OPERATIONS)

    # Everything from here through the Postgres bookkeeping below is one
    # read-merge-write sequence against the SAME master file. Without this
    # lock, two people uploading to the same format at nearly the same time
    # can both read the master at its pre-upload state, merge independently,
    # and then whichever write lands last silently overwrites the other's
    # rows — no error, no conflict, just missing data. The lock makes a
    # second upload to this exact format_key wait for the first to fully
    # finish (MinIO write + all three Postgres writes) before it even reads
    # the master file, so it always merges against the true latest state.
    # Uploads to a DIFFERENT format_key are untouched — they take a
    # different lock id and run fully in parallel.
    with _upload_lock(f"master_upload:{format_key}"):
        existing_text = None
        if minio().object_exists(master_key):
            existing_text = minio().get_object_text(master_key)

        try:
            merged_csv, stats = spark().merge_into_master(existing_text, cleaned_text, dedupe_mode, key_columns, delimiter=",")
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
        "detected_delimiter": delimiter,
    })