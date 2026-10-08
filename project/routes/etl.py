"""
Manual "additional cleaning" step (Bab 2.1's Silver: "opsional, pembersihan
tambahan") — everything already gets auto-cleaned once at upload time
(routes/upload.py's AUTO_CLEAN_OPERATIONS), this is for cleaning a dataset
further, on demand, after the fact.

CHANGED (dynamic-dataset feature, 2026-09-24): full rewrite. There is no
more separate Silver layer/dataset to create — with Bronze/Silver/Gold
collapsed into one Delta table per dataset_id, running additional cleaning
now cleans and OVERWRITES that same dataset in place (recorded as a new
version, same as any other full-dataset rewrite — see
CatalogService.write_dataset_df). Takes `dataset_id` instead of
`layer`/`name`, and no longer creates a second "(cleaned)" copy — if you
want to keep both the original and a cleaned copy, download the original
first.
"""
from flask import Blueprint, jsonify, request

from routes._common import admin_required, catalog, config, postgres, spark

bp = Blueprint("etl", __name__)


@bp.post("/etl/clean")
@admin_required
def run_clean(user):
    data = request.get_json(silent=True) or {}
    dataset_id = data.get("dataset_id")
    operations = data.get("operations") or []
    if not dataset_id:
        return jsonify({"error": "dataset_id is required."}), 400

    valid_ops = {op["id"] for op in config().CLEANING_OPERATIONS}
    operations = [op for op in operations if op in valid_ops]
    if not operations:
        return jsonify({"error": "Choose at least one cleaning operation."}), 400

    try:
        csv_text = catalog().read_dataset_csv(dataset_id)
        cleaned_csv = spark().clean(csv_text, operations)
        cleaned_df = spark().read_csv_text(cleaned_csv)
        catalog().write_dataset_df(dataset_id, cleaned_df)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    row_count = len(cleaned_df)
    postgres().record_version(
        "Master", dataset_id, row_count=row_count,
        size_bytes=len(cleaned_csv.encode("utf-8")), created_by=user["id"],
        details={"event": "manual_clean", "operations": operations},
    )
    postgres().log_action(user["id"], user["username"], "clean_dataset", dataset_id, {"operations": operations})

    return jsonify({"ok": True, "dataset_id": dataset_id, "rows": row_count})
