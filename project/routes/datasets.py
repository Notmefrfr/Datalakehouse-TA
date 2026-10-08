"""
CHANGED (dynamic-dataset feature, 2026-09-24): full rewrite. Every route
here used to take `<layer>/<name>` (Bronze/Silver/Gold/Master); there's no
more layer concept — everything is one Delta table per dataset_id, so
every route now takes just `<dataset_id>`. _match_format() is GONE (it
resolved a fixed division/dataset_type from Config.SUMMARY_CONFIG, which
no longer exists) — /visualize/summary and /visualize/summary-charts now
call spark().compute_auto_summary()/compute_auto_charts(), which read the
dataset's own columns instead of a hardcoded per-format config. GET
/config no longer returns `divisions` (nothing left to return there).
Rename is now a request that needs Administrator approval unless the
requester already is one — see services/dataset_service.py's
request_rename()/approve_rename()/reject_rename() (approve/reject live in
routes/admin.py, next to the schema-change approval queue).

NOTE for whoever updates the frontend: every fetch that used to build a
URL like `/api/datasets/${layer}/${name}/...` now needs to drop the layer
segment (`/api/datasets/${dataset_id}/...`), and any UI reading
`division`/`dtype_label`/`available_chart_types` from column-types or
matched_format needs to be dropped too — see dataset_column_types() below,
it no longer returns a `matched_format` at all.
"""
import csv
import io

from flask import Blueprint, Response, jsonify, request

from routes._common import catalog, config, dataset_service, login_required, postgres, spark

bp = Blueprint("datasets", __name__)


@bp.get("/config")
@login_required
def get_config(user):
    return jsonify({"cleaning_operations": config().CLEANING_OPERATIONS})


@bp.get("/datasets")
@login_required
def list_datasets(user):
    include_archived = request.args.get("include_archived") == "true"
    return jsonify(catalog().list_catalog(include_archived=include_archived))


@bp.get("/datasets/<dataset_id>/preview")
@login_required
def preview_dataset(user, dataset_id):
    limit = min(int(request.args.get("limit", 15)), 200)
    try:
        csv_text = catalog().read_dataset_csv(dataset_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 404

    lines = [l for l in csv_text.splitlines() if l.strip()]
    if not lines:
        return jsonify({"fields": [], "rows": [], "total_rows": 0})

    df = spark().read_csv_text(csv_text)
    fields = list(df.columns)
    preview_rows = df.head(limit).where(df.notnull(), None).to_dict(orient="records")
    return jsonify({"fields": fields, "rows": preview_rows, "total_rows": len(df)})


@bp.get("/datasets/<dataset_id>/column-types")
@login_required
def dataset_column_types(user, dataset_id):
    """Powers dynamic dropdown filtering on the Visualize page — tells the
    frontend which columns are numeric/date/categorical (by actual data,
    not name) so it can only offer chart-column combinations that will
    actually work."""
    try:
        csv_text = catalog().read_dataset_csv(dataset_id)
        column_types = spark().detect_column_types(csv_text)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"columns": column_types})


@bp.get("/datasets/<dataset_id>/download")
@login_required
def download_dataset(user, dataset_id):
    try:
        csv_text = catalog().read_dataset_csv(dataset_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 404
    # NOTE: deliberately NOT calling spark().sanitize_for_export() here —
    # per your earlier explicit decision to keep your friend's version
    # as-is, including its CSV-formula-injection regression. Say the word
    # if you want this re-added.
    filename = f"{dataset_id}.csv"
    return Response(
        csv_text, mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@bp.post("/datasets/<dataset_id>/rename")
@login_required
def rename_dataset(user, dataset_id):
    """Any signed-in user can REQUEST a rename; whether it applies
    immediately or waits for an Administrator's approval is decided inside
    DatasetService.request_rename() (immediate only if the requester
    already is an Administrator)."""
    data = request.get_json(silent=True) or {}
    new_name = (data.get("display_name") or "").strip()
    if not new_name:
        return jsonify({"error": "display_name is required."}), 400
    try:
        result = dataset_service().request_rename(dataset_id, new_name, user)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"ok": True, **result})


@bp.post("/datasets/<dataset_id>/archive")
@login_required
def archive_dataset(user, dataset_id):
    if user["role"] != "Administrator":
        return jsonify({"error": "Administrator access required."}), 403
    postgres().upsert_metadata("Master", dataset_id, archived=True)
    postgres().log_action(user["id"], user["username"], "archive_dataset", dataset_id)
    return jsonify({"ok": True})


@bp.post("/datasets/<dataset_id>/delete")
@login_required
def delete_dataset(user, dataset_id):
    """Permanently deletes the underlying Delta table and all metadata —
    unlike archive (which just hides it from the library), this actually
    removes it and cannot be undone."""
    if user["role"] != "Administrator":
        return jsonify({"error": "Administrator access required."}), 403
    try:
        catalog().delete_dataset_storage(dataset_id)
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    postgres().delete_metadata("Master", dataset_id)
    postgres().log_action(user["id"], user["username"], "delete_dataset", dataset_id)
    return jsonify({"ok": True})


@bp.get("/dashboard/stats")
@login_required
def dashboard_stats(user):
    user_count = postgres().count_users()
    return jsonify(catalog().compute_stats(user_count=user_count))


@bp.post("/visualize/aggregate")
@login_required
def visualize_aggregate(user):
    data = request.get_json(silent=True) or {}
    dataset_id = data.get("dataset_id") or ""
    x_col, y_col = data.get("x"), data.get("y")
    method = data.get("method", "sum")
    if not (dataset_id and x_col) or (method != "count" and not y_col):
        return jsonify({"error": "dataset_id, x (and y, unless method is 'count') are required."}), 400
    try:
        csv_text = catalog().read_dataset_csv(dataset_id)
        labels, values = spark().aggregate(csv_text, x_col, y_col, method=method)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"labels": labels, "values": values})


@bp.post("/visualize/histogram")
@login_required
def visualize_histogram(user):
    data = request.get_json(silent=True) or {}
    dataset_id, column = (data.get("dataset_id") or ""), data.get("column")
    if not (dataset_id and column):
        return jsonify({"error": "dataset_id and column are required."}), 400
    try:
        csv_text = catalog().read_dataset_csv(dataset_id)
        labels, values = spark().histogram(csv_text, column)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"labels": labels, "values": values})


@bp.post("/visualize/scatter")
@login_required
def visualize_scatter(user):
    data = request.get_json(silent=True) or {}
    dataset_id = data.get("dataset_id") or ""
    x_col, y_col = data.get("x"), data.get("y")
    if not (dataset_id and x_col and y_col):
        return jsonify({"error": "dataset_id, x and y are required."}), 400
    try:
        csv_text = catalog().read_dataset_csv(dataset_id)
        points = spark().scatter(csv_text, x_col, y_col)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"points": points})


@bp.get("/datasets/<dataset_id>/date-columns")
@login_required
def dataset_date_columns(user, dataset_id):
    try:
        csv_text = catalog().read_dataset_csv(dataset_id)
        types = spark().detect_column_types(csv_text)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"date_columns": [c for c, info in types.items() if info["type"] == "date"]})


@bp.post("/visualize/daily")
@login_required
def visualize_daily(user):
    data = request.get_json(silent=True) or {}
    dataset_id = data.get("dataset_id") or ""
    date_column, target_date = data.get("date_column"), data.get("date")
    if not (dataset_id and date_column and target_date):
        return jsonify({"error": "dataset_id, date_column and date are required."}), 400
    try:
        csv_text = catalog().read_dataset_csv(dataset_id)
        result = spark().daily_summary_and_preview(csv_text, date_column, target_date)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    if result is None:
        return jsonify({"error": f"'{date_column}' is not a valid date column on this dataset."}), 400
    return jsonify(result)


@bp.post("/visualize/summary")
@login_required
def visualize_summary(user):
    """No longer resolves a standardized division/dtype — this is now a
    generic, column-type-driven summary of whatever the dataset's current
    schema actually is (spark().compute_auto_summary()), so it's available
    for every dataset instead of only ones matching a hardcoded format."""
    data = request.get_json(silent=True) or {}
    dataset_id = data.get("dataset_id") or ""
    if not dataset_id:
        return jsonify({"error": "dataset_id is required."}), 400

    try:
        csv_text = catalog().read_dataset_csv(dataset_id)
        result = spark().compute_auto_summary(csv_text)
    except Exception:
        return jsonify({
            "available": False,
            "message": "Summary unavailable: could not read this dataset.",
        })
    return jsonify({"available": True, "kpis": result["kpis"], "rows": result["rows"]})


@bp.post("/visualize/summary-charts")
@login_required
def visualize_summary_charts(user):
    """Companion to /visualize/summary — every auto-generated chart of the
    requested chart_type for this dataset's current schema
    (spark().compute_auto_charts()), replacing the old hardcoded
    Config.SUMMARY_CONFIG['charts'] lookup."""
    data = request.get_json(silent=True) or {}
    dataset_id = data.get("dataset_id") or ""
    chart_type = data.get("chart_type", "bar")
    if not dataset_id:
        return jsonify({"error": "dataset_id is required."}), 400
    if chart_type not in ("bar", "pie", "line"):
        return jsonify({"error": "Invalid chart_type."}), 400

    try:
        csv_text = catalog().read_dataset_csv(dataset_id)
        charts = spark().compute_auto_charts(csv_text, chart_type)
    except Exception:
        return jsonify({"available": False, "message": "Could not read this dataset.", "charts": []})

    if not charts:
        return jsonify({
            "available": False,
            "message": "This dataset doesn't currently have usable columns for this chart type.",
            "charts": [],
        })
    return jsonify({"available": True, "charts": charts})


@bp.post("/merge/preview")
@login_required
def merge_preview(user):
    data = request.get_json(silent=True) or {}
    a, b, join_type = data.get("a"), data.get("b"), data.get("join_type", "inner")
    if not a or not b:
        return jsonify({"error": "Pick both Dataset A and Dataset B."}), 400
    if a == b:
        return jsonify({"error": "Pick two different datasets."}), 400

    try:
        csv_a = catalog().read_dataset_csv(a)
        csv_b = catalog().read_dataset_csv(b)
        columns, rows, join_col = spark().merge(csv_a, csv_b, join_type)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    return jsonify({
        "columns": columns,
        "rows": rows[:15],
        "total_rows": len(rows),
        "join_column": join_col,
    })


@bp.post("/merge/download")
@login_required
def merge_download(user):
    data = request.get_json(silent=True) or {}
    a, b, join_type = data.get("a"), data.get("b"), data.get("join_type", "inner")
    if not a or not b:
        return jsonify({"error": "Pick both Dataset A and Dataset B."}), 400
    if a == b:
        return jsonify({"error": "Pick two different datasets."}), 400

    try:
        csv_a = catalog().read_dataset_csv(a)
        csv_b = catalog().read_dataset_csv(b)
        columns, rows, join_col = spark().merge(csv_a, csv_b, join_type)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    # NOTE: deliberately NOT calling spark().sanitize_for_export() here —
    # same as download_dataset() above, keeping your friend's version's
    # behavior as-is per your earlier explicit choice.

    postgres().log_action(
        user["id"], user["username"], "merge_download", f"{a}+{b}",
        {"join_type": join_type, "join_column": join_col, "rows": len(rows)},
    )

    filename = f"merged_{a}_{b}.csv"
    return Response(
        buffer.getvalue(), mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
