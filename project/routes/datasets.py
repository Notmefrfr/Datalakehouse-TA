import csv
import io

from flask import Blueprint, Response, jsonify, request

from routes._common import admin_required, catalog, config, login_required, postgres, spark

bp = Blueprint("datasets", __name__)


@bp.get("/config")
@login_required
def get_config(user):
    return jsonify({
        "divisions": config().DIVISIONS,
        "cleaning_operations": config().CLEANING_OPERATIONS,
    })


@bp.get("/datasets")
@login_required
def list_datasets(user):
    include_archived = request.args.get("include_archived") == "true"
    return jsonify(catalog().list_catalog(include_archived=include_archived))


@bp.get("/datasets/<layer>/<name>/preview")
@login_required
def preview_dataset(user, layer, name):
    limit = min(int(request.args.get("limit", 15)), 200)
    try:
        csv_text = catalog().read_dataset_csv(layer, name)
    except Exception as e:
        return jsonify({"error": str(e)}), 404

    lines = [l for l in csv_text.splitlines() if l.strip()]
    if not lines:
        return jsonify({"fields": [], "rows": [], "total_rows": 0})

    df = spark().read_csv_text(csv_text)
    fields = list(df.columns)
    preview_rows = df.head(limit).where(df.notnull(), None).to_dict(orient="records")
    return jsonify({"fields": fields, "rows": preview_rows, "total_rows": len(df)})


def _match_format(header_columns):
    """Given the set of column names actually present in a dataset, find the
    single dataset_type — searched across ALL divisions, not one chosen by
    the user — whose required_columns are fully satisfied. This is the one
    source of truth for 'which standardized format is this dataset', used
    by both the column-types endpoint (to tell the frontend which division
    to display, and which chart types are valid) and /visualize/summary
    (to pick the right hardcoded config). Removes the old failure mode
    where a user could pick a division unrelated to the actual dataset and
    get a generic 'summary cannot generate' error — there's no such choice
    to get wrong anymore.

    "Other Format" is deliberately excluded (its required_columns is always
    empty, so it would otherwise match everything). Returns
    (division_id, division_label, dtype_dict) or (None, None, None)."""
    for division in config().DIVISIONS:
        if division["id"] == "other":
            continue
        for dtype in division["dataset_types"]:
            required = dtype.get("required_columns") or []
            if required and set(required).issubset(header_columns):
                return division["id"], division["label"], dtype
    return None, None, None


@bp.get("/datasets/<layer>/<name>/column-types")
@login_required
def dataset_column_types(user, layer, name):
    """Powers dynamic dropdown filtering on the Visualize page — tells the
    frontend which columns are numeric/date/categorical (by actual data,
    not name) so it can only offer chart-column combinations that will
    actually work, instead of letting someone pick an invalid pair and
    then showing a red error after the fact.

    Also resolves and returns the dataset's matched standardized format
    (division/dtype), if any, plus which Summary chart types actually have
    a hardcoded comparison defined for it — this is what lets the frontend
    show "Select section to summarize" as a fixed, correct value instead of
    a dropdown the user could set to the wrong thing, and grey out chart
    types with nothing valid to show (e.g. Bar/Pie for a format with no
    categorical column)."""
    try:
        csv_text = catalog().read_dataset_csv(layer, name)
        column_types = spark().detect_column_types(csv_text)
        header = set(spark().read_csv_text(csv_text).columns)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    division_id, division_label, dtype = _match_format(header)
    matched_format = None
    if dtype:
        format_config = config().SUMMARY_CONFIG.get(dtype["id"])
        available_chart_types = [
            chart_type for chart_type in ("bar", "pie", "line")
            if format_config and format_config.get("charts", {}).get(chart_type)
        ]
        matched_format = {
            "division_id": division_id,
            "division_label": division_label,
            "dtype_id": dtype["id"],
            "dtype_label": dtype["label"],
            "summary_available": bool(format_config),
            "available_chart_types": available_chart_types,
        }
    return jsonify({"columns": column_types, "matched_format": matched_format})


@bp.get("/datasets/<layer>/<name>/download")
@login_required
def download_dataset(user, layer, name):
    try:
        csv_text = catalog().read_dataset_csv(layer, name)
    except Exception as e:
        return jsonify({"error": str(e)}), 404
    filename = name.split("__")[-1] + ".csv"
    return Response(
        csv_text, mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@bp.post("/datasets/<layer>/<name>/rename")
@admin_required
def rename_dataset(user, layer, name):
    data = request.get_json(silent=True) or {}
    new_name = (data.get("display_name") or "").strip()
    if not new_name:
        return jsonify({"error": "display_name is required."}), 400
    postgres().upsert_metadata(layer, name, display_name=new_name)
    postgres().log_action(user["id"], user["username"], "rename_dataset", f"{layer}::{name}", {"new_name": new_name})
    return jsonify({"ok": True})


@bp.post("/datasets/<layer>/<name>/archive")
@admin_required
def archive_dataset(user, layer, name):
    postgres().upsert_metadata(layer, name, archived=True)
    postgres().log_action(user["id"], user["username"], "archive_dataset", f"{layer}::{name}")
    return jsonify({"ok": True})


@bp.post("/datasets/<layer>/<name>/delete")
@admin_required
def delete_dataset(user, layer, name):
    """Permanently deletes the underlying data — unlike archive (which just
    hides it from the library), this actually removes it and cannot be
    undone. Removes the storage (a single object for Bronze/Silver/Gold, a
    whole Delta table for Master — see CatalogService.delete_dataset_storage)
    and the Postgres metadata row, so it fully disappears from every
    dataset dropdown/list too."""
    try:
        catalog().delete_dataset_storage(layer, name)
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    postgres().delete_metadata(layer, name)
    postgres().log_action(user["id"], user["username"], "delete_dataset", f"{layer}::{name}")
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
    layer, name = (data.get("layer") or ""), (data.get("name") or "")
    x_col, y_col = data.get("x"), data.get("y")
    method = data.get("method", "sum")
    if not (layer and name and x_col) or (method != "count" and not y_col):
        return jsonify({"error": "layer, name, x (and y, unless method is 'count') are required."}), 400
    try:
        csv_text = catalog().read_dataset_csv(layer, name)
        labels, values = spark().aggregate(csv_text, x_col, y_col, method=method)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"labels": labels, "values": values})


@bp.post("/visualize/histogram")
@login_required
def visualize_histogram(user):
    data = request.get_json(silent=True) or {}
    layer, name, column = (data.get("layer") or ""), (data.get("name") or ""), data.get("column")
    if not (layer and name and column):
        return jsonify({"error": "layer, name and column are required."}), 400
    try:
        csv_text = catalog().read_dataset_csv(layer, name)
        labels, values = spark().histogram(csv_text, column)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"labels": labels, "values": values})


@bp.post("/visualize/scatter")
@login_required
def visualize_scatter(user):
    data = request.get_json(silent=True) or {}
    layer, name = (data.get("layer") or ""), (data.get("name") or "")
    x_col, y_col = data.get("x"), data.get("y")
    if not (layer and name and x_col and y_col):
        return jsonify({"error": "layer, name, x and y are required."}), 400
    try:
        csv_text = catalog().read_dataset_csv(layer, name)
        points = spark().scatter(csv_text, x_col, y_col)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"points": points})


@bp.get("/datasets/<layer>/<name>/date-columns")
@login_required
def dataset_date_columns(user, layer, name):
    """Which columns are actually date-typed (by content, not name) — lets
    the frontend offer the optional Daily Summary/Preview date picker only
    when at least one exists, and offer a real choice when a dataset has
    more than one (e.g. Inventory's Item Receive Date vs Item Return
    Date — both valid; picking one is a genuine choice, not something to
    silently guess)."""
    try:
        csv_text = catalog().read_dataset_csv(layer, name)
        types = spark().detect_column_types(csv_text)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"date_columns": [c for c, info in types.items() if info["type"] == "date"]})


@bp.post("/visualize/daily")
@login_required
def visualize_daily(user):
    """Optional, secondary feature shown below KPI Cards: a generic 'what
    happened on this date' breakdown plus a raw-row preview, built from
    whatever columns actually exist on the dataset — never hardcoded to
    one category, so it keeps working for formats added later (Operation,
    Procurement, Asset, Maintenance, etc.) without any code change here."""
    data = request.get_json(silent=True) or {}
    layer, name = (data.get("layer") or ""), (data.get("name") or "")
    date_column, target_date = data.get("date_column"), data.get("date")
    if not (layer and name and date_column and target_date):
        return jsonify({"error": "layer, name, date_column and date are required."}), 400
    try:
        csv_text = catalog().read_dataset_csv(layer, name)
        result = spark().daily_summary_and_preview(csv_text, date_column, target_date)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    if result is None:
        return jsonify({"error": f"'{date_column}' is not a valid date column on this dataset."}), 400
    return jsonify(result)


@bp.post("/visualize/summary")
@login_required
def visualize_summary(user):
    """No longer takes a 'division' — the standardized format (and
    therefore which hardcoded config applies) is resolved purely from the
    dataset's own header via _match_format(), same as summary-charts below.
    There is nothing left for the user to pick that could mismatch the
    actual data."""
    data = request.get_json(silent=True) or {}
    layer, name = (data.get("layer") or ""), (data.get("name") or "")
    if not (layer and name):
        return jsonify({"error": "layer and name are required."}), 400

    try:
        csv_text = catalog().read_dataset_csv(layer, name)
        header = set(spark().read_csv_text(csv_text).columns)
    except Exception as e:
        return jsonify({
            "available": False,
            "message": "Summary unavailable: could not read this dataset.",
        })

    division_id, division_label, dtype = _match_format(header)
    matched_config = config().SUMMARY_CONFIG.get(dtype["id"]) if dtype else None
    if not matched_config:
        return jsonify({
            "available": False,
            "message": "Summary is unavailable for this dataset because it doesn't match one of the standardized formats.",
        })

    try:
        result = spark().compute_hardcoded_summary(csv_text, matched_config)
    except Exception:
        return jsonify({
            "available": False,
            "message": "Summary unavailable: this dataset does not contain the required fields for this format.",
        })
    return jsonify({
        "available": True,
        "division_label": division_label,
        "dtype_label": dtype["label"],
        "kpis": result["kpis"],
        "rows": result["rows"],
    })


@bp.post("/visualize/summary-charts")
@login_required
def visualize_summary_charts(user):
    """Companion to /visualize/summary — same dataset, but produces EVERY
    valid chart for the requested chart_type instead of one table. 'Valid'
    is read straight from Config.SUMMARY_CONFIG['charts'][chart_type] (see
    config.py) rather than statistically guessed: guessing previously
    produced nonsense pairings — e.g. summing a reference-number column
    grouped by region — because a generic 'strongest relationship' score
    can't know a numeric-looking column is actually an identifier. A format
    with no valid comparisons for the requested chart_type (e.g. Bar/Pie
    for a format with no categorical column) returns available: false with
    an explanatory message rather than falling back to a guess.

    Independent endpoint — a chart failure here doesn't touch the Summary
    table's own request/response."""
    data = request.get_json(silent=True) or {}
    layer, name = (data.get("layer") or ""), (data.get("name") or "")
    chart_type = data.get("chart_type", "bar")
    if not (layer and name):
        return jsonify({"error": "layer and name are required."}), 400
    if chart_type not in ("bar", "pie", "line"):
        return jsonify({"error": "Invalid chart_type."}), 400

    try:
        csv_text = catalog().read_dataset_csv(layer, name)
        header = set(spark().read_csv_text(csv_text).columns)
    except Exception:
        return jsonify({"available": False, "message": "Could not read this dataset.", "charts": []})

    _, _, dtype = _match_format(header)
    format_config = config().SUMMARY_CONFIG.get(dtype["id"]) if dtype else None
    chart_specs = (format_config or {}).get("charts", {}).get(chart_type, [])
    if not chart_specs:
        return jsonify({
            "available": False,
            "message": "No standardized chart comparisons are defined for this dataset and chart type.",
            "charts": [],
        })

    try:
        charts = spark().compute_hardcoded_charts(csv_text, chart_specs, chart_type)
    except Exception:
        return jsonify({"available": False, "message": "Could not generate charts for this dataset.", "charts": []})

    if not charts:
        return jsonify({
            "available": False,
            "message": "This dataset doesn't currently have usable data for any of the standard comparisons.",
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

    layer_a, name_a = a.split("::")
    layer_b, name_b = b.split("::")
    try:
        csv_a = catalog().read_dataset_csv(layer_a, name_a)
        csv_b = catalog().read_dataset_csv(layer_b, name_b)
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
    """Recomputes the SAME merge as /merge/preview (identical inputs, same
    spark().merge() call) but returns the FULL result as a downloadable CSV
    instead of a 15-row JSON preview. Nothing is persisted to MinIO for
    this — it's generated fresh on demand, which keeps this endpoint
    self-contained and avoids adding new stored-object bookkeeping just for
    a download button. The frontend only enables the Download button after
    a successful preview with these same parameters, so in normal use this
    reproduces exactly what was just previewed."""
    data = request.get_json(silent=True) or {}
    a, b, join_type = data.get("a"), data.get("b"), data.get("join_type", "inner")
    if not a or not b:
        return jsonify({"error": "Pick both Dataset A and Dataset B."}), 400
    if a == b:
        return jsonify({"error": "Pick two different datasets."}), 400

    layer_a, name_a = a.split("::")
    layer_b, name_b = b.split("::")
    try:
        csv_a = catalog().read_dataset_csv(layer_a, name_a)
        csv_b = catalog().read_dataset_csv(layer_b, name_b)
        columns, rows, join_col = spark().merge(csv_a, csv_b, join_type)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)

    postgres().log_action(
        user["id"], user["username"], "merge_download", f"{a}+{b}",
        {"join_type": join_type, "join_column": join_col, "rows": len(rows)},
    )

    stub_a = name_a.split("__")[-1]
    stub_b = name_b.split("__")[-1]
    filename = f"merged_{stub_a}_{stub_b}.csv"
    return Response(
        buffer.getvalue(), mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )