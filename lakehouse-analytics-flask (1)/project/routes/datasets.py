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
    if not (layer and name and x_col and y_col):
        return jsonify({"error": "layer, name, x and y are required."}), 400
    try:
        csv_text = catalog().read_dataset_csv(layer, name)
        labels, values = spark().aggregate(csv_text, x_col, y_col)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"labels": labels, "values": values})


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
