"""
Administrator-only routes: row-level edits, deletes, dataset deletion, audit log.
Every modification here is recorded in Postgres (audit_log), per the spec that
"every modification must be recorded."
"""
from flask import Blueprint, jsonify, request

from routes._common import admin_required, catalog, minio, postgres, spark

bp = Blueprint("admin", __name__)


@bp.post("/admin/update-row")
@admin_required
def update_row(user):
    data = request.get_json(silent=True) or {}
    layer, name = data.get("layer"), data.get("name")
    row_index, updates = data.get("row_index"), data.get("updates") or {}
    if layer is None or name is None or row_index is None or not updates:
        return jsonify({"error": "layer, name, row_index and updates are required."}), 400

    try:
        csv_text = catalog().read_dataset_csv(layer, name)
        df = spark().read_csv_text(csv_text)
        if row_index < 0 or row_index >= len(df):
            return jsonify({"error": "row_index out of range."}), 400
        for col, value in updates.items():
            if col in df.columns:
                df.at[row_index, col] = value
        new_csv = spark().to_csv_text(df)
        key = catalog().object_key_for(layer, name)
        minio().put_object_text(key, new_csv, content_type="text/csv")
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    postgres().log_action(user["id"], user["username"], "update_row", f"{layer}::{name}", {"row_index": row_index, "updates": updates})
    return jsonify({"ok": True})


@bp.post("/admin/delete-row")
@admin_required
def delete_row(user):
    data = request.get_json(silent=True) or {}
    layer, name, row_index = data.get("layer"), data.get("name"), data.get("row_index")
    if layer is None or name is None or row_index is None:
        return jsonify({"error": "layer, name and row_index are required."}), 400

    try:
        csv_text = catalog().read_dataset_csv(layer, name)
        df = spark().read_csv_text(csv_text)
        if row_index < 0 or row_index >= len(df):
            return jsonify({"error": "row_index out of range."}), 400
        df = df.drop(df.index[row_index]).reset_index(drop=True)
        new_csv = spark().to_csv_text(df)
        key = catalog().object_key_for(layer, name)
        minio().put_object_text(key, new_csv, content_type="text/csv")
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    postgres().log_action(user["id"], user["username"], "delete_row", f"{layer}::{name}", {"row_index": row_index})
    return jsonify({"ok": True})


@bp.post("/admin/delete-dataset")
@admin_required
def delete_dataset(user):
    data = request.get_json(silent=True) or {}
    layer, name = data.get("layer"), data.get("name")
    if not layer or not name:
        return jsonify({"error": "layer and name are required."}), 400

    try:
        key = catalog().object_key_for(layer, name)
        if not key:
            return jsonify({"error": "Unknown dataset."}), 404
        minio().delete_object(key)
        postgres().delete_metadata(layer, name)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    postgres().log_action(user["id"], user["username"], "delete_dataset", f"{layer}::{name}")
    return jsonify({"ok": True})


@bp.get("/admin/audit-log")
@admin_required
def audit_log(user):
    limit = min(int(request.args.get("limit", 100)), 500)
    return jsonify(postgres().list_audit_log(limit=limit))
