"""
Administrator-only routes: row-level edits, deletes, dataset deletion, audit log.
Every modification here is recorded in Postgres (audit_log), per the spec that
"every modification must be recorded."
"""
from flask import Blueprint, jsonify, request

from routes._common import admin_required, catalog, postgres

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
        df = catalog().read_dataset_df(layer, name)
        if row_index < 0 or row_index >= len(df):
            return jsonify({"error": "row_index out of range."}), 400
        for col, value in updates.items():
            if col in df.columns:
                df.at[row_index, col] = value
        catalog().write_dataset_df(layer, name, df)
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
        df = catalog().read_dataset_df(layer, name)
        if row_index < 0 or row_index >= len(df):
            return jsonify({"error": "row_index out of range."}), 400
        df = df.drop(df.index[row_index]).reset_index(drop=True)
        catalog().write_dataset_df(layer, name, df)
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
        catalog().delete_dataset_storage(layer, name)
        postgres().delete_metadata(layer, name)
    except ValueError:
        return jsonify({"error": "Unknown dataset."}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    postgres().log_action(user["id"], user["username"], "delete_dataset", f"{layer}::{name}")
    return jsonify({"ok": True})


@bp.get("/admin/audit-log")
@admin_required
def audit_log(user):
    limit = min(int(request.args.get("limit", 100)), 500)
    return jsonify(postgres().list_audit_log(limit=limit))
