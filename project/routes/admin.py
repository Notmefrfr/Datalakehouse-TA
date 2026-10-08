"""
Administrator-only routes: row-level edits, deletes, dataset deletion, audit
log, and — new for the dynamic-dataset feature — the pending-changes
approval queue (schema changes + dataset renames requested by non-admins).

CHANGED (dynamic-dataset feature, 2026-09-24): update_row/delete_row/
delete_dataset dropped their `layer` parameter (there's only one kind of
dataset now — see catalog_service.py). New: GET /admin/pending-changes,
POST /admin/pending-changes/<id>/approve, POST /admin/pending-changes/<id>/reject
— these dispatch to DatasetService's approve_schema_change()/
reject_schema_change() or approve_rename()/reject_rename() depending on
the request's change_type, so the frontend doesn't need two different
approve/reject buttons wired to two different endpoints.
"""
from flask import Blueprint, jsonify, request

from routes._common import admin_required, catalog, dataset_service, postgres

bp = Blueprint("admin", __name__)


@bp.post("/admin/update-row")
@admin_required
def update_row(user):
    data = request.get_json(silent=True) or {}
    dataset_id = data.get("dataset_id")
    row_index, updates = data.get("row_index"), data.get("updates") or {}
    if dataset_id is None or row_index is None or not updates:
        return jsonify({"error": "dataset_id, row_index and updates are required."}), 400

    try:
        df = catalog().read_dataset_df(dataset_id)
        if row_index < 0 or row_index >= len(df):
            return jsonify({"error": "row_index out of range."}), 400
        for col, value in updates.items():
            if col in df.columns:
                df.at[row_index, col] = value
        catalog().write_dataset_df(dataset_id, df)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    postgres().log_action(user["id"], user["username"], "update_row", dataset_id, {"row_index": row_index, "updates": updates})
    return jsonify({"ok": True})


@bp.post("/admin/delete-row")
@admin_required
def delete_row(user):
    data = request.get_json(silent=True) or {}
    dataset_id, row_index = data.get("dataset_id"), data.get("row_index")
    if dataset_id is None or row_index is None:
        return jsonify({"error": "dataset_id and row_index are required."}), 400

    try:
        df = catalog().read_dataset_df(dataset_id)
        if row_index < 0 or row_index >= len(df):
            return jsonify({"error": "row_index out of range."}), 400
        df = df.drop(df.index[row_index]).reset_index(drop=True)
        catalog().write_dataset_df(dataset_id, df)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    postgres().log_action(user["id"], user["username"], "delete_row", dataset_id, {"row_index": row_index})
    return jsonify({"ok": True})


@bp.post("/admin/delete-dataset")
@admin_required
def delete_dataset(user):
    data = request.get_json(silent=True) or {}
    dataset_id = data.get("dataset_id")
    if not dataset_id:
        return jsonify({"error": "dataset_id is required."}), 400

    try:
        catalog().delete_dataset_storage(dataset_id)
        postgres().delete_metadata("Master", dataset_id)
    except ValueError:
        return jsonify({"error": "Unknown dataset."}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    postgres().log_action(user["id"], user["username"], "delete_dataset", dataset_id)
    return jsonify({"ok": True})


@bp.get("/admin/audit-log")
@admin_required
def audit_log(user):
    limit = min(int(request.args.get("limit", 100)), 500)
    return jsonify(postgres().list_audit_log(limit=limit))


# -- pending-changes approval queue (schema changes + renames) ---------------

@bp.get("/admin/pending-changes")
@admin_required
def list_pending_changes(user):
    status = request.args.get("status", "pending")
    limit = min(int(request.args.get("limit", 100)), 500)
    return jsonify(postgres().list_pending_changes(status=status, limit=limit))


@bp.post("/admin/pending-changes/<int:pending_id>/approve")
@admin_required
def approve_pending_change(user, pending_id):
    change = postgres().get_pending_change(pending_id)
    if not change:
        return jsonify({"error": "No such pending request."}), 404

    try:
        if change["change_type"] == "schema_change":
            result = dataset_service().approve_schema_change(pending_id, user)
        elif change["change_type"] == "rename":
            result = dataset_service().approve_rename(pending_id, user)
        else:
            return jsonify({"error": f"Unknown change_type '{change['change_type']}'."}), 400
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    return jsonify({"ok": True, **result})


@bp.post("/admin/pending-changes/<int:pending_id>/reject")
@admin_required
def reject_pending_change(user, pending_id):
    change = postgres().get_pending_change(pending_id)
    if not change:
        return jsonify({"error": "No such pending request."}), 404

    try:
        if change["change_type"] == "schema_change":
            result = dataset_service().reject_schema_change(pending_id, user)
        elif change["change_type"] == "rename":
            result = dataset_service().reject_rename(pending_id, user)
        else:
            return jsonify({"error": f"Unknown change_type '{change['change_type']}'."}), 400
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    return jsonify({"ok": True, **result})
