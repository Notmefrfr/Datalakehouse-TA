from flask import Blueprint, jsonify, request

from routes._common import catalog, config, login_required, minio, postgres, spark

bp = Blueprint("etl", __name__)


@bp.post("/etl/clean")
@login_required
def run_clean(user):
    data = request.get_json(silent=True) or {}
    layer, name = data.get("layer"), data.get("name")
    operations = data.get("operations") or []
    if not layer or not name:
        return jsonify({"error": "layer and name are required."}), 400

    valid_ops = {op["id"] for op in config().CLEANING_OPERATIONS}
    operations = [op for op in operations if op in valid_ops]
    if not operations:
        return jsonify({"error": "Choose at least one cleaning operation."}), 400

    try:
        csv_text = catalog().read_dataset_csv(layer, name)
        cleaned_csv = spark().clean(csv_text, operations)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    source_meta = postgres().get_metadata(layer, name)
    base_name = name.split("__")[-1] if layer == "Bronze" else name
    output_name = catalog().new_stub(base_name) + "_cleaned"

    silver_key = f"{config().SILVER_PREFIX}{output_name}/data.csv"
    minio().put_object_text(silver_key, cleaned_csv, content_type="text/csv")

    postgres().upsert_metadata(
        "Silver", output_name,
        display_name=f"{source_meta.get('display_name') or name} (cleaned)",
        owner=user["full_name"] or user["username"],
        division_override=source_meta.get("division_override"),
        archived=False,
    )
    row_count = max(len([l for l in cleaned_csv.splitlines() if l.strip()]) - 1, 0)
    postgres().record_version("Silver", output_name, row_count=row_count, size_bytes=len(cleaned_csv.encode("utf-8")), created_by=user["id"])
    postgres().log_action(user["id"], user["username"], "clean_dataset", output_name, {"source": f"{layer}::{name}", "operations": operations})

    return jsonify({"ok": True, "layer": "Silver", "name": output_name})
