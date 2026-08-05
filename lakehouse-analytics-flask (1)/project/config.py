"""
Central configuration, loaded from environment variables (see .env.example).
No secrets (MinIO keys, Postgres DSN, Flask secret) ever reach the browser —
the frontend only ever talks to our own /api/* routes.
"""
import os

from dotenv import load_dotenv

load_dotenv()


def _bool(name, default="false"):
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "on")


class Config:
    # --- Flask ---
    SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-change-me")
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = "Lax"
    MAX_CONTENT_LENGTH = 60 * 1024 * 1024  # 50MB files + headroom

    # --- MinIO / S3 ---
    MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
    MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "password123")
    MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "datalakev3")
    MINIO_SECURE = _bool("MINIO_SECURE", "false")

    # --- PostgreSQL ---
    PG_HOST = os.environ.get("PG_HOST", "localhost")
    PG_PORT = os.environ.get("PG_PORT", "5433")
    PG_DB = os.environ.get("PG_DB", "lakehouse")
    PG_USER = os.environ.get("PG_USER", "lakehouse")
    PG_PASSWORD = os.environ.get("PG_PASSWORD", "password123")

    # --- Spark ---
    # "local"   (default) — runs ETL in-process with pandas. No cluster needed;
    #           good for dev environments and small/medium files.
    # "cluster" — runs through a real PySpark SparkSession against SPARK_MASTER_URL.
    SPARK_MODE = os.environ.get("SPARK_MODE", "local")
    SPARK_MASTER_URL = os.environ.get("SPARK_MASTER_URL", "spark://localhost:7077")

    # Storage layout inside the bucket (kept from the previous browser-only build
    # so existing v3 data is not orphaned by this migration).
    BRONZE_PREFIX = "bronze_v3/"
    SILVER_PREFIX = "silver_v3/"
    GOLD_PREFIX = "gold_v3/"
    METADATA_PREFIX = "metadata_v3/"

    # Automatic merge: each predefined division/dataset-type gets exactly one
    # continuously-growing "Master" dataset here, instead of a new file per
    # upload. "Other Format" (skip_merge=True below) is the only type that
    # still gets a fresh Bronze file per upload.
    MASTER_PREFIX = "master_v3/"

    # Dataset catalog: divisions + required-column contracts per dataset type.
    # Lives on the server so the frontend never encodes business rules.
    DIVISIONS = [
        {
            "id": "network_operation", "label": "Network Operation", "dataset_types": [
                {"id": "daily_fiber_inspection", "label": "Daily Fiber Inspection",
                 "required_columns": ["inspection_date", "fiber_id", "status", "technician"]},
                {"id": "outage_report", "label": "Outage Report",
                 "required_columns": ["outage_date", "region", "duration_minutes", "cause"]},
            ],
        },
        {
            "id": "customer_service", "label": "Customer Service", "dataset_types": [
                {"id": "ticket_log", "label": "Ticket Log",
                 "required_columns": ["ticket_id", "customer_id", "opened_date", "status"]},
                {"id": "satisfaction_survey", "label": "Satisfaction Survey",
                 "required_columns": ["survey_date", "customer_id", "score"]},
            ],
        },
        {
            "id": "finance", "label": "Finance", "dataset_types": [
                {"id": "invoice_summary", "label": "Invoice Summary",
                 "required_columns": ["invoice_date", "region", "amount"]},
            ],
        },
        {
            "id": "other", "label": "Other Format", "dataset_types": [
                {"id": "other", "label": "Other Format", "required_columns": [], "skip_merge": True},
            ],
        },
    ]

    CLEANING_OPERATIONS = [
        {"id": "trim", "label": "Trim whitespace", "desc": "Remove leading and trailing spaces from text columns."},
        {"id": "normalize_case", "label": "Normalize case", "desc": "Make text columns consistently title case."},
        {"id": "fix_types", "label": "Fix data types", "desc": "Convert numeric- or date-looking text columns to proper types."},
        {"id": "dedupe", "label": "Remove duplicates", "desc": "Drop exact duplicate rows."},
        {"id": "fill_missing", "label": "Fill missing values", "desc": "Replace empty cells with a sensible default."},
    ]

    DIVISION_LOOKUP = {d["id"]: d for d in DIVISIONS}

    @classmethod
    def dataset_type_lookup(cls, division_id, type_id):
        division = cls.DIVISION_LOOKUP.get(division_id)
        if not division:
            return None
        for t in division["dataset_types"]:
            if t["id"] == type_id:
                return t
        return None
