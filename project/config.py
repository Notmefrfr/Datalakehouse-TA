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
    MAX_CONTENT_LENGTH = 2 * 1024 * 1024 * 1024  # 2GB files + headroom

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

    # Hardcoded, per-format Summary configuration for the Visualize page —
    # deliberately NOT inferred generically, since "what's a meaningful
    # metric" genuinely differs per format (e.g. summing duration_minutes
    # is meaningful for Outage Report, but there's no equivalent numeric
    # column in Daily Fiber Inspection at all). Keyed by dataset_type id
    # (not division), because a single division can contain multiple
    # differently-shaped formats — see DIVISIONS above — and each format
    # needs its own metrics, not one config shared across all of them.
    #
    # Each entry has:
    #   "kpis"   — the small set of headline numbers shown as cards
    #   "table"  — the fuller metric/value breakdown shown below the chart
    #   "charts" — every VALID chart comparison for this format, grouped by
    #              chart type ("bar"/"pie"/"line"). Deliberately hardcoded
    #              rather than auto-picked (e.g. by statistical "strongest
    #              relationship" scoring) — that approach previously
    #              produced nonsense pairings like summing a reference/ID
    #              column grouped by region, since a generic score can't
    #              know a column is an identifier just because it looks
    #              numeric. A chart_type with an empty (or missing) list
    #              here means that chart type has NO valid comparison for
    #              this format — the frontend disables that option rather
    #              than falling back to guessing.
    # "kpis"/"table" are lists of {"label", "type", "column"} — "type" is
    # one of: count, sum, avg, max, min, nunique, top_category,
    # category_counts. "charts" entries are {"label", "x", "y"} where "x"
    # is always categorical or date, and "y" is either a numeric column
    # (sum-aggregated per x) or omitted/None (count of rows per x instead —
    # used when a format has no meaningful numeric column, e.g. counting
    # inspections per technician rather than summing an ID).
    # spark_service.py's compute_hardcoded_summary()/compute_hardcoded_charts()
    # are the two interpreters that read these for every format — adding a
    # new format later means adding entries here, not writing new
    # aggregation code.
    SUMMARY_CONFIG = {
        "daily_fiber_inspection": {
            "kpis": [
                {"label": "Total Inspections", "type": "count"},
                {"label": "OK Status Share", "type": "category_share", "column": "status", "value": "OK"},
                {"label": "Unique Fiber IDs", "type": "nunique", "column": "fiber_id"},
                {"label": "Technicians Involved", "type": "nunique", "column": "technician"},
            ],
            "table": [
                {"label": "Total Records", "type": "count"},
                {"label": "Status Breakdown", "type": "category_counts", "column": "status"},
                {"label": "Technician Breakdown", "type": "category_counts", "column": "technician"},
            ],
            # No genuine numeric column exists in this format (fiber_id is an
            # identifier, not a quantity) — every chart here is a count of
            # inspections, never a sum.
            "charts": {
                "bar": [
                    {"label": "Inspections by Status", "x": "status", "y": None},
                    {"label": "Inspections by Technician", "x": "technician", "y": None},
                ],
                "pie": [
                    {"label": "Status Share", "x": "status", "y": None},
                    {"label": "Technician Share", "x": "technician", "y": None},
                ],
                "line": [
                    {"label": "Inspections Over Time", "x": "inspection_date", "y": None},
                ],
            },
        },
        "outage_report": {
            "kpis": [
                {"label": "Total Outages", "type": "count"},
                {"label": "Total Downtime (min)", "type": "sum", "column": "duration_minutes"},
                {"label": "Average Duration (min)", "type": "avg", "column": "duration_minutes"},
                {"label": "Longest Outage (min)", "type": "max", "column": "duration_minutes"},
            ],
            "table": [
                {"label": "Total Records", "type": "count"},
                {"label": "Region Breakdown", "type": "category_counts", "column": "region"},
                {"label": "Cause Breakdown", "type": "category_counts", "column": "cause"},
                {"label": "Total Downtime (min)", "type": "sum", "column": "duration_minutes"},
                {"label": "Average Duration (min)", "type": "avg", "column": "duration_minutes"},
                {"label": "Longest Outage (min)", "type": "max", "column": "duration_minutes"},
            ],
            "charts": {
                "bar": [
                    {"label": "Total Downtime by Region", "x": "region", "y": "duration_minutes"},
                    {"label": "Total Downtime by Cause", "x": "cause", "y": "duration_minutes"},
                    {"label": "Outage Count by Region", "x": "region", "y": None},
                    {"label": "Outage Count by Cause", "x": "cause", "y": None},
                ],
                "pie": [
                    {"label": "Downtime Share by Region", "x": "region", "y": "duration_minutes"},
                    {"label": "Downtime Share by Cause", "x": "cause", "y": "duration_minutes"},
                ],
                "line": [
                    {"label": "Downtime Over Time", "x": "outage_date", "y": "duration_minutes"},
                    {"label": "Outage Count Over Time", "x": "outage_date", "y": None},
                ],
            },
        },
        "ticket_log": {
            "kpis": [
                {"label": "Total Tickets", "type": "count"},
                {"label": "Unique Customers", "type": "nunique", "column": "customer_id"},
                {"label": "Most Common Status", "type": "top_category", "column": "status"},
            ],
            "table": [
                {"label": "Total Records", "type": "count"},
                {"label": "Status Breakdown", "type": "category_counts", "column": "status"},
                {"label": "Unique Customers", "type": "nunique", "column": "customer_id"},
            ],
            # customer_id/ticket_id are identifiers, not quantities — every
            # chart here counts tickets, it never sums an ID.
            "charts": {
                "bar": [
                    {"label": "Tickets by Status", "x": "status", "y": None},
                ],
                "pie": [
                    {"label": "Status Share", "x": "status", "y": None},
                ],
                "line": [
                    {"label": "Tickets Opened Over Time", "x": "opened_date", "y": None},
                ],
            },
        },
        "satisfaction_survey": {
            "kpis": [
                {"label": "Total Surveys", "type": "count"},
                {"label": "Average Score", "type": "avg", "column": "score"},
                {"label": "Highest Score", "type": "max", "column": "score"},
                {"label": "Lowest Score", "type": "min", "column": "score"},
            ],
            "table": [
                {"label": "Total Records", "type": "count"},
                {"label": "Average Score", "type": "avg", "column": "score"},
                {"label": "Highest Score", "type": "max", "column": "score"},
                {"label": "Lowest Score", "type": "min", "column": "score"},
                {"label": "Unique Customers", "type": "nunique", "column": "customer_id"},
            ],
            # This format has no genuine low-cardinality categorical column
            # to group by (only customer_id, an identifier) — so there is no
            # valid Bar or Pie comparison at all, only a Line trend of score
            # over time. Deliberately left empty rather than reaching for
            # something like "score vs customer_id" just to fill the slot.
            "charts": {
                "bar": [],
                "pie": [],
                "line": [
                    {"label": "Average Score Over Time", "x": "survey_date", "y": "score"},
                ],
            },
        },
        "invoice_summary": {
            "kpis": [
                {"label": "Total Invoices", "type": "count"},
                {"label": "Total Amount", "type": "sum", "column": "amount"},
                {"label": "Average Amount", "type": "avg", "column": "amount"},
                {"label": "Highest Invoice", "type": "max", "column": "amount"},
            ],
            "table": [
                {"label": "Total Records", "type": "count"},
                {"label": "Region Breakdown", "type": "category_counts", "column": "region"},
                {"label": "Total Amount", "type": "sum", "column": "amount"},
                {"label": "Average Amount", "type": "avg", "column": "amount"},
                {"label": "Highest Invoice", "type": "max", "column": "amount"},
                {"label": "Lowest Invoice", "type": "min", "column": "amount"},
            ],
            "charts": {
                "bar": [
                    {"label": "Total Amount by Region", "x": "region", "y": "amount"},
                    {"label": "Invoice Count by Region", "x": "region", "y": None},
                ],
                "pie": [
                    {"label": "Amount Share by Region", "x": "region", "y": "amount"},
                    {"label": "Invoice Count Share by Region", "x": "region", "y": None},
                ],
                "line": [
                    {"label": "Amount Over Time", "x": "invoice_date", "y": "amount"},
                    {"label": "Invoice Count Over Time", "x": "invoice_date", "y": None},
                ],
            },
        },
    }

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