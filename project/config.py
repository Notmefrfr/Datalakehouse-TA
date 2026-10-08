"""
Central configuration, loaded from environment variables (see .env.example).
No secrets (MinIO keys, Postgres DSN, Flask secret) ever reach the browser —
the frontend only ever talks to our own /api/* routes.

CHANGED (dynamic-dataset feature, 2026-09-24): Config.DIVISIONS,
Config.SUMMARY_CONFIG, Config.DIVISION_LOOKUP and Config.dataset_type_lookup()
are GONE. There is no more fixed division/dataset_type catalog — every
dataset is identified purely by its own dataset_id (a slug generated from a
name given at upload time), and which columns/KPIs/charts apply to it is
worked out dynamically from the data itself (see services/dataset_service.py
for matching, services/spark_service.py's compute_auto_summary()/
compute_auto_charts() for the generic KPI/chart logic that replaces
SUMMARY_CONFIG). Anything that imported Config.DIVISIONS or
Config.SUMMARY_CONFIG needs to be updated — this touched catalog_service.py,
routes/upload.py, and routes/datasets.py in this same change.
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
    # Big-data upload (routes/upload.py + services/dataset_service.py stream the
    # file to disk and process it in row chunks, so these are disk/time/RAM
    # budgets rather than hard limits). nginx.conf's client_max_body_size must
    # stay >= MAX_UPLOAD_SIZE_GB.
    MAX_UPLOAD_SIZE_GB = float(os.environ.get("MAX_UPLOAD_SIZE_GB", 10.0))
    UPLOAD_CHUNK_ROWS = int(os.environ.get("UPLOAD_CHUNK_ROWS", 200000))
    DEDUPE_GLOBAL_MAX_ROWS = int(os.environ.get("DEDUPE_GLOBAL_MAX_ROWS", 5000000))
    # Rows read from the start of an upload to decide, once, which columns are
    # numeric/date (SparkETLService.infer_column_types) for every chunk.
    TYPE_SAMPLE_ROWS = int(os.environ.get("TYPE_SAMPLE_ROWS", 50000))
    MAX_CONTENT_LENGTH = int(MAX_UPLOAD_SIZE_GB * 1024 * 1024 * 1024)

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

    # Master datasets live as real Delta Lake tables (Parquet data files +
    # a _delta_log transaction log) under this prefix, one table per
    # dataset_id, instead of one giant CSV object. Every upload lands as
    # its own small Parquet file; COMPACT_JOB below periodically rewrites
    # the small files a dataset has accumulated into fewer, larger ones.
    MASTER_DELTA_PREFIX = "master_delta_v1/"

    # MASTER_PREFIX (legacy) held one single ever-growing CSV per
    # division__dtype format under the old model — kept here ONLY so
    # DeltaService can migrate an old file into a Delta table the first
    # time it's touched (see DeltaService.ensure_table). New data never
    # gets written here again, and no NEW dataset created under the
    # dynamic model ever uses this prefix.
    MASTER_PREFIX = "master_v3/"

    # Small-file compaction (see services/compaction_job.py). A background
    # job checks, every COMPACT_CHECK_INTERVAL_HOURS, whether each
    # dataset's Delta table was last compacted more than
    # COMPACT_INTERVAL_DAYS ago; if so it rewrites its small files into
    # ones sized up to roughly COMPACT_TARGET_FILE_SIZE_MB.
    COMPACT_TARGET_FILE_SIZE_MB = int(os.environ.get("COMPACT_TARGET_FILE_SIZE_MB", 5000))  # 5GB
    COMPACT_INTERVAL_DAYS = int(os.environ.get("COMPACT_INTERVAL_DAYS", 7))
    COMPACT_CHECK_INTERVAL_HOURS = int(os.environ.get("COMPACT_CHECK_INTERVAL_HOURS", 6))

    # --- AI assistant (services/llm_service.py, services/chat_agent.py) ---
    # A local Ollama server running Qwen3 8B. Nothing is sent to any outside
    # service. Bump LLM_NUM_CTX if you have the RAM/VRAM (8192 is a safe
    # default for an 8B model; Ollama's own default of 2048 is far too small
    # once the dataset list + tool results are in the prompt).
    LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "http://localhost:11434")
    LLM_MODEL = os.environ.get("LLM_MODEL", "qwen3:8b")
    LLM_NUM_CTX = int(os.environ.get("LLM_NUM_CTX", 8192))
    LLM_TEMPERATURE = float(os.environ.get("LLM_TEMPERATURE", 0.3))
    LLM_TIMEOUT_SECONDS = int(os.environ.get("LLM_TIMEOUT_SECONDS", 300))
    LLM_KEEP_ALIVE = os.environ.get("LLM_KEEP_ALIVE", "30m")   # keep the model loaded between questions
    LLM_MAX_TOOL_STEPS = int(os.environ.get("LLM_MAX_TOOL_STEPS", 8))
    CHAT_MAX_ROWS = int(os.environ.get("CHAT_MAX_ROWS", 500))   # rows a query may return to the browser
    CHAT_SQL_TIMEOUT_SECONDS = int(os.environ.get("CHAT_SQL_TIMEOUT_SECONDS", 20))
    CHAT_HISTORY_MESSAGES = int(os.environ.get("CHAT_HISTORY_MESSAGES", 12))
    CHAT_RATE_LIMIT = os.environ.get("CHAT_RATE_LIMIT", "20 per minute")
    # Every chat tool EXCEPT run_query (which streams the full table lazily through
    # DuckDB — see services/sql_sandbox.load_sample_dataframe) works off a bounded,
    # randomly-sampled slice of a dataset rather than the full table, so a big
    # dataset can never be fully materialized into one worker's RAM just to answer
    # a schema/statistics/chart/ML question. Always disclosed to the model when a
    # result is actually sampled (services/chat_agent.py's _frame()).
    CHAT_SAMPLE_MAX_ROWS = int(os.environ.get("CHAT_SAMPLE_MAX_ROWS", 500000))
    # Wall-clock cap for any single tool call (run_query has its own, tighter,
    # CHAT_SQL_TIMEOUT_SECONDS above). This is a soft timeout — the request gets a
    # clean error back, but Python can't forcibly kill the worker thread still
    # running underneath, so it keeps consuming CPU/memory until it naturally
    # finishes. It bounds how long a person waits, not the server's resource use.
    CHAT_TOOL_TIMEOUT_SECONDS = int(os.environ.get("CHAT_TOOL_TIMEOUT_SECONDS", 45))
    # How many data/analysis/ML tool calls may run at once PER WORKER PROCESS. This
    # is a per-process threading.Semaphore, not a cluster-wide limit — with 3 app
    # replicas x 4 gunicorn workers each, the real ceiling is up to 12x this number
    # across the whole deployment. A true cross-replica limit would need a shared
    # coordinator (e.g. a Postgres advisory lock or Redis counter), which isn't in
    # place yet.
    CHAT_MAX_CONCURRENT_TOOLS = int(os.environ.get("CHAT_MAX_CONCURRENT_TOOLS", 4))
    # Machine-learning tools (decision_tree, KNN, regressions, KMeans) train on at most
    # this many rows (a seeded random sample beyond that) so one chat question can't
    # exhaust the Flask worker. Scatter plots draw at most CHART_MAX_POINTS points.
    ML_MAX_TRAIN_ROWS = int(os.environ.get("ML_MAX_TRAIN_ROWS", 50000))
    CHART_MAX_POINTS = int(os.environ.get("CHART_MAX_POINTS", 500))

    CLEANING_OPERATIONS = [
        {"id": "trim", "label": "Trim whitespace", "desc": "Remove leading and trailing spaces from text columns."},
        {"id": "normalize_case", "label": "Normalize case", "desc": "Make text columns consistently title case."},
        {"id": "fix_types", "label": "Fix data types", "desc": "Convert numeric- or date-looking text columns to proper types."},
        {"id": "dedupe", "label": "Remove duplicates", "desc": "Drop exact duplicate rows."},
        {"id": "fill_missing", "label": "Fill missing values", "desc": "Replace empty cells with a sensible default."},
    ]
