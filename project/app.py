"""
Application factory. Wires up MinIO / Postgres / Spark / Delta services and
registers all route blueprints. The frontend only ever calls the /api/*
routes registered here.

CHANGED (dynamic-dataset feature, 2026-09-24): wires up the new
DatasetService (services/dataset_service.py) — the "no more
division/dataset_type" matching/approval brain — into app.extensions, same
pattern as every other service. routes/upload.py and routes/admin.py's
pending-changes endpoints depend on it (via routes/_common.py's new
dataset_service() accessor).

NOTE: routes/etl.py is registered below unchanged — I haven't seen its
current contents in this pass, and it likely still references the old
layer/division model (it was the Silver "manual cleaning" step). Flag this
to whoever wires up the frontend: it probably needs the same
layer-removal/dataset_id treatment as routes/datasets.py, and its blueprint
registration here may need to change too. Not touched in this change.
"""
import os
import time
from datetime import timedelta

from flask import Flask, jsonify, render_template, request, session
from flask_limiter.errors import RateLimitExceeded
from werkzeug.middleware.proxy_fix import ProxyFix

from config import Config
from services.catalog_service import CatalogService
from services.dataset_service import DatasetService
from services.delta_service import DeltaService
from services.llm_service import LLMService
from services.minio_service import MinioService
from services.postgres_service import PostgresService
from services.rate_limiter import limiter
from services.spark_service import SparkETLService

# How long a session can sit idle before the next request forces a fresh
# login.
SESSION_IDLE_TIMEOUT_SECONDS = int(os.environ.get("SESSION_IDLE_TIMEOUT_SECONDS", 30 * 60))


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    app.config["APP_CONFIG"] = Config

    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

    # --- session cookie hardening ---
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SECURE"] = os.environ.get("SESSION_COOKIE_SECURE", "false").lower() == "true"
    app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=12)

    # --- services (each owns exactly one external system) ---
    minio_service = MinioService(Config)
    postgres_service = PostgresService(Config)
    spark_service = SparkETLService(Config)
    delta_service = DeltaService(Config, minio_service)
    catalog_service = CatalogService(Config, minio_service, postgres_service, delta_service)
    dataset_service = DatasetService(Config, minio_service, postgres_service, delta_service, spark_service)

    app.extensions["minio"] = minio_service
    app.extensions["postgres"] = postgres_service
    app.extensions["spark"] = spark_service
    app.extensions["delta"] = delta_service
    app.extensions["catalog"] = catalog_service
    app.extensions["dataset_service"] = dataset_service
    app.extensions["llm"] = LLMService(Config)

    limiter.init_app(app)

    @app.errorhandler(RateLimitExceeded)
    def ratelimit_handler(e):
        return jsonify({"error": "Too many attempts. Please try again shortly."}), 429

    @app.errorhandler(413)
    def file_too_large_handler(e):
        return jsonify({"error": f"File is too large. Maximum allowed upload size is {Config.MAX_UPLOAD_SIZE_GB}GB."}), 413

    @app.errorhandler(500)
    def internal_error_handler(e):
        app.logger.exception("Unhandled exception")
        # ALSO written straight to a file (upload_error.log, next to app.py),
        # not just the terminal/app.logger — added 2026-10-01 while chasing
        # an upload 500 whose traceback was hard to locate in the terminal
        # (background process, hidden console window, scrollback cut off,
        # etc.). This guarantees the full traceback for every 500 is sitting
        # in one plain-text file to open and copy, no terminal-hunting
        # needed. Safe to delete this whole try/except block once that
        # investigation is done — it's not meant to be permanent.
        try:
            import traceback as _traceback
            from datetime import datetime as _datetime
            log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "upload_error.log")
            with open(log_path, "a", encoding="utf-8") as _f:
                _f.write(f"\n{'=' * 70}\n{_datetime.now().isoformat()}  {request.method} {request.path}\n{'=' * 70}\n")
                _f.write(_traceback.format_exc())
                _f.write("\n")
        except Exception:
            pass
        return jsonify({"error": "Something went wrong on our end. Please try again."}), 500

    @app.before_request
    def _enforce_session_idle_timeout():
        if "user_id" not in session:
            return
        last_activity = session.get("last_activity")
        now = time.time()
        if last_activity is not None and (now - last_activity) > SESSION_IDLE_TIMEOUT_SECONDS:
            session.clear()
        else:
            session["last_activity"] = now

    # --- blueprints ---
    from routes.admin import bp as admin_bp
    from routes.auth import bp as auth_bp
    from routes.chat import bp as chat_bp
    from routes.datasets import bp as datasets_bp
    from routes.etl import bp as etl_bp
    from routes.upload import bp as upload_bp

    app.register_blueprint(auth_bp, url_prefix="/api")
    app.register_blueprint(datasets_bp, url_prefix="/api")
    app.register_blueprint(upload_bp, url_prefix="/api")
    app.register_blueprint(etl_bp, url_prefix="/api")
    app.register_blueprint(admin_bp, url_prefix="/api")
    app.register_blueprint(chat_bp, url_prefix="/api")

    @app.get("/api/health")
    def health():
        checks = {}
        try:
            checks["minio"] = minio_service.health_check()
        except Exception as e:
            checks["minio"] = f"error: {e}"
        try:
            checks["postgres"] = postgres_service.health_check()
        except Exception as e:
            checks["postgres"] = f"error: {e}"
        return jsonify(checks)

    @app.get("/")
    def index():
        # max_upload_size_gb is read by static/js/script.js's client-side
        # size check (a UX fast-path only, not a security boundary — the
        # server-side MAX_CONTENT_LENGTH is what actually enforces it), so
        # the two numbers can never drift apart the way two hardcoded
        # copies of "2GB" eventually did.
        return render_template("index.html", max_upload_size_gb=Config.MAX_UPLOAD_SIZE_GB)

    # --- weekly Delta small-file compaction ---
    if os.environ.get("WERKZEUG_RUN_MAIN") or not app.debug:
        from services.compaction_job import start_compaction_scheduler
        start_compaction_scheduler(app, catalog_service, postgres_service, delta_service)

    return app


if __name__ == "__main__":
    application = create_app()
    debug_mode = os.environ.get("FLASK_DEBUG") == "1"
    application.run(host="0.0.0.0", port=5000, debug=debug_mode)
