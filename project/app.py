"""
Application factory. Wires up MinIO / Postgres / Spark services and registers
all route blueprints. The frontend (templates/index.html + static/js/script.js)
only ever calls the /api/* routes registered here.
"""
import os
import time
from datetime import timedelta

from flask import Flask, jsonify, render_template, session
from flask_limiter.errors import RateLimitExceeded
from werkzeug.middleware.proxy_fix import ProxyFix

from config import Config
from services.catalog_service import CatalogService
from services.minio_service import MinioService
from services.postgres_service import PostgresService
from services.rate_limiter import limiter
from services.spark_service import SparkETLService

# How long a session can sit idle before the next request forces a fresh
# login. This is what makes "quit and come back later" actually re-prompt
# for a password — browser session-cookie behavior alone isn't reliable for
# this, since several browsers restore cookies across a restart when their
# "reopen tabs" / session-restore feature is on, so a real logout can't
# depend on the browser fully discarding the cookie. Overridable via env
# for local dev if 30 minutes is annoying to test against.
SESSION_IDLE_TIMEOUT_SECONDS = int(os.environ.get("SESSION_IDLE_TIMEOUT_SECONDS", 30 * 60))


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    app.config["APP_CONFIG"] = Config

    # nginx now sits in front of this app (see nginx.conf) as a load
    # balancer across multiple app containers. Without this, every request
    # Flask sees has remote_addr = nginx's own container IP, not the real
    # visitor's — which would silently break the per-IP login rate limit
    # added earlier: every person behind nginx would look identical, so one
    # person's failed logins would lock everyone else out too. ProxyFix
    # makes Flask trust nginx's X-Forwarded-For/X-Forwarded-Proto/X-Forwarded-Host
    # headers instead of the raw socket address.
    # x_for=1 / x_proto=1 / x_host=1: trust exactly ONE layer of proxy
    # (nginx). If another proxy/CDN is ever added in front of nginx, this
    # number must increase to match, or a client could spoof these headers
    # and impersonate a different IP to dodge rate limiting.
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

    # --- session cookie hardening ---
    # SESSION_COOKIE_SAMESITE="Lax" is the actual CSRF defense: browsers
    # withhold the cookie on cross-site POST/PUT/DELETE requests (the shape
    # every CSRF attack takes), while still sending it on normal same-site
    # use and on top-level cross-site GET navigation (so following a link
    # into the app from email/Slack/etc. still works).
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_HTTPONLY"] = True  # JS can't read the cookie even via an XSS bug
    # Off by default so local http:// docker-compose keeps working — flip
    # this on (env var, not a code change) once this is served over HTTPS,
    # since a Secure cookie is silently never sent over plain HTTP at all.
    app.config["SESSION_COOKIE_SECURE"] = os.environ.get("SESSION_COOKIE_SECURE", "false").lower() == "true"
    # Belt-and-suspenders cap in case anything ever sets session.permanent =
    # True — without that call sessions are already browser-session-only
    # (die when the browser fully discards cookies), this just bounds the
    # worst case at 12h instead of Flask's 31-day default.
    app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=12)

    # --- services (each owns exactly one external system) ---
    minio_service = MinioService(Config)
    postgres_service = PostgresService(Config)
    spark_service = SparkETLService(Config)
    catalog_service = CatalogService(Config, minio_service, postgres_service)

    app.extensions["minio"] = minio_service
    app.extensions["postgres"] = postgres_service
    app.extensions["spark"] = spark_service
    app.extensions["catalog"] = catalog_service

    limiter.init_app(app)

    @app.errorhandler(RateLimitExceeded)
    def ratelimit_handler(e):
        # headers_enabled=True (see services/rate_limiter.py) attaches an
        # accurate Retry-After header to this response; the frontend reads
        # it to drive a live countdown. This text is just the fallback.
        return jsonify({"error": "Too many attempts. Please try again shortly."}), 429

    @app.errorhandler(413)
    def file_too_large_handler(e):
        # Flask/Werkzeug raises this automatically the moment a request body
        # exceeds Config.MAX_CONTENT_LENGTH, before our route code even runs
        # — this is what actually enforces the upload size cap, not just a
        # frontend label. Kept as a plain MB/GB figure rather than reading
        # MAX_CONTENT_LENGTH back out, since that's set once at startup and
        # rarely changes; update the number here if that value changes.
        return jsonify({"error": "File is too large. Maximum allowed upload size is 2GB."}), 413

    @app.errorhandler(500)
    def internal_error_handler(e):
        # Safety net for anything that slips past a route's own try/except
        # (a bug, a missing dependency, a genuinely unexpected crash). The
        # frontend's fetch wrapper falls back to res.statusText when a
        # response isn't JSON, which is literally the string "Internal
        # Server Error" — this replaces that raw, unhelpful text with a
        # real message everywhere in the app, not just the routes we
        # remembered to wrap individually. The actual exception still goes
        # to the server log (app.logger) for debugging; the person using
        # the app never sees a Python traceback.
        app.logger.exception("Unhandled exception")
        return jsonify({"error": "Something went wrong on our end. Please try again."}), 500

    @app.before_request
    def _enforce_session_idle_timeout():
        """Signed in but haven't made a request in SESSION_IDLE_TIMEOUT_SECONDS?
        Session gets cleared — the next call to /api/me (or any @login_required
        route) reports logged-out, and the frontend routes back to the login
        screen. Every authenticated request refreshes the clock (sliding
        window), so this is "log out after N minutes of inactivity," not
        "log out exactly N minutes after login."""
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
    from routes.datasets import bp as datasets_bp
    from routes.etl import bp as etl_bp
    from routes.upload import bp as upload_bp

    app.register_blueprint(auth_bp, url_prefix="/api")
    app.register_blueprint(datasets_bp, url_prefix="/api")
    app.register_blueprint(upload_bp, url_prefix="/api")
    app.register_blueprint(etl_bp, url_prefix="/api")
    app.register_blueprint(admin_bp, url_prefix="/api")

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
        return render_template("index.html")

    return app


if __name__ == "__main__":
    application = create_app()
    # Debug mode is OFF unless explicitly requested — Werkzeug's interactive
    # debugger lets anyone who triggers an unhandled exception run arbitrary
    # Python in the browser, so it must never be on by default. To develop
    # locally with auto-reload + the debugger, set FLASK_DEBUG=1 first:
    #   Windows (PowerShell):  $env:FLASK_DEBUG = "1"; python app.py
    #   macOS/Linux:           FLASK_DEBUG=1 python app.py
    debug_mode = os.environ.get("FLASK_DEBUG") == "1"
    application.run(host="0.0.0.0", port=5000, debug=debug_mode)