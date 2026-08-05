"""
Application factory. Wires up MinIO / Postgres / Spark services and registers
all route blueprints. The frontend (templates/index.html + static/js/script.js)
only ever calls the /api/* routes registered here.
"""
from flask import Flask, jsonify, render_template

from config import Config
from services.catalog_service import CatalogService
from services.minio_service import MinioService
from services.postgres_service import PostgresService
from services.spark_service import SparkETLService


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    app.config["APP_CONFIG"] = Config

    # --- services (each owns exactly one external system) ---
    minio_service = MinioService(Config)
    postgres_service = PostgresService(Config)
    spark_service = SparkETLService(Config)
    catalog_service = CatalogService(Config, minio_service, postgres_service)

    app.extensions["minio"] = minio_service
    app.extensions["postgres"] = postgres_service
    app.extensions["spark"] = spark_service
    app.extensions["catalog"] = catalog_service

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
    application.run(host="0.0.0.0", port=5000, debug=True)
