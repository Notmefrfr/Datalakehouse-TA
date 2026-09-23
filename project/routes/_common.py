"""Shared helpers for all route blueprints: service accessors + auth decorators."""
from functools import wraps

from flask import current_app, jsonify, session


def minio():
    return current_app.extensions["minio"]


def postgres():
    return current_app.extensions["postgres"]


def spark():
    return current_app.extensions["spark"]


def delta():
    return current_app.extensions["delta"]


def catalog():
    return current_app.extensions["catalog"]


def config():
    return current_app.config["APP_CONFIG"]


def current_user():
    if "user_id" not in session:
        return None
    return postgres().get_user_by_id(session["user_id"])


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        user = current_user()
        if not user:
            return jsonify({"error": "Authentication required."}), 401
        return fn(*args, user=user, **kwargs)
    return wrapper


def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        user = current_user()
        if not user:
            return jsonify({"error": "Authentication required."}), 401
        if user["role"] != "Administrator":
            return jsonify({"error": "Administrator access required."}), 403
        return fn(*args, user=user, **kwargs)
    return wrapper
