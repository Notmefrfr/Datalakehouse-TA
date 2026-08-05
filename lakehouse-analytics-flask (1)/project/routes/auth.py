from flask import Blueprint, jsonify, request, session
from werkzeug.security import check_password_hash

from routes._common import current_user, postgres

bp = Blueprint("auth", __name__)


@bp.post("/login")
def login():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    if not username or not password:
        return jsonify({"error": "Username and password are required."}), 400

    user = postgres().get_user_by_username(username)
    if not user or not check_password_hash(user["password_hash"], password):
        return jsonify({"error": "Invalid username or password."}), 401

    session.clear()
    session["user_id"] = user["id"]
    postgres().log_action(user["id"], user["username"], "login")
    return jsonify({
        "id": user["id"], "username": user["username"],
        "role": user["role"], "full_name": user["full_name"],
    })


@bp.post("/logout")
def logout():
    user = current_user()
    if user:
        postgres().log_action(user["id"], user["username"], "logout")
    session.clear()
    return jsonify({"ok": True})


@bp.get("/me")
def me():
    user = current_user()
    if not user:
        return jsonify({"authenticated": False})
    return jsonify({
        "authenticated": True,
        "id": user["id"], "username": user["username"],
        "role": user["role"], "full_name": user["full_name"],
    })
