import os
import time

from flask import Blueprint, jsonify, request, session
from flask_limiter.util import get_remote_address
from werkzeug.security import check_password_hash

from routes._common import current_user, postgres
from services.rate_limiter import limiter

bp = Blueprint("auth", __name__)

# Two INDEPENDENT limits guard /login — exceeding either one blocks the
# request, regardless of the other:
#
#   - per-IP: stops one IP from hammering many different accounts
#     (username enumeration / credential spraying). Looser, since a shared
#     office/campus/NAT connection can legitimately have several people
#     failing a password at once.
#   - per-username: stops one specific account from being brute-forced no
#     matter how many different IPs the attempts come from (e.g. a
#     botnet). This is the one that matters most for "this exact account
#     is under attack," so it's the tighter of the two.
#
# Previously this was a single limit keyed on ip+username COMBINED, which
# meant an attacker rotating IPs against one username reset the counter on
# every new IP (never actually capped), and one IP trying many different
# usernames got a fresh bucket per username too (also never capped). Two
# separate limits close both gaps at once.
#
# Overridable for local dev iteration only — e.g. set
# LOGIN_USERNAME_RATE_LIMIT="5 per 15 seconds" in your shell before
# `python app.py` so you don't have to wait 5 minutes between manual test
# rounds. Leave unset for the real protection (what actually ships).
LOGIN_IP_RATE_LIMIT = os.environ.get("LOGIN_IP_RATE_LIMIT", "20 per 5 minutes")
LOGIN_USERNAME_RATE_LIMIT = os.environ.get("LOGIN_USERNAME_RATE_LIMIT", "5 per 5 minutes")


def _login_ip_key():
    return f"login-ip:{get_remote_address()}"


def _login_username_key():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip().lower()
    # Still needs a bucket even when blank/malformed, so a request with no
    # username can't dodge the per-username limit entirely by omission —
    # every blank attempt shares one bucket rather than each being "free."
    return f"login-username:{username or '(blank)'}"


@bp.post("/login")
@limiter.limit(
    LOGIN_IP_RATE_LIMIT,
    key_func=_login_ip_key,
    deduct_when=lambda response: response.status_code != 200,
)
@limiter.limit(
    LOGIN_USERNAME_RATE_LIMIT,
    key_func=_login_username_key,
    deduct_when=lambda response: response.status_code != 200,
)
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
    # Explicit, not just "left at Flask's default": this cookie dies with
    # the browser session rather than persisting for days. Paired with
    # app.py's idle-timeout before_request hook, a stale session can't be
    # replayed by leaving a tab open indefinitely either.
    session.permanent = False
    session["user_id"] = user["id"]
    session["last_activity"] = time.time()
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
