"""
Shared Flask-Limiter instance. Created without an app (the app factory calls
limiter.init_app(app) in create_app()), so any blueprint can import `limiter`
and decorate routes with it without worrying about import order.

Storage backend is controlled by the REDIS_URL environment variable:

  - REDIS_URL unset  -> in-memory storage. Only correct for a single
    process (no gunicorn -w > 1, no multiple containers). Fine for quick
    local dev; the real deployment must set REDIS_URL.
  - REDIS_URL set (e.g. "redis://redis:6379/0") -> shared storage. Every
    worker process / every replica counts against the SAME limit state,
    which is the only way "5 attempts per 5 minutes" actually means 5 and
    not "5 per worker" (see ARCHITECTURE-RISK-AUDIT.md #2 — this was
    previously silently multiplying the real limit by the worker count).

Requires the `redis` package once REDIS_URL is set — add `redis` to
requirements.txt (flask-limiter's redis storage backend depends on it, and
it isn't pulled in automatically since most flask-limiter installs don't
need it).
"""
import os

from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

REDIS_URL = os.environ.get("REDIS_URL", "").strip()

# No default_limits: nothing is rate-limited unless a route explicitly opts
# in with its own @limiter.limit(...) — keeps this from silently throttling
# routes nobody asked to protect.
# headers_enabled=True adds a Retry-After header (seconds until the window
# resets) to 429 responses, which app.py's error handler reads to tell the
# person exactly how long to wait instead of a vague "try again later".
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[],
    headers_enabled=True,
    storage_uri=REDIS_URL or "memory://",
)
