"""
One-time seed script: creates the first Administrator account.
Run after applying schema.sql:

    python db/seed.py <username> <password> [full_name]

Example:
    python db/seed.py admin "correct horse battery staple" "Workspace Admin"

With no arguments, it falls back to SEED_ADMIN_USERNAME / SEED_ADMIN_PASSWORD /
SEED_ADMIN_FULL_NAME from the environment (used by the docker-compose "seed"
service, which passes no CLI args).
"""
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from werkzeug.security import generate_password_hash  # noqa: E402

from config import Config  # noqa: E402
from services.postgres_service import PostgresService  # noqa: E402


def main():
    if len(sys.argv) >= 3:
        username, password = sys.argv[1], sys.argv[2]
        full_name = sys.argv[3] if len(sys.argv) > 3 else username
    else:
        username = os.environ.get("SEED_ADMIN_USERNAME")
        password = os.environ.get("SEED_ADMIN_PASSWORD")
        full_name = os.environ.get("SEED_ADMIN_FULL_NAME", username)
        if not username or not password:
            print("Usage: python db/seed.py <username> <password> [full_name]")
            print("  (or set SEED_ADMIN_USERNAME / SEED_ADMIN_PASSWORD env vars)")
            sys.exit(1)

    print(f"Connecting to Postgres at {Config.PG_HOST}:{Config.PG_PORT}/{Config.PG_DB} as {Config.PG_USER}...")

    pg = None
    for attempt in range(10):
        try:
            pg = PostgresService(Config)
            pg.health_check()
            break
        except Exception:
            time.sleep(2)
    if pg is None:
        print("Could not reach PostgreSQL after retrying — check PG_HOST/PG_PORT.")
        sys.exit(1)

    if pg.get_user_by_username(username):
        print(f"User '{username}' already exists.")
        return

    user_id = pg.create_user(
        username=username,
        password_hash=generate_password_hash(password),
        role="Administrator",
        full_name=full_name,
    )
    print(f"Created Administrator '{username}' (id={user_id}).")


if __name__ == "__main__":
    main()
