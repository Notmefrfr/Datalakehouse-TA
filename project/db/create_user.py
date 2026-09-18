"""
Create an additional user account (Administrator or Employee).

Usage:
    python db/create_user.py <username> <password> <role> [full_name]

    <role> must be exactly "Administrator" or "Employee" (matches the
    CHECK constraint on users.role in db/schema.sql).

Examples:
    python db/create_user.py jsmith "a-strong-password" Employee "Jane Smith"
    python db/create_user.py mchen  "another-strong-pw" Administrator

Run this the same way you'd run db/seed.py — either inside the app
container (docker compose exec app python db/create_user.py ...) or from
your venv with the .env pointed at the right Postgres instance.
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from werkzeug.security import generate_password_hash  # noqa: E402

from config import Config  # noqa: E402
from services.postgres_service import PostgresService  # noqa: E402

VALID_ROLES = ("Administrator", "Employee")


def main():
    if len(sys.argv) < 4:
        print(__doc__)
        sys.exit(1)

    username, password, role = sys.argv[1], sys.argv[2], sys.argv[3]
    full_name = sys.argv[4] if len(sys.argv) > 4 else username

    if role not in VALID_ROLES:
        print(f"Invalid role '{role}'. Must be one of: {', '.join(VALID_ROLES)}")
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
        role=role,
        full_name=full_name,
    )
    print(f"Created {role} '{username}' (id={user_id}).")


if __name__ == "__main__":
    main()
