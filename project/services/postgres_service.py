"""
PostgreSQL service.

The only module allowed to import psycopg2. Owns: users, dataset metadata
(display name / division override / archived flag), audit log, dataset
version history, and Spark's reporting tables.
"""
import hashlib
import json
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool


class PostgresService:
    def __init__(self, app_config):
        self._pool = SimpleConnectionPool(
            1, 10,
            host=app_config.PG_HOST, port=app_config.PG_PORT,
            dbname=app_config.PG_DB, user=app_config.PG_USER,
            password=app_config.PG_PASSWORD,
        )

    # -- low level -----------------------------------------------------------

    def _conn(self):
        return self._pool.getconn()

    def _release(self, conn):
        self._pool.putconn(conn)

    def health_check(self):
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        finally:
            self._release(conn)

    @staticmethod
    def _lock_id(key: str) -> int:
        # pg_advisory_lock takes a signed bigint. Deterministically hash the
        # string key down into one — deliberately NOT Python's built-in
        # hash(), which is randomized per-process (PYTHONHASHSEED) unless
        # disabled. That randomization is exactly the kind of thing that
        # would silently break this: two Gunicorn worker processes would
        # compute two DIFFERENT lock ids for the same format_key and never
        # actually block each other, defeating the whole point without any
        # error to indicate it. sha256 gives the same integer for the same
        # string on every process, every run, forever.
        digest = hashlib.sha256(key.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big", signed=True)

    @contextmanager
    def advisory_lock(self, key: str):
        """Serializes everything inside the `with` block against every other
        process/thread that calls this with the same key, using a Postgres
        session-level advisory lock (SELECT pg_advisory_lock — blocks until
        free, doesn't fail). Use this to wrap a read-modify-write sequence
        that must not race with an identical sequence for the same key, e.g.
        two uploads to the same master dataset arriving at the same time.

        Uses its own dedicated connection for the lock's whole lifetime,
        separate from whatever connections the code inside the `with` block
        acquires for its own reads/writes — advisory locks are tied to the
        session that took them, so releasing requires that exact same
        connection, not just "a" connection from the pool.
        """
        lock_id = self._lock_id(key)
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_lock(%s)", (lock_id,))
            try:
                yield
            finally:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))
                conn.commit()
        finally:
            self._release(conn)

    @contextmanager
    def try_advisory_lock(self, key: str):
        """Non-blocking sibling of advisory_lock() — used for the periodic
        compaction check (services/compaction_job.py), which runs
        independently in every app replica. Yields True/False for whether
        the lock was actually acquired; a replica that loses the race just
        skips this run instead of blocking, since another replica is
        already compacting this exact format."""
        lock_id = self._lock_id(key)
        conn = self._conn()
        acquired = False
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
                acquired = cur.fetchone()[0]
            try:
                yield acquired
            finally:
                if acquired:
                    with conn.cursor() as cur:
                        cur.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))
                    conn.commit()
        finally:
            self._release(conn)

    # -- users -----------------------------------------------------------------

    def get_user_by_username(self, username):
        conn = self._conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, username, password_hash, role, full_name FROM users WHERE username = %s",
                    (username,),
                )
                return cur.fetchone()
        finally:
            self._release(conn)

    def get_user_by_id(self, user_id):
        conn = self._conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, username, role, full_name FROM users WHERE id = %s", (user_id,)
                )
                return cur.fetchone()
        finally:
            self._release(conn)

    def count_users(self):
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users")
                return cur.fetchone()[0]
        finally:
            self._release(conn)

    def create_user(self, username, password_hash, role, full_name=None):
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (username, password_hash, role, full_name) "
                    "VALUES (%s, %s, %s, %s) RETURNING id",
                    (username, password_hash, role, full_name),
                )
                new_id = cur.fetchone()[0]
            conn.commit()
            return new_id
        finally:
            self._release(conn)

    # -- dataset metadata --------------------------------------------------

    def get_metadata(self, layer, name):
        conn = self._conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT display_name, owner, division_override, archived, updated_at "
                    "FROM dataset_metadata WHERE layer = %s AND name = %s",
                    (layer, name),
                )
                row = cur.fetchone()
                return dict(row) if row else {}
        finally:
            self._release(conn)

    def upsert_metadata(self, layer, name, **fields):
        allowed = {"display_name", "owner", "division_override", "archived"}
        fields = {k: v for k, v in fields.items() if k in allowed}
        if not fields:
            return
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM dataset_metadata WHERE layer = %s AND name = %s", (layer, name)
                )
                exists = cur.fetchone() is not None
                if exists:
                    set_clause = ", ".join(f"{k} = %s" for k in fields)
                    cur.execute(
                        f"UPDATE dataset_metadata SET {set_clause}, updated_at = now() "
                        f"WHERE layer = %s AND name = %s",
                        (*fields.values(), layer, name),
                    )
                else:
                    cols = ", ".join(["layer", "name", *fields.keys()])
                    placeholders = ", ".join(["%s"] * (2 + len(fields)))
                    cur.execute(
                        f"INSERT INTO dataset_metadata ({cols}) VALUES ({placeholders})",
                        (layer, name, *fields.values()),
                    )
            conn.commit()
        finally:
            self._release(conn)

    def list_metadata_names(self, layer):
        """(name, updated_at) for every dataset_metadata row in this layer —
        used as the source of truth for which Master datasets exist, since
        a Master dataset is a whole Delta table (many MinIO objects), not
        one object CatalogService can discover by listing a prefix."""
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT name, updated_at FROM dataset_metadata WHERE layer = %s", (layer,))
                return cur.fetchall()
        finally:
            self._release(conn)

    def delete_metadata(self, layer, name):
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM dataset_metadata WHERE layer = %s AND name = %s", (layer, name))
            conn.commit()
        finally:
            self._release(conn)

    # -- audit log -----------------------------------------------------------

    def log_action(self, user_id, username, action, dataset_name=None, details=None):
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO audit_log (user_id, username, action, dataset_name, details) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (user_id, username, action, dataset_name, json.dumps(details) if details else None),
                )
            conn.commit()
        finally:
            self._release(conn)

    def list_audit_log(self, limit=100):
        conn = self._conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, user_id, username, action, dataset_name, details, created_at "
                    "FROM audit_log ORDER BY created_at DESC LIMIT %s",
                    (limit,),
                )
                return [dict(r) for r in cur.fetchall()]
        finally:
            self._release(conn)

    # -- dataset version history ----------------------------------------------

    def record_version(self, layer, name, row_count, size_bytes, created_by=None, details=None):
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO dataset_versions (layer, name, row_count, size_bytes, created_by, details) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (layer, name, row_count, size_bytes, created_by, json.dumps(details) if details else None),
                )
            conn.commit()
        finally:
            self._release(conn)

    def count_versions(self, layer, name):
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM dataset_versions WHERE layer = %s AND name = %s", (layer, name))
                return cur.fetchone()[0]
        finally:
            self._release(conn)

    def list_versions(self, layer, name, limit=50):
        conn = self._conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT dv.id, dv.row_count, dv.size_bytes, dv.details, dv.created_at, u.username AS created_by_username "
                    "FROM dataset_versions dv LEFT JOIN users u ON u.id = dv.created_by "
                    "WHERE dv.layer = %s AND dv.name = %s ORDER BY dv.created_at DESC LIMIT %s",
                    (layer, name, limit),
                )
                return [dict(r) for r in cur.fetchall()]
        finally:
            self._release(conn)

    # -- Delta small-file compaction state (services/compaction_job.py) --------

    def get_last_compacted(self, format_key):
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT last_compacted_at FROM delta_compaction_log WHERE format_key = %s",
                    (format_key,),
                )
                row = cur.fetchone()
                return row[0] if row else None
        finally:
            self._release(conn)

    def record_compaction(self, format_key, files_before, files_after, bytes_before, bytes_after):
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO delta_compaction_log "
                    "(format_key, last_compacted_at, files_before, files_after, bytes_before, bytes_after, updated_at) "
                    "VALUES (%s, now(), %s, %s, %s, %s, now()) "
                    "ON CONFLICT (format_key) DO UPDATE SET "
                    "last_compacted_at = now(), files_before = EXCLUDED.files_before, "
                    "files_after = EXCLUDED.files_after, bytes_before = EXCLUDED.bytes_before, "
                    "bytes_after = EXCLUDED.bytes_after, updated_at = now()",
                    (format_key, files_before, files_after, bytes_before, bytes_after),
                )
            conn.commit()
        finally:
            self._release(conn)

    def delete_compaction_state(self, format_key):
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM delta_compaction_log WHERE format_key = %s", (format_key,))
            conn.commit()
        finally:
            self._release(conn)

    # -- reporting tables (populated by Spark, read by Tableau / this app) ----

    def upsert_reporting_metric(self, dataset_name, metric_name, metric_value):
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO reporting_metrics (dataset_name, metric_name, metric_value, computed_at) "
                    "VALUES (%s, %s, %s, now()) "
                    "ON CONFLICT (dataset_name, metric_name) "
                    "DO UPDATE SET metric_value = EXCLUDED.metric_value, computed_at = now()",
                    (dataset_name, metric_name, metric_value),
                )
            conn.commit()
        finally:
            self._release(conn)
