"""
PostgreSQL service.

The only module allowed to import psycopg2. Owns: users, dataset metadata
(display name / division override / archived flag), audit log, dataset
version history, Spark's reporting tables, and — since the "no more
division/dataset_type" feature — each dataset's known column schema and the
admin-approval queue for schema changes / renames.
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
        two uploads to the same dataset arriving at the same time.

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
        used as the source of truth for which datasets exist, since a
        dataset is a whole Delta table (many MinIO objects), not one object
        CatalogService can discover by listing a prefix."""
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

    # -- reporting tables (populated by Spark, read by BI tools) ----

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

    # -- dynamic dataset schema (services/dataset_service.py) ------------------
    # Replaces the old fixed division/dataset_type contract: this is the
    # fast lookup used at upload time to find which existing dataset (if
    # any) a new file's columns match, without opening every dataset's
    # Delta table on every single upload.

    def get_dataset_columns(self, dataset_id):
        """Ordered list of this dataset's currently-known columns, or None
        if dataset_id isn't a real dataset yet."""
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT columns FROM dataset_schema WHERE dataset_id = %s", (dataset_id,))
                row = cur.fetchone()
                return row[0] if row else None
        finally:
            self._release(conn)

    def list_all_dataset_schemas(self):
        """{dataset_id: [columns...]} for every known dataset — used to score
        every existing dataset against a freshly-uploaded file's columns in
        one round trip, rather than one query per candidate."""
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT dataset_id, columns FROM dataset_schema")
                return {row[0]: row[1] for row in cur.fetchall()}
        finally:
            self._release(conn)

    def set_dataset_columns(self, dataset_id, columns, key_columns=None):
        """Creates or overwrites a dataset's known column list. Called when
        a brand-new dataset is created (no approval needed) and again when
        a schema_change pending_change is approved (the column list grows
        to include the newly-approved column(s)).

        key_columns is optional and only touched when explicitly passed:
        omitting it (the schema_change-approval call site) leaves whatever
        key was chosen at dataset creation untouched, since growing the
        column list is not the same thing as re-choosing row identity."""
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                if key_columns is not None:
                    cur.execute(
                        "INSERT INTO dataset_schema (dataset_id, columns, key_columns, updated_at) "
                        "VALUES (%s, %s, %s, now()) "
                        "ON CONFLICT (dataset_id) DO UPDATE SET "
                        "columns = EXCLUDED.columns, key_columns = EXCLUDED.key_columns, updated_at = now()",
                        (dataset_id, json.dumps(columns), json.dumps(key_columns)),
                    )
                else:
                    cur.execute(
                        "INSERT INTO dataset_schema (dataset_id, columns, updated_at) "
                        "VALUES (%s, %s, now()) "
                        "ON CONFLICT (dataset_id) DO UPDATE SET columns = EXCLUDED.columns, updated_at = now()",
                        (dataset_id, json.dumps(columns)),
                    )
            conn.commit()
        finally:
            self._release(conn)

    def get_dataset_key_columns(self, dataset_id):
        """The column(s) that identify "the same row" for this dataset, as
        last chosen by set_dataset_columns(..., key_columns=...) — or None
        if the dataset doesn't exist yet, or was created before this
        feature existed and has never had a key chosen."""
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT key_columns FROM dataset_schema WHERE dataset_id = %s", (dataset_id,))
                row = cur.fetchone()
                return row[0] if row and row[0] else None
        finally:
            self._release(conn)

    def delete_dataset_schema(self, dataset_id):
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM dataset_schema WHERE dataset_id = %s", (dataset_id,))
            conn.commit()
        finally:
            self._release(conn)

    # -- pending admin-approval queue (services/dataset_service.py, routes/admin.py) --

    def create_pending_change(self, change_type, dataset_id, requested_by, requested_by_username, payload):
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO pending_changes (change_type, dataset_id, requested_by, requested_by_username, payload) "
                    "VALUES (%s, %s, %s, %s, %s) RETURNING id",
                    (change_type, dataset_id, requested_by, requested_by_username, json.dumps(payload)),
                )
                new_id = cur.fetchone()[0]
            conn.commit()
            return new_id
        finally:
            self._release(conn)

    def get_pending_change(self, pending_id):
        conn = self._conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM pending_changes WHERE id = %s", (pending_id,))
                row = cur.fetchone()
                return dict(row) if row else None
        finally:
            self._release(conn)

    def list_pending_changes(self, status="pending", limit=100):
        conn = self._conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if status:
                    cur.execute(
                        "SELECT * FROM pending_changes WHERE status = %s ORDER BY created_at DESC LIMIT %s",
                        (status, limit),
                    )
                else:
                    cur.execute("SELECT * FROM pending_changes ORDER BY created_at DESC LIMIT %s", (limit,))
                return [dict(r) for r in cur.fetchall()]
        finally:
            self._release(conn)

    def resolve_pending_change(self, pending_id, status, reviewed_by):
        """status: 'approved' or 'rejected'. Only actually applying the
        change (writing the staged data, renaming the dataset) is the
        caller's job — this just marks the request itself resolved so it
        drops out of the pending queue and can't be actioned twice."""
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE pending_changes SET status = %s, reviewed_by = %s, reviewed_at = now() "
                    "WHERE id = %s AND status = 'pending' RETURNING id",
                    (status, reviewed_by, pending_id),
                )
                row = cur.fetchone()
            conn.commit()
            return row is not None  # False means it was already resolved by someone else
        finally:
            self._release(conn)
