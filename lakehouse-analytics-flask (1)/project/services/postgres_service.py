"""
PostgreSQL service.

The only module allowed to import psycopg2. Owns: users, dataset metadata
(display name / division override / archived flag), audit log, dataset
version history, and Spark's reporting tables.
"""
import json

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
                    "SELECT display_name, owner, division_override, archived "
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
