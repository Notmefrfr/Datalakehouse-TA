"""
Read-only SQL sandbox for the AI assistant.

The model writes SQL; this module is the ONLY thing that ever runs it. Every
dataset is a Delta table read into pandas, so there is no SQL engine in the
stack today — DuckDB (in-process, no server) is used to give each dataset a
queryable table name equal to its dataset_id.

Safety model (defence in depth — any one layer failing must not be enough):
  1. Exactly one statement, and DuckDB's own parser must classify it as a
     SELECT (which includes WITH ... SELECT). No INSERT/UPDATE/DDL/COPY/
     ATTACH/PRAGMA/SET/INSTALL ever reaches execution.
  2. A fresh in-memory connection per query with enable_external_access=false
     (no read_csv('/etc/passwd'), no httpfs, no file writes) and
     lock_configuration=true (the query can't turn that back on).
  3. Only datasets the query actually references are loaded, and they are
     registered as in-memory views — the Delta tables are never touched for
     writing; this module has no write path to storage at all.
  4. Hard wall-clock timeout (con.interrupt()), memory limit, and a row cap.
"""
import re
import threading
import time

import duckdb

MAX_SQL_CHARS = 8000
_QUERY_START_RE = re.compile(r"^\(*\s*(select|with|from|values)\b", re.IGNORECASE)
_LEADING_COMMENT_RE = re.compile(r"^(\s+|--[^\n]*(\n|$)|/\*.*?\*/)+", re.DOTALL)


def _strip_leading_comments(sql):
    return _LEADING_COMMENT_RE.sub("", sql, count=1)


class SqlSandboxError(Exception):
    """A problem with the SQL itself — safe to show to the model so it can retry."""


def _quote(identifier):
    return '"' + identifier.replace('"', '""') + '"'


def referenced_datasets(sql, dataset_ids):
    """Which known dataset_ids are mentioned in the SQL. Deliberately a
    superset check (whole-word, case-insensitive): loading a dataset that turns
    out not to be used is only wasted work, while missing one would fail the
    query. dataset_ids are slugs ([a-z0-9_-]), so a lookaround on the
    identifier characters is an exact whole-word test."""
    found = []
    for ds in dataset_ids:
        if re.search(r"(?<![A-Za-z0-9_-])" + re.escape(ds) + r"(?![A-Za-z0-9_-])", sql, re.IGNORECASE):
            found.append(ds)
    return found


def validate_select(sql):
    """Raises SqlSandboxError unless `sql` is exactly one SELECT statement."""
    if not isinstance(sql, str) or not sql.strip():
        raise SqlSandboxError("SQL is empty.")
    if len(sql) > MAX_SQL_CHARS:
        raise SqlSandboxError(f"SQL is too long (max {MAX_SQL_CHARS} characters).")
    probe = duckdb.connect(":memory:")
    try:
        try:
            statements = probe.extract_statements(sql)
        except duckdb.Error as e:
            raise SqlSandboxError(f"SQL syntax error: {e}") from None
        if len(statements) != 1:
            raise SqlSandboxError("Only a single SQL statement is allowed.")
        if statements[0].type != duckdb.StatementType.SELECT:
            raise SqlSandboxError("Only read-only SELECT queries are allowed.")
        # DuckDB classifies a few non-SELECT commands (e.g. PRAGMA ...) as SELECT
        # internally, so also require the statement to *start* like a query.
        if not _QUERY_START_RE.match(_strip_leading_comments(sql)):
            raise SqlSandboxError("Only read-only SELECT queries are allowed.")
    finally:
        probe.close()


def _locked_down_connection(frames, memory_limit):
    """A fresh in-memory DuckDB connection with every dataset in `frames`
    ({dataset_id: DataFrame-or-lazy-pyarrow-Dataset}) registered as a view, then
    locked down (see module docstring). DuckDB's register() accepts either a
    pandas DataFrame or a pyarrow Dataset/Table transparently — a lazy pyarrow
    Dataset (services/delta_service.py's read_dataset_arrow) is never read here;
    DuckDB only scans the parts of it the actual query touches, with column and
    predicate pushdown, which is what lets run_query answer an aggregate query
    over a dataset far bigger than RAM. Caller must con.close() when done.
    """
    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute("SET threads=2")
    for i, (ds, source) in enumerate(frames.items()):
        con.register(f"__df_{i}", source)
        con.execute(f"CREATE VIEW {_quote(ds)} AS SELECT * FROM __df_{i}")
    # Order matters: lock down AFTER the views exist, and lock the config so
    # the query itself can't undo it.
    con.execute("SET enable_external_access=false")
    con.execute("SET lock_configuration=true")
    return con


def _run_with_timeout(con, run, timeout_seconds):
    """Runs `run(con)` under DuckDB's own cooperative cancellation
    (con.interrupt()) — unlike a generic thread timeout, this actually stops
    DuckDB's query execution rather than just abandoning the wait for it."""
    timer = None
    timed_out = threading.Event()
    try:
        def _on_timeout():
            timed_out.set()
            con.interrupt()

        timer = threading.Timer(timeout_seconds, _on_timeout)
        timer.start()
        try:
            return run(con)
        except duckdb.Error as e:
            if timed_out.is_set():
                raise SqlSandboxError(
                    f"Query exceeded the {timeout_seconds}s time limit. Add filters or aggregate more."
                ) from None
            raise SqlSandboxError(_clean_error(e)) from None
    finally:
        if timer:
            timer.cancel()


def run_query(sql, frames, max_rows=500, timeout_seconds=20, memory_limit="1GB"):
    """
    Execute one read-only SELECT over `frames` ({dataset_id: DataFrame-or-lazy-
    pyarrow-Dataset}).

    Returns {"columns": [...], "rows": [[...], ...], "row_count": int,
             "truncated": bool, "elapsed_ms": int}. Values are JSON-safe (see
    _jsonable) — this is the shape sent both to the browser and, compacted, to
    the model, so it deliberately throws away exact dtypes. For a full pandas
    DataFrame with real dtypes preserved, see load_sample_dataframe() below.
    Raises SqlSandboxError with a model-readable message on any failure.
    """
    validate_select(sql)
    con = _locked_down_connection(frames, memory_limit)
    try:
        started = time.monotonic()

        def _exec(con):
            cur = con.execute(sql)
            columns = [d[0] for d in cur.description]
            fetched = cur.fetchmany(max_rows + 1)
            return columns, fetched

        columns, fetched = _run_with_timeout(con, _exec, timeout_seconds)
        elapsed_ms = int((time.monotonic() - started) * 1000)
    finally:
        con.close()

    truncated = len(fetched) > max_rows
    rows = [[_jsonable(v) for v in row] for row in fetched[:max_rows]]
    return {
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
        "truncated": truncated,
        "elapsed_ms": elapsed_ms,
    }


def load_sample_dataframe(dataset_id, source, cap, total_rows, timeout_seconds=20, memory_limit="1GB"):
    """
    Returns (DataFrame, sampled: bool) — up to `cap` rows of `source` (a lazy
    pyarrow Dataset or a DataFrame) as a REAL pandas DataFrame with proper
    dtypes preserved (via DuckDB's native .df(), not the JSON-sanitized shape
    run_query returns). This is how services/chat_agent.py feeds every tool
    *except* run_query: rather than ever materializing a big dataset's full
    table, it pulls a bounded, randomly-sampled slice through DuckDB so a
    huge dataset can't blow out the worker's memory. When total_rows <= cap
    this reads the whole (small) table and sampled is False — callers should
    disclose sampled=True results as approximate, never as exact totals.
    DuckDB's SAMPLE clause is itself randomized per call (no fixed seed), so
    repeated calls over the same request are cached by the caller rather than
    re-sampled.
    """
    sampled = total_rows > cap
    sql = f"SELECT * FROM {_quote(dataset_id)}" + (f" USING SAMPLE {cap} ROWS" if sampled else "")
    con = _locked_down_connection({dataset_id: source}, memory_limit)
    try:
        df = _run_with_timeout(con, lambda con: con.execute(sql).df(), timeout_seconds)
    finally:
        con.close()
    return df, sampled


def _clean_error(exc):
    # DuckDB errors are already readable ("Binder Error: Referenced column ... not found
    # ... Candidate bindings: ...") and that hint is exactly what lets the model self-correct.
    return str(exc).strip()[:700]


def _jsonable(v):
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, float):
        return None if v != v or v in (float("inf"), float("-inf")) else v
    return str(v)  # dates, decimals, timestamps, etc.
