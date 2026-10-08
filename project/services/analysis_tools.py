"""
The assistant's toolbox: 20 read-only tools in four groups.

    Data           list_datasets  get_schema  get_sample  get_statistics  run_query
    Visualization  bar_chart  line_chart  scatter_plot  heatmap  histogram  boxplot
    Analysis       correlation  outlier_detection  missing_value_analysis  summary_statistics
    Machine learning  decision_tree  KNN  linear_regression  logistic_regression  KMeans

Design rule (the whole point of this module): the LLM never sees a dataset.
Every tool computes on the full data inside this process (DuckDB / pandas /
scikit-learn) and returns only a small result — a handful of aggregates,
metrics or plot points. Two consumers get two different views of that result:

    "model"   a compact dict that goes back to Qwen (a few hundred tokens), so
              it can interpret the numbers without blowing its context window
    "events"  richer payloads streamed to the browser (tables, chart data)

Every handler returns {"model": dict, "events": [dict, ...]}. Problems with the
request (unknown column, wrong type, too few rows ...) raise SqlSandboxError
with a message written for the model, so it can correct itself and retry.
Nothing here can write to storage: frames are read-only inputs.
"""
import json
import math
import re

import numpy as np
import pandas as pd

from services.sql_sandbox import SqlSandboxError, referenced_datasets, run_query as sandbox_run_query

# ------------------------------------------------------------------ limits
MAX_CLASSES = 20            # a classification target may have at most this many distinct values
REGRESSION_MIN_UNIQUE = 16  # numeric target with more distinct values than this => regression
MAX_ONEHOT_UNIQUE = 20      # text feature with more distinct values is skipped (ID-like / free text)
MAX_CATEGORICAL_FEATURES = 10
MAX_FEATURES = 120          # after one-hot
MAX_HEATMAP = 12            # heatmap axes are capped at this many labels
SEED = 42


# ------------------------------------------------------------------ helpers
_DS = {"type": "string"}
_COLS = {"type": "array", "items": {"type": "string"}}


def _py(v):
    """numpy/pandas scalars -> plain JSON-safe Python (NaN/inf -> None)."""
    if v is None or isinstance(v, (bool, str)):
        return v
    if isinstance(v, (np.bool_,)):
        return bool(v)
    if isinstance(v, (int, np.integer)):
        return int(v)
    if isinstance(v, (float, np.floating)):
        f = float(v)
        return None if math.isnan(f) or math.isinf(f) else f
    if isinstance(v, dict):
        return {str(k): _py(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, np.ndarray)):
        return [_py(x) for x in v]
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return str(v)


def _r(v, nd=4):
    v = _py(v)
    if isinstance(v, float):
        return round(v, nd)
    return v


def _shorten(value, limit=60):
    s = "" if value is None else str(value)
    return s if len(s) <= limit else s[: limit - 1] + "…"


def _sql_type(series):
    dt = series.dtype
    if pd.api.types.is_bool_dtype(dt):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(dt):
        return "BIGINT"
    if pd.api.types.is_float_dtype(dt):
        return "DOUBLE"
    if pd.api.types.is_datetime64_any_dtype(dt):
        return "TIMESTAMP"
    return "VARCHAR"


def _as_numeric(series):
    """Numeric view of a column, or None. Text columns whose values are >=90%
    numeric-looking (the lakehouse often stores numbers as VARCHAR) are coerced."""
    if pd.api.types.is_bool_dtype(series.dtype):
        return None
    if pd.api.types.is_numeric_dtype(series.dtype):
        return series.astype(float)
    nonnull = series.dropna()
    if nonnull.empty or pd.api.types.is_datetime64_any_dtype(series.dtype):
        return None
    coerced = pd.to_numeric(nonnull.head(1000), errors="coerce")
    if coerced.notna().mean() >= 0.9:
        return pd.to_numeric(series, errors="coerce")
    return None


def _as_datetime(series):
    if pd.api.types.is_datetime64_any_dtype(series.dtype):
        return series
    if pd.api.types.is_numeric_dtype(series.dtype) or pd.api.types.is_bool_dtype(series.dtype):
        return None
    nonnull = series.dropna()
    if nonnull.empty:
        return None
    sample = nonnull.head(500).astype(str)
    if pd.to_datetime(sample, errors="coerce", format="mixed").notna().mean() >= 0.9:
        return pd.to_datetime(series, errors="coerce", format="mixed")
    return None


def _col(df, name):
    """Exact column match, then case-insensitive; otherwise a helpful error."""
    name = str(name or "").strip()
    if name in df.columns:
        return name
    lowered = {str(c).lower(): c for c in df.columns}
    if name.lower() in lowered:
        return lowered[name.lower()]
    raise SqlSandboxError(f"Unknown column '{name}'. Columns: {', '.join(map(str, list(df.columns)[:60]))}.")


def _num(df, name):
    c = _col(df, name)
    s = _as_numeric(df[c])
    if s is None:
        raise SqlSandboxError(f"Column '{c}' is not numeric (type {_sql_type(df[c])}).")
    return c, s


def _is_id_like(name, s):
    """An all-unique column named id / *_id: a row label, not a measurement."""
    return bool(re.search(r"(^|_)id$", str(name), re.I)) and s.nunique(dropna=True) == s.notna().sum()


def _numeric_columns(df, limit=None):
    """Numeric columns worth including when the user didn't name any (no constants, no row IDs)."""
    out = []
    for c in df.columns:
        s = _as_numeric(df[c])
        if s is not None and s.notna().sum() >= 2 and s.nunique(dropna=True) > 1 and not _is_id_like(c, s):
            out.append(c)
    return out[:limit] if limit else out


def _int(args, key, default, lo, hi):
    try:
        v = int(args.get(key, default))
    except (TypeError, ValueError):
        v = default
    return max(lo, min(hi, v))


def _float(args, key, default, lo, hi):
    try:
        v = float(args.get(key, default))
    except (TypeError, ValueError):
        v = default
    return max(lo, min(hi, v))


def _cols_arg(df, args, key="columns"):
    raw = args.get(key)
    if raw is None or raw == "" or raw == []:
        return None
    if isinstance(raw, str):
        raw = [p for p in re.split(r"\s*,\s*", raw) if p]
    if not isinstance(raw, list):
        raise SqlSandboxError(f"'{key}' must be a list of column names.")
    return [_col(df, c) for c in raw][:60]


def _table(title, columns, rows):
    return {"title": title, "columns": list(columns), "rows": [[_py(v) for v in r] for r in rows]}


def _analysis(name, title, summary=None, tables=None, text=None):
    ev = {"type": "analysis", "name": name, "title": title,
          "summary": _py(summary or {}), "tables": tables or []}
    if text:
        ev["text"] = text
    return ev


# ------------------------------------------------------------------ the toolbox
class AnalysisToolkit:
    """One instance per chat request. `frame(ds)` returns a (cached) DataFrame."""

    def __init__(self, frame, entries, schemas, config, arrow_frame=None, true_row_count=None):
        self._frame = frame
        self._entries = entries      # callable -> catalog entries
        self._schemas = schemas      # callable -> {dataset_id: [columns]}
        self.cfg = config
        # run_query reads through this instead of self._frame: a lazy pyarrow Dataset
        # (no row data loaded yet), not a capped sample, so it gets true full-table
        # SQL semantics even on a dataset far bigger than RAM. Falls back to
        # self._frame for callers (e.g. older tests) that don't pass it.
        self._arrow_frame = arrow_frame or frame
        # Exact row count from metadata, no data read — used to disclose ML sample
        # sizes accurately even though self._frame itself may already be a capped
        # sample (so "trained on X of Y rows" always reports the TRUE Y).
        self._true_rows = true_row_count or (lambda ds: len(frame(ds)))
        self.last_query = None       # most recent run_query result (bar_chart/line_chart can reuse it)
        self.handlers = {
            "list_datasets": self.list_datasets, "get_schema": self.get_schema,
            "get_sample": self.get_sample, "get_statistics": self.get_statistics,
            "run_query": self.run_query,
            "bar_chart": self.bar_chart, "line_chart": self.line_chart,
            "scatter_plot": self.scatter_plot, "heatmap": self.heatmap,
            "histogram": self.histogram, "boxplot": self.boxplot,
            "correlation": self.correlation, "outlier_detection": self.outlier_detection,
            "missing_value_analysis": self.missing_value_analysis,
            "summary_statistics": self.summary_statistics,
            "decision_tree": self.decision_tree, "KNN": self.knn,
            "linear_regression": self.linear_regression,
            "logistic_regression": self.logistic_regression, "KMeans": self.kmeans,
        }

    # ---- plumbing ---------------------------------------------------------
    CHART_TOOLS = frozenset({"bar_chart", "line_chart", "scatter_plot", "heatmap", "histogram", "boxplot"})

    def known_ids(self):
        return [e["dataset_id"] for e in self._entries()]

    def _df(self, args):
        ds = str(args.get("dataset_id", "")).strip()
        if ds not in self.known_ids():
            raise SqlSandboxError(f"Unknown dataset '{ds}'. Known: {', '.join(self.known_ids()) or 'none'}.")
        return ds, self._frame(ds)

    def call(self, name, args):
        if name not in self.handlers:
            raise SqlSandboxError(f"Unknown tool '{name}'. Available: {', '.join(self.handlers)}.")
        if not isinstance(args, dict):
            args = {}
        return self.handlers[name](args)

    def audit_info(self, name, args, result):
        """(detail, row_count, dataset_ids, action) for the audit log."""
        if name == "run_query":
            ids = referenced_datasets(str(args.get("sql", "")), self.known_ids())
            return str(args.get("sql", "")), result["model"].get("rows_returned", 0), ids, "ai_sql"
        ds = str(args.get("dataset_id", "") or "")
        return f"{name} {json.dumps(args, default=str)[:500]}", 0, [ds] if ds else [], "ai_tool"

    @staticmethod
    def schemas():
        return TOOL_SCHEMAS

    # =====================================================================
    # 1. DATA TOOLS
    # =====================================================================
    def list_datasets(self, _args):
        schemas = self._schemas()
        ds = [{"dataset_id": e["dataset_id"], "name": e.get("display_name"), "rows": e.get("rows"),
               "columns": schemas.get(e["dataset_id"], [])} for e in self._entries()]
        return {"model": {"datasets": ds}, "events": []}

    def get_schema(self, args):
        ds, df = self._df(args)
        cols = []
        for name in list(df.columns)[:80]:
            s = df[name]
            info = {"name": name, "type": _sql_type(s), "nulls": int(s.isna().sum())}
            nonnull = s.dropna()
            if info["type"] == "VARCHAR" and len(nonnull):
                sample = nonnull.head(500)
                if pd.to_numeric(sample, errors="coerce").notna().mean() > 0.9:
                    info["note"] = "values look numeric — use TRY_CAST(col AS DOUBLE)"
                elif pd.to_datetime(sample, errors="coerce", format="mixed").notna().mean() > 0.9:
                    info["note"] = "values look like dates — use TRY_CAST(col AS DATE)"
                if nonnull.nunique() <= 15:
                    info["values"] = [_shorten(v, 40) for v in nonnull.unique()[:15]]
            cols.append(info)
        # True total, not len(df) — df here may already be a capped sample (see
        # ChatAgent._frame), and "how many rows" is basic metadata that should
        # always be exact; it costs nothing extra to get right.
        return {"model": {"dataset_id": ds, "rows": self._true_rows(ds), "columns": cols,
                          "truncated_columns": len(df.columns) > 80}, "events": []}

    def get_sample(self, args):
        ds, df = self._df(args)
        n = _int(args, "n", 5, 1, 20)
        cols = _cols_arg(df, args) or list(df.columns)[:30]
        part = df[cols]
        part = part.sample(min(n, len(part)), random_state=SEED) if args.get("random") else part.head(n)
        rows = [[_shorten(v, 80) if v is not None and v == v else None for v in r]
                for r in part.astype(object).where(part.notna(), None).values.tolist()]
        ev = {"type": "sql_result", "sql": f"-- sample of {ds}", "columns": [str(c) for c in cols],
              "rows": rows, "row_count": len(rows), "truncated": False, "elapsed_ms": 0}
        return {"model": {"dataset_id": ds, "columns": [str(c) for c in cols], "rows": rows}, "events": [ev]}

    def get_statistics(self, args):
        ds, df = self._df(args)
        cols = _cols_arg(df, args) or list(df.columns)[:40]
        profile, table = [], []
        for c in cols:
            s = df[c]
            info = {"name": c, "type": _sql_type(s), "nulls": int(s.isna().sum()),
                    "unique": int(s.nunique(dropna=True))}
            n = _as_numeric(s)
            if n is not None and n.notna().any():
                info.update(min=_r(n.min()), max=_r(n.max()), mean=_r(n.mean()),
                            median=_r(n.median()), std=_r(n.std()))
                if info["type"] == "VARCHAR":
                    info["note"] = "numeric stored as text"
                table.append([c, info["type"], info["nulls"], info["unique"], info["min"], info["max"],
                              info["mean"], info["median"], info["std"], ""])
            else:
                top = s.dropna().astype(str).value_counts().head(3)
                info["top"] = {_shorten(k, 30): int(v) for k, v in top.items()}
                table.append([c, info["type"], info["nulls"], info["unique"], None, None, None, None, None,
                              ", ".join(f"{k} ({v})" for k, v in info["top"].items())])
            profile.append(info)
        dupes = int(df.duplicated().sum()) if len(df) <= 2_000_000 else None
        summary = {"rows": int(len(df)), "columns": int(df.shape[1]), "duplicate_rows": dupes}
        ev = _analysis("get_statistics", f"Profile of {ds}", summary, [_table(
            "Columns", ["column", "type", "nulls", "unique", "min", "max", "mean", "median", "std", "top values"], table)])
        return {"model": {"dataset_id": ds, **summary, "columns": profile}, "events": [ev]}

    def run_query(self, args):
        sql = str(args.get("sql", "")).strip().rstrip(";")
        ids = referenced_datasets(sql, self.known_ids())
        # Lazy pyarrow Datasets, not materialized DataFrames: DuckDB streams through
        # the underlying Parquet files with column/predicate pushdown, so this stays
        # correct and bounded-memory even when a dataset is far bigger than RAM.
        frames = {ds: self._arrow_frame(ds) for ds in ids}
        result = sandbox_run_query(sql, frames, max_rows=self.cfg.CHAT_MAX_ROWS,
                                   timeout_seconds=self.cfg.CHAT_SQL_TIMEOUT_SECONDS)
        self.last_query = result
        rows = result["rows"][:30]
        model = {"columns": result["columns"], "rows": rows, "rows_returned": result["row_count"],
                 "note": ("Only the first 30 rows are shown to you; " if result["row_count"] > 30 else "")
                         + ("the result was truncated at the row cap." if result["truncated"] else "")}
        return {"model": model, "events": [{"type": "sql_result", "sql": sql, **result}]}

    # =====================================================================
    # 2. VISUALIZATION TOOLS
    # =====================================================================
    @staticmethod
    def _agg(grouped_values, how):
        return {"sum": grouped_values.sum, "avg": grouped_values.mean, "mean": grouped_values.mean,
                "min": grouped_values.min, "max": grouped_values.max, "median": grouped_values.median,
                "count": grouped_values.count}[how]()

    def _series_for_chart(self, args, line):
        """(labels, values, x, y, dataset_label) — aggregated from a dataset, or taken
        straight from the last run_query result when no dataset_id is given."""
        x_name, y_name = args.get("x"), args.get("y")
        if not args.get("dataset_id"):
            res = self.last_query
            if not res:
                raise SqlSandboxError("Pass dataset_id (with x, y, agg), or call run_query first to chart its result.")
            cols = res["columns"]
            if x_name not in cols or y_name not in cols:
                raise SqlSandboxError(f"x and y must be columns of the last query result: {', '.join(cols)}.")
            xi, yi = cols.index(x_name), cols.index(y_name)
            labels, values = [], []
            for row in res["rows"][:200]:
                try:
                    v = float(row[yi])
                except (TypeError, ValueError):
                    continue
                labels.append(_shorten(row[xi], 40))
                values.append(v)
            if not values:
                raise SqlSandboxError(f"Column '{y_name}' has no numeric values to chart.")
            return labels, values, x_name, y_name

        ds, df = self._df(args)
        x = _col(df, x_name)
        agg = str(args.get("agg") or ("sum" if y_name else "count")).lower()
        if agg not in ("sum", "avg", "mean", "min", "max", "median", "count"):
            raise SqlSandboxError("agg must be one of sum, avg, min, max, median, count.")
        if agg == "count" or not y_name:
            work = pd.DataFrame({"x": df[x], "y": 1.0})
            agg, y_label = "count", "count"
        else:
            y, ys = _num(df, y_name)
            work = pd.DataFrame({"x": df[x], "y": ys})
            y_label = f"{agg}({y})"
        key = work["x"]
        if line:
            dt = _as_datetime(key)
            if dt is not None:
                period = str(args.get("period") or "").lower()
                if period not in ("day", "month", "year"):
                    period = "day" if dt.dt.normalize().nunique() <= 120 else "month"
                freq = {"day": "D", "month": "M", "year": "Y"}[period]
                key = dt.dt.to_period(freq).astype(str).where(dt.notna())
        work["k"] = key
        work = work[work["k"].notna()]
        grouped = work.groupby("k")["y"]
        out = self._agg(grouped, agg)
        if line:
            sorted_idx = sorted(out.index, key=lambda v: (0, float(v)) if _is_number(v) else (1, str(v)))
            out = out.loc[sorted_idx].tail(200)
        else:
            out = out.sort_values(ascending=str(args.get("sort", "desc")).lower() == "asc").head(
                _int(args, "top_n", 15, 1, 30))
        return [_shorten(i, 40) for i in out.index], [float(v) for v in out.values], x, y_label

    def _chart_result(self, tool, chart_type, args, line):
        labels, values, x, y = self._series_for_chart(args, line)
        title = _shorten(args.get("title") or f"{y} by {x}", 90)
        ev = {"type": "chart", "chart_type": chart_type, "title": title, "x": x, "y": y,
              "labels": labels, "values": values}
        shown = list(zip(labels, [_r(v) for v in values]))[:20]
        return {"model": {"ok": True, "chart": f"{chart_type} chart displayed to the user", "x": x, "y": y,
                          "points": len(values), "data": shown}, "events": [ev]}

    def bar_chart(self, args):
        ctype = args.get("chart_type") if args.get("chart_type") in ("bar", "pie") else "bar"
        return self._chart_result("bar_chart", ctype, args, line=False)

    def line_chart(self, args):
        return self._chart_result("line_chart", "line", args, line=True)

    def scatter_plot(self, args):
        ds, df = self._df(args)
        (xc, xs), (yc, ys) = _num(df, args.get("x")), _num(df, args.get("y"))
        pair = pd.DataFrame({"x": xs, "y": ys}).dropna()
        if len(pair) < 3:
            raise SqlSandboxError("Fewer than 3 rows have both values — nothing to plot.")
        r = pair["x"].corr(pair["y"])
        max_pts = _int(args, "max_points", getattr(self.cfg, "CHART_MAX_POINTS", 500), 50, 2000)
        shown = pair.sample(max_pts, random_state=SEED) if len(pair) > max_pts else pair
        ev = {"type": "chart", "chart_type": "scatter", "title": _shorten(args.get("title") or f"{yc} vs {xc}", 90),
              "x": xc, "y": yc, "points": [{"x": _r(a), "y": _r(b)} for a, b in shown.values],
              "r": _r(r), "n": int(len(pair))}
        return {"model": {"ok": True, "chart": "scatter plot displayed to the user", "x": xc, "y": yc,
                          "pearson_r": _r(r, 3), "rows_used": int(len(pair)), "points_plotted": int(len(shown))},
                "events": [ev]}

    def histogram(self, args):
        ds, df = self._df(args)
        c, s = _num(df, args.get("column"))
        s = s.dropna()
        if s.empty:
            raise SqlSandboxError(f"Column '{c}' has no numeric values.")
        bins = _int(args, "bins", 10, 2, 50)
        counts, edges = np.histogram(s.values, bins=bins)
        labels = [f"{_fmt(edges[i])}–{_fmt(edges[i + 1])}" for i in range(len(counts))]
        ev = {"type": "chart", "chart_type": "histogram", "title": _shorten(args.get("title") or f"Distribution of {c}", 90),
              "x": c, "y": "count", "labels": labels, "values": [int(v) for v in counts]}
        model = {"ok": True, "chart": "histogram displayed to the user", "column": c, "n": int(len(s)),
                 "min": _r(s.min()), "max": _r(s.max()), "mean": _r(s.mean()), "median": _r(s.median()),
                 "skew": _r(s.skew(), 3), "bins": [[l, int(v)] for l, v in zip(labels, counts)]}
        return {"model": model, "events": [ev]}

    @staticmethod
    def _box_stats(label, s):
        s = s.dropna()
        q1, med, q3 = s.quantile([0.25, 0.5, 0.75])
        iqr = q3 - q1
        lo_fence, hi_fence = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        inside = s[(s >= lo_fence) & (s <= hi_fence)]
        return {"label": _shorten(label, 30), "n": int(len(s)), "low": _r(inside.min()), "q1": _r(q1),
                "median": _r(med), "q3": _r(q3), "high": _r(inside.max()),
                "outliers": int(len(s) - len(inside)), "min": _r(s.min()), "max": _r(s.max())}

    def boxplot(self, args):
        ds, df = self._df(args)
        c, s = _num(df, args.get("column"))
        group_by = args.get("group_by")
        groups = []
        if group_by:
            g = _col(df, group_by)
            work = pd.DataFrame({"g": df[g].astype(str), "v": s}).dropna(subset=["v"])
            top = work["g"].value_counts().head(_int(args, "top_groups", 8, 1, 12)).index
            for name in top:
                groups.append(self._box_stats(name, work.loc[work["g"] == name, "v"]))
        else:
            if s.notna().sum() == 0:
                raise SqlSandboxError(f"Column '{c}' has no numeric values.")
            groups.append(self._box_stats(c, s))
        ev = {"type": "chart", "chart_type": "boxplot", "title": _shorten(args.get("title") or f"{c} distribution", 90),
              "x": group_by or c, "y": c, "groups": groups}
        return {"model": {"ok": True, "chart": "box plot displayed to the user", "column": c,
                          "group_by": group_by, "groups": groups}, "events": [ev]}

    def heatmap(self, args):
        ds, df = self._df(args)
        kind = str(args.get("kind") or "correlation").lower()
        if kind == "correlation":
            cols = _cols_arg(df, args)
            cols = [c for c in (cols or _numeric_columns(df)) if _as_numeric(df[c]) is not None][:MAX_HEATMAP]
            if len(cols) < 2:
                raise SqlSandboxError("Need at least two numeric columns for a correlation heatmap.")
            m = pd.DataFrame({c: _as_numeric(df[c]) for c in cols}).corr(method="pearson")
            matrix = [[_r(m.loc[a, b], 2) for b in cols] for a in cols]
            ev = {"type": "chart", "chart_type": "heatmap", "title": _shorten(args.get("title") or "Correlation heatmap", 90),
                  "x_labels": [_shorten(c, 18) for c in cols], "y_labels": [_shorten(c, 18) for c in cols],
                  "matrix": matrix, "diverging": True, "value_label": "Pearson r"}
            pairs = _top_pairs(m, 8)
            return {"model": {"ok": True, "chart": "correlation heatmap displayed to the user",
                              "columns": cols, "strongest_pairs": pairs}, "events": [ev]}
        if kind not in ("pivot", "crosstab"):
            raise SqlSandboxError("kind must be 'correlation' or 'pivot'.")
        x, y = _col(df, args.get("x")), _col(df, args.get("y"))
        if x == y:
            raise SqlSandboxError("x and y must be different columns.")
        agg = str(args.get("agg") or ("sum" if args.get("value") else "count")).lower()
        if agg not in ("sum", "avg", "mean", "min", "max", "median", "count"):
            raise SqlSandboxError("agg must be one of sum, avg, min, max, median, count.")
        work = pd.DataFrame({"x": df[x].astype(str), "y": df[y].astype(str)})[df[x].notna() & df[y].notna()]
        if agg == "count" or not args.get("value"):
            work["v"] = 1.0
            agg, vlabel = "count", "count"
        else:
            vc, vs = _num(df, args.get("value"))
            work["v"] = vs
            vlabel = f"{agg}({vc})"
        xs = work["x"].value_counts().head(MAX_HEATMAP).index
        ys = work["y"].value_counts().head(MAX_HEATMAP).index
        work = work[work["x"].isin(xs) & work["y"].isin(ys)]
        piv = work.pivot_table(index="y", columns="x", values="v", aggfunc={"avg": "mean"}.get(agg, agg))
        piv = piv.reindex(index=list(ys), columns=list(xs))
        matrix = [[_r(piv.loc[b, a], 2) for a in xs] for b in ys]
        ev = {"type": "chart", "chart_type": "heatmap", "title": _shorten(args.get("title") or f"{vlabel}: {y} × {x}", 90),
              "x_labels": [_shorten(a, 18) for a in xs], "y_labels": [_shorten(b, 18) for b in ys],
              "matrix": matrix, "diverging": False, "value_label": vlabel}
        flat = sorted(((m, ys[i], xs[j]) for i, row in enumerate(matrix) for j, m in enumerate(row) if m is not None),
                      key=lambda t: -t[0])[:8]
        return {"model": {"ok": True, "chart": "heatmap displayed to the user", "value": vlabel,
                          "x": x, "y": y, "cells_shown": len(xs) * len(ys),
                          "highest_cells": [{"y": _shorten(b, 25), "x": _shorten(a, 25), "value": m} for m, b, a in flat]},
                "events": [ev]}

    # =====================================================================
    # 3. ANALYSIS TOOLS
    # =====================================================================
    def correlation(self, args):
        ds, df = self._df(args)
        method = str(args.get("method") or "pearson").lower()
        if method not in ("pearson", "spearman"):
            raise SqlSandboxError("method must be pearson or spearman.")
        cols = _cols_arg(df, args)
        cols = [c for c in (cols or _numeric_columns(df, 25)) if _as_numeric(df[c]) is not None]
        if len(cols) < 2:
            raise SqlSandboxError("Need at least two numeric columns.")
        num = pd.DataFrame({c: _as_numeric(df[c]) for c in cols})
        m = num.corr(method=method)
        tables, model = [], {"dataset_id": ds, "method": method, "columns_used": len(cols)}
        if args.get("target"):
            t = _col(df, args["target"])
            if t not in m.columns:
                raise SqlSandboxError(f"Target '{t}' is not numeric.")
            s = m[t].drop(t).dropna()
            s = s.reindex(s.abs().sort_values(ascending=False).index).head(12)
            model["with_target"] = {"target": t, "top": {k: _r(v, 3) for k, v in s.items()}}
            tables.append(_table(f"Correlation with {t}", ["column", "r"], [[k, _r(v, 3)] for k, v in s.items()]))
        pairs = _top_pairs(m, 10)
        model["strongest_pairs"] = pairs
        tables.append(_table("Strongest pairs", ["column A", "column B", "r"], [[p["a"], p["b"], p["r"]] for p in pairs]))
        model["note"] = "Correlation is not causation; rows with missing values are dropped pairwise."
        return {"model": model, "events": [_analysis("correlation", f"{method.title()} correlation in {ds}",
                                                      {"columns": len(cols)}, tables)]}

    def outlier_detection(self, args):
        ds, df = self._df(args)
        method = str(args.get("method") or "iqr").lower()
        if method not in ("iqr", "zscore"):
            raise SqlSandboxError("method must be iqr or zscore.")
        k = _float(args, "threshold", 1.5 if method == "iqr" else 3.0, 0.5, 10)
        cols = [_col(df, args["column"])] if args.get("column") else _numeric_columns(df, 15)
        results, rows = [], []
        for c in cols:
            s = _as_numeric(df[c])
            if s is None:
                raise SqlSandboxError(f"Column '{c}' is not numeric.")
            s = s.dropna()
            if len(s) < 4:
                continue
            if method == "iqr":
                q1, q3 = s.quantile(0.25), s.quantile(0.75)
                lo, hi = q1 - k * (q3 - q1), q3 + k * (q3 - q1)
            else:
                mu, sd = s.mean(), s.std()
                if not sd:
                    continue
                lo, hi = mu - k * sd, mu + k * sd
            out = s[(s < lo) | (s > hi)]
            extremes = out.reindex(out.sub(s.median()).abs().sort_values(ascending=False).index).head(5)
            info = {"column": c, "outliers": int(len(out)), "pct": _r(100 * len(out) / len(s), 2), "n": int(len(s)),
                    "lower_bound": _r(lo), "upper_bound": _r(hi), "min": _r(s.min()), "max": _r(s.max()),
                    "most_extreme": [_r(v) for v in extremes.values]}
            results.append(info)
            rows.append([c, info["outliers"], info["pct"], info["lower_bound"], info["upper_bound"],
                         info["min"], info["max"], ", ".join(str(v) for v in info["most_extreme"])])
        if not results:
            raise SqlSandboxError("No numeric column with enough variation to test.")
        results.sort(key=lambda r: -r["outliers"])
        rows.sort(key=lambda r: -r[1])
        label = f"{'1.5×IQR' if method == 'iqr' and k == 1.5 else f'{method} k={k:g}'}"
        ev = _analysis("outlier_detection", f"Outliers in {ds} ({label})", {"method": method, "threshold": k},
                       [_table("By column", ["column", "outliers", "% of rows", "lower bound", "upper bound",
                                              "min", "max", "most extreme"], rows)])
        return {"model": {"dataset_id": ds, "method": method, "threshold": k, "columns": results[:10]}, "events": [ev]}

    def missing_value_analysis(self, args):
        ds, df = self._df(args)
        n = len(df)
        rows, info = [], []
        blank_any = pd.Series(False, index=df.index)
        for c in df.columns:
            s = df[c]
            nulls = s.isna()
            if s.dtype == object or pd.api.types.is_string_dtype(s.dtype):
                blanks = s.astype(str).str.strip().eq("") & ~nulls
            else:
                blanks = pd.Series(False, index=df.index)
            miss = nulls | blanks
            blank_any |= miss
            cnt = int(miss.sum())
            if cnt:
                info.append({"column": c, "missing": cnt, "pct": _r(100 * cnt / n, 2) if n else 0,
                             "nulls": int(nulls.sum()), "blank_strings": int(blanks.sum())})
        info.sort(key=lambda r: -r["missing"])
        rows = [[r["column"], r["missing"], r["pct"], r["nulls"], r["blank_strings"]] for r in info[:40]]
        incomplete = int(blank_any.sum())
        summary = {"rows": int(n), "columns_with_missing": len(info), "columns_total": int(df.shape[1]),
                   "rows_with_any_missing": incomplete,
                   "complete_rows_pct": _r(100 * (n - incomplete) / n, 2) if n else None}
        ev = _analysis("missing_value_analysis", f"Missing values in {ds}", summary,
                       [_table("Columns with missing values", ["column", "missing", "% of rows", "nulls", "blank strings"], rows)])
        return {"model": {"dataset_id": ds, **summary, "columns": info[:15]}, "events": [ev]}

    def summary_statistics(self, args):
        ds, df = self._df(args)
        cols = _cols_arg(df, args)
        cols = [c for c in (cols or _numeric_columns(df, 12)) if _as_numeric(df[c]) is not None][:12]
        if not cols:
            raise SqlSandboxError("No numeric columns to summarise.")
        num = pd.DataFrame({c: _as_numeric(df[c]) for c in cols})

        def stats(frame):
            d = frame.describe(percentiles=[0.25, 0.5, 0.75]).T
            d["skew"] = frame.skew()
            return d

        def row(label, c, r):
            return [label, c, int(r["count"]), _r(r["mean"]), _r(r["std"]), _r(r["min"]), _r(r["25%"]),
                    _r(r["50%"]), _r(r["75%"]), _r(r["max"]), _r(r["skew"], 3)]

        head = ["group", "column", "count", "mean", "std", "min", "q25", "median", "q75", "max", "skew"]
        model_rows, table_rows = [], []
        if args.get("group_by"):
            g = _col(df, args["group_by"])
            keys = df[g].astype(str)
            top = keys.value_counts().head(_int(args, "top_groups", 8, 1, 12)).index
            for name in top:
                d = stats(num[keys == name])
                for c in cols[:4]:
                    table_rows.append(row(_shorten(name, 30), c, d.loc[c]))
            model_rows = [dict(zip(head, r)) for r in table_rows]
        else:
            d = stats(num)
            for c in cols:
                table_rows.append(row("all", c, d.loc[c]))
            model_rows = [{k: v for k, v in zip(head, r) if k != "group"} for r in table_rows]
        ev = _analysis("summary_statistics", f"Summary statistics for {ds}" + (f" by {args['group_by']}" if args.get("group_by") else ""),
                       {"columns": len(cols)}, [_table("Descriptive statistics", head, table_rows)])
        return {"model": {"dataset_id": ds, "stats": model_rows,
                          "note": "group_by limited to top groups by size and first 4 columns" if args.get("group_by") else ""},
                "events": [ev]}

    # =====================================================================
    # 4. MACHINE LEARNING
    # =====================================================================
    def _prepare(self, ds, df, target, features, task):
        """Turn a dataset into a numeric design matrix. Returns a dict with
        X (DataFrame), y (Series), task, feature_names, notes, sampled."""
        t = _col(df, target)
        feats = _cols_arg(df, {"f": features}, "f") if features else None
        feats = [f for f in (feats or list(df.columns)) if f != t]
        if not feats:
            raise SqlSandboxError("No feature columns to use.")
        ty_num = _as_numeric(df[t])
        n_unique = int(df[t].nunique(dropna=True))
        if task == "auto":
            task = "regression" if (ty_num is not None and n_unique >= REGRESSION_MIN_UNIQUE) else "classification"
        if task == "regression":
            if ty_num is None:
                raise SqlSandboxError(f"Target '{t}' is not numeric, so it can't be used for regression.")
            y = ty_num
        else:
            if n_unique > MAX_CLASSES:
                raise SqlSandboxError(f"Target '{t}' has {n_unique} distinct values — too many for classification "
                                      f"(max {MAX_CLASSES}). Use a categorical target or linear_regression.")
            y = df[t].astype(str).where(df[t].notna())
        keep = y.notna()
        notes, parts, skipped = [], [], []
        cat_budget = MAX_CATEGORICAL_FEATURES
        for c in feats:
            s = df[c]
            n = _as_numeric(s)
            if n is not None:
                if n.nunique(dropna=True) <= 1:
                    skipped.append(f"{c} (constant)")
                    continue
                if _is_id_like(c, n):
                    skipped.append(f"{c} (looks like an ID)")
                    continue
                parts.append(n.rename(c))
            else:
                u = s.nunique(dropna=True)
                if u <= 1 or u > MAX_ONEHOT_UNIQUE or cat_budget <= 0:
                    skipped.append(f"{c} ({'too many distinct values' if u > MAX_ONEHOT_UNIQUE else 'unused'})")
                    continue
                cat_budget -= 1
                parts.append(pd.get_dummies(s.astype(str).where(s.notna(), "(missing)"), prefix=str(c)).astype(float))
        if not parts:
            raise SqlSandboxError("None of the columns could be used as features (all constant, ID-like or high-cardinality text).")
        X = pd.concat(parts, axis=1)
        if X.shape[1] > MAX_FEATURES:
            raise SqlSandboxError(f"{X.shape[1]} features after encoding — pass a smaller 'features' list.")
        X, y = X[keep], y[keep]
        max_rows = int(getattr(self.cfg, "ML_MAX_TRAIN_ROWS", 50000))
        sampled = None
        if len(X) > max_rows:
            idx = X.sample(max_rows, random_state=SEED).index
            # True total from metadata, not len(df) — df itself may already be a
            # capped sample (see ChatAgent._frame), so len(df) would under-report
            # the real dataset size here.
            sampled, X, y = self._true_rows(ds), X.loc[idx], y.loc[idx]
        if len(X) < 20:
            raise SqlSandboxError(f"Only {len(X)} usable rows — need at least 20 to train and evaluate a model.")
        X = X.fillna(X.median(numeric_only=True)).fillna(0.0)
        if skipped:
            notes.append("skipped: " + "; ".join(skipped[:8]))
        if sampled:
            notes.append(f"trained on a random sample of {len(X):,} of {sampled:,} rows")
        return {"X": X, "y": y, "task": task, "target": t, "notes": notes}

    @staticmethod
    def _split(prep, args):
        from sklearn.model_selection import train_test_split
        test_size = _float(args, "test_size", 0.2, 0.1, 0.4)
        X, y = prep["X"], prep["y"]
        strat = None
        if prep["task"] == "classification":
            vc = y.value_counts()
            if len(vc) < 2:
                raise SqlSandboxError(f"Target '{prep['target']}' has only one class — nothing to predict.")
            if vc.min() >= 2 and int(len(y) * test_size) >= len(vc):
                strat = y
        return train_test_split(X, y, test_size=test_size, random_state=SEED, stratify=strat)

    @staticmethod
    def _metrics(task, y_true, y_pred, y_train):
        from sklearn import metrics as M
        if task == "regression":
            base = float(np.sqrt(M.mean_squared_error(y_true, np.full(len(y_true), y_train.mean()))))
            return {"r2": _r(M.r2_score(y_true, y_pred)), "rmse": _r(np.sqrt(M.mean_squared_error(y_true, y_pred))),
                    "mae": _r(M.mean_absolute_error(y_true, y_pred)), "baseline_rmse_predicting_mean": _r(base)}
        majority = y_train.value_counts(normalize=True).iloc[0]
        out = {"accuracy": _r(M.accuracy_score(y_true, y_pred)),
               "f1_weighted": _r(M.f1_score(y_true, y_pred, average="weighted", zero_division=0)),
               "baseline_accuracy_majority_class": _r(majority), "classes": sorted(map(str, y_train.unique()))[:MAX_CLASSES]}
        if y_train.nunique() <= 5:
            labels = sorted(y_train.unique())
            out["confusion_matrix"] = {"labels": labels, "rows_actual_cols_predicted":
                                       M.confusion_matrix(y_true, y_pred, labels=labels).tolist()}
        return out

    def _ml_result(self, name, ds, prep, metrics, extra=None, tables=None, text=None):
        task = prep["task"]
        head = {"dataset_id": ds, "target": prep["target"], "task": task, "rows_used": int(len(prep["X"])),
                "features_used": int(prep["X"].shape[1])}
        flat = {k: v for k, v in metrics.items() if not isinstance(v, (dict, list))}
        model = {**head, "test_metrics": metrics, **(extra or {}), "notes": prep["notes"],
                 "caveat": "Held-out 20% test split; compare to the baseline before trusting the model."}
        ev = _analysis(name, f"{name.replace('_', ' ').title()}: predict {prep['target']} ({task})",
                       {**head, **flat}, tables or [], text)
        return {"model": model, "events": [ev]}

    @staticmethod
    def _importance_table(names, values, title="Most important features", col="importance", k=10):
        order = np.argsort(-np.abs(np.asarray(values, dtype=float)))[:k]
        rows = [[_shorten(names[i], 40), _r(values[i], 4)] for i in order]
        return rows, _table(title, ["feature", col], rows)

    def decision_tree(self, args):
        from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor, export_text
        ds, df = self._df(args)
        prep = self._prepare(ds, df, args.get("target"), args.get("features"), str(args.get("task") or "auto").lower())
        Xtr, Xte, ytr, yte = self._split(prep, args)
        depth = _int(args, "max_depth", 4, 1, 8)
        cls = DecisionTreeRegressor if prep["task"] == "regression" else DecisionTreeClassifier
        model = cls(max_depth=depth, min_samples_leaf=5, random_state=SEED).fit(Xtr, ytr)
        metrics = self._metrics(prep["task"], yte, model.predict(Xte), ytr)
        metrics["max_depth"] = depth
        rows, tbl = self._importance_table(list(Xtr.columns), model.feature_importances_)
        rows = [r for r in rows if r[1]]
        rules = export_text(model, feature_names=[str(c) for c in Xtr.columns], max_depth=3)[:1800]
        return self._ml_result("decision_tree", ds, prep, metrics,
                               {"top_features": {r[0]: r[1] for r in rows[:8]}, "rules_top_levels": rules[:900]},
                               [tbl], rules)

    def knn(self, args):
        from sklearn.neighbors import KNeighborsClassifier, KNeighborsRegressor
        from sklearn.preprocessing import StandardScaler
        ds, df = self._df(args)
        prep = self._prepare(ds, df, args.get("target"), args.get("features"), str(args.get("task") or "auto").lower())
        Xtr, Xte, ytr, yte = self._split(prep, args)
        k = _int(args, "k", 5, 1, 50)
        k = min(k, len(Xtr))
        sc = StandardScaler().fit(Xtr)
        cls = KNeighborsRegressor if prep["task"] == "regression" else KNeighborsClassifier
        model = cls(n_neighbors=k).fit(sc.transform(Xtr), ytr)
        metrics = self._metrics(prep["task"], yte, model.predict(sc.transform(Xte)), ytr)
        metrics["k"] = k
        return self._ml_result("KNN", ds, prep, metrics,
                               {"note": "features were standardised; KNN has no feature importances"})

    def linear_regression(self, args):
        from sklearn.linear_model import LinearRegression
        from sklearn.preprocessing import StandardScaler
        ds, df = self._df(args)
        prep = self._prepare(ds, df, args.get("target"), args.get("features"), "regression")
        Xtr, Xte, ytr, yte = self._split(prep, args)
        sc = StandardScaler().fit(Xtr)
        model = LinearRegression().fit(sc.transform(Xtr), ytr)
        pred = model.predict(sc.transform(Xte))
        metrics = self._metrics("regression", yte, pred, ytr)
        metrics["train_r2"] = _r(model.score(sc.transform(Xtr), ytr))
        names, std_coef = list(Xtr.columns), model.coef_
        raw_coef = std_coef / np.where(sc.scale_ == 0, 1, sc.scale_)
        order = np.argsort(-np.abs(std_coef))[:10]
        rows = [[_shorten(names[i], 40), _r(raw_coef[i]), _r(std_coef[i])] for i in order]
        tbl = _table("Coefficients (largest effect first)", ["feature", "per 1 unit", "per 1 std dev"], rows)
        return self._ml_result("linear_regression", ds, prep, metrics,
                               {"coefficients": [{"feature": r[0], "per_unit": r[1], "per_std": r[2]} for r in rows[:8]]},
                               [tbl])

    def logistic_regression(self, args):
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import roc_auc_score
        from sklearn.preprocessing import StandardScaler
        ds, df = self._df(args)
        prep = self._prepare(ds, df, args.get("target"), args.get("features"), "classification")
        Xtr, Xte, ytr, yte = self._split(prep, args)
        sc = StandardScaler().fit(Xtr)
        model = LogisticRegression(max_iter=1000).fit(sc.transform(Xtr), ytr)
        Xte_s = sc.transform(Xte)
        metrics = self._metrics("classification", yte, model.predict(Xte_s), ytr)
        names = list(Xtr.columns)
        if len(model.classes_) == 2:
            try:
                metrics["roc_auc"] = _r(roc_auc_score(yte, model.predict_proba(Xte_s)[:, 1]))
            except ValueError:
                pass
            coef = model.coef_[0]
            order = np.argsort(-np.abs(coef))[:10]
            rows = [[_shorten(names[i], 40), _r(coef[i]), _r(math.exp(coef[i]), 3)] for i in order]
            tbl = _table(f"Coefficients toward class '{model.classes_[1]}' (per 1 std dev)", ["feature", "coefficient", "odds ratio"], rows)
            extra = {"positive_class": str(model.classes_[1]),
                     "coefficients": [{"feature": r[0], "coef": r[1], "odds_ratio": r[2]} for r in rows[:8]]}
        else:
            coef = np.abs(model.coef_).mean(axis=0)
            rows, tbl = self._importance_table(names, coef, "Average absolute coefficient", "|coef|")
            extra = {"top_features": {r[0]: r[1] for r in rows[:8]}}
        return self._ml_result("logistic_regression", ds, prep, metrics, extra, [tbl])

    def kmeans(self, args):
        from sklearn.cluster import KMeans
        from sklearn.metrics import silhouette_score
        from sklearn.preprocessing import StandardScaler
        ds, df = self._df(args)
        cols = _cols_arg(df, args, "features") or _numeric_columns(df, 12)
        cols = [c for c in cols if _as_numeric(df[c]) is not None]
        if not cols:
            raise SqlSandboxError("KMeans needs numeric feature columns.")
        X = pd.DataFrame({c: _as_numeric(df[c]) for c in cols})
        X = X.dropna(thresh=max(1, len(cols) // 2 + 1))
        X = X.fillna(X.median())
        max_rows = int(getattr(self.cfg, "ML_MAX_TRAIN_ROWS", 50000))
        notes = []
        if len(X) > max_rows:
            # True total from metadata — X here may already be built from a capped
            # sample (see ChatAgent._frame), so len(X) would under-report it.
            notes.append(f"clustered a random sample of {max_rows:,} of {self._true_rows(ds):,} rows")
            X = X.sample(max_rows, random_state=SEED)
        if len(X) < 10:
            raise SqlSandboxError("Need at least 10 usable rows to cluster.")
        scaler = StandardScaler().fit(X)
        Z = scaler.transform(X)
        sil_sample = Z if len(Z) <= 5000 else Z[np.random.RandomState(SEED).choice(len(Z), 5000, replace=False)]

        def fit(k):
            return KMeans(n_clusters=k, n_init=10, random_state=SEED).fit(Z)

        tried = None
        if args.get("k"):
            k = _int(args, "k", 3, 2, 10)
        else:
            tried = {}
            for kk in range(2, min(7, len(X) - 1)):
                m = fit(kk)
                tried[kk] = float(silhouette_score(sil_sample, m.predict(sil_sample)))
            k = max(tried, key=tried.get)
            notes.append(f"k chosen by best silhouette over 2–6: {', '.join(f'{a}→{b:.2f}' for a, b in tried.items())}")
        model = fit(k)
        sil = float(silhouette_score(sil_sample, model.predict(sil_sample)))
        sizes = np.bincount(model.labels_, minlength=k)
        centroids = scaler.inverse_transform(model.cluster_centers_)
        order = np.argsort(-sizes)
        rows = [[f"cluster {n + 1}", int(sizes[i]), _r(100 * sizes[i] / len(X), 1)] + [_r(centroids[i][j], 3) for j in range(len(cols))]
                for n, i in enumerate(order)]
        head = ["cluster", "rows", "% of rows"] + [_shorten(c, 24) for c in cols]
        summary = {"dataset_id": ds, "k": int(k), "rows_clustered": int(len(X)), "silhouette": _r(sil, 3),
                   "features": cols}
        ev = _analysis("KMeans", f"KMeans clustering of {ds} (k={k})", summary,
                       [_table("Cluster sizes and centroids (original units)", head, rows)])
        return {"model": {**summary, "interpretation": "silhouette near 1 = well separated, near 0 = overlapping",
                          "clusters": [dict(zip(["cluster", "rows", "pct"] + cols, r)) for r in rows], "notes": notes},
                "events": [ev]}


# ------------------------------------------------------------------ small module-level helpers
def _is_number(v):
    try:
        float(v)
        return True
    except (TypeError, ValueError):
        return False


def _fmt(v):
    v = float(v)
    if abs(v) >= 1000:
        return f"{v:,.0f}"
    return f"{v:.3g}"


def _top_pairs(m, k):
    cols = list(m.columns)
    pairs = []
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            r = m.iloc[i, j]
            if r == r:
                pairs.append({"a": cols[i], "b": cols[j], "r": _r(r, 3)})
    pairs.sort(key=lambda p: -abs(p["r"]))
    return pairs[:k]


# ------------------------------------------------------------------ schemas the model sees
# Kept terse on purpose: 20 tools' worth of schema is paid for in the 8B model's context
# window on every single request. Handlers also accept a few optional arguments that are
# deliberately not advertised here (title, top_n, bins ... see each tool) to save tokens.
_AGG = {"type": "string", "enum": ["sum", "avg", "min", "max", "median", "count"]}
_STR = {"type": "string"}


def _t(name, desc, props=None, required=()):
    return {"type": "function", "function": {"name": name, "description": desc, "parameters": {
        "type": "object", "properties": props or {}, **({"required": list(required)} if required else {})}}}


TOOL_SCHEMAS = [
    _t("list_datasets", "List all datasets with row counts and columns."),
    _t("get_schema", "Columns, types, null counts and common values of a dataset. Call before using a dataset.",
       {"dataset_id": _DS}, ["dataset_id"]),
    _t("get_sample", "Show a few rows of a dataset.", {"dataset_id": _DS, "n": {"type": "integer"}}, ["dataset_id"]),
    _t("get_statistics", "Dataset profile: rows, duplicates, per-column min/max/mean/median/std or top values.",
       {"dataset_id": _DS}, ["dataset_id"]),
    _t("run_query", "ONE read-only DuckDB SELECT. Datasets are tables named by dataset_id (double-quoted); JOINs allowed.",
       {"sql": _STR}, ["sql"]),
    _t("bar_chart", "Bar chart of y (aggregated by agg, default sum; count if no y) per category x. Omit dataset_id to chart the last run_query result.",
       {"dataset_id": _DS, "x": _STR, "y": _STR, "agg": _AGG}, ["x"]),
    _t("line_chart", "Line chart of y over ordered x (dates or numbers); period=day|month|year groups dates. Omit dataset_id to chart the last run_query result.",
       {"dataset_id": _DS, "x": _STR, "y": _STR, "agg": _AGG, "period": {"type": "string", "enum": ["day", "month", "year"]}}, ["x"]),
    _t("scatter_plot", "Scatter plot of two numeric columns with their correlation.",
       {"dataset_id": _DS, "x": _STR, "y": _STR}, ["dataset_id", "x", "y"]),
    _t("heatmap", "kind=correlation: numeric columns. kind=pivot: x by y, counts or value aggregated by agg.",
       {"dataset_id": _DS, "kind": {"type": "string", "enum": ["correlation", "pivot"]}, "x": _STR, "y": _STR,
        "value": _STR, "agg": _AGG}, ["dataset_id"]),
    _t("histogram", "Distribution of a numeric column in bins.", {"dataset_id": _DS, "column": _STR}, ["dataset_id", "column"]),
    _t("boxplot", "Box plot of a numeric column, optionally per group_by value.",
       {"dataset_id": _DS, "column": _STR, "group_by": _STR}, ["dataset_id", "column"]),
    _t("correlation", "Strongest correlations among numeric columns, or with a target column.",
       {"dataset_id": _DS, "target": _STR, "method": {"type": "string", "enum": ["pearson", "spearman"]}}, ["dataset_id"]),
    _t("outlier_detection", "Find outliers in one column or all numeric columns (IQR or z-score).",
       {"dataset_id": _DS, "column": _STR, "method": {"type": "string", "enum": ["iqr", "zscore"]}}, ["dataset_id"]),
    _t("missing_value_analysis", "Missing/blank counts per column and share of complete rows.", {"dataset_id": _DS}, ["dataset_id"]),
    _t("summary_statistics", "Mean, std, quartiles, skew of numeric columns, optionally per group_by value.",
       {"dataset_id": _DS, "group_by": _STR}, ["dataset_id"]),
    _t("decision_tree", "Decision tree predicting target (classification or regression, auto). Returns test metric, key features, rules.",
       {"dataset_id": _DS, "target": _STR, "features": _COLS}, ["dataset_id", "target"]),
    _t("KNN", "k-nearest-neighbours predicting target (classification or regression, auto). Returns test metric.",
       {"dataset_id": _DS, "target": _STR, "features": _COLS, "k": {"type": "integer"}}, ["dataset_id", "target"]),
    _t("linear_regression", "Linear regression of a numeric target. Returns R², RMSE, coefficients.",
       {"dataset_id": _DS, "target": _STR, "features": _COLS}, ["dataset_id", "target"]),
    _t("logistic_regression", "Logistic regression of a categorical target. Returns accuracy, F1, coefficients.",
       {"dataset_id": _DS, "target": _STR, "features": _COLS}, ["dataset_id", "target"]),
    _t("KMeans", "Cluster rows by numeric features into k groups (k auto-chosen if omitted). Returns sizes, centroids.",
       {"dataset_id": _DS, "features": _COLS, "k": {"type": "integer"}}, ["dataset_id"]),
]
