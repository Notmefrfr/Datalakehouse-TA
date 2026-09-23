"""
Spark ETL service.

Owns every data transformation: cleaning, deduping, joining/merging, and
chart aggregation. Nothing upstream (routes, frontend) implements this logic
itself — they only call into this service.

Two backends, selected by Config.SPARK_MODE:

  "local"   (default) — runs the exact same transformations with pandas,
            in-process. No cluster required; ideal for dev and for datasets
            that comfortably fit in memory.
  "cluster" — runs against a real PySpark SparkSession (Config.SPARK_MASTER_URL),
            for production-scale data. Requires a reachable Spark cluster and
            the pyspark package.

Both backends implement the same public methods so routes/services never
need to know which one is active.
"""
import io
import re

import pandas as pd


class SparkETLService:
    def __init__(self, app_config):
        self.mode = app_config.SPARK_MODE
        self._spark = None
        if self.mode == "cluster":
            self._spark = self._build_spark_session(app_config)

    # -- session bootstrap (cluster mode only) --------------------------------

    def _build_spark_session(self, app_config):
        from pyspark.sql import SparkSession  # imported lazily — only needed in cluster mode

        return (
            SparkSession.builder
            .appName("lakehouse-etl")
            .master(app_config.SPARK_MASTER_URL)
            .getOrCreate()
        )

    # -- CSV <-> DataFrame -----------------------------------------------------

    @staticmethod
    def read_csv_text(csv_text, delimiter=","):
        """Parse CSV text into a pandas DataFrame. Used for both backends —
        cluster mode converts to/from a Spark DataFrame only for the actual
        transform step, so callers always deal in pandas + CSV text.

        `delimiter` only ever matters for a freshly-uploaded file (some
        exports use ';' or tab) — everything already stored in MinIO is
        always canonical comma-separated, since to_csv_text() below never
        writes anything else. That's what lets Prepare/Merge/Visualize and
        the automatic-merge master files stay delimiter-agnostic downstream."""
        return pd.read_csv(io.StringIO(csv_text), sep=delimiter, dtype=None, keep_default_na=True)

    @staticmethod
    def to_csv_text(df):
        return df.to_csv(index=False)

    @staticmethod
    def sanitize_for_export(csv_text):
        """Neutralize CSV formula injection before a file is offered for
        download. Excel/Sheets treats any cell starting with =, +, -, or @
        as a live formula the moment the file is opened — since cell values
        here can come from anyone's upload, a value like
        `=cmd|'/c calc'!A1` or `=HYPERLINK("http://evil","click")` would run
        for whoever downloads and opens this file locally. Prefixing such
        cells with a single quote keeps Excel/Sheets from treating them as
        formulas (they display as plain text instead) while leaving every
        other value untouched.

        Deliberately NOT applied inside to_csv_text() / anywhere data is
        merely stored or round-tripped internally (clean(), the Delta
        upsert/append path in DeltaService, etc.) — only at the point a
        file is actually handed to someone to
        download. Doing it everywhere would risk quoting values that are
        later read back and re-parsed as numbers/dates internally; doing it
        only at export time avoids that while still protecting the person
        who opens the downloaded file."""
        df = SparkETLService.read_csv_text(csv_text)
        for col in df.columns:
            df[col] = df[col].map(
                lambda v: "'" + v if isinstance(v, str) and v[:1] in ("=", "+", "-", "@") else v
            )
        return SparkETLService.to_csv_text(df)

    # -- validation --------------------------------------------------------------

    @staticmethod
    def validate_upload(csv_text, required_columns, delimiter=","):
        checks = []
        lines = csv_text.splitlines()
        if not lines or not lines[0].strip():
            return {"valid": False, "checks": [{"ok": False, "message": "File is empty."}]}

        header = [h.strip() for h in lines[0].split(delimiter)]
        if len(header) == 1 and delimiter != ",":
            checks.append({"ok": False, "message": f"Couldn't confidently parse this file's columns using the detected delimiter ('{delimiter}') — the file may be malformed."})

        seen, dupes = set(), set()
        for h in header:
            if h in seen:
                dupes.add(h)
            seen.add(h)
        checks.append(
            {"ok": False, "message": "Duplicate column(s): " + ", ".join(sorted(dupes))} if dupes
            else {"ok": True, "message": "No duplicate columns."}
        )

        missing = [c for c in required_columns if c not in header]
        checks.append(
            {"ok": False, "message": "Missing required column(s): " + ", ".join(missing)} if missing
            else {"ok": True, "message": "All required columns present (" + ", ".join(required_columns) + ")."}
        )

        data_line_count = len([l for l in lines if l.strip()]) - 1
        checks.append(
            {"ok": True, "message": f"{data_line_count} data row(s) found."} if data_line_count > 0
            else {"ok": False, "message": "File has a header but no data rows."}
        )

        return {"valid": all(c["ok"] for c in checks), "checks": checks}

    # -- cleaning ----------------------------------------------------------------

    def clean(self, csv_text, operations):
        """Apply the requested cleaning operations and return cleaned CSV text.
        Cluster mode hands the same steps to Spark; local mode uses pandas directly."""
        if self.mode == "cluster":
            return self._clean_cluster(csv_text, operations)
        return self._clean_local(csv_text, operations)

    def _clean_local(self, csv_text, operations):
        df = self.read_csv_text(csv_text)
        df = self._apply_cleaning_ops(df, operations)
        return self.to_csv_text(df)

    def _clean_cluster(self, csv_text, operations):
        from pyspark.sql import functions as F  # noqa: N812

        pdf = self.read_csv_text(csv_text)
        sdf = self._spark.createDataFrame(pdf.astype(object).where(pdf.notnull(), None))

        if "trim" in operations:
            for c, dtype in sdf.dtypes:
                if dtype == "string":
                    sdf = sdf.withColumn(c, F.trim(F.col(c)))
        if "normalize_case" in operations:
            for c, dtype in sdf.dtypes:
                if dtype == "string":
                    sdf = sdf.withColumn(c, F.initcap(F.col(c)))
        if "dedupe" in operations:
            sdf = sdf.dropDuplicates()
        if "fill_missing" in operations:
            # Same reasoning as _apply_cleaning_ops' local-mode fill_missing:
            # "" is indistinguishable from a genuinely missing cell once
            # written to and re-read from CSV, so a real placeholder is used
            # instead — keeps cluster mode's output identical to local mode's.
            sdf = sdf.na.fill(0).na.fill("Not Provided")
        # "fix_types" is applied after the round-trip below via pandas, since
        # per-column type inference is simpler there and this endpoint targets
        # the same preview-sized datasets either way.

        result_pdf = sdf.toPandas()
        if "fix_types" in operations:
            result_pdf = self._fix_types(result_pdf)
        return self.to_csv_text(result_pdf)

    @staticmethod
    def _fix_types(df):
        for col in df.columns:
            series = df[col].dropna()
            if len(series) == 0:
                continue
            series_str = series.astype(str)
            numeric_ok = pd.to_numeric(series_str, errors="coerce").notna().mean()
            if numeric_ok > 0.8:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                continue
            # format="mixed": parses EACH value independently instead of
            # inferring one format from the first row and forcing every
            # other row to match it. Without this, a column mixing
            # "17/09/2024" and "09/17/2024" would parse the first fine and
            # silently turn the second into NaT (invalid), since pandas'
            # default behavior locks onto whatever format the first
            # non-null value looked like. dayfirst=True matches this app's
            # convention (DD/MM over MM/DD) for the genuinely ambiguous
            # cases where either reading would be a valid date.
            date_ok = pd.to_datetime(series_str, errors="coerce", format="mixed", dayfirst=True).notna().mean()
            if date_ok > 0.8:
                df[col] = pd.to_datetime(df[col], errors="coerce", format="mixed", dayfirst=True).dt.strftime("%Y-%m-%d")
        return df

    def _apply_cleaning_ops(self, df, operations):
        df = df.copy()
        string_cols = df.select_dtypes(include="object").columns

        if "trim" in operations:
            for c in string_cols:
                df[c] = df[c].apply(lambda v: v.strip() if isinstance(v, str) else v)

        if "normalize_case" in operations:
            def _title(v):
                return re.sub(r"\w\S*", lambda m: m.group()[0].upper() + m.group()[1:].lower(), v)
            for c in string_cols:
                df[c] = df[c].apply(lambda v: _title(v) if isinstance(v, str) else v)

        if "fix_types" in operations:
            df = self._fix_types(df)

        if "dedupe" in operations:
            df = df.drop_duplicates()

        if "fill_missing" in operations:
            numeric_cols = df.select_dtypes(include="number").columns
            for c in df.columns:
                if c in numeric_cols:
                    df[c] = df[c].fillna(0)
                else:
                    # NOT fillna("") — an empty string and a genuinely missing
                    # cell are written to CSV identically (nothing between
                    # the commas), so the very next read_csv_text() call
                    # would see this "filled" cell as missing again and this
                    # operation's effect would be invisible one round-trip
                    # later. A real placeholder value survives being written
                    # to and re-read from CSV, which "" cannot.
                    df[c] = df[c].fillna("Not Provided")

        return df

    # -- merge / join --------------------------------------------------------

    def merge(self, csv_text_a, csv_text_b, join_type):
        """Join two datasets on their first shared column. Returns
        (columns, rows_as_dicts, join_column) or raises ValueError if there's
        no common column."""
        df_a = self.read_csv_text(csv_text_a)
        df_b = self.read_csv_text(csv_text_b)
        common = [c for c in df_a.columns if c in df_b.columns]
        if not common:
            raise ValueError("These datasets have no column in common to join on.")
        join_col = common[0]

        how = {"inner": "inner", "left": "left", "right": "right", "outer": "outer"}.get(join_type, "inner")
        merged = pd.merge(df_a, df_b, on=join_col, how=how, suffixes=("", "_b"))
        merged = merged.where(pd.notnull(merged), None)
        return list(merged.columns), merged.to_dict(orient="records"), join_col

    # -- aggregation for charts ----------------------------------------------

    @staticmethod
    def detect_column_types(csv_text):
        """Classify each column as 'numeric', 'date', or 'categorical' by
        testing the actual data — not the column name — so a column named
        'id' full of numbers is still numeric, and 'notes' full of dates
        (rare, but possible) is still detected as date. Uses the same
        80%-successfully-parsed threshold _fix_types() already uses for
        cleaning, so a column is classified the same way everywhere in the
        app. Also returns unique_count, which chart-compatibility filtering
        (e.g. keeping Pie charts off high-cardinality columns) needs.

        Returns {column_name: {"type": "numeric"|"date"|"categorical",
                                "unique_count": int}}.
        """
        df = SparkETLService.read_csv_text(csv_text)
        result = {}
        for col in df.columns:
            series = df[col].dropna()
            unique_count = int(series.nunique())
            if len(series) == 0:
                result[col] = {"type": "categorical", "unique_count": 0}
                continue
            series_str = series.astype(str)
            numeric_ok = pd.to_numeric(series_str, errors="coerce").notna().mean()
            if numeric_ok > 0.8:
                result[col] = {"type": "numeric", "unique_count": unique_count}
                continue
            date_ok = pd.to_datetime(series_str, errors="coerce", format="mixed").notna().mean()
            if date_ok > 0.8:
                result[col] = {"type": "date", "unique_count": unique_count}
                continue
            result[col] = {"type": "categorical", "unique_count": unique_count}
        return result

    def aggregate(self, csv_text, x_col, y_col=None, method="sum", limit=20):
        """Groups by x_col and reduces y_col per group. `method` defaults
        to "sum" so every existing caller (hardcoded chart specs in
        Config.SUMMARY_CONFIG, etc.) behaves exactly as before without
        changes. "count" is the one method that doesn't need a numeric
        y_col at all — it's just how many rows fall in each x_col group —
        so y_col is optional specifically for that case, same convention
        Config.SUMMARY_CONFIG already uses (y: None means count)."""
        df = self.read_csv_text(csv_text)
        if x_col not in df.columns:
            raise ValueError("Chosen column is not present in this dataset.")

        if method == "count":
            counts = df.groupby(x_col).size().sort_values(ascending=False).head(limit)
            return [str(k) for k in counts.index], [int(v) for v in counts.values]

        if method not in ("sum", "avg", "median"):
            raise ValueError(f"Unknown calculation method '{method}'.")
        if not y_col or y_col not in df.columns:
            raise ValueError("A value column is required for this calculation method.")

        y_numeric = pd.to_numeric(df[y_col], errors="coerce")
        work = pd.DataFrame({x_col: df[x_col], "__value__": y_numeric}).dropna(subset=["__value__"])
        if work.empty:
            raise ValueError(f"'{y_col}' doesn't have usable numbers to chart.")

        grouped = work.groupby(x_col)["__value__"]
        reduced = {"sum": grouped.sum, "avg": grouped.mean, "median": grouped.median}[method]()
        reduced = reduced.sort_values(ascending=False).head(limit)
        return [str(k) for k in reduced.index], [float(v) for v in reduced.values]

    def histogram(self, csv_text, column, bins=10):
        """Bins a single numeric column into `bins` equal-width buckets.
        Returns (bin_labels, counts) — same shape as aggregate()'s
        (labels, values) so the frontend can reuse its existing bar-chart
        rendering path for histograms too."""
        df = self.read_csv_text(csv_text)
        if column not in df.columns:
            raise ValueError("Chosen column is not present in this dataset.")
        values = pd.to_numeric(df[column], errors="coerce").dropna()
        if values.empty:
            raise ValueError(f"'{column}' doesn't have usable numbers to chart.")
        counts = values.groupby(pd.cut(values, bins=bins, include_lowest=True), observed=True).size()
        labels = [f"{interval.left:.1f}–{interval.right:.1f}" for interval in counts.index]
        return labels, [int(v) for v in counts.values]

    def scatter(self, csv_text, x_col, y_col, limit=500):
        """Raw (x, y) numeric point pairs — deliberately NOT aggregated,
        unlike aggregate()/histogram() above, since a scatter plot's whole
        point is showing individual data points, not a summary of them.
        Capped at `limit` points so a huge dataset doesn't render an
        unreadable/slow chart; sampled evenly rather than just truncated so
        the plot still represents the full dataset's spread."""
        df = self.read_csv_text(csv_text)
        if x_col not in df.columns or y_col not in df.columns:
            raise ValueError("Chosen columns are not present in this dataset.")
        x_numeric = pd.to_numeric(df[x_col], errors="coerce")
        y_numeric = pd.to_numeric(df[y_col], errors="coerce")
        work = pd.DataFrame({"x": x_numeric, "y": y_numeric}).dropna()
        if work.empty:
            raise ValueError(f"'{x_col}' and '{y_col}' don't have enough usable numbers to chart together.")
        if len(work) > limit:
            work = work.sample(n=limit, random_state=0).sort_index()
        return [{"x": float(r.x), "y": float(r.y)} for r in work.itertuples()]

    def monthly_trend(self, csv_text, date_col, num_col):
        """Like aggregate(), but for Line charts specifically: buckets by
        month and stays in CHRONOLOGICAL order (aggregate() sorts by value
        descending, which turns a trend line into a jagged, meaningless
        zigzag). Returns (month_labels, values)."""
        df = self.read_csv_text(csv_text)
        if date_col not in df.columns or num_col not in df.columns:
            raise ValueError("Chosen columns are not present in this dataset.")
        dates = pd.to_datetime(df[date_col], errors="coerce")
        work = pd.DataFrame({"d": dates, "n": pd.to_numeric(df[num_col], errors="coerce")}).dropna()
        if work.empty:
            raise ValueError(f"'{date_col}' and '{num_col}' don't have enough usable data to chart together.")
        monthly = work.groupby(work["d"].dt.to_period("M"))["n"].sum().sort_index()
        return [str(p) for p in monthly.index], [float(v) for v in monthly.values]

    def category_counts(self, csv_text, column, limit=20):
        """Count of rows per category — the 'y=None' half of a hardcoded
        chart spec (see Config.SUMMARY_CONFIG): used whenever a format has
        no genuine numeric column to sum (e.g. counting inspections per
        technician, since there's nothing to sum them by), so a Bar/Pie
        chart still has something meaningful to show. Same (labels, values)
        shape as aggregate(), sorted by count descending like aggregate()'s
        totals are, so both slot into the same rendering path downstream."""
        df = self.read_csv_text(csv_text)
        if column not in df.columns:
            raise ValueError("Chosen column is not present in this dataset.")
        counts = df[column].dropna().astype(str).value_counts().head(limit)
        if counts.empty:
            raise ValueError(f"'{column}' has no usable values to chart.")
        return [str(k) for k in counts.index], [int(v) for v in counts.values]

    def monthly_count_trend(self, csv_text, date_col, limit=None):
        """Like monthly_trend(), but counts rows per month instead of
        summing a numeric column — the Line-chart equivalent of
        category_counts() above, for formats with a date column but no
        numeric column to trend (e.g. inspections per month)."""
        df = self.read_csv_text(csv_text)
        if date_col not in df.columns:
            raise ValueError("Chosen column is not present in this dataset.")
        dates = pd.to_datetime(df[date_col], errors="coerce").dropna()
        if dates.empty:
            raise ValueError(f"'{date_col}' doesn't have enough usable dates to chart.")
        monthly = dates.groupby(dates.dt.to_period("M")).size().sort_index()
        return [str(p) for p in monthly.index], [int(v) for v in monthly.values]

    def compute_hardcoded_charts(self, csv_text, chart_specs, chart_type):
        """Interprets a format's hardcoded Config.SUMMARY_CONFIG['charts']
        entries for ONE chart type against the actual dataset — the chart
        counterpart to compute_hardcoded_summary() above, and built for the
        same reason: 'what's a valid comparison' genuinely differs per
        format, so this reads a declared list of (x, y) pairs instead of
        statistically guessing one (which previously produced nonsense like
        summing a reference-number column grouped by region).

        Each spec is {"label", "x", "y"}; "y" of None means 'count of rows
        per x' rather than 'sum of y per x'. line uses month-bucketed
        trends (monthly_trend/monthly_count_trend); bar/pie use
        aggregate()/category_counts(), which are just sum-vs-count over a
        category — both already sorted descending. Returns a list of
        {"label", "x", "y", "labels", "values"}, one per spec. A single
        spec whose columns don't actually work for THIS dataset instance
        (e.g. a numeric column that turned out to be entirely blank) is
        silently skipped rather than failing the whole batch, same
        skip-on-failure spirit as compute_hardcoded_summary()."""
        charts = []
        for spec in chart_specs:
            x_col, y_col, label = spec["x"], spec.get("y"), spec["label"]
            try:
                if chart_type == "line":
                    labels, values = (
                        self.monthly_trend(csv_text, x_col, y_col) if y_col
                        else self.monthly_count_trend(csv_text, x_col)
                    )
                else:  # bar, pie
                    labels, values = (
                        self.aggregate(csv_text, x_col, y_col) if y_col
                        else self.category_counts(csv_text, x_col)
                    )
            except (ValueError, KeyError):
                continue
            charts.append({"label": label, "x": x_col, "y": y_col, "labels": labels, "values": values})
        return charts

    def daily_summary_and_preview(self, csv_text, date_col, target_date, preview_limit=50):
        """Generic 'what happened on this date' — works for ANY dataset
        with a date column, not hardcoded to a particular category (brief
        point #6: must keep working for future categories like Operation,
        Procurement, Asset, Maintenance without code changes). Filters rows
        to the given calendar date (ignoring time-of-day if present), then:

          - counts rows per value for each categorical column (status,
            jenis, or anything else that's actually present — discovered
            via detect_column_types, the same classifier used everywhere
            else in the app) — this is what makes a status/category
            breakdown for that specific day fall out for free, without
            hardcoding "status" or "jenis" by name (brief point #5).
          - sums/averages each genuine numeric column. Identifier-like
            columns (name ends in "_id"/"id") are excluded — summing an ID
            number is meaningless, same reasoning used elsewhere in this
            file.

        Returns None only if date_col isn't a real column on this dataset
        (a caller error) — a date with zero matching rows is a normal,
        valid result (total_records: 0), not an error, since "nothing
        happened that day" is a legitimate answer."""
        df = self.read_csv_text(csv_text)
        if date_col not in df.columns:
            return None

        dates = pd.to_datetime(df[date_col], errors="coerce").dt.date
        target = pd.to_datetime(target_date, errors="coerce")
        if pd.isna(target):
            return None
        day_df = df[dates == target.date()]

        if day_df.empty:
            return {"total_records": 0, "category_breakdown": [], "numeric_summary": [], "preview_rows": [], "preview_fields": []}

        # Classified on the FULL dataset, not just this day's slice, so a
        # column's type doesn't flip depending on which day happens to be
        # picked (e.g. a numeric column with only one row that day
        # shouldn't suddenly look "categorical" just from a tiny sample).
        types = self.detect_column_types(csv_text)

        category_breakdown = []
        for col, info in types.items():
            if info["type"] != "categorical":
                continue
            if col.strip().lower() == "id" or col.strip().lower().endswith("_id"):
                continue
            counts = day_df[col].dropna().astype(str).value_counts()
            if counts.empty:
                continue
            category_breakdown.append({
                "column": col,
                "counts": [{"value": v, "count": int(c)} for v, c in counts.items()],
            })

        numeric_summary = []
        for col, info in types.items():
            if info["type"] != "numeric":
                continue
            if col.strip().lower() == "id" or col.strip().lower().endswith("_id"):
                continue
            series = pd.to_numeric(day_df[col], errors="coerce").dropna()
            if series.empty:
                continue
            numeric_summary.append({"column": col, "sum": float(series.sum()), "avg": float(series.mean())})

        preview_df = day_df.head(preview_limit).where(day_df.notnull(), None)
        return {
            "total_records": int(len(day_df)),
            "category_breakdown": category_breakdown,
            "numeric_summary": numeric_summary,
            "preview_rows": preview_df.to_dict(orient="records"),
            "preview_fields": list(day_df.columns),
        }

    def compute_hardcoded_summary(self, csv_text, format_config):
        """Interprets ONE format's hardcoded Config.SUMMARY_CONFIG entry
        (kpis + table specs) against the actual dataset. This is what makes
        the config declarative instead of needing new aggregation code
        every time a format's metrics change — the metric TYPES here
        (count/sum/avg/max/min/nunique/top_category/category_counts) are
        the only vocabulary the config can use; adding a new format is
        just adding entries built from these, not writing new code.

        Returns {"kpis": [{"label", "value"}], "rows": [{"metric", "value"}]}.
        Silently skips any metric whose column isn't in the dataset (rather
        than crashing) — this is the actual dataset's data, and a hardcoded
        config making an assumption that doesn't hold shouldn't take down
        the whole summary."""
        df = self.read_csv_text(csv_text)

        def metric_value(spec):
            mtype = spec["type"]
            col = spec.get("column")
            if col is not None and col not in df.columns:
                return None
            if mtype == "count":
                return f"{len(df):,}"
            series = pd.to_numeric(df[col], errors="coerce").dropna() if col else None
            if mtype == "sum":
                return None if series is None or series.empty else f"{series.sum():,.2f}"
            if mtype == "avg":
                return None if series is None or series.empty else f"{series.mean():,.2f}"
            if mtype == "max":
                return None if series is None or series.empty else f"{series.max():,.2f}"
            if mtype == "min":
                return None if series is None or series.empty else f"{series.min():,.2f}"
            if mtype == "nunique":
                return f"{df[col].dropna().nunique():,}"
            if mtype == "top_category":
                counts = df[col].dropna().astype(str).value_counts()
                return None if counts.empty else f"{counts.index[0]} ({counts.iloc[0]:,})"
            if mtype == "category_share":
                total = df[col].dropna().shape[0]
                if total == 0:
                    return None
                match = (df[col].dropna().astype(str) == str(spec["value"])).sum()
                return f"{match / total:.0%}"
            return None

        kpis = []
        for spec in format_config.get("kpis", []):
            value = metric_value(spec)
            if value is not None:
                kpis.append({"label": spec["label"], "value": value})

        rows = []
        for spec in format_config.get("table", []):
            if spec["type"] == "category_counts":
                col = spec["column"]
                if col not in df.columns:
                    continue
                top = df[col].dropna().astype(str).value_counts().head(5)
                for value, count in top.items():
                    rows.append({"metric": f"{spec['label']} — {value}", "value": f"{count:,}"})
            else:
                value = metric_value(spec)
                if value is not None:
                    rows.append({"metric": spec["label"], "value": value})

        return {"kpis": kpis, "rows": rows}

    # Automatic merge of an upload into its format's continuously-growing
    # master dataset used to live here as merge_into_master(), rewriting the
    # whole master CSV on every upload. That's now DeltaService.append() /
    # DeltaService.upsert() (services/delta_service.py) — each upload lands
    # as its own small Parquet file (a real Delta MERGE for the
    # remove/replace dedupe modes) instead of a full rewrite.