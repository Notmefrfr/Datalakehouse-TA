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

CHANGED (dynamic-dataset feature, 2026-09-24): compute_hardcoded_summary()/
compute_hardcoded_charts() are GONE — they read Config.SUMMARY_CONFIG,
which no longer exists (there's no more fixed division/dataset_type
catalog to hang a hardcoded metric config off of). They're replaced by
compute_auto_summary()/compute_auto_charts() at the bottom of this file:
column-type-driven, so they work for ANY dataset's actual current schema,
including one that just grew a column via an approved schema change,
without any per-format config to maintain.
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
        """Parse CSV text into a pandas DataFrame. `delimiter` only ever
        matters for a freshly-uploaded file (some exports use ';' or tab) —
        everything already stored is always canonical comma-separated,
        since to_csv_text() below never writes anything else."""
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
        other value untouched."""
        df = SparkETLService.read_csv_text(csv_text)
        for col in df.columns:
            df[col] = df[col].map(
                lambda v: "'" + v if isinstance(v, str) and v[:1] in ("=", "+", "-", "@") else v
            )
        return SparkETLService.to_csv_text(df)

    # -- validation --------------------------------------------------------------

    @staticmethod
    def validate_upload(csv_text, required_columns=None, delimiter=","):
        """required_columns is now optional/typically empty — with no more
        fixed dataset_type contracts, there's nothing to require by name
        any more; the only real per-file checks left are structural (no
        duplicate columns, at least one data row). A caller doing
        dataset-matching (services/dataset_service.py) checks column
        OVERLAP against existing datasets separately, after this passes."""
        required_columns = required_columns or []
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

        if required_columns:
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

        # Returned so the frontend can offer a key-column picker (for the
        # row-identity matching services/dataset_service.py does on upload)
        # without a second round trip just to learn the header.
        return {"valid": all(c["ok"] for c in checks), "checks": checks, "columns": header}

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

    def clean_chunk(self, df, operations, seen_hashes=None, type_plan=None):
        """Chunked-upload counterpart to clean(): cleans one already-parsed
        DataFrame chunk in place of a full CSV-text round trip (services/
        dataset_service.py's streaming upload path calls this once per
        chunk instead of materializing the whole file as one DataFrame).

        Always cluster-mode-agnostic — chunked ETL only runs through pandas
        locally; SPARK_MODE=cluster's job is genuinely-distributed
        processing of data that's already landed, not this ingestion path.

        `seen_hashes`, if given, is a set() the caller keeps across chunks:
        when present, "dedupe" removes a row that duplicates one from an
        EARLIER chunk too (not just within this chunk), by hashing each
        row and checking/adding to that set. Pass None to just dedupe each
        chunk against itself (cheaper, no cross-chunk memory).

        `type_plan`, if given (see infer_column_types()), is the single,
        whole-file numeric/date decision every chunk's "fix_types" must
        use — without it, each chunk would decide for itself from only its
        own rows, which is what used to make two chunks of the very same
        column disagree on its dtype (one int64, one float64) and break
        the Delta table's schema partway through a big upload."""
        operations = list(operations)
        cross_chunk_dedupe = seen_hashes is not None and "dedupe" in operations
        if cross_chunk_dedupe:
            operations = [op for op in operations if op != "dedupe"]

        df = self._apply_cleaning_ops(df, operations, type_plan=type_plan)

        if cross_chunk_dedupe and len(df):
            row_hashes = pd.util.hash_pandas_object(df, index=False)
            keep_mask = []
            for h in row_hashes:
                h = int(h)
                if h in seen_hashes:
                    keep_mask.append(False)
                else:
                    seen_hashes.add(h)
                    keep_mask.append(True)
            df = df[keep_mask]

        return df

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
            sdf = sdf.na.fill(0).na.fill("Not Provided")

        result_pdf = sdf.toPandas()
        if "fix_types" in operations:
            result_pdf = self._fix_types(result_pdf)
        return self.to_csv_text(result_pdf)

    # Matches a number written as digit groups separated by a thousands
    # character, with an optional decimal tail — the ONE shape a genuine
    # decimal never has, since every group after the first must be exactly
    # 3 digits (a real decimal's precision is whatever it happens to be,
    # not always 3), so a bare "5.1" or "5.10" never matches either regex
    # and is never misread as thousands-grouped:
    #   Indonesian style — "." thousands, "," decimal: "5.200.000",
    #   "5.200.000,50"
    #   US style        — "," thousands, "." decimal: "5,200,000",
    #   "5,200,000.50"
    _ID_THOUSANDS_RE = re.compile(r"^-?\d{1,3}(\.\d{3})+(,\d+)?$")
    _US_THOUSANDS_RE = re.compile(r"^-?\d{1,3}(,\d{3})+(\.\d+)?$")
    _RP_PREFIX_RE = re.compile(r"^Rp\.?\s*", re.IGNORECASE)

    @staticmethod
    def _normalize_numeric_string(value):
        """Strips a leading "Rp" currency prefix and, when the remaining
        text is unambiguously thousands-grouped (Indonesian "." or US ","
        style — see the regexes above), converts it to plain digits
        pd.to_numeric can parse. Left untouched otherwise. Non-string
        values pass through unchanged."""
        if not isinstance(value, str):
            return value
        s = SparkETLService._RP_PREFIX_RE.sub("", value.strip()).strip()
        if SparkETLService._ID_THOUSANDS_RE.match(s):
            s = s.replace(".", "").replace(",", ".")
        elif SparkETLService._US_THOUSANDS_RE.match(s):
            s = s.replace(",", "")
        return s

    _RP_SAMPLE_SIZE = 200

    @classmethod
    def _coerce_numeric(cls, series):
        """Numeric coercion used to always route every single value through
        _normalize_numeric_string() first — a Python function call plus up
        to three regex tests per cell, paid even for the ordinary case of
        a column that's already plain decimal numbers with no "Rp" prefix
        or thousands separators at all (true of most numeric columns in
        practice). This tries pandas' own vectorized pd.to_numeric() FIRST
        — no per-cell Python calls — and only falls back to the slower
        per-cell normalization for the values that one couldn't parse.

        That fallback is itself gated by a small sample check: a genuinely
        non-numeric (plain text) column has EVERY value "unresolved" by the
        direct pass, and none of them would be rescued by Rp/thousands
        normalization either (it's just words) — running the full per-cell
        pass over such a column is pure waste, so a sample of up to
        _RP_SAMPLE_SIZE unresolved values is checked first, and the full
        fallback only runs when that sample shows it would actually help."""
        direct = pd.to_numeric(series, errors="coerce")
        unresolved = direct.isna() & series.notna()
        if not unresolved.any():
            return direct

        sample = series[unresolved].head(cls._RP_SAMPLE_SIZE)
        sample_rescued = pd.to_numeric(sample.map(cls._normalize_numeric_string), errors="coerce").notna()
        if not sample_rescued.any():
            return direct  # not a numeric-with-formatting column — just not numeric

        normalized = series[unresolved].map(cls._normalize_numeric_string)
        direct = direct.copy()
        direct.loc[unresolved] = pd.to_numeric(normalized, errors="coerce")
        return direct

    # A cheap screen before ever calling pd.to_datetime(..., format="mixed")
    # below, which is the expensive part of this whole function: with no
    # single consistent format to assume, pandas falls back to parsing each
    # value's string one at a time through dateutil's flexible parser, and
    # paying that per-value cost on a column that was never a date at all
    # (a free-text "desc"/"notes" column, a category column like "pass"/
    # "run") is pure waste — a real NFL-play-by-play-shaped dataset (~100
    # columns, most of them categorical or free text, only a handful
    # numeric or date) could otherwise spend most of its cleaning time here
    # for nothing. A small sample is enough: a genuine date column has
    # date-shaped digits in essentially every value, so if even a sample
    # doesn't look date-like, the full column won't either.
    _DATE_LIKE_RE = re.compile(r"\d{1,4}[-/.]\d{1,2}[-/.]\d{1,4}|\b\d{8}\b")
    _DATE_SAMPLE_SIZE = 50

    @classmethod
    def _looks_date_like(cls, series_str):
        sample = series_str.head(cls._DATE_SAMPLE_SIZE)
        if sample.empty:
            return False
        return sample.map(lambda v: bool(cls._DATE_LIKE_RE.search(v))).mean() > 0.5

    @classmethod
    def _fix_types(cls, df):
        for col in df.columns:
            series = df[col].dropna()
            if len(series) == 0:
                continue

            # Already a real numeric/bool dtype (which is what pandas' own
            # CSV parser gives an ordinary numeric column straight away) —
            # there's no Rp-prefix/thousands-separator text to normalize and
            # nothing to re-parse, so skip straight to the next column
            # instead of round-tripping every value through str() and back.
            if pd.api.types.is_numeric_dtype(series) or pd.api.types.is_bool_dtype(series):
                continue

            series_str = series.astype(str)
            numeric_ok = cls._coerce_numeric(series_str).notna().mean()
            if numeric_ok > 0.8:
                df[col] = cls._coerce_numeric(df[col])
                continue

            if not cls._looks_date_like(series_str):
                continue  # categorical/free-text column — not worth a full date-parse pass

            date_ok = pd.to_datetime(series_str, errors="coerce", format="mixed", dayfirst=True).notna().mean()
            if date_ok > 0.8:
                df[col] = pd.to_datetime(df[col], errors="coerce", format="mixed", dayfirst=True).dt.strftime("%Y-%m-%d")
        return df

    @classmethod
    def infer_column_types(cls, sample_df):
        """Decides ONCE, from a representative sample of the whole file,
        which columns fix_types should treat as numeric/date — see
        _fix_types_with_plan() below. This exists only for the CHUNKED
        upload path (services/dataset_service.py's handle_upload_stream):
        letting each chunk decide this independently (plain _fix_types()
        above, still used by the non-chunked clean() for Prepare/small
        edits) is exactly what corrupts a big upload's Delta table — a
        chunk with no missing values for a column reads as int64, the next
        chunk with a single blank cell in that same column reads as
        float64, and Delta refuses to append a chunk whose schema doesn't
        match the table's. Returns {column: "numeric"|"date"|None}."""
        decisions = {}
        for col in sample_df.columns:
            series = sample_df[col].dropna()
            if len(series) == 0:
                decisions[col] = None
                continue
            if pd.api.types.is_numeric_dtype(series) or pd.api.types.is_bool_dtype(series):
                decisions[col] = "numeric"
                continue
            series_str = series.astype(str)
            numeric_ok = cls._coerce_numeric(series_str).notna().mean()
            if numeric_ok > 0.8:
                decisions[col] = "numeric"
                continue
            if not cls._looks_date_like(series_str):
                decisions[col] = None
                continue
            date_ok = pd.to_datetime(series_str, errors="coerce", format="mixed", dayfirst=True).notna().mean()
            decisions[col] = "date" if date_ok > 0.8 else None
        return decisions

    @classmethod
    def _fix_types_with_plan(cls, df, plan):
        for col, kind in plan.items():
            if col not in df.columns:
                continue
            if kind == "numeric":
                # Always float64 — never left to come out as int64 on some
                # chunks and float64 on others depending on whether that
                # particular chunk happened to contain a missing value.
                # One fixed dtype for every chunk is the entire point.
                df[col] = cls._coerce_numeric(df[col]).astype("float64")
            elif kind == "date":
                df[col] = pd.to_datetime(df[col], errors="coerce", format="mixed", dayfirst=True).dt.strftime("%Y-%m-%d")
        return df

    def _apply_cleaning_ops(self, df, operations, type_plan=None):
        df = df.copy()

        if "fix_types" in operations:
            # type_plan, when given, is a decision made once for the whole
            # file (see infer_column_types) — every chunk applies that SAME
            # decision instead of re-deciding for itself from only its own
            # rows (see infer_column_types' docstring for why that matters).
            #
            # CHANGED (2026-10-01): moved ahead of trim/normalize_case below,
            # and string_cols is now computed AFTER this runs, not before.
            # The chunked upload path (services/dataset_service.py's
            # _iter_chunks) reads every column as dtype=str so no chunk can
            # silently disagree with another on a column's type (see
            # infer_column_types' docstring) — but that also means, read
            # that way, EVERY column looks like "object" dtype at this
            # point, including ones about to become numeric right below.
            # With the old ordering, trim/normalize_case ran their slow
            # per-cell Python loop over every one of those columns too
            # (e.g. all ~100 columns of a wide dataset) only for fix_types
            # to immediately throw the result away and recompute the
            # column as a number anyway. Doing fix_types first means a
            # column it claims is only ever "object" dtype afterward if it
            # genuinely stayed text — exactly what the non-chunked path
            # already got for free (dtype=None there lets pandas' own
            # parser infer numeric columns before any cleaning op runs).
            df = self._fix_types_with_plan(df, type_plan) if type_plan is not None else self._fix_types(df)

        string_cols = df.select_dtypes(include="object").columns

        # CHANGED (2026-10-01): skip a column here entirely when it has no
        # non-null value AT ALL in this particular chunk (df[c].notna().any()
        # is False) — found while chasing a real-data upload crash
        # ("Cannot cast string ... to value of Float64 type") that traced
        # back to THIS loop, not to fix_types. pandas' Series.apply(), when
        # every single value it touches is missing, does not reliably keep
        # the column's original (string/object) dtype for its result — it
        # can come back float64 instead, silently, even though nothing
        # "numeric" happened. A column a wide, sparse real-world file (e.g.
        # ~100 columns where several are only populated in some rows) can
        # easily be 100% empty in one chunk and have real text in another;
        # type_plan correctly calls such a column non-numeric either way
        # (see infer_column_types), but if THIS chunk's copy gets silently
        # flipped to float64 right here, fill_missing below (which decides
        # numeric-vs-text purely from the live pandas dtype) wrongly
        # 0-fills it instead of "Not Provided"-filling it — producing a
        # chunk whose Arrow type for that column doesn't match every other
        # chunk's, which Delta's schema-merge rejects outright. Skipping an
        # all-null column here (nothing to trim/normalize anyway) leaves
        # its dtype exactly as fix_types/the chunk read decided, so
        # fill_missing's dtype check stays trustworthy for every chunk.
        if "trim" in operations:
            for c in string_cols:
                if df[c].notna().any():
                    df[c] = df[c].apply(lambda v: v.strip() if isinstance(v, str) else v)

        if "normalize_case" in operations:
            def _title(v):
                return re.sub(r"\w\S*", lambda m: m.group()[0].upper() + m.group()[1:].lower(), v)
            for c in string_cols:
                if df[c].notna().any():
                    df[c] = df[c].apply(lambda v: _title(v) if isinstance(v, str) else v)

        if "dedupe" in operations:
            df = df.drop_duplicates()

        if "fill_missing" in operations:
            numeric_cols = df.select_dtypes(include="number").columns
            for c in df.columns:
                if c in numeric_cols:
                    df[c] = df[c].fillna(0)
                else:
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
        testing the actual data — not the column name. Also returns
        unique_count, which chart-compatibility filtering (e.g. keeping Pie
        charts off high-cardinality columns) needs, and which
        compute_auto_summary()/compute_auto_charts() below use to decide
        what's a meaningful metric for THIS dataset's current schema.

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
            # Same Rp/thousands-separator normalization _fix_types() applies
            # before converting — without it, a column that's genuinely
            # numeric but still Rp/dot-formatted (not yet cleaned) would be
            # misclassified as categorical and excluded from KPIs/charts.
            numeric_ok = SparkETLService._coerce_numeric(series_str).notna().mean()
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
        df = self.read_csv_text(csv_text)
        if column not in df.columns:
            raise ValueError("Chosen column is not present in this dataset.")
        counts = df[column].dropna().astype(str).value_counts().head(limit)
        if counts.empty:
            raise ValueError(f"'{column}' has no usable values to chart.")
        return [str(k) for k in counts.index], [int(v) for v in counts.values]

    def monthly_count_trend(self, csv_text, date_col, limit=None):
        df = self.read_csv_text(csv_text)
        if date_col not in df.columns:
            raise ValueError("Chosen column is not present in this dataset.")
        dates = pd.to_datetime(df[date_col], errors="coerce").dropna()
        if dates.empty:
            raise ValueError(f"'{date_col}' doesn't have enough usable dates to chart.")
        monthly = dates.groupby(dates.dt.to_period("M")).size().sort_index()
        return [str(p) for p in monthly.index], [int(v) for v in monthly.values]

    def daily_summary_and_preview(self, csv_text, date_col, target_date, preview_limit=50):
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

    # -- generic (config-free) Summary / Charts, replacing SUMMARY_CONFIG ------
    #
    # These read ONLY the dataset's actual current columns + detected types
    # (numeric/date/categorical) — nothing about "which format is this" is
    # declared anywhere, so this keeps working for a dataset whose schema
    # just grew a column via an approved schema change, or for a brand-new
    # dataset nobody wrote config for. The rules, chosen to mirror what the
    # old hardcoded SUMMARY_CONFIG entries did by hand:
    #
    #   numeric column          -> KPI: Sum + Average; table row: Sum/Avg/Max/Min
    #   date column              -> KPI: date range (earliest/latest); a Line
    #                               chart trend per numeric column, or a
    #                               row-count-over-time trend if there's no
    #                               numeric column at all
    #   categorical, low card.   -> KPI: most common value; table: category
    #   (<=30 unique values)        breakdown; Bar/Pie chart (count, or sum of
    #                               the first numeric column if one exists)
    #   categorical, high card.  -> treated as an identifier (e.g. a ticket_id
    #   (>30 unique, or column      or customer_id column) — excluded from
    #   name is/ends "_id")         KPIs, tables and charts entirely, same
    #                               "don't sum/group by an ID" reasoning the
    #                               old per-format configs applied by hand.
    _LOW_CARDINALITY_LIMIT = 30

    @classmethod
    def _is_identifier_like(cls, col, info):
        name = col.strip().lower()
        if name == "id" or name.endswith("_id"):
            return True
        return info["type"] == "categorical" and info["unique_count"] > cls._LOW_CARDINALITY_LIMIT

    def compute_auto_summary(self, csv_text):
        """Generic replacement for compute_hardcoded_summary(): {"kpis":
        [{"label","value"}], "rows": [{"metric","value"}]}, built purely
        from this dataset's own columns — no config file to keep in sync."""
        df = self.read_csv_text(csv_text)
        types = self.detect_column_types(csv_text)
        kpis = [{"label": "Total Records", "value": f"{len(df):,}"}]
        rows = [{"metric": "Total Records", "value": f"{len(df):,}"}]

        for col, info in types.items():
            if self._is_identifier_like(col, info):
                continue

            if info["type"] == "numeric":
                series = pd.to_numeric(df[col], errors="coerce").dropna()
                if series.empty:
                    continue
                kpis.append({"label": f"Total {col}", "value": f"{series.sum():,.2f}"})
                kpis.append({"label": f"Average {col}", "value": f"{series.mean():,.2f}"})
                rows.append({"metric": f"Total {col}", "value": f"{series.sum():,.2f}"})
                rows.append({"metric": f"Average {col}", "value": f"{series.mean():,.2f}"})
                rows.append({"metric": f"Highest {col}", "value": f"{series.max():,.2f}"})
                rows.append({"metric": f"Lowest {col}", "value": f"{series.min():,.2f}"})

            elif info["type"] == "date":
                dates = pd.to_datetime(df[col], errors="coerce").dropna()
                if dates.empty:
                    continue
                kpis.append({"label": f"{col} Range", "value": f"{dates.min().date()} – {dates.max().date()}"})

            else:  # low-cardinality categorical
                counts = df[col].dropna().astype(str).value_counts()
                if counts.empty:
                    continue
                kpis.append({"label": f"Most Common {col}", "value": f"{counts.index[0]} ({counts.iloc[0]:,})"})
                for value, count in counts.head(5).items():
                    rows.append({"metric": f"{col} — {value}", "value": f"{count:,}"})

        return {"kpis": kpis[:8], "rows": rows}

    def compute_auto_charts(self, csv_text, chart_type):
        """Generic replacement for compute_hardcoded_charts(): every valid
        chart of `chart_type` ("bar"/"pie"/"line") this dataset's current
        schema actually supports, discovered from detect_column_types()
        instead of a per-format config list. Returns a list of {"label",
        "x", "y", "labels", "values"}, same shape the frontend already
        expects from the old hardcoded endpoint."""
        types = self.detect_column_types(csv_text)
        numeric_cols = [c for c, i in types.items() if i["type"] == "numeric" and not self._is_identifier_like(c, i)]
        date_cols = [c for c, i in types.items() if i["type"] == "date"]
        categorical_cols = [
            c for c, i in types.items()
            if i["type"] == "categorical" and not self._is_identifier_like(c, i) and i["unique_count"] > 1
        ]

        charts = []
        if chart_type == "line":
            for date_col in date_cols:
                if numeric_cols:
                    for num_col in numeric_cols:
                        try:
                            labels, values = self.monthly_trend(csv_text, date_col, num_col)
                        except ValueError:
                            continue
                        charts.append({"label": f"{num_col} Over Time", "x": date_col, "y": num_col, "labels": labels, "values": values})
                else:
                    try:
                        labels, values = self.monthly_count_trend(csv_text, date_col)
                    except ValueError:
                        continue
                    charts.append({"label": "Record Count Over Time", "x": date_col, "y": None, "labels": labels, "values": values})
        else:  # bar, pie
            for cat_col in categorical_cols:
                if numeric_cols:
                    num_col = numeric_cols[0]
                    try:
                        labels, values = self.aggregate(csv_text, cat_col, num_col, method="sum")
                    except ValueError:
                        continue
                    charts.append({"label": f"Total {num_col} by {cat_col}", "x": cat_col, "y": num_col, "labels": labels, "values": values})
                try:
                    labels, values = self.category_counts(csv_text, cat_col)
                except ValueError:
                    continue
                charts.append({"label": f"Record Count by {cat_col}", "x": cat_col, "y": None, "labels": labels, "values": values})

        return charts[:8]

    # Automatic merge of an upload into its dataset's continuously-growing
    # Delta table lives in services/dataset_service.py + services/delta_service.py.
