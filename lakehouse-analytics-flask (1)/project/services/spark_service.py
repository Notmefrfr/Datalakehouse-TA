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

    # -- validation --------------------------------------------------------------

    @staticmethod
    def validate_upload(csv_text, required_columns, delimiter=","):
        checks = []
        lines = csv_text.splitlines()
        if not lines or not lines[0].strip():
            return {"valid": False, "checks": [{"ok": False, "message": "File is empty."}]}

        header = [h.strip() for h in lines[0].split(delimiter)]
        if len(header) == 1 and delimiter != ",":
            checks.append({"ok": False, "message": f"Couldn't split the header using '{delimiter}' as the delimiter — check you picked the right one."})

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
            sdf = sdf.na.fill(0).na.fill("")
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
            date_ok = pd.to_datetime(series_str, errors="coerce").notna().mean()
            if date_ok > 0.8:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
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
                    df[c] = df[c].fillna("")

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

    def aggregate(self, csv_text, x_col, y_col, limit=20):
        df = self.read_csv_text(csv_text)
        if x_col not in df.columns or y_col not in df.columns:
            raise ValueError("Chosen columns are not present in this dataset.")
        y_numeric = pd.to_numeric(df[y_col], errors="coerce")
        work = pd.DataFrame({x_col: df[x_col], "__value__": y_numeric}).dropna(subset=["__value__"])
        if work.empty:
            raise ValueError(f"'{y_col}' doesn't have usable numbers to chart.")
        totals = work.groupby(x_col)["__value__"].sum().sort_values(ascending=False).head(limit)
        return [str(k) for k in totals.index], [float(v) for v in totals.values]

    # -- automatic merge (upload -> one continuously-growing master dataset) --

    def merge_into_master(self, existing_csv_text, new_csv_text, dedupe_mode, key_columns, delimiter=","):
        """
        Appends/merges a freshly-uploaded batch into a format's existing master
        dataset (or creates it, if existing_csv_text is None — first upload for
        that format). This is THE automatic-merge logic: same format, uploaded
        again, becomes rows in the SAME file instead of a new dataset.

        dedupe_mode:
          "remove"      — new rows matching an existing key are skipped
          "replace"      — new rows matching an existing key overwrite it (upsert)
          "keep"/"append_raw" — no dedup at all, straight concatenation

        `delimiter` applies only to `new_csv_text` — the incoming upload.
        `existing_csv_text` is always read as canonical comma-separated,
        since to_csv_text() never writes anything else, regardless of what
        delimiter the original upload that created the master used.

        Returns (merged_csv_text, stats) where stats has rows_uploaded,
        rows_added, duplicates_handled, total_rows — exactly the fields the
        Upload Summary panel displays.

        Note: unlike clean(), this always runs in pandas regardless of
        SPARK_MODE. A real Spark DataFrame version (upsert via anti-join +
        union) would be the natural next step if you're merging datasets too
        large to hold in memory on the Flask host — the local/pandas path
        here is what SPARK_MODE=local already uses everywhere else, so it's
        consistent with today's default, just not yet cluster-accelerated.
        """
        new_df = self.read_csv_text(new_csv_text, delimiter=delimiter)
        rows_uploaded = len(new_df)

        if existing_csv_text is None:
            merged = new_df
            rows_added = rows_uploaded
            duplicates_handled = 0
        else:
            existing_df = self.read_csv_text(existing_csv_text)
            # Union of columns (existing ∪ new), existing columns first, so a
            # slightly different-but-compatible upload doesn't lose data.
            all_cols = list(existing_df.columns) + [c for c in new_df.columns if c not in existing_df.columns]
            existing_df = existing_df.reindex(columns=all_cols)
            new_df = new_df.reindex(columns=all_cols)

            if dedupe_mode in ("remove", "replace") and key_columns:
                missing_key_cols = [c for c in key_columns if c not in all_cols]
                if missing_key_cols:
                    raise ValueError(f"Duplicate key column(s) not found: {', '.join(missing_key_cols)}")

                existing_keys = existing_df[key_columns].astype(str).agg("||".join, axis=1)

                if dedupe_mode == "remove":
                    new_keys = new_df[key_columns].astype(str).agg("||".join, axis=1)
                    is_dup = new_keys.isin(set(existing_keys))
                    duplicates_handled = int(is_dup.sum())
                    to_append = new_df[~is_dup]
                    merged = pd.concat([existing_df, to_append], ignore_index=True)
                    rows_added = len(to_append)
                else:  # replace (upsert)
                    existing_df = existing_df.copy()
                    existing_df["__key__"] = existing_keys
                    new_df = new_df.copy()
                    new_df["__key__"] = new_df[key_columns].astype(str).agg("||".join, axis=1)

                    dup_mask = existing_df["__key__"].isin(set(new_df["__key__"]))
                    duplicates_handled = int(dup_mask.sum())
                    kept_existing = existing_df[~dup_mask].drop(columns="__key__")
                    incoming = new_df.drop(columns="__key__")
                    merged = pd.concat([kept_existing, incoming], ignore_index=True)
                    rows_added = len(new_df) - duplicates_handled
            else:
                # "keep" / "append_raw": no dedup at all
                merged = pd.concat([existing_df, new_df], ignore_index=True)
                rows_added = rows_uploaded
                duplicates_handled = 0

        stats = {
            "rows_uploaded": rows_uploaded,
            "rows_added": rows_added,
            "duplicates_handled": duplicates_handled,
            "total_rows": len(merged),
        }
        return self.to_csv_text(merged), stats
