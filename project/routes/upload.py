"""
Upload route.

CHANGED (dynamic-dataset feature, 2026-09-24): full rewrite. There is no
more division/dataset_type picker — the form now sends just a file, plus
an OPTIONAL new_dataset_name (only needed the first time a genuinely new
dataset is created; required only in that case). Validation, delimiter
detection, and the HTML/script-markup rejection are unchanged from before.
What happens after that is entirely delegated to
services/dataset_service.py's handle_upload(), which decides for itself
whether this is a new dataset, a straight append, or a schema change that
needs an Administrator's approval — see that file's module docstring for
the exact rule. dedupe_mode (remove/replace/keep duplicate handling) is
gone: duplicates within a single file are still deduped by AUTO_CLEAN
same as before, but across uploads there's no longer a "how do you want
duplicates handled" choice — matching rows are always MERGEd (see
dataset_service's row-identity docstring section).

CHANGED (row-identity feature, 2026-09-24, second round): re-added an
OPTIONAL `key_columns` form field — a comma-separated list of column
name(s) the uploader explicitly wants to identify "the same row" (single
column or compound key). Left blank, handle_upload() falls back to
whatever key was used last time for that dataset, or that dataset's
first-ever column. This is genuinely optional (unlike before the dynamic-
dataset rewrite, it's no longer a required per-upload dedupe setting) —
most uploads to an existing dataset never need to set it.

CHANGED (big-data streaming, 2026-10-01): the file is no longer read into
one Python string (file.read()) before anything happens to it — that held
the ENTIRE upload in RAM at once, on top of however many further copies
_find_markup/validate_upload/clean() each made of it, which made anything
much bigger than a few hundred MB risky and anything in the GB+ range
basically guaranteed to exhaust memory. Instead, the incoming file is
streamed straight to a temp file on disk (file.save(), not file.read()),
every check below reads from that file rather than a big in-memory string,
and the actual clean+write work (services/dataset_service.py's
handle_upload_stream()) processes it in row-chunks so memory use stays
roughly flat no matter the file size — see Config.MAX_UPLOAD_SIZE_GB,
Config.UPLOAD_CHUNK_ROWS and Config.DEDUPE_GLOBAL_MAX_ROWS in config.py.
"""
import csv
import os
import re
import tempfile

import pandas as pd
from flask import Blueprint, jsonify, request

from routes._common import config, dataset_service, login_required

bp = Blueprint("upload", __name__)

# The only delimiters we know how to canonicalize down to comma. Detection
# below never returns anything outside this set.
DELIMITER_MAP = {"comma": ",", "semicolon": ";", "tab": "\t", "pipe": "|"}
DELIMITER_LABELS = {",": "Comma ( , )", ";": "Semicolon ( ; )", "\t": "Tab", "|": "Pipe ( | )"}

# Applied automatically to every upload, right after validation and before
# anything is matched/written — this is what makes "upload -> already
# clean" true (Bab 2.1's "Bronze: auto-clean saat upload"): nothing raw is
# ever stored for the rest of the app to see.
AUTO_CLEAN_OPERATIONS = ["trim", "normalize_case", "fix_types", "dedupe", "fill_missing"]

# Any HTML tag (<img ...>, <script>, </script>, <svg ...>, etc.) or a
# javascript: URI in a header or cell gets the whole file rejected outright.
_HTML_TAG_RE = re.compile(r"<\s*/?\s*[a-zA-Z!][^>]*>")
_JS_URI_RE = re.compile(r"javascript\s*:", re.IGNORECASE)

# How much of the file is read (not the whole thing) just to sniff its
# delimiter and offer an alternative-delimiter suggestion on failure.
_SNIFF_BYTES = 65536


def _save_upload_to_temp(file):
    """Streams the uploaded file straight to a temp file on disk. Werkzeug's
    FileStorage.save() copies in chunks internally — this never holds the
    whole upload as one Python bytes/str object the way file.read() would,
    so a 50GB upload costs 50GB of disk, not 50GB of RAM."""
    fd, path = tempfile.mkstemp(prefix="lakehouse_upload_", suffix=".csv")
    os.close(fd)
    file.save(path)
    return path


def _cleanup_temp(path):
    try:
        os.remove(path)
    except OSError:
        pass


def _read_sample_lines(path):
    with open(path, "rb") as f:
        raw = f.read(_SNIFF_BYTES)
    text = raw.decode("utf-8", errors="replace")
    lines = text.splitlines()
    if len(raw) >= _SNIFF_BYTES and lines:
        lines = lines[:-1]  # drop a possibly-truncated last line
    return [l for l in lines if l.strip()][:10]


def _detect_delimiter(sample_lines):
    """Auto-detect which of comma/semicolon/tab/pipe a freshly-uploaded file
    uses, so the user never has to pick it themselves. Only ever looks at a
    small sample from the start of the file (see _read_sample_lines) —
    never the whole thing, even for a huge upload."""
    if not sample_lines:
        return ","
    sample = "\n".join(sample_lines)

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters="".join(DELIMITER_MAP.values()))
        if dialect.delimiter in DELIMITER_MAP.values():
            return dialect.delimiter
    except csv.Error:
        pass  # fall through to the frequency-count heuristic below

    header = sample_lines[0]
    counts = {d: header.count(d) for d in DELIMITER_MAP.values()}
    best = max(counts, key=counts.get)
    return best if counts[best] > 0 else ","


def _suggest_alternative_delimiter(sample_lines, current_delimiter):
    """Called only when validation has already failed. With no more
    per-format required_columns to score against, this now just scores by
    'how many columns does each candidate delimiter actually split the
    header into' — a delimiter that produces only 1 column when others
    produce several is the strongest signal something's wrong."""
    if not sample_lines:
        return None
    header_line = sample_lines[0]

    def score(delimiter):
        return len([c for c in header_line.split(delimiter) if c.strip()])

    current_score = score(current_delimiter)
    best_delimiter, best_score = current_delimiter, current_score
    for delimiter in DELIMITER_MAP.values():
        candidate_score = score(delimiter)
        if candidate_score > best_score:
            best_delimiter, best_score = delimiter, candidate_score

    return best_delimiter if best_delimiter != current_delimiter else None


_COMBINED_MARKUP_RE = re.compile(_HTML_TAG_RE.pattern + "|" + _JS_URI_RE.pattern, re.IGNORECASE)
_PRECHECK_BLOCK_BYTES = 1 << 20  # 1MB at a time


def _fast_markup_precheck(path):
    """Answers just "does this file contain ANY HTML tag or javascript: URI
    at all?" with one regex sweep over the file read in large blocks (never
    loaded as one giant string, and never checked cell-by-cell). The
    overwhelming majority of uploads are clean, so this is what lets
    _scan_file skip the much more expensive per-cell localization pass
    (_locate_markup) on every ordinary upload — it only runs on the rare
    file that actually trips this precheck. A 256-byte overlap between
    blocks means a tag split across a block boundary still gets caught."""
    overlap = ""
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        while True:
            block = f.read(_PRECHECK_BLOCK_BYTES)
            if not block:
                return False
            if _COMBINED_MARKUP_RE.search(overlap + block):
                return True
            overlap = block[-256:]


def _locate_markup(path, delimiter, header):
    """Only called after _fast_markup_precheck finds something — pinpoints
    which row(s)/column(s), using pandas' vectorized per-column string
    matching (one regex pass per column per chunk, in pandas' C-level
    string code) instead of a Python for-loop over every individual cell.
    chunk.index keeps counting from the start of the file across chunks
    (pandas doesn't reset it per chunk), so idx + 1 is already the data
    row's 1-based position, matching what the old cell-by-cell scan
    reported."""
    findings = []
    for col_index, cell in enumerate(header):
        if _COMBINED_MARKUP_RE.search(cell):
            findings.append({"location": "Header row", "column": f"column {col_index + 1}"})

    for chunk in pd.read_csv(path, sep=delimiter, dtype=str, keep_default_na=False, chunksize=100_000):
        for col in chunk.columns:
            mask = chunk[col].str.contains(_COMBINED_MARKUP_RE, na=False, regex=True)
            if mask.any():
                for idx in chunk.index[mask]:
                    findings.append({"location": f"Row {idx + 1}", "column": col})
    return findings


def _scan_file(path, delimiter):
    """Structural validation (duplicate columns, at least one data row) plus
    the HTML/script-markup check, over the WHOLE file but without ever
    loading it into memory as one string and without the per-cell Python
    loop the original implementation used (fine for a small file, far too
    slow once a file has hundreds of columns and hundreds of thousands of
    rows — exactly the shape of a realistic "big data" CSV). Header parsing
    and row counting both go through pandas' C CSV parser, and the markup
    scan only pays the expensive per-cell price when _fast_markup_precheck
    actually finds something to localize. Returns (checks, columns,
    data_row_count, markup_findings)."""
    try:
        header = [h.strip() for h in pd.read_csv(path, sep=delimiter, nrows=0, dtype=str).columns]
    except pd.errors.EmptyDataError:
        return [{"ok": False, "message": "File is empty."}], [], 0, []

    if not header or (len(header) == 1 and not header[0].strip()):
        return [{"ok": False, "message": "File is empty."}], [], 0, []

    checks = []
    if len(header) == 1 and delimiter != ",":
        checks.append({
            "ok": False,
            "message": f"Couldn't confidently parse this file's columns using the detected delimiter ('{delimiter}') — the file may be malformed.",
        })

    seen, dupes = set(), set()
    for h in header:
        if h in seen:
            dupes.add(h)
        seen.add(h)
    checks.append(
        {"ok": False, "message": "Duplicate column(s): " + ", ".join(sorted(dupes))} if dupes
        else {"ok": True, "message": "No duplicate columns."}
    )

    # usecols=[0]: the C parser still has to tokenize every row to find the
    # column boundaries (so a malformed row is still caught), but skips
    # converting/materializing the other 99% of columns — several times
    # faster than reading (and dtype=str-converting) every column just to
    # throw the values away and keep a count.
    data_row_count = 0
    for chunk in pd.read_csv(path, sep=delimiter, usecols=[0], chunksize=200_000):
        data_row_count += len(chunk)
    checks.append(
        {"ok": True, "message": f"{data_row_count} data row(s) found."} if data_row_count > 0
        else {"ok": False, "message": "File has a header but no data rows."}
    )

    markup_findings = _locate_markup(path, delimiter, header) if _fast_markup_precheck(path) else []
    return checks, header, data_row_count, markup_findings


def _read_and_validate(path, override_code):
    """Shared by both endpoints below: sniffs the delimiter, scans the file
    once, and folds the markup findings into the check list the exact same
    way validate_upload()/_find_markup() used to. Returns
    (delimiter, checks, columns, valid, markup_findings)."""
    is_override = bool(override_code)
    delimiter = DELIMITER_MAP[override_code] if is_override else _detect_delimiter(_read_sample_lines(path))

    checks, columns, _data_row_count, markup_findings = _scan_file(path, delimiter)
    checks = [{
        "ok": True,
        "message": (
            f"Using {DELIMITER_LABELS.get(delimiter, delimiter)} as selected."
            if is_override else
            f"Detected file format: {DELIMITER_LABELS.get(delimiter, delimiter)}-separated."
        ),
    }] + checks

    valid = all(c["ok"] for c in checks)
    if markup_findings:
        count = len(markup_findings)
        checks.append({
            "ok": False,
            "message": f"{count} cell{'s' if count != 1 else ''} contain HTML or script-like content and were rejected.",
        })
        valid = False
    else:
        checks.append({"ok": True, "message": "No HTML or script-like content in headers or cells."})

    return delimiter, checks, columns, valid, markup_findings


def _delimiter_suggestion(path, current_delimiter):
    suggestion = _suggest_alternative_delimiter(_read_sample_lines(path), current_delimiter)
    return suggestion


@bp.post("/upload/validate")
@login_required
def validate_upload(user):
    file = request.files.get("file")
    if not file:
        return jsonify({"valid": False, "checks": [{"ok": False, "message": "file is required."}]}), 400
    if not file.filename.lower().endswith(".csv"):
        return jsonify({"valid": False, "checks": [{"ok": False, "message": "Only .csv files are accepted."}]}), 400

    override_code = (request.form.get("delimiter_override") or "").strip().lower()
    if override_code and override_code not in DELIMITER_MAP:
        return jsonify({"valid": False, "checks": [{"ok": False, "message": f"Unknown delimiter '{override_code}'."}]}), 400

    path = _save_upload_to_temp(file)
    try:
        delimiter, checks, columns, valid, markup_findings = _read_and_validate(path, override_code)
        result = {"valid": valid, "checks": checks, "columns": columns, "detected_delimiter": delimiter}
        if markup_findings:
            result["markup_findings"] = markup_findings
        if not valid:
            suggestion = _delimiter_suggestion(path, delimiter)
            if suggestion:
                result["delimiter_issue"] = True
                result["suggested_delimiter"] = suggestion
        return jsonify(result)
    finally:
        _cleanup_temp(path)


@bp.post("/upload")
@login_required
def upload(user):
    file = request.files.get("file")
    if not file:
        return jsonify({"error": "file is required."}), 400
    if not file.filename.lower().endswith(".csv"):
        return jsonify({"error": "Only .csv files are accepted."}), 400

    override_code = (request.form.get("delimiter_override") or "").strip().lower()
    if override_code and override_code not in DELIMITER_MAP:
        return jsonify({"error": f"Unknown delimiter '{override_code}'."}), 400

    path = _save_upload_to_temp(file)
    try:
        delimiter, checks, _columns, valid, markup_findings = _read_and_validate(path, override_code)
        if not valid:
            response = {"error": "File failed validation.", "checks": checks}
            if markup_findings:
                response["markup_findings"] = markup_findings
            suggestion = _delimiter_suggestion(path, delimiter)
            if suggestion:
                response["delimiter_issue"] = True
                response["suggested_delimiter"] = suggestion
            return jsonify(response), 400

        new_dataset_name = (request.form.get("new_dataset_name") or "").strip() or None
        key_columns_raw = request.form.get("key_columns", "")
        key_columns = [c.strip() for c in key_columns_raw.split(",") if c.strip()] or None

        try:
            outcome = dataset_service().handle_upload_stream(
                path, delimiter, AUTO_CLEAN_OPERATIONS, new_dataset_name, user, key_columns=key_columns,
            )
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

        outcome["detected_delimiter"] = delimiter
        outcome["ok"] = True
        return jsonify(outcome)
    finally:
        _cleanup_temp(path)
