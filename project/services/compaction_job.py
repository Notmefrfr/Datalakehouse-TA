"""
Small-file compaction, scheduled.

Every upload to a Master dataset lands as its own small Parquet file (see
services/delta_service.py) — great for avoiding a full-table rewrite on
every upload, bad for query/storage efficiency once hundreds of them pile
up. This module is the fix: a background job that periodically checks
every Master format and, once it's gone Config.COMPACT_INTERVAL_DAYS since
its last compaction (or has never been compacted), rewrites its small
files into fewer ones sized up to Config.COMPACT_TARGET_FILE_SIZE_MB.

No admin-facing "compact now" button exists yet — this scheduled check is
the only trigger today. A manual one is a reasonable future addition (the
open question being where it'd live, since admins only see this app's UI,
never MinIO directly) but isn't needed yet, so it's deliberately left out.
"""
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.background import BackgroundScheduler


def _run_due_compactions(app, catalog, postgres, delta):
    with app.app_context():
        interval = timedelta(days=app.config["APP_CONFIG"].COMPACT_INTERVAL_DAYS)
        now = datetime.now(timezone.utc)

        for name, _updated_at in postgres.list_metadata_names("Master"):
            format_key = name
            last_compacted = postgres.get_last_compacted(format_key)
            if last_compacted is not None and (now - last_compacted) < interval:
                continue  # not due yet

            # Non-blocking: if another replica/worker is already compacting
            # this exact format (or already got here first this cycle),
            # skip it rather than wait — it'll be picked up next check.
            with postgres.try_advisory_lock(f"compact:{format_key}") as acquired:
                if not acquired:
                    continue
                try:
                    if not delta.exists(format_key):
                        continue
                    files_before = delta.file_count(format_key)
                    bytes_before = delta.table_size_bytes(format_key)
                    delta.compact(format_key)
                    files_after = delta.file_count(format_key)
                    bytes_after = delta.table_size_bytes(format_key)
                    postgres.record_compaction(format_key, files_before, files_after, bytes_before, bytes_after)
                    postgres.log_action(
                        None, "system", "compact_dataset", format_key,
                        {"files_before": files_before, "files_after": files_after,
                         "bytes_before": bytes_before, "bytes_after": bytes_after},
                    )
                except Exception:
                    app.logger.exception("Compaction failed for format '%s'", format_key)


def start_compaction_scheduler(app, catalog, postgres, delta):
    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(
        lambda: _run_due_compactions(app, catalog, postgres, delta),
        trigger="interval",
        hours=app.config["APP_CONFIG"].COMPACT_CHECK_INTERVAL_HOURS,
        next_run_time=datetime.now(timezone.utc) + timedelta(minutes=1),  # small delay so app startup isn't blocked
        id="delta_compaction_check",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.start()
    return scheduler
