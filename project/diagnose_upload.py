"""
Diagnostic script — run this directly on the machine where Flask runs,
from the project root (same folder as app.py / config.py), using the
SAME Python environment (venv) the Flask app uses:

    python diagnose_upload.py

It does NOT touch your real data. It:
  1. Loads your real .env / config.py settings.
  2. Checks Postgres connectivity.
  3. Checks MinIO connectivity + bucket access (boto3, same client the
     app uses).
  4. Writes a tiny THROWAWAY Delta table to a test path in your MinIO
     bucket (master_delta_v1/_diagnose_test/) and reads it back, which
     exercises the EXACT same write_deltalake()/DeltaTable() calls a
     real upload uses — then deletes it.

Whichever step fails, it prints the FULL traceback so we can see
exactly what's broken, instead of guessing from screenshots.
"""
import sys
import traceback

print("=" * 70)
print("STEP 0: importing project config")
print("=" * 70)
try:
    from config import Config
    print("OK — config.py imported")
    print(f"  PG_HOST={Config.PG_HOST} PG_PORT={Config.PG_PORT} PG_DB={Config.PG_DB} PG_USER={Config.PG_USER}")
    print(f"  MINIO_ENDPOINT={Config.MINIO_ENDPOINT} MINIO_BUCKET={Config.MINIO_BUCKET} MINIO_SECURE={Config.MINIO_SECURE}")
except Exception:
    print("FAILED to import config.py — run this from the project root (same folder as app.py)")
    traceback.print_exc()
    sys.exit(1)

print()
print("=" * 70)
print("STEP 1: Postgres connectivity")
print("=" * 70)
try:
    import psycopg2
    conn = psycopg2.connect(
        host=Config.PG_HOST, port=Config.PG_PORT, dbname=Config.PG_DB,
        user=Config.PG_USER, password=Config.PG_PASSWORD, connect_timeout=5,
    )
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        cur.fetchone()
    conn.close()
    print("OK — connected to Postgres and ran a query")
except Exception:
    print("FAILED — cannot reach/query Postgres with these settings")
    traceback.print_exc()

print()
print("=" * 70)
print("STEP 2: MinIO connectivity (boto3, same client shape as the app)")
print("=" * 70)
bucket_ok = False
try:
    import boto3
    from botocore.client import Config as BotoConfig

    client = boto3.client(
        "s3",
        endpoint_url=Config.MINIO_ENDPOINT,
        aws_access_key_id=Config.MINIO_ACCESS_KEY,
        aws_secret_access_key=Config.MINIO_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )
    client.head_bucket(Bucket=Config.MINIO_BUCKET)
    print(f"OK — bucket '{Config.MINIO_BUCKET}' exists and is reachable")
    bucket_ok = True

    test_key = "_diagnose_test/ping.txt"
    client.put_object(Bucket=Config.MINIO_BUCKET, Key=test_key, Body=b"ping")
    print(f"OK — wrote a test object to s3://{Config.MINIO_BUCKET}/{test_key}")
    client.delete_object(Bucket=Config.MINIO_BUCKET, Key=test_key)
    print("OK — deleted the test object")
except Exception:
    print("FAILED — cannot reach/write to MinIO with these settings")
    traceback.print_exc()

print()
print("=" * 70)
print("STEP 3: real write_deltalake() + DeltaTable() round trip (the exact")
print("         calls a real upload makes) against a throwaway test path")
print("=" * 70)
if not bucket_ok:
    print("SKIPPED — step 2 (MinIO) failed, so this would fail too for the same reason")
else:
    try:
        import pandas as pd
        import pyarrow as pa
        from deltalake import DeltaTable, write_deltalake

        test_uri = f"s3://{Config.MINIO_BUCKET}/_diagnose_test/delta_roundtrip/"
        storage_options = {
            "AWS_ENDPOINT_URL": Config.MINIO_ENDPOINT,
            "AWS_ACCESS_KEY_ID": Config.MINIO_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": Config.MINIO_SECRET_KEY,
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "false" if Config.MINIO_SECURE else "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }

        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"], "amount": [1.5, 2.5, 3.5]})
        write_deltalake(
            test_uri, pa.Table.from_pandas(df, preserve_index=False),
            mode="overwrite", schema_mode="overwrite", storage_options=storage_options,
        )
        print("OK — write_deltalake() committed successfully")

        dt = DeltaTable(test_uri, storage_options=storage_options)
        row_count = dt.to_pyarrow_dataset().count_rows()
        print(f"OK — read the table back, row_count={row_count} (expected 3)")
        if row_count != 3:
            print("!! row_count mismatch — the write silently lost rows, this is the bug")

        import deltalake
        print(f"\ndeltalake package version: {deltalake.__version__}")
        import pyarrow
        print(f"pyarrow package version: {pyarrow.__version__}")

        # cleanup
        import boto3
        client = boto3.client(
            "s3", endpoint_url=Config.MINIO_ENDPOINT,
            aws_access_key_id=Config.MINIO_ACCESS_KEY, aws_secret_access_key=Config.MINIO_SECRET_KEY,
        )
        resp = client.list_objects_v2(Bucket=Config.MINIO_BUCKET, Prefix="_diagnose_test/")
        for obj in resp.get("Contents", []):
            client.delete_object(Bucket=Config.MINIO_BUCKET, Key=obj["Key"])
        print("OK — cleaned up test objects")

    except Exception:
        print("FAILED — this is very likely the same thing breaking your real uploads")
        traceback.print_exc()

print()
print("=" * 70)
print("DONE — copy everything above (especially any FAILED section's")
print("traceback) and send it back.")
print("=" * 70)