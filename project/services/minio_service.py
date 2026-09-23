"""
MinIO service.

This is the only module in the whole application allowed to import boto3 or
know the MinIO endpoint/credentials. Routes and the frontend never touch
MinIO directly — everything goes through the methods below.
"""
import io

import boto3
import pandas as pd
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError


class MinioService:
    def __init__(self, app_config):
        self.bucket = app_config.MINIO_BUCKET
        self._client = boto3.client(
            "s3",
            endpoint_url=app_config.MINIO_ENDPOINT,
            aws_access_key_id=app_config.MINIO_ACCESS_KEY,
            aws_secret_access_key=app_config.MINIO_SECRET_KEY,
            config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
            region_name="us-east-1",
        )
        self._ensure_bucket()

    def _ensure_bucket(self):
        try:
            self._client.head_bucket(Bucket=self.bucket)
        except ClientError:
            try:
                self._client.create_bucket(Bucket=self.bucket)
            except ClientError:
                # Bucket may already exist / be created by another worker — non-fatal.
                pass

    def health_check(self):
        self._client.list_objects_v2(Bucket=self.bucket, MaxKeys=1)
        return True

    def list_all_objects(self, prefix):
        objects = []
        token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            res = self._client.list_objects_v2(**kwargs)
            objects.extend(res.get("Contents", []))
            token = res.get("NextContinuationToken") if res.get("IsTruncated") else None
            if not token:
                break
        return objects

    def get_object_text(self, key):
        res = self._client.get_object(Bucket=self.bucket, Key=key)
        return res["Body"].read().decode("utf-8")

    def get_object_bytes(self, key):
        res = self._client.get_object(Bucket=self.bucket, Key=key)
        return res["Body"].read()

    def put_object_text(self, key, text, content_type="text/plain"):
        self._client.put_object(
            Bucket=self.bucket, Key=key,
            Body=text.encode("utf-8"), ContentType=content_type,
        )

    def put_object_bytes(self, key, data, content_type="application/octet-stream"):
        self._client.put_object(
            Bucket=self.bucket, Key=key,
            Body=io.BytesIO(data) if not isinstance(data, (bytes, bytearray)) else data,
            ContentType=content_type,
        )

    def put_object_fileobj(self, key, fileobj, content_type="text/csv"):
        self._client.upload_fileobj(fileobj, self.bucket, key, ExtraArgs={"ContentType": content_type})

    def put_object_parquet(self, key, df):
        """Bronze uploads ('Other Format', skip_merge) land here instead of
        put_object_text — same one-file-per-upload shape as before, just
        stored as compressed columnar Parquet instead of CSV text. Master
        datasets do NOT use this; they go through DeltaService instead,
        since they need the Delta transaction log on top of plain Parquet."""
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)
        self._client.put_object(
            Bucket=self.bucket, Key=key,
            Body=buffer.getvalue(), ContentType="application/octet-stream",
        )

    def get_object_parquet(self, key):
        data = self.get_object_bytes(key)
        return pd.read_parquet(io.BytesIO(data), engine="pyarrow")

    def delete_object(self, key):
        self._client.delete_object(Bucket=self.bucket, Key=key)

    def delete_prefix(self, prefix):
        """Deletes every object under `prefix` — used to drop a whole Delta
        table (data files + _delta_log) in one call, since those aren't a
        single object key the way a CSV/Parquet dataset is."""
        objects = self.list_all_objects(prefix)
        if not objects:
            return
        # delete_objects takes at most 1000 keys per call.
        for i in range(0, len(objects), 1000):
            batch = objects[i:i + 1000]
            self._client.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": [{"Key": o["Key"]} for o in batch]},
            )

    def object_exists(self, key):
        try:
            self._client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False
