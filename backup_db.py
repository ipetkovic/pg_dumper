#!/usr/bin/env python3
"""Backup Neon PostgreSQL database to S3-compatible storage via pg_dump."""

import argparse
import os
import shutil
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from archiver import PostgresArchiver
from storage import S3Storage


def parse_args():
    parser = argparse.ArgumentParser(
        description="Dump a Neon PostgreSQL database and upload to S3-compatible storage."
    )
    parser.add_argument(
        "--db-url",
        required=True,
        help="PostgreSQL connection URL (e.g. postgresql://user:pass@host/db?sslmode=require)",
    )
    parser.add_argument(
        "--s3-endpoint",
        required=True,
        help="S3-compatible endpoint URL (e.g. https://<id>.r2.cloudflarestorage.com)",
    )
    parser.add_argument(
        "--s3-bucket",
        required=True,
        help="Target S3 bucket name",
    )
    parser.add_argument(
        "--prefix",
        default="backups/",
        help="Key prefix inside the bucket (default: backups/)",
    )
    parser.add_argument(
        "--name",
        default="default",
        help="Backup name. 'default' uses meliori_{timestamp}. Otherwise uses the provided name directly.",
    )
    parser.add_argument(
        "--keep",
        type=int,
        default=None,
        help="Delete dumps older than N days from the bucket prefix",
    )
    return parser.parse_args()


def check_prerequisites():
    """Verify pg_dump is available and S3 credentials are set."""
    if not shutil.which("pg_dump"):
        sys.exit("Error: pg_dump not found on PATH. Install PostgreSQL client tools.")

    missing = [v for v in ("AWS_ACCESS_KEY_ID",
                           "AWS_SECRET_ACCESS_KEY") if not os.environ.get(v)]
    if missing:
        sys.exit(
            f"Error: missing environment variable(s): {', '.join(missing)}")


def cleanup_old_dumps(storage: S3Storage, prefix: str, keep_days: int) -> int:
    """Delete objects under prefix that are older than keep_days."""
    import boto3

    cutoff = datetime.now(timezone.utc) - timedelta(days=keep_days)

    paginator = storage._client.get_paginator("list_objects_v2")
    to_delete = []
    for page in paginator.paginate(Bucket=storage._bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["LastModified"] < cutoff:
                to_delete.append({"Key": obj["Key"]})

    if not to_delete:
        return 0

    # Delete in batches of 1000 (S3 limit)
    deleted = 0
    for i in range(0, len(to_delete), 1000):
        batch = to_delete[i: i + 1000]
        storage._client.delete_objects(
            Bucket=storage._bucket, Delete={"Objects": batch})
        deleted += len(batch)
    return deleted


def main():
    print("Starting backup process...")
    args = parse_args()
    check_prerequisites()

    storage = S3Storage(args.s3_endpoint, args.s3_bucket)
    print("Starting backup process 2...")
    archiver = PostgresArchiver(args.db_url)

    print("Starting backup process 3...")

    if args.name == "default":
        timestamp = datetime.now(ZoneInfo("Europe/Zagreb")).strftime("%Y-%m-%d_%H-%M-%S")
        dump_name = f"meliori_{timestamp}.dump"
    else:
        dump_name = f"{args.name}.dump"

    with tempfile.TemporaryDirectory() as tmpdir:
        dump_path = os.path.join(tmpdir, dump_name)

        print("Running pg_dump...")
        archiver.backup(dump_path)

        file_size = os.path.getsize(dump_path)
        print(f"Dump created: {dump_name} ({file_size / 1024 / 1024:.1f} MB)")

        key = f"{args.prefix}{dump_name}"
        print(f"Uploading to s3://{args.s3_bucket}/{key} ...")
        start = time.monotonic()
        storage.upload(dump_path, key)
        duration = time.monotonic() - start
        print(f"Upload complete in {duration:.1f}s")

    if args.keep is not None:
        print(f"Cleaning up dumps older than {args.keep} days...")
        deleted = cleanup_old_dumps(storage, args.prefix, args.keep)
        print(f"Deleted {deleted} old dump(s)")

    print("Done.")


if __name__ == "__main__":
    main()
