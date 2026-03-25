#!/usr/bin/env python3
"""Restore a Neon PostgreSQL database from an S3-compatible storage dump."""

import argparse
import os
import shutil
import sys
import tempfile

from archiver import PostgresArchiver
from storage import S3Storage


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download a pg_dump from S3-compatible storage and restore it."
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
        help="Source S3 bucket name",
    )
    parser.add_argument(
        "--prefix",
        default="backups/",
        help="Key prefix inside the bucket (default: backups/)",
    )
    parser.add_argument(
        "--dump-name",
        default=None,
        help="Specific dump file name to restore (default: latest)",
    )
    return parser.parse_args()


def check_prerequisites():
    """Verify pg_restore is available and S3 credentials are set."""
    if not shutil.which("pg_restore"):
        sys.exit("Error: pg_restore not found on PATH. Install PostgreSQL client tools.")

    missing = [v for v in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY") if not os.environ.get(v)]
    if missing:
        sys.exit(f"Error: missing environment variable(s): {', '.join(missing)}")


def main():
    args = parse_args()
    check_prerequisites()

    storage = S3Storage(args.s3_endpoint, args.s3_bucket)
    archiver = PostgresArchiver(args.db_url)

    # Determine which dump to restore
    keys = storage.list(args.prefix)
    dump_keys = [k for k in keys if k.endswith(".dump")]

    if not dump_keys:
        sys.exit(f"No dumps found under s3://{args.s3_bucket}/{args.prefix}")

    if args.dump_name:
        target_key = f"{args.prefix}{args.dump_name}"
        if target_key not in dump_keys:
            sys.exit(f"Dump not found: {target_key}\nAvailable: {', '.join(dump_keys)}")
    else:
        target_key = dump_keys[-1]  # latest (sorted alphabetically by timestamp)

    print(f"Restoring from: {target_key}")

    with tempfile.TemporaryDirectory() as tmpdir:
        dump_filename = os.path.basename(target_key)
        local_path = os.path.join(tmpdir, dump_filename)

        print("Downloading dump...")
        storage.download(target_key, local_path)

        file_size = os.path.getsize(local_path)
        print(f"Downloaded: {dump_filename} ({file_size / 1024 / 1024:.1f} MB)")

        print("Restoring database (single transaction)...")
        archiver.restore(local_path)

    print("Done.")


if __name__ == "__main__":
    main()
