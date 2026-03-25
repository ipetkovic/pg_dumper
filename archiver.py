#!/usr/bin/env python3
"""Database archiver abstraction for backup/restore operations."""

import subprocess
import sys
from abc import ABC, abstractmethod


class DatabaseArchiver(ABC):
    @abstractmethod
    def backup(self, dest_path: str) -> None: ...

    @abstractmethod
    def restore(self, dump_path: str) -> None: ...


class PostgresArchiver(DatabaseArchiver):
    def __init__(self, db_url: str):
        self._db_url = db_url

    def backup(self, dest_path: str) -> None:
        result = subprocess.run(
            ["pg_dump", "-Fc", "-f", dest_path, self._db_url],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            sys.exit(f"pg_dump failed (exit {result.returncode}):\n{result.stderr}")

    def restore(self, dump_path: str) -> None:
        result = subprocess.run(
            [
                "pg_restore",
                "--clean",
                "--if-exists",
                "--no-owner",
                "--no-privileges",
                "--single-transaction",
                "-d",
                self._db_url,
                dump_path,
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            sys.exit(f"pg_restore failed (exit {result.returncode}):\n{result.stderr}")
