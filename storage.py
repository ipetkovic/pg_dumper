#!/usr/bin/env python3
"""Storage backend abstraction for backup/restore operations."""

from abc import ABC, abstractmethod


class StorageBackend(ABC):
    @abstractmethod
    def upload(self, local_path: str, key: str) -> None: ...

    @abstractmethod
    def list(self, prefix: str) -> list[str]: ...

    @abstractmethod
    def download(self, key: str, local_path: str) -> None: ...


class S3Storage(StorageBackend):
    def __init__(self, endpoint: str, bucket: str):
        import boto3

        self._client = boto3.client("s3", endpoint_url=endpoint)
        self._bucket = bucket

    def upload(self, local_path: str, key: str) -> None:
        self._client.upload_file(local_path, self._bucket, key)

    def list(self, prefix: str) -> list[str]:
        keys: list[str] = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        keys.sort()
        return keys

    def download(self, key: str, local_path: str) -> None:
        self._client.download_file(self._bucket, key, local_path)
