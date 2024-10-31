# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""A module containing the `S3PipelineStorage` model."""

import logging
import re
from typing import Any

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from datashaper import Progress

from graphrag.index.storage.pipeline_storage import PipelineStorage
from graphrag.logging.types import ProgressReporter

logger = logging.getLogger(__name__)


class S3PipelineStorage(PipelineStorage):
    """S3 storage class definition."""

    bucket_name: str
    prefix: str | None
    bucket_paginator: str
    encoding: str = "utf-8"
    s3_client: BaseClient

    def __init__(
        self,
        bucket_name: str,
        prefix: str | None = "",
        bucket_paginator: str = "list_objects_v2",
        encoding: str = "utf-8",
    ):
        """Instantiate a new instance of the `S3PipelineStorage` class."""
        self.bucket_name = bucket_name
        self.prefix = prefix or ""
        self.bucket_paginator = bucket_paginator
        self.encoding = encoding
        self.s3_client = boto3.client("s3")

    def find(
        self,
        file_pattern: re.Pattern[str],
        base_dir: str | None = None,
        progress: ProgressReporter | None = None,
        file_filter: dict[str, Any] | None = None,
        max_count: int = -1,
    ):
        """Find files in the storage using a filter pattern and/or custom filter function."""

        def item_filter(item: dict[str, Any]) -> bool:
            if file_filter is None:
                return True

            return all(re.match(value, item[key]) for key, value in file_filter.items())

        search_prefix = f"{self.prefix}/{base_dir or ''}".strip("/")
        logger.info(
            "search %s for files matching %s", search_prefix, file_pattern.pattern
        )

        paginator = self.s3_client.get_paginator(self.bucket_paginator)
        page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix)

        num_loaded = 0
        num_filtered = 0
        num_total = 0

        for page in page_iterator:
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                num_total += 1
                key = obj["Key"]
                match = file_pattern.match(key)
                if match:
                    group = match.groupdict()
                    if item_filter(group):
                        filename = key.replace(self.prefix, "").lstrip("/")
                        yield (filename, group)
                        num_loaded += 1
                        if max_count > 0 and num_loaded >= max_count:
                            return
                    else:
                        num_filtered += 1
                else:
                    num_filtered += 1
                if progress is not None:
                    progress(
                        _create_progress_status(num_loaded, num_filtered, num_total)
                    )

    async def get(
        self, key: str, as_bytes: bool | None = False, encoding: str | None = None
    ) -> Any:
        """Get method definition."""
        s3_key = f"{self.prefix}/{key}".strip("/")

        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            content = response["Body"].read()
            if as_bytes:
                return content
            return content.decode(encoding or self.encoding)
        except ClientError:
            logger.exception(
                "Failed to get object %s from bucket %s", s3_key, self.bucket_name
            )
            return None

    async def set(self, key: str, value: Any, encoding: str | None = None) -> None:
        """Set method definition."""
        s3_key = f"{self.prefix}/{key}".strip("/")
        is_bytes = isinstance(value, bytes)
        content = value if is_bytes else value.encode(encoding or self.encoding)

        try:
            self.s3_client.put_object(Bucket=self.bucket_name, Key=s3_key, Body=content)
        except ClientError:
            logger.exception(
                "Failed to put object %s from bucket %s", s3_key, self.bucket_name
            )

    async def has(self, key: str) -> bool:
        """Has method definition."""
        s3_key = f"{self.prefix}/{key}".strip("/")

        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
        except ClientError:
            return False
        else:
            return True

    async def delete(self, key: str) -> None:
        """Delete method definition."""
        s3_key = f"{self.prefix}/{key}".strip("/")

        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
        except ClientError:
            logger.exception(
                "Failed to delete object %s from bucket %s", s3_key, self.bucket_name
            )

    async def clear(self) -> None:
        """Clear method definition."""
        paginator = self.s3_client.get_paginator(self.bucket_paginator)
        page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix)

        for page in page_iterator:
            if "Contents" not in page:
                continue

            delete_keys = [{"Key": obj["Key"]} for obj in page["Contents"]]
            if delete_keys:
                try:
                    self.s3_client.delete_objects(
                        Bucket=self.bucket_name, Delete={"Objects": delete_keys}
                    )
                except ClientError:
                    logger.exception(
                        "Failed to delete objects from bucket %s and prefix %s",
                        self.bucket_name,
                        self.prefix,
                    )

    def child(self, name: str | None) -> PipelineStorage:
        """Create a child storage instance."""
        if name is None:
            return self
        return S3PipelineStorage(self.bucket_name, f"{self.prefix}/{name}".strip("/"))

    def keys(self) -> list[str]:
        """Return the keys in the storage."""
        paginator = self.s3_client.get_paginator(self.bucket_paginator)
        page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix)

        return [
            obj["Key"].replace(self.prefix, "").lstrip("/")
            for page in page_iterator
            if "Contents" in page
            for obj in page["Contents"]
        ]


def create_s3_file_storage(
    bucket_name: str,
    prefix: str | None = None,
    bucket_paginator: str = "list_objects_v2",
    encoding: str = "utf-8",
) -> PipelineStorage:
    """Create a s3 based storage."""
    logger.info("Creating file storage at %s/%s", bucket_name, prefix)
    return S3PipelineStorage(bucket_name, prefix, bucket_paginator, encoding)


def _create_progress_status(
    num_loaded: int, num_filtered: int, num_total: int
) -> Progress:
    return Progress(
        total_items=num_total,
        completed_items=num_loaded + num_filtered,
        description=f"{num_loaded} files loaded ({num_filtered} filtered)",
    )
