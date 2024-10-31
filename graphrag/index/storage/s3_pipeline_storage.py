# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""A module containing the `S3PipelineStorage` model."""

import logging
import re
from typing import Any

import boto3
from botocore.client import BaseClient
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

    def __init__(self, bucket_name: str, prefix: str | None = "", bucket_paginator: str = "list_objects_v2", encoding: str = "utf-8"):
        """Instantiate a new instance of the `S3PipelineStorage` class."""
        self.bucket_name = bucket_name
        self.prefix = prefix or ""
        self.bucket_paginator = bucket_paginator
        self.encoding = encoding
        self.s3_client = boto3.client("s3")

    def find(self,
             file_pattern: re.Pattern[str],
             base_dir: str | None = None,
             progress: ProgressReporter | None = None,
             file_filter: dict[str, Any] | None = None,
             max_count: int = -1):
        """Find files in the storage using a filter pattern and/or custom filter function."""

        def item_filter(item: dict[str, Any]) -> bool:
            if file_filter is None:
                return True
            
            return all(re.match(value, item[key]) for key, value in file_filter.items())
        
        search_prefix = f"{self.prefix}/{base_dir or ''}".strip("/")
        logger.info("search %s for files matching %s", search_prefix, file_pattern.pattern)

        paginator = self.s3_client.get_paginator(self.bucket_paginator)
        page_iterator = paginator.paginate(Bucket = self.bucket_name, Prefix = self.prefix)

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

def create_s3_file_storage(
    bucket_name:str, prefix: str | None = None, bucket_paginator: str = "list_objects_v2", encoding: str = "utf-8"
) -> PipelineStorage:
    """Create a s3 based storage."""
    logger.info("Creating file storage at %s/%s", bucket_name, prefix)
    return S3PipelineStorage(bucket_name, prefix, bucket_paginator, encoding)

def _create_progress_status(num_loaded: int, num_filtered: int, num_total: int) -> Progress:
    return Progress(
        total_items=num_total,
        completed_items=num_loaded + num_filtered,
        description=f"{num_loaded} files loaded ({num_filtered} filtered)"
    )
