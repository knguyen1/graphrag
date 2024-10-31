# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""Tests for `S3PipelineStorage` module."""

import re
from collections.abc import Generator
from typing import Any

import boto3
import pytest
from moto import mock_aws

from graphrag.index.storage import S3PipelineStorage

BUCKET_NAME = "test-bucket"


@pytest.fixture
def s3_storage() -> Generator[S3PipelineStorage, Any, Any]:
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=BUCKET_NAME)
        yield S3PipelineStorage(bucket_name=BUCKET_NAME)


@pytest.mark.asyncio
async def test_find(s3_storage: S3PipelineStorage):
    s3_storage.s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key="tests/fixtures/text/input/dulce.txt",
        Body="Sample content",
    )

    items = list(
        s3_storage.find(
            base_dir="tests/fixtures/text/input", file_pattern=re.compile(r".*\.txt$")
        )
    )

    assert items == [("tests/fixtures/text/input/dulce.txt", {})]

    output = await s3_storage.get("tests/fixtures/text/input/dulce.txt")
    assert output == "Sample content"


@pytest.mark.asyncio
async def test_set_and_get(s3_storage: S3PipelineStorage):
    await s3_storage.set("test-file.txt", "Test content")
    output = await s3_storage.get("test-file.txt")
    assert output == "Test content"

    # Check that setting and getting bytes works as expected
    await s3_storage.set("test-bytes-file.txt", b"Bytes content")
    output_bytes = await s3_storage.get("test-bytes-file.txt", as_bytes=True)
    assert output_bytes == b"Bytes content"


@pytest.mark.asyncio
async def test_has(s3_storage: S3PipelineStorage):
    await s3_storage.set("exists-file.txt", "Exists content")
    assert await s3_storage.has("exists-file.txt") is True
    assert await s3_storage.has("non-existent-file.txt") is False


@pytest.mark.asyncio
async def test_delete(s3_storage: S3PipelineStorage):
    await s3_storage.set("delete-me.txt", "Delete content")
    assert await s3_storage.has("delete-me.txt") is True
    await s3_storage.delete("delete-me.txt")
    assert await s3_storage.has("delete-me.txt") is False


@pytest.mark.asyncio
async def test_clear(s3_storage: S3PipelineStorage):
    await s3_storage.set("file1.txt", "Content 1")
    await s3_storage.set("file2.txt", "Content 2")
    assert len(s3_storage.keys()) == 2
    await s3_storage.clear()
    assert len(s3_storage.keys()) == 0


@pytest.mark.asyncio
async def test_keys(s3_storage: S3PipelineStorage):
    await s3_storage.set("file1.txt", "Content 1")
    await s3_storage.set("file2.txt", "Content 2")
    keys = s3_storage.keys()
    assert "file1.txt" in keys
    assert "file2.txt" in keys
    assert len(keys) == 2


@pytest.mark.asyncio
async def test_find_with_file_filter(s3_storage: S3PipelineStorage):
    await s3_storage.set("dir1/file1.txt", "Content 1")
    await s3_storage.set("dir2/file2.txt", "Content 2")
    await s3_storage.set("dir1/file3.log", "Log Content")

    # Now use 'Key' to match against the S3 object's actual structure
    items = list(
        s3_storage.find(
            base_dir="dir1",
            file_pattern=re.compile(r"(?P<Key>.*\.txt)$"),
            file_filter={"Key": r"dir1/file1\.txt"},  # Regex pattern as value
        )
    )

    assert items == [("dir1/file1.txt", {"Key": "dir1/file1.txt"})]
