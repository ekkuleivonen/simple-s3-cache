from __future__ import annotations

# pyright: reportMissingImports=false

import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import boto3
import pytest
from botocore.config import Config as BotoConfig
from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[2]
E2E_ROOT = Path(__file__).resolve().parents[1]

load_dotenv(REPO_ROOT / ".env", override=False)
load_dotenv(E2E_ROOT / ".env", override=False)


@dataclass(frozen=True)
class E2EConfig:
    bucket: str
    endpoint_url: str | None
    region: str
    prefix: str
    access_key_id: str | None
    secret_access_key: str | None
    session_token: str | None


@pytest.fixture(scope="session")
def e2e_config() -> E2EConfig:
    bucket = _first_env("S3CACHE_S3_BUCKET", "S3_BUCKET", "AWS_BUCKET")
    if not bucket:
        pytest.fail("set S3CACHE_S3_BUCKET or S3_BUCKET in the environment/.env")

    base_prefix = _first_env("S3CACHE_E2E_PREFIX") or "simple-s3-cache-e2e"
    run_id = uuid.uuid4().hex

    return E2EConfig(
        bucket=bucket,
        endpoint_url=_first_env(
            "S3CACHE_S3_ENDPOINT_URL",
            "S3_ENDPOINT_URL",
            "S3_ENDPOINT",
            "AWS_ENDPOINT_URL_S3",
            "AWS_ENDPOINT_URL",
            "MINIO_ENDPOINT_URL",
            "MINIO_ENDPOINT",
        ),
        region=_first_env("S3CACHE_S3_REGION", "AWS_REGION", "AWS_DEFAULT_REGION") or "us-east-1",
        prefix=f"{base_prefix.rstrip('/')}/{run_id}",
        access_key_id=_first_env(
            "S3CACHE_S3_ACCESS_KEY_ID",
            "S3_ACCESS_KEY_ID",
            "S3_ACCESS_KEY",
            "AWS_ACCESS_KEY_ID",
            "MINIO_ACCESS_KEY",
            "MINIO_ROOT_USER",
        ),
        secret_access_key=_first_env(
            "S3CACHE_S3_SECRET_ACCESS_KEY",
            "S3_SECRET_ACCESS_KEY",
            "S3_SECRET_KEY",
            "AWS_SECRET_ACCESS_KEY",
            "MINIO_SECRET_KEY",
            "MINIO_ROOT_PASSWORD",
        ),
        session_token=_first_env("S3CACHE_S3_SESSION_TOKEN", "AWS_SESSION_TOKEN"),
    )


@pytest.fixture(scope="session")
def s3_client(e2e_config: E2EConfig):
    session = boto3.session.Session(
        aws_access_key_id=e2e_config.access_key_id,
        aws_secret_access_key=e2e_config.secret_access_key,
        aws_session_token=e2e_config.session_token,
        region_name=e2e_config.region,
    )
    if session.get_credentials() is None:
        pytest.fail(
            "unable to locate S3 credentials; set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
            "or one of the S3CACHE_S3/S3/MINIO credential aliases"
        )

    return session.client(
        "s3",
        endpoint_url=e2e_config.endpoint_url,
        config=BotoConfig(
            retries={"max_attempts": 3, "mode": "standard"},
            s3={"addressing_style": "path"},
        ),
    )


@pytest.fixture(scope="session", autouse=True)
def cleanup_e2e_prefix(e2e_config: E2EConfig, s3_client) -> Iterator[None]:
    yield

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=e2e_config.bucket, Prefix=e2e_config.prefix + "/")

    for page in pages:
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        for batch in _chunks(objects, 1000):
            s3_client.delete_objects(Bucket=e2e_config.bucket, Delete={"Objects": batch})


@pytest.fixture
def object_key(e2e_config: E2EConfig) -> str:
    return f"{e2e_config.prefix}/objects/{uuid.uuid4().hex}.bin"


def _first_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def _chunks[T](items: list[T], size: int) -> Iterator[list[T]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]
