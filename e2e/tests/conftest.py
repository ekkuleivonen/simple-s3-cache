from __future__ import annotations

# pyright: reportMissingImports=false

import os
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
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
    access_key_id: str | None = field(repr=False)
    secret_access_key: str | None = field(repr=False)
    session_token: str | None = field(repr=False)


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
    return _s3_client(e2e_config, e2e_config.endpoint_url)


@pytest.fixture(scope="session")
def cache_endpoint(e2e_config: E2EConfig, tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    port = _free_port()
    endpoint = f"http://127.0.0.1:{port}"
    work_dir = tmp_path_factory.mktemp("simple-s3-cache")
    config_path = work_dir / "simple-s3-cache.yaml"
    cache_path = work_dir / "cache"
    upstream_endpoint = e2e_config.endpoint_url or _aws_s3_endpoint(e2e_config.region)

    config_path.write_text(
        "\n".join(
            [
                f'listen: "127.0.0.1:{port}"',
                "upstream:",
                f"  endpoint: {upstream_endpoint}",
                f"  region: {e2e_config.region}",
                "cache:",
                f"  path: {cache_path}",
                "  max_size: 1GB",
                "  page_size: 4MB",
                "",
            ]
        ),
        encoding="utf-8",
    )

    process = subprocess.Popen(
        [
            "go",
            "run",
            "-ldflags=-linkmode=external",
            "./cmd/simple-s3-cache",
            "-config",
            str(config_path),
        ],
        cwd=REPO_ROOT,
        env=_proxy_env(e2e_config),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        _wait_for_healthz(endpoint, process)
        yield endpoint
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


@pytest.fixture
def cache_s3_client(e2e_config: E2EConfig, cache_endpoint: str):
    return _s3_client(e2e_config, cache_endpoint)


def _s3_client(e2e_config: E2EConfig, endpoint_url: str | None):
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
        endpoint_url=endpoint_url,
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


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_healthz(endpoint: str, process: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + 20
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        if process.poll() is not None:
            output = process.stdout.read() if process.stdout else ""
            pytest.fail(f"simple-s3-cache exited before becoming ready:\n{output}")

        try:
            with urllib.request.urlopen(f"{endpoint}/healthz", timeout=1) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = exc

        time.sleep(0.1)

    output = process.stdout.read() if process.stdout else ""
    pytest.fail(f"simple-s3-cache did not become ready: {last_error}\n{output}")


def _proxy_env(e2e_config: E2EConfig) -> dict[str, str]:
    env = os.environ.copy()
    env["AWS_REGION"] = e2e_config.region
    env["AWS_DEFAULT_REGION"] = e2e_config.region
    if e2e_config.access_key_id:
        env["AWS_ACCESS_KEY_ID"] = e2e_config.access_key_id
    if e2e_config.secret_access_key:
        env["AWS_SECRET_ACCESS_KEY"] = e2e_config.secret_access_key
    if e2e_config.session_token:
        env["AWS_SESSION_TOKEN"] = e2e_config.session_token
    return env


def _aws_s3_endpoint(region: str) -> str:
    if region == "us-east-1":
        return "https://s3.amazonaws.com"
    return f"https://s3.{region}.amazonaws.com"


def _chunks[T](items: list[T], size: int) -> Iterator[list[T]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]
