from __future__ import annotations

# pyright: reportMissingImports=false

import hashlib
import json
import os
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import boto3
import pytest
from botocore.exceptions import ClientError

from conftest import E2EConfig, _proxy_env, _s3_client
from test_proxy_contract import _abort_multipart, _status


pytestmark = pytest.mark.e2e

PAGE_SIZE = 1024 * 1024
LARGE_SIZE = 6 * PAGE_SIZE + 123
PEER_IDS = ["cache-0", "cache-1", "cache-2", "cache-3"]


@dataclass
class LivePeer:
    id: str
    port: int
    endpoint: str
    config_path: Path
    cache_path: Path
    meta_path: Path
    spool_path: Path
    log_path: Path
    process: subprocess.Popen[str] | None = None


class LivePeerCluster:
    def __init__(self, e2e_config: E2EConfig, root: Path):
        self.e2e_config = e2e_config
        self.root = root
        self.peers = [
            LivePeer(
                id=peer_id,
                port=_free_port(),
                endpoint="",
                config_path=root / peer_id / "simple-s3-cache.yaml",
                cache_path=root / peer_id / "cache-bytes",
                meta_path=root / peer_id / "cache-meta",
                spool_path=root / peer_id / "spool",
                log_path=root / peer_id / "simple-s3-cache.log",
            )
            for peer_id in PEER_IDS
        ]
        for peer in self.peers:
            peer.endpoint = f"http://127.0.0.1:{peer.port}"

    def start(self) -> None:
        self._write_configs()
        for peer in self.peers:
            self.start_peer(peer.id)

    def stop(self) -> None:
        for peer in self.peers:
            self.stop_peer(peer.id)

    def restart(self) -> None:
        self.stop()
        for peer in self.peers:
            self.start_peer(peer.id)

    def start_peer(self, peer_id: str) -> None:
        peer = self.peer(peer_id)
        if peer.process is not None and peer.process.poll() is None:
            return
        peer.config_path.parent.mkdir(parents=True, exist_ok=True)
        log = peer.log_path.open("a", encoding="utf-8")
        peer.process = subprocess.Popen(
            [
                "go",
                "run",
                "-ldflags=-linkmode=external",
                "./cmd/simple-s3-cache",
                "-config",
                str(peer.config_path),
            ],
            cwd=Path(__file__).resolve().parents[2],
            env=_proxy_env(self.e2e_config),
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        _wait_for_ready(peer)

    def stop_peer(self, peer_id: str) -> None:
        peer = self.peer(peer_id)
        if peer.process is None:
            return
        try:
            os.killpg(peer.process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        peer.process.terminate()
        try:
            peer.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(peer.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            peer.process.kill()
            peer.process.wait(timeout=10)
        finally:
            peer.process = None

    def peer(self, peer_id: str) -> LivePeer:
        for peer in self.peers:
            if peer.id == peer_id:
                return peer
        raise KeyError(peer_id)

    def client(self, peer_id: str):
        return _s3_client(self.e2e_config, self.peer(peer_id).endpoint)

    def metrics(self, peer_id: str) -> str:
        with urllib.request.urlopen(f"{self.peer(peer_id).endpoint}/metrics", timeout=5) as response:
            return response.read().decode("utf-8")

    def assert_all_ready(self) -> None:
        for peer in self.peers:
            _wait_for_ready(peer)

    def _write_configs(self) -> None:
        upstream_endpoint = self.e2e_config.endpoint_url or _aws_s3_endpoint(self.e2e_config.region)
        peer_yaml = [
            f"  - id: {peer.id}\n    url: {peer.endpoint}"
            for peer in sorted(self.peers, key=lambda item: item.id)
        ]
        for peer in self.peers:
            peer.config_path.parent.mkdir(parents=True, exist_ok=True)
            peer.config_path.write_text(
                "\n".join(
                    [
                        f'listen: "127.0.0.1:{peer.port}"',
                        "upstream:",
                        f"  endpoint: {upstream_endpoint}",
                        f"  region: {self.e2e_config.region}",
                        f"  access_key: {_yaml_quote(self.e2e_config.access_key_id or '')}",
                        f"  secret_key: {_yaml_quote(self.e2e_config.secret_access_key or '')}",
                        *(
                            [f"  session_token: {_yaml_quote(self.e2e_config.session_token)}"]
                            if self.e2e_config.session_token
                            else []
                        ),
                        "cache:",
                        f"  cache_path: {peer.cache_path}",
                        f"  meta_path: {peer.meta_path}",
                        "  max_size: 256MB",
                        "  page_size: 1MB",
                        "http:",
                        "  read_header_timeout: 5s",
                        "  read_timeout: 2m",
                        "  write_timeout: 2m",
                        "  idle_timeout: 30s",
                        "upload:",
                        f"  spool_path: {peer.spool_path}",
                        "  max_spool_size: 256MB",
                        "logging:",
                        "  access_log: true",
                        "  internal_peer_access_log: false",
                        "  internal_peer_success_log: false",
                        "operator:",
                        "  enabled: true",
                        "  path: /debug/peer",
                        "peer:",
                        "  mode: peer",
                        f"  local_id: {peer.id}",
                        "  auth_secret: test-peer-secret",
                        "  read_sharding: auto",
                        "  page_sharding_min_pages: 2",
                        "  max_peer_fill_concurrency: 16",
                        "  max_peer_object_fill_concurrency: 4",
                        "  forward_timeout: 2m",
                        "  peers:",
                        *peer_yaml,
                        "",
                    ]
                ),
                encoding="utf-8",
            )


@pytest.fixture(scope="module")
def peer_cluster(e2e_config: E2EConfig, tmp_path_factory: pytest.TempPathFactory):
    cluster = LivePeerCluster(e2e_config, tmp_path_factory.mktemp("simple-s3-cache-peer-live"))
    cluster.start()
    try:
        yield cluster
    finally:
        cluster.stop()


def test_live_peer_page_sharded_large_read(peer_cluster: LivePeerCluster, s3_client, e2e_config) -> None:
    key = f"{e2e_config.prefix}/peer-live/page-sharded-large.bin"
    body = _patterned_body(LARGE_SIZE, seed=11)
    s3_client.put_object(Bucket=e2e_config.bucket, Key=key, Body=body)

    response = peer_cluster.client("cache-0").get_object(Bucket=e2e_config.bucket, Key=key)
    assert response["Body"].read() == body

    range_start = PAGE_SIZE - 19
    range_end = (3 * PAGE_SIZE) + 31
    ranged = peer_cluster.client("cache-2").get_object(
        Bucket=e2e_config.bucket,
        Key=key,
        Range=f"bytes={range_start}-{range_end}",
    )
    assert ranged["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert ranged["Body"].read() == body[range_start : range_end + 1]

    metrics = peer_cluster.metrics("cache-0") + peer_cluster.metrics("cache-2")
    assert 'simple_s3_cache_read_strategy_selected_total{bucket="' in metrics
    assert 'strategy="page"}' in metrics
    assert "simple_s3_cache_page_owner_requests_total" in metrics


def test_live_peer_large_mutation_correctness(peer_cluster: LivePeerCluster, s3_client, e2e_config) -> None:
    _assert_put_overwrite_large(peer_cluster, e2e_config)
    _assert_delete_large(peer_cluster, e2e_config)
    _assert_copy_destination_large(peer_cluster, s3_client, e2e_config)
    _assert_multipart_complete_large(peer_cluster, e2e_config)


def test_live_peer_owner_down_before_commit_falls_back(
    peer_cluster: LivePeerCluster,
    s3_client,
    e2e_config,
) -> None:
    stopped_peer = "cache-1"
    coordinator = "cache-0"
    key = _key_with_page_owner(e2e_config.bucket, f"{e2e_config.prefix}/peer-live/down-before", stopped_peer)
    body = _patterned_body(LARGE_SIZE, seed=77)
    s3_client.put_object(Bucket=e2e_config.bucket, Key=key, Body=body)

    page_index = _first_page_for_owner(e2e_config.bucket, key, stopped_peer)
    assert page_index is not None
    start = page_index * PAGE_SIZE
    end = min(start + PAGE_SIZE - 1, len(body) - 1)
    warm_response = peer_cluster.client(coordinator).get_object(
        Bucket=e2e_config.bucket,
        Key=key,
        Range=f"bytes={start}-{end}",
    )
    assert warm_response["Body"].read() == body[start : end + 1]

    peer_cluster.stop_peer(stopped_peer)
    try:
        response = peer_cluster.client(coordinator).get_object(
            Bucket=e2e_config.bucket,
            Key=key,
            Range=f"bytes={start}-{end}",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 206
        assert response["Body"].read() == body[start : end + 1]
        metrics = peer_cluster.metrics(coordinator)
        assert "simple_s3_cache_peer_read_fallbacks_total" in metrics
    finally:
        peer_cluster.start_peer(stopped_peer)
        peer_cluster.assert_all_ready()


def test_live_peer_invalidation_failure_marks_writer_not_ready(
    peer_cluster: LivePeerCluster,
    e2e_config,
) -> None:
    stopped_peer = "cache-1"
    writer_id = "cache-0"
    key = _key_with_object_owner(
        e2e_config.bucket,
        f"{e2e_config.prefix}/peer-live/invalidation-failure-large",
        writer_id,
    )
    original = _patterned_body(LARGE_SIZE, seed=81)
    updated = _patterned_body(LARGE_SIZE, seed=82)
    writer = peer_cluster.client(writer_id)
    reader = peer_cluster.client("cache-2")

    writer.put_object(Bucket=e2e_config.bucket, Key=key, Body=original)
    assert reader.get_object(Bucket=e2e_config.bucket, Key=key)["Body"].read() == original

    peer_cluster.stop_peer(stopped_peer)
    try:
        writer.put_object(Bucket=e2e_config.bucket, Key=key, Body=updated)
        assert _readiness_status(peer_cluster.peer(writer_id)) == 503
        health = _healthz(peer_cluster.peer(writer_id))
        assert health["ready"] is False
        assert health["reason_code"] == "peer_invalidation_failed"
        metrics = peer_cluster.metrics(writer_id)
        assert 'simple_s3_cache_degraded{reason_code="peer_invalidation_failed"} 1' in metrics
        assert 'simple_s3_cache_invalidation_broadcasts_total{bucket="' in metrics
        assert f'peer_id="{stopped_peer}",status="failure"' in metrics
    finally:
        peer_cluster.restart()
        peer_cluster.assert_all_ready()

    _assert_full_and_ranges(reader, e2e_config.bucket, key, updated)


def _assert_put_overwrite_large(peer_cluster: LivePeerCluster, e2e_config) -> None:
    key = f"{e2e_config.prefix}/peer-live/put-overwrite-large.bin"
    original = _patterned_body(LARGE_SIZE, seed=21)
    updated = _patterned_body(LARGE_SIZE, seed=22)
    writer = peer_cluster.client("cache-0")
    reader = peer_cluster.client("cache-3")

    writer.put_object(Bucket=e2e_config.bucket, Key=key, Body=original)
    assert reader.get_object(Bucket=e2e_config.bucket, Key=key)["Body"].read() == original
    writer.put_object(Bucket=e2e_config.bucket, Key=key, Body=updated)
    _assert_full_and_ranges(reader, e2e_config.bucket, key, updated)


def _assert_delete_large(peer_cluster: LivePeerCluster, e2e_config) -> None:
    key = f"{e2e_config.prefix}/peer-live/delete-large.bin"
    body = _patterned_body(LARGE_SIZE, seed=31)
    writer = peer_cluster.client("cache-1")
    reader = peer_cluster.client("cache-2")

    writer.put_object(Bucket=e2e_config.bucket, Key=key, Body=body)
    assert reader.get_object(Bucket=e2e_config.bucket, Key=key)["Body"].read() == body
    writer.delete_object(Bucket=e2e_config.bucket, Key=key)
    with pytest.raises(ClientError) as deleted:
        reader.get_object(Bucket=e2e_config.bucket, Key=key)
    assert _status(deleted.value) == 404


def _assert_copy_destination_large(peer_cluster: LivePeerCluster, s3_client, e2e_config) -> None:
    source_key = f"{e2e_config.prefix}/peer-live/copy-source-large.bin"
    destination_key = f"{e2e_config.prefix}/peer-live/copy-destination-large.bin"
    source = _patterned_body(LARGE_SIZE, seed=41)
    destination = _patterned_body(LARGE_SIZE, seed=42)
    s3_client.put_object(Bucket=e2e_config.bucket, Key=source_key, Body=source)
    s3_client.put_object(Bucket=e2e_config.bucket, Key=destination_key, Body=destination)

    writer = peer_cluster.client("cache-2")
    reader = peer_cluster.client("cache-0")
    assert reader.get_object(Bucket=e2e_config.bucket, Key=destination_key)["Body"].read() == destination
    writer.copy_object(
        Bucket=e2e_config.bucket,
        Key=destination_key,
        CopySource={"Bucket": e2e_config.bucket, "Key": source_key},
    )
    _assert_full_and_ranges(reader, e2e_config.bucket, destination_key, source)


def _assert_multipart_complete_large(peer_cluster: LivePeerCluster, e2e_config) -> None:
    key = f"{e2e_config.prefix}/peer-live/multipart-large.bin"
    old_body = _patterned_body(LARGE_SIZE, seed=51)
    new_body = _patterned_body((6 * 1024 * 1024) + 17, seed=52)
    writer = peer_cluster.client("cache-3")
    reader = peer_cluster.client("cache-1")
    writer.put_object(Bucket=e2e_config.bucket, Key=key, Body=old_body)
    assert reader.get_object(Bucket=e2e_config.bucket, Key=key)["Body"].read() == old_body

    upload_id = None
    try:
        upload = writer.create_multipart_upload(Bucket=e2e_config.bucket, Key=key)
        upload_id = upload["UploadId"]
        first = writer.upload_part(
            Bucket=e2e_config.bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            Body=new_body[: 5 * 1024 * 1024],
        )
        second = writer.upload_part(
            Bucket=e2e_config.bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=2,
            Body=new_body[5 * 1024 * 1024 :],
        )
        writer.complete_multipart_upload(
            Bucket=e2e_config.bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [
                    {"ETag": first["ETag"], "PartNumber": 1},
                    {"ETag": second["ETag"], "PartNumber": 2},
                ]
            },
        )
    except ClientError as exc:
        if upload_id:
            _abort_multipart(writer, e2e_config.bucket, key, upload_id)
        pytest.skip(f"multipart upload is not supported by this backend: {exc}")

    _assert_full_and_ranges(reader, e2e_config.bucket, key, new_body)


def _assert_full_and_ranges(client, bucket: str, key: str, body: bytes) -> None:
    assert client.get_object(Bucket=bucket, Key=key)["Body"].read() == body
    ranges = [
        (0, min(1023, len(body) - 1)),
        (PAGE_SIZE - 13, PAGE_SIZE + 29),
        (3 * PAGE_SIZE, (3 * PAGE_SIZE) + 4095),
        (len(body) - 2048, len(body) - 1),
    ]
    for start, end in ranges:
        response = client.get_object(Bucket=bucket, Key=key, Range=f"bytes={start}-{end}")
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 206
        assert response["Body"].read() == body[start : end + 1]


def _key_with_page_owner(bucket: str, prefix: str, owner_id: str) -> str:
    for i in range(256):
        key = f"{prefix}-{i}.bin"
        if _first_page_for_owner(bucket, key, owner_id) is not None:
            return key
    raise AssertionError(f"could not find test key with page owned by {owner_id}")


def _key_with_object_owner(bucket: str, prefix: str, owner_id: str) -> str:
    for i in range(256):
        key = f"{prefix}-{i}.bin"
        if _object_owner(bucket, key) == owner_id:
            return key
    raise AssertionError(f"could not find test key owned by {owner_id}")


def _object_owner(bucket: str, key: str) -> str:
    return max(PEER_IDS, key=lambda peer_id: _rendezvous_score(f"{bucket}/{key}", peer_id))


def _first_page_for_owner(bucket: str, key: str, owner_id: str) -> int | None:
    for page_index in range(0, 6):
        if _page_owner(bucket, key, page_index) == owner_id:
            return page_index
    return None


def _page_owner(bucket: str, key: str, page_index: int) -> str:
    routing_key = f"{bucket}/{key}\x00page\x00{page_index}"
    return max(PEER_IDS, key=lambda peer_id: _rendezvous_score(routing_key, peer_id))


def _rendezvous_score(routing_key: str, peer_id: str) -> int:
    digest = hashlib.sha256(f"{routing_key}\x00{peer_id}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


def _patterned_body(size: int, seed: int) -> bytes:
    pattern = bytes((seed + i) % 251 for i in range(8192))
    repetitions, remainder = divmod(size, len(pattern))
    return pattern * repetitions + pattern[:remainder]


def _wait_for_ready(peer: LivePeer) -> None:
    deadline = time.monotonic() + 30
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if peer.process is not None and peer.process.poll() is not None:
            output = peer.log_path.read_text(encoding="utf-8") if peer.log_path.exists() else ""
            pytest.fail(f"{peer.id} exited before becoming ready:\n{output}")
        try:
            with urllib.request.urlopen(f"{peer.endpoint}/readyz", timeout=1) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
        time.sleep(0.1)
    output = peer.log_path.read_text(encoding="utf-8") if peer.log_path.exists() else ""
    pytest.fail(f"{peer.id} did not become ready: {last_error}\n{output}")


def _readiness_status(peer: LivePeer) -> int:
    try:
        with urllib.request.urlopen(f"{peer.endpoint}/readyz", timeout=2) as response:
            return int(response.status)
    except urllib.error.HTTPError as exc:
        return int(exc.code)


def _healthz(peer: LivePeer) -> dict[str, object]:
    with urllib.request.urlopen(f"{peer.endpoint}/healthz", timeout=2) as response:
        return json.loads(response.read().decode("utf-8"))


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _yaml_quote(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _aws_s3_endpoint(region: str) -> str:
    if region == "us-east-1":
        return "https://s3.amazonaws.com"
    return f"https://s3.{region}.amazonaws.com"
