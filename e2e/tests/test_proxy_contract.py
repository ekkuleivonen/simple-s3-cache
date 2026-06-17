from __future__ import annotations

# pyright: reportMissingImports=false

import base64
import concurrent.futures
import datetime as dt
import hashlib
import http.client
import os
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from conftest import CacheProxy


pytestmark = pytest.mark.e2e

PAGE_SIZE = 4 * 1024 * 1024


def test_proxy_put_head_get_range_delete(cache_s3_client, s3_client, e2e_config, object_key: str) -> None:
    body = b"simple-s3-cache proxy smoke\n" + bytes(range(128))

    cache_s3_client.put_object(
        Bucket=e2e_config.bucket,
        Key=object_key,
        Body=body,
        ContentType="application/octet-stream",
        Metadata={"test": "proxy-smoke"},
    )

    backend_head = s3_client.head_object(Bucket=e2e_config.bucket, Key=object_key)
    proxy_head = cache_s3_client.head_object(Bucket=e2e_config.bucket, Key=object_key)
    assert proxy_head["ContentLength"] == len(body)
    assert proxy_head["ContentLength"] == backend_head["ContentLength"]
    assert proxy_head["ContentType"] == "application/octet-stream"
    assert _lower_metadata(proxy_head["Metadata"]) == {"test": "proxy-smoke"}
    assert proxy_head["ETag"] == backend_head["ETag"]

    full = cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key)
    assert full["Body"].read() == body
    assert full["ContentLength"] == len(body)

    partial = cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key, Range="bytes=7-18")
    assert partial["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert partial["Body"].read() == body[7:19]
    assert partial["ContentRange"] == f"bytes 7-18/{len(body)}"

    cache_s3_client.delete_object(Bucket=e2e_config.bucket, Key=object_key)

    with pytest.raises(ClientError) as deleted:
        s3_client.head_object(Bucket=e2e_config.bucket, Key=object_key)
    assert _status(deleted.value) == 404


def test_proxy_chunked_put_body_passes_through(cache_endpoint: str, s3_client, e2e_config) -> None:
    key = f"{e2e_config.prefix}/objects/chunked-put.bin"
    body = b"chunked put through simple-s3-cache\n" + bytes(range(64))
    url = urllib.parse.urlsplit(_object_url(cache_endpoint, e2e_config.bucket, key))
    assert url.scheme == "http"
    assert url.hostname is not None

    connection = http.client.HTTPConnection(url.hostname, url.port, timeout=10)
    try:
        connection.request(
            "PUT",
            url.path,
            body=iter([body[:13], body[13:37], body[37:]]),
            headers={"Content-Type": "application/octet-stream"},
            encode_chunked=True,
        )
        response = connection.getresponse()
        response_body = response.read()
    finally:
        connection.close()

    assert 200 <= response.status < 300, response_body
    stored = s3_client.get_object(Bucket=e2e_config.bucket, Key=key)
    assert stored["Body"].read() == body
    assert stored["ContentType"] == "application/octet-stream"


def test_proxy_aws_chunked_put_body_is_decoded_before_upstream(cache_endpoint: str, s3_client, e2e_config) -> None:
    key = f"{e2e_config.prefix}/objects/aws-chunked-put.bin"
    body = b"aws chunked put through simple-s3-cache\n" + bytes(range(64))
    encoded = _aws_chunked_body(body[:17], body[17:43], body[43:])
    url = urllib.parse.urlsplit(_object_url(cache_endpoint, e2e_config.bucket, key))
    assert url.scheme == "http"
    assert url.hostname is not None

    connection = http.client.HTTPConnection(url.hostname, url.port, timeout=10)
    try:
        connection.request(
            "PUT",
            url.path,
            body=encoded,
            headers={
                "Content-Encoding": "aws-chunked",
                "Content-Type": "application/octet-stream",
                "X-Amz-Content-Sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
                "X-Amz-Decoded-Content-Length": str(len(body)),
            },
        )
        response = connection.getresponse()
        response_body = response.read()
    finally:
        connection.close()

    assert 200 <= response.status < 300, response_body
    stored = s3_client.get_object(Bucket=e2e_config.bucket, Key=key)
    assert stored["Body"].read() == body
    assert stored["ContentType"] == "application/octet-stream"


def test_proxy_full_get_round_trips_large_object(cache_s3_client, s3_client, e2e_config, object_key: str) -> None:
    body = _patterned_body(PAGE_SIZE + 12345)
    s3_client.put_object(
        Bucket=e2e_config.bucket,
        Key=object_key,
        Body=body,
        ContentType="application/octet-stream",
    )

    response = cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key)

    assert response["Body"].read() == body
    assert response["ContentLength"] == len(body)
    assert response["ContentType"] == "application/octet-stream"
    assert response["ETag"] == s3_client.head_object(Bucket=e2e_config.bucket, Key=object_key)["ETag"]


def test_proxy_head_returns_metadata_without_body(cache_s3_client, s3_client, e2e_config, object_key: str) -> None:
    body = b"metadata only\n"
    s3_client.put_object(
        Bucket=e2e_config.bucket,
        Key=object_key,
        Body=body,
        ContentType="text/plain",
        Metadata={"purpose": "head"},
    )

    response = cache_s3_client.head_object(Bucket=e2e_config.bucket, Key=object_key)

    assert response["ContentLength"] == len(body)
    assert response["ContentType"] == "text/plain"
    assert _lower_metadata(response["Metadata"]) == {"purpose": "head"}
    assert "Body" not in response


def test_proxy_missing_object_matches_backend(cache_s3_client, s3_client, e2e_config, object_key: str) -> None:
    for client in (s3_client, cache_s3_client):
        with pytest.raises(ClientError) as head_error:
            client.head_object(Bucket=e2e_config.bucket, Key=object_key)
        assert _status(head_error.value) == 404

        with pytest.raises(ClientError) as get_error:
            client.get_object(Bucket=e2e_config.bucket, Key=object_key)
        assert _status(get_error.value) == 404


def test_proxy_repeated_full_get_uses_cached_bytes(cache_s3_client, s3_client, e2e_config, object_key: str) -> None:
    original = b"cached-version"
    updated = b"direct-upstream-update"
    s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=original)

    assert cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key)["Body"].read() == original

    s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=updated)

    assert cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key)["Body"].read() == original


def test_proxy_single_range_get_variants(cache_s3_client, s3_client, e2e_config, object_key: str) -> None:
    body = _patterned_body(257)
    s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=body)

    cases = [
        ("bytes=10-24", body[10:25], f"bytes 10-24/{len(body)}"),
        ("bytes=240-", body[240:], f"bytes 240-256/{len(body)}"),
        ("bytes=-17", body[-17:], f"bytes 240-256/{len(body)}"),
    ]
    for range_header, expected_body, expected_content_range in cases:
        response = cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key, Range=range_header)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 206
        assert response["Body"].read() == expected_body
        assert response["ContentRange"] == expected_content_range


def test_proxy_overlapping_ranges_share_cached_pages(cache_s3_client, s3_client, e2e_config, object_key: str) -> None:
    body = _patterned_body(PAGE_SIZE + 4096)
    s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=body)

    first = cache_s3_client.get_object(
        Bucket=e2e_config.bucket,
        Key=object_key,
        Range=f"bytes={PAGE_SIZE - 128}-{PAGE_SIZE + 127}",
    )
    second = cache_s3_client.get_object(
        Bucket=e2e_config.bucket,
        Key=object_key,
        Range=f"bytes={PAGE_SIZE - 64}-{PAGE_SIZE + 255}",
    )

    assert first["Body"].read() == body[PAGE_SIZE - 128 : PAGE_SIZE + 128]
    assert second["Body"].read() == body[PAGE_SIZE - 64 : PAGE_SIZE + 256]


def test_proxy_full_get_after_range_get(cache_s3_client, s3_client, e2e_config, object_key: str) -> None:
    body = _patterned_body(PAGE_SIZE + 511)
    s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=body)

    warm_range = cache_s3_client.get_object(
        Bucket=e2e_config.bucket,
        Key=object_key,
        Range=f"bytes={PAGE_SIZE - 20}-{PAGE_SIZE + 20}",
    )
    assert warm_range["Body"].read() == body[PAGE_SIZE - 20 : PAGE_SIZE + 21]

    full = cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key)
    assert full["Body"].read() == body


def test_proxy_range_get_after_full_get(cache_s3_client, s3_client, e2e_config, object_key: str) -> None:
    body = _patterned_body(PAGE_SIZE + 333)
    s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=body)

    assert cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key)["Body"].read() == body

    response = cache_s3_client.get_object(
        Bucket=e2e_config.bucket,
        Key=object_key,
        Range=f"bytes={PAGE_SIZE - 10}-{PAGE_SIZE + 10}",
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert response["Body"].read() == body[PAGE_SIZE - 10 : PAGE_SIZE + 11]
    assert response["ContentRange"] == f"bytes {PAGE_SIZE - 10}-{PAGE_SIZE + 10}/{len(body)}"


def test_proxy_put_invalidates_cached_object(cache_s3_client, s3_client, e2e_config, object_key: str) -> None:
    original = b"before invalidation"
    updated = b"after invalidation"
    s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=original, ContentType="text/plain")
    assert cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key)["Body"].read() == original

    cache_s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=updated, ContentType="text/plain")

    response = cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key)
    assert response["Body"].read() == updated
    assert response["ContentLength"] == len(updated)


def test_proxy_delete_invalidates_cached_object(cache_s3_client, s3_client, e2e_config, object_key: str) -> None:
    s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=b"delete me")
    assert cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key)["Body"].read() == b"delete me"

    cache_s3_client.delete_object(Bucket=e2e_config.bucket, Key=object_key)

    with pytest.raises(ClientError) as deleted:
        cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key)
    assert _status(deleted.value) == 404


def test_proxy_copy_invalidates_destination_only(cache_s3_client, s3_client, e2e_config) -> None:
    source_key = f"{e2e_config.prefix}/objects/copy-source.bin"
    destination_key = f"{e2e_config.prefix}/objects/copy-destination.bin"
    source_body = b"copied body"
    destination_body = b"old destination body"
    s3_client.put_object(Bucket=e2e_config.bucket, Key=source_key, Body=source_body)
    s3_client.put_object(Bucket=e2e_config.bucket, Key=destination_key, Body=destination_body)
    assert cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=source_key)["Body"].read() == source_body
    assert cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=destination_key)["Body"].read() == destination_body

    cache_s3_client.copy_object(
        Bucket=e2e_config.bucket,
        Key=destination_key,
        CopySource={"Bucket": e2e_config.bucket, "Key": source_key},
    )

    assert cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=destination_key)["Body"].read() == source_body
    assert cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=source_key)["Body"].read() == source_body


def test_proxy_multipart_complete_invalidates_target(cache_s3_client, s3_client, e2e_config, object_key: str) -> None:
    old_body = b"old multipart target"
    new_body = b"a" * (5 * 1024 * 1024) + b"final-part"
    s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=old_body)
    assert cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key)["Body"].read() == old_body

    upload_id = None
    try:
        upload = cache_s3_client.create_multipart_upload(Bucket=e2e_config.bucket, Key=object_key)
        upload_id = upload["UploadId"]
        first = cache_s3_client.upload_part(
            Bucket=e2e_config.bucket,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=1,
            Body=new_body[: 5 * 1024 * 1024],
        )
        second = cache_s3_client.upload_part(
            Bucket=e2e_config.bucket,
            Key=object_key,
            UploadId=upload_id,
            PartNumber=2,
            Body=new_body[5 * 1024 * 1024 :],
        )
        cache_s3_client.complete_multipart_upload(
            Bucket=e2e_config.bucket,
            Key=object_key,
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
            _abort_multipart(cache_s3_client, e2e_config.bucket, object_key, upload_id)
        pytest.skip(f"multipart upload is not supported by this backend: {exc}")

    assert cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key)["Body"].read() == new_body


def test_proxy_bucket_operations_pass_through(cache_s3_client, s3_client, e2e_config, object_key: str) -> None:
    s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=b"listed")

    listing = cache_s3_client.list_objects_v2(
        Bucket=e2e_config.bucket,
        Prefix=f"{e2e_config.prefix}/objects/",
    )

    assert any(item["Key"] == object_key for item in listing.get("Contents", []))


def test_proxy_multirange_get_matches_backend(cache_s3_client, s3_client, e2e_config, object_key: str) -> None:
    body = b"abcdefghijklmnopqrstuvwxyz"
    s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=body)

    backend = _get_status_and_body(s3_client, e2e_config.bucket, object_key, Range="bytes=0-1,4-5")
    proxy = _get_status_and_body(cache_s3_client, e2e_config.bucket, object_key, Range="bytes=0-1,4-5")

    assert proxy[0] == backend[0]
    if backend[0] < 400:
        assert proxy[1] == backend[1]


def test_proxy_query_subresources_pass_through(cache_s3_client, s3_client, e2e_config, object_key: str) -> None:
    s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=b"tagged", Tagging="purpose=subresource")

    tags = cache_s3_client.get_object_tagging(Bucket=e2e_config.bucket, Key=object_key)

    assert tags["TagSet"] == [{"Key": "purpose", "Value": "subresource"}]


def test_proxy_response_override_query_passes_through(cache_s3_client, s3_client, e2e_config, object_key: str) -> None:
    body = b"override content type"
    s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=body, ContentType="application/octet-stream")

    response = cache_s3_client.get_object(
        Bucket=e2e_config.bucket,
        Key=object_key,
        ResponseContentType="text/plain",
    )

    assert response["Body"].read() == body
    assert response["ContentType"] == "text/plain"


def test_proxy_sse_c_requests_pass_through(cache_s3_client, s3_client, e2e_config, object_key: str) -> None:
    key = os.urandom(32)
    customer_key = base64.b64encode(key).decode("ascii")
    customer_key_md5 = base64.b64encode(hashlib.md5(key).digest()).decode("ascii")
    kwargs = {
        "SSECustomerAlgorithm": "AES256",
        "SSECustomerKey": customer_key,
        "SSECustomerKeyMD5": customer_key_md5,
    }
    body = b"sse-c body"

    try:
        s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=body, **kwargs)
    except ClientError as exc:
        pytest.skip(f"SSE-C is not supported by this backend: {exc}")

    response = cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key, **kwargs)

    assert response["Body"].read() == body


def test_proxy_conditional_get_if_none_match_uses_cached_metadata(
    cache_s3_client,
    s3_client,
    e2e_config,
    object_key: str,
) -> None:
    old_body = b"old conditional"
    new_body = b"new conditional"
    s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=old_body)
    old_etag = s3_client.head_object(Bucket=e2e_config.bucket, Key=object_key)["ETag"]
    assert cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key)["Body"].read() == old_body
    s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=new_body)

    with pytest.raises(ClientError) as not_modified:
        cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key, IfNoneMatch=old_etag)
    assert _status(not_modified.value) == 304


def test_proxy_conditional_get_if_modified_since_matches_backend(
    cache_s3_client,
    s3_client,
    e2e_config,
    object_key: str,
) -> None:
    s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=b"conditional time")
    future = dt.datetime.now(dt.UTC) + dt.timedelta(days=1)

    backend = _get_status_and_body(s3_client, e2e_config.bucket, object_key, IfModifiedSince=future)
    proxy = _get_status_and_body(cache_s3_client, e2e_config.bucket, object_key, IfModifiedSince=future)

    assert proxy[0] == backend[0]


def test_proxy_client_signature_is_ignored(cache_endpoint: str, s3_client, e2e_config, object_key: str) -> None:
    body = b"unsigned client request"
    s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=body)
    request = urllib.request.Request(
        _object_url(cache_endpoint, e2e_config.bucket, object_key),
        headers={
            "Authorization": "AWS4-HMAC-SHA256 Credential=bad/20260616/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=bad",
            "X-Amz-Date": "20260616T000000Z",
        },
    )

    with urllib.request.urlopen(request, timeout=10) as response:
        assert response.status == 200
        assert response.read() == body


def test_proxy_path_style_keys_with_escaping(cache_s3_client, s3_client, e2e_config) -> None:
    key = f"{e2e_config.prefix}/objects/path with spaces/plus+and%25.txt"
    body = b"escaped key"
    s3_client.put_object(Bucket=e2e_config.bucket, Key=key, Body=body)

    response = cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=key)

    assert response["Body"].read() == body


def test_proxy_healthz(cache_endpoint: str) -> None:
    with urllib.request.urlopen(f"{cache_endpoint}/healthz", timeout=5) as response:
        assert response.status == 200
        assert response.read() == b'{"status":"ok","ready":true}\n'


def test_proxy_readyz(cache_endpoint: str) -> None:
    with urllib.request.urlopen(f"{cache_endpoint}/readyz", timeout=5) as response:
        assert response.status == 200
        assert response.read() == b'{"status":"ready"}\n'


def test_proxy_restart_with_existing_cache(cache_proxy: CacheProxy, cache_s3_client, s3_client, e2e_config, object_key: str) -> None:
    body = b"survives restart"
    s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=body)
    assert cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key)["Body"].read() == body

    cache_proxy.restart()

    assert cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key)["Body"].read() == body


def test_proxy_restart_with_deleted_cache_dirs(
    cache_proxy: CacheProxy,
    cache_s3_client,
    s3_client,
    e2e_config,
    object_key: str,
) -> None:
    body = b"upstream source of truth"
    s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=body)
    assert cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key)["Body"].read() == body

    cache_proxy.delete_cache_state_and_restart()

    assert cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key)["Body"].read() == body


def test_proxy_cache_page_file_missing_recovers(
    cache_proxy: CacheProxy,
    cache_s3_client,
    s3_client,
    e2e_config,
    object_key: str,
) -> None:
    body = _patterned_body(1024)
    s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=body)
    assert cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key)["Body"].read() == body
    page_path = _first_cached_page_path(cache_proxy.meta_path / "cache.db", cache_proxy.cache_path)
    page_path.unlink()

    assert cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key)["Body"].read() == body


def test_proxy_concurrent_range_reads_same_object(cache_s3_client, s3_client, e2e_config, object_key: str) -> None:
    body = _patterned_body(PAGE_SIZE + 2048)
    s3_client.put_object(Bucket=e2e_config.bucket, Key=object_key, Body=body)
    ranges = [
        (0, 1023),
        (512, 1535),
        (PAGE_SIZE - 512, PAGE_SIZE + 511),
        (PAGE_SIZE, PAGE_SIZE + 1023),
        (len(body) - 1024, len(body) - 1),
    ]

    def read_range(start: int, end: int) -> bytes:
        response = cache_s3_client.get_object(
            Bucket=e2e_config.bucket,
            Key=object_key,
            Range=f"bytes={start}-{end}",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 206
        return response["Body"].read()

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(ranges)) as executor:
        results = list(executor.map(lambda item: read_range(*item), ranges))

    for (start, end), result in zip(ranges, results, strict=True):
        assert result == body[start : end + 1]


def _patterned_body(size: int) -> bytes:
    pattern = bytes(range(251))
    repetitions, remainder = divmod(size, len(pattern))
    return pattern * repetitions + pattern[:remainder]


def _status(exc: ClientError) -> int:
    return int(exc.response["ResponseMetadata"]["HTTPStatusCode"])


def _lower_metadata(metadata: dict[str, str]) -> dict[str, str]:
    return {key.lower(): value for key, value in metadata.items()}


def _get_status_and_body(client, bucket: str, key: str, **kwargs) -> tuple[int, bytes]:
    try:
        response = client.get_object(Bucket=bucket, Key=key, **kwargs)
    except ClientError as exc:
        return _status(exc), b""
    return int(response["ResponseMetadata"]["HTTPStatusCode"]), response["Body"].read()


def _abort_multipart(client, bucket: str, key: str, upload_id: str) -> None:
    try:
        client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
    except ClientError:
        pass


def _object_url(endpoint: str, bucket: str, key: str) -> str:
    return (
        endpoint.rstrip("/")
        + "/"
        + urllib.parse.quote(bucket, safe="")
        + "/"
        + urllib.parse.quote(key, safe="/")
    )


def _aws_chunked_body(*chunks: bytes) -> bytes:
    body = bytearray()
    for chunk in chunks:
        body.extend(f"{len(chunk):x};chunk-signature={'0' * 64}\r\n".encode("ascii"))
        body.extend(chunk)
        body.extend(b"\r\n")
    body.extend(b"0;chunk-signature=0000000000000000000000000000000000000000000000000000000000000000\r\n\r\n")
    return bytes(body)


def _first_cached_page_path(db_path: Path, cache_path: Path) -> Path:
    with sqlite3.connect(db_path) as db:
        row = db.execute("SELECT path FROM pages ORDER BY created_at LIMIT 1").fetchone()
    assert row is not None
    return cache_path / row[0]
