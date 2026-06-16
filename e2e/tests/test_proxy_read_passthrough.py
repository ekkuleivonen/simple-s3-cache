from __future__ import annotations

# pyright: reportMissingImports=false

import pytest
from botocore.exceptions import ClientError


pytestmark = pytest.mark.e2e


def test_proxy_head_full_get_and_range_get(s3_client, cache_s3_client, e2e_config, object_key: str) -> None:
    body = b"simple-s3-cache proxy read passthrough\n" + bytes(range(128))

    s3_client.put_object(
        Bucket=e2e_config.bucket,
        Key=object_key,
        Body=body,
        ContentType="application/octet-stream",
        Metadata={"test": "proxy-read-passthrough"},
    )

    backend_head = s3_client.head_object(Bucket=e2e_config.bucket, Key=object_key)
    proxy_head = cache_s3_client.head_object(Bucket=e2e_config.bucket, Key=object_key)
    assert proxy_head["ContentLength"] == backend_head["ContentLength"]
    assert proxy_head["ContentType"] == backend_head["ContentType"]
    assert _lower_metadata(proxy_head["Metadata"]) == _lower_metadata(backend_head["Metadata"])
    assert proxy_head["ETag"] == backend_head["ETag"]

    backend_full = s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key)
    proxy_full = cache_s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key)
    assert proxy_full["Body"].read() == backend_full["Body"].read()
    assert proxy_full["ContentLength"] == backend_full["ContentLength"]
    assert proxy_full["ContentType"] == backend_full["ContentType"]
    assert proxy_full["ETag"] == backend_full["ETag"]

    proxy_partial = cache_s3_client.get_object(
        Bucket=e2e_config.bucket,
        Key=object_key,
        Range="bytes=7-18",
    )
    assert proxy_partial["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert proxy_partial["Body"].read() == body[7:19]
    assert proxy_partial["ContentRange"] == f"bytes 7-18/{len(body)}"
    assert proxy_partial["ETag"] == backend_head["ETag"]


def test_proxy_forwards_pass_through_requests(s3_client, cache_s3_client, e2e_config, object_key: str) -> None:
    body = b"simple-s3-cache proxy generic passthrough\n"

    s3_client.put_object(
        Bucket=e2e_config.bucket,
        Key=object_key,
        Body=body,
        Tagging="purpose=passthrough",
    )

    head = s3_client.head_object(Bucket=e2e_config.bucket, Key=object_key)
    with pytest.raises(ClientError) as not_modified:
        cache_s3_client.get_object(
            Bucket=e2e_config.bucket,
            Key=object_key,
            IfNoneMatch=head["ETag"],
        )
    assert not_modified.value.response["ResponseMetadata"]["HTTPStatusCode"] == 304

    listing = cache_s3_client.list_objects_v2(
        Bucket=e2e_config.bucket,
        Prefix=f"{e2e_config.prefix}/objects/",
    )
    assert any(item["Key"] == object_key for item in listing.get("Contents", []))

    tags = cache_s3_client.get_object_tagging(Bucket=e2e_config.bucket, Key=object_key)
    assert tags["TagSet"] == [{"Key": "purpose", "Value": "passthrough"}]

    proxy_written_key = f"{e2e_config.prefix}/objects/proxy-put.bin"
    proxy_body = b"written through simple-s3-cache\n"
    cache_s3_client.put_object(
        Bucket=e2e_config.bucket,
        Key=proxy_written_key,
        Body=proxy_body,
        ContentType="text/plain",
    )
    written = s3_client.get_object(Bucket=e2e_config.bucket, Key=proxy_written_key)
    assert written["Body"].read() == proxy_body
    assert written["ContentType"] == "text/plain"

    cache_s3_client.delete_object(Bucket=e2e_config.bucket, Key=proxy_written_key)
    with pytest.raises(ClientError) as deleted:
        s3_client.head_object(Bucket=e2e_config.bucket, Key=proxy_written_key)
    assert deleted.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def _lower_metadata(metadata: dict[str, str]) -> dict[str, str]:
    return {key.lower(): value for key, value in metadata.items()}
