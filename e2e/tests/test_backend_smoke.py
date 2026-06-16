from __future__ import annotations

# pyright: reportMissingImports=false

import pytest
from botocore.exceptions import ClientError


pytestmark = pytest.mark.e2e


def test_backend_put_head_range_get_delete(s3_client, e2e_config, object_key: str) -> None:
    body = b"simple-s3-cache e2e smoke\n" + bytes(range(64))

    s3_client.put_object(
        Bucket=e2e_config.bucket,
        Key=object_key,
        Body=body,
        ContentType="application/octet-stream",
        Metadata={"test": "backend-smoke"},
    )

    head = s3_client.head_object(Bucket=e2e_config.bucket, Key=object_key)
    assert head["ContentLength"] == len(body)
    assert head["ContentType"] == "application/octet-stream"
    assert head["Metadata"]["test"] == "backend-smoke"
    assert "ETag" in head

    full = s3_client.get_object(Bucket=e2e_config.bucket, Key=object_key)
    assert full["Body"].read() == body

    partial = s3_client.get_object(
        Bucket=e2e_config.bucket,
        Key=object_key,
        Range="bytes=7-18",
    )
    assert partial["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert partial["Body"].read() == body[7:19]
    assert partial["ContentRange"] == f"bytes 7-18/{len(body)}"

    s3_client.delete_object(Bucket=e2e_config.bucket, Key=object_key)

    with pytest.raises(ClientError) as exc_info:
        s3_client.head_object(Bucket=e2e_config.bucket, Key=object_key)

    status = exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"]
    assert status == 404
