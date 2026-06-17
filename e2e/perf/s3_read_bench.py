from __future__ import annotations

# pyright: reportMissingImports=false

import argparse
import concurrent.futures
import json
import math
import os
import random
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config as BotoConfig
from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[2]
E2E_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_OBJECT_SIZE = 64 * 1024 * 1024
DEFAULT_RANGE_SIZE = 256 * 1024
DEFAULT_RANGE_COUNT = 256


@dataclass(frozen=True)
class Target:
    name: str
    endpoint: str


@dataclass(frozen=True)
class Workload:
    name: str
    key: str
    ranges: tuple[tuple[int, int] | None, ...]
    requests: int
    concurrency: int


@dataclass(frozen=True)
class RequestResult:
    latency_seconds: float
    bytes_read: int
    status: int
    error: str | None = None


def main() -> int:
    load_dotenv(REPO_ROOT / ".env", override=False)
    load_dotenv(E2E_ROOT / ".env", override=False)
    args = parse_args()

    random.seed(args.seed)
    targets = parse_targets(args.target)
    metrics_urls = parse_named_urls(args.metrics)
    bucket = args.bucket or first_env("S3CACHE_S3_BUCKET", "S3_BUCKET", "AWS_BUCKET")
    if not bucket:
        raise SystemExit("set --bucket or S3CACHE_S3_BUCKET/S3_BUCKET")

    region = args.region or first_env("S3CACHE_S3_REGION", "AWS_REGION", "AWS_DEFAULT_REGION") or "us-east-1"
    upstream_endpoint = args.upstream_endpoint or first_env(
        "S3CACHE_S3_ENDPOINT_URL",
        "S3_ENDPOINT_URL",
        "S3_ENDPOINT",
        "AWS_ENDPOINT_URL_S3",
        "AWS_ENDPOINT_URL",
        "MINIO_ENDPOINT_URL",
        "MINIO_ENDPOINT",
    )
    prefix = args.prefix.rstrip("/") if args.prefix else f"simple-s3-cache-perf/{uuid.uuid4().hex}"

    s3 = s3_client(region, upstream_endpoint)
    object_size = parse_size(args.object_size)
    range_size = parse_size(args.range_size)
    keys_by_target = {
        target.name: {
            "large": f"{prefix}/{target.name}/large-{object_size}.bin",
            "range": f"{prefix}/{target.name}/range-{object_size}.bin",
        }
        for target in targets
    }

    if not args.skip_upload:
        print(f"uploading benchmark objects to s3://{bucket}/{prefix}/ ...", flush=True)
        for target_name, keys in keys_by_target.items():
            print(f"  {target_name}", flush=True)
            put_patterned_object(s3, bucket, keys["large"], object_size, args.upload_part_size)
            put_patterned_object(s3, bucket, keys["range"], object_size, args.upload_part_size)

    range_plan = sparse_ranges(object_size, range_size, args.range_count)

    started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    output: dict[str, Any] = {
        "started_at": started_at,
        "bucket": bucket,
        "prefix": prefix,
        "object_size": object_size,
        "range_size": range_size,
        "range_count": len(range_plan),
        "requests": args.requests,
        "concurrency": args.concurrency,
        "targets": {},
    }

    for target in targets:
        print(f"running target {target.name} ({target.endpoint}) ...", flush=True)
        keys = keys_by_target[target.name]
        workloads = [
            Workload(
                name="large_stream_cold",
                key=keys["large"],
                ranges=(None,),
                requests=1,
                concurrency=1,
            ),
            Workload(
                name="large_stream_warm",
                key=keys["large"],
                ranges=(None,),
                requests=args.requests,
                concurrency=args.concurrency,
            ),
            Workload(
                name="sparse_range_cold",
                key=keys["range"],
                ranges=tuple(range_plan),
                requests=len(range_plan),
                concurrency=args.concurrency,
            ),
            Workload(
                name="sparse_range_warm",
                key=keys["range"],
                ranges=tuple(range_plan),
                requests=args.requests,
                concurrency=args.concurrency,
            ),
        ]
        before_metrics = scrape_metrics(metrics_urls.get(target.name))
        target_result: dict[str, Any] = {
            "endpoint": target.endpoint,
            "keys": keys,
            "metrics_before": before_metrics,
            "workloads": {},
        }
        for workload in workloads:
            print(f"  {workload.name}", flush=True)
            results, wall_seconds = run_workload(target, bucket, workload, args.timeout)
            target_result["workloads"][workload.name] = summarize_results(results, wall_seconds)
        target_result["metrics_after"] = scrape_metrics(metrics_urls.get(target.name))
        target_result["metric_delta"] = metric_delta(
            target_result["metrics_before"],
            target_result["metrics_after"],
        )
        output["targets"][target.name] = target_result

    if args.output:
        Path(args.output).write_text(json.dumps(output, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"wrote {args.output}", flush=True)
    else:
        print(json.dumps(output, indent=2, sort_keys=True))

    if not args.keep_objects:
        delete_prefix(s3, bucket, prefix)

    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run comparable read workloads against simple-s3-cache endpoints.",
    )
    parser.add_argument(
        "--target",
        action="append",
        required=True,
        metavar="NAME=URL",
        help="Endpoint to benchmark. Repeat for single, peer, gateway, etc.",
    )
    parser.add_argument(
        "--metrics",
        action="append",
        default=[],
        metavar="NAME=URL",
        help="Optional Prometheus metrics URL for a target. Repeat as needed.",
    )
    parser.add_argument("--bucket", default=None, help="S3 bucket for benchmark objects.")
    parser.add_argument("--prefix", default=None, help="Object prefix. Defaults to a unique perf prefix.")
    parser.add_argument("--upstream-endpoint", default=None, help="S3 endpoint used for object setup.")
    parser.add_argument("--region", default=None, help="S3 region. Defaults to env or us-east-1.")
    parser.add_argument("--object-size", default="64MiB", help="Size of each benchmark object.")
    parser.add_argument("--range-size", default="256KiB", help="Size of sparse range reads.")
    parser.add_argument("--range-count", type=int, default=DEFAULT_RANGE_COUNT, help="Sparse ranges per cold range pass.")
    parser.add_argument("--requests", type=int, default=64, help="Warm workload request count.")
    parser.add_argument("--concurrency", type=int, default=8, help="Concurrent requests for warm/range workloads.")
    parser.add_argument("--timeout", type=float, default=300.0, help="Per-request timeout in seconds.")
    parser.add_argument("--upload-part-size", type=int, default=8 * 1024 * 1024, help="Upload chunk size in bytes.")
    parser.add_argument("--seed", type=int, default=1, help="Sparse range random seed.")
    parser.add_argument("--skip-upload", action="store_true", help="Reuse existing objects at --prefix.")
    parser.add_argument("--keep-objects", action="store_true", help="Keep benchmark objects after the run.")
    parser.add_argument("--output", default=None, help="Write JSON results to this path.")
    return parser.parse_args()


def parse_targets(values: Iterable[str]) -> list[Target]:
    targets = []
    names = set()
    for value in values:
        name, url = parse_name_url(value, "--target")
        if name in names:
            raise SystemExit(f"duplicate target name {name!r}")
        names.add(name)
        targets.append(Target(name=name, endpoint=url.rstrip("/")))
    return targets


def parse_named_urls(values: Iterable[str]) -> dict[str, str]:
    urls = {}
    for value in values:
        name, url = parse_name_url(value, "--metrics")
        urls[name] = url
    return urls


def parse_name_url(value: str, flag: str) -> tuple[str, str]:
    if "=" not in value:
        raise SystemExit(f"{flag} must be NAME=URL")
    name, url = value.split("=", 1)
    name = name.strip()
    url = url.strip()
    if not name or not url:
        raise SystemExit(f"{flag} must be NAME=URL")
    return name, url


def s3_client(region: str, endpoint_url: str | None):
    session = boto3.session.Session(
        aws_access_key_id=first_env(
            "S3CACHE_S3_ACCESS_KEY_ID",
            "S3_ACCESS_KEY_ID",
            "S3_ACCESS_KEY",
            "AWS_ACCESS_KEY_ID",
            "MINIO_ACCESS_KEY",
            "MINIO_ROOT_USER",
        ),
        aws_secret_access_key=first_env(
            "S3CACHE_S3_SECRET_ACCESS_KEY",
            "S3_SECRET_ACCESS_KEY",
            "S3_SECRET_KEY",
            "AWS_SECRET_ACCESS_KEY",
            "MINIO_SECRET_KEY",
            "MINIO_ROOT_PASSWORD",
        ),
        aws_session_token=first_env("S3CACHE_S3_SESSION_TOKEN", "AWS_SESSION_TOKEN"),
        region_name=region,
    )
    if session.get_credentials() is None:
        raise SystemExit("unable to locate S3 credentials for benchmark object setup")
    return session.client(
        "s3",
        endpoint_url=endpoint_url,
        config=BotoConfig(
            retries={"max_attempts": 3, "mode": "standard"},
            s3={"addressing_style": "path"},
        ),
    )


def put_patterned_object(client, bucket: str, key: str, size: int, chunk_size: int) -> None:
    if size <= 5 * 1024 * 1024:
        client.put_object(Bucket=bucket, Key=key, Body=pattern_bytes(size))
        return

    upload = client.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = upload["UploadId"]
    parts = []
    try:
        offset = 0
        part_number = 1
        while offset < size:
            part_size = min(chunk_size, size - offset)
            # Non-final multipart parts must be at least 5 MiB.
            if size - offset > part_size and part_size < 5 * 1024 * 1024:
                part_size = 5 * 1024 * 1024
            body = pattern_bytes(part_size, offset)
            response = client.upload_part(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=body,
            )
            parts.append({"ETag": response["ETag"], "PartNumber": part_number})
            offset += part_size
            part_number += 1
        client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
    except Exception:
        client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise


def pattern_bytes(size: int, offset: int = 0) -> bytes:
    pattern = bytes(range(251))
    start = offset % len(pattern)
    rotated = pattern[start:] + pattern[:start]
    repetitions, remainder = divmod(size, len(rotated))
    return rotated * repetitions + rotated[:remainder]


def sparse_ranges(object_size: int, range_size: int, count: int) -> list[tuple[int, int]]:
    if range_size <= 0 or range_size > object_size:
        raise SystemExit("--range-size must be greater than zero and no larger than --object-size")
    max_start = object_size - range_size
    if count <= 0:
        raise SystemExit("--range-count must be greater than zero")
    starts = [random.randint(0, max_start) for _ in range(count)]
    return [(start, start + range_size - 1) for start in starts]


def run_workload(
    target: Target,
    bucket: str,
    workload: Workload,
    timeout: float,
) -> tuple[list[RequestResult], float]:
    planned_ranges = expand_ranges(workload.ranges, workload.requests)
    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workload.concurrency) as executor:
        futures = [
            executor.submit(read_object, target.endpoint, bucket, workload.key, byte_range, timeout)
            for byte_range in planned_ranges
        ]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
    return results, time.perf_counter() - started


def expand_ranges(
    ranges: tuple[tuple[int, int] | None, ...],
    requests: int,
) -> list[tuple[int, int] | None]:
    if len(ranges) == requests:
        return list(ranges)
    return [ranges[index % len(ranges)] for index in range(requests)]


def read_object(
    endpoint: str,
    bucket: str,
    key: str,
    byte_range: tuple[int, int] | None,
    timeout: float,
) -> RequestResult:
    url = object_url(endpoint, bucket, key)
    headers = {}
    if byte_range is not None:
        headers["Range"] = f"bytes={byte_range[0]}-{byte_range[1]}"
    request = urllib.request.Request(url, headers=headers)
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
            return RequestResult(
                latency_seconds=time.perf_counter() - started,
                bytes_read=len(body),
                status=response.status,
            )
    except urllib.error.HTTPError as exc:
        _ = exc.read()
        return RequestResult(
            latency_seconds=time.perf_counter() - started,
            bytes_read=0,
            status=exc.code,
            error=str(exc),
        )
    except Exception as exc:
        return RequestResult(
            latency_seconds=time.perf_counter() - started,
            bytes_read=0,
            status=0,
            error=repr(exc),
        )


def summarize_results(results: list[RequestResult], wall_seconds: float) -> dict[str, Any]:
    latencies = [result.latency_seconds for result in results]
    bytes_read = sum(result.bytes_read for result in results)
    errors = [result.error for result in results if result.error]
    return {
        "requests": len(results),
        "successful_requests": sum(1 for result in results if 200 <= result.status < 300),
        "errors": len(errors),
        "error_samples": errors[:5],
        "bytes_read": bytes_read,
        "aggregate_request_seconds": sum(latencies),
        "wall_seconds": wall_seconds,
        "throughput_bytes_per_second": bytes_read / wall_seconds if wall_seconds > 0 else 0,
        "latency_seconds": {
            "min": min(latencies),
            "p50": percentile(latencies, 50),
            "p95": percentile(latencies, 95),
            "p99": percentile(latencies, 99),
            "max": max(latencies),
            "mean": statistics.fmean(latencies),
        },
        "status_counts": status_counts(results),
    }


def status_counts(results: list[RequestResult]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for result in results:
        key = str(result.status)
        counts[key] = counts.get(key, 0) + 1
    return counts


def percentile(values: list[float], pct: int) -> float:
    if not values:
        return math.nan
    ordered = sorted(values)
    index = math.ceil((pct / 100) * len(ordered)) - 1
    return ordered[max(0, min(index, len(ordered) - 1))]


def scrape_metrics(url: str | None) -> dict[str, float]:
    if not url:
        return {}
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            text = response.read().decode("utf-8", errors="replace")
    except Exception as exc:
        return {"__scrape_error__": 1.0, "__scrape_error_hash__": float(abs(hash(repr(exc))) % 1_000_000)}

    kept_gauges = {
        "simple_s3_cache_cached_bytes",
        "simple_s3_cache_cache_max_bytes",
        "simple_s3_cache_peer_ring_info",
    }
    metrics: dict[str, float] = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        name_and_labels, _, value_text = line.partition(" ")
        if not value_text:
            continue
        name = name_and_labels.split("{", 1)[0]
        if not (
            name.endswith("_total")
            or name.endswith("_sum")
            or name.endswith("_count")
            or name in kept_gauges
        ):
            continue
        try:
            value = float(value_text)
        except ValueError:
            continue
        key = name_and_labels if name == "simple_s3_cache_peer_ring_info" else name
        metrics[key] = metrics.get(key, 0.0) + value
    return metrics


def metric_delta(before: dict[str, float], after: dict[str, float]) -> dict[str, float]:
    keys = set(before) | set(after)
    return {key: after.get(key, 0.0) - before.get(key, 0.0) for key in sorted(keys)}


def object_url(endpoint: str, bucket: str, key: str) -> str:
    return (
        endpoint.rstrip("/")
        + "/"
        + urllib.parse.quote(bucket, safe="")
        + "/"
        + urllib.parse.quote(key, safe="/")
    )


def delete_prefix(client, bucket: str, prefix: str) -> None:
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        for start in range(0, len(objects), 1000):
            client.delete_objects(Bucket=bucket, Delete={"Objects": objects[start : start + 1000]})


def parse_size(input_value: str) -> int:
    value = input_value.strip()
    index = 0
    while index < len(value) and (value[index].isdigit() or value[index] == "."):
        index += 1
    if index == 0:
        raise SystemExit(f"invalid size {input_value!r}")
    number = float(value[:index])
    unit = value[index:].strip().lower()
    multipliers = {
        "": 1,
        "b": 1,
        "k": 1024,
        "kb": 1024,
        "kib": 1024,
        "m": 1024**2,
        "mb": 1024**2,
        "mib": 1024**2,
        "g": 1024**3,
        "gb": 1024**3,
        "gib": 1024**3,
    }
    if unit not in multipliers:
        raise SystemExit(f"unsupported size unit {unit!r}")
    parsed = int(number * multipliers[unit])
    if parsed <= 0:
        raise SystemExit("size must be greater than zero")
    return parsed


def first_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


if __name__ == "__main__":
    raise SystemExit(main())
