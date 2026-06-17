# Performance Test

## Purpose

Prove that v2 peer mode improves aggregate throughput for large-object and
range-heavy workloads without materially regressing small-object reads.

The key claim to validate:

- `peer.read_sharding: auto` should keep small objects on the `object` strategy.
- Large objects should use page sharding and spread page-owner bytes across all
  cache peers.
- Aggregate throughput improves under concurrent reads. One single HTTP response
  is still capped by the coordinator peer's egress path.

## Setup

Deploy the same cache version, image tag
`ghcr.io/ekkuleivonen/simple-s3-cache:0.0.2`, in these configurations:

1. `single` baseline, one cache pod.
2. `peer` with `peer.read_sharding: object`.
3. `peer` with `peer.read_sharding: auto`.
4. Optional: `peer` with `peer.read_sharding: page`.

Use the same RustFS cluster, bucket, object set, and compute-cluster test pods
for each run. Restart or clear cache state between cold-cache runs.

## Workloads

Run at least these workload classes:

- Large object full reads:
  - object size: 256MiB, 1GiB if practical.
  - concurrency: enough to exercise all cache peers.
- Sparse parquet-like ranges:
  - object size: 256MiB or larger.
  - range size: representative column chunk / row group reads.
  - range count: enough to touch many pages.
- Small object reads:
  - object size below one configured page.
  - high request count and moderate concurrency.
- Mixed workload:
  - roughly representative of production shape:
    - mostly parquet/datalake bytes;
    - some video-like sequential reads;
    - small HTML/document reads.

The existing e2e performance runner can be used from compute-cluster test pods:

```bash
cd e2e
uv run python perf/s3_read_bench.py \
  --target single=http://single-cache.example.internal:8080 \
  --target peer-object=http://peer-object-cache.example.internal:8080 \
  --target peer-auto=http://peer-auto-cache.example.internal:8080 \
  --metrics single=http://single-cache.example.internal:8080/metrics \
  --metrics peer-object=http://peer-object-cache.example.internal:8080/metrics \
  --metrics peer-auto=http://peer-auto-cache.example.internal:8080/metrics \
  --bucket "$S3CACHE_S3_BUCKET" \
  --object-size 256MiB \
  --range-size 4MiB \
  --range-count 512 \
  --requests 512 \
  --concurrency 32 \
  --output perf-results.json
```

Adjust object size, range size, request count, and concurrency to match the test
cluster and workload. Run each scenario more than once.

## Metrics To Capture

Capture per-peer metrics before and after each run:

- `simple_s3_cache_read_strategy_selected_total`
- `simple_s3_cache_coordinator_requests_total`
- `simple_s3_cache_page_owner_requests_total`
- `simple_s3_cache_page_owner_bytes_served_total`
- `simple_s3_cache_page_owner_upstream_fill_bytes_total`
- `simple_s3_cache_internal_peer_requests_per_client_request`
- `simple_s3_cache_page_batch_size`
- `simple_s3_cache_fill_coalesced_total`
- `simple_s3_cache_page_hits_total`
- `simple_s3_cache_page_misses_total`
- `simple_s3_cache_bytes_served_from_cache_total`
- `simple_s3_cache_bytes_served_from_upstream_total`
- `simple_s3_cache_upstream_fill_bytes_total`
- `simple_s3_cache_upstream_request_failures_total`
- `simple_s3_cache_degraded`

Also collect:

- client-side p50/p95/p99 latency;
- aggregate throughput in Gbit/s;
- per-peer network throughput;
- RustFS request rate, error rate, and latency;
- pod CPU, memory, and throttling;
- disk read/write throughput on cache nodes.

## What To Look For

Expected for large objects:

- `peer-auto` should select `page` for objects at or above
  `page_sharding_min_pages`.
- `simple_s3_cache_page_owner_bytes_served_total` should be materially spread
  across all cache peers.
- Aggregate throughput should be higher than `peer-object` under enough
  concurrency.
- p95 should not get worse enough to erase the throughput gain.
- `simple_s3_cache_internal_peer_requests_per_client_request` and
  `simple_s3_cache_page_batch_size` should show batching, not one peer request
  per page in large ranges.

Expected for small objects:

- `peer-auto` should select `object`.
- Small-object p50/p95 should be close to `peer-object`.
- Internal peer fanout should stay low.

Expected for cold cache:

- `simple_s3_cache_fill_coalesced_total` should show duplicate fill suppression
  when overlapping cold pages are requested.
- RustFS should not see a stampede of identical page range requests.

## Pass Criteria

- Large-object `peer-auto` or `peer-page` beats `peer-object` at p50 and p95
  under realistic concurrency.
- Small-object `peer-auto` does not materially regress against `peer-object`.
- Page-owner bytes are not pinned to one peer for hot large objects.
- Upstream failures stay at zero or are understood and not cache-induced.
- `simple_s3_cache_degraded` remains `0` throughout healthy runs.

## Failure Criteria

- Large-object bytes are still strongly pinned to one peer under `auto` or
  `page`.
- p95/p99 latency regresses significantly because of peer fanout.
- Cold cache creates duplicate upstream fills for the same page/ETag/epoch.
- Any peer reports degraded state during healthy performance runs.
- RustFS error rate increases due to cache fanout.
