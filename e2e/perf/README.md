# Performance Validation

This directory contains the performance harness. It does not deploy the cache.
Deploy one or more supported modes first, then run the same read workloads
against each endpoint:

- `single`: one cache instance, least operational complexity.
- `peer`: cache peers behind a Service, using the configured `object`, `page`,
  or `auto` read strategy.

The goal is repeatable evidence, not a synthetic leaderboard. Run this in the
same network where real clients or compute jobs run.

## Run

From `e2e/`:

```bash
uv run python perf/s3_read_bench.py \
  --target single=http://single-cache.example.internal:8080 \
  --target peer=http://peer-cache.example.internal:8080 \
  --metrics single=http://single-cache.example.internal:8080/metrics \
  --metrics peer=http://peer-cache.example.internal:8080/metrics \
  --bucket "$S3CACHE_S3_BUCKET" \
  --object-size 64MiB \
  --range-size 256KiB \
  --range-count 256 \
  --requests 64 \
  --concurrency 8 \
  --output perf-results.json
```

The script uses the same environment conventions as the e2e tests:

```dotenv
S3CACHE_S3_BUCKET=your-test-bucket
S3CACHE_S3_ENDPOINT_URL=http://rustfs.example.internal:9000
S3CACHE_S3_REGION=us-east-1
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```

It uploads fresh objects under a unique prefix, reads them through each target,
records latency and throughput summaries, optionally scrapes metrics before and
after each target, and deletes the objects unless `--keep-objects` is set.

## Workloads

The runner executes four read workloads per target:

- `large_stream_cold`: one full-object read against a fresh object key.
- `large_stream_warm`: repeated full-object reads against the same key.
- `sparse_range_cold`: scattered single-range reads across a fresh object key.
- `sparse_range_warm`: repeated scattered single-range reads after the cold pass.

Each target receives distinct object keys under the same prefix. This avoids one
target warming the same cache entries that another target later measures.

## Interpreting Results

Compare modes by workload, not by one aggregate number.

For `single`, look for the baseline hit latency, upstream miss cost, and the
point where one pod's disk or network saturates.

For `peer`, look for improved aggregate throughput or capacity compared with
`single`. With `read_sharding: auto`, small objects should behave close to the
object strategy while large-object workloads should use distributed page
sharding.

Metrics deltas are included when `--metrics NAME=URL` is provided. The most
useful checks are:

- page hits and misses
- bytes served from cache versus upstream
- upstream fill bytes
- peer fanout and page-owner request metrics
- page batch size and coalesced fill metrics
- invalidation broadcast failures
- peer ring info consistency across processes

## Methodology Notes

Cold-cache numbers are only meaningful with fresh object keys or empty cache
state. The runner creates fresh keys by default, which is usually enough. If you
reuse `--prefix --skip-upload`, restart or clear the cache first if you need cold
numbers.

Run each mode more than once before drawing conclusions. Use object sizes,
range sizes, request counts, and concurrency that resemble the real workload
being evaluated.

Changing the peer list changes page ownership and cold-starts moved pages. Do not
include a peer-list rollout in the same measurement window as a latency or
throughput comparison.
