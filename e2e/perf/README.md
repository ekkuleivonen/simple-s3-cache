# Performance Validation

This directory contains the milestone-3 performance harness. It does not deploy
the cache. Deploy one or more supported topologies first, then run the same read
workloads against each endpoint:

- `single`: one cache instance, least operational complexity.
- `peer`: cache peers behind a Service, with possible peer-to-peer relay.
- `gateway`: stateless gateway routing directly to owner peers.

The goal is repeatable evidence, not a synthetic leaderboard. Run this in the
same network where real clients or compute jobs run.

## Run

From `e2e/`:

```bash
uv run python perf/s3_read_bench.py \
  --target single=http://single-cache.example.internal:8080 \
  --target peer=http://peer-cache.example.internal:8080 \
  --target gateway=http://gateway.example.internal:8080 \
  --metrics single=http://single-cache.example.internal:8080/metrics \
  --metrics peer=http://peer-cache.example.internal:8080/metrics \
  --metrics gateway=http://gateway.example.internal:8080/metrics \
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

Each target receives distinct object keys under the same prefix. This avoids a
direct peer endpoint warming the same cache entries that a gateway endpoint later
measures when both point at the same peer ring.

## Interpreting Results

Compare modes by workload, not by one aggregate number.

For `single`, look for the baseline hit latency, upstream miss cost, and the
point where one pod's disk or network saturates.

For direct `peer`, look for improved aggregate throughput or capacity compared
with `single`, and quantify the cost of non-owner relay. Peer mode is attractive
when avoiding a separate gateway matters more than removing that extra hop.

For `gateway`, look for similar cache-hit correctness and better large-response
or high-concurrency behavior than direct `peer`. Gateway mode should reduce
peer-to-peer relay overhead, but it is still static owner sharding and depends on
the same peer ring discipline.

Metrics deltas are included when `--metrics NAME=URL` is provided. The most
useful checks are:

- page hits and misses
- bytes served from cache versus upstream
- upstream fill bytes
- peer or gateway forwarding failures
- gateway response copy and body-read durations
- peer ring info consistency across processes

## Methodology Notes

Cold-cache numbers are only meaningful with fresh object keys or empty cache
state. The runner creates fresh keys by default, which is usually enough. If you
reuse `--prefix --skip-upload`, restart or clear the cache first if you need cold
numbers.

Run each topology more than once before drawing conclusions. Use object sizes,
range sizes, request counts, and concurrency that resemble the real workload
being evaluated.

Changing the peer list changes ownership and cold-starts moved objects. Do not
include a peer-list rollout in the same measurement window as a latency or
throughput comparison.
