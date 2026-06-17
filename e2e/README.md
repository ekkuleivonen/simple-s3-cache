# E2E Tests

The e2e harness is separate from `go test ./...`. It uses real S3-compatible
credentials from the local environment or the repository `.env`.

Real `.env` files are local developer state and must not be committed.

## Run

From this directory:

```bash
uv run pytest
```

## Environment

Required:

```dotenv
S3CACHE_S3_BUCKET=your-test-bucket
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```

Optional:

```dotenv
S3CACHE_S3_ENDPOINT_URL=http://localhost:9000
S3CACHE_S3_REGION=us-east-1
AWS_SESSION_TOKEN=...
S3CACHE_E2E_PREFIX=simple-s3-cache-e2e
```

The harness also accepts common aliases such as `S3_BUCKET`,
`S3_ENDPOINT`, `S3_ENDPOINT_URL`, `AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_S3`,
`MINIO_ENDPOINT`, `AWS_REGION`, and `AWS_DEFAULT_REGION`. Credential aliases
include `S3_ACCESS_KEY`, `S3_ACCESS_KEY_ID`, `S3_SECRET_KEY`,
`S3_SECRET_ACCESS_KEY`, `MINIO_ACCESS_KEY`, and `MINIO_SECRET_KEY`.

Each run writes objects under a generated prefix:

```text
<S3CACHE_E2E_PREFIX>/<run-id>/
```

Cleanup deletes only objects under that run prefix.

## Current Scope

The first e2e test validates direct backend access: `PUT`, `HEAD`, full `GET`,
single-range `GET`, and `DELETE`. Proxy e2e tests should reuse these fixtures
once pass-through proxy support exists.

## Performance Validation

Milestone-3 performance validation lives under `perf/` and is intentionally
separate from correctness e2e tests:

```bash
uv run python perf/s3_read_bench.py \
  --target single=http://single-cache.example.internal:8080 \
  --target peer=http://peer-cache.example.internal:8080 \
  --target gateway=http://gateway.example.internal:8080 \
  --bucket "$S3CACHE_S3_BUCKET" \
  --output perf-results.json
```

See [perf/README.md](perf/README.md) for the full workload matrix and guidance
for comparing single, direct peer, and gateway deployments.
