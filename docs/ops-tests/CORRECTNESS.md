# Correctness Test

## Purpose

Prove that v2 peer mode does not serve stale or mixed-version bytes after
mutating S3 operations.

The key claim to validate:

- Mutating paths advance or invalidate object identity across the peer ring.
- Page owners serve only pages matching coordinator-supplied ETag and epoch.
- No stale page remains readable after a successful write/delete/COPY/multipart
  completion.

## Setup

Deploy peer mode with image tag
`ghcr.io/ekkuleivonen/simple-s3-cache:0.0.2`:

```yaml
peer:
  mode: peer
  read_sharding: auto
  page_sharding_min_pages: 2
```

Use compute-cluster test pods that can:

- write objects through the cache Service;
- read objects through the cache Service;
- optionally read directly from RustFS as the source-of-truth comparator.

All writes for this test must go through `simple-s3-cache`.

## Test Data Pattern

Use deterministic object bodies so stale reads are easy to detect.

Example:

- version A body: repeated byte pattern or checksum-marked chunks.
- version B body: different repeated byte pattern.
- large objects should span many cache pages.
- small objects should fit inside one page.

For range tests, verify both:

- returned bytes;
- returned `ETag`, `Content-Length`, and `Content-Range` where applicable.

## Scenarios

Run each scenario for both:

- small object: less than one configured page;
- large object: many configured pages.

### PUT overwrite

1. PUT version A through cache.
2. Read full object through cache until warm.
3. Read several ranges through cache until warm.
4. PUT version B through cache to the same key.
5. Immediately read full object through cache.
6. Immediately read ranges that overlap previously cached pages.
7. Compare every response to version B.

Expected:

- No version A bytes after the version B PUT succeeds.
- No mixed A/B response.
- Invalidation broadcast metrics show success for every peer.

### DELETE

1. PUT version A through cache.
2. Warm full and range reads.
3. DELETE object through cache.
4. Read full object through cache.
5. Read range through cache.

Expected:

- Reads after DELETE match upstream behavior, usually `404`.
- No peer serves cached version A bytes.

### COPY destination

1. PUT source object with version B bytes.
2. PUT destination object with version A bytes.
3. Warm destination through cache.
4. Issue S3 COPY to overwrite destination from source through cache.
5. Read destination full object and ranges through cache.

Expected:

- Destination reads return source/version B bytes.
- Destination cached version A pages are gone.
- Source object is not invalidated unless separately mutated.

### Multipart complete

1. Multipart upload version A through cache.
2. Warm full and range reads.
3. Multipart upload version B to the same key through cache.
4. Complete multipart upload.
5. Read full object and ranges through cache.

Expected:

- Reads after completion return version B bytes.
- No mixed old/new multipart pages.

### Conditional write success

1. PUT version A.
2. Capture ETag.
3. Warm reads.
4. Perform conditional overwrite that succeeds.
5. Read full and ranges.

Expected:

- Successful conditional write invalidates/advances epoch.
- Reads return the new object identity only.

### Conditional write failure

1. PUT version A.
2. Warm reads.
3. Perform conditional overwrite that fails upstream.
4. Read full and ranges.

Expected:

- Failed write does not invalidate the still-current version A cache.
- Reads remain consistent with upstream.

## Metrics To Capture

- `simple_s3_cache_invalidation_broadcasts_total`
- `simple_s3_cache_degraded`
- `simple_s3_cache_read_strategy_selected_total`
- `simple_s3_cache_page_owner_requests_total`
- `simple_s3_cache_peer_read_fallbacks_total`
- `simple_s3_cache_upstream_request_failures_total`
- `simple_s3_cache_page_hits_total`
- `simple_s3_cache_page_misses_total`

For every mutating operation, confirm invalidation broadcasts have `status`
showing success for all peers.

## Logs To Capture

Collect structured logs from all cache peers for:

- invalidation broadcast;
- epoch advance;
- page owner serve/fill;
- stale identity rejection;
- degraded/readiness changes.

There should be no unexpected degraded logs in the healthy correctness test.

## Pass Criteria

- No stale bytes after PUT, DELETE, COPY, multipart completion, overwrite, or
  successful conditional write.
- No mixed-version response under full or range reads.
- Failed conditional write does not invalidate the current object.
- Every peer applies invalidation/epoch advance for successful mutations.
- `simple_s3_cache_degraded` remains `0`.

## Failure Criteria

- Any read after a successful mutation returns old bytes.
- Any full or range response contains mixed old/new bytes.
- Any peer misses invalidation without becoming not-ready.
- Any internal stale identity is served instead of rejected/refilled.
