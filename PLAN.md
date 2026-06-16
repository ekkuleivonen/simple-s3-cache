# Implementation Plan

`simple-s3-cache` is a small S3-compatible page-based read-through cache written
in Go. It sits between trusted clients and an upstream S3-compatible object
store.

The upstream store remains the source of truth. The local cache is disposable.

Clients should not need to know this proxy is sitting in front of the real S3
backend for the supported production scope: path-style object
`GET`/`HEAD`/single-range reads, with all writes passing through and
invalidating.

Think nginx cache, but for S3 object reads.

Cross-cutting risks that must stay visible throughout implementation are tracked
in `HAZARDS.md`. Several of them (mixed-ETag pages, invalidation races, chunked
PUT signing) are load-bearing for the consistency and transparency claims, not
polish.

## Design Priorities

See [README.md](README.md) for the product contract: design priorities, target
workloads (DuckDB, Polars, Spark, Arrow, Parquet readers, SFTPGo, and general
S3-backed apps), and when the cache is safe to use. This document covers *how*
that contract is implemented.

The one priority that drives the implementation: optimize the safe read paths
without narrowing the product to a single workload. Unsupported or uncacheable
S3 behavior passes through to upstream.

## Design Implications

The cache should optimize the safe read paths without narrowing the product to
one workload.

- Use a page cache internally so range reads and full-object reads share the
  same storage model.
- Preserve S3-compatible request and response behavior as much as possible.
- Pass through writes, bucket operations, multipart operations, and unsupported
  read shapes rather than emulating more S3 behavior than necessary.
- Treat local SQLite and page files as disposable cache state.
- Keep concurrency correctness explicit: multiple clients may request the same
  object, range, or missing page at the same time.
- Target a single active cache instance. Do not add distributed
  invalidation, cache ownership, shared cache storage, or external coordination.
- Avoid clever cache policy. Fetch pages only when requested.

## Deployment Model

Single active cache instance. See [README.md](README.md#deployment-model) for the
topology and the multi-instance staleness caveat (also hazard H11). The
implementation consequence is that invalidation is purely local: successful
writes through the cache invalidate local pages and metadata, and no distributed
coordination is required.

### Failure Model

If the cache instance fails, it can be restarted with an empty cache.

Consequences:

- there may be a temporary interruption during restart
- the cache is cold after recovery
- upstream load may increase while the cache warms

No object data is lost because upstream S3-compatible storage remains the
source of truth.

## Core Decisions

### Authentication

See [README.md](README.md#authentication) for the auth contract. Implementation
implications: client signatures are not validated or forwarded; every upstream
request is re-signed with the single configured credential, so all clients share
its permissions. Presigned-URL reads (signature in the query string) are
classified as pass-through, never cached.

### Cache Scope

Only object reads and metadata reads are cached:

- `GET Object`
- `HEAD Object`
- single-range `GET` requests

`HEAD` caching is intentionally in scope. Many S3-compatible analytical clients
issue `HEAD` before range reads, so metadata caching is part of correct support
for the target workloads. If cached metadata cannot satisfy a `HEAD` or
conditional request transparently, pass the request through to upstream.

Multi-range `GET` requests pass through.

All writes and all bucket-level operations pass through to upstream.

Successful writes invalidate cached data for the affected object.

Only "plain" object reads are cached. A `GET` or `HEAD` that carries a
subresource or response-shaping query parameter is passed through, never
cached as object data. Examples that must pass through:

- `?versionId` (versioned reads are not cached)
- `?acl`, `?tagging`, `?attributes`, `?retention`, `?legal-hold`, `?torrent`
- `?uploadId`, `?partNumber` (multipart)
- response override parameters such as `response-content-type`,
  `response-content-disposition`, `response-cache-control`,
  `response-content-language`, `response-content-encoding`, and
  `response-expires`

Classification must not treat "has bucket + key" as sufficient to cache. See
hazard H3 in `HAZARDS.md`.

Requests using SSE-C / customer-provided encryption headers pass through.
Caching decrypted object bytes or replaying encryption-sensitive headers would
expand the trust and correctness model beyond the production scope. See hazard
H12.

Client conditional headers (`If-None-Match`, `If-Modified-Since`, `If-Match`,
`If-Unmodified-Since`) must be honored when serving from cached metadata, or
the request must be passed through. Serving a `200` where upstream would return
`304` breaks transparency. See hazard H4.

### Storage Model

Objects are cached as fixed-size pages rather than complete files. Only
requested pages are stored locally.

Cached pages are stored under the configured `cache.cache_path`. A cached object
may be fully cached, partially cached, or not cached.

The cache also keeps a local SQLite index under the configured
`cache.meta_path`. SQLite is part of the disposable cache, not a source of truth.

The cache paths contain:

- zero or more immutable page files
- `cache.db`, which tracks objects, pages, headers, sizes, and access times

Example:

```text
/cache/
  meta/
    cache.db
  objects/
    ab/cd/object-hash/
      page-000000
      page-000001
      page-000002
```

Object directories are addressed by a stable hash of bucket and object key,
avoiding filesystem path edge cases and excessive directory fanout.

Pages are immutable. A page is either present and complete, or absent.

Writes to the cache use temporary files followed by atomic rename. Partial or
failed page downloads must never become visible as cache hits.

Every page belongs to a specific object version, identified by the object
`ETag`. Pages from different ETags must never be assembled into the same
response. Page fetches use `If-Match: <etag>`; a `412` means the object changed
and the cached entry is invalidated and refetched. This guard, combined with a
per-object epoch counter on invalidation, is what keeps the page cache
consistent under concurrent reads and writes. See hazards H1 and H2 in
`HAZARDS.md`.

## Go Project Shape

Suggested package layout:

```text
cmd/simple-s3-cache/
  main.go

internal/config/
  config.go

internal/server/
  server.go
  routes.go
  request.go

internal/upstream/
  client.go
  signer.go

internal/cache/
  cache.go
  index.go
  page.go
  key.go
  eviction.go

internal/metrics/
  metrics.go
```

### `cmd/simple-s3-cache`

Process entrypoint.

Responsibilities:

- load configuration
- initialize upstream client
- initialize disk cache
- start HTTP server
- handle shutdown

### `internal/config`

Configuration parsing and validation.

Initial config:

```yaml
listen: ":8080"

upstream:
  endpoint: http://rustfs:9000
  region: us-east-1
  access_key: simple-s3-cache
  secret_key: change-me
  path_style: true

cache:
  cache_path: /cache/objects
  meta_path: /cache/meta
  max_size: 1TB
  page_size: 4MB
```

Page size is the primary tuning knob and the most important performance choice.
Larger pages reduce metadata overhead but amplify over-fetch: a tiny scattered
read (e.g. a Parquet footer far smaller than the page) still pulls and stores a
whole page. The default of 4 MB favors the analytical/random-access workloads we
target over metadata compactness. Measure read amplification and hit rate per
workload before committing to a different default. See hazard H9 in
`HAZARDS.md`.

Environment variable overrides are optional; file-based configuration is enough
for production readiness.

### Tuning Strategy

Production deployments use a single global `page_size` and a single global
`max_size`. There is no per-bucket page size and no per-bucket cache quota.

This is intentional. Different buckets often have different read patterns (small
scattered Parquet reads vs large sequential scans), and a global LRU pool can let
one hot bucket evict another's pages. Per-bucket knobs would address that, but
they add config surface and operator footguns before we know whether the
workload needs them.

Instead, production readiness requires conservative global defaults and
observability that exposes enough data to tune from evidence, not guesses.

The SQLite index stores `page_size` per object (set at first cache from the
global default), so changing page-size behavior later would not require a
storage redesign.

### `internal/server`

HTTP-facing S3-compatible behavior.

Responsibilities:

- parse bucket and object key from path-style requests
- classify requests as cacheable reads, invalidating writes, or pass-through
- preserve request headers that matter to S3 semantics
- preserve upstream status codes and error bodies
- keep clients unaware of whether a response came from cache or upstream
- serve locally available pages from disk
- fetch missing pages from upstream
- assemble full-object and single-range responses from cached and fetched pages

The first version should prefer path-style addressing:

```text
GET /bucket/key
HEAD /bucket/key
PUT /bucket/key
DELETE /bucket/key
```

Virtual-hosted-style addressing is not part of the production scope.

### `internal/upstream`

Small upstream S3 HTTP client.

Responsibilities:

- build upstream URLs
- sign upstream requests with configured AWS Signature V4 credentials
- forward request bodies for pass-through operations
- return upstream responses without translating S3 errors

This package should not know about cache policy.

Prefer the `aws-sdk-go-v2` signer over a hand-rolled one.

Pass-through PUTs need care: most AWS SDKs default to chunked, streaming-signed
request bodies (`Content-Encoding: aws-chunked`,
`x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD`). Because we ignore
the client signature and re-sign with our own credential, forwarding the
chunk-framed body verbatim will produce a body/signature mismatch upstream. We
must either pass the streaming headers through untouched without re-hashing the
payload, or de-chunk the body before re-signing. This must be exercised by an
integration test using a real SDK with default settings. See hazard H5 in
`HAZARDS.md`.

### `internal/cache`

Disk cache implementation.

Responsibilities:

- map `(bucket, key)` to cache paths
- read and write the SQLite cache index
- track which pages are present locally
- open cached pages for serving
- store fetched pages atomically
- coalesce concurrent fetches for the same missing page
- fence stores against invalidation using a per-object epoch and the object
  `ETag`, so a fetch that completes after an invalidation is discarded
- invalidate objects (bumps the object epoch)
- track approximate cache size
- evict old entries when over `max_size`

Eviction is least-recently-used by page access time recorded in SQLite. Run it
in a background sweeper rather than inline in the request path, so it does not
add latency or compete with the request hot path for the single SQLite writer.
A successful cache write may signal the sweeper, but should not block on it.
See hazard H8 in `HAZARDS.md`.

## Concurrency Model

Go's HTTP server should handle concurrent clients normally: each request runs in
its own goroutine. Cache correctness must not depend on single-client access.

Expected behavior:

- Concurrent reads of already cached pages proceed independently.
- Concurrent requests for the same missing page are coalesced so only one
  upstream fetch writes that page.
- Other requests waiting for the same page should either wait for the in-flight
  fetch or re-check the cache after it completes.
- Page files become visible only after a successful temporary-file write and
  atomic rename.
- SQLite rows are inserted only after the corresponding page file is complete.
- If a page file appears before its SQLite row due to a crash, it is treated as
  an orphan and ignored.
- A page is committed only if the object epoch captured at fetch start is
  unchanged and the fetched `ETag` matches the stored object `ETag`. Otherwise
  the fetched page is discarded. This closes the invalidation-vs-in-flight-fetch
  race (hazards H1, H2).

Use a `singleflight`-style mechanism keyed by object hash and page number.

Each object carries an epoch counter. Invalidation bumps the epoch and removes
rows and files. A fetch captures the epoch before contacting upstream and
re-checks it (under the index lock) before committing, so a write that lands
mid-fetch cannot be overwritten by a stale page.

SQLite should be configured for concurrent read-heavy access:

- enable WAL mode
- set a `busy_timeout`
- keep transactions short
- index object and page lookups
- store page data in files, not SQLite blobs

SQLite permits many concurrent readers but only one writer at a time. Avoid
turning cache hits into SQLite write storms. Access times can be updated
opportunistically, batched in memory, or sampled rather than written on every
page hit.

The implementation must not run `UPDATE pages SET last_accessed_at = ...` on
every cache hit. Treat LRU timestamps as approximate: buffer dirty access marks
in memory, flush them periodically, and/or sample hits so the request hot path
stays read-mostly. See hazard H7 in `HAZARDS.md`.

## Request Behavior

### GET Object

Flow:

1. Parse bucket and key.
2. Ensure object metadata is available.
3. Determine the requested byte range.
4. Translate the byte range into page numbers.
5. Serve locally available pages from disk.
6. Fetch missing pages from upstream using range requests.
7. Store newly fetched pages on disk.
8. Assemble and return the requested response.

The same code path serves:

- full-object `GET` requests
- single-range `GET` requests

A full-object request does not require a separate full-object cache path. It is
served as a request for all pages in the object.

Full-object responses stream page-by-page. If page 0 is cached and page 1 is
missing, the handler may send page 0 from disk and fetch page 1 from upstream as
the response progresses; it must not wait to prefetch every missing page before
starting the response. This keeps memory usage bounded for very large objects.
Once headers are committed, mid-stream upstream failures follow H16.

If metadata is missing before a `GET`, fetch it from upstream first. This keeps
range parsing, final-page sizing, and response headers consistent.

Upstream page fetches send `If-Match: <etag>` so a mid-fetch change to the
object is detected (`412`) rather than silently producing a mixed-version
response.

If an upstream page fetch fails after response headers have been sent (a large
multi-page `GET` that fails partway), the connection must be aborted so the
client detects a short read; the cache must never emit a success status with a
`Content-Length` it cannot fulfill. Where practical, validate the first required
upstream fetch before committing response headers. See hazard H16 in
`HAZARDS.md`.

Honor client conditional headers before serving. A cached `GET`/`HEAD` that
would be a `304` upstream must return `304`.

### HEAD Object

Flow:

1. If object metadata is cached, serve headers from metadata.
2. Otherwise forward to upstream.
3. Store successful upstream `HEAD` metadata.

`HEAD` never requires object data pages.

`HEAD` caching remains in scope because analytical clients often issue `HEAD`
requests before range reads. Cached metadata and cached page data share the same
consistency assumptions.

If cached metadata is missing, incomplete, stale under the production contract,
or unable to honor client conditionals exactly, the handler should pass through
to upstream rather than approximate S3 behavior.

`HEAD` responses may become stale if objects are modified directly in upstream
storage outside of `simple-s3-cache`.

### Range Requests

Range requests are first-class cacheable operations.

Default page size:

```yaml
cache:
  page_size: 4MB
```

Single range request example:

```text
GET /bucket/file.parquet
Range: bytes=33554432-50331647
```

Flow:

1. Parse bucket and key.
2. Ensure object metadata is available.
3. Determine the requested byte range.
4. Translate the byte range into page numbers.
5. Check the local page inventory.
6. Serve locally available pages.
7. Fetch missing pages from upstream using range requests.
8. Store newly fetched pages on disk.
9. Assemble and return the requested range.

Future requests for overlapping ranges should reuse cached pages.

Serving single ranges must preserve:

- `206 Partial Content`
- `Content-Range`
- `Content-Length`
- `Accept-Ranges`
- relevant object metadata headers

Multi-range requests are pass-through.

### Large Objects

Objects larger than available cache space are supported.

Only accessed pages are stored. For example, reading the first 64 MB of a
100 GB object with a 16 MB page size stores 4 pages, not the full object. (The
default page size is 4 MB; this example uses 16 MB for round numbers.)

### Cache Efficiency

This design is optimized for analytical and random-access workloads, including:

- Parquet
- Arrow
- DuckDB
- Polars
- Spark
- video seeking
- random access archives

These workloads often make repeated range reads against a small subset of a
large object. Caching pages instead of complete objects improves cache
utilization and avoids unnecessary upstream reads.

### PUT Object

Flow:

1. Forward request body to upstream.
2. Return upstream response to the client.
3. If upstream reports success, invalidate the cached object.

### DELETE Object

Flow:

1. Forward request to upstream.
2. Return upstream response to the client.
3. If upstream reports success, invalidate the cached object.

### COPY Object

S3 copy writes a destination object and may read a source object. Copy is not a
distinct HTTP verb: clients issue it as a `PUT` (or `UploadPartCopy`) carrying an
`x-amz-copy-source` header. Classification must detect that header rather than
relying on a dedicated COPY route. See hazard H15 in `HAZARDS.md`.

Behavior:

- pass through to upstream
- on success, invalidate the destination object
- do not invalidate the source object

### Multipart Uploads

Behavior:

- pass through all multipart operations
- invalidate the target object after successful `CompleteMultipartUpload`
- invalidate the target object after successful `AbortMultipartUpload`

Part uploads should not be cached.

### Bucket Operations

Bucket operations are pass-through.

Examples:

- list buckets
- create bucket
- delete bucket
- list objects
- bucket policy operations

The cache should not try to infer object invalidation from bucket operations.

## Cache Index

Metadata and page inventory are stored in a local SQLite database:

```text
/cache/cache.db
```

This is an embedded disposable index, not an external metadata database. At
large cache sizes, such as 4 TB with 1M objects, SQLite keeps startup,
eviction, and page lookups practical without turning the cache into a separate
storage system.

Suggested tables:

```sql
CREATE TABLE objects (
  id INTEGER PRIMARY KEY,
  bucket TEXT NOT NULL,
  key TEXT NOT NULL,
  object_hash TEXT NOT NULL UNIQUE,
  etag TEXT,
  size INTEGER NOT NULL,
  page_size INTEGER NOT NULL,
  content_type TEXT,
  last_modified TEXT,
  headers_json TEXT NOT NULL,
  epoch INTEGER NOT NULL DEFAULT 0,
  stored_at TEXT NOT NULL,
  last_accessed_at TEXT NOT NULL,
  UNIQUE(bucket, key)
);

CREATE TABLE pages (
  object_id INTEGER NOT NULL,
  page_number INTEGER NOT NULL,
  size INTEGER NOT NULL,
  path TEXT NOT NULL,
  stored_at TEXT NOT NULL,
  last_accessed_at TEXT NOT NULL,
  PRIMARY KEY (object_id, page_number),
  FOREIGN KEY (object_id) REFERENCES objects(id) ON DELETE CASCADE
);

CREATE INDEX pages_lru_idx ON pages(last_accessed_at);
CREATE INDEX objects_lookup_idx ON objects(bucket, key);
```

The cache never needs a separate flag for whether the entire object is cached.
It only needs to know which page rows exist.

Do not store hop-by-hop HTTP headers.

Examples of headers to avoid storing:

- `Connection`
- `Transfer-Encoding`
- `Keep-Alive`
- `Proxy-Authenticate`
- `Proxy-Authorization`
- `TE`
- `Trailer`
- `Upgrade`

Store the full set of object metadata headers needed to replay a transparent
response on cache hits, for both full-object and range responses:
`Content-Type`, `Content-Encoding`, `Cache-Control`, `Content-Disposition`,
`x-amz-meta-*`, server-side-encryption headers, storage class,
`x-amz-version-id`, and the object `ETag`. Losing these on range responses is a
common transparency bug. See hazard H6 in `HAZARDS.md`.

### Index Recovery

The cache should tolerate index and filesystem drift.

If `cache.db` is missing or corrupt, the cache may delete all page files and
start with an empty cache. Rebuilding the index by scanning page files is not
required for correctness.

If SQLite says a page exists but the page file is missing, treat it as a cache
miss and remove the stale row.

If a page file exists without a matching SQLite row, ignore it. A later sweeper
can delete orphaned files.

If page rows exist without a usable object metadata row, they are not cache hits.
Delete or ignore the page rows and refetch metadata from upstream. Correctness
depends on object metadata and page inventory being coherent enough to prove
which `ETag`, size, headers, and page size the bytes belong to. See hazard H17
in `HAZARDS.md`.

### Cache Write Failure

Cache storage failure must not turn a readable upstream object into a failed
client read unless the upstream read itself failed.

If a page cannot be stored because the cache data path is full, SQLite is
locked, permissions changed, or a disk write fails:

- continue streaming the upstream response to the client when possible
- skip committing the failed page
- record a cache write failure metric and structured log field
- let later requests retry the page as a miss

Eviction may be signaled after a failed store, but request success must not
depend on synchronous eviction. See hazards H8 and H13.

## Consistency

The cache is allowed to be stale if upstream changes bypass
`simple-s3-cache`.

If all writes pass through `simple-s3-cache`, successful write invalidation
keeps future reads fresh for affected objects.

Two consistency mechanisms are part of the production contract and are not
optional:

- `If-Match: <etag>` on upstream page fetches, so pages from different object
  versions are never assembled into one response (hazard H1)
- a per-object epoch that fences in-flight fetches against concurrent
  invalidation (hazard H2)

Proactive revalidation of already-cached objects is not part of the production
contract. Deployments that allow out-of-band upstream writes must accept stale
cached reads until invalidation or eviction.

## Limitations

See [README.md](README.md#limitations). In short: writes must pass through the
cache to stay fresh (there is no freshness TTL — hazard H14), objects modified
out-of-band may remain stale until invalidated or evicted, and multiple active
instances do not coordinate.

## Production Readiness Checklist

The project is production-deployable when all items below are satisfied. After
that point, changes should be steady-state development, compatibility fixes,
operational polish, and performance optimization. They should not require a
major rewrite of the storage model, consistency model, deployment model, or
request contract.

### Request Contract

- Path-style bucket/key parsing is implemented and tested.
- Plain `GET`, `HEAD`, and single-range `GET` are cached transparently.
- Full-object `GET` streams through the page cache without prefetching the
  entire object before responding.
- Multi-range `GET`, subresources, versioned reads, response override
  parameters, SSE-C requests, bucket operations, and unsupported request shapes
  pass through without being cached.
- Client conditional headers are honored from cached metadata, or the request is
  passed through when behavior is uncertain.
- Upstream status codes, error bodies, and relevant response headers are
  preserved.

### Write Path And Invalidation

- `PUT`, `DELETE`, `COPY`, and multipart operations pass through to upstream.
- Successful writes invalidate affected cached object metadata and pages.
- `COPY` invalidates the destination object only.
- Multipart completion invalidates the target object.
- Streaming/chunked SDK-default PUT uploads are covered by an integration test.

### Consistency And Concurrency

- Pages are immutable and become visible only after complete write plus atomic
  rename.
- SQLite page rows are committed only after the corresponding page file is
  complete.
- Upstream page fetches use `If-Match` against the cached object `ETag`.
- Page commit is fenced by per-object epoch so in-flight fetches cannot
  resurrect invalidated data.
- Concurrent misses for the same page are coalesced.
- Cache hits do not require SQLite writes on the hot path; access-time updates
  are approximate, batched, buffered, or sampled.

### Storage And Failure Behavior

- Cache state is disposable: deleting the cache data or metadata paths never
  loses upstream data.
- Missing or corrupt `cache.db` starts cleanly with an empty cache.
- Missing page files, orphaned page files, interrupted downloads, and failed
  cache writes are tolerated.
- Page rows without usable object metadata are ignored or deleted, never served
  as hits.
- Disk-full or cache write failure degrades to a miss and does not fail a
  readable upstream object response when the response can still be streamed.
- Background eviction enforces `max_size` without blocking the request path.

### Operations

- Configuration validates listen address, upstream endpoint, credentials,
  cache path, `max_size`, and `page_size`.
- Health/readiness endpoints exist.
- Graceful shutdown does not corrupt in-flight cache writes.
- Structured logs include request method, bucket, key, cache result, status,
  bytes requested, bytes sent, bytes fetched upstream, and upstream duration.
- Metrics expose hit/miss counts, pass-through counts, invalidations, cache
  write failures, evictions, upstream failures, cache bytes, upstream fill
  bytes, and read amplification, with bucket labels where practical.
- Deployment docs clearly state the single-active-instance, trusted-network,
  path-style, one-upstream-credential assumptions.

### Testing

- Unit tests cover request classification, range/page math, cache key hashing,
  SQLite index behavior, invalidation, singleflight keying, and eviction
  selection.
- Integration tests run against at least one local S3-compatible backend.
- Failure tests cover interrupted downloads, index corruption, missing files,
  disk-full behavior, cache write failures, in-flight invalidation, and
  mid-fetch object mutation.
- A compatibility pass checks behavior against AWS S3 semantics for request
  classification, conditionals, range responses, metadata headers, and
  streaming PUTs.

## Eviction

The cache has a configured maximum size.

Initial eviction strategy:

1. Track page size and approximate last access time in SQLite.
2. Buffer, batch, or sample access-time updates instead of writing on every hit.
3. Estimate total cache size; a successful cache write may signal the sweeper.
4. In a background sweeper (not the request path), if over limit, delete
   least-recently-used pages until below limit.
5. Delete page files and their SQLite rows together.

Keeping eviction off the request path avoids tail-latency spikes and contention
for the single SQLite writer. See hazard H8 in `HAZARDS.md`.

The cache must continue to work if files disappear while the process is
running.

If the cache is over limit and the sweeper cannot free space quickly enough,
page stores may fail or be skipped. This should reduce hit rate, not break
object reads. See hazard H13.

## Observability

Expose logs and metrics from the beginning. Production deployments keep global
tuning knobs; the point of observability is to make steady-state tuning and
performance optimization evidence-driven.

### Core counters

Global totals, plus the same counters **labeled by `bucket`** wherever cheap:

- page hits
- page misses
- pass-through requests
- invalidations
- cache write failures
- evictions
- upstream request failures
- bytes served from cache
- bytes served from upstream
- **bytes fetched upstream to fill cache** (on misses; may exceed client
  requested bytes because whole pages are pulled)

Derived ratios worth exposing or computing in dashboards:

- **hit rate** — page hits / (page hits + page misses)
- **read amplification** — upstream bytes fetched to fill cache ÷ client bytes
  requested (per request and aggregated)

### Gauges

- total cached bytes vs configured `max_size`
- **cached bytes by bucket** (needed to spot quota candidates)

### Histograms

- requested range size
- pages touched per request
- read amplification per request
- upstream duration on miss vs serve-from-disk on hit

### Request log fields

Structured logs should carry enough per-request detail to compute amplification
and bucket-level breakdowns offline if not all series are exported as metrics:

- method
- bucket
- key
- cache result
- requested range
- **bytes requested** (client range size)
- pages requested
- pages hit
- pages missed
- status code
- bytes sent
- **bytes fetched upstream** (on misses)
- upstream duration

### How to read the signals

| Observation | Evidence it provides |
|---|---|
| High read amplification + small ranges on one bucket | The global page size is likely too large for that access pattern; consider lowering the global default |
| High evictions on one bucket while others stay cold | The shared LRU pool is contended; revisit global `max_size` (per-bucket quotas are out of scope) |
| Good hit rate but huge upstream fill bytes | Page size too large for the access pattern |
| Good hit rate, low amplification | Global defaults are fine; do not tune prematurely |

## Testing Strategy

### Reference Backends

The first integration target should be one local S3-compatible backend, such as
MinIO or RustFS, run in CI or a local docker-compose environment.

Use the reference backend matrix to avoid accidentally depending on one
implementation's quirks:

- **Required for production readiness:** one local backend that covers
  pass-through requests, object metadata, single ranges, conditional requests,
  multipart completion, and SDK-default streaming PUTs.
- **Compatibility target:** AWS S3 behavior for request classification,
  conditionals, range responses, and metadata headers, even if AWS-backed tests
  are not part of every local run.
- **Compatibility expansion:** additional S3-compatible backends when users
  report compatibility issues or when backend-specific behavior affects
  correctness.

### Unit Tests

Cover:

- request classification
- bucket/key parsing
- cache key hashing
- SQLite index read/write
- range calculation
- page calculation
- page inventory updates
- singleflight keying
- invalidation behavior
- eviction selection

### Integration Tests

Use a local S3-compatible backend such as MinIO or RustFS.

Cover:

- full-object GET miss stores pages
- full-object GET hit serves pages from disk
- HEAD hit serves metadata
- single range miss fetches and stores missing pages
- single range hit serves `206` from disk
- overlapping ranges reuse cached pages
- multi-range requests pass through
- GET subresource requests (e.g. `?tagging`, `?versionId`) pass through and are
  not cached as object data
- response override query parameters pass through and are not cached as object
  data
- SSE-C requests pass through and are not cached
- client conditional GET/HEAD returns `304` from cached metadata
- concurrent requests for the same missing page share one upstream fetch
- repeated full-object reads work for file-serving clients
- streaming/chunked PUT from a default-configured AWS SDK passes through and
  uploads correctly
- PUT invalidates cache
- DELETE invalidates cache
- multipart complete invalidates cache
- upstream errors pass through unchanged

### Failure Tests

Cover:

- interrupted upstream download does not create visible pages
- cache data or metadata path deletion does not lose upstream data
- missing or corrupt `cache.db` starts with an empty cache
- stale SQLite page rows are ignored and removed
- orphaned page files are ignored
- disk write failure still returns upstream response when possible
- disk-full cache writes skip storing and continue serving from upstream when
  possible
- concurrent failed page fetches do not leave visible partial pages
- invalidation during an in-flight fetch does not resurrect a stale page
  (epoch fence)
- an object changed mid-fetch yields a clean refetch, never a mixed-version
  response (`If-Match` / `412`)

## Milestones

### Milestone 1: Skeleton

- initialize Go module
- add config loading
- start HTTP server
- add health endpoint
- add basic request logging

### Milestone 2: Pass-Through Proxy

- parse path-style S3 requests
- classify requests; pass through GET subresources and `?versionId`
- sign upstream requests
- forward all methods to upstream, including streaming/chunked PUT bodies
- preserve response status, headers, and bodies
- add integration test with local S3-compatible backend

### Milestone 3: Page Cache Core

- add SQLite cache index
- add object rows and page files
- add page inventory tracking through SQLite
- atomically store fetched pages
- ignore incomplete or corrupt pages
- coalesce concurrent fetches for the same missing page
- fetch pages with `If-Match` and fence stores with a per-object epoch

### Milestone 4: GET, HEAD, and Single Range Cache

- serve `HEAD` from cached metadata
- serve full-object `GET` through the page cache
- serve single range requests through the page cache
- honor client conditional requests (`304` from cached metadata)
- fetch and store missing pages from upstream
- pass through multi-range requests
- preserve transparent S3 response headers and status codes

### Milestone 5: Invalidation

- invalidate cached object after successful `PUT`
- invalidate cached object after successful `DELETE`
- invalidate destination object after successful `COPY`
- invalidate after successful multipart completion

### Milestone 6: Size Limit and Eviction

- track cache size
- enforce `max_size`
- implement background LRU eviction, out of the request hot path
- tolerate missing or corrupt cache files

### Milestone 7: Operations Polish

- add metrics endpoint with bucket-labeled counters and read-amplification data
- add structured logs with bytes requested and bytes fetched upstream
- add graceful shutdown
- document deployment assumptions
- document known limitations and production tuning strategy

## Implementation Boundaries

Keep the production contract boring:

- path-style S3 only
- one upstream credential
- page-based cache entries
- global `page_size` and global `max_size` only (no per-bucket tuning)
- single-range support
- no distributed coordination
- no external metadata database
- no proactive revalidation of already-cached objects (the `If-Match` guard on
  page fetches still applies)
- no prefetching
- no compression

After the production readiness checklist is complete, the project should be in
steady-state development: compatibility fixes, operational polish, and measured
performance optimizations rather than major product-shape changes.
