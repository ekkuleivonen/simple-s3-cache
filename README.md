# simple-s3-cache

A boring, transparent, page-based read-through cache for S3-compatible object
storage.

Clients should not need to know this is sitting in front of the real S3
backend for the supported production scope: path-style object reads (`GET`,
`HEAD`, single-range `GET`) with all writes passing through and invalidating.

Think nginx cache, but for S3 object reads.

## Goals

* Cache object reads on local NVMe.
* Pass all writes directly to upstream S3.
* Invalidate cached data on writes.
* Work with any S3-compatible backend.
* Require no client changes.
* Use minimal resources.
* Be easy to understand and operate.

## Design Priorities

simple-s3-cache should be:

* Performant for repeated reads, range-heavy reads, and large objects.
* Reliable under concurrent client access.
* Boring to deploy, inspect, and operate.
* Transparent to S3-compatible clients.
* Useful across many S3-backed applications.

This includes analytical clients like DuckDB, Polars, Spark, Arrow, and
Parquet readers, as well as services such as SFTPGo or other applications built
on top of S3-compatible storage.

Analytical workloads are likely to benefit the most because they repeatedly
read small byte ranges from large objects, but the proxy should remain general
purpose.

## Production Scope

The production contract is intentionally narrow:

* Path-style S3 requests only (`/bucket/key`).
* One active cache instance.
* One configured upstream credential shared by all clients.
* Trusted network or external authentication layer.
* Writes must pass through simple-s3-cache for fresh cached reads.
* Plain object `GET`, `HEAD`, and single-range `GET` are the only cached
  operations.

Anyone who can reach simple-s3-cache effectively gets the configured upstream
credential's permissions. Do not expose it directly to untrusted clients.

Unsupported S3 behavior should pass through to upstream whenever possible.

The project is production-deployable when the scope above is implemented,
tested, observable, and documented clearly enough that remaining work is steady
state development and performance tuning, not a change to the product contract.

## Non-Goals

* Distributed cache.
* High availability.
* Write-back caching.
* External metadata databases.
* Object replication.
* Tiering.
* Deduplication.
* Compression.
* Prefetching.
* POSIX filesystems.
* Being clever.

## Authentication

simple-s3-cache does not implement S3 authentication or authorization.

Clients connect directly to simple-s3-cache from a trusted network or through an
existing authentication layer.

This is a hard deployment requirement, not an optional best practice. Because
the proxy ignores client signatures and re-signs upstream requests with one
configured credential, it must be protected by network policy, ingress auth,
VPN, mTLS, or another external control appropriate for the deployment.

simple-s3-cache does not forward or validate client AWS Signature V4
credentials.

All upstream access uses a single configured credential.

Validating incoming AWS Signature V4 requests, IAM policies, and multi-tenant
authorization is not implemented.

Because client signatures are ignored and every upstream request is re-signed
with the configured credential, simple-s3-cache is transparent to the *data
plane*, not the *auth plane*: it relocates authentication to the trusted network
or an upstream auth layer.

Presigned-URL reads carry their signature in the query string and are treated as
pass-through (uncached); the embedded client signature is not used.

## Safe To Use When

simple-s3-cache is a good fit when:

* Clients can reach it through a trusted network or existing auth layer.
* A single cache instance is acceptable.
* Stale reads are unacceptable only for writes that pass through the cache.
* Clients use path-style S3 requests for object reads and writes.
* All clients may share the same upstream permissions.

Do not use simple-s3-cache as-is when:

* Multiple active cache replicas must stay coherent.
* Clients rely on per-user IAM authorization at the proxy.
* Objects may be modified directly in upstream storage and stale reads are not
  acceptable.
* Virtual-hosted-style S3 addressing is required.
* Unsupported S3 request semantics must be cached rather than passed through.

## Architecture

```text
Client
  ↓
simple-s3-cache
  ↓
S3-compatible storage
```

Cache hits are served from local NVMe.

Cache misses are fetched from upstream, streamed to the client, and stored locally.

Full-object `GET` responses use the same page path as range reads: cached pages
are streamed from disk, missing pages are fetched from upstream as needed, and
the cache does not prefetch the whole object before responding.

## Quickstart

Run simple-s3-cache in front of a local S3-compatible backend (RustFS shown;
MinIO works the same way):

```yaml
# docker-compose.yml
services:
  rustfs:
    image: rustfs/rustfs:latest
    ports: ["9000:9000"]

  cache:
    image: simple-s3-cache:latest
    depends_on: [rustfs]
    ports: ["8080:8080"]
    volumes:
      - ./simple-s3-cache.yaml:/etc/simple-s3-cache.yaml:ro
      - cache-data:/cache

volumes:
  cache-data:
```

With the configuration from [Configuration](#configuration), object reads go
through the cache:

```bash
# First read: miss, filled from upstream and stored on local NVMe.
curl -s http://localhost:8080/my-bucket/big.parquet -o /dev/null

# Repeat read or overlapping range: served from the local page cache.
curl -s -H 'Range: bytes=0-1048575' \
  http://localhost:8080/my-bucket/big.parquet -o /dev/null
```

Writes go to the same endpoint, pass through to upstream, and invalidate any
cached pages for the affected object on success.

## Deployment Model

simple-s3-cache is designed for a single active cache instance.

```text
Client
  ↓
LB / Ingress
  ↓
simple-s3-cache (1 replica)
  ↓
S3-compatible storage
```

This keeps cache invalidation local and avoids distributed coordination.

Multiple independent cache instances do not coordinate invalidation. If a write
is routed through one cache instance, other cache instances may continue serving
stale cached metadata or pages for the same object.

If the cache instance fails, it can be restarted with an empty cache. No object
data is lost because upstream S3-compatible storage remains the source of truth.

## Behavior

| Request | Production behavior |
|---|---|
| Plain `GET Object` | Cached as pages |
| Plain `HEAD Object` | Cached as object metadata; pass through if cached metadata cannot answer transparently |
| Single-range `GET Object` | Cached as pages |
| `PUT Object`, `DELETE Object`, `COPY Object` | Pass-through; invalidate affected cached object on success |
| Multipart upload operations | Pass-through; invalidate target on successful completion or abort |
| Bucket operations | Pass-through |
| Multi-range `GET Object` | Pass-through |
| Object subresources such as `?versionId`, `?acl`, `?tagging`, `?attributes` | Pass-through |
| Response override query parameters such as `response-content-type` | Pass-through |
| SSE-C / customer-provided encryption requests | Pass-through |
| Everything else | Pass-through |

Client conditional requests (`If-None-Match`, `If-Modified-Since`) are honored
against cached metadata so responses match upstream behavior.

### Invalidation

Successful writes immediately invalidate cached data for affected objects.

COPY invalidates the destination object only.

## Storage

Objects are cached as fixed-size pages rather than complete files.

Only requested pages are stored locally.

Cached object pages are stored on local disk.

A local SQLite index tracks cached pages and object metadata. It is part of the
disposable cache, not a source of truth.

The cache is disposable.

Deleting the cache data or metadata paths should never result in data loss.

## Consistency

The upstream S3 system is always the source of truth.

The cache never modifies objects.

The cache never stores writes.

Cached pages are tied to an object version via its `ETag`. Page fetches use
`If-Match`, and a per-object epoch fences in-flight fetches against concurrent
invalidation, so pages from different versions are never mixed and a write that
lands mid-read cannot be overwritten by a stale page.

There is no freshness TTL. A fully-cached object never issues a page miss, so the
`If-Match` guard never re-checks it; if a write bypasses the cache, that object
can stay stale until it is invalidated through the cache or evicted. See hazard
H14 in [HAZARDS.md](HAZARDS.md).

## Limitations

simple-s3-cache assumes writes pass through the cache.

Objects modified directly in upstream storage may remain stale until their
cached entries are invalidated or evicted.

HEAD responses may become stale if objects are modified directly in upstream
storage outside of simple-s3-cache.

Multiple active cache instances require distributed invalidation, cache
ownership, shared cache storage, or upstream validation checks. Those are not
part of this production scope.

The production scope supports path-style addressing only.

Operational and correctness risks to watch during development are tracked in
[HAZARDS.md](HAZARDS.md).

## Failure Behavior

simple-s3-cache returns its own error only when it cannot serve a request at all.
Whenever upstream bytes are obtainable, the cache degrades to a pass-through or a
miss rather than failing the client.

| Condition | Behavior |
|---|---|
| Upstream returns an error (4xx/5xx) | Pass the upstream status and body through unchanged |
| Cache write fails (disk full, locked, permissions) | Serve the upstream response, skip storing the page, record a cache-write-failure metric (H13) |
| `cache.db` missing or corrupt | Start with an empty cache; never lose upstream data |
| SQLite says a page exists but the file is gone | Treat as a miss, remove the stale row |
| Upstream fetch fails mid-stream, after headers are sent | Abort the connection so the client detects a short read; never pad or fake success (H16) |
| Cache instance restart | Cold cache; upstream load rises while it warms (H10) |

## Resource Usage

Typical deployments are expected to consume modest CPU and memory resources.

Actual usage depends on object size, request concurrency, page size, cache hit
rate, and upstream latency.

Most storage is consumed by cached page files.

## Configuration

```yaml
listen: ":8080"

upstream:
  endpoint: http://rustfs:9000
  region: us-east-1
  access_key: simple-s3-cache
  secret_key: change-me
  response_header_timeout: 30s

cache:
  cache_path: /cache/objects
  meta_path: /cache/meta
  max_size: 1TB
  page_size: 4MB

http:
  read_header_timeout: 5s
  read_timeout: 10m
  write_timeout: 10m
  idle_timeout: 2m

upload:
  spool_path: /cache/spool
  max_spool_size: 10GB
```

`cache_path` stores cached page files. `meta_path` stores the SQLite cache
index. Both are disposable cache state, but they may be placed on separate
volumes when that is operationally useful.

Page size is the primary tuning knob. Larger pages reduce metadata overhead but
amplify over-fetch on small scattered reads (a tiny Parquet footer still pulls a
whole page). The 4 MB default favors the analytical and random-access workloads
this cache targets.

Production deployments use one global `page_size` and one global `max_size` for
all buckets. Metrics and structured logs must expose enough data — read
amplification, hit rate, evictions, and cached bytes, broken down by bucket — to
guide steady-state tuning from evidence rather than guesses. See the Tuning
strategy and Observability sections in [PLAN.md](PLAN.md).

`upstream.response_header_timeout` bounds how long the proxy waits for upstream
S3-compatible storage to begin responding after a request is sent. The proxy
also configures upstream connection, TLS handshake, idle connection, and
expect-continue timeouts internally. `http.*_timeout` values bound client-facing
server connections; tune `read_timeout` and `write_timeout` high enough for the
largest expected uploads and downloads.

AWS SigV4 streaming uploads (`Content-Encoding: aws-chunked`) are decoded and
streamed directly upstream when `X-Amz-Decoded-Content-Length` is present. Other
unknown-length uploads are spooled to `upload.spool_path` before forwarding so
the proxy can re-sign them with a fixed content length. `upload.max_spool_size`
bounds that fallback disk usage.

## Observability

Metrics and structured logs are part of production readiness, not an
afterthought. They must answer:

- Are we over-fetching? (read amplification: upstream fill bytes vs client
  requested bytes)
- Which buckets drive misses, evictions, and cache usage?
- Is the global page size wrong for a specific access pattern?

Key signals include page hit/miss rate, bytes from cache vs upstream, bytes
fetched to fill cache, evictions, and cached bytes — globally and **per bucket**.
Full counter, gauge, histogram, and log-field requirements are in
[PLAN.md](PLAN.md#observability).

## Why?

Because sometimes you just want:

```text
NVMe cache
     ↓
Object storage
```

without introducing another storage system.

