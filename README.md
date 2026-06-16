# simple-s3-cache

A boring read-through cache for S3-compatible object storage.

## Goals

* Cache object reads on local NVMe.
* Pass all writes directly to upstream S3.
* Invalidate cached objects on writes.
* Work with any S3-compatible backend.
* Require no client changes.
* Use minimal resources.
* Be easy to understand and operate.

## Non-Goals

* Distributed cache.
* High availability.
* Write-back caching.
* Metadata databases.
* Object replication.
* Tiering.
* Deduplication.
* Compression.
* Prefetching.
* POSIX filesystems.
* Being clever.

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

## Behavior

### Cached

* GET Object
* HEAD Object
* Range Requests

### Pass-through

* PUT Object
* DELETE Object
* COPY Object
* Multipart Uploads
* Bucket Operations
* Everything else

### Invalidation

Successful writes immediately invalidate cached copies of affected objects.

## Storage

Cached objects are stored on local disk.

The cache is disposable.

Deleting the cache directory should never result in data loss.

## Consistency

The upstream S3 system is always the source of truth.

The cache never modifies objects.

The cache never stores writes.

## Resource Usage

Typical:

* CPU: <1 core
* Memory: <1 GB
* Storage: configurable

Most resources are consumed by the cache disk.

## Configuration

```yaml
listen: ":8080"

upstream:
  endpoint: http://rustfs:9000

cache:
  path: /cache
  max_size: 1TB
```

## Why?

Because sometimes you just want:

```text
NVMe cache
     ↓
Object storage
```

without introducing another storage system.
