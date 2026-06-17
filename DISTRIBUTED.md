# Distributed Peer Mode Design

This document describes the v2 distributed peer mode for `simple-s3-cache`.
It is the implementation design for the `v2` branch.

The goal is to keep deployment boring while making peer mode capable of using
the aggregate cache, disk, and network capacity of a small storage cluster.
Single mode should remain simple and local. Peer mode can be smarter, but should
avoid external state systems such as Redis, NATS, ZooKeeper, or etcd.

## Goals

- Keep `peer.mode: single` as a simple local cache with no cluster concepts.
- Reduce deployment modes to two:
  - `single`: one cache process, local cache only.
  - `peer`: static peer ring, deterministic page sharding, peer invalidation.
- Remove the owner-aware gateway as a correctness requirement.
- Allow any cache peer to receive any request and coordinate the response.
- Spread large-object range traffic across all cache peers automatically.
- Preserve correctness when all writes pass through the cache.
- Keep cache state disposable; upstream S3-compatible storage remains source of
  truth.
- Avoid consensus, leader election, external queues, and mutable global load
  state.

## Non-Goals

- Do not make single mode distributed.
- Do not provide strong consistency for out-of-band upstream writes.
- Do not optimize a single HTTP response beyond the egress capacity of the peer
  that accepted that response.
- Do not implement dynamic least-loaded-peer routing as the primary strategy.
- Do not require operators to deploy extra stateful services.

## Previous Object-Owner Limitation

The v1 peer model used object ownership:

```text
bucket/key -> one owner peer
```

That keeps invalidation simple, but it can create byte skew. A few hot or large
objects may hash to one peer, so request counts look balanced while bytes served,
disk reads, and NIC utilization are not.

In a storage cluster with four cache peers on four 2.5Gbps nodes, one hot object
owned by one peer can be capped by that peer's NIC even if the compute cluster
has a 10Gbps uplink to the storage cluster.

## Proposed Model

V2 peer mode is a deterministic distributed page cache.

There is no object owner for reads. Any peer can coordinate a request.

```text
Client / compute workload
  |
Kubernetes Service / LoadBalancer
  |
Any simple-s3-cache peer, acting as request coordinator
  |
Page owner peers, selected by deterministic page hash
  |
RustFS / upstream S3-compatible storage on misses
```

The sharding unit is the cache page:

```text
bucket/key/pageIndex -> page owner peer
```

The receiving peer is the coordinator. It computes the page owners, fetches page
bytes from those owners, and streams the response to the client in byte order.

## Deployment Shape

The preferred distributed deployment becomes:

```text
Compute cluster
  |
  | 10Gbps uplink
  v
Storage cluster Service / LoadBalancer
  |
  v
4 cache peers, one near each RustFS pod/node
  |
  v
4 RustFS pods / disks / 2.5Gbps node NICs
```

For many concurrent large-object or range-heavy reads, different client
responses should land on different coordinators, while page ownership spreads
cache byte serving across all peers. This can approach aggregate throughput from
the four 2.5Gbps nodes.

For one single large HTTP response, the coordinator peer still owns the client
TCP stream. That one response is limited by the coordinator's egress capacity.
The model improves aggregate throughput across concurrent requests, not the
throughput of one response beyond one peer's client-facing NIC.

## Request Routing

External routing no longer needs object awareness.

The app can send S3 traffic to a normal Service in front of cache peers:

```text
App -> cache Service -> any cache peer
```

The owner-aware gateway is not part of the v2 deployment model.

## Peer Ring

All peers still use a static peer list and a ring fingerprint.

Requirements:

- Every peer must have the same peer list.
- Every peer must have a stable `peer.local_id`.
- Internal peer requests must carry the ring fingerprint.
- A peer must reject internal requests with a missing or mismatched ring
  fingerprint.

The ring is used for page ownership, not object ownership.

Suggested page owner key:

```text
bucket + "/" + key + "\x00page\x00" + pageIndex
```

The exact format should be centralized in the peer router package so tests and
later migrations do not duplicate it.

## Metadata

Metadata can stay deliberately boring.

A coordinator that needs object metadata first checks its local metadata cache.
On miss, it performs `HEAD` against upstream, stores metadata locally, and uses
that metadata to plan pages.

Duplicated metadata is acceptable only if object identity is treated as a
versioned contract. The identity used for a read includes ETag, epoch, object
size, page size, and response headers. Delete/recreate, overwrite, COPY,
multipart completion, and conditional writes must all advance or invalidate that
identity through the same mechanism.

Page owners do not need to be metadata authorities. The coordinator includes the
current object identity in page requests:

- bucket
- key
- page index or page range
- object size
- page size
- ETag
- cache epoch or version token

The page owner may serve a local page only if the cached page matches the ETag
and epoch supplied by the coordinator. Otherwise it treats the page as a miss.

This means metadata can be duplicated across coordinators without making any
peer authoritative for object freshness.

## Page Reads

For cacheable full-object and single-range `GET`:

1. Coordinator obtains object metadata.
2. Coordinator parses the client range or full-object range.
3. Coordinator computes page spans.
4. Coordinator groups page spans by page owner.
5. Coordinator asks each page owner for the needed page or page range.
6. Page owner opens a local cached page if ETag and epoch match.
7. On miss, page owner fills the page from upstream using:
   - `GET`
   - page-aligned `Range`
   - `If-Match: <etag>`
   - SigV4 signing after final headers are set
8. Page owner streams page bytes to coordinator.
9. Coordinator writes requested spans to the client in byte order.

The coordinator should preserve S3 response behavior:

- Range responses must return `206`.
- Full-object responses must return `200`.
- Client conditionals must be honored before page fanout where possible.
- Response headers must match current single-node behavior.

## Batching

Naive one-HTTP-request-per-page fanout will add overhead.

Implementation should batch by owner:

```text
page owner A: pages 0, 1, 4
page owner B: pages 2, 3
page owner C: pages 5
```

The internal API should support requesting multiple pages from one owner in one
peer request. The simplest initial implementation can return pages in increasing
page index order with small per-page framing.

Possible internal response framing:

```text
page_index uint64
page_size uint64
page_bytes...
page_index uint64
page_size uint64
page_bytes...
```

JSON is easier to debug but should not wrap page bytes. A compact binary frame
or multipart-like stream is more appropriate for large responses.

Internal framing is a real protocol. The protocol needs explicit handling for
cancellation, backpressure, partial page frames, truncated frames, and later
compatibility. Tests must cover corrupted or truncated internal page responses.

## Internal APIs

Internal routes should be separate from client S3 routes and guarded by peer
headers.

Suggested routes:

```text
POST /internal/v1/pages/read
POST /internal/v1/invalidate
```

### Page Read

Request fields:

```json
{
  "bucket": "bucket",
  "key": "large/00003.bin",
  "object_size": 1073741824,
  "page_size": 4194304,
  "etag": "\"8aec89b556df3c52893db237a4201dc0\"",
  "epoch": 42,
  "pages": [8, 9, 10]
}
```

Response:

- `200` with framed page bytes if all pages are served or filled.
- `409` or `412` if the page owner detects stale object identity.
- `502` if upstream fill fails.
- `503` if the peer is overloaded and cannot accept more fills.

The page owner should not trust client-visible headers from the coordinator. It
should build and sign its own upstream fill requests.

### Invalidate

Request fields:

```json
{
  "bucket": "bucket",
  "key": "large/00003.bin",
  "epoch": 43,
  "reason": "put_object"
}
```

Behavior:

- Delete local pages for `bucket/key`.
- Delete or mark local metadata stale for `bucket/key`.
- Record the latest invalidation epoch if needed to reject old in-flight page
  commits.
- Return success if the object was already absent.

Invalidation must be idempotent.

## Writes And Invalidation

Writes remain pass-through to upstream.

After a successful write/delete/COPY/multipart completion, the peer that handled
the write broadcasts invalidation to all peers, including itself.

Broadcasting to all peers is intentionally simple. Peer counts are expected to
be small, and this avoids needing to know which peers currently store pages for
the object.

Important failure question:

- If upstream write succeeds but invalidation fails on one peer, stale cached
  pages might survive.

Recommended fail-soft behavior:

- Try invalidation after successful upstream write.
- If any peer invalidation fails after upstream write success, mark the local
  cache process unhealthy for readiness, emit loud warning/error logs, and stop
  accepting new traffic until restart or explicit recovery.
- Return a visible error for the write response if possible. This does not roll
  back the upstream write, but it makes the consistency risk visible.
- Failing readiness lets Kubernetes or another supervisor rotate the pod without
  needing an external coordination system.

Readiness-failing the writer is not sufficient by itself. The peer that missed
invalidation may still be alive and serving stale pages. Distributed mode needs
a concrete epoch protocol:

- Mutating paths must advance an object epoch before future cache reads can use
  the new object identity.
- Every peer must either apply the invalidation/epoch advance or become
  not-ready.
- Page owners may serve or commit pages only for the exact ETag and epoch
  supplied by the coordinator.
- Coordinators must not plan cache reads using an epoch older than the latest
  locally known invalidation for that object.
- Tests must prove no stale page is served after PUT, DELETE, COPY, multipart
  completion, overwrite, and failed invalidation.

This avoids needing Redis or another external state service, but it is still a
distributed protocol and should be implemented as such.

## Epoch And ETag Fencing

The current cache already relies on two important invariants:

- Upstream page fills use `If-Match: <etag>`.
- Page commits are fenced by an object epoch so in-flight fills cannot resurrect
  invalidated data.

Distributed mode should preserve both invariants.

The coordinator supplies the ETag and epoch to page owners. Page owners:

- only serve pages matching that ETag and epoch;
- fill from upstream with `If-Match`;
- commit pages only if their local page state is still current for that ETag and
  epoch;
- discard pages on invalidate.

## Failure Behavior

Peer down during read:

- If the response has not been committed, fall back to a pass-through read from
  upstream instead of returning `502`.
- Invalidate the local metadata/pages for that object before falling back, so the
  coordinator does not keep planning reads from a possibly stale or unreachable
  peer state.
- Emit a loud warning log with coordinator ID, page owner ID, bucket/key, page
  indexes, and fallback reason.
- Record a metric for peer-read fallback.
- Do not store the pass-through response in the distributed page cache in the
  first version. The fallback is an availability path, not a second cache
  placement policy.
- If response bytes have already been sent, close the downstream connection as a
  committed stream failure.

Peer down during invalidation:

- Mark any peer that cannot apply the invalidation/epoch advance unhealthy for
  readiness and emit loud warning/error logs. If the handling peer cannot prove
  which peer failed, it must mark itself not-ready and expose enough diagnostics
  for operators to identify the failed broadcast.
- Keep liveness separate from readiness. The process can stay alive long enough
  to expose diagnostics and metrics, but should not receive new client traffic.
- Startup with an empty cache is safe, so restart is an acceptable recovery
  mechanism.

Ring mismatch:

- Internal requests fail closed with `502`.
- Metrics should expose the local ring fingerprint.

Coordinator cancellation:

- Cancel outstanding page-owner requests.
- Page owners may finish in-flight page fills and keep the page if still valid,
  or abort if context cancellation reaches them.

Partial stream failure:

- If response headers have not been sent, return `502`.
- If response bytes have been sent, close the downstream connection as current
  code does for committed response failures.

## Boring Fail-Soft Patterns

The distributed mode should prefer simple recovery rules that preserve
correctness and make degraded behavior obvious.

Useful patterns:

- **Read fallback to upstream:** when a peer read path fails before response
  commit, invalidate local planning state and pass the original read through to
  upstream. This trades cache performance for availability without inventing a
  second ownership scheme.
- **Readiness fail on consistency uncertainty:** when a write succeeds upstream
  but invalidation cannot be confirmed, mark the handling peer not-ready. This
  avoids silently serving potentially stale data while letting the platform
  rotate the process.
- **No fallback on ring mismatch:** ring mismatch indicates configuration
  disagreement. Fail closed; do not pass through as if the cluster were healthy.
- **No alternate page owner on page-owner failure:** do not reroute the same page
  to a different cache owner. That would duplicate cache placement and make
  invalidation harder. Use upstream pass-through for the whole client request
  instead.
- **No cache store on degraded pass-through:** fallback reads should not populate
  distributed pages until the peer path is healthy again.
- **Local self-quarantine:** a peer that detects local corruption, repeated
  invalidation failures, or persistent ring mismatch should fail readiness and
  continue exposing metrics.
- **Bounded retries only:** retry peer calls once or with a very small bounded
  budget. Do not hide cluster problems behind long retry loops.
- **Loud logs and counters:** every degraded path should have structured logs
  and metrics so operators can distinguish healthy cache misses from peer
  recovery behavior.

## Performance Expectations

This model helps most when reads are byte-heavy and concentrated in large
objects.

It improves aggregate concurrency, not one TCP stream. A single large HTTP
response still exits through the coordinator peer and is capped by that peer's
client-facing egress capacity. Operators should expect better use of all peers
under many concurrent large reads, not 10Gbps for one single-object download.

Expected wins:

- Hot large objects no longer bottleneck on one object owner.
- Warm cache bytes are spread across peer disks and NICs.
- Cold fills write pages across peer disks.
- Many concurrent range requests can approach aggregate cache peer egress.
- A normal Kubernetes Service can distribute coordinator egress across peers.

Limited or negative gains:

- Small objects may not benefit; object-level hashing already spreads them.
- Single-page reads may incur one internal hop.
- One huge HTTP response is still limited by the coordinator's egress NIC.
- Cold cache can still bottleneck on RustFS.
- Cold cache can multiply RustFS pressure if overlapping misses are not
  coalesced by page owner.
- Naive per-page fanout can hurt latency unless batched by owner.

Distributed reads should use an explicit read strategy, not vague policy names:

```text
object: one peer handles/cache-owns the object read path
page: pages are deterministically sharded across peers
auto: choose object or page from object size and configured page size
```

`auto` should be the default target behavior for peer mode once this design is
stable. It uses the same page size users already configure globally or per
bucket:

```text
effective_page_size = bucket page_size override, otherwise global page_size
page_count = ceil(object_size / effective_page_size)

if page_count < page_sharding_min_pages:
  use object strategy
else:
  use page strategy
```

This keeps small HTML pages, small documents, and other one-page objects away
from peer fanout while letting large video and parquet objects use distributed
page sharding. Bucket-specific page sizes naturally affect the decision.

The page strategy must beat the object strategy for large objects at both p50
and p95 under realistic concurrency before it should become the default peer
strategy.

## Metrics And Logs

Distributed mode needs visibility by coordinator and page owner.

Recommended metrics:

- peer ring fingerprint by peer.
- coordinator requests by method, bucket, status class.
- page-owner requests by bucket, owner peer, status class.
- page bytes served by page owner.
- page bytes fetched from upstream by page owner.
- page hits and misses by page owner.
- invalidation broadcasts by status and failed peer.
- internal peer request duration.
- internal peer requests per client request.
- page batch size and pages per coordinator request.
- coalesced page fills by page owner.
- upstream fill errors by status and response body class.

Recommended log fields:

- coordinator peer ID.
- page owner peer ID.
- bucket and key.
- page indexes or page range.
- ETag and epoch.
- ring ID.
- upstream status and capped response body on fill failure.

## Configuration

Proposed configuration:

```yaml
peer:
  mode: peer
  local_id: simple-s3-cache-0
  peers:
    - id: simple-s3-cache-0
      url: http://simple-s3-cache-0.simple-s3-cache-peers:8080
    - id: simple-s3-cache-1
      url: http://simple-s3-cache-1.simple-s3-cache-peers:8080
  read_sharding: auto # object | page | auto
  page_sharding_min_pages: 2
  invalidation_timeout: 5s
  page_request_timeout: 30s
  max_peer_fill_concurrency: 32
```

Read strategy behavior:

- `object`: use object-style peer handling for every cacheable object read.
- `page`: use distributed page sharding for every cacheable object read that
  spans cache pages.
- `auto`: use `object` below `page_sharding_min_pages`; use `page` at or above
  that threshold.

Defaults should be boring:

- `single` remains the default mode.
- `auto` is the intended default for `peer.mode: peer`.
- `page_sharding_min_pages: 2` means one-page objects use `object`; objects that
  span at least two pages use `page`.

## Implementation Plan

### Milestone 1: Preserve Single Mode

- [x] Keep `peer.mode: single` behavior unchanged.
- [x] Add regression tests proving single mode does not require peer config.
- [x] Keep existing local cache metadata/page storage paths compatible.
- [x] Keep existing single-node read, write, invalidation, and eviction behavior.

### Milestone 2: Peer Ring And Page Ownership

- [x] Add `PageOwner(bucket, key, pageIndex)` to the peer router.
- [x] Centralize the page-owner hash key format.
- [x] Add stable ownership tests for representative buckets, keys, and page
  indexes.
- [x] Add distribution tests showing large objects spread pages across peers.
- [x] Keep ring fingerprint behavior deterministic.
- [x] Reject internal requests with missing or mismatched ring fingerprints.

### Milestone 3: Read Strategy Selection

- [x] Add `peer.read_sharding: object | page | auto`.
- [x] Add `peer.page_sharding_min_pages`.
- [x] Compute effective page size from bucket override or global page size.
- [x] Implement `auto` strategy:
  - [x] one-page objects use `object`;
  - [x] objects at or above `page_sharding_min_pages` use `page`.
- [x] Add tests for global and bucket-specific page-size decisions.
- [x] Add metrics/log fields for selected read strategy.

### Milestone 4: Internal Peer API Boundary

- [x] Add internal route namespace, for example `/internal/v1/*`.
- [x] Strip client-supplied peer/internal headers at public boundaries.
- [x] Require peer identity and ring fingerprint on internal routes.
- [x] Fail closed on ring mismatch before touching local cache state.
- [x] Decide whether internal routes share the public listener or use a separate
  listener.
- [x] Add tests for missing, spoofed, and mismatched internal headers.

### Milestone 5: Versioned Page Frame Protocol

- [x] Define page response protocol version.
- [x] Define frame fields:
  - [x] page index;
  - [x] byte length;
  - [x] page bytes;
  - [x] end-of-stream marker or equivalent.
- [x] Define behavior for duplicate, unexpected, truncated, oversized, and
  out-of-order frames.
- [x] Define cancellation and backpressure behavior.
- [x] Add corruption/truncation tests before using the protocol in the read path.

### Milestone 6: Page Owner Read Endpoint

- [ ] Add internal page read request schema with object identity:
  - [ ] bucket;
  - [ ] key;
  - [ ] object size;
  - [ ] page size;
  - [ ] ETag;
  - [ ] epoch;
  - [ ] page list.
- [ ] Serve local pages only when ETag and epoch match.
- [ ] Treat non-matching local pages as misses.
- [ ] Fill missing pages from upstream with page-aligned `Range`.
- [ ] Send `If-Match: <etag>` on upstream fills.
- [ ] Sign upstream fill requests after final headers are set.
- [ ] Coalesce concurrent fills by `bucket/key/pageIndex/etag/epoch`.
- [ ] Bound per-peer and per-object fill concurrency.
- [ ] Return framed page bytes in increasing page index order.
- [ ] Add tests for hit, miss, coalesced miss, upstream failure, and stale
  identity.

### Milestone 7: Coordinator Read Path

- [ ] Obtain metadata locally or by upstream `HEAD`.
- [ ] Treat metadata as object identity: ETag, epoch, size, page size, headers.
- [ ] Honor client conditionals before peer fanout where possible.
- [ ] Parse full-object and single-range reads into page spans.
- [ ] Select read strategy using `object`, `page`, or `auto`.
- [ ] Group page spans by page owner.
- [ ] Batch page requests by owner.
- [ ] Stream response bytes to the client in correct byte order.
- [ ] Fall back to upstream pass-through before response commit when a peer read
  fails.
- [ ] Do not store degraded pass-through bytes into distributed pages.
- [ ] Close the downstream connection on post-commit peer/page failures.
- [ ] Add tests for single-page, multi-page, multi-owner, fallback, and
  post-commit failure behavior.

### Milestone 8: Distributed Invalidation And Epochs

- [ ] Define object epoch storage and update rules.
- [ ] Advance epoch on every mutating path:
  - [ ] PUT object;
  - [ ] DELETE object;
  - [ ] COPY destination;
  - [ ] multipart complete;
  - [ ] multipart abort where needed;
  - [ ] overwrite;
  - [ ] conditional write success.
- [ ] Broadcast invalidation/epoch advance to every peer, including self.
- [ ] Make invalidation idempotent.
- [ ] Delete or mark local metadata and pages stale on invalidation.
- [ ] Ensure every peer either applies the invalidation/epoch advance or becomes
  not-ready.
- [ ] Ensure coordinators do not plan reads using an older known epoch.
- [ ] Surface write success plus invalidation failure visibly where possible.
- [ ] Add tests proving no stale page after each mutating path.
- [ ] Add tests for partial invalidation failure and readiness failure.

### Milestone 9: Readiness, Health, And Self-Quarantine

- [ ] Separate liveness from readiness.
- [ ] Add degraded state for consistency uncertainty.
- [ ] Fail readiness on:
  - [ ] failed invalidation/epoch application;
  - [ ] persistent ring mismatch;
  - [ ] detected local cache corruption that cannot be repaired safely.
- [ ] Keep liveness up for diagnostics when possible.
- [ ] Expose degraded reason in health output, logs, and metrics.
- [ ] Add tests that degraded peers stop reporting ready.

### Milestone 10: Observability

- [ ] Add coordinator request metrics by method, bucket, strategy, and status.
- [ ] Add page-owner request metrics by bucket, owner peer, and status.
- [ ] Add page bytes served by page owner.
- [ ] Add upstream fill bytes by page owner.
- [ ] Add internal peer requests per client request.
- [ ] Add page batch size metrics.
- [ ] Add fill coalescing metrics.
- [ ] Add invalidation broadcast success/failure metrics by peer.
- [ ] Add structured logs with coordinator ID, page owner ID, ring ID, bucket,
  key, page indexes, ETag, epoch, and fallback/degraded reason.

### Milestone 11: Performance Proof

- [ ] Benchmark `object`, `page`, and `auto` strategies under realistic parquet
  concurrency.
- [ ] Prove `page` or `auto` beats `object` for large-object workloads at p50.
- [ ] Prove `page` or `auto` beats `object` for large-object workloads at p95.
- [ ] Prove small and medium object workloads do not materially regress under
  `auto`.
- [ ] Measure RustFS pressure during cold cache with overlapping reads.
- [ ] Verify fill coalescing reduces duplicate upstream page fills.
- [ ] Verify aggregate throughput improves under many concurrent large reads.
- [ ] Document that one client TCP stream remains coordinator-egress limited.

### Milestone 12: Documentation And Deployment

- [ ] Update README with v2 peer mode behavior.
- [ ] Update Helm values for peer read strategy, page threshold, and peer
  timeouts.
- [ ] Update examples for the two supported modes:
  - [ ] `single`;
  - [ ] `peer`.
- [ ] Remove or clearly deprecate owner-aware gateway guidance if no longer
  required.
- [ ] Document failure semantics for peer read fallback, invalidation failure,
  readiness failure, and ring mismatch.
- [ ] Document operational metrics and alert suggestions.

## Acceptance Criteria

Correctness:

- No stale page after PUT, DELETE, COPY, multipart completion, overwrite, or
  failed invalidation.
- Mutating S3 paths advance or invalidate the object identity contract.
- Page owners never serve pages that do not match coordinator-supplied ETag and
  epoch.

Performance:

- `page` or `auto` strategy beats `object` strategy for large-object workloads at
  p50 and p95 under realistic concurrency.
- Small and medium object workloads do not regress materially because `auto`
  keeps them on the object strategy.
- Peer fanout, batch size, and coalesced fills are visible in metrics.

Failure behavior:

- Peer down before response headers falls back to upstream cleanly and does not
  store distributed pages.
- Peer down after response headers fails predictably by closing the downstream
  response without corrupting cache state.
- Ring mismatch, invalidation failure, internal frame corruption, and local
  cache corruption are obvious in readiness, metrics, and logs.

Operations:

- Documentation states clearly that distributed page sharding improves aggregate
  throughput under concurrent reads, not the throughput of one client TCP stream.
- The internal peer protocol is versioned from day one.

## Design Decisions

- `page_sharding_min_pages` defaults to `2`.
  - One-page objects stay on the `object` strategy.
  - Objects spanning at least two configured cache pages use `page` under
    `auto`.
  - If performance tests show too much fanout for medium objects, raise the
    default later with data.
- Metadata remains duplicated per coordinator.
  - Do not shard metadata in v2.
  - Metadata is treated as an object identity contract: ETag, epoch, object
    size, page size, and response headers.
  - Page owners validate pages against coordinator-supplied ETag and epoch
    instead of becoming metadata authorities.
- Write invalidation after upstream success is strict.
  - Every peer must apply the invalidation/epoch advance or become not-ready.
  - The handling peer returns a visible error where possible, but correctness
    does not depend on the client seeing that error.
  - Readiness failure is the recovery signal for the platform.
- Internal page response framing uses a versioned binary protocol.
  - JSON may describe requests and errors.
  - Page bytes are streamed as binary frames with protocol version, page index,
    byte length, and page bytes.
  - Coordinators reject duplicate, unexpected, truncated, oversized, or
    out-of-order frames.
- Internal peer routes share the same listener in v2.
  - Keep deployment simple: one container port and one Service.
  - Protect internal routes with path namespace, peer identity headers, and ring
    fingerprint checks.
  - A separate listener can be added later if operational isolation is needed.
- No read-through replication in v2.
  - A page has exactly one deterministic owner.
  - Peer read failure falls back to upstream pass-through before response commit
    and does not store distributed pages.
  - Replication for very hot single-page reads can be reconsidered only after the
    deterministic page-owner model is proven.

## Summary

The simplest high-performance distributed design is:

- any peer can coordinate;
- pages, not objects, are deterministically sharded;
- writes broadcast invalidation to all peers;
- no external state workload is required;
- single mode stays local and simple;
- peer mode becomes the one distributed deployment mode.

This moves complexity from deployment topology into a small peer protocol. That
is acceptable if the protocol remains deterministic, idempotent, and fail-closed.
