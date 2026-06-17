# Hazards

Things we need to keep an eye on. These are not bugs to fix once and forget;
they are properties of the design that can silently regress into incorrect or
slow behavior. Each entry lists the risk, why it matters, the current
mitigation, and what to watch.

A hazard is "managed" when its mitigation is implemented *and* covered by a
test. Until then, treat it as load-bearing.

## Correctness

### H1. Mixed-ETag pages (mid-fetch object mutation)

* **Risk:** Pages for one object accumulate across many requests over time. If
  the object changes upstream between page fetches, the cache can assemble a
  response that mixes pages from different object versions.
* **Why it matters:** Silent data corruption. The client receives bytes that
  never existed as a single object.
* **Mitigation:** Stamp every page with the object's `ETag`. Send
  `If-Match: <etag>` on every upstream page fetch. On `412 Precondition
  Failed`, invalidate the object and refetch metadata before serving.
* **Watch:** Any code path that stores a page without checking the fetched
  ETag against the stored object ETag.

### H2. Invalidation vs. in-flight fetch race

* **Risk:** A write invalidates an object (deletes rows + files) while a read's
  singleflight fetch for that object is still in flight. The fetch completes
  afterward and writes a now-stale page, resurrecting deleted state.
* **Why it matters:** Post-invalidation stale reads. Defeats the entire
  write-through invalidation guarantee.
* **Mitigation:** Per-object epoch/generation counter. Capture the epoch when a
  fetch starts; on completion, only commit the page if the epoch is unchanged.
  Invalidation bumps the epoch. Combine with H1's ETag guard.
* **Watch:** Stores that commit a page based only on "fetch succeeded" without
  re-checking epoch/ETag under the index lock.

### H3. GET subresource, versioned, and response-shaped reads mis-cached

* **Risk:** `GET /bucket/key?tagging`, `?acl`, `?attributes`, `?versionId=...`,
  and response override parameters such as `response-content-type` look like
  object GETs but change the response semantics. Caching them as object bytes,
  or collapsing `?versionId` into the same `hash(bucket,key)`, serves wrong
  content or wrong headers.
* **Why it matters:** Correctness bug that is easy to ship because the request
  superficially matches the cached GET path.
* **Mitigation:** Classification must pass through any GET that carries a
  subresource or query parameter that changes the response semantics. Pass
  through `?versionId` reads; do not cache versioned reads. Pass through
  response override parameters such as `response-content-type`,
  `response-content-disposition`, and related `response-*` parameters.
* **Watch:** Request classifier treating "has bucket + key" as sufficient to
  cache.

### H4. Client conditional requests

* **Risk:** Clients send `If-None-Match` / `If-Modified-Since` (and
  `If-Match` / `If-Unmodified-Since`). Serving from cached metadata without
  honoring these diverges from upstream behavior (wrong `200` vs `304`).
* **Why it matters:** Breaks transparency for clients that rely on conditional
  GET/HEAD.
* **Mitigation:** Evaluate client conditional headers against cached metadata
  and emit correct `304 Not Modified` / `412` responses, or pass through when
  unsure.
* **Watch:** GET/HEAD handlers that ignore request validators.

### H5. SigV4 streaming PUT bodies (`aws-chunked`)

* **Risk:** Most AWS SDKs default to `Content-Encoding: aws-chunked` with
  `x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD`. Re-signing to
  upstream with our credential while forwarding the chunk-framed body verbatim
  produces a body/signature mismatch.
* **Why it matters:** PUT pass-through silently fails or corrupts uploads for
  default SDK configurations.
* **Mitigation:** Either pass streaming headers through untouched and avoid
  re-hashing the payload, or de-chunk the body before re-signing. Cover with an
  integration test using a real SDK default config.
* **Watch:** Any assumption that "forward the body bytes + re-sign" is
  sufficient for PUT.

### H6. Response header fidelity

* **Risk:** Transparency requires faithfully replaying object metadata headers
  on both full and range responses: `Content-Type`, `Content-Encoding`,
  `Cache-Control`, `Content-Disposition`, `x-amz-meta-*`, SSE headers, storage
  class, `x-amz-version-id`, and the object `ETag`.
* **Why it matters:** Subtle divergences break clients that depend on metadata.
* **Mitigation:** Store the full allow-listed header set with object metadata;
  replay it on cache hits. Drop only hop-by-hop headers.
* **Watch:** Range responses that only set range/length headers and lose object
  metadata.

### H12. SSE-C / customer-provided encryption cached by accident

* **Risk:** Requests with customer-provided encryption headers using the
  `x-amz-server-side-encryption-customer-` prefix are cached as normal object
  reads.
* **Why it matters:** The cache may store decrypted bytes or replay
  encryption-sensitive headers outside the trust model clients expect.
* **Mitigation:** Pass through SSE-C requests and do not store pages or metadata
  from them.
* **Watch:** Cacheability checks that ignore request headers and classify only
  by method, bucket, key, and query string.

### H14. Indefinite staleness of fully-cached objects (no TTL)

* **Risk:** Freshness depends entirely on invalidation. A fully-cached object
  never issues a page miss, so the `If-Match` guard (H1) never re-runs for it. If
  a write bypasses the cache (out-of-band upstream change), that object can serve
  stale bytes indefinitely — until it is invalidated through the cache or evicted
  by size pressure.
* **Why it matters:** The "writes pass through" assumption is the only thing
  keeping hot, resident objects fresh. Deployments that allow out-of-band writes
  have no self-healing path for objects that stay resident.
* **Mitigation:** Documented and accepted for the production scope (writes pass
  through). An optional per-object `max_age` / revalidate-on-access knob would
  bound staleness for out-of-band-write deployments; it is intentionally out of
  the initial scope. Eviction is the only current recovery for resident objects.
* **Watch:** Deployments with out-of-band writers; any assumption that a
  freshness TTL exists when it does not.

### H15. COPY issued as PUT with `x-amz-copy-source`

* **Risk:** S3 copy is not a distinct HTTP verb. SDKs issue it as a `PUT` (or
  `UploadPartCopy`) carrying an `x-amz-copy-source` header. A classifier that only
  invalidates on an explicit "COPY" route treats the copy as a normal write — or
  misses the destination invalidation entirely.
* **Why it matters:** The destination object keeps serving stale cached pages
  after a successful server-side copy.
* **Mitigation:** Detect `x-amz-copy-source` on `PUT`/part requests and treat it
  as a write that invalidates the destination on success (never the source).
  Cover with a test that uses the SDK copy API, not a hand-built COPY route.
* **Watch:** Invalidation keyed on an explicit COPY route rather than on the
  `x-amz-copy-source` header.

## Performance

### H7. SQLite single-writer contention

* **Risk:** SQLite permits many readers but one writer. Miss-heavy or cold-cache
  bursts plus per-hit `last_accessed_at` updates create write storms.
* **Why it matters:** Latency spikes and stalls under load.
* **Mitigation:** WAL mode, `busy_timeout`, short transactions, indexed
  lookups. Batch, buffer, or sample `last_accessed_at` updates; do not write an
  access update to SQLite on every page hit.
* **Watch:** Any write on the cache-hit path.

### H8. Synchronous eviction in the request path

* **Risk:** Running LRU eviction inline after writes adds latency and competes
  for the single SQLite writer.
* **Why it matters:** Tail-latency spikes under churn.
* **Mitigation:** Run eviction asynchronously / in a background sweeper, out of
  the request path.
* **Watch:** Eviction triggered synchronously after each page store.

### H9. Page-size over-fetch

* **Risk:** A large page size amplifies tiny scattered reads (e.g. Parquet
  footers / column chunks far smaller than the page), wasting upstream
  bandwidth and cache space.
* **Why it matters:** Hurts exactly the analytical workloads we target.
* **Mitigation:** Default to a smaller page size (currently 4 MB) and treat page
  size as the primary tuning knob. Production deployments keep a global default;
  measure read amplification and hit rate per bucket via metrics and logs before
  changing tuning behavior. See Tuning strategy and Observability in `PLAN.md`.
* **Watch:** Read amplification (upstream fill bytes ÷ client requested bytes);
  cache bytes stored / bytes served ratio; upstream bytes per client byte on
  analytical workloads; bucket-level breakdowns.

## Operational

### H10. Cold cache after restart

* **Risk:** Restart starts cold; upstream load spikes while the cache warms.
* **Why it matters:** Temporary interruption and upstream pressure.
* **Mitigation:** Documented and accepted because the cache is disposable. Watch
  upstream error/latency during warm-up.
* **Watch:** Upstream failure counters immediately after restart.

### H11. Multi-instance staleness

* **Risk:** Multiple cache instances that do not participate in the same v2 peer
  ring and invalidation protocol can serve stale data. A write through one cache
  while another cache is outside the ring leaves the second cache unaware of the
  new object epoch.
* **Why it matters:** Peer mode is correct only when peers agree on page
  ownership and every mutating path advances or invalidates object identity
  across the ring. Ordinary replicas outside peer mode still do not coordinate.
* **Mitigation:** `single` mode remains the simplest topology. In `peer` mode,
  all peers share the same static peer list and ring fingerprint. Page reads use
  deterministic page ownership, internal page/invalidation requests carry the
  ring fingerprint, and missing or mismatched ring IDs fail closed before
  touching local cache state. Successful writes broadcast invalidation and epoch
  advancement to every peer; any peer that cannot apply the update must fail
  readiness.
* **Watch:** Mixed peer-list rollouts, pods with stale config, ring IDs that
  differ across peers, cache replicas outside peer mode, invalidation broadcast
  failures, or any code path that touches distributed page state before the
  ring/identity checks.

### H13. Disk-full or cache write failure breaks reads

* **Risk:** Cache page storage fails because the cache data path is full,
  SQLite is locked, permissions changed, or disk writes fail, and the client
  request is failed even though upstream object bytes were readable.
* **Why it matters:** The disposable cache becomes a source of user-visible
  read failures.
* **Mitigation:** Continue serving the upstream response when possible, skip
  committing the failed page, record a cache write failure, and let future
  requests retry as misses. Eviction can be signaled, but request success must
  not depend on synchronous eviction.
* **Watch:** Error handling that couples "failed to store page" with "failed to
  serve object."

### H16. Upstream failure mid-stream, after the response is committed

* **Risk:** A `GET` assembled from cached + freshly-fetched pages streams to the
  client after status and `Content-Length` are already sent. If an upstream page
  fetch fails partway through (e.g. page 7 of 50), the headers promised a full
  body the cache can no longer deliver.
* **Why it matters:** The client sees a truncated body under a `200`/`206` with a
  `Content-Length` that does not match — silent truncation, the worst failure
  mode for a "transparent" proxy.
* **Mitigation:** Where practical, validate the first required upstream fetch
  before committing response headers. Once headers are flushed and an upstream
  fetch fails, abort the connection (reset / truncated transfer) rather than
  padding or faking success, so the client detects the short read. Record an
  upstream-failure metric.
* **Watch:** Any handler that writes response headers before all required
  upstream fetches can fail; a success status emitted before the body is known to
  be deliverable.

### H17. Metadata/page divergence

* **Risk:** The SQLite object metadata and page inventory drift apart: metadata
  rows exist while page rows or files are missing, page rows exist without usable
  object metadata, or orphaned page files remain after crashes, bugs, manual
  cleanup, or partial eviction.
* **Why it matters:** A cached page is only correct when the cache can prove
  which object version, size, page size, and response headers it belongs to.
  Serving pages without coherent metadata risks wrong headers, wrong ranges, or
  mixed object versions.
* **Mitigation:** Treat metadata and page rows as an inseparable cache index. A
  missing page file is a miss and should remove the stale row. Page rows without
  a usable object metadata row are ignored or deleted, then metadata is refetched
  from upstream. Orphaned files without SQLite rows are never hits and may be
  removed by a sweeper. It is always valid to delete local cache state and start
  cold.
* **Watch:** Code paths that serve a page based only on file existence, rebuild
  object metadata from page files, or delete metadata and pages in separate
  steps without tolerating crashes between them.

### H18. Distributed invalidation partial failure

* **Risk:** In v2 page-sharded peer mode, an upstream write/delete succeeds
  but one or more peers fail the invalidation broadcast and retain stale pages or
  metadata for the object.
* **Why it matters:** A later read coordinated by any peer may fetch a stale page
  from a peer that missed invalidation, breaking the write-through freshness
  guarantee.
* **Mitigation:** Invalidation must be idempotent and broadcast to all peers.
  If invalidation cannot be confirmed after upstream write success, the handling
  peer must fail readiness, emit loud logs, and expose metrics. This is not
  sufficient by itself: every peer must either apply the invalidation/epoch
  advance or become not-ready. Page commits and serves must remain fenced by ETag
  and epoch.
* **Watch:** Write paths that treat invalidation failure as a warning-only event;
  peers that keep passing readiness after consistency uncertainty; missing tests
  for partial invalidation failure.

### H19. Distributed read fallback stores or reroutes incorrectly

* **Risk:** In v2 page-sharded peer mode, a page owner is down or rejects a
  page read. The coordinator falls back by routing the page to another cache
  owner or by storing the fallback upstream response into distributed cache.
* **Why it matters:** Alternate page ownership duplicates placement and makes
  invalidation ambiguous. Storing degraded fallback responses can create cache
  state that does not follow the deterministic page-owner rule.
* **Mitigation:** Before response commit, failed peer reads may fall back to a
  pass-through upstream read for availability, but must not populate distributed
  pages. Do not reroute a page to a non-owner. Log and meter the fallback.
* **Watch:** Recovery code that silently picks another peer for a page; fallback
  reads that call cache store paths; missing metrics for peer-read fallback.

### H20. Page-owner ring mismatch

* **Risk:** Peers disagree on the static peer ring while using deterministic page
  ownership, so different peers compute different owners for the same
  `bucket/key/pageIndex`.
* **Why it matters:** The cluster can serve duplicate, stale, or missing pages
  depending on which peer coordinates the request. This is the page-sharded
  version of multi-instance staleness.
* **Mitigation:** Every internal page and invalidation request must carry the
  ring fingerprint. Peers must fail closed on missing or mismatched ring IDs
  before touching local page state. Metrics must expose local ring fingerprints.
* **Watch:** Mixed peer-list rollouts, internal routes that skip ring checks,
  page-owner calculations outside the central router, ring IDs that differ
  across peers.

### H21. Coordinator/page-owner object identity mismatch

* **Risk:** In v2 page-sharded peer mode, the coordinator sends stale or
  inconsistent object identity to a page owner: wrong ETag, epoch, object size,
  or page size.
* **Why it matters:** The page owner may serve a page for the wrong object
  version or calculate page bounds differently from the coordinator, producing
  corrupt or truncated responses.
* **Mitigation:** Internal page read requests must include the object identity
  used to plan the response. Page owners may serve only pages that match ETag and
  epoch, and must compute page bounds from the supplied page size and object
  size. Upstream fills still use `If-Match`. PUT, DELETE, COPY, multipart
  completion, overwrite, and conditional writes must all advance or invalidate
  the object identity through one mechanism.
* **Watch:** Internal APIs that identify pages only by bucket/key/page index;
  page owners serving local files without checking ETag/epoch/page size; tests
  that do not cover object replacement between metadata fetch and page fill.

### H22. Peer fanout amplification

* **Risk:** Distributed page reads turn one client request into many internal
  peer requests, especially for multi-page ranges or full-object reads.
* **Why it matters:** Naive fanout can increase latency, socket churn, CPU, and
  internal network traffic enough to erase page-sharding throughput gains.
* **Mitigation:** Group requested pages by page owner and batch them into bounded
  internal requests, but stream page payloads through the internal frame protocol
  instead of materializing full batches in memory. Bound per-peer and per-object
  fill concurrency. Use the `object` read strategy for small objects and `auto`
  thresholds based on effective page size.
* **Watch:** Internal requests per client request, page batch sizes, peer request
  latency, fill concurrency, request amplification on small-object workloads.

### H23. Internal page framing corruption

* **Risk:** V2 distributed page reads use an internal framed page protocol,
  and a coordinator accepts truncated, misordered, duplicate, or corrupted page
  frames from a page owner.
* **Why it matters:** The client can receive corrupt bytes under a successful S3
  response, or the coordinator can block indefinitely waiting for bytes that will
  never arrive.
* **Mitigation:** Version the internal page protocol from day one. Each frame
  must identify page index and byte length, and coordinators must reject
  unexpected, duplicate, truncated, or oversized frames. Page payloads should be
  consumed as streams so HTTP/file backpressure, not page-sized heap buffers,
  controls memory use.
* **Watch:** Internal page responses without protocol versioning; tests that only
  cover happy-path frames; handlers that stream page-owner bytes directly to
  clients without validating frame boundaries.

### H24. Readiness self-quarantine does not trigger

* **Risk:** A peer detects local corruption, repeated invalidation failures, or
  distributed consistency uncertainty but continues reporting ready.
* **Why it matters:** The platform keeps routing client traffic to a peer that
  may serve stale or inconsistent data.
* **Mitigation:** Separate liveness from readiness. Consistency uncertainty must
  fail readiness while keeping liveness and diagnostics available. Restart with
  an empty cache remains safe.
* **Watch:** Error paths that only log consistency uncertainty; readiness checks
  that ignore degraded state; tests that do not assert readiness failure after
  invalidation uncertainty.

### H25. Internal peer API trust boundary

* **Risk:** V2 internal page or invalidation endpoints accept client traffic,
  trust client-supplied peer coordination headers, or expose cache-control
  operations outside the peer ring.
* **Why it matters:** A client could read internal page frames, trigger
  invalidation, bypass routing checks, or poison peer coordination.
* **Mitigation:** Strip peer headers at public boundaries. Internal routes must
  require peer headers, matching ring fingerprint, and a signed peer MAC before
  touching cache state. They should be kept under a clearly internal path or
  listener. Never trust client-visible headers for upstream page fills.
* **Watch:** Public handlers that route `/internal/*`; peer code that forwards
  client-supplied internal headers; internal endpoints without tests for missing
  or mismatched peer identity.
