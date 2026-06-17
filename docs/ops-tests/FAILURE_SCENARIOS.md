# Failure Scenarios Test

## Purpose

Prove that v2 peer mode fails predictably and visibly when peers, routing, or
invalidation paths break.

The key claim to validate:

- Peer read failure before response commit falls back to upstream and does not
  store distributed pages.
- Peer read failure after response commit closes the response predictably.
- Invalidation failure causes readiness degradation rather than silent stale
  serving.
- Ring mismatch and internal protocol problems are visible in readiness,
  metrics, and logs.

## Setup

Deploy peer mode with image/tag `v0.0.2`:

```yaml
peer:
  mode: peer
  read_sharding: auto
  page_sharding_min_pages: 2
```

Use at least four peers if possible. The compute-cluster test pods should send
requests through the cache Service.

Before each scenario:

- confirm all peers are ready;
- confirm all peers expose the same `simple_s3_cache_peer_ring_info`;
- confirm `simple_s3_cache_degraded` is `0`;
- warm or clear cache state depending on the scenario.

## Scenario 1: Page Owner Down Before Response Commit

Goal: prove pre-commit peer read failure falls back to upstream.

Steps:

1. Create a large object that spans many pages.
2. Warm only some pages, or start from cold cache.
3. Identify a page owner for pages needed by the read.
4. Make that page owner unavailable before issuing the client read:
   - scale the pod down;
   - block peer-to-peer traffic with a temporary NetworkPolicy;
   - or otherwise make the peer unreachable from other cache peers.
5. Issue a range read through the cache Service that needs pages from the down
   peer.

Expected:

- If response headers were not committed, client gets a successful upstream
  pass-through response.
- `simple_s3_cache_peer_read_fallbacks_total` increments.
- Logs include coordinator ID, failed page owner ID, bucket/key, page indexes,
  and fallback reason.
- The fallback response is not stored into distributed page cache.
- No peer becomes degraded solely because a read fallback happened.

Failure:

- Client receives `502` before response commit instead of fallback.
- Fallback bytes are stored as distributed pages.
- Logs do not identify the failed page owner.

## Scenario 2: Page Owner Down After Response Commit

Goal: prove post-commit failure is detectable by clients and does not corrupt
cache state.

Steps:

1. Create and warm a large multi-page object.
2. Start a large range or full-object read through the cache Service.
3. After response bytes begin flowing, disrupt a page owner needed later in the
   response.

Expected:

- Client sees a failed/truncated response rather than a successful corrupt body.
- Cache does not store partial/corrupt pages.
- Logs identify post-commit peer/page failure.
- Metrics show peer read failure/fallback or upstream/internal failure as
  appropriate.

Failure:

- Client receives a successful response with missing/corrupt bytes.
- Partial page frames are accepted as valid.
- Cache later serves corrupt bytes as a hit.

## Scenario 3: Invalidation Broadcast Failure

Goal: prove consistency uncertainty causes readiness degradation.

Steps:

1. Create and warm a large object.
2. Block invalidation traffic from one peer or to one peer.
3. Perform a successful mutating operation through the cache Service:
   - PUT overwrite;
   - DELETE;
   - COPY destination overwrite;
   - multipart completion.
4. Observe readiness and metrics on all peers.

Expected:

- The mutation is visible as an invalidation broadcast attempt.
- The peer that cannot apply invalidation/epoch advance becomes not-ready, or the
  handling peer becomes not-ready if it cannot prove which peer failed.
- `simple_s3_cache_degraded` reports a non-zero value with a useful reason.
- Logs identify the failed peer and object key.
- Subsequent reads do not silently serve stale cached pages from the failed peer.

Failure:

- Invalidation failure is warning-only and all peers remain ready.
- A stale page remains readable after the mutation.
- Degraded reason is missing or not actionable.

## Scenario 4: Ring Mismatch

Goal: prove peer-list disagreement fails closed before cache state is touched.

Steps:

1. Deploy peers with a known-good ring.
2. Introduce one peer with a different peer list or peer URL set.
3. Send reads through the cache Service that require internal page requests.

Expected:

- Internal requests with mismatched ring fingerprint fail closed.
- The affected peer reports degraded/not-ready if ring mismatch is persistent.
- `simple_s3_cache_peer_ring_info` differs for the bad peer.
- Logs clearly show ring mismatch.

Failure:

- Peers with different rings serve or store distributed pages.
- Ring mismatch is visible only as generic `502` without ring details.

## Scenario 5: Internal Frame Corruption Or Truncation

Goal: prove the coordinator rejects bad internal page frames.

If there is no supported chaos hook to corrupt frames, this can be done with a
test peer or proxy that sits between peers and truncates the internal response.

Steps:

1. Route one page-owner response through a fault-injecting proxy.
2. Inject one of:
   - truncated frame;
   - wrong page index;
   - duplicate page index;
   - oversized frame length;
   - early connection close.
3. Issue a client read requiring that page.

Expected:

- Coordinator rejects the internal response.
- If client response was not committed, request falls back to upstream or fails
  predictably according to the read phase.
- If committed, downstream connection is closed.
- Corrupt frame is not stored as a valid page.
- Logs and metrics identify internal frame/protocol failure.

Failure:

- Corrupt or truncated frame is accepted.
- Client gets a successful response with corrupt bytes.
- Cache stores corrupt page bytes.

## Metrics To Capture

- `simple_s3_cache_degraded`
- `simple_s3_cache_peer_ring_info`
- `simple_s3_cache_peer_read_fallbacks_total`
- `simple_s3_cache_invalidation_broadcasts_total`
- `simple_s3_cache_page_owner_requests_total`
- `simple_s3_cache_coordinator_requests_total`
- `simple_s3_cache_upstream_request_failures_total`
- `simple_s3_cache_fill_coalesced_total`

Also capture readiness probe results over time for every peer.

## Pass Criteria

- Peer read failure before response commit falls back cleanly and does not store
  distributed pages.
- Peer read failure after response commit is client-detectable and does not
  corrupt cache state.
- Invalidation failure causes readiness degradation and is visible in metrics and
  logs.
- Ring mismatch fails closed before local distributed cache state is touched.
- Internal frame corruption is rejected.

## Failure Criteria

- Any failure scenario results in a successful corrupt response.
- Any stale page is served after failed invalidation.
- Any degraded consistency state remains ready and routable.
- Any internal trust/ring failure is silent or indistinguishable from a normal
  miss.
