# OPS Test Pack For v0.0.2

These notes are for validating the v2 distributed peer mode from Git tag
`v0.0.2` and image tag `ghcr.io/ekkuleivonen/simple-s3-cache:0.0.2` in a
storage-cluster deployment.

The goal is not to prove every unit-level invariant. The goal is to confirm that
the deployed system behaves correctly under realistic compute-cluster traffic,
uses the storage-cluster peers as intended, and fails in predictable ways.

## Deployment Shape

The gateway concept is gone for v2. Deploy only:

- `single` mode for one-pod baseline tests, if desired.
- `peer` mode for distributed cache tests.

For the target storage-cluster shape:

```text
Compute cluster test pods
  |
  | 10Gbps uplink
  v
Storage cluster cache Service
  |
  v
simple-s3-cache peer pods, one near each RustFS pod/node
  |
  v
RustFS pods / disks / node NICs
```

The compute-side test pods should send S3 path-style traffic to the cache
Service, not to a gateway. Any cache peer can coordinate a request. In peer mode,
large cacheable reads should use page ownership across peers when
`peer.read_sharding: auto` selects the `page` strategy.

## Suggested Peer Config

Use four cache peers to match the four RustFS/storage nodes.

Recommended v0.0.2 read strategy:

```yaml
peer:
  mode: peer
  read_sharding: auto
  page_sharding_min_pages: 2
```

Keep the same peer list on every cache pod. Keep peer IDs stable. Do not combine
peer-list changes with performance or failure measurements.

## Metrics To Scrape

Scrape every cache peer's `/metrics` endpoint. These are the important signals:

- `simple_s3_cache_peer_ring_info`
- `simple_s3_cache_degraded`
- `simple_s3_cache_read_strategy_selected_total`
- `simple_s3_cache_coordinator_requests_total`
- `simple_s3_cache_page_owner_requests_total`
- `simple_s3_cache_page_owner_bytes_served_total`
- `simple_s3_cache_page_owner_upstream_fill_bytes_total`
- `simple_s3_cache_internal_peer_requests_per_client_request`
- `simple_s3_cache_page_batch_size`
- `simple_s3_cache_fill_coalesced_total`
- `simple_s3_cache_invalidation_broadcasts_total`
- `simple_s3_cache_peer_read_fallbacks_total`
- `simple_s3_cache_page_hits_total`
- `simple_s3_cache_page_misses_total`
- `simple_s3_cache_bytes_served_from_cache_total`
- `simple_s3_cache_bytes_served_from_upstream_total`
- `simple_s3_cache_upstream_fill_bytes_total`
- `simple_s3_cache_upstream_request_failures_total`

Also collect structured logs from all cache peers. For failures, look for
coordinator ID, page owner ID, ring ID, bucket/key, page indexes, ETag, epoch,
fallback reason, and degraded reason.

## Test Files

- [Performance Test](PERFORMANCE.md)
- [Correctness Test](CORRECTNESS.md)
- [Failure Scenarios Test](FAILURE_SCENARIOS.md)

## Pass/Fail Summary

The tag is a good release candidate if:

- Performance: peer `auto` or `page` improves large-object aggregate throughput
  over `object` and avoids material small-object regression.
- Correctness: reads after mutating paths never return stale bytes.
- Failure: peer failures either fall back before response commit or fail
  predictably after commit, without cache corruption.
- Operations: ring mismatch, invalidation failure, and degraded state are visible
  in readiness, metrics, and logs.
