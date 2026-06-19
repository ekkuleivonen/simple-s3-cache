# simple-s3-cache Helm Chart

This chart deploys simple-s3-cache in one of two supported modes:

- `single`: one cache pod. This is the default and has the least operational complexity.
- `peer`: several cache peers. Any peer can coordinate a request; large-object
  pages are deterministically sharded across peers.

Peer mode is the distributed deployment for v2.

## Install

Single mode:

```bash
helm upgrade --install simple-s3-cache ./charts/simple-s3-cache \
  -n rustfs --create-namespace \
  -f charts/simple-s3-cache/examples/single-values.yaml
```

Direct peer mode:

```bash
helm upgrade --install simple-s3-cache ./charts/simple-s3-cache \
  -n rustfs --create-namespace \
  -f charts/simple-s3-cache/examples/peer-values.yaml
```

## Topology Details

### Single

Single mode creates one cache StatefulSet pod and a client-facing cache Service.
The generated cache config uses `peer.mode: single`.

Use this when one node has enough NVMe capacity and bandwidth.

### Peer

Peer mode creates a cache StatefulSet with `replicaCount` pods, a headless peer
Service, and a client-facing cache Service. Production deployments can also
create one LoadBalancer Service per peer.

Each pod renders `peer.local_id` from its StatefulSet pod name. The peer list is
generated from the StatefulSet DNS names:

```text
<release>-simple-s3-cache-0.<release>-simple-s3-cache-peers.<namespace>.svc.cluster.local
```

Use this when you want more aggregate cache capacity or disk bandwidth. Any cache
pod can coordinate a client request. Large cache-hit reads benefit from many
concurrent client requests because owner page reads spread across peers; one
client TCP stream remains bounded by the chosen coordinator's egress path.

### Production Peer Topology

The recommended steady-state Kubernetes shape for cross-cluster clients is:

- storage cluster: one StatefulSet cache ring next to object storage;
- storage cluster: one headless peer Service for stable StatefulSet DNS;
- storage cluster: one LoadBalancer Service per peer, each selecting exactly one
  StatefulSet pod;
- services cluster: DNS round-robin or EndpointSlice records over the per-peer
  VIPs.

This avoids one shared LoadBalancer IP becoming the coordinator ingress
bottleneck. The chart can create the per-peer Services:

```yaml
peerLoadBalancerServices:
  enabled: true
  port: 9000
  loadBalancerIPs:
    - 192.168.30.217
    - 192.168.30.218
    - 192.168.30.219
    - 192.168.30.220
```

Each generated Service is named `<release>-simple-s3-cache-peer-<ordinal>` and
selects a single pod with `statefulset.kubernetes.io/pod-name`.

Peer mode uses the configured page size and `peer.page_sharding_min_pages` to
choose between object-style reads and distributed page reads under the `auto`
strategy. Writes pass through to upstream and broadcast invalidation/epoch
advancement to every peer.

The owner-aware gateway topology from early v2 experiments is legacy. For
steady-state peer mode, point clients at either the cache Service for simple
in-cluster deployments or DNS round-robin across per-peer VIPs for production
cross-cluster deployments. Any peer can coordinate.

## Credentials

For production, prefer an existing Secret:

```yaml
upstream:
  credentials:
    existingSecret: rustfs-credentials
    accessKeyKey: RUSTFS_ACCESS_KEY
    secretKeyKey: RUSTFS_SECRET_KEY
```

For local tests, `upstream.credentials.accessKey` and
`upstream.credentials.secretKey` create a chart-managed Secret.

Peer mode also needs a shared peer-auth secret. For production, prefer an
existing Secret:

```yaml
peer:
  auth:
    existingSecret: simple-s3-cache-peer-auth
    key: auth_secret
```

`peer.authSecret` is still available for local-only tests, but peer mode fails
template rendering if it is empty or set to the placeholder `change-me`.

## Rollout Rules

Peer mode uses a static page ownership ring. Treat `replicaCount`, release name,
namespace, and peer Service name as part of that ring.

- Avoid mixed peer-list rollouts.
- Prefer a pinned `image.tag`; `latest` fails template rendering.
- Changing `replicaCount` moves some pages to new owners and makes them cold.
- Restarted cache pods start cold and may temporarily increase upstream load.
- Use one shared peer-auth Secret across all peers.
- Set `pdb`, `topologySpreadConstraints`, `podAntiAffinity`, and
  `updateStrategy` values intentionally for your platform.
- Writes must pass through simple-s3-cache. Out-of-band upstream writes can leave
  resident cached objects stale until invalidation or eviction.
- If a page owner is unavailable before response headers are committed, the
  coordinator falls back to upstream pass-through and does not store distributed
  pages.

## Monitoring

Set `serviceMonitor.enabled: true` when using the Prometheus Operator. Watch at
least these signals:

- `simple_s3_cache_peer_ring_info`
- `simple_s3_cache_degraded{reason_code="..."}`
- `simple_s3_cache_coordinator_requests_total`
- `simple_s3_cache_page_owner_requests_total`
- `simple_s3_cache_cache_requests_total`
- `simple_s3_cache_cache_bytes_total`
- `simple_s3_cache_internal_peer_request_duration_seconds`
- `simple_s3_cache_internal_peer_request_failures_total`
- `simple_s3_cache_invalidation_broadcasts_total`
- `simple_s3_cache_invalidation_broadcast_duration_seconds`
- `simple_s3_cache_peer_read_fallbacks_total`
- peer fanout, page batch size, and coalesced fill metrics
- per-peer hit rate, upstream fill bytes, cached bytes, and evictions

`/healthz` remains live where possible and reports `ready:false` with a stable
degraded reason. `/readyz` returns `503` when a peer self-quarantines. Enable the
operator endpoint with `operator.enabled: true` for a lightweight peer-state JSON
view; set `operator.bearerToken` unless access is protected by a private ops
network.

Set `prometheusRule.enabled: true` to install starter alerts for degraded peers,
invalidation failures, and peer-read fallbacks. Treat them as a baseline and tune
durations/severity for your cluster.

When `simple_s3_cache_degraded{reason_code="peer_invalidation_failed"}` fires,
treat the peer as self-quarantined because a post-write invalidation could not be
confirmed. First check `simple_s3_cache_invalidation_broadcasts_total{status="failure"}`
by `peer_id` and `bucket`, then inspect the writer pod logs for the failed
`/internal/v1/invalidate` request and whether the error was a parent request
cancellation, peer timeout, ring mismatch, or peer-side rejection. Compare that
with the target peer's `/readyz`, `/healthz`, and metrics before declaring the
target peer unavailable. Restart the quarantined peer only if bounded retry does
not clear readiness; restart is a safe recovery because cache state is
disposable, but it also starts cold and can temporarily increase upstream load.

Useful dashboard snippets:

```promql
sum by (reason_code) (simple_s3_cache_degraded)
sum by (peer_id, bucket) (increase(simple_s3_cache_invalidation_broadcasts_total{status="failure"}[5m]))
sum by (bucket, cache_status) (rate(simple_s3_cache_cache_bytes_total[5m]))
sum by (owner_id, status_class) (rate(simple_s3_cache_page_owner_requests_total[5m]))
histogram_quantile(0.95, sum by (le) (rate(simple_s3_cache_internal_peer_request_duration_seconds_bucket[5m])))
sum by (reason) (rate(simple_s3_cache_peer_read_fallbacks_total[5m]))
```

## Performance Validation

After deploying a topology, use the e2e performance runner to compare it with the
other supported modes from the same client network:

```bash
cd e2e
uv run python perf/s3_read_bench.py \
  --target single=http://single-cache.example.internal:8080 \
  --target peer=http://peer-cache.example.internal:8080 \
  --bucket "$S3CACHE_S3_BUCKET" \
  --output perf-results.json
```

The runner covers cold and warm full-object reads plus cold and warm sparse range
reads. Use it to decide whether single mode is sufficient or peer mode is needed
for aggregate capacity and bandwidth.

## Network Policy

`networkPolicy.enabled` installs component-specific policies. In peer mode,
cache peers accept client ingress through the cache Service and internal ingress
from cache peers in the same release. Egress is unrestricted by default because
upstream object storage and cross-cluster peer endpoints vary by cluster.

Tighten `networkPolicy` values for your cluster's client, peer, DNS, and
upstream storage paths before exposing the service outside a trusted network.
