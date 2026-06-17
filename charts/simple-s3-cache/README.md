# simple-s3-cache Helm Chart

This chart deploys simple-s3-cache in one of two supported modes:

- `single`: one cache pod. This is the default and has the least operational complexity.
- `peer`: several cache peers behind one Service. Any peer can coordinate a
  request; large-object pages are deterministically sharded across peers.

Peer mode is the distributed deployment for v2. It does not require an
owner-aware gateway.

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
Service, and a client-facing cache Service.

Each pod renders `peer.local_id` from its StatefulSet pod name. The peer list is
generated from the StatefulSet DNS names:

```text
<release>-simple-s3-cache-0.<release>-simple-s3-cache-peers.<namespace>.svc.cluster.local
```

Use this when you want more aggregate cache capacity or disk bandwidth without
running a separate gateway component.

Peer mode uses the configured page size and `peer.page_sharding_min_pages` to
choose between object-style reads and distributed page reads under the `auto`
strategy. Writes pass through to upstream and broadcast invalidation/epoch
advancement to every peer.

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

## Rollout Rules

Peer mode uses a static page ownership ring. Treat `replicaCount`, release name,
namespace, and peer Service name as part of that ring.

- Avoid mixed peer-list rollouts.
- Changing `replicaCount` moves some pages to new owners and makes them cold.
- Restarted cache pods start cold and may temporarily increase upstream load.
- Writes must pass through simple-s3-cache. Out-of-band upstream writes can leave
  resident cached objects stale until invalidation or eviction.
- If a page owner is unavailable before response headers are committed, the
  coordinator falls back to upstream pass-through and does not store distributed
  pages.

## Monitoring

Set `serviceMonitor.enabled: true` when using the Prometheus Operator. Watch at
least these signals:

- `simple_s3_cache_peer_ring_info`
- peer coordinator request metrics
- page-owner request metrics
- invalidation broadcast failures
- peer fanout, page batch size, and coalesced fill metrics
- per-peer hit rate, upstream fill bytes, cached bytes, and evictions

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
