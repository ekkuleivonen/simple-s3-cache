# simple-s3-cache Helm Chart

This chart deploys simple-s3-cache in one of three supported topologies:

- `single`: one cache pod. This is the default and has the least operational complexity.
- `peer`: several cache peers behind one Service. Non-owner requests are forwarded peer-to-peer.
- `gateway`: stateless gateway pods route directly to owner cache peers.

The chart keeps single mode and direct peer mode available. Gateway mode is an
optimization for high-throughput distributed deployments, not a requirement.

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

Gateway mode:

```bash
helm upgrade --install simple-s3-cache ./charts/simple-s3-cache \
  -n rustfs --create-namespace \
  -f charts/simple-s3-cache/examples/gateway-values.yaml
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

### Gateway

Gateway mode creates the same cache StatefulSet plus a stateless gateway
Deployment and gateway Service. The regular cache Service is disabled by default
so clients naturally use the gateway.

The gateway receives the same generated peer ring as the cache pods and routes
object-scoped requests directly to the owner peer.

Use this when peer-to-peer relay overhead is meaningful for your workload.

Gateway mode is still static owner sharding. It is not replicated cache storage,
distributed metadata, or consensus-based HA.

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

Peer and gateway modes use static owner sharding. Treat `replicaCount`, release
name, namespace, and peer Service name as part of the cache ownership ring.

- Avoid mixed peer-list rollouts.
- Changing `replicaCount` moves some objects to new owners and makes them cold.
- Restarted cache pods start cold and may temporarily increase upstream load.
- Writes must pass through simple-s3-cache. Out-of-band upstream writes can leave
  resident cached objects stale until invalidation or eviction.
- Do not expose cache peers publicly in gateway mode unless that is intentional.
- If an owner is unavailable, requests for that owner's objects should fail
  closed instead of being served by the wrong peer.

## Monitoring

Set `serviceMonitor.enabled: true` when using the Prometheus Operator. Watch at
least these signals:

- `simple_s3_cache_peer_ring_info`
- `simple_s3_cache_peer_forward_failures_total`
- `simple_s3_cache_gateway_forward_failures_total`
- `simple_s3_cache_peer_owner_decisions_total`
- per-peer hit rate, upstream fill bytes, cached bytes, and evictions

## Network Policy

`networkPolicy.enabled` installs component-specific policies. In gateway mode,
cache peers accept ingress from gateway pods and cache peers in the same release;
gateway pods accept client ingress from `networkPolicy.ingressFrom`, or from the
release namespace when `ingressFrom` is empty. Egress is unrestricted by default
because upstream object storage endpoints vary by cluster.

Tighten `networkPolicy` values for your cluster's client, gateway, peer, DNS,
and upstream storage paths before exposing the service outside a trusted network.
