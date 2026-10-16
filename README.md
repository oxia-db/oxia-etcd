<p align="center">
  <img src="logo.svg" width="200" alt="oxia-etcd"/>
</p>

<h2 align="center">oxia-etcd</h2>

<p align="center">
An etcd v3 API gateway backed by <a href="https://github.com/oxia-db/oxia">Oxia</a>.
Point any etcd client, including the Kubernetes API server, at it and keep the data in Oxia.
</p>

## Supported APIs

| Service | API | Supported |
|---|---|:---:|
| KV | `Range` | ✅ |
| | `RangeStream` | ✅ |
| | `Put` | ✅ |
| | `DeleteRange` | ✅ |
| | `Txn` | ⚠️ [1] |
| | `Compact` | ✅ |
| Watch | `Watch` | ✅ |
| Lease | `LeaseGrant` | ⚠️ [2] |
| | `LeaseRevoke` | ⚠️ [2] |
| | `LeaseKeepAlive` | ⚠️ [2] |
| | `LeaseTimeToLive` | ⚠️ [2] |
| | `LeaseLeases` | ⚠️ [2] |

[1] Oxia has no multi-key transaction. A transaction that writes one key, such as the compare-and-swap writes of the
Kubernetes API server, is atomic. A transaction that writes several keys is isolated from the other requests, but each
write gets its own revision, and a failure in the middle leaves the first writes applied.

[2] Oxia has no time-based lease. The gateway keeps the lease expiry in memory, and stores only the lease and its TTL in
Oxia. When another gateway takes over, the leases start their TTL again.
