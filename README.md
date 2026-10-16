# oxia-etcd

An etcd v3 API gateway backed by [Oxia](https://github.com/oxia-db/oxia). Any etcd v3 client, including the
Kubernetes API server, can use it as its etcd.

```
kube-apiserver / etcdctl / clientv3 ──etcd v3 gRPC──▶ oxia-etcd ──Oxia client──▶ Oxia (shard leader)
```

## How it works

- **One shard per etcd cluster.** All the records of a cluster share one Oxia partition key, so they live in one
  shard, and a key prefix keeps them apart from other clusters in the same namespace.
- **Revisions are Oxia version ids.** The shard's version id grows with every write, so it orders writes like
  etcd's revision.
- **Every write is a compare-and-swap** on the record's version id. A deleted key keeps a tombstone record, so the
  deletion gets a revision too. Compaction removes the tombstones.
- **One gateway writes at a time.** The gateway holds an ephemeral owner record. Other gateways of the same cluster
  stand by and take over when it goes away. Writes are serialized, so revisions, events and responses follow one
  order.
- **Watches and past reads come from an in-memory event history** (`--history-size`). The history starts when a
  gateway takes over. A watch or read before it gets `ErrCompacted`, and clients such as the Kubernetes API
  server read the keys again, as they do after an etcd compaction.

## etcd API coverage

| Service | Supported |
|---|---|
| KV | `Range` (prefix, from-key, limit, count, keys-only, sort, min/max revision filters, past revisions), `RangeStream`, `Put` (prev kv, ignore value/lease, lease), `DeleteRange`, `Txn` (any compares and ops, nested), `Compact` |
| Watch | Ranges, start revision, prev kv, put/delete filters, client watch ids, cancel, compaction, progress notifications and progress requests |
| Lease | `Grant`, `Revoke`, `KeepAlive`, `TimeToLive` (with keys), `Leases`, expiry |
| Maintenance | `Status`, `HashKV`, `Hash`, `Alarm`, `Defragment` (no-op) |
| Cluster | `MemberList` (the gateway) |
| HTTP | `/version`, `/health`, `/readyz`, `/livez` |

Not supported: `Snapshot` (use Oxia's backups), membership changes, `MoveLeader`, `Downgrade`, and authentication
(protect access with the network or TLS).

Differences from etcd:

- Each write in a transaction, or each key of a `DeleteRange`, gets its own revision. etcd gives them one revision.
  The revisions grow, but they are not contiguous.
- A transaction is atomic against other requests, but a backend failure in the middle of it can leave its first
  writes applied.

## Run it

```sh
go build -o bin/oxia-etcd ./cmd/oxia-etcd
./bin/oxia-etcd --oxia localhost:6648 --cluster my-cluster --listen 0.0.0.0:2379
```

| Flag | Default | |
|---|---|---|
| `--oxia` | `localhost:6648` | Oxia service address |
| `--namespace` | `default` | Oxia namespace |
| `--cluster` | `default` | etcd cluster name: its records share one Oxia shard |
| `--listen` | `0.0.0.0:2379` | Address of the etcd API |
| `--advertise-client-url` | `http://127.0.0.1:2379` | URL reported by `MemberList` |
| `--history-size` | `100000` | Events kept for watchers and past reads |
| `--progress-interval` | `5s` | Interval of watch progress notifications |
| `--session-timeout` | `10s` | Oxia session timeout of the gateway's ownership |

## Demo: Kubernetes on Oxia

`demo/up.sh` starts Oxia and the gateway in Docker, then a [kind](https://kind.sigs.k8s.io) cluster whose API
server uses the gateway as its external etcd (`demo/kind.yaml`):

```sh
./demo/up.sh
kubectl --context kind-oxia-etcd-demo get pods -A   # no etcd pod
./demo/down.sh
```

## Tests

The tests drive the gateway with the etcd client. They use an in-memory backend, or Oxia when `OXIA_ADDR` is set:

```sh
go test -race ./...
docker run -d -p 6648:6648 oxia/oxia:0.17.1 oxia standalone
OXIA_ADDR=localhost:6648 go test -race ./...
```
