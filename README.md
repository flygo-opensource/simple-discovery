# simple-discovery

Discovery transports that share one contract, one signed discovery envelope. Every package implements the same `Discovery<T>`:
an `Observable` of `DiscoveryMessage<T>` plus `broadcast(message)`. An application can switch how
nodes find each other without changing the code that consumes discovery.

| Package | Directory | How nodes find each other |
| --- | --- | --- |
| [`@simple-discovery/udp`](packages/udp/README.md) | [`packages/udp/`](packages/udp) | Signed UDP packets over multicast, explicit peers (IP or hostname), or both. Works over VPNs such as NetBird or WireGuard with `multicast: false`. |
| [`@simple-discovery/http`](packages/http/README.md) | [`packages/http/`](packages/http) | An HTTP registry; nodes register, heartbeat and deregister over HTTP. |
| [`@simple-discovery/redis`](packages/redis/README.md) | [`packages/redis/`](packages/redis) | Redis pub/sub channel per namespace. |
| [`@simple-discovery/nats`](packages/nats/README.md) | [`packages/nats/`](packages/nats) | NATS subject per namespace. |
| [`@simple-discovery/amqp`](packages/amqp/README.md) | [`packages/amqp/`](packages/amqp) | AMQP 0-9-1 (RabbitMQ) fanout exchange per namespace, one private queue per node. |

Which one to pick:

- **udp**: no infrastructure, nodes on one LAN or VPN.
- **redis**, **nats**, **amqp**: you already run that broker, or nodes span networks where multicast
  does not reach (clouds, Kubernetes). A node that joins late learns the others at once: its first
  broadcast carries a `hello` flag and every node answers exactly once. After a broker reconnect the
  node says hello again.
- **http**: a central registry that also reports nodes going offline.

Each package is self-contained: install only the one you use. The shared code lives in
[`packages/core`](packages/core/README.md), a private workspace package that is bundled (code and
types) into every published package at build time and never published on its own.

```ts
import { UdpDiscovery } from '@simple-discovery/udp'

const discovery = new UdpDiscovery<{ name: string }>({
  namespace: 'shop',
  tags: ['worker'],
  key: process.env.DISCOVERY_KEY!,
})

discovery.subscribe(message => console.log('found', message.node_id, message.data))

await discovery.broadcast({
  node_id: 'worker-1',
  namespace: 'shop',
  tags: ['worker'],
  version: '1',
  created_at: Date.now(),
  seq: 1,
  data: { name: 'worker-1' },
})
```

Used by Spider Mesh (`@spider-mesh/tcp`) and LiveQuery for discovery on their networks.

## Configuration

Options passed to the constructor win; otherwise each package reads environment variables with the
`SIMPLE_DISCOVERY_` prefix.

| Variable | Package | Default |
| --- | --- | --- |
| `SIMPLE_DISCOVERY_KEY` | udp, http | `'simple-discovery'`. Always set your own key in production. |
| `SIMPLE_DISCOVERY_PORT` | udp, http | `11001` (udp), `12001` (http) |
| `SIMPLE_DISCOVERY_UDP_MULTICAST` | udp | on; `off`/`false`/`0` sends to `peers` only |
| `SIMPLE_DISCOVERY_UDP_MULTICAST_ADDRESS` | udp | `239.0.1.1` |
| `SIMPLE_DISCOVERY_UDP_WHITELIST_ADDRESS` | udp | none; comma-separated IPs, hostnames or `/24` prefixes |
| `SIMPLE_DISCOVERY_UDP_BROADCAST_COPIES` | udp | `3` |
| `SIMPLE_DISCOVERY_REDIS_URL` | redis | `redis://127.0.0.1:6379` |
| `SIMPLE_DISCOVERY_NATS_SERVERS` | nats | `nats://127.0.0.1:4222`; comma-separated |
| `SIMPLE_DISCOVERY_AMQP_URL` | amqp | `amqp://127.0.0.1` |
| `SIMPLE_DISCOVERY_<TRANSPORT>_DEBUG` | all | off; `UDP`, `HTTP`, `REDIS`, `NATS` or `AMQP`, prints network errors to stderr |

These replace the `OHAYO_*` variables of the pre-release `@ohayo/*` packages.

## Development

This is a [Bun workspace](https://bun.sh/docs/install/workspaces). Each package is built, tested and
published on its own.

```bash
bun install
docker compose up -d   # Redis, NATS and RabbitMQ for the broker tests
bun run build          # every package (tsup bundles core into each)
bun run test           # every package
```

The redis, nats and amqp suites skip themselves when their broker is not reachable, except in CI
(`CI` set), where a missing broker fails the run. Point them elsewhere with `SIMPLE_DISCOVERY_REDIS_URL`,
`SIMPLE_DISCOVERY_NATS_SERVERS` and `SIMPLE_DISCOVERY_AMQP_URL`.

Work on one package:

```bash
cd packages/udp
bun run build
bun run test
```

End-to-end runs on real machines, driven over SSH (see the comments at the top of each script):

- `packages/udp/e2e/lan-peers/run.sh`: UDP peers mode between this machine and a second Linux machine.
- `e2e/brokers/run.sh`: Redis, NATS and AMQP across this machine and several Linux hosts, with the
  brokers in Docker on one of them, including a broker restart mid-run.

## Publishing

Build everything, then publish from each package directory, in any order: no published package
depends on another. `core` is private and is never published.

```bash
bun run build
for p in udp http redis nats amqp; do (cd packages/$p && bun publish); done
```

## License

MIT
