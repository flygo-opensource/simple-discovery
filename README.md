# simple-discovery

Discovery transports that share one contract, one signed discovery envelope. Every package implements the same `Discovery<T>`:
an `Observable` of `DiscoveryMessage<T>` plus `broadcast(message)`. An application can switch how
nodes find each other without changing the code that consumes discovery.

| Package | Directory | How nodes find each other |
| --- | --- | --- |
| [`@simple-discovery/udp`](packages/udp/README.md) | [`packages/udp/`](packages/udp) | Signed UDP packets over multicast, explicit peers (IP or hostname), or both. Works over VPNs such as NetBird or WireGuard with `multicast: false`. |
| [`@simple-discovery/http`](packages/http/README.md) | [`packages/http/`](packages/http) | An HTTP registry; nodes register, heartbeat and deregister over HTTP. |

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
| `SIMPLE_DISCOVERY_UDP_DEBUG`, `SIMPLE_DISCOVERY_HTTP_DEBUG` | udp, http | off; prints network errors to stderr |

These replace the `OHAYO_*` variables of the pre-release `@ohayo/*` packages.

## Development

This is a [Bun workspace](https://bun.sh/docs/install/workspaces). Each package is built, tested and
published on its own.

```bash
bun install
bun run build   # every package
bun run test    # every package
```

Work on one package:

```bash
cd packages/udp
bun run build
bun run test
```

`packages/udp/e2e/lan-peers/run.sh` runs the peers mode between this machine and a second Linux machine; see
the comments at the top of the script.

## Publishing

Publish from the package directory, in any order (the packages do not depend on each other):

```bash
(cd packages/udp && bun run build && bun publish)
(cd packages/http && bun run build && bun publish)
```

## License

MIT
