# Ohayo HTTP Discovery

Ohayo HTTP Discovery la transport discovery dua tren HTTP registry. No implement chung contract `Discovery<T>` de app co the doi transport giua HTTP, UDP, TCP hoac mesh ma khong doi logic gateway/service.

HTTP phu hop production hon UDP multicast vi de firewall, de quan sat bang log/metrics, co request timeout, retry, heartbeat va graceful deregister ro rang.

```bash
bun add @ohayo/http
```

Package export `HttpDiscovery<T>`, `Discovery<T>`, `DiscoveryMessage<T>`,
`DiscoveryOfflineData`, `isDiscoveryOfflineData()` va lifecycle `status$`. No khong phu thuoc
Livequery hay Spider Mesh.

## Contract Chung

```ts
import type { Observable } from 'rxjs'

export type DiscoveryMessage<T> = {
  node_id: string
  namespace: string
  tags: string[]
  version: string
  created_at: number
  seq: number
  data: T
  remote_host?: string
}

export type DiscoveryOptions = {
  namespace: string
  tags: string[]
  node_id?: string
}

export type Discovery<T> = Observable<DiscoveryMessage<T>> & {
  broadcast(message: DiscoveryMessage<T>): Promise<void>
  close(): void
}
```

Day la cung structural contract voi `@ohayo/udp`. HTTP co them helper
`DiscoveryOfflineData` va `isDiscoveryOfflineData()` vi registry co the phat su kien
TTL/deregister. De nhan offline event type-safe, dua no vao payload union:

```ts
import { HttpDiscovery, type DiscoveryOfflineData } from '@ohayo/http'

type Metadata = ServiceMetadata | DiscoveryOfflineData
const discovery = new HttpDiscovery<Metadata>(options)
```

Neu consumer da tu kiem tra offline payload tu `unknown` (nhu `@livequery/core`), co the dung
`HttpDiscovery<ServiceMetadata>` de instance khop truc tiep `Discovery<ServiceMetadata>`.

Rule bat buoc:

- Transport khong hieu app payload. Metadata rieng cua app luon nam trong `message.data`.
- `namespace` phai match chinh xac.
- `tags` dung rule contains-all: message co the co tag phu, nhung phai co du moi tag trong constructor options.
- Neu constructor co `node_id`, outbound message phai dung node do; inbound message tu cung node bi ignore de tranh self-discovery.
- `remote_host` la transport metadata, neu co thi nam o envelope level, khong nhap vao `data`.

## Constructor

Server mode:

```ts
new HttpDiscovery<T>({
  mode: 'server'
  namespace: string
  tags: string[]
  node_id?: string
  key?: string
  host?: string
  port?: number
  ttlMs?: number
})
```

Client mode:

```ts
new HttpDiscovery<T>({
  mode: 'client'
  namespace: string
  tags: string[]
  node_id?: string
  key?: string
  servers: string[]
  heartbeatMs?: number
  requestTimeoutMs?: number
  retryAttempts?: number
})
```

Options:

- `mode`: bat buoc. `server` mo HTTP registry; `client` dang ky metadata toi cac registry.
- `namespace`: discovery namespace bat buoc.
- `tags`: required tags bat buoc.
- `node_id`: node id co dinh cho instance hien tai.
- `key`: bearer token. Mac dinh doc `OHAYO_DISCOVERY_KEY`, fallback development la `ohayo`.
- `host`: server-only bind address; bo trong de Node bind tren cac interface mac dinh.
- `port`: server-only registry port. Mac dinh doc `OHAYO_DISCOVERY_PORT`, fallback `12001`; `0` chon random free port.
- `ttlMs`: server-only expiry window cho missed heartbeat; mac dinh 35 giay.
- `servers`: client-only, bat buoc va khong duoc rong. Moi entry la `host:port` hoac HTTP(S) URL.
- `heartbeatMs`: client-only heartbeat interval cho last broadcast message; mac dinh 10 giay, `0` de tat.
- `requestTimeoutMs`: client-only timeout moi register/deregister request; mac dinh 2 giay.
- `retryAttempts`: client-only so lan retry toi da cho moi server; mac dinh 5.

Discriminated union lam TypeScript bat loi config sai mode: server khong nhan `servers`, client khong
nhan `port`/`ttlMs`. Runtime cung reject client co `servers: []`.

## Env

```sh
OHAYO_DISCOVERY_KEY=shared-secret
OHAYO_DISCOVERY_PORT=12001
```

- `namespace` duoc truyen bang constructor; package khong doc `OHAYO_DISCOVERY_NAMESPACE`.
- `OHAYO_DISCOVERY_PORT` la registry port noi bo, khong phai public API port.
- Danh sach server khong doc tu environment; client phai nhan `servers` trong constructor.
- Tat ca environment variable cua package bat dau bang `OHAYO_`.

## Server Mode

Server mode mo HTTP registry noi bo:

```ts
const discovery = new HttpDiscovery<Metadata>({
  mode: 'server',
  namespace: 'default',
  tags: ['livequery'],
  node_id: 'registry-1',
  key: 'shared-secret',
  host: '0.0.0.0',
  port: 12001,
  ttlMs: 35_000,
})
```

```txt
POST   /register
DELETE /register/:node_id
GET    /health
GET    /nodes
```

### `GET /health`

Public health probe.

Response:

```json
{ "ok": true }
```

### `POST /register`

Dang ky hoac refresh node metadata.

Headers:

```txt
Authorization: Bearer <OHAYO_DISCOVERY_KEY>
Content-Type: application/json
```

Body la `DiscoveryMessage<T>`:

```json
{
  "node_id": "posts-service-1",
  "namespace": "default",
  "tags": ["livequery", "service"],
  "version": "1780000000000",
  "created_at": 1780000000000,
  "seq": 42,
  "data": {
    "role": "service",
    "name": "posts",
    "host": "10.0.0.20",
    "port": 3001,
    "paths": [
      { "method": "GET", "path": "livequery/posts" }
    ],
    "linked": []
  }
}
```

Behavior:

1. Check bearer token.
2. Validate envelope fields.
3. Check exact `namespace`.
4. Check contains-all `tags`.
5. Ignore self message when `node_id` matches local node.
6. If an existing node has `seq >= incoming.seq`, update `lastSeen` but do not emit.
7. Store latest message and attach `remote_host`.
8. Emit accepted message through Observable.

Status:

- `204` on accepted registration.
- `400` on invalid envelope, namespace, or tags.
- `401` on invalid token.

### `DELETE /register/:node_id`

Graceful deregistration.

Headers:

```txt
Authorization: Bearer <OHAYO_DISCOVERY_KEY>
```

Behavior:

- Remove node from registry if present.
- Emit offline event:

```json
{
  "node_id": "posts-service-1",
  "namespace": "default",
  "tags": ["livequery", "service"],
  "version": "1780000000001",
  "created_at": 1780000000001,
  "seq": 43,
  "data": { "status": "offline" }
}
```

Status:

- `204` whether the node existed or not.
- `401` on invalid token.

### `GET /nodes`

Authenticated debug/snapshot endpoint.

Headers:

```txt
Authorization: Bearer <OHAYO_DISCOVERY_KEY>
```

Response:

```json
{
  "nodes": [
    {
      "node_id": "posts-service-1",
      "namespace": "default",
      "tags": ["livequery", "service"],
      "version": "1780000000000",
      "created_at": 1780000000000,
      "seq": 42,
      "data": { "role": "service" },
      "remote_host": "10.0.0.20"
    }
  ]
}
```

## Client Mode

Client mode khong mo HTTP server. No gui registration song song toi tat ca server duoc truyen trong
constructor:

```ts
const discovery = new HttpDiscovery<ServiceMetadata>({
  mode: 'client',
  namespace: 'default',
  tags: ['livequery'],
  node_id: 'posts-service-1',
  key: 'shared-secret',
  servers: ['10.0.0.10:12001', 'https://discovery-2.internal'],
})

await discovery.broadcast({
  node_id: 'posts-service-1',
  namespace: 'default',
  tags: ['livequery', 'service'],
  version: String(Date.now()),
  created_at: Date.now(),
  seq: 1,
  data: {
    role: 'service',
    name: 'posts',
    host: '10.0.0.20',
    port: 3001,
    paths: [{ method: 'GET', path: 'livequery/posts' }],
    linked: [],
  },
})
```

Behavior:

- `broadcast(message)` validates outbound envelope before sending.
- Registration uses `POST /register` song song toi moi server.
- Loi cua mot server khong ngan registration toi cac server con lai.
- Moi server loi co retry/exponential backoff doc lap.
- Heartbeat rebroadcasts last message toi tat ca server va bump `version`, `created_at`, `seq`.
- `close()` gui best-effort `DELETE /register/:node_id` toi tat ca server.

Server URL normalization:

- `10.0.0.10:12001` becomes `http://10.0.0.10:12001`.
- `http://10.0.0.10:12001/` is accepted.

## Heartbeat, TTL, And Offline

HTTP server/client cung quan ly registry liveness:

- Service heartbeat refreshes registration.
- Gateway TTL expires stale registrations.
- Expiry emits `DiscoveryMessage<DiscoveryOfflineData>`.
- Graceful close also emits offline via `DELETE /register/:node_id`.

Consumers should handle offline events by checking:

```ts
import { isDiscoveryOfflineData } from '@ohayo/http'

discovery.subscribe(message => {
  if (isDiscoveryOfflineData(message.data)) {
    console.log('offline', message.node_id)
    return
  }
  console.log('online', message.node_id, message.data)
})
```

## Livequery Integration

`ApiGatewayHandler` va `ApiServiceLinker` nhan discovery theo structural contract, nen Ohayo HTTP
instance duoc truyen thang vao constructor:

```ts
import { HttpDiscovery } from '@ohayo/http'
import {
  ApiGatewayHandler,
  ApiServiceLinker,
  type ServiceApiMetadata,
} from '@livequery/core'

const gatewayDiscovery = new HttpDiscovery<ServiceApiMetadata>({
  mode: 'server',
  namespace: 'default',
  tags: ['livequery'],
  node_id: 'gateway-1',
  key: 'shared-secret',
  port: 12001,
})

const gateway = new ApiGatewayHandler({ discovery: gatewayDiscovery })

const serviceDiscovery = new HttpDiscovery<ServiceApiMetadata>({
  mode: 'client',
  namespace: 'default',
  tags: ['livequery'],
  node_id: 'products-service-1',
  key: 'shared-secret',
  servers: ['10.0.0.10:12001', '10.0.0.11:12001'],
})

const service = new ApiServiceLinker({
  paths: [{ method: 'GET', path: 'livequery/products' }],
  discovery: serviceDiscovery,
})

service.start('products-service', 3001)
```

Moi linker so huu discovery instance duoc truyen vao: `gateway.close()` hoac `service.close()` se
goi `discovery.close()`. Khong chia se cung mot instance cho hai linker co lifecycle doc lap.

## Security

Minimum security is bearer auth:

```txt
Authorization: Bearer <OHAYO_DISCOVERY_KEY>
```

Operational requirements:

- Treat registry port as internal-only.
- Do not expose it directly to the internet.
- Use firewall, private subnet, service mesh, or network policy.
- Rotate `OHAYO_DISCOVERY_KEY` like any shared secret.
- Log rejected auth attempts.

Bearer auth proves the sender knows the secret, but it does not sign the body. If an implementation needs replay/body integrity, add timestamped HMAC headers as an extension without changing `DiscoveryMessage<T>`.

## Required Tests

A conforming HTTP implementation should test:

- Registers a valid `DiscoveryMessage<T>` through `POST /register`.
- Rejects missing or invalid bearer token.
- Rejects malformed envelopes.
- Filters inbound messages by exact namespace.
- Filters inbound messages by contains-all tags.
- Rejects outbound messages with wrong namespace, missing tags, or wrong fixed `node_id`.
- Ignores self inbound messages when `node_id` is configured.
- Ignores stale registrations where `seq` is older or equal while refreshing `lastSeen`.
- `GET /health` returns `200`.
- `GET /nodes` requires auth and returns registered node snapshots.
- Heartbeat rebroadcasts the last message with bumped `version`, `created_at`, and `seq`.
- Retry registers successfully when gateway becomes available after service broadcast.
- TTL expiry emits offline event.
- `close()` sends best-effort deregistration.
- `close()` is idempotent and completes observable/status streams.

## Relation To UDP Discovery

HTTP and UDP are sibling transports:

```txt
Discovery<T>
  - HttpDiscovery<T>
  - UdpDiscovery<T>
  - TcpDiscovery<T>
  - MeshDiscovery<T>
```

HTTP should be the default for production. UDP remains useful for local network zero-config discovery. Both must keep app metadata inside `data` and expose the same `DiscoveryMessage<T>` contract to consumers.
