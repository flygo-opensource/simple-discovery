# Hướng dẫn dùng `@simple-discovery/udp` (dành cho agent)

Tài liệu này dành cho agent tích hợp discovery vào một dự án. Làm đúng theo thứ tự; phần "Quy tắc bắt
buộc" là những lỗi hay gặp nhất.

## 1. Gói này làm gì

Các process trong cùng mạng tự tìm thấy nhau, không cần server trung tâm. Mỗi process định kỳ phát
(`broadcast`) một message có chữ ký HMAC; các process khác cùng `namespace`, cùng `tags` và cùng
`key` sẽ nhận được qua `subscribe`. Payload `data` là dữ liệu tuỳ ý của ứng dụng (địa chỉ, cổng, vai
trò...).

- Chạy trên Node.js ≥ 18 và Bun. ESM only.
- Tìm nhau bằng multicast trong một subnet, hoặc bằng danh sách `peers` (IP/hostname) khi mạng chặn
  multicast (VPN như NetBird, WireGuard, Tailscale).
- **Không có tin "rời đi"**: process tắt hay mất mạng thì không ai được báo. Ứng dụng phải tự coi node
  im lặng quá lâu là đã rời đi (xem mục 3).

## 2. Cài đặt

```bash
bun add @simple-discovery/udp rxjs
# hoặc: npm install @simple-discovery/udp rxjs
```

Tên cũ `@ohayo/udp` và biến môi trường `OHAYO_*` **không còn dùng**. Nếu dự án đang có chúng, thay
toàn bộ bằng `@simple-discovery/udp` và `SIMPLE_DISCOVERY_*`.

## 3. Mẫu tích hợp chuẩn (copy rồi sửa `Node`)

```ts
import { UdpDiscovery, type DiscoveryMessage } from '@simple-discovery/udp'

type Node = { host: string; port: number; role: string }

const NAMESPACE = process.env.DISCOVERY_NAMESPACE ?? 'my-app'
const TAGS = ['my-app', 'worker']
const NODE_ID = process.env.NODE_ID ?? `${process.env.HOSTNAME ?? 'node'}-${process.pid}`
const HEARTBEAT_MS = 5_000
const OFFLINE_AFTER_MS = 3 * HEARTBEAT_MS

const discovery = new UdpDiscovery<Node>({
    namespace: NAMESPACE,
    tags: TAGS,
    node_id: NODE_ID,
    key: process.env.SIMPLE_DISCOVERY_KEY!, // bắt buộc đặt ở production
})

// Danh sách node đang sống: node_id -> message mới nhất + thời điểm thấy lần cuối.
const nodes = new Map<string, { message: DiscoveryMessage<Node>; lastSeen: number }>()

discovery.subscribe(message => {
    const known = nodes.get(message.node_id)
    if (!known) console.log('node joined', message.node_id, message.remote_host, message.data)
    nodes.set(message.node_id, { message, lastSeen: Date.now() })
})

// Node im lặng quá lâu coi như đã rời đi.
setInterval(() => {
    for (const [id, node] of nodes) {
        if (Date.now() - node.lastSeen > OFFLINE_AFTER_MS) {
            nodes.delete(id)
            console.log('node left', id)
        }
    }
}, HEARTBEAT_MS).unref()

// Heartbeat: mỗi lần phát phải tăng `seq` và đổi `created_at`/`version`.
let seq = 0
async function announce() {
    const now = Date.now()
    await discovery.broadcast({
        node_id: NODE_ID,
        namespace: NAMESPACE,
        tags: TAGS,
        version: String(now),
        created_at: now,
        seq: ++seq,
        data: { host: '10.0.0.5', port: 3000, role: 'worker' },
    })
}
await announce()
const heartbeat = setInterval(() => void announce().catch(console.error), HEARTBEAT_MS)

// Tắt gọn gàng.
for (const signal of ['SIGINT', 'SIGTERM'] as const) {
    process.once(signal, () => {
        clearInterval(heartbeat)
        discovery.close()
        process.exit(0)
    })
}
```

## 4. Quy tắc bắt buộc

1. **`namespace` của message phải trùng `namespace` của discovery**, và `tags` của message phải chứa
   đủ `tags` của discovery. Sai thì `broadcast()` ném lỗi.
2. **Nếu đặt `node_id` trong options**, message gửi đi phải mang đúng `node_id` đó. Discovery tự bỏ
   qua message của chính mình, nên đừng dùng việc "nhận lại gói của mình" để kiểm tra.
3. **Luôn đặt `key` riêng** (qua option `key` hoặc `SIMPLE_DISCOVERY_KEY`). Khoá mặc định
   `'simple-discovery'` là công khai. Mọi process của một hệ thống dùng **cùng** một khoá; khác khoá
   là không thấy nhau và **không có lỗi nào báo**.
4. **Mọi process phải dùng cùng `port`** (mặc định `11001`) và mở UDP cổng đó trên firewall/VPN.
5. **Đồng hồ các máy phải đồng bộ (NTP)**. Gói lệch giờ quá `packetTtlMs` (30 giây) bị bỏ im lặng.
6. **Không dựa vào thứ tự hay tính duy nhất của message**: gói có thể mất, trùng hoặc đến sai thứ tự.
   Dùng `seq` để bỏ message cũ nếu ứng dụng cần.
7. **`broadcast()` tự đợi socket sẵn sàng**; không cần đợi `status$` trước khi gọi.
8. **Gọi `close()` khi tắt process** để giải phóng cổng UDP. Gọi nhiều lần không sao.
9. **`remote_host`** do discovery tự điền khi nhận (địa chỉ IP người gửi). Đừng đặt nó khi gửi, trừ
   khi muốn ghi đè có chủ đích.

## 5. Cấu hình bằng biến môi trường

Option trong constructor luôn thắng biến môi trường.

| Biến | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `SIMPLE_DISCOVERY_KEY` | `'simple-discovery'` | Khoá ký HMAC-SHA256. |
| `SIMPLE_DISCOVERY_PORT` | `11001` | Cổng UDP. |
| `SIMPLE_DISCOVERY_UDP_MULTICAST` | bật | `off`/`false`/`0` để chỉ gửi tới `peers`. |
| `SIMPLE_DISCOVERY_UDP_MULTICAST_ADDRESS` | `239.0.1.1` | Nhóm multicast. |
| `SIMPLE_DISCOVERY_UDP_WHITELIST_ADDRESS` | – | `peers`, phân cách bằng dấu phẩy: IP, hostname hoặc 3 octet `/24`. |
| `SIMPLE_DISCOVERY_UDP_BROADCAST_COPIES` | `3` | Số bản gửi mỗi lần broadcast. |
| `SIMPLE_DISCOVERY_UDP_DEBUG` | tắt | `1` để in lỗi mạng ra stderr. |

## 6. Chọn chế độ mạng

| Môi trường | Cấu hình |
| --- | --- |
| Cùng máy, hoặc cùng LAN/subnet | Mặc định (multicast). Không cần gì thêm. |
| Khác subnet, cloud VPC chặn multicast | Thêm `peers: ['10.0.1.12', 'worker-2.internal']`. |
| VPN (NetBird, WireGuard, Tailscale) | `multicast: false` + `peers` là IP/hostname trong VPN. |
| Docker | Dùng `network_mode: host`, hoặc liệt kê `peers` và publish `11001/udp`. |
| Kubernetes | Dùng headless Service làm hostname trong `peers`, hoặc cân nhắc transport khác. |

`peers` chỉ cần khai **một chiều**: node mới liệt kê các node đã có là đủ, node cũ tự nhớ địa chỉ node
mới khi nhận được message của nó. Không dùng dải `/24` trong VPN (IP VPN thường nằm trong dải `/16`).

## 7. Tự kiểm tra sau khi tích hợp

1. Chạy 2 process cùng máy với cùng `namespace`, `tags`, `key`: mỗi bên phải log `node joined` bên kia
   trong vài giây.
2. Tắt một process: bên còn lại phải log `node left` sau khoảng `OFFLINE_AFTER_MS`.
3. Đổi `key` ở một bên: hai bên **không** được thấy nhau.
4. Không thấy nhau thì bật `SIMPLE_DISCOVERY_UDP_DEBUG=1` và kiểm tra theo thứ tự: cùng `key` →
   cùng `port` → cùng `namespace`/`tags` → firewall mở UDP `port` → đồng hồ lệch → mạng có chặn
   multicast không (nếu có, dùng `peers`).

## 8. Tích hợp Spider Mesh

Dự án dùng `@spider-mesh/tcp` thì bọc discovery bằng `TopologyDiscoveryAdapter` thay vì tự quản lý
heartbeat như mục 3:

```ts
import { SpiderMesh, Topology, type SpiderMeshNode } from '@spider-mesh/core'
import { Http2Rpc, TopologyDiscoveryAdapter } from '@spider-mesh/tcp'
import { UdpDiscovery } from '@simple-discovery/udp'

const udp = new UdpDiscovery<SpiderMeshNode>({
    namespace: process.env.SPIDERMESH_NAMESPACE ?? 'default', // phải trùng namespace của mesh
    tags: ['spider-mesh', 'node'],
    key: process.env.SIMPLE_DISCOVERY_KEY!,
})

const topology = new Topology({
    discovery: new TopologyDiscoveryAdapter(udp, {
        onError: error => console.error('discovery broadcast failed', error),
    }),
    removeUnreachableAfterMs: 5 * 60_000,
})

const mesh = new SpiderMesh({ topology, transporters: [new Http2Rpc()] })
```

## 9. Tham khảo

- README đầy đủ: https://github.com/flygo-opensource/simple-discovery/tree/main/packages/udp#readme
- npm: https://www.npmjs.com/package/@simple-discovery/udp
