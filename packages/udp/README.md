# @simple-discovery/udp

Discovery qua UDP: các process trong cùng mạng LAN công bố thông tin của mình và nhận thông tin của
nhau, không cần server trung tâm. Tìm nhau bằng multicast, hoặc bằng danh sách địa chỉ khi mạng chặn
multicast. Mọi gói đều được ký, nên chỉ các process dùng chung khoá mới thấy nhau.

Gói không phụ thuộc framework nào: payload là dữ liệu tuỳ ý của ứng dụng. Với Spider Mesh, xem
[phần tích hợp](#dùng-với-spider-mesh).

## Cài đặt

```bash
bun add @simple-discovery/udp rxjs
```

Chạy trên Node.js và Bun.

## Ví dụ

```ts
import { UdpDiscovery } from '@simple-discovery/udp'

type Worker = { name: string; httpPort: number }

const discovery = new UdpDiscovery<Worker>({
  namespace: 'shop',
  tags: ['worker'],
  node_id: 'worker-1',
  key: process.env.DISCOVERY_KEY!,
})

// Nhận thông tin từ các process khác (không nhận lại gói của chính mình).
discovery.subscribe(message => {
  console.log(`${message.node_id} at ${message.remote_host}:`, message.data)
})

// Công bố thông tin của process này.
await discovery.broadcast({
  node_id: 'worker-1',
  namespace: 'shop',
  tags: ['worker'],
  version: '1',
  created_at: Date.now(),
  seq: 1,
  data: { name: 'worker-1', httpPort: 3000 },
})

// Khi tắt process.
discovery.close()
```

### Nội dung một message

| Field | Ý nghĩa |
| --- | --- |
| `node_id` | Định danh process gửi. |
| `namespace` | Phải **trùng** `namespace` của discovery, nếu không `broadcast()` ném lỗi. |
| `tags` | Phải **chứa đủ** các `tags` của discovery, nếu không `broadcast()` ném lỗi. |
| `version`, `seq`, `created_at` | Do ứng dụng quản lý; discovery không so sánh hay sắp xếp. |
| `data` | Payload của ứng dụng. |
| `remote_host` | Discovery tự điền khi nhận: địa chỉ gửi tới. |

## Tuỳ chọn

| Tuỳ chọn | Biến môi trường | Mặc định | Ý nghĩa |
| --- | --- | --- | --- |
| `namespace` | – | *(bắt buộc)* | Chỉ nhận message cùng namespace. |
| `tags` | – | *(bắt buộc)* | Chỉ nhận message chứa đủ các tag này. |
| `node_id` | – | – | Nếu đặt: message gửi đi phải mang đúng ID này, và bỏ qua message nhận về có ID này. |
| `key` | `SIMPLE_DISCOVERY_KEY` | `'simple-discovery'` | Khoá ký gói (HMAC-SHA256). **Luôn đặt khoá riêng**, xem bên dưới. |
| `port` | `SIMPLE_DISCOVERY_PORT` | `11001` | Cổng UDP; mọi process phải dùng cùng cổng. |
| `multicast` | `SIMPLE_DISCOVERY_UDP_MULTICAST` (`off`/`false`/`0` để tắt) | `true` | Gửi tới nhóm multicast. Tắt khi mạng không chuyển multicast (VPN). |
| `multicastAddress` | `SIMPLE_DISCOVERY_UDP_MULTICAST_ADDRESS` | `239.0.1.1` | Nhóm multicast. |
| `peers` | `SIMPLE_DISCOVERY_UDP_WHITELIST_ADDRESS` (phân cách bằng dấu phẩy) | – | IP, hostname hoặc dải `/24` để gửi trực tiếp, xem bên dưới. |
| `packetTtlMs` | – | `30000` | Bỏ gói có thời gian lệch hơn mức này. |
| `broadcastCopies` | `SIMPLE_DISCOVERY_UDP_BROADCAST_COPIES` | `3` | Mỗi lần broadcast gửi mấy bản, bù cho gói bị mất. |

`SIMPLE_DISCOVERY_UDP_DEBUG=1` in lỗi mạng ra stderr.

## Chạy ở production

- **Khoá**: giá trị mặc định `'simple-discovery'` là công khai, ai trong mạng cũng giả mạo được message. Mọi
  process của một hệ thống dùng chung một khoá bí mật; hệ thống khác dùng khoá khác.
- **Đồng hồ**: gói lệch giờ quá `packetTtlMs` (30 giây) bị bỏ **không báo lỗi**, để chống phát lại
  gói cũ. Các máy phải đồng bộ giờ (NTP).
- **Mạng**: mở UDP cổng `port` giữa các máy. Multicast chỉ đi trong một subnet; khác subnet hoặc mạng
  chặn multicast thì dùng `peers`.
- **Không có tin rời đi**: UDP không báo khi một process tắt hay mất mạng. Ứng dụng phải biết điều
  này bằng cách khác: hoặc tự broadcast lại định kỳ và coi process im lặng quá lâu là đã rời đi, hoặc
  (như Spider Mesh) dựa vào kết nối thật giữa các process.
- **Không đảm bảo thứ tự**: gói có thể mất, trùng hoặc đến sai thứ tự; discovery không lọc trùng.

## Multicast và peers

Mặc định discovery gửi tới nhóm multicast, nên mọi process cùng subnet nhận được. Khi multicast không
dùng được, liệt kê các máy trong `peers`. Mỗi mục là một IPv4, một hostname (phân giải DNS mỗi lần
gửi, cần bản ghi IPv4), hoặc 3 octet đầu để gửi tới cả dải `/24`:

```ts
import { UdpDiscovery } from '@simple-discovery/udp'

const discovery = new UdpDiscovery<{ name: string }>({
  namespace: 'shop',
  tags: ['worker'],
  key: process.env.DISCOVERY_KEY!,
  peers: ['192.168.1.21', 'worker-2.internal', '10.0.5'],
})
```

Multicast vẫn bật cùng `peers`, trừ khi đặt `multicast: false`.

**Chỉ cần khai một chiều.** Khi nhận message hợp lệ từ một địa chỉ, discovery nhớ địa chỉ đó và gửi cả
các lần broadcast sau tới nó. Vì vậy node mới chỉ cần liệt kê các node đã có; node cũ không phải sửa
cấu hình. Hai node chưa từng nói chuyện với nhau thì không tự biết nhau: mỗi node cần liệt kê được ít
nhất những node nó phải tìm thấy.

### Qua VPN (NetBird, WireGuard, Tailscale)

VPN kiểu này chỉ chuyển gói unicast, không chuyển multicast hay broadcast. Tắt multicast và liệt kê
các máy bằng IP hoặc tên DNS trong VPN:

```ts
import { UdpDiscovery } from '@simple-discovery/udp'

const discovery = new UdpDiscovery<{ name: string }>({
  namespace: 'shop',
  tags: ['worker'],
  key: process.env.DISCOVERY_KEY!,
  multicast: false,
  peers: ['worker-1.netbird.cloud', 'worker-2.netbird.cloud'],
})
```

Hoặc chỉ bằng biến môi trường:

```bash
SIMPLE_DISCOVERY_UDP_MULTICAST=off SIMPLE_DISCOVERY_UDP_WHITELIST_ADDRESS=worker-1.netbird.cloud,worker-2.netbird.cloud
```

- Chính sách truy cập của VPN phải cho phép UDP cổng `port` giữa các máy.
- Dải `/24` không phù hợp: IP trong VPN thường rải trên một dải lớn hơn (NetBird dùng `/16`).
- Các process trên cùng một máy vẫn chuyển tiếp gói cho nhau qua multicast nội bộ (TTL 1), kể cả khi
  `multicast: false`.

**Nhiều process trên một máy** dùng chung cổng. Gói unicast từ máy khác chỉ tới một process, và
process đó chuyển tiếp cho các process còn lại trên máy, nên mọi process đều nhận được.

**Process vào sau** biết ngay các process đã có mặt: khi nhận message mới của một process khác,
discovery tự gửi lại message gần nhất của mình.

## Vòng đời

```ts
import { UdpDiscovery } from '@simple-discovery/udp'
import { filter, firstValueFrom } from 'rxjs'

const discovery = new UdpDiscovery<{ name: string }>({
  namespace: 'shop',
  tags: ['worker'],
  key: process.env.DISCOVERY_KEY!,
})

// 'not_ready' -> 'ready' khi socket đã bind; 'closed' sau close().
discovery.status$.subscribe(status => console.log('discovery', status))
await firstValueFrom(discovery.status$.pipe(filter(status => status === 'ready')))

discovery.close() // gọi lại nhiều lần cũng không sao
```

`broadcast()` tự đợi tới khi `ready`.

## Dùng với Spider Mesh

`@spider-mesh/tcp` dùng discovery này để các server tự tìm nhau. Nối vào `Topology` qua
`TopologyDiscoveryAdapter` có sẵn trong `@spider-mesh/tcp`:

```ts
import { SpiderMesh, Topology, type SpiderMeshNode } from '@spider-mesh/core'
import { Http2Rpc, TopologyDiscoveryAdapter } from '@spider-mesh/tcp'
import { UdpDiscovery } from '@simple-discovery/udp'

const udp = new UdpDiscovery<SpiderMeshNode>({
  namespace: process.env.SPIDERMESH_NAMESPACE ?? 'default', // phải trùng namespace của mesh
  tags: ['spider-mesh', 'node'],
  key: process.env.DISCOVERY_KEY!,
})

const topology = new Topology({
  discovery: new TopologyDiscoveryAdapter(udp, {
    onError: error => console.error('discovery broadcast failed', error),
  }),
  // UDP chỉ để tìm thấy nhau; kết nối HTTP/2 cho biết node còn sống.
  removeUnreachableAfterMs: 5 * 60_000,
})

const mesh = new SpiderMesh({ topology, transporters: [new Http2Rpc()] })
```

Hướng dẫn đầy đủ (biến môi trường, cổng cần mở, lỗi hay gặp) nằm trong README của `@spider-mesh/tcp`.
