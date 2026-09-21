# @simple-discovery/nats

Discovery qua NATS: mọi process kết nối tới cùng một NATS (hoặc cùng một cluster) publish thông tin của
mình lên một subject và nhận thông tin của nhau. Hợp với hệ thống đã dùng NATS, chạy trên nhiều mạng,
nhiều cluster Kubernetes, hoặc qua leaf node.

Gói tin giống hệt các transport khác của simple-discovery: msgpack, ký HMAC-SHA256, chống phát lại.

## Cài đặt

```bash
bun add @simple-discovery/nats rxjs
```

Chạy trên Node.js ≥ 18 và Bun. Dùng [`@nats-io/transport-node`](https://github.com/nats-io/nats.js) v3.

## Ví dụ

```ts
import { NatsDiscovery } from '@simple-discovery/nats'

type Worker = { host: string; httpPort: number }

const discovery = new NatsDiscovery<Worker>({
  servers: ['nats://nats-1.internal:4222', 'nats://nats-2.internal:4222'],
  namespace: 'shop',
  tags: ['worker'],
  node_id: 'worker-1',
  key: process.env.SIMPLE_DISCOVERY_KEY!,
})

discovery.subscribe(message => console.log(message.node_id, message.data))

await discovery.broadcast({
  node_id: 'worker-1',
  namespace: 'shop',
  tags: ['worker'],
  version: '1',
  created_at: Date.now(),
  seq: 1,
  data: { host: '10.0.0.5', httpPort: 3000 },
})

discovery.close()
```

Nội dung message, `namespace`, `tags`, `node_id` và vòng đời (`status$`, `close()`) giống
[`@simple-discovery/udp`](../udp/README.md#nội-dung-một-message).

## Tuỳ chọn

| Tuỳ chọn | Biến môi trường | Mặc định | Ý nghĩa |
| --- | --- | --- | --- |
| `namespace`, `tags`, `node_id` | – | *(bắt buộc: `namespace`, `tags`)* | Như mọi transport. |
| `key` | `SIMPLE_DISCOVERY_KEY` | `'simple-discovery'` | Khoá ký gói. **Luôn đặt khoá riêng.** |
| `servers` | `SIMPLE_DISCOVERY_NATS_SERVERS` (phân cách bằng dấu phẩy) | `nats://127.0.0.1:4222` | Server NATS. Bỏ qua khi có `connection`. |
| `connectOptions` | – | – | Tuỳ chọn thêm khi kết nối (user/pass, token, NKey, TLS...). |
| `connection` | – | – | Dùng một `NatsConnection` có sẵn, xem bên dưới. |
| `subject` | – | `simple-discovery.<namespace>` | Subject để publish và subscribe. |
| `packetTtlMs` | – | `30000` | Bỏ gói lệch giờ quá mức này. |

`namespace` phải hợp lệ trong subject: không có khoảng trắng, `*`, `>`, và không có dấu chấm ở đầu,
cuối hay hai dấu chấm liền nhau. Sai thì constructor ném lỗi. Dấu chấm ở giữa được phép (`shop.eu`).

`SIMPLE_DISCOVERY_NATS_DEBUG=1` in lỗi và trạng thái kết nối ra stderr.

## Dùng kết nối có sẵn

```ts
import { connect } from '@nats-io/transport-node'
import { NatsDiscovery } from '@simple-discovery/nats'

const nc = await connect({ servers: process.env.NATS_URL })
const discovery = new NatsDiscovery<{ host: string }>({ connection: nc, namespace: 'shop', tags: ['worker'] })
```

`close()` chỉ huỷ subscription của discovery, **không** đóng `nc`. Nếu `nc` bị đóng, stream của
discovery báo lỗi và chuyển sang `closed`.

## Cách hoạt động

- Mỗi node publish lên `simple-discovery.<namespace>` và subscribe subject đó; node bỏ qua gói của chính
  mình.
- **Node vào sau biết ngay các node đã có** nhờ cờ `hello` ở lần broadcast đầu: mọi node đang có mặt
  trả lời đúng một lần.
- **Mất kết nối**: kết nối discovery tự mở thử lại vô hạn (`maxReconnectAttempts: -1`, ghi đè được qua
  `connectOptions`). Khi kết nối lại, discovery broadcast kèm `hello` để biết lại các node. Nếu kết nối
  đóng hẳn, stream báo lỗi.
- **Kết nối đầu tiên thất bại** (sai địa chỉ, sai quyền): stream báo lỗi, `broadcast()` reject.
- **Không có tin rời đi**: ứng dụng tự broadcast định kỳ và coi node im lặng quá lâu là đã rời đi.

## Chạy ở production

- Tài khoản NATS cần quyền publish và subscribe trên subject discovery.
- Core NATS không lưu message: node không kết nối lúc đó sẽ không nhận được. Cơ chế `hello` và
  heartbeat bù cho điều này.
- Đồng bộ giờ các máy (NTP): gói lệch quá `packetTtlMs` bị bỏ im lặng.
