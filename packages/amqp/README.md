# @simple-discovery/amqp

Discovery qua AMQP 0-9-1 (RabbitMQ): mọi process kết nối tới cùng một broker publish thông tin của mình
lên một fanout exchange và nhận thông tin của nhau qua một queue riêng. Hợp với hệ thống đã có
RabbitMQ.

Gói tin giống hệt các transport khác của simple-discovery: msgpack, ký HMAC-SHA256, chống phát lại.

## Cài đặt

```bash
bun add @simple-discovery/amqp rxjs
```

Chạy trên Node.js ≥ 18 và Bun. Dùng [`amqplib`](https://github.com/amqp-node/amqplib) v2.

## Ví dụ

```ts
import { AmqpDiscovery } from '@simple-discovery/amqp'

type Worker = { host: string; httpPort: number }

const discovery = new AmqpDiscovery<Worker>({
  url: 'amqp://user:pass@rabbitmq.internal',
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
| `url` | `SIMPLE_DISCOVERY_AMQP_URL` | `amqp://127.0.0.1` | Broker. Bỏ qua khi có `connection`. |
| `socketOptions` | – | – | Tuỳ chọn socket của amqplib (TLS...). |
| `recovery` | – | thử lại vô hạn, 100 ms → 30 s | Backoff khi kết nối lại (`initialDelay`, `maxDelay`, `maxRetries`...). |
| `connection` | – | – | Dùng một kết nối amqplib có sẵn, xem bên dưới. |
| `exchange` | – | `simple-discovery.<namespace>` | Fanout exchange. |
| `packetTtlMs` | – | `30000` | Bỏ gói lệch giờ quá mức này. |

`SIMPLE_DISCOVERY_AMQP_DEBUG=1` in lỗi kết nối ra stderr.

## Dùng kết nối có sẵn

```ts
import { connect } from 'amqplib'
import { AmqpDiscovery } from '@simple-discovery/amqp'

const connection = await connect(process.env.AMQP_URL!)
const discovery = new AmqpDiscovery<{ host: string }>({ connection, namespace: 'shop', tags: ['worker'] })
```

Discovery mở channel riêng trên kết nối này và `close()` chỉ đóng channel đó. Discovery **không** tự
kết nối lại một kết nối bạn truyền vào: nếu channel bị đóng, stream báo lỗi. Muốn tự kết nối lại, truyền
`url` thay vì `connection`.

## Cách hoạt động

- Discovery khai báo fanout exchange `simple-discovery.<namespace>` (non-durable) và một queue riêng
  cho mỗi node (server đặt tên, `exclusive`, `autoDelete`). Queue tự biến mất khi node ngắt kết nối.
- **Node vào sau biết ngay các node đã có** nhờ cờ `hello` ở lần broadcast đầu: mọi node đang có mặt
  trả lời đúng một lần.
- **Mất kết nối**: dùng cơ chế recovery của amqplib 2, thử lại vô hạn theo backoff. Sau mỗi lần kết nối
  lại, discovery khai báo lại exchange/queue rồi broadcast kèm `hello`. Nếu chỉ channel bị đóng mà kết
  nối còn sống, discovery đóng kết nối để amqplib dựng lại từ đầu.
- **Broker chưa lên khi khởi động**: discovery cứ thử lại và ở trạng thái `not_ready`; `broadcast()` đợi
  tới khi kết nối được. Đặt `recovery.maxRetries` nếu muốn dừng và báo lỗi sau một số lần.
- Trong lúc đang kết nối lại, `broadcast()` bị bỏ qua; lần broadcast sau khi có kết nối sẽ đi.
- **Không có tin rời đi**: ứng dụng tự broadcast định kỳ và coi node im lặng quá lâu là đã rời đi.

## Chạy ở production

- User RabbitMQ cần quyền `configure` (tạo exchange/queue), `write` và `read` trên vhost dùng cho
  discovery.
- Mọi process dùng chung `key`, `namespace` và cùng vhost.
- Đồng bộ giờ các máy (NTP): gói lệch quá `packetTtlMs` bị bỏ im lặng.
