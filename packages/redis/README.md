# @simple-discovery/redis

Discovery qua Redis pub/sub: mọi process kết nối tới cùng một Redis công bố thông tin của mình lên một
channel và nhận thông tin của nhau. Hợp với hệ thống đã có Redis, chạy trên nhiều mạng hoặc trong
Kubernetes, nơi multicast UDP không dùng được.

Gói tin giống hệt các transport khác của simple-discovery: msgpack, ký HMAC-SHA256, chống phát lại.
Nhiều hệ thống dùng chung một Redis vẫn tách biệt nhờ `namespace`, `tags` và `key`.

## Cài đặt

```bash
bun add @simple-discovery/redis rxjs
```

Chạy trên Node.js ≥ 20 và Bun. Dùng [`ioredis`](https://github.com/redis/ioredis).

## Ví dụ

```ts
import { RedisDiscovery } from '@simple-discovery/redis'

type Worker = { host: string; httpPort: number }

const discovery = new RedisDiscovery<Worker>({
  url: 'redis://redis.internal:6379',
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
| `url` | `SIMPLE_DISCOVERY_REDIS_URL` | `redis://127.0.0.1:6379` | Redis để kết nối. Bỏ qua khi có `client`. |
| `redisOptions` | – | – | Tuỳ chọn thêm cho ioredis (mật khẩu, TLS...). |
| `client` | – | – | Dùng một client ioredis có sẵn, xem bên dưới. |
| `channel` | – | `simple-discovery:<namespace>` | Channel pub/sub. |
| `packetTtlMs` | – | `30000` | Bỏ gói lệch giờ quá mức này. |

`SIMPLE_DISCOVERY_REDIS_DEBUG=1` in lỗi kết nối ra stderr.

## Dùng client có sẵn

```ts
import { Redis } from 'ioredis'
import { RedisDiscovery } from '@simple-discovery/redis'

const redis = new Redis(process.env.REDIS_URL!)
const discovery = new RedisDiscovery<{ host: string }>({ client: redis, namespace: 'shop', tags: ['worker'] })
```

Discovery publish qua `redis` và subscribe trên một bản `redis.duplicate()` (kết nối đang subscribe
không chạy được lệnh khác). `close()` chỉ đóng bản duplicate, **không** đóng client bạn truyền vào.

## Cách hoạt động

- Mỗi node publish lên channel `simple-discovery:<namespace>` và subscribe channel đó; node bỏ qua gói
  của chính mình.
- **Node vào sau biết ngay các node đã có**: lần broadcast đầu tiên mang cờ `hello`, mọi node đang có
  mặt trả lời một lần bằng message gần nhất của mình. Các lần broadcast sau không kích hoạt trả lời,
  nên lưu lượng không tăng theo bình phương số node.
- **Kết nối đầu tiên thất bại** (sai địa chỉ, Redis chưa lên): stream báo lỗi, `broadcast()` reject.
  Ứng dụng tự khởi động lại hoặc tạo discovery mới.
- **Mất kết nối giữa chừng**: ioredis tự kết nối lại. Khi kết nối trở lại, discovery subscribe lại rồi broadcast
  kèm `hello`, nên biết lại những node vào trong lúc nó mất kết nối.
- **Không có tin rời đi**: như UDP, ứng dụng tự broadcast định kỳ và coi node im lặng quá lâu là đã
  rời đi. Xem mẫu heartbeat trong [hướng dẫn cho agent của udp](../udp/AGENT_GUIDE.md#3-mẫu-tích-hợp-chuẩn-copy-rồi-sửa-node).

## Chạy ở production

- Mọi process của một hệ thống dùng chung `key`, `namespace` và cùng một Redis (hoặc cùng một cluster
  có pub/sub lan toả, như Redis Cluster).
- Pub/sub của Redis không lưu message: node không kết nối lúc đó sẽ không nhận được. Cơ chế `hello` và
  heartbeat bù cho điều này.
- Đồng bộ giờ các máy (NTP): gói lệch quá `packetTtlMs` bị bỏ im lặng.
