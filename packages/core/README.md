# @simple-discovery/core

Phần dùng chung của các transport simple-discovery. Ứng dụng thường **không** cần cài trực tiếp: mỗi
transport (`@simple-discovery/redis`, `nats`, `amqp`) đã re-export mọi thứ trong gói này.

Gói gồm:

- **Contract** `Discovery<T>`: một `Observable<DiscoveryMessage<T>>` cộng `broadcast(message)` và
  `close()`. Tương thích cấu trúc với `DiscoveryTransporter<T>` của `@spider-mesh/core`.
- **Gói tin** `encodePacket` / `decodePacket`: msgpack, ký HMAC-SHA256, kiểm tra timestamp để chống phát
  lại. Cờ `hello` yêu cầu các node khác trả lời bằng message gần nhất của chúng.
- **`BrokerDiscovery<T>`**: lớp cơ sở cho transport chạy qua broker publish/subscribe.

## Viết một transport mới

Lớp con chỉ lo chuyển bytes; lọc namespace/tag, ký, chống phát lại và trả lời `hello` do lớp cơ sở làm.

```ts
import { BrokerDiscovery, type BrokerDiscoveryOptions } from '@simple-discovery/core'

export class MyDiscovery<T> extends BrokerDiscovery<T> {
  constructor(options: BrokerDiscoveryOptions & { url: string }) {
    super('my', options) // bật debug bằng SIMPLE_DISCOVERY_MY_DEBUG=1
  }

  // Kết nối và subscribe; mỗi payload nhận được thì gọi this.receive(bytes).
  // Khi kết nối trở lại sau khi mất, gọi this.resync(). Khi mất hẳn, gọi this.fail(error).
  protected async connect(): Promise<void> {}

  // Gửi một payload tới mọi node, kể cả chính mình (lớp cơ sở tự bỏ gói của mình).
  protected async publish(raw: Uint8Array): Promise<void> {}

  // Giải phóng những gì connect() đã mở.
  protected async disconnect(): Promise<void> {}
}
```

- `connect()` chạy ngay sau constructor. Nếu nó ném lỗi, stream báo lỗi, trạng thái chuyển sang
  `closed` và `broadcast()` reject.
- `status$`: `'not_ready'` → `'ready'` khi `connect()` xong → `'closed'` sau `close()` hoặc `fail()`.
- Bộ test hợp đồng dùng chung nằm ở [`tests/contract.ts`](tests/contract.ts); mỗi transport chạy nó với
  broker thật.
