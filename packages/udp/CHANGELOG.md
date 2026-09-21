# Changelog

## 3.0.0 — stable generic discovery contract

- Thêm `multicast: false` (`OHAYO_UDP_MULTICAST=off`) để chỉ gửi tới `peers`, cho mạng không chuyển
  multicast như VPN NetBird/WireGuard. Đã kiểm chứng giữa macOS và Linux qua NetBird, peers là hostname.
- `peers` nhận hostname. Trước đây mọi chuỗi có 3 phần bị coi là dải `/24`, nên tên như
  `worker.netbird.cloud` bị bung thành 254 địa chỉ sai; tên có 1, 2 hay 5 phần thì bị bỏ.
- Sửa: `broadcast(message, targetIp)` tới chính máy này (ví dụ `127.0.0.1`) không tới process khác
  trên máy khi chạy Linux. Linux giao gói unicast cho socket bind cuối cùng, có thể là socket của
  chính bên gửi, và gói bị bỏ vì là của chính mình. Giờ gửi có đích là địa chỉ local thì cũng phát
  lại trong máy. Lỗi lộ ra khi CI lần đầu chạy trên Linux.
- Discovery nhớ địa chỉ của node đã gửi message hợp lệ và gửi các broadcast sau tới đó, nên `peers`
  chỉ cần khai một chiều.

- Sửa: gói từ peer không được phát lại cho các process khác trên cùng máy nếu hệ điều hành giao
  nó cho `#localSocket` thay vì `#externalSocket`. Hai socket cùng bind một cổng nên OS chỉ giao gói
  unicast cho một socket: macOS chọn socket bind đầu tiên, **Linux chọn socket bind cuối cùng**. Vì
  vậy trên Linux, process thứ hai trên máy không bao giờ thấy peer ở máy khác. Đã kiểm chứng trên LAN
  thật giữa macOS và Linux, 3/3 lần. Giờ hai socket dùng chung `#shouldRelay()`.
- Thêm `tests/peers-lan.test.ts` (LAN giả qua `createSocket()`, chạy trong CI) và
  `e2e/lan-peers/run.sh` (hai máy thật cùng LAN, chạy tay: `bun run e2e:lan-peers`).

### Breaking
- First major release of the signed generic `DiscoveryMessage<T>` UDP transport.
- Consumers migrating from Spider Mesh TCP's legacy `UdpDiscovery` must import `UdpDiscovery` from
  `@simple-discovery/udp` and provide `namespace`, required `tags`, and an optional local `node_id`.

### Added
- HMAC-signed msgpack packets, anti-replay TTL, namespace/tag filtering, explicit peers, local relay,
  redundant broadcasts, automatic one-shot reply for late peers, and observable lifecycle status.
