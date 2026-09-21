# E2E chế độ peers giữa hai máy thật

Dành cho người phát triển `@ohayo/udp`; thư mục này không nằm trong gói publish.

Peers chỉ có ý nghĩa giữa nhiều máy, nên có hai lớp test:

- `bun test`: gồm `tests/peers-lan.test.ts`, dựng một LAN giả và chạy với cả hai kiểu hệ điều hành
  giao gói unicast (macOS: socket bind đầu tiên; Linux: socket bind cuối cùng).
- `bun run e2e:lan-peers`: chạy thật giữa máy này và một máy Linux **cùng LAN vật lý**. SSH chỉ dùng để
  điều khiển; gói discovery đi qua địa chỉ LAN. Script tự mang bun lên thư mục tạm của máy kia và tự dọn.

```bash
REMOTE=<user>@<linux-host> REMOTE_LAN_IP=<ip LAN của máy Linux> LOCAL_LAN_IP=<ip LAN của máy này> \
  bun run e2e:lan-peers
```

Kịch bản: 1 agent trên máy này, 2 agent trên máy Linux, hai máy dùng hai multicast group khác nhau nên
chỉ thấy nhau được qua `peers`. Ca đối chứng (không peers) phải cho kết quả không thấy nhau.
