import { afterEach, describe, expect, test } from 'bun:test'
import { EventEmitter } from 'node:events'
import type { RemoteInfo, Socket } from 'node:dgram'
import { UdpDiscovery, type DiscoveryMessage } from '../src/index.js'

/**
 * Chế độ peers chỉ có ý nghĩa giữa NHIỀU máy: gói unicast từ máy khác tới được một process, rồi
 * process đó phát lại cho các process cùng máy qua `#relayLocal`. Trên một máy thật mọi gói đều
 * có IP nguồn nội bộ nên đường này không bao giờ chạy, vì vậy test dựng một LAN giả qua hook
 * `createSocket()`:
 *
 * - mỗi host có IP riêng trong dải TEST-NET-2 (198.51.100.0/24), không trùng interface thật nào;
 * - multicast KHÔNG đi qua giữa các host (router/Wi-Fi chặn multicast — lý do phải dùng peers);
 * - unicast tới IP của host khác thì tới được, IP nguồn là IP host gửi (nên bị coi là "từ xa");
 * - nhiều socket cùng bind một cổng thì một gói unicast chỉ tới MỘT socket. Socket nào nhận là
 *   tuỳ hệ điều hành, nên mọi kịch bản chạy với cả hai chính sách `first-bound` và `last-bound`.
 */

type Delivery = 'first-bound' | 'last-bound'
type Metadata = { host: string }

const LOOPBACK = '127.0.0.1'
const isMulticast = (address: string) => {
    const first = Number(address.split('.')[0])
    return first >= 224 && first <= 239
}

class FakeLan {
    readonly hosts = new Map<string, FakeHost>()
    /** DNS giả: hostname -> IP. */
    readonly names = new Map<string, string>()
    constructor(readonly delivery: Delivery) {}

    host(ip: string, hostname?: string) {
        const host = new FakeHost(this, ip)
        this.hosts.set(ip, host)
        if (hostname) this.names.set(hostname, ip)
        return host
    }

    route(from: FakeSocket, data: Buffer, port: number, destination: string) {
        destination = this.names.get(destination) ?? destination
        const source = from.host
        const deliver = (target: FakeSocket, address: string) => {
            const info: RemoteInfo = { address, family: 'IPv4', port: from.port ?? 0, size: data.length }
            setTimeout(() => {
                if (!target.closed) target.emit('message', Buffer.from(data), info)
            }, 0)
        }

        // Multicast chỉ tới các socket cùng host đã join group (loopback bật, kể cả chính mình).
        if (isMulticast(destination)) {
            for (const socket of source.boundTo(port)) {
                if (socket.groups.has(destination)) deliver(socket, LOOPBACK)
            }
            return
        }

        // Unicast nội bộ trên cùng host: IP nguồn là địa chỉ nội bộ.
        if (destination === LOOPBACK || destination === source.ip) {
            const target = source.pickUnicastTarget(port)
            if (target) deliver(target, LOOPBACK)
            return
        }

        // Unicast sang host khác trong LAN: IP nguồn là IP host gửi, tức là "từ xa".
        const target = this.hosts.get(destination)?.pickUnicastTarget(port)
        if (target) deliver(target, source.ip)
    }
}

class FakeHost {
    readonly sockets: FakeSocket[] = []
    constructor(readonly lan: FakeLan, readonly ip: string) {}

    boundTo(port: number) {
        return this.sockets.filter(socket => socket.port === port && !socket.closed)
    }

    pickUnicastTarget(port: number) {
        const bound = this.boundTo(port)
        return this.lan.delivery === 'first-bound' ? bound[0] : bound[bound.length - 1]
    }
}

class FakeSocket extends EventEmitter {
    port?: number
    closed = false
    readonly groups = new Set<string>()
    readonly sentTo: string[] = []

    constructor(readonly host: FakeHost) {
        super()
    }

    bind(port: number) {
        this.port = port
        this.host.sockets.push(this)
        setTimeout(() => this.emit('listening'), 0)
        return this
    }

    addMembership(group: string) {
        this.groups.add(group)
    }

    setMulticastTTL() {}
    setMulticastLoopback() {}
    setMulticastInterface() {}

    send(raw: Buffer, offset: number, length: number, port: number, destination: string, callback?: (error: Error | null) => void) {
        const data = Buffer.from(raw.subarray(offset, offset + length))
        this.sentTo.push(destination)
        setTimeout(() => {
            if (!this.closed) this.host.lan.route(this, data, port, destination)
            callback?.(null)
        }, 0)
    }

    close() {
        this.closed = true
    }
}

// `createSocket()` chạy ngay trong constructor của UdpDiscovery, nên host hiện tại phải được đặt
// trước khi `new`.
let constructingOn: FakeHost | undefined

class LanDiscovery extends UdpDiscovery<Metadata> {
    protected override createSocket(): Socket {
        if (!constructingOn) throw new Error('LanDiscovery must be created through spawn()')
        return new FakeSocket(constructingOn) as unknown as Socket
    }
}

const PORT = 11_001
const opened: LanDiscovery[] = []

afterEach(() => {
    for (const discovery of opened.splice(0)) discovery.close()
})

function spawn(host: FakeHost, node_id: string, peers: string[] = [], multicast?: boolean) {
    constructingOn = host
    try {
        const discovery = new LanDiscovery({
            namespace: 'lan-test',
            tags: ['simple-discovery', 'lan'],
            node_id,
            key: 'lan-key',
            port: PORT,
            multicastAddress: '239.0.1.9',
            peers,
            multicast,
        })
        opened.push(discovery)
        return discovery
    } finally {
        constructingOn = undefined
    }
}

/** Gom mọi message một discovery nhận được, theo node_id người gửi (giữ mọi bản sao). */
function record(discovery: LanDiscovery) {
    const seen = new Map<string, DiscoveryMessage<Metadata>[]>()
    discovery.subscribe(message => seen.set(message.node_id, [...(seen.get(message.node_id) ?? []), message]))
    return seen
}

const remoteHosts = (copies: DiscoveryMessage<Metadata>[] | undefined) => (copies ?? []).map(copy => copy.remote_host)

function announce(discovery: LanDiscovery, node_id: string, host: FakeHost) {
    return discovery.broadcast({
        node_id,
        namespace: 'lan-test',
        tags: ['simple-discovery', 'lan'],
        version: '1',
        created_at: Date.now(),
        seq: 1,
        data: { host: host.ip },
    })
}

async function waitUntil(check: () => boolean, timeoutMs = 1_000) {
    const deadline = Date.now() + timeoutMs
    while (!check()) {
        if (Date.now() > deadline) return false
        await Bun.sleep(10)
    }
    return true
}

describe.each<Delivery>(['first-bound', 'last-bound'])('@simple-discovery/udp peers across a LAN without multicast (%s)', delivery => {
    test('control: hosts without peers never see each other', async () => {
        // Chứng minh LAN giả thật sự cách ly multicast giữa các host. Nếu ca này hỏng thì mọi ca
        // "peers hoạt động" bên dưới không còn chứng minh được gì.
        const lan = new FakeLan(delivery)
        const hostA = lan.host('198.51.100.10')
        const hostB = lan.host('198.51.100.20')
        const a = spawn(hostA, 'a')
        const b = spawn(hostB, 'b')
        const seenByA = record(a)
        const seenByB = record(b)

        await announce(a, 'a', hostA)
        await announce(b, 'b', hostB)

        expect(await waitUntil(() => seenByA.has('b') || seenByB.has('a'), 300)).toBe(false)
    })

    test('explicit peers discover each other in both directions', async () => {
        const lan = new FakeLan(delivery)
        const hostA = lan.host('198.51.100.10')
        const hostB = lan.host('198.51.100.20')
        const a = spawn(hostA, 'a', [hostB.ip])
        const b = spawn(hostB, 'b', [hostA.ip])
        const seenByA = record(a)
        const seenByB = record(b)

        await announce(a, 'a', hostA)
        await announce(b, 'b', hostB)

        expect(await waitUntil(() => seenByA.has('b') && seenByB.has('a'))).toBe(true)
        // Có ít nhất một bản mang IP host gửi, tức là gói đã đi qua đường peer. Các bản phát lại
        // qua multicast nội bộ thì mang `127.0.0.1`; `remote_host` không được ai đọc nên vô hại.
        expect(remoteHosts(seenByA.get('b'))).toContain(hostB.ip)
        expect(remoteHosts(seenByB.get('a'))).toContain(hostA.ip)
    })

    test('a message from a peer reaches every process on the receiving host', async () => {
        // Gói unicast chỉ tới một socket trên host B; process còn lại chỉ nhận được nếu process
        // kia phát lại (`#relayLocal`) qua multicast nội bộ.
        const lan = new FakeLan(delivery)
        const hostA = lan.host('198.51.100.10')
        const hostB = lan.host('198.51.100.20')
        const a = spawn(hostA, 'a', [hostB.ip])
        const b1 = spawn(hostB, 'b1')
        const b2 = spawn(hostB, 'b2')
        const seenByB1 = record(b1)
        const seenByB2 = record(b2)

        await announce(a, 'a', hostA)

        expect(await waitUntil(() => seenByB1.has('a') && seenByB2.has('a'))).toBe(true)

        // Bản phát lại mang IP nguồn nội bộ nên không được phát lại thêm lần nào: số bản sao phải
        // ngừng tăng. Nếu tăng mãi là có vòng lặp phát lại.
        const copies = () => (seenByB1.get('a')?.length ?? 0) + (seenByB2.get('a')?.length ?? 0)
        await Bun.sleep(200)
        const settled = copies()
        await Bun.sleep(300)
        expect(copies()).toBe(settled)
    })

    test('a /24 prefix peer expands to every host in that subnet', async () => {
        const lan = new FakeLan(delivery)
        const hostA = lan.host('198.51.100.10')
        const hostB = lan.host('198.51.100.77')
        const a = spawn(hostA, 'a', ['198.51.100'])
        const b = spawn(hostB, 'b')
        const seenByB = record(b)

        await announce(a, 'a', hostA)

        expect(await waitUntil(() => seenByB.has('a'))).toBe(true)
    })

    test('a late joiner learns an existing peer from its reply, without waiting for a heartbeat', async () => {
        const lan = new FakeLan(delivery)
        const hostA = lan.host('198.51.100.10')
        const hostB = lan.host('198.51.100.20')
        const existing = spawn(hostB, 'existing', [hostA.ip])
        await announce(existing, 'existing', hostB)

        const newcomer = spawn(hostA, 'newcomer', [hostB.ip])
        const seenByNewcomer = record(newcomer)
        await announce(newcomer, 'newcomer', hostA)

        // `existing` không broadcast lại lần nào nữa; newcomer chỉ biết nó qua gói đáp lại.
        expect(await waitUntil(() => seenByNewcomer.has('existing'))).toBe(true)
    })

    test('peers can be hostnames, including three-label names such as a VPN domain', async () => {
        // Trước đây mọi chuỗi 3 phần bị coi là dải /24, nên `b.netbird.cloud` thành 254 địa chỉ rác.
        const lan = new FakeLan(delivery)
        const hostA = lan.host('198.51.100.10', 'a.netbird.cloud')
        const hostB = lan.host('198.51.100.20', 'b.netbird.cloud')
        const a = spawn(hostA, 'a', ['b.netbird.cloud'], false)
        const b = spawn(hostB, 'b', ['a.netbird.cloud'], false)
        const seenByA = record(a)
        const seenByB = record(b)

        await announce(a, 'a', hostA)
        await announce(b, 'b', hostB)

        expect(await waitUntil(() => seenByA.has('b') && seenByB.has('a'))).toBe(true)
        expect(hostA.sockets[0].sentTo).toContain('b.netbird.cloud')
    })

    test('multicast: false sends to peers only, never to the multicast group across hosts', async () => {
        const lan = new FakeLan(delivery)
        const hostA = lan.host('198.51.100.10')
        const hostB = lan.host('198.51.100.20')
        const quiet = spawn(hostA, 'quiet', [hostB.ip], false)
        const loud = spawn(hostB, 'loud', [hostA.ip])

        await announce(quiet, 'quiet', hostA)
        await announce(loud, 'loud', hostB)

        // Socket bind đầu tiên là socket gửi ra ngoài; socket còn lại chỉ phát lại trong máy.
        expect(hostA.sockets[0].sentTo.some(isMulticast)).toBe(false)
        expect(hostA.sockets[0].sentTo).toContain(hostB.ip)
        expect(hostB.sockets[0].sentTo.some(isMulticast)).toBe(true)
    })

    test('a node listed on one side only learns the sender and keeps it updated', async () => {
        // Chỉ node mới cần biết địa chỉ node cũ; node cũ không phải sửa cấu hình.
        const lan = new FakeLan(delivery)
        const hostA = lan.host('198.51.100.10')
        const hostB = lan.host('198.51.100.20')
        const oldNode = spawn(hostB, 'old', [], false)
        const seenByOld = record(oldNode)
        await announce(oldNode, 'old', hostB)

        const newNode = spawn(hostA, 'new', [hostB.ip], false)
        const seenByNew = record(newNode)
        await announce(newNode, 'new', hostA)

        expect(await waitUntil(() => seenByOld.has('new') && seenByNew.has('old'))).toBe(true)

        // Một thay đổi sau đó của node cũ vẫn tới node mới dù node mới không có trong `peers`.
        await oldNode.broadcast({
            node_id: 'old', namespace: 'lan-test', tags: ['simple-discovery', 'lan'],
            version: '2', created_at: Date.now(), seq: 2, data: { host: hostB.ip },
        })
        expect(await waitUntil(() => (seenByNew.get('old') ?? []).some(copy => copy.seq === 2))).toBe(true)
    })

    test('a targeted broadcast to this host reaches the other processes on it', async () => {
        // Linux giao gói unicast cho socket bind cuối cùng, có thể là socket của chính bên gửi.
        const lan = new FakeLan(delivery)
        const host = lan.host('198.51.100.10')
        const receiver = spawn(host, 'receiver')
        const sender = spawn(host, 'sender')
        const seen = record(receiver)

        await sender.broadcast({
            node_id: 'sender', namespace: 'lan-test', tags: ['simple-discovery', 'lan'],
            version: '1', created_at: Date.now(), seq: 1, data: { host: host.ip },
        }, '127.0.0.1')

        expect(await waitUntil(() => seen.has('sender'))).toBe(true)
    })
})
