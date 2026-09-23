import { createHmac, timingSafeEqual } from 'node:crypto'
import { createSocket, type RemoteInfo, type Socket } from 'node:dgram'
import { networkInterfaces } from 'node:os'
import { BehaviorSubject, Observable, Subject, type Subscriber, type Subscription } from 'rxjs'
import { pack, unpack } from 'msgpackr'
import {
    containsAllTags,
    hasDiscoveryEnvelope,
    type Discovery,
    type DiscoveryMessage,
    type DiscoveryOptions,
} from '@simple-discovery/core'

export type UdpDiscoveryOptions = DiscoveryOptions & {
    key?: string
    port?: number
    /** IPv4, 3 octet đầu của một dải /24, hoặc hostname (phân giải DNS mỗi lần gửi). */
    peers?: string[]
    /** Gửi tới nhóm multicast. Tắt khi mạng không chuyển multicast (VPN như NetBird, WireGuard). */
    multicast?: boolean
    multicastAddress?: string
    packetTtlMs?: number
    broadcastCopies?: number
}

export type UdpDiscoveryPacket<T> = {
    version: 1
    sender_id: string
    timestamp: number
    message: DiscoveryMessage<T>
    signature: string
}

type UnsignedPacket<T> = Omit<UdpDiscoveryPacket<T>, 'signature'>
type DecodedPacket<T> = { packet: UdpDiscoveryPacket<T>; unsigned: UnsignedPacket<T> }
/** Bản phát lại trong máy: gói gốc nguyên vẹn kèm địa chỉ đã gửi nó. Chỉ đi qua loopback. */
type RelayPacket = { relay: 1; from: string; raw: Uint8Array }
export type UdpDiscoveryStatus = 'not_ready' | 'ready' | 'closed'

const DEFAULT_PORT = 11001
const DEFAULT_MULTICAST_ADDRESS = '239.0.1.1'
const DEFAULT_PACKET_TTL_MS = 30_000
const LOOPBACK = '127.0.0.1'
/** Trần số chữ ký nhớ để lọc gói trùng, để gói rác không làm phình bộ nhớ. */
const MAX_SEEN_PACKETS = 10_000

export class UdpDiscovery<T> extends Observable<DiscoveryMessage<T>> implements Discovery<T> {
    readonly #events = new Subject<DiscoveryMessage<T>>()
    readonly #status = new BehaviorSubject<UdpDiscoveryStatus>('not_ready')
    readonly #externalSocket: Socket
    readonly #localSocket: Socket
    readonly #localAddresses = this.#readLocalAddresses()
    readonly #options: UdpDiscoveryOptions
    readonly #key: string
    readonly #port: number
    readonly #multicastAddress: string
    readonly #peers: string[]
    /** Địa chỉ của node đã gửi gói hợp lệ tới đây; gửi lại cho chúng dù không có trong `peers`. */
    readonly #learnedPeers = new Set<string>()
    readonly #packetTtlMs: number
    readonly #broadcastCopies: number
    readonly #replyMarkers = new Map<string, string>()
    /** Chữ ký gói đã thấy -> thời điểm hết hạn, theo thứ tự thêm vào (cũng là thứ tự hết hạn). */
    readonly #seenPackets = new Map<string, number>()
    #localMessage?: DiscoveryMessage<T>

    readonly status$ = this.#status.asObservable()

    constructor(options: UdpDiscoveryOptions) {
        super((subscriber: Subscriber<DiscoveryMessage<T>>) => this.#events.subscribe(subscriber))
        this.#options = options
        this.#key = options.key ?? process.env.SIMPLE_DISCOVERY_KEY ?? 'simple-discovery'
        this.#port = options.port ?? Number(process.env.SIMPLE_DISCOVERY_PORT || DEFAULT_PORT)
        this.#multicastAddress = options.multicastAddress
            ?? process.env.SIMPLE_DISCOVERY_UDP_MULTICAST_ADDRESS
            ?? DEFAULT_MULTICAST_ADDRESS
        this.#packetTtlMs = options.packetTtlMs ?? DEFAULT_PACKET_TTL_MS
        this.#broadcastCopies = Math.max(1, options.broadcastCopies
            ?? Number(process.env.SIMPLE_DISCOVERY_UDP_BROADCAST_COPIES || 3))
        const multicast = options.multicast ?? !/^(0|false|off|no)$/i.test(process.env.SIMPLE_DISCOVERY_UDP_MULTICAST ?? '')
        this.#peers = [...new Set([
            ...(multicast ? [this.#multicastAddress] : []),
            ...this.#expandPeers(options.peers ?? this.#envPeers()),
        ])]
        this.#externalSocket = this.createSocket()
        this.#localSocket = this.createSocket()
        this.#bindSockets()
    }

    protected createSocket(): Socket {
        return createSocket({ type: 'udp4', reuseAddr: true })
    }

    async broadcast(message: DiscoveryMessage<T>, targetIp?: string | string[]): Promise<void> {
        await this.#waitReady()
        if (this.#isClosed()) return

        const outbound = this.#validateOutbound(message)
        this.#localMessage = outbound
        const targets = targetIp ? [targetIp].flat() : [...new Set([...this.#peers, ...this.#learnedPeers])]
        const packet = this.#createPacket(outbound)
        const raw = pack(packet)
        for (let copy = 0; copy < this.#broadcastCopies; copy++) {
            await Promise.all(targets.map(ip => this.#send(this.#externalSocket, raw, ip)))
            // Gói unicast tới máy này chỉ tới MỘT socket (Linux: socket bind cuối cùng, có thể là
            // của chính process gửi, và nó bỏ gói của chính mình). Phát lại trong máy để mọi process
            // đều nhận được. Loopback không mất gói nên một bản là đủ.
            const local = !targetIp || targets.some(target => this.#isLocalAddress(target))
            if (copy === 0 && local) await this.#relayLocal(raw, LOOPBACK)
            if (copy + 1 < this.#broadcastCopies) await new Promise(resolve => setTimeout(resolve, 5))
        }
    }

    close(): void {
        if (this.#isClosed()) return
        this.#status.next('closed')
        this.#events.complete()
        this.#status.complete()
        this.#closeSocket(this.#externalSocket)
        this.#closeSocket(this.#localSocket)
    }

    #bindSockets() {
        // Hai socket cùng bind một cổng nên hệ điều hành chọn MỘT socket nhận gói unicast (macOS:
        // socket bind đầu tiên, Linux: socket bind cuối cùng); vì vậy cả hai xử lý gói như nhau.
        const onMessage = (raw: Buffer, remote: RemoteInfo) => this.#onMessage(raw, remote)
        const externalReady = this.#bindSocket(this.#externalSocket, onMessage)
        const localReady = this.#bindSocket(this.#localSocket, onMessage, () => {
            // Bản phát lại chỉ đi qua loopback nên không bao giờ ra mạng. Nếu đi qua interface LAN,
            // máy khác nhận nó như gói "từ xa", phát lại tiếp, và hai máy đẩy gói qua lại mãi.
            try { this.#localSocket.setMulticastInterface(LOOPBACK) } catch { }
        })

        void Promise.all([externalReady, localReady]).then(() => {
            if (!this.#isClosed()) this.#status.next('ready')
        })
    }

    #bindSocket(socket: Socket, onMessage: (raw: Buffer, remote: RemoteInfo) => void, onListening?: () => void) {
        socket.on('message', onMessage)
        socket.on('error', error => this.#onError(error))
        const ready = new Promise<void>(resolve => socket.once('listening', () => {
            try { socket.setMulticastTTL(1) } catch { }
            try { socket.setMulticastLoopback(true) } catch { }
            try { socket.addMembership(this.#multicastAddress, '0.0.0.0') } catch { }
            for (const address of this.#localAddresses) {
                if (!address.includes('.') || address === '0.0.0.0') continue
                try { socket.addMembership(this.#multicastAddress, address) } catch { }
            }
            onListening?.()
            resolve()
        }))
        socket.bind(this.#port, '0.0.0.0')
        return ready
    }

    #onMessage(raw: Buffer, remote: RemoteInfo) {
        if (this.#isClosed()) return
        const relayed = this.#decodeRelay(raw)
        if (relayed) {
            // Bản phát lại chỉ hợp lệ khi đến từ chính máy này, và không bao giờ phát lại lần nữa.
            if (this.#isLocalAddress(remote.address)) this.#receive(relayed.raw, relayed.from, false)
            return
        }
        // Gói từ máy khác, hoặc từ một process trong máy không phải discovery (không gửi từ cổng
        // discovery), chỉ tới một socket; phải phát lại thì các process khác trên máy mới thấy.
        // Gói của discovery trong máy thì bên gửi đã tự phát lại.
        const relay = !this.#isLocalAddress(remote.address) || remote.port !== this.#port
        this.#receive(raw, remote.address, relay)
    }

    #receive(raw: Buffer, from: string, relay: boolean) {
        const decoded = this.#decode(raw)
        if (!decoded) return
        const { packet, unsigned } = decoded
        if (Math.abs(Date.now() - packet.timestamp) > this.#packetTtlMs) return
        // Một gói tới nhiều lần: `broadcastCopies` bản, qua cả hai socket, qua multicast lẫn peers,
        // và qua bản phát lại của mọi process trong máy. Chỉ xử lý bản đầu tiên.
        if (!this.#firstSight(packet.signature)) return
        // Phát lại trước khi kiểm chữ ký: process khác trên máy có thể dùng key khác cùng cổng.
        if (relay) void this.#relayLocal(raw, from)
        if (!this.#signatureMatches(packet.signature, this.#sign(unsigned))) return
        this.#consume(packet, from)
    }

    #consume(packet: UdpDiscoveryPacket<T>, remoteHost: string) {
        if (!this.#validInbound(packet.message)) return
        // Bản phát lại mang địa chỉ máy gửi gốc, nên process không nhận trực tiếp gói unicast vẫn
        // học được peer. Biết thêm địa chỉ mới cũng là lý do để trả lời lại.
        const newPeer = !this.#isLocalAddress(remoteHost) && !this.#learnedPeers.has(remoteHost)
        if (newPeer) this.#learnedPeers.add(remoteHost)
        const message = {
            ...packet.message,
            remote_host: packet.message.remote_host || remoteHost,
        }
        this.#events.next(message)

        const marker = `${message.version}:${message.created_at}`
        if (this.#replyMarkers.get(message.node_id) !== marker || newPeer) {
            this.#replyMarkers.set(message.node_id, marker)
            if (this.#localMessage) void this.broadcast(this.#localMessage)
        }
    }

    #decodeRelay(raw: Buffer): { from: string; raw: Buffer } | undefined {
        try {
            const packet = unpack(raw) as Partial<RelayPacket>
            if (packet.relay !== 1 || typeof packet.from !== 'string' || !(packet.raw instanceof Uint8Array)) return
            return { from: packet.from, raw: Buffer.from(packet.raw) }
        } catch {
            return
        }
    }

    #firstSight(signature: string) {
        const now = Date.now()
        for (const [key, expiresAt] of this.#seenPackets) {
            if (expiresAt > now && this.#seenPackets.size < MAX_SEEN_PACKETS) break
            this.#seenPackets.delete(key)
        }
        if (this.#seenPackets.has(signature)) return false
        // Gói còn hợp lệ tới khi lệch quá `packetTtlMs` về cả hai phía.
        this.#seenPackets.set(signature, now + 2 * this.#packetTtlMs)
        return true
    }

    #decode(raw: Buffer): DecodedPacket<T> | undefined {
        try {
            const packet = unpack(raw) as Partial<UdpDiscoveryPacket<T>>
            if (packet.version !== 1) return
            if (typeof packet.sender_id !== 'string' || typeof packet.timestamp !== 'number') return
            if (typeof packet.signature !== 'string' || !hasDiscoveryEnvelope<T>(packet.message)) return
            if (packet.sender_id !== packet.message.node_id) return
            const { signature, ...unsigned } = packet as UdpDiscoveryPacket<T>
            return { packet: packet as UdpDiscoveryPacket<T>, unsigned }
        } catch {
            return
        }
    }

    #createPacket(message: DiscoveryMessage<T>): UdpDiscoveryPacket<T> {
        const unsigned: UnsignedPacket<T> = {
            version: 1,
            sender_id: message.node_id,
            timestamp: Date.now(),
            message,
        }
        return { ...unsigned, signature: this.#sign(unsigned) }
    }

    #validInbound(message: DiscoveryMessage<T>) {
        if (message.namespace !== this.#options.namespace) return false
        if (!containsAllTags(message.tags, this.#options.tags)) return false
        if (message.node_id === this.#localMessage?.node_id) return false
        return !this.#options.node_id || message.node_id !== this.#options.node_id
    }

    #validateOutbound(message: DiscoveryMessage<T>) {
        if (!hasDiscoveryEnvelope<T>(message)) throw new Error('Invalid discovery message envelope')
        if (message.namespace !== this.#options.namespace) {
            throw new Error(`Discovery message namespace must be ${this.#options.namespace}`)
        }
        if (!containsAllTags(message.tags, this.#options.tags)) {
            throw new Error(`Discovery message must contain tags: ${this.#options.tags.join(',')}`)
        }
        if (this.#options.node_id && message.node_id !== this.#options.node_id) {
            throw new Error(`Discovery message node_id must be ${this.#options.node_id}`)
        }
        return message
    }

    #sign(packet: UnsignedPacket<T>) {
        return createHmac('sha256', this.#key).update(pack(packet)).digest('hex')
    }

    #signatureMatches(actual: string, expected: string) {
        try {
            const actualBytes = Buffer.from(actual, 'hex')
            const expectedBytes = Buffer.from(expected, 'hex')
            return actualBytes.length === expectedBytes.length && timingSafeEqual(actualBytes, expectedBytes)
        } catch {
            return false
        }
    }

    /** Gửi gói cho mọi process discovery trên máy này, kèm địa chỉ đã gửi gói tới đây. */
    #relayLocal(raw: Buffer, from: string) {
        const relay: RelayPacket = { relay: 1, from, raw }
        return this.#send(this.#localSocket, pack(relay), this.#multicastAddress)
    }

    #send(socket: Socket, raw: Buffer, host: string): Promise<void> {
        return new Promise(resolve => {
            if (this.#isClosed()) return resolve()
            socket.send(raw, 0, raw.length, this.#port, host, error => {
                if (error) this.#onError(error)
                resolve()
            })
        })
    }

    #waitReady(): Promise<void> {
        const current = this.#status.value
        if (current === 'ready' || current === 'closed') return Promise.resolve()
        return new Promise(resolve => {
            let subscription: Subscription | undefined
            subscription = this.#status.subscribe(status => {
                if (status !== 'ready' && status !== 'closed') return
                subscription?.unsubscribe()
                resolve()
            })
        })
    }

    #readLocalAddresses() {
        return new Set([
            '127.0.0.1',
            '0.0.0.0',
            ...Object.values(networkInterfaces()).flatMap(items => items ?? []).map(item => item.address),
        ])
    }

    #isLocalAddress(address: string) {
        return this.#localAddresses.has(address)
    }

    #envPeers() {
        return (process.env.SIMPLE_DISCOVERY_UDP_WHITELIST_ADDRESS || '')
            .split(',')
            .map(value => value.trim())
            .filter(Boolean)
    }

    #expandPeers(peers: string[]) {
        return peers.flatMap(peer => {
            const value = peer.trim()
            if (!value) return []
            if (/^\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(value)) {
                return Array.from({ length: 254 }, (_, index) => `${value}.${index + 1}`)
            }
            // IPv4 đầy đủ hoặc hostname; dgram tự phân giải hostname mỗi lần gửi.
            return [value]
        })
    }

    #onError(error: unknown) {
        if (!this.#isClosed() && process.env.SIMPLE_DISCOVERY_UDP_DEBUG) console.error(error)
    }

    #closeSocket(socket: Socket) {
        try { socket.close() } catch { }
    }

    #isClosed() {
        return this.#status.value === 'closed'
    }
}
