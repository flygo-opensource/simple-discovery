import { BehaviorSubject, Observable, Subject, type Subscriber } from 'rxjs'
import { decodePacket, encodePacket } from './packet.js'
import {
    containsAllTags,
    hasDiscoveryEnvelope,
    type Discovery,
    type DiscoveryMessage,
    type DiscoveryOptions,
} from './types.js'

export type BrokerDiscoveryOptions = DiscoveryOptions & {
    /** HMAC-SHA256 key; every node of one system shares it. */
    key?: string
    /** Packets whose timestamp differs from this clock by more than this are dropped. */
    packetTtlMs?: number
}

export type DiscoveryStatus = 'not_ready' | 'ready' | 'closed'

const DEFAULT_PACKET_TTL_MS = 30_000

/**
 * Discovery over a publish/subscribe broker (Redis, NATS, AMQP...). Every node publishes signed
 * packets to one topic per namespace and receives everyone else's.
 *
 * A subclass only moves bytes: `connect()` subscribes and hands each payload to `receive()`,
 * `publish()` sends one, `disconnect()` releases the connection. Call `resync()` after the broker
 * connection comes back so the node re-announces itself and relearns who is there.
 */
export abstract class BrokerDiscovery<T> extends Observable<DiscoveryMessage<T>> implements Discovery<T> {
    readonly #events = new Subject<DiscoveryMessage<T>>()
    readonly #status = new BehaviorSubject<DiscoveryStatus>('not_ready')
    readonly #options: BrokerDiscoveryOptions
    readonly #key: string
    readonly #packetTtlMs: number
    readonly #debugEnv: string
    readonly #connected: Promise<void>
    #localMessage?: DiscoveryMessage<T>
    #helloPending = true

    readonly status$ = this.#status.asObservable()

    /** @param name transport name, used for the `SIMPLE_DISCOVERY_<NAME>_DEBUG` variable */
    constructor(name: string, options: BrokerDiscoveryOptions) {
        super((subscriber: Subscriber<DiscoveryMessage<T>>) => this.#events.subscribe(subscriber))
        this.#options = options
        this.#key = options.key ?? process.env.SIMPLE_DISCOVERY_KEY ?? 'simple-discovery'
        this.#packetTtlMs = options.packetTtlMs ?? DEFAULT_PACKET_TTL_MS
        this.#debugEnv = `SIMPLE_DISCOVERY_${name.toUpperCase()}_DEBUG`
        // Deferred so the subclass constructor has finished before connect() runs.
        this.#connected = Promise.resolve().then(() => this.#connect())
        this.#connected.catch(() => {})
    }

    protected abstract connect(): Promise<void>
    protected abstract publish(raw: Uint8Array): Promise<void>
    protected abstract disconnect(): Promise<void>

    get status(): DiscoveryStatus {
        return this.#status.value
    }

    async broadcast(message: DiscoveryMessage<T>): Promise<void> {
        await this.#connected
        if (this.#isClosed()) return
        const outbound = this.#validateOutbound(message)
        this.#localMessage = outbound
        const hello = this.#helloPending
        this.#helloPending = false
        try {
            await this.publish(encodePacket(outbound, this.#key, hello))
        } catch (error) {
            if (hello) this.#helloPending = true
            throw error
        }
    }

    close(): void {
        if (this.#isClosed()) return
        this.#status.next('closed')
        this.#events.complete()
        this.#status.complete()
        void this.#connected
            // A failed connect() already released what it opened.
            .then(() => this.disconnect(), () => {})
            .catch(error => this.debug(error))
    }

    /** Hand one payload from the broker to discovery. Invalid or foreign packets are ignored. */
    protected receive(raw: Uint8Array): void {
        if (this.#isClosed()) return
        const packet = decodePacket<T>(raw, this.#key, this.#packetTtlMs)
        if (!packet || !this.#validInbound(packet.message)) return
        this.#events.next(packet.message)
        if (packet.hello && this.#localMessage) {
            this.publish(encodePacket(this.#localMessage, this.#key)).catch(error => this.debug(error))
        }
    }

    /** The broker connection came back: announce again, asking the others to answer. */
    protected resync(): void {
        if (this.#isClosed()) return
        this.#helloPending = true
        if (this.#localMessage) this.broadcast(this.#localMessage).catch(error => this.debug(error))
    }

    /** The broker is gone for good: error the stream, mark closed and release the connection. */
    protected fail(error: unknown): void {
        if (this.#isClosed()) return
        this.#events.error(error)
        this.#status.next('closed')
        this.#status.complete()
        this.disconnect().catch(cause => this.debug(cause))
    }

    protected debug(error: unknown): void {
        if (process.env[this.#debugEnv]) console.error(error)
    }

    protected get closed(): boolean {
        return this.#isClosed()
    }

    async #connect(): Promise<void> {
        if (this.#isClosed()) return
        try {
            await this.connect()
        } catch (error) {
            this.fail(error)
            throw error
        }
        if (!this.#isClosed()) this.#status.next('ready')
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

    #isClosed() {
        return this.#status.value === 'closed'
    }
}
