import { BrokerDiscovery, type BrokerDiscoveryOptions } from '@simple-discovery/core'
import { Redis, type RedisOptions } from 'ioredis'

export type RedisDiscoveryOptions = BrokerDiscoveryOptions & {
    /** Default: `SIMPLE_DISCOVERY_REDIS_URL`, then `redis://127.0.0.1:6379`. Ignored with `client`. */
    url?: string
    /** Extra ioredis options for the connections discovery opens itself. */
    redisOptions?: RedisOptions
    /**
     * Publish through an existing client. Discovery subscribes on a duplicate of it (a subscribed
     * Redis connection cannot run other commands) and never closes the client you pass.
     */
    client?: Redis
    /** Pub/sub channel. Default: `simple-discovery:<namespace>`. */
    channel?: string
}

const DEFAULT_URL = 'redis://127.0.0.1:6379'

export class RedisDiscovery<T> extends BrokerDiscovery<T> {
    readonly #channel: string
    readonly #publisher: Redis
    readonly #subscriber: Redis
    readonly #ownsPublisher: boolean
    #subscribed = false

    constructor(options: RedisDiscoveryOptions) {
        super('redis', options)
        this.#channel = options.channel ?? `simple-discovery:${options.namespace}`
        this.#ownsPublisher = !options.client
        this.#publisher = options.client ?? new Redis(
            options.url ?? process.env.SIMPLE_DISCOVERY_REDIS_URL ?? DEFAULT_URL,
            { lazyConnect: true, ...options.redisOptions },
        )
        this.#subscriber = this.#publisher.duplicate({ lazyConnect: true })
    }

    protected async connect(): Promise<void> {
        this.#subscriber.on('error', error => this.debug(error))
        if (this.#ownsPublisher) this.#publisher.on('error', error => this.debug(error))
        this.#subscriber.on('messageBuffer', (channel: Buffer, message: Buffer) => {
            if (channel.toString() === this.#channel) this.receive(message)
        })
        // ioredis reconnects on its own; once subscribed again, ask the others to answer.
        this.#subscriber.on('ready', () => {
            if (!this.#subscribed || this.closed) return
            this.#subscriber.subscribe(this.#channel)
                .then(() => this.resync())
                .catch(error => this.debug(error))
        })

        await this.#subscriber.connect()
        await this.#subscriber.subscribe(this.#channel)
        this.#subscribed = true
        if (this.#publisher.status === 'wait') await this.#publisher.connect()
    }

    protected async publish(raw: Uint8Array): Promise<void> {
        await this.#publisher.publish(this.#channel, Buffer.isBuffer(raw) ? raw : Buffer.from(raw))
    }

    protected async disconnect(): Promise<void> {
        this.#subscriber.disconnect()
        if (this.#ownsPublisher) this.#publisher.disconnect()
    }
}
