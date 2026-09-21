import { BrokerDiscovery, type BrokerDiscoveryOptions } from '@simple-discovery/core'
import { connect, type NatsConnection, type NodeConnectionOptions, type Subscription } from '@nats-io/transport-node'

export type NatsDiscoveryOptions = BrokerDiscoveryOptions & {
    /**
     * Default: `SIMPLE_DISCOVERY_NATS_SERVERS` (comma separated), then `nats://127.0.0.1:4222`.
     * Ignored with `connection`.
     */
    servers?: string | string[]
    /** Extra options for the connection discovery opens itself (credentials, TLS...). */
    connectOptions?: NodeConnectionOptions
    /** Use an existing connection. Discovery never closes it. */
    connection?: NatsConnection
    /** Subject to publish and subscribe on. Default: `simple-discovery.<namespace>`. */
    subject?: string
}

const DEFAULT_SERVERS = 'nats://127.0.0.1:4222'

export class NatsDiscovery<T> extends BrokerDiscovery<T> {
    readonly #options: NatsDiscoveryOptions
    readonly #subject: string
    #connection?: NatsConnection
    #subscription?: Subscription

    constructor(options: NatsDiscoveryOptions) {
        // Validate before super(), which schedules the connection.
        const subject = options.subject ?? `simple-discovery.${options.namespace}`
        if (!/^[^\s*>.]+(\.[^\s*>.]+)*$/.test(subject)) {
            throw new Error(`Invalid NATS subject "${subject}"; set a namespace or subject without spaces, "*" or ">"`)
        }
        super('nats', options)
        this.#options = options
        this.#subject = subject
    }

    protected async connect(): Promise<void> {
        const connection = this.#options.connection ?? await connect({
            servers: this.#servers(),
            // Discovery should outlive broker restarts; the default gives up after ten tries.
            maxReconnectAttempts: -1,
            ...this.#options.connectOptions,
        })
        this.#connection = connection
        this.#subscription = connection.subscribe(this.#subject, {
            callback: (error, message) => error ? this.debug(error) : this.receive(message.data),
        })
        // Make sure the server has the subscription before discovery reports ready.
        await connection.flush()
        void this.#watch(connection)
    }

    protected async publish(raw: Uint8Array): Promise<void> {
        this.#connection?.publish(this.#subject, raw)
    }

    protected async disconnect(): Promise<void> {
        this.#subscription?.unsubscribe()
        if (!this.#options.connection) await this.#connection?.close()
    }

    async #watch(connection: NatsConnection) {
        void connection.closed().then(error => {
            if (!this.closed) this.fail(error ?? new Error('NATS connection closed'))
        })
        for await (const status of connection.status()) {
            if (status.type === 'reconnect') this.resync()
            else if (status.type === 'disconnect' || status.type === 'error') this.debug(status)
        }
    }

    #servers() {
        const servers = this.#options.servers ?? process.env.SIMPLE_DISCOVERY_NATS_SERVERS ?? DEFAULT_SERVERS
        return [servers].flat().flatMap(value => value.split(',')).map(value => value.trim()).filter(Boolean)
    }
}
