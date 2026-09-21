import { BrokerDiscovery, type BrokerDiscoveryOptions } from '@simple-discovery/core'
import {
    connect,
    type Channel,
    type ChannelModel,
    type Options,
    type RecoveringChannelModel,
    type RecoveryOptions,
    type SocketOptions,
} from 'amqplib'

export type AmqpDiscoveryOptions = BrokerDiscoveryOptions & {
    /** Default: `SIMPLE_DISCOVERY_AMQP_URL`, then `amqp://127.0.0.1`. Ignored with `connection`. */
    url?: string | Options.Connect
    /** Socket options for the connection discovery opens itself (TLS...). */
    socketOptions?: SocketOptions
    /** Reconnect backoff for the connection discovery opens itself. Default: retry forever. */
    recovery?: Omit<RecoveryOptions, 'setup'>
    /**
     * Use an existing connection. Discovery opens its own channel on it, never closes the
     * connection, and does not reconnect it: if that channel closes, the stream errors.
     */
    connection?: ChannelModel
    /** Fanout exchange. Default: `simple-discovery.<namespace>`. */
    exchange?: string
}

const DEFAULT_URL = 'amqp://127.0.0.1'

export class AmqpDiscovery<T> extends BrokerDiscovery<T> {
    readonly #options: AmqpDiscoveryOptions
    readonly #exchange: string
    #recovering?: RecoveringChannelModel
    #channel?: Channel
    #connectedOnce = false

    constructor(options: AmqpDiscoveryOptions) {
        super('amqp', options)
        this.#options = options
        this.#exchange = options.exchange ?? `simple-discovery.${options.namespace}`
    }

    protected async connect(): Promise<void> {
        if (this.#options.connection) {
            await this.#setup(this.#options.connection, error => this.fail(error))
            return
        }

        const url = this.#options.url ?? process.env.SIMPLE_DISCOVERY_AMQP_URL ?? DEFAULT_URL
        this.#recovering = await connect(url, {
            ...this.#options.socketOptions,
            recovery: {
                ...this.#options.recovery,
                // Runs after every successful (re)connection, before amqplib reports it.
                setup: async (model: ChannelModel) => {
                    // A channel can die while the connection lives; closing the connection makes
                    // amqplib reconnect and run this setup again.
                    await this.#setup(model, () => void model.close().catch(() => {}))
                    if (this.#connectedOnce) this.resync()
                    this.#connectedOnce = true
                },
            },
        })
        this.#recovering.on('error', error => this.debug(error))
        this.#recovering.on('disconnect', error => {
            this.#channel = undefined
            this.debug(error)
        })
        this.#recovering.on('connect-failed', error => this.debug(error))
        this.#recovering.on('reconnect-failed', error => this.fail(error))
    }

    protected async publish(raw: Uint8Array): Promise<void> {
        // While reconnecting there is no channel; the next broadcast goes out once it is back.
        this.#channel?.publish(this.#exchange, '', Buffer.isBuffer(raw) ? raw : Buffer.from(raw))
    }

    protected async disconnect(): Promise<void> {
        const channel = this.#channel
        this.#channel = undefined
        if (this.#recovering) {
            await this.#recovering.close()
        } else {
            await channel?.close().catch(() => {})
        }
    }

    async #setup(model: ChannelModel, onLost: (error: Error) => void) {
        const channel = await model.createChannel()
        channel.on('error', error => this.debug(error))
        channel.on('close', () => {
            if (this.#channel !== channel || this.closed) return
            this.#channel = undefined
            onLost(new Error(`AMQP channel for ${this.#exchange} closed`))
        })
        await channel.assertExchange(this.#exchange, 'fanout', { durable: false })
        // One private queue per node, removed by the broker when the node goes away.
        const { queue } = await channel.assertQueue('', { exclusive: true, autoDelete: true })
        await channel.bindQueue(queue, this.#exchange, '')
        await channel.consume(queue, message => {
            if (message) this.receive(message.content)
        }, { noAck: true })
        this.#channel = channel
    }
}
