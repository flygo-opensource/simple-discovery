import { expect, test } from 'bun:test'
import { connect } from 'amqplib'
import { filter, firstValueFrom, timeout } from 'rxjs'
import { brokerContract, tcpReachable, type Metadata } from '../../core/tests/contract.js'
import { AmqpDiscovery } from '../src/index.js'

const url = process.env.SIMPLE_DISCOVERY_AMQP_URL ?? 'amqp://127.0.0.1'
const available = await tcpReachable(url, 5672)

await brokerContract('@simple-discovery/amqp', options => new AmqpDiscovery<Metadata>({ ...options, url }), async () => available)

const message = (namespace: string, node_id: string, seq: number) =>
    ({ node_id, namespace, tags: ['t'], version: String(seq), created_at: Date.now(), seq, data: { value: seq } })

test.skipIf(!available)('reuses a connection it was given and leaves it open', async () => {
    const connection = await connect(url)
    const namespace = `given-${Date.now()}`
    const a = new AmqpDiscovery<Metadata>({ namespace, tags: ['t'], key: 'k', connection })
    const b = new AmqpDiscovery<Metadata>({ namespace, tags: ['t'], key: 'k', url })
    try {
        const seen = firstValueFrom(b.pipe(filter(event => event.node_id === 'a'), timeout(3_000)))
        await b.broadcast(message(namespace, 'b', 1))
        await a.broadcast(message(namespace, 'a', 1))
        expect((await seen).seq).toBe(1)
        a.close()
        await new Promise(resolve => setTimeout(resolve, 100))
        const channel = await connection.createChannel()
        await channel.close()
    } finally {
        a.close()
        b.close()
        await connection.close()
    }
})

/** The RabbitMQ container, from docker compose locally or a service container in CI. */
function rabbitContainer(): string | undefined {
    try {
        const result = Bun.spawnSync(['docker', 'ps', '-q', '--filter', 'ancestor=rabbitmq:4-alpine'])
        return result.stdout.toString().trim().split('\n')[0] || undefined
    } catch {
        return undefined
    }
}
const container = available ? rabbitContainer() : undefined

test.skipIf(!container)('after the broker drops its connection it reconnects and relearns its peers', async () => {
    const namespace = `reconnect-${Date.now()}`
    const recovery = { initialDelay: 50, maxDelay: 500 }
    const a = new AmqpDiscovery<Metadata>({ namespace, tags: ['t'], key: 'k', url, recovery })
    const b = new AmqpDiscovery<Metadata>({ namespace, tags: ['t'], key: 'k', url, recovery })
    try {
        const bLearnsA = firstValueFrom(b.pipe(filter(event => event.node_id === 'a'), timeout(3_000)))
        await b.broadcast(message(namespace, 'b', 1))
        await a.broadcast(message(namespace, 'a', 1))
        await bLearnsA

        const relearned = firstValueFrom(b.pipe(filter(event => event.node_id === 'a'), timeout(10_000)))
        Bun.spawnSync(['docker', 'exec', container!, 'rabbitmqctl', 'close_all_connections', 'test'])
        expect((await relearned).node_id).toBe('a')
        expect(b.status).toBe('ready')
    } finally {
        a.close()
        b.close()
    }
}, 15_000)
