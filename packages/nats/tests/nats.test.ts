import { expect, test } from 'bun:test'
import { connect } from '@nats-io/transport-node'
import { filter, firstValueFrom, timeout } from 'rxjs'
import { brokerContract, tcpReachable, type Metadata } from '../../core/tests/contract.js'
import { NatsDiscovery } from '../src/index.js'

const servers = process.env.SIMPLE_DISCOVERY_NATS_SERVERS ?? 'nats://127.0.0.1:4222'
const available = await tcpReachable(servers.split(',')[0]!, 4222)

await brokerContract('@simple-discovery/nats', options => new NatsDiscovery<Metadata>({ ...options, servers }), async () => available)

const message = (namespace: string, node_id: string, seq: number) =>
    ({ node_id, namespace, tags: ['t'], version: String(seq), created_at: Date.now(), seq, data: { value: seq } })

test('rejects a namespace that is not a valid subject', () => {
    expect(() => new NatsDiscovery({ namespace: 'has space', tags: [] })).toThrow('Invalid NATS subject')
    expect(() => new NatsDiscovery({ namespace: 'shop.*', tags: [] })).toThrow('Invalid NATS subject')
    expect(() => new NatsDiscovery({ namespace: 'shop.eu', tags: [] }).close()).not.toThrow()
})

test.skipIf(!available)('reuses a connection it was given and leaves it open', async () => {
    const connection = await connect({ servers })
    const namespace = `given-${Date.now()}`
    const a = new NatsDiscovery<Metadata>({ namespace, tags: ['t'], key: 'k', connection })
    const b = new NatsDiscovery<Metadata>({ namespace, tags: ['t'], key: 'k', servers })
    try {
        const seen = firstValueFrom(b.pipe(filter(event => event.node_id === 'a'), timeout(3_000)))
        await b.broadcast(message(namespace, 'b', 1))
        await a.broadcast(message(namespace, 'a', 1))
        expect((await seen).seq).toBe(1)
        a.close()
        await new Promise(resolve => setTimeout(resolve, 50))
        expect(connection.isClosed()).toBe(false)
    } finally {
        a.close()
        b.close()
        await connection.close()
    }
})

test.skipIf(!available)('after a reconnect it says hello again and relearns its peers', async () => {
    const connection = await connect({ servers, maxReconnectAttempts: -1, reconnectTimeWait: 100 })
    const namespace = `reconnect-${Date.now()}`
    const a = new NatsDiscovery<Metadata>({ namespace, tags: ['t'], key: 'k', servers })
    const b = new NatsDiscovery<Metadata>({ namespace, tags: ['t'], key: 'k', connection })
    try {
        const bLearnsA = firstValueFrom(b.pipe(filter(event => event.node_id === 'a'), timeout(3_000)))
        await b.broadcast(message(namespace, 'b', 1))
        await a.broadcast(message(namespace, 'a', 1))
        await bLearnsA

        const relearned = firstValueFrom(b.pipe(filter(event => event.node_id === 'a'), timeout(5_000)))
        connection.reconnect()
        expect((await relearned).node_id).toBe('a')
    } finally {
        a.close()
        b.close()
        await connection.close()
    }
})
