import { expect, test } from 'bun:test'
import { Redis } from 'ioredis'
import { filter, firstValueFrom, timeout } from 'rxjs'
import { brokerContract, tcpReachable, type Metadata } from '../../core/tests/contract.js'
import { RedisDiscovery } from '../src/index.js'

const url = process.env.SIMPLE_DISCOVERY_REDIS_URL ?? 'redis://127.0.0.1:6379'
const available = await tcpReachable(url, 6379)

await brokerContract('@simple-discovery/redis', options => new RedisDiscovery<Metadata>({ ...options, url }), async () => available)

const message = (namespace: string, node_id: string, seq: number) =>
    ({ node_id, namespace, tags: ['t'], version: String(seq), created_at: Date.now(), seq, data: { value: seq } })

test.skipIf(!available)('reuses a client it was given and leaves it open', async () => {
    const client = new Redis(url)
    const namespace = `given-${Date.now()}`
    const a = new RedisDiscovery<Metadata>({ namespace, tags: ['t'], key: 'k', client })
    const b = new RedisDiscovery<Metadata>({ namespace, tags: ['t'], key: 'k', url })
    try {
        const seen = firstValueFrom(b.pipe(filter(event => event.node_id === 'a'), timeout(3_000)))
        await b.broadcast(message(namespace, 'b', 1))
        await a.broadcast(message(namespace, 'a', 1))
        expect((await seen).seq).toBe(1)
        a.close()
        await new Promise(resolve => setTimeout(resolve, 50))
        expect(await client.ping()).toBe('PONG')
    } finally {
        a.close()
        b.close()
        client.disconnect()
    }
})

test.skipIf(!available)('after its connection is killed it reconnects and relearns its peers', async () => {
    const admin = new Redis(url)
    const namespace = `reconnect-${Date.now()}`
    const a = new RedisDiscovery<Metadata>({ namespace, tags: ['t'], key: 'k', url })
    const b = new RedisDiscovery<Metadata>({ namespace, tags: ['t'], key: 'k', url })
    try {
        const bLearnsA = firstValueFrom(b.pipe(filter(event => event.node_id === 'a'), timeout(3_000)))
        await b.broadcast(message(namespace, 'b', 1))
        await a.broadcast(message(namespace, 'a', 1))
        await bLearnsA

        // Drop every pub/sub connection; ioredis reconnects and discovery says hello again.
        const relearned = firstValueFrom(b.pipe(filter(event => event.node_id === 'a'), timeout(5_000)))
        await admin.client('KILL', 'TYPE', 'pubsub')
        expect((await relearned).node_id).toBe('a')
    } finally {
        a.close()
        b.close()
        admin.disconnect()
    }
})
