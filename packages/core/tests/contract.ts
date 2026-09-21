import { afterEach, describe, expect, test } from 'bun:test'
import { filter, firstValueFrom, timeout, toArray, takeUntil, timer } from 'rxjs'
import type { BrokerDiscovery, BrokerDiscoveryOptions, DiscoveryMessage } from '../src/index.js'

/**
 * Behaviour every broker transport must have. Each package runs it against a real broker:
 *   brokerContract('@simple-discovery/redis', options => new RedisDiscovery(options), available)
 * When the broker is unreachable the suite is skipped locally and fails in CI.
 */
export type Metadata = { value: number }
export type Factory = (options: BrokerDiscoveryOptions) => BrokerDiscovery<Metadata>

export async function brokerContract(name: string, create: Factory, reachable: () => Promise<boolean>) {
    const available = await reachable()
    if (!available && process.env.CI) throw new Error(`${name}: broker is not reachable in CI`)
    if (!available) console.warn(`${name}: broker not reachable, skipping (start it with docker compose up -d)`)
    const suite = available ? describe : describe.skip

    const opened: BrokerDiscovery<Metadata>[] = []
    afterEach(() => {
        for (const discovery of opened.splice(0)) discovery.close()
    })

    let counter = 0
    function setup(overrides: Partial<BrokerDiscoveryOptions> = {}) {
        const namespace = `contract-${process.pid}-${Date.now()}-${++counter}`
        const open = (nodeId: string, extra: Partial<BrokerDiscoveryOptions> = {}) => {
            const discovery = create({ namespace, tags: ['test'], node_id: nodeId, key: 'test-key', ...overrides, ...extra })
            opened.push(discovery)
            return discovery
        }
        const message = (nodeId: string, value: number, extra: Partial<DiscoveryMessage<Metadata>> = {}): DiscoveryMessage<Metadata> => ({
            node_id: nodeId,
            namespace,
            tags: ['test'],
            version: String(value),
            created_at: Date.now(),
            seq: value,
            data: { value },
            ...extra,
        })
        return { namespace, open, message }
    }

    const ready = (discovery: BrokerDiscovery<Metadata>) =>
        firstValueFrom(discovery.status$.pipe(filter(status => status === 'ready'), timeout(5_000)))
    const from = (discovery: BrokerDiscovery<Metadata>, nodeId: string) =>
        firstValueFrom(discovery.pipe(filter(event => event.node_id === nodeId), timeout({ first: 5_000 })))
    const collect = (discovery: BrokerDiscovery<Metadata>, ms: number) =>
        firstValueFrom(discovery.pipe(takeUntil(timer(ms)), toArray()))

    suite(name, () => {
        test('delivers a broadcast to another node', async () => {
            const { open, message } = setup()
            const receiver = open('receiver')
            const sender = open('sender')
            await Promise.all([ready(receiver), ready(sender)])

            const received = from(receiver, 'sender')
            await sender.broadcast(message('sender', 1))
            expect(await received).toMatchObject({ node_id: 'sender', seq: 1, data: { value: 1 } })
        })

        test('a late joiner learns the nodes that were already there', async () => {
            const { open, message } = setup()
            const existing = open('existing')
            await ready(existing)
            await existing.broadcast(message('existing', 1))

            const newcomer = open('newcomer')
            await ready(newcomer)
            const learned = from(newcomer, 'existing')
            await newcomer.broadcast(message('newcomer', 1))
            expect((await learned).data).toEqual({ value: 1 })
        })

        test('answers a newcomer once, not on every later broadcast', async () => {
            const { open, message } = setup()
            const a = open('a')
            const b = open('b')
            await Promise.all([ready(a), ready(b)])
            await a.broadcast(message('a', 1))
            const bLearnsA = from(b, 'a')
            await b.broadcast(message('b', 1))
            await bLearnsA
            // a's answer to b's hello may still be in flight; let it land.
            await collect(b, 300)

            // Steady state: a heartbeat from b must not make a answer again.
            const seenByB = collect(b, 400)
            await b.broadcast(message('b', 2))
            expect((await seenByB).filter(event => event.node_id === 'a')).toHaveLength(0)
        })

        test('never delivers a node its own messages', async () => {
            const { open, message } = setup()
            const node = open('self')
            await ready(node)
            const seen = collect(node, 300)
            await node.broadcast(message('self', 1))
            expect(await seen).toHaveLength(0)
        })

        test('ignores packets signed with another key', async () => {
            const { open, message } = setup()
            const receiver = open('receiver')
            const intruder = open('intruder', { key: 'other-key' })
            await Promise.all([ready(receiver), ready(intruder)])
            const seen = collect(receiver, 300)
            await intruder.broadcast(message('intruder', 1))
            expect(await seen).toHaveLength(0)
        })

        test('ignores messages that lack the required tags', async () => {
            const { open, message } = setup()
            const receiver = open('receiver', { tags: ['test', 'worker'] })
            const sender = open('sender')
            await Promise.all([ready(receiver), ready(sender)])
            const seen = collect(receiver, 300)
            await sender.broadcast(message('sender', 1))
            expect(await seen).toHaveLength(0)
        })

        test('rejects a broadcast outside its namespace, tags or node_id', async () => {
            const { open, message } = setup()
            const node = open('node')
            await ready(node)
            await expect(node.broadcast(message('node', 1, { namespace: 'elsewhere' }))).rejects.toThrow('namespace')
            await expect(node.broadcast(message('node', 1, { tags: [] }))).rejects.toThrow('tags')
            await expect(node.broadcast(message('other', 1))).rejects.toThrow('node_id')
        })

        test('close() completes the stream and is idempotent', async () => {
            const { open } = setup()
            const node = open('node')
            await ready(node)
            const completed = firstValueFrom(node.pipe(toArray(), timeout(2_000)))
            node.close()
            node.close()
            expect(await completed).toEqual([])
            expect(node.status).toBe('closed')
        })
    })
}

/** True when a TCP connection to `url`'s host and port opens within a second. */
export async function tcpReachable(url: string, defaultPort: number): Promise<boolean> {
    const { createConnection } = await import('node:net')
    const parsed = new URL(url)
    const port = Number(parsed.port || defaultPort)
    return new Promise(resolve => {
        const socket = createConnection({ host: parsed.hostname, port })
        const done = (ok: boolean) => { socket.destroy(); resolve(ok) }
        socket.setTimeout(1_000, () => done(false))
        socket.once('connect', () => done(true))
        socket.once('error', () => done(false))
    })
}
