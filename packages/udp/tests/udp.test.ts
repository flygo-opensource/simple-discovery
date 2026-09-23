import { afterEach, describe, expect, test } from 'bun:test'
import { createHmac } from 'node:crypto'
import { createSocket } from 'node:dgram'
import { pack } from 'msgpackr'
import { filter, firstValueFrom, timeout } from 'rxjs'
import {
    UdpDiscovery,
    type Discovery,
    type DiscoveryMessage,
    type UdpDiscoveryPacket,
} from '../src/index.js'

type Metadata = { role: 'sender' | 'receiver'; value: number }
const opened: UdpDiscovery<Metadata>[] = []

afterEach(() => {
    for (const discovery of opened.splice(0)) discovery.close()
})

describe('@simple-discovery/udp', () => {
    test('is a generic Discovery<T> and broadcasts its envelope', async () => {
        const port = randomPort()
        const receiver = createDiscovery('receiver', port)
        const sender: Discovery<Metadata> = createDiscovery('sender', port)
        await Promise.all([ready(receiver), ready(sender as UdpDiscovery<Metadata>)])

        const received = firstValueFrom(receiver.pipe(
            filter(event => event.node_id === 'sender'),
            timeout({ first: 2_000 }),
        ))
        await sender.broadcast(message('sender', 1))

        expect(await received).toMatchObject({
            node_id: 'sender',
            namespace: 'test',
            tags: ['simple-discovery', 'test'],
            seq: 1,
            data: { role: 'sender', value: 1 },
        })
    })

    test('supports explicit peers', async () => {
        const port = randomPort()
        const receiver = createDiscovery('receiver', port)
        const sender = createDiscovery('sender', port)
        await Promise.all([ready(receiver), ready(sender)])
        const received = next(receiver)
        await sender.broadcast(message('sender', 2), '127.0.0.1')
        expect((await received).seq).toBe(2)
    })

    test('replies once with the latest local announcement when a late peer appears', async () => {
        const port = randomPort()
        const existing = createDiscovery('existing', port)
        await ready(existing)
        await existing.broadcast(message('existing', 1))

        const newcomer = createDiscovery('newcomer', port)
        await ready(newcomer)
        const reply = firstValueFrom(newcomer.pipe(
            filter(event => event.node_id === 'existing'),
            timeout({ first: 2_000 }),
        ))

        await newcomer.broadcast(message('newcomer', 1))
        expect((await reply).node_id).toBe('existing')
    })

    test('rejects wrong signatures and expired packets', async () => {
        const port = randomPort()
        const receiver = createDiscovery('receiver', port, { packetTtlMs: 100 })
        await ready(receiver)
        let count = 0
        receiver.subscribe(() => count++)

        await sendRaw(rawPacket('wrong-key', message('sender', 1)), port)
        await sendRaw(rawPacket('test-key', message('sender', 2), Date.now() - 1_000), port)
        await Bun.sleep(150)
        expect(count).toBe(0)
    })

    test('filters namespace, required tags, and self packets', async () => {
        const port = randomPort()
        const receiver = createDiscovery('receiver', port)
        await ready(receiver)
        let count = 0
        receiver.subscribe(() => count++)

        await sendRaw(rawPacket('test-key', { ...message('sender', 1), namespace: 'other' }), port)
        await sendRaw(rawPacket('test-key', { ...message('sender', 2), tags: ['simple-discovery'] }), port)
        await sendRaw(rawPacket('test-key', message('receiver', 3)), port)
        await Bun.sleep(150)
        expect(count).toBe(0)
    })

    test('rejects malformed and legacy node packets', async () => {
        const port = randomPort()
        const receiver = createDiscovery('receiver', port)
        await ready(receiver)
        let count = 0
        receiver.subscribe(() => count++)

        await sendRaw(pack({ bad: true }), port)
        await sendRaw(pack({ version: 1, sender_id: 'legacy', timestamp: Date.now(), node: {} }), port)
        await Bun.sleep(150)
        expect(count).toBe(0)
    })

    test('does not deduplicate or reorder distinct messages', async () => {
        const port = randomPort()
        const receiver = createDiscovery('receiver', port)
        await ready(receiver)
        const seqs: number[] = []
        receiver.subscribe(event => seqs.push(event.seq))

        // Hai message cùng seq nhưng khác created_at là hai message khác nhau.
        const now = Date.now()
        for (const [index, seq] of [2, 1, 1, 3].entries()) {
            await sendRaw(rawPacket('test-key', { ...message('sender', seq), created_at: now + index }), port)
        }
        await Bun.sleep(150)
        expect(seqs).toEqual([2, 1, 1, 3])
    })

    test('delivers the same packet once however many copies arrive', async () => {
        const port = randomPort()
        const receiver = createDiscovery('receiver', port)
        await ready(receiver)
        let count = 0
        receiver.subscribe(() => count++)

        const raw = rawPacket('test-key', message('sender', 1))
        for (let copy = 0; copy < 3; copy++) await sendRaw(raw, port)
        await Bun.sleep(150)
        expect(count).toBe(1)
    })

    test('validates outbound envelopes', async () => {
        const port = randomPort()
        const sender = createDiscovery('sender', port)
        await ready(sender)
        await expect(sender.broadcast({ ...message('other', 1) })).rejects.toThrow('node_id')
        await expect(sender.broadcast({ ...message('sender', 1), namespace: 'other' })).rejects.toThrow('namespace')
        await expect(sender.broadcast({ ...message('sender', 1), tags: ['simple-discovery'] })).rejects.toThrow('tags')
    })

    test('close is idempotent and exposes lifecycle status', async () => {
        const discovery = createDiscovery('receiver', randomPort())
        const statuses: string[] = []
        discovery.status$.subscribe(status => statuses.push(status))
        await ready(discovery)
        discovery.close()
        discovery.close()
        expect(statuses).toEqual(['not_ready', 'ready', 'closed'])
    })
})

function createDiscovery(
    node_id: string,
    port: number,
    extra: Partial<ConstructorParameters<typeof UdpDiscovery<Metadata>>[0]> = {},
) {
    const discovery = new UdpDiscovery<Metadata>({
        namespace: 'test',
        tags: ['simple-discovery', 'test'],
        node_id,
        key: 'test-key',
        port,
        multicastAddress: '239.0.1.9',
        ...extra,
    })
    opened.push(discovery)
    return discovery
}

function message(node_id: string, seq: number): DiscoveryMessage<Metadata> {
    return {
        node_id,
        namespace: 'test',
        tags: ['simple-discovery', 'test'],
        version: String(seq),
        created_at: Date.now(),
        seq,
        data: { role: node_id === 'receiver' ? 'receiver' : 'sender', value: seq },
    }
}

function rawPacket(key: string, body: DiscoveryMessage<Metadata>, timestamp = body.created_at) {
    const unsigned: Omit<UdpDiscoveryPacket<Metadata>, 'signature'> = {
        version: 1,
        sender_id: body.node_id,
        timestamp,
        message: body,
    }
    const signature = createHmac('sha256', key).update(pack(unsigned)).digest('hex')
    return pack({ ...unsigned, signature })
}

function sendRaw(raw: Buffer, port: number) {
    return new Promise<void>((resolve, reject) => {
        const socket = createSocket('udp4')
        socket.send(raw, port, '127.0.0.1', error => {
            socket.close()
            error ? reject(error) : resolve()
        })
    })
}

function ready(discovery: UdpDiscovery<Metadata>) {
    return firstValueFrom(discovery.status$.pipe(
        filter(status => status === 'ready'),
        timeout({ first: 2_000 }),
    ))
}

function next(discovery: UdpDiscovery<Metadata>) {
    return firstValueFrom(discovery.pipe(timeout({ first: 2_000 })))
}

function randomPort() {
    return 20_000 + Math.floor(Math.random() * 20_000)
}
