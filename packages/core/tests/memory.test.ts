import { expect, test } from 'bun:test'
import { EventEmitter } from 'node:events'
import { firstValueFrom, filter, timeout } from 'rxjs'
import { BrokerDiscovery, decodePacket, encodePacket, type BrokerDiscoveryOptions } from '../src/index.js'
import { brokerContract, type Metadata } from './contract.js'

/** In-memory broker: one topic per namespace, delivered asynchronously like a real one. */
const bus = new EventEmitter().setMaxListeners(0)

class MemoryDiscovery<T> extends BrokerDiscovery<T> {
    readonly #topic: string
    readonly #listener = (raw: Uint8Array) => this.receive(raw)

    constructor(options: BrokerDiscoveryOptions) {
        super('memory', options)
        this.#topic = options.namespace
    }

    protected async connect() {
        bus.on(this.#topic, this.#listener)
    }

    protected async publish(raw: Uint8Array) {
        setImmediate(() => bus.emit(this.#topic, raw))
    }

    protected async disconnect() {
        bus.off(this.#topic, this.#listener)
    }

    reconnect() {
        this.resync()
    }
}

await brokerContract('@simple-discovery/core (memory broker)', options => new MemoryDiscovery<Metadata>(options), async () => true)

test('packets round-trip and reject tampering, stale timestamps and the wrong key', () => {
    const message = { node_id: 'a', namespace: 'n', tags: ['t'], version: '1', created_at: 1, seq: 1, data: { x: 1 } }
    const raw = encodePacket(message, 'k', true)
    expect(decodePacket(raw, 'k', 1_000)).toMatchObject({ sender_id: 'a', hello: true, message })
    expect(decodePacket(raw, 'other', 1_000)).toBeUndefined()

    const tampered = Buffer.from(raw)
    tampered[tampered.indexOf(0x78)] = 0x79 // "x" -> "y" inside the payload
    expect(decodePacket(tampered, 'k', 1_000)).toBeUndefined()

    const stale = encodePacket(message, 'k')
    expect(decodePacket(stale, 'k', -1)).toBeUndefined()
    expect(decodePacket(Buffer.from('not msgpack'), 'k', 1_000)).toBeUndefined()
})

test('after a reconnect the node asks again and relearns its peers', async () => {
    const options = { namespace: `resync-${Date.now()}`, tags: ['t'], key: 'k' }
    const message = (node_id: string, seq: number) => ({ ...options, node_id, version: String(seq), created_at: Date.now(), seq, data: { value: seq } })
    const a = new MemoryDiscovery<Metadata>(options)
    const b = new MemoryDiscovery<Metadata>(options)
    try {
        await a.broadcast(message('a', 1))
        await b.broadcast(message('b', 1))
        const relearned = firstValueFrom(a.pipe(filter(event => event.node_id === 'b'), timeout(2_000)))
        a.reconnect()
        expect((await relearned).seq).toBe(1)
    } finally {
        a.close()
        b.close()
    }
})

test('a failed connection errors the stream and rejects broadcast', async () => {
    class Broken extends MemoryDiscovery<Metadata> {
        protected override async connect() { throw new Error('broker down') }
    }
    const broken = new Broken({ namespace: 'x', tags: [], key: 'k' })
    const errored = firstValueFrom(broken).catch(error => error)
    expect(String(await errored)).toContain('broker down')
    await expect(broken.broadcast({ node_id: 'a', namespace: 'x', tags: [], version: '1', created_at: 1, seq: 1, data: { value: 1 } })).rejects.toThrow('broker down')
    expect(broken.status).toBe('closed')
})
