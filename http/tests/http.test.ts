import { afterEach, describe, expect, test } from 'bun:test'
import { filter, firstValueFrom, timeout } from 'rxjs'
import {
    HttpDiscovery,
    isDiscoveryOfflineData,
    type Discovery,
    type DiscoveryMessage,
} from '../src/index.js'

type Metadata = { role: 'service'; name: string } | { status: 'offline' }

const discoveries: HttpDiscovery<Metadata>[] = []

afterEach(() => {
    for (const discovery of discoveries.splice(0)) discovery.close()
})

describe('HttpDiscovery', () => {
    test('implements the generic discovery contract and registers messages', async () => {
        const registry = track(new HttpDiscovery<Metadata>({
            mode: 'server',
            namespace: 'test',
            tags: ['ohayo'],
            node_id: 'gateway',
            key: 'secret',
            port: 0,
        }))
        await firstValueFrom(registry.status$.pipe(timeout(1_000)))
        await waitUntil(() => registry.port !== undefined)

        const service = track(new HttpDiscovery<Metadata>({
            mode: 'client',
            namespace: 'test',
            tags: ['ohayo'],
            node_id: 'service',
            key: 'secret',
            servers: [`127.0.0.1:${registry.port}`],
            heartbeatMs: 0,
        }))

        const contract: Discovery<Metadata> = service
        const received = firstValueFrom(registry.pipe(timeout(1_000)))
        await contract.broadcast(message('service', { role: 'service', name: 'products' }))

        expect((await received).data).toEqual({ role: 'service', name: 'products' })
    })

    test('rejects an invalid bearer token', async () => {
        const registry = track(new HttpDiscovery<Metadata>({
            mode: 'server',
            namespace: 'test',
            tags: ['ohayo'],
            key: 'secret',
            port: 0,
        }))
        await waitUntil(() => registry.port !== undefined)

        const response = await fetch(`http://127.0.0.1:${registry.port}/register`, {
            method: 'POST',
            headers: { authorization: 'Bearer wrong', 'content-type': 'application/json' },
            body: JSON.stringify(message('service', { role: 'service', name: 'products' })),
        })
        expect(response.status).toBe(401)
    })

    test('client registers with every configured server', async () => {
        const first = track(new HttpDiscovery<Metadata>({
            mode: 'server',
            namespace: 'test',
            tags: ['ohayo'],
            node_id: 'server-1',
            key: 'secret',
            port: 0,
        }))
        const second = track(new HttpDiscovery<Metadata>({
            mode: 'server',
            namespace: 'test',
            tags: ['ohayo'],
            node_id: 'server-2',
            key: 'secret',
            port: 0,
        }))
        await waitUntil(() => first.port !== undefined && second.port !== undefined)

        const client = track(new HttpDiscovery<Metadata>({
            mode: 'client',
            namespace: 'test',
            tags: ['ohayo'],
            node_id: 'service',
            key: 'secret',
            servers: [
                `127.0.0.1:${first.port}`,
                `http://127.0.0.1:${second.port}/`,
            ],
            heartbeatMs: 0,
        }))

        const receivedByFirst = firstValueFrom(first.pipe(timeout(1_000)))
        const receivedBySecond = firstValueFrom(second.pipe(timeout(1_000)))
        await client.broadcast(message('service', { role: 'service', name: 'products' }))

        expect((await receivedByFirst).node_id).toBe('service')
        expect((await receivedBySecond).node_id).toBe('service')
    })

    test('client mode requires a non-empty server list', () => {
        expect(() => new HttpDiscovery<Metadata>({
            mode: 'client',
            namespace: 'test',
            tags: ['ohayo'],
            servers: [],
        })).toThrow('requires at least one server')
    })

    test('server emits offline when a client deregisters', async () => {
        const server = track(new HttpDiscovery<Metadata>({
            mode: 'server',
            namespace: 'test',
            tags: ['ohayo'],
            node_id: 'server',
            key: 'secret',
            port: 0,
        }))
        await waitUntil(() => server.port !== undefined)

        const client = track(new HttpDiscovery<Metadata>({
            mode: 'client',
            namespace: 'test',
            tags: ['ohayo'],
            node_id: 'service',
            key: 'secret',
            servers: [`127.0.0.1:${server.port}`],
            heartbeatMs: 0,
        }))
        await client.broadcast(message('service', { role: 'service', name: 'products' }))

        const offline = firstValueFrom(server.pipe(timeout(1_000)))
        client.close()
        const event = await offline
        expect(event.node_id).toBe('service')
        expect(isDiscoveryOfflineData(event.data)).toBe(true)
    })

    test('server expires a client after its TTL', async () => {
        const server = track(new HttpDiscovery<Metadata>({
            mode: 'server',
            namespace: 'test',
            tags: ['ohayo'],
            node_id: 'server',
            key: 'secret',
            port: 0,
            ttlMs: 60,
        }))
        await waitUntil(() => server.port !== undefined)

        const client = track(new HttpDiscovery<Metadata>({
            mode: 'client',
            namespace: 'test',
            tags: ['ohayo'],
            node_id: 'service',
            key: 'secret',
            servers: [`127.0.0.1:${server.port}`],
            heartbeatMs: 0,
        }))

        const offline = firstValueFrom(server.pipe(
            filter(event => isDiscoveryOfflineData(event.data)),
            timeout(1_000),
        ))
        await client.broadcast(message('service', { role: 'service', name: 'products' }))

        expect((await offline).node_id).toBe('service')
    })
})

function track<T extends HttpDiscovery<Metadata>>(discovery: T): T {
    discoveries.push(discovery)
    return discovery
}

function message(node_id: string, data: Metadata): DiscoveryMessage<Metadata> {
    const now = Date.now()
    return {
        node_id,
        namespace: 'test',
        tags: ['ohayo'],
        version: String(now),
        created_at: now,
        seq: 1,
        data,
    }
}

async function waitUntil(predicate: () => boolean): Promise<void> {
    const deadline = Date.now() + 1_000
    while (!predicate()) {
        if (Date.now() > deadline) throw new Error('Timed out')
        await Bun.sleep(5)
    }
}
