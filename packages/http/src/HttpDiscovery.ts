import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import { BehaviorSubject, Observable, Subject, type Subscriber } from 'rxjs'
import {
    containsAllTags,
    hasDiscoveryEnvelope,
    type Discovery,
    type DiscoveryMessage,
    type DiscoveryOfflineData,
    type DiscoveryOptions,
} from './types.js'

export type HttpDiscoveryStatus = 'not_ready' | 'ready' | 'closed'

type HttpDiscoverySharedOptions = DiscoveryOptions & {
    key?: string
}

export type HttpDiscoveryServerOptions = HttpDiscoverySharedOptions & {
    mode: 'server'
    host?: string
    port?: number
    ttlMs?: number
}

export type HttpDiscoveryClientOptions = HttpDiscoverySharedOptions & {
    mode: 'client'
    servers: string[]
    heartbeatMs?: number
    requestTimeoutMs?: number
    retryAttempts?: number
}

export type HttpDiscoveryOptions = HttpDiscoveryServerOptions | HttpDiscoveryClientOptions

type StoredNode<T> = {
    message: DiscoveryMessage<T>
    lastSeen: number
}

const DEFAULT_PORT = 12001
const DEFAULT_HEARTBEAT_MS = 10_000
const DEFAULT_TTL_MS = 35_000
const DEFAULT_REQUEST_TIMEOUT_MS = 2_000
const MAX_RETRY_ATTEMPTS = 5

export class HttpDiscovery<T> extends Observable<DiscoveryMessage<T>> implements Discovery<T> {
    readonly #events = new Subject<DiscoveryMessage<T>>()
    readonly #status = new BehaviorSubject<HttpDiscoveryStatus>('not_ready')
    readonly #nodes = new Map<string, StoredNode<T>>()
    readonly #options: HttpDiscoveryOptions
    readonly #key: string
    readonly #servers: string[]
    readonly #heartbeatMs: number
    readonly #ttlMs: number
    readonly #requestTimeoutMs: number

    readonly status$ = this.#status.asObservable()

    #server?: Server
    #heartbeatTimer?: ReturnType<typeof setInterval>
    #ttlTimer?: ReturnType<typeof setInterval>
    #lastMessage?: DiscoveryMessage<T>
    #closed = false

    constructor(options: HttpDiscoveryOptions) {
        super((subscriber: Subscriber<DiscoveryMessage<T>>) => this.#events.subscribe(subscriber))
        this.#options = options
        this.#key = options.key ?? process.env.OHAYO_DISCOVERY_KEY ?? 'ohayo'
        this.#servers = [...new Set((options.mode === 'client' ? options.servers : [])
            .map(server => server.trim())
            .filter(Boolean)
            .map(server => this.#normalizeServer(server)))]
        if (options.mode === 'client' && this.#servers.length === 0) {
            throw new Error('HttpDiscovery client mode requires at least one server')
        }
        this.#heartbeatMs = options.mode === 'client' ? options.heartbeatMs ?? DEFAULT_HEARTBEAT_MS : 0
        this.#ttlMs = options.mode === 'server' ? options.ttlMs ?? DEFAULT_TTL_MS : 0
        this.#requestTimeoutMs = options.mode === 'client'
            ? options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS
            : DEFAULT_REQUEST_TIMEOUT_MS

        if (options.mode === 'server') {
            this.#listen(
                options.port ?? Number(process.env.OHAYO_DISCOVERY_PORT || DEFAULT_PORT),
                options.host,
            )
        } else {
            this.#status.next('ready')
        }
    }

    get port(): number | undefined {
        const address = this.#server?.address()
        return typeof address === 'object' && address ? address.port : undefined
    }

    async broadcast(message: DiscoveryMessage<T>): Promise<void> {
        if (this.#closed) return
        const outbound = this.#validateOutbound(message)
        this.#lastMessage = outbound
        if (this.#options.mode === 'server') return
        this.#startHeartbeat()
        await Promise.all(this.#servers.map(server => this.#register(server, outbound, 0)))
    }

    close(): void {
        if (this.#closed) return
        this.#closed = true
        clearInterval(this.#heartbeatTimer)
        clearInterval(this.#ttlTimer)

        if (this.#lastMessage) {
            for (const server of this.#servers) {
                void this.#deregister(server, this.#lastMessage.node_id).catch(() => {})
            }
        }

        this.#status.next('closed')
        this.#events.complete()
        this.#status.complete()
        this.#server?.close()
    }

    #listen(port: number, host?: string): void {
        this.#server = createServer((request, response) => {
            void this.#handle(request, response).catch(error => {
                this.#json(response, 500, { error: { message: String(error), code: 'INTERNAL_ERROR' } })
            })
        })
        this.#server.listen(port, host, () => {
            if (!this.#closed) this.#status.next('ready')
        })
        this.#server.on('error', error => this.#events.error(error))
        this.#ttlTimer = setInterval(() => this.#expireNodes(), Math.max(50, Math.floor(this.#ttlMs / 3)))
        this.#ttlTimer.unref?.()
    }

    async #handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
        const url = new URL(request.url ?? '/', 'http://127.0.0.1')
        if (request.method === 'GET' && url.pathname === '/health') {
            this.#json(response, 200, { ok: true })
            return
        }
        if (request.method === 'GET' && url.pathname === '/nodes') {
            if (!this.#authorized(request)) return this.#json(response, 401, { error: { message: 'Unauthorized', code: 'UNAUTHORIZED' } })
            this.#json(response, 200, { nodes: [...this.#nodes.values()].map(node => node.message) })
            return
        }
        if (request.method === 'POST' && url.pathname === '/register') {
            if (!this.#authorized(request)) return this.#json(response, 401, { error: { message: 'Unauthorized', code: 'UNAUTHORIZED' } })
            const body = await this.#readJson(request)
            if (!this.#validInbound(body)) return this.#json(response, 400, { error: { message: 'Invalid discovery message', code: 'INVALID_DISCOVERY_MESSAGE' } })
            this.#upsert(body, this.#remoteHost(request))
            this.#json(response, 204)
            return
        }
        if (request.method === 'DELETE' && url.pathname.startsWith('/register/')) {
            if (!this.#authorized(request)) return this.#json(response, 401, { error: { message: 'Unauthorized', code: 'UNAUTHORIZED' } })
            this.#remove(decodeURIComponent(url.pathname.slice('/register/'.length)))
            this.#json(response, 204)
            return
        }
        this.#json(response, 404, { error: { message: 'Not found', code: 'NOT_FOUND' } })
    }

    #upsert(message: DiscoveryMessage<T>, remoteHost: string): void {
        const existing = this.#nodes.get(message.node_id)
        if (existing && message.seq <= existing.message.seq) {
            existing.lastSeen = Date.now()
            return
        }
        const enriched = { ...message, remote_host: remoteHost }
        this.#nodes.set(message.node_id, { message: enriched, lastSeen: Date.now() })
        this.#events.next(enriched)
    }

    #remove(nodeId: string): void {
        const existing = this.#nodes.get(nodeId)
        if (!existing) return
        this.#nodes.delete(nodeId)
        this.#events.next(this.#offlineMessage(existing.message))
    }

    #expireNodes(): void {
        if (this.#closed) return
        const deadline = Date.now() - this.#ttlMs
        for (const [nodeId, node] of this.#nodes) {
            if (node.lastSeen > deadline) continue
            this.#nodes.delete(nodeId)
            this.#events.next(this.#offlineMessage(node.message))
        }
    }

    #offlineMessage(message: DiscoveryMessage<T>): DiscoveryMessage<T> {
        const now = Date.now()
        return {
            ...message,
            version: String(now),
            created_at: now,
            seq: message.seq + 1,
            data: { status: 'offline' } as DiscoveryOfflineData as T,
        }
    }

    #validInbound(value: unknown): value is DiscoveryMessage<T> {
        if (!hasDiscoveryEnvelope<T>(value)) return false
        if (value.namespace !== this.#options.namespace) return false
        if (!containsAllTags(value.tags, this.#options.tags)) return false
        return !this.#options.node_id || value.node_id !== this.#options.node_id
    }

    #validateOutbound(message: DiscoveryMessage<T>): DiscoveryMessage<T> {
        if (!hasDiscoveryEnvelope<T>(message)) throw new Error('Invalid discovery message envelope')
        if (message.namespace !== this.#options.namespace) throw new Error(`Discovery message namespace must be ${this.#options.namespace}`)
        if (!containsAllTags(message.tags, this.#options.tags)) throw new Error(`Discovery message must contain tags: ${this.#options.tags.join(',')}`)
        if (this.#options.node_id && message.node_id !== this.#options.node_id) throw new Error(`Discovery message node_id must be ${this.#options.node_id}`)
        return message
    }

    #startHeartbeat(): void {
        if (this.#heartbeatTimer || this.#heartbeatMs <= 0 || this.#servers.length === 0) return
        this.#heartbeatTimer = setInterval(() => {
            if (!this.#lastMessage || this.#closed) return
            this.#lastMessage = this.#bump(this.#lastMessage)
            for (const server of this.#servers) void this.#register(server, this.#lastMessage, 0)
        }, this.#heartbeatMs)
        this.#heartbeatTimer.unref?.()
    }

    #bump(message: DiscoveryMessage<T>): DiscoveryMessage<T> {
        const now = Date.now()
        return { ...message, version: String(now), created_at: now, seq: message.seq + 1 }
    }

    async #register(server: string, message: DiscoveryMessage<T>, attempt: number): Promise<void> {
        try {
            const response = await fetch(`${server}/register`, {
                method: 'POST',
                headers: this.#headers(),
                body: JSON.stringify(message),
                signal: AbortSignal.timeout(this.#requestTimeoutMs),
            })
            if (!response.ok) throw new Error(`HTTP ${response.status}`)
        } catch (error) {
            this.#scheduleRetry(server, message, attempt, error)
        }
    }

    async #deregister(server: string, nodeId: string): Promise<void> {
        await fetch(`${server}/register/${encodeURIComponent(nodeId)}`, {
            method: 'DELETE',
            headers: this.#headers(),
            signal: AbortSignal.timeout(this.#requestTimeoutMs),
        })
    }

    #scheduleRetry(server: string, message: DiscoveryMessage<T>, attempt: number, error: unknown): void {
        const retryAttempts = this.#options.mode === 'client'
            ? this.#options.retryAttempts ?? MAX_RETRY_ATTEMPTS
            : 0
        if (this.#closed || attempt >= retryAttempts) {
            if (!this.#closed && process.env.OHAYO_HTTP_DEBUG) console.error(error)
            return
        }
        const delay = Math.min(30_000, 250 * 2 ** attempt)
        const timer = setTimeout(() => void this.#register(server, this.#bump(message), attempt + 1), delay)
        timer.unref?.()
    }

    #headers(): HeadersInit {
        return { authorization: `Bearer ${this.#key}`, 'content-type': 'application/json' }
    }

    #authorized(request: IncomingMessage): boolean {
        return !this.#key || request.headers.authorization === `Bearer ${this.#key}`
    }

    async #readJson(request: IncomingMessage): Promise<unknown> {
        const chunks: Buffer[] = []
        for await (const chunk of request) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk))
        return chunks.length === 0 ? undefined : JSON.parse(Buffer.concat(chunks).toString('utf8'))
    }

    #json(response: ServerResponse, status: number, body?: unknown): void {
        response.statusCode = status
        if (body === undefined) {
            response.end()
            return
        }
        response.setHeader('content-type', 'application/json')
        response.end(JSON.stringify(body))
    }

    #remoteHost(request: IncomingMessage): string {
        return (request.socket.remoteAddress ?? '').replace(/^::ffff:/, '') || '127.0.0.1'
    }

    #normalizeServer(server: string): string {
        const value = /^https?:\/\//.test(server) ? server : `http://${server}`
        const url = new URL(value)
        if (url.protocol !== 'http:' && url.protocol !== 'https:') {
            throw new Error(`Unsupported discovery server protocol: ${url.protocol}`)
        }
        return url.toString().replace(/\/+$/, '')
    }
}
