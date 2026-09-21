import type { Observable } from 'rxjs'

export type DiscoveryMessage<T> = {
    node_id: string
    namespace: string
    tags: string[]
    version: string
    created_at: number
    seq: number
    data: T
    remote_host?: string
}

export type DiscoveryOptions = {
    namespace: string
    tags: string[]
    node_id?: string
}

/**
 * Generic discovery contract shared with `@ohayo/udp`.
 */
export type Discovery<T> = Observable<DiscoveryMessage<T>> & {
    broadcast(message: DiscoveryMessage<T>): Promise<void>
    close(): void
}

export type DiscoveryOfflineData = { status: 'offline' }

export function isDiscoveryOfflineData(data: unknown): data is DiscoveryOfflineData {
    return typeof data === 'object'
        && data !== null
        && (data as { status?: unknown }).status === 'offline'
}

export function hasDiscoveryEnvelope<T>(value: unknown): value is DiscoveryMessage<T> {
    if (typeof value !== 'object' || value === null) return false
    const message = value as Partial<DiscoveryMessage<T>>
    return typeof message.node_id === 'string'
        && message.node_id.length > 0
        && typeof message.namespace === 'string'
        && Array.isArray(message.tags)
        && message.tags.every(tag => typeof tag === 'string')
        && typeof message.version === 'string'
        && typeof message.created_at === 'number'
        && Number.isFinite(message.created_at)
        && typeof message.seq === 'number'
        && Number.isFinite(message.seq)
        && 'data' in message
}

export function containsAllTags(actual: readonly string[] | undefined, required: readonly string[]) {
    return !!actual && required.every(tag => actual.includes(tag))
}
