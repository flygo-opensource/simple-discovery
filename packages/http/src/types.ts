export {
    containsAllTags,
    hasDiscoveryEnvelope,
    type Discovery,
    type DiscoveryMessage,
    type DiscoveryOptions,
} from '@simple-discovery/core'

export type DiscoveryOfflineData = { status: 'offline' }

export function isDiscoveryOfflineData(data: unknown): data is DiscoveryOfflineData {
    return typeof data === 'object'
        && data !== null
        && (data as { status?: unknown }).status === 'offline'
}
