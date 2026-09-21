import { createHmac, timingSafeEqual } from 'node:crypto'
import { pack, unpack } from 'msgpackr'
import { hasDiscoveryEnvelope, type DiscoveryMessage } from './types.js'

/**
 * Signed wire format shared by every transport. `hello` asks the receivers to answer with their own
 * latest announcement, so a node that just started learns the nodes that were already there.
 */
export type DiscoveryPacket<T> = {
    version: 1
    sender_id: string
    timestamp: number
    message: DiscoveryMessage<T>
    hello?: true
    signature: string
}

type UnsignedPacket<T> = Omit<DiscoveryPacket<T>, 'signature'>

export function encodePacket<T>(message: DiscoveryMessage<T>, key: string, hello = false): Buffer {
    const unsigned: UnsignedPacket<T> = {
        version: 1,
        sender_id: message.node_id,
        timestamp: Date.now(),
        message,
        ...(hello ? { hello: true as const } : {}),
    }
    return pack({ ...unsigned, signature: sign(unsigned, key) })
}

/** Returns the packet when it is well formed, correctly signed and not older than `ttlMs`. */
export function decodePacket<T>(raw: Uint8Array, key: string, ttlMs: number): DiscoveryPacket<T> | undefined {
    let packet: Partial<DiscoveryPacket<T>>
    try {
        packet = unpack(raw) as Partial<DiscoveryPacket<T>>
    } catch {
        return
    }
    if (typeof packet !== 'object' || packet === null || packet.version !== 1) return
    if (typeof packet.sender_id !== 'string' || typeof packet.timestamp !== 'number') return
    if (typeof packet.signature !== 'string' || !hasDiscoveryEnvelope<T>(packet.message)) return
    if (packet.sender_id !== packet.message.node_id) return
    if (packet.hello !== undefined && packet.hello !== true) return
    if (Math.abs(Date.now() - packet.timestamp) > ttlMs) return
    const { signature, ...unsigned } = packet as DiscoveryPacket<T>
    if (!signatureMatches(signature, sign(unsigned, key))) return
    return packet as DiscoveryPacket<T>
}

function sign<T>(packet: UnsignedPacket<T>, key: string) {
    return createHmac('sha256', key).update(pack(packet)).digest('hex')
}

function signatureMatches(actual: string, expected: string) {
    const actualBytes = Buffer.from(actual, 'hex')
    const expectedBytes = Buffer.from(expected, 'hex')
    return actualBytes.length === expectedBytes.length && timingSafeEqual(actualBytes, expectedBytes)
}
