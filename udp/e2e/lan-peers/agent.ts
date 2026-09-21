// Agent e2e cho chế độ peers: phát announcement định kỳ, ghi lại mọi message nhận được.
import { UdpDiscovery } from '../../src/index.ts'

const name = process.env.AGENT_NAME!
const namespace = process.env.AGENT_NAMESPACE!
const peers = (process.env.AGENT_PEERS || '').split(',').map(v => v.trim()).filter(Boolean)
const durationMs = Number(process.env.AGENT_DURATION_MS || 8000)
const log = (event: Record<string, unknown>) => console.log(JSON.stringify({ at: Date.now(), agent: name, ...event }))

const discovery = new UdpDiscovery<{ name: string }>({
    namespace,
    tags: ['ohayo', 'peers-e2e'],
    node_id: name,
    key: process.env.AGENT_KEY!,
    port: Number(process.env.AGENT_PORT),
    multicastAddress: process.env.AGENT_GROUP!,
    peers, // mảng rỗng = không có peers (không rơi về biến môi trường)
    multicast: process.env.AGENT_MULTICAST !== 'off',
})

discovery.subscribe(message => log({ ev: 'seen', from: message.node_id, remote_host: message.remote_host }))
await new Promise<void>(resolve => {
    const sub = discovery.status$.subscribe(status => {
        if (status === 'ready') { queueMicrotask(() => sub.unsubscribe()); resolve() }
    })
})
log({ ev: 'ready', peers, group: process.env.AGENT_GROUP })

const announce = () => discovery.broadcast({
    node_id: name, namespace, tags: ['ohayo', 'peers-e2e'],
    version: '1', created_at: Date.now(), seq: 1, data: { name },
})
await announce()
const timer = setInterval(() => void announce(), 1500)

setTimeout(() => {
    clearInterval(timer)
    discovery.close()
    log({ ev: 'done' })
    process.exit(0)
}, durationMs)
