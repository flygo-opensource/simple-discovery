// One discovery node for the cross-host broker e2e. Announces itself once (no heartbeat), so it
// can only learn the others through hello replies, and prints what it sees as JSON lines.
import { AmqpDiscovery } from '../../packages/amqp/src/index.ts'
import { NatsDiscovery } from '../../packages/nats/src/index.ts'
import { RedisDiscovery } from '../../packages/redis/src/index.ts'

const env = (name: string) => process.env[name] ?? (() => { throw new Error(`${name} is required`) })()
const name = env('AGENT_NAME')
const transport = env('AGENT_TRANSPORT')
const restartAt = Number(env('AGENT_RESTART_AT'))
const options = { namespace: env('AGENT_NAMESPACE'), tags: ['e2e'], node_id: name, key: env('AGENT_KEY') }
const url = env('AGENT_URL')

const discovery = transport === 'redis' ? new RedisDiscovery<{ host: string }>({ ...options, url })
    : transport === 'nats' ? new NatsDiscovery<{ host: string }>({ ...options, servers: url })
    : new AmqpDiscovery<{ host: string }>({ ...options, url, recovery: { initialDelay: 100, maxDelay: 1_000 } })

const log = (event: Record<string, unknown>) => console.log(JSON.stringify({ agent: name, t: Date.now(), ...event }))
discovery.status$.subscribe(status => log({ ev: 'status', status }))
discovery.subscribe({
    next: message => log({ ev: 'seen', from: message.node_id, phase: Date.now() < restartAt ? 1 : 2 }),
    error: error => log({ ev: 'error', error: String(error) }),
})

const now = Date.now()
await discovery.broadcast({ ...options, version: String(now), created_at: now, seq: 1, data: { host: name } })
setTimeout(() => { discovery.close(); process.exit(0) }, Number(env('AGENT_DURATION_MS')))
