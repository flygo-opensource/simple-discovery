# Changelog

## 3.0.0

### Added
- Private workspace package, bundled into every published package; never published itself.
- First release: the `Discovery<T>` contract, the signed msgpack packet (`encodePacket`, `decodePacket`)
  with a `hello` flag, and `BrokerDiscovery<T>`, the base class of the Redis, NATS and AMQP transports.
