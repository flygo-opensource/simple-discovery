# Changelog

## 3.0.0

### Added
- First release: discovery over an AMQP 0-9-1 fanout exchange `simple-discovery.<namespace>` with one
  exclusive queue per node, signed like every simple-discovery transport. Uses amqplib 2 recovery to
  reconnect forever and says hello again after each reconnect. Accepts an existing connection.
