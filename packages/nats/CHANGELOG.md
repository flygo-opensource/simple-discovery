# Changelog

## 3.0.0

### Added
- First release: discovery over NATS on `simple-discovery.<namespace>`, signed like every
  simple-discovery transport. Late joiners learn existing nodes through `hello`; reconnects forever by
  default and says hello again after each reconnect. Accepts an existing `NatsConnection`.
