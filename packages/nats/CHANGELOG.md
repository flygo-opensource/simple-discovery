# Changelog

## 3.0.0

### Changed
- Self-contained: the shared core (contract, signed packet, broker base class) is bundled into this
  package, code and types, so it installs no other `@simple-discovery/*` package.

### Added
- First release: discovery over NATS on `simple-discovery.<namespace>`, signed like every
  simple-discovery transport. Late joiners learn existing nodes through `hello`; reconnects forever by
  default and says hello again after each reconnect. Accepts an existing `NatsConnection`.
