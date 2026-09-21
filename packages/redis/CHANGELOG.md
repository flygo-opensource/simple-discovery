# Changelog

## 3.0.0

### Changed
- Self-contained: the shared core (contract, signed packet, broker base class) is bundled into this
  package, code and types, so it installs no other `@simple-discovery/*` package.

### Added
- First release: discovery over Redis pub/sub on `simple-discovery:<namespace>`, signed like every
  simple-discovery transport. Late joiners learn existing nodes through `hello`; after ioredis
  reconnects, discovery resubscribes and says hello again. Accepts an existing ioredis client.
