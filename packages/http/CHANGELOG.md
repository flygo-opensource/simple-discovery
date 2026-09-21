# Changelog

## 3.0.0 — stable generic discovery contract

### Breaking
- Package renamed from `@ohayo/http` (never published) to `@simple-discovery/http`; the repository is
  now `flygo-opensource/simple-discovery`.
- Environment variables renamed from `OHAYO_*` to `SIMPLE_DISCOVERY_*`: `SIMPLE_DISCOVERY_KEY`,
  `SIMPLE_DISCOVERY_PORT`, `SIMPLE_DISCOVERY_HTTP_DEBUG`. The old names are no longer read.
- The default bearer token changed from `'ohayo'` to `'simple-discovery'`. Set an explicit `key` in
  every deployment.

### Changed
- Self-contained: the shared contract is bundled into this package, code and types, so it installs no
  other `@simple-discovery/*` package.

### Added
- HTTP registry transport implementing the shared `Discovery<T>` contract: register, heartbeat,
  deregister, TTL expiry, and `DiscoveryOfflineData` notices.
