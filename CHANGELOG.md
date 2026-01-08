# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.6] - 2026-01-08

### Fixed
- Protocol version negotiation causing client disconnections with "broker does not support METADATA" error
- Request/response header encoding for Kafka flexible protocol versions (v9+ Metadata/Produce, v12+ Fetch)
- Background metadata refresh "early eof" error caused by duplicate length prefix in request encoding
- Proper handler wiring in ConnectionHandler replacing stub implementations with real protocol handlers

### Changed
- Background metadata refresh now uses API v4 instead of v9 for broader broker compatibility

## [0.5.5] - 2025-12-19

### Added
- CHANGELOG.md following Keep a Changelog format
- CLAUDE.md AI/contributor guidance document
- Core library README for crates.io publishing
- crates.io publishing in release workflow
- Scoop publishing for Windows package manager
- Comprehensive Installation section in README (Homebrew, Shell, PowerShell, Scoop, Docker)
- "Try It Yourself" section with example configurations
- Comparison table with Kroxylicious, Conduktor Gateway, and kafka-proxy
- Documentation table linking to all guides

### Changed
- Updated comparison table to accurately reflect Conduktor's Concentrated Topics feature
- License badge updated to Apache-2.0

### Fixed
- E2E test target name in workflow (e2e → e2e_tests)
- Benchmark command in workflow (added --bench partition_remapping flag)
- CI test failures for formatting and clippy lints

## [0.5.4] - 2025-12-10

### Fixed
- Update Rust version to 1.83 for crc crate compatibility

## [0.5.3] - 2025-12-10

### Fixed
- Update Dockerfile to handle bench files for dependency caching

## [0.5.2] - 2025-12-10

### Fixed
- Track Cargo.lock for reproducible builds and Docker

## [0.5.1] - 2025-12-10

### Fixed
- Enable all features in compilation check
- Resolve all clippy warnings
- Regenerate release.yml with cargo-dist and add homepage

## [0.5.0] - 2025-12-09

### Added
- TLS/SASL authentication and CI workflows
- cargo-dist configuration for multi-platform releases
- Homebrew formula publishing
- Docker image publishing to Docker Hub
- Comprehensive CI pipeline (unit tests, integration tests, E2E tests)
- Code coverage reporting via Codecov
- Security audit with cargo-audit
- Semantic version checking for API changes

### Changed
- Enhanced README with SEO-optimized content

### Fixed
- Apply formatting and cargo-dist configuration

## [0.4.0] - 2025-12-09

Initial public release.

### Added
- Kafka partition remapping proxy core functionality
- Protocol-level partition translation (transparent to clients)
- Offset translation for consumer groups
- Support for key Kafka APIs (Produce, Fetch, Metadata, etc.)
- YAML-based configuration
- Prometheus metrics endpoint
- Docker support with multi-stage builds
- Comprehensive documentation

[Unreleased]: https://github.com/osodevops/kafka-partition-remapper/compare/v0.5.5...HEAD
[0.5.5]: https://github.com/osodevops/kafka-partition-remapper/compare/v0.5.4...v0.5.5
[0.5.4]: https://github.com/osodevops/kafka-partition-remapper/compare/v0.5.3...v0.5.4
[0.5.3]: https://github.com/osodevops/kafka-partition-remapper/compare/v0.5.2...v0.5.3
[0.5.2]: https://github.com/osodevops/kafka-partition-remapper/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/osodevops/kafka-partition-remapper/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/osodevops/kafka-partition-remapper/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/osodevops/kafka-partition-remapper/releases/tag/v0.4.0
