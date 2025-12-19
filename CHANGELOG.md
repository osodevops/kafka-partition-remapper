# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Authentication and principal extraction support
- Example configurations for various deployment scenarios

### Fixed
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

[Unreleased]: https://github.com/osodevops/kafka-partition-remapper/compare/v0.5.4...HEAD
[0.5.4]: https://github.com/osodevops/kafka-partition-remapper/compare/v0.5.3...v0.5.4
[0.5.3]: https://github.com/osodevops/kafka-partition-remapper/compare/v0.5.2...v0.5.3
[0.5.2]: https://github.com/osodevops/kafka-partition-remapper/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/osodevops/kafka-partition-remapper/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/osodevops/kafka-partition-remapper/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/osodevops/kafka-partition-remapper/releases/tag/v0.4.0
