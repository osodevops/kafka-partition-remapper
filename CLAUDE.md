# CLAUDE.md

This document provides context for AI-assisted development on this project.

## Project Overview

**OSO Kafka Partition Remapping Proxy** is a high-performance TCP proxy that transparently remaps virtual Kafka partitions to physical partitions, enabling cost reduction on managed Kafka services like Confluent Cloud.

### Key Problems Solved

- **Cost Reduction**: Reduce partition costs by mapping many virtual partitions to fewer physical partitions
- **Zero Code Changes**: Clients connect through the proxy without modification
- **Transparent Protocol Handling**: Full Kafka protocol support with partition/offset translation
- **Flexible Authentication**: Support for PLAINTEXT, TLS, SASL/PLAIN, SASL/SCRAM, mTLS, and OAUTHBEARER

## Repository Structure

```
kafka-partition-remapper/
├── Cargo.toml              # Workspace root with shared dependencies
├── Cargo.lock              # Locked dependencies for reproducible builds
├── CHANGELOG.md            # Version history (Keep a Changelog format)
├── README.md               # Main documentation
├── crates/
│   ├── kafka-remapper-core/     # Core library (partition remapping, protocol handling)
│   │   ├── src/
│   │   │   ├── lib.rs           # Library entry point, re-exports
│   │   │   ├── config.rs        # Configuration types (ProxyConfig, etc.)
│   │   │   ├── error.rs         # Error types (ProxyError, RemapError, etc.)
│   │   │   ├── auth/            # Authentication (SASL, principal extraction)
│   │   │   ├── broker/          # Kafka broker connection pool
│   │   │   ├── handlers/        # Kafka protocol request handlers
│   │   │   ├── metrics/         # Prometheus metrics
│   │   │   ├── network/         # TCP listener, codec, connection handling
│   │   │   ├── remapper/        # Core partition/offset mapping logic
│   │   │   ├── testing/         # Test utilities (MockBroker, ProxyTestHarness)
│   │   │   └── tls/             # TLS client/server configuration
│   │   ├── tests/               # Integration tests
│   │   └── benches/             # Performance benchmarks (criterion)
│   └── kafka-remapper-cli/      # CLI binary wrapper
│       └── src/main.rs          # Entry point, CLI args, runtime setup
├── config/                      # Example configurations
│   ├── example.yaml             # Basic example
│   ├── sasl-ssl-confluent-cloud.yaml
│   ├── mtls-enterprise.yaml
│   └── ...
├── docker/
│   ├── Dockerfile               # Multi-stage build for container
│   ├── docker-compose.yml       # Local development environment
│   └── prometheus.yml           # Prometheus scrape config
└── docs/                        # Detailed documentation
    ├── architecture.md          # System design
    ├── authentication.md        # Auth mechanisms guide
    ├── configuration.md         # Configuration reference
    ├── protocol-mapping.md      # Kafka protocol implementation
    └── ...
```

## Build & Test Commands

```bash
# Build
cargo build                      # Debug build
cargo build --release            # Release build

# Test
cargo test                       # All tests
cargo test --lib                 # Library tests only
cargo test --test integration    # Integration tests (requires testing feature)
cargo test --all-features        # All tests with all features

# Lint & Format
cargo fmt --all --check          # Check formatting
cargo fmt --all                  # Apply formatting
cargo clippy --all-targets --all-features -- -D warnings

# Check compilation
cargo check --all-targets

# Run with example config
cargo run -- --config config/example.yaml

# Benchmarks
cargo bench --bench partition_remapping
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              Kafka Clients                               │
│                    (Producers, Consumers, Admin tools)                   │
└───────────────────────────────────┬─────────────────────────────────────┘
                                    │ Kafka Protocol (TCP/TLS)
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         Kafka Partition Proxy                            │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────────────────┐  │
│  │   Network   │  │    Auth      │  │         Handlers               │  │
│  │   Listener  │──│   (SASL/TLS) │──│  Metadata, Produce, Fetch,     │  │
│  │             │  │              │  │  OffsetCommit, OffsetFetch...  │  │
│  └─────────────┘  └──────────────┘  └────────────────────────────────┘  │
│                                              │                           │
│  ┌───────────────────────────────────────────┴────────────────────────┐  │
│  │                    Partition Remapper                               │  │
│  │   virtual_partition ←→ physical_partition                          │  │
│  │   virtual_offset ←→ physical_offset                                │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                              │                           │
│  ┌───────────────────────────────────────────┴────────────────────────┐  │
│  │                     Broker Pool                                     │  │
│  │   Connection management, metadata refresh, request routing         │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────┬─────────────────────────────────────┘
                                    │ Kafka Protocol (TCP/TLS/SASL)
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         Kafka Cluster                                    │
│                    (Fewer physical partitions)                           │
└─────────────────────────────────────────────────────────────────────────┘
```

## Key Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `kafka-protocol` | 0.17 | Kafka protocol encoding/decoding |
| `tokio` | 1.42 | Async runtime with full features |
| `tokio-rustls` | 0.26 | TLS support for async connections |
| `rustls` | 0.23 | Modern TLS implementation |
| `serde` / `serde_yaml` | 1.0 / 0.9 | Configuration parsing |
| `clap` | 4.5 | CLI argument parsing |
| `tracing` | 0.1 | Structured logging |
| `prometheus` | 0.13 | Metrics collection |
| `thiserror` | 2.0 | Error type derivation |
| `hyper` | 1.5 | HTTP server for metrics endpoint |
| `dashmap` | 6.1 | Concurrent hash maps |
| `x509-parser` | 0.16 | Certificate parsing for mTLS principal extraction |

## Core Patterns

### Error Handling

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProxyError {
    #[error("connection error: {0}")]
    Connection(#[from] std::io::Error),

    #[error("broker {broker_id} unavailable: {message}")]
    BrokerUnavailable { broker_id: i32, message: String },

    #[error("remapping error: {0}")]
    Remap(#[from] RemapError),
}

pub type Result<T> = std::result::Result<T, ProxyError>;
```

### Async Pattern

```rust
use tokio::sync::watch;
use std::sync::Arc;

// Shared state with Arc
let broker_pool = Arc::new(BrokerPool::new(config.kafka.clone()));

// Graceful shutdown with watch channel
let (shutdown_tx, shutdown_rx) = watch::channel(false);

// Spawn background tasks
tokio::spawn(async move {
    refresher.run().await;
});
```

### Configuration Loading

```rust
use crate::config::ProxyConfig;

// Load and validate from file
let config = ProxyConfig::from_file("config.yaml")?;

// Or from string (useful for tests)
let config = ProxyConfig::from_str(yaml_content)?;
```

## Key Types

### ProxyConfig (config.rs)

```rust
pub struct ProxyConfig {
    pub listen: ListenConfig,      // TCP listener settings
    pub kafka: KafkaConfig,        // Kafka cluster connection
    pub mapping: MappingConfig,    // Partition mapping rules
    pub metrics: MetricsConfig,    // Prometheus metrics
    pub logging: LoggingConfig,    // Log level and format
}

pub struct MappingConfig {
    pub virtual_partitions: u32,   // What clients see
    pub physical_partitions: u32,  // What Kafka has
    pub offset_range: u64,         // Offset space per virtual partition
    pub topics: HashMap<String, TopicMappingConfig>,  // Per-topic overrides
}
```

### PartitionRemapper (remapper/partition_remapper.rs)

```rust
pub struct PartitionRemapper {
    virtual_partitions: u32,
    physical_partitions: u32,
    compression_ratio: u32,
    offset_range: u64,
}

impl PartitionRemapper {
    pub fn virtual_to_physical(&self, virtual_partition: i32) -> RemapResult<PhysicalMapping>;
    pub fn physical_to_virtual(&self, physical_partition: i32, physical_offset: i64) -> RemapResult<VirtualMapping>;
}
```

### Error Types (error.rs)

```rust
pub enum ConfigError { ... }   // Configuration parsing/validation
pub enum ProxyError { ... }    // Runtime proxy errors
pub enum RemapError { ... }    // Partition/offset remapping errors
pub enum TlsError { ... }      // TLS configuration/handshake errors
pub enum AuthError { ... }     // SASL authentication errors
```

## CLI Commands

```bash
# Basic usage
kafka-partition-proxy --config config.yaml

# Override listen address
kafka-partition-proxy --config config.yaml --listen 0.0.0.0:19092

# Verbose logging
kafka-partition-proxy --config config.yaml -v      # debug
kafka-partition-proxy --config config.yaml -vv     # trace

# Help
kafka-partition-proxy --help
```

## Configuration Format

```yaml
# Listener configuration
listen:
  address: "0.0.0.0:9092"
  advertised_address: "proxy.example.com:9092"  # Optional
  max_connections: 1000
  security:  # Optional client-facing security
    protocol: SASL_SSL
    tls:
      cert_path: /etc/ssl/server.crt
      key_path: /etc/ssl/server.key
    sasl:
      enabled_mechanisms: [PLAIN, SCRAM-SHA-256]
      credentials:
        users:
          - username: client1
            password: "${CLIENT_PASSWORD}"

# Kafka cluster connection
kafka:
  bootstrap_servers:
    - "kafka-1:9092"
    - "kafka-2:9092"
  connection_timeout_ms: 10000
  request_timeout_ms: 30000
  metadata_refresh_interval_secs: 30
  security_protocol: SASL_SSL  # PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
  tls:
    ca_cert_path: /etc/ssl/ca.crt
  sasl:
    mechanism: SCRAM-SHA-256
    username: "${KAFKA_USERNAME}"
    password: "${KAFKA_PASSWORD}"

# Partition mapping
mapping:
  virtual_partitions: 100
  physical_partitions: 10
  offset_range: 1099511627776  # 2^40
  topics:  # Optional per-topic overrides
    high-throughput-topic:
      virtual_partitions: 200
      physical_partitions: 20

# Metrics (Prometheus)
metrics:
  enabled: true
  address: "0.0.0.0:9090"

# Logging
logging:
  level: info  # trace, debug, info, warn, error
  json: false
```

## Performance Targets

- **Throughput**: Line-rate forwarding (limited by network, not proxy)
- **Latency**: < 1ms p99 added latency
- **Memory**: < 100MB base + ~1KB per connection
- **Connections**: 10,000+ concurrent clients

## Important Design Decisions

1. **kafka-protocol crate**: Uses the native Rust Kafka protocol implementation for zero-copy parsing where possible
2. **tokio runtime**: Multi-threaded async for high concurrency
3. **rustls over OpenSSL**: Pure Rust TLS for easier deployment and security
4. **DashMap**: Lock-free concurrent maps for connection state
5. **Watch channels**: Efficient broadcast for shutdown signals
6. **Offset segmentation**: Each virtual partition gets a dedicated offset range to prevent collisions

## Development Notes

### Adding a New Kafka Protocol Handler

1. Create handler in `crates/kafka-remapper-core/src/handlers/`
2. Implement request parsing and response building using `kafka-protocol` types
3. Add partition/offset remapping logic using `PartitionRemapper`
4. Register in `handlers/mod.rs`

### Adding a New SASL Mechanism

1. Add variant to `SaslMechanism` enum in `config.rs`
2. Implement authenticator in `auth/sasl/`
3. Register in `SaslServer::new()` in `auth/mod.rs`

### Feature Flags

- `testing`: Enables `MockBroker` and `ProxyTestHarness` for integration tests
- `oauthbearer-jwt`: Enables JWT validation for OAUTHBEARER (requires `jsonwebtoken`)

## Testing

```bash
# Unit tests
cargo test --lib

# Integration tests (uses MockBroker)
cargo test --test integration --features testing

# All tests including slow ones
cargo test --all-features -- --include-ignored

# Run specific test
cargo test test_partition_remapping

# With logging
RUST_LOG=debug cargo test -- --nocapture
```

## Common Tasks

### Debug Connection Issues

```bash
# Run with trace logging
RUST_LOG=trace cargo run -- --config config.yaml

# Check Kafka connectivity
kafkacat -b localhost:9092 -L
```

### Inspect Partition Mapping

The proxy logs mapping operations at debug level:
```
DEBUG virtual_partition=42 physical_partition=2 virtual_group=4 "mapped partition"
```

### Validate Configuration

```bash
# Dry run - loads and validates config without starting
cargo run -- --config config.yaml --help  # Config is validated during load
```

## Documentation Reference

| Document | Purpose |
|----------|---------|
| [Quick Start](docs/quickstart.md) | Get running in 5 minutes |
| [Configuration](docs/configuration.md) | Complete configuration reference |
| [Architecture](docs/architecture.md) | System design and internals |
| [Authentication](docs/authentication.md) | TLS, SASL, mTLS setup guide |
| [Protocol Mapping](docs/protocol-mapping.md) | Kafka protocol implementation details |
| [Getting Started](docs/getting-started.md) | Detailed setup and deployment |
