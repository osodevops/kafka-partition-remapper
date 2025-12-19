# kafka-remapper-core

[![Crates.io](https://img.shields.io/crates/v/kafka-remapper-core.svg)](https://crates.io/crates/kafka-remapper-core)
[![Documentation](https://docs.rs/kafka-remapper-core/badge.svg)](https://docs.rs/kafka-remapper-core)
[![License](https://img.shields.io/crates/l/kafka-remapper-core.svg)](https://github.com/osodevops/kafka-partition-remapper/blob/main/LICENSE)

Core library for building Kafka partition remapping proxies. This library provides the foundational components for transparently remapping virtual Kafka partitions to physical partitions.

## Features

- **Partition Remapping**: Map N virtual partitions to M physical partitions with automatic offset translation
- **Protocol Handling**: Full Kafka protocol support via `kafka-protocol` crate
- **TLS Support**: Client and server TLS with `rustls` (mTLS supported)
- **SASL Authentication**: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER
- **Metrics**: Built-in Prometheus metrics collection
- **Async Runtime**: Built on `tokio` for high-performance async I/O

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
kafka-remapper-core = "0.5"
tokio = { version = "1", features = ["full"] }
```

## Usage

### Configuration

```rust
use kafka_remapper_core::config::ProxyConfig;

// Load configuration from YAML file
let config = ProxyConfig::from_file("config.yaml")?;

// Or parse from string
let yaml = r#"
listen:
  address: "0.0.0.0:9092"
kafka:
  bootstrap_servers:
    - "kafka:9092"
mapping:
  virtual_partitions: 100
  physical_partitions: 10
"#;
let config = ProxyConfig::from_str(yaml)?;
```

### Partition Remapping

```rust
use kafka_remapper_core::config::MappingConfig;
use kafka_remapper_core::remapper::PartitionRemapper;

let config = MappingConfig {
    virtual_partitions: 100,
    physical_partitions: 10,
    offset_range: 1 << 40,
    topics: Default::default(),
};

let remapper = PartitionRemapper::new(&config);

// Map virtual partition 42 to physical
let mapping = remapper.virtual_to_physical(42)?;
println!("Physical partition: {}", mapping.physical_partition);  // 2
println!("Virtual group: {}", mapping.virtual_group);            // 4

// Map with offset translation
let offset_mapping = remapper.virtual_to_physical_offset(42, 1000)?;
println!("Physical offset: {}", offset_mapping.physical_offset);
```

### Broker Connection Pool

```rust
use kafka_remapper_core::broker::BrokerPool;
use kafka_remapper_core::config::KafkaConfig;
use std::sync::Arc;

let kafka_config = KafkaConfig {
    bootstrap_servers: vec!["localhost:9092".to_string()],
    ..Default::default()
};

let pool = Arc::new(BrokerPool::new(kafka_config));
pool.connect().await?;

// Get connection to specific broker
let conn = pool.get_connection(0).await?;
```

## Main Components

| Module | Description |
|--------|-------------|
| `config` | Configuration types (`ProxyConfig`, `MappingConfig`, etc.) |
| `error` | Error types (`ProxyError`, `RemapError`, `AuthError`, etc.) |
| `remapper` | Core partition/offset mapping logic |
| `broker` | Kafka broker connection pool and metadata management |
| `network` | TCP listener and Kafka protocol codec |
| `handlers` | Kafka protocol request handlers |
| `auth` | SASL authentication and principal extraction |
| `tls` | TLS client/server configuration |
| `metrics` | Prometheus metrics collection |

## Feature Flags

- `testing` - Enables test utilities (`MockBroker`, `ProxyTestHarness`)
- `oauthbearer-jwt` - Enables JWT validation for OAUTHBEARER authentication

## Documentation

- [API Documentation](https://docs.rs/kafka-remapper-core)
- [GitHub Repository](https://github.com/osodevops/kafka-partition-remapper)
- [Configuration Guide](https://github.com/osodevops/kafka-partition-remapper/blob/main/docs/configuration.md)
- [Architecture](https://github.com/osodevops/kafka-partition-remapper/blob/main/docs/architecture.md)

## CLI Tool

For a ready-to-use proxy binary, see the [kafka-partition-proxy](https://github.com/osodevops/kafka-partition-remapper) CLI tool:

```bash
# Install via Homebrew
brew install osodevops/tap/kafka-partition-proxy

# Or download from releases
curl -LsSf https://github.com/osodevops/kafka-partition-remapper/releases/latest/download/kafka-remapper-cli-installer.sh | sh
```

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](https://github.com/osodevops/kafka-partition-remapper/blob/main/LICENSE) for details.
