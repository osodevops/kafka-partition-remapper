<p align="center">
  <h1 align="center">kafka-partition-remapper</h1>
  <p align="center">
    Reduce managed Kafka costs by remapping virtual partitions to fewer physical partitions
  </p>
</p>

<p align="center">
  <a href="https://github.com/osodevops/kafka-partition-remapper/actions/workflows/test.yml">
    <img src="https://github.com/osodevops/kafka-partition-remapper/actions/workflows/test.yml/badge.svg" alt="CI Status">
  </a>
  <a href="https://github.com/osodevops/kafka-partition-remapper/blob/main/LICENSE">
    <img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License: MIT">
  </a>
  <a href="https://github.com/osodevops/kafka-partition-remapper/releases">
    <img src="https://img.shields.io/github/v/release/osodevops/kafka-partition-remapper" alt="Release">
  </a>
</p>

---

**kafka-partition-remapper** is a high-performance TCP proxy written in Rust that sits between Kafka clients and managed Kafka clusters (like Confluent Cloud). It enables applications to use many virtual partitions while the actual Kafka cluster uses fewer physical partitions, reducing per-partition costs on managed services.

## The Problem

Managed Kafka services like Confluent Cloud charge per partition. High-cardinality use cases (multi-tenant SaaS, IoT, event sourcing) often need thousands of logical partitions for proper data isolation, but this becomes prohibitively expensive.

## The Solution

This proxy transparently remaps virtual partitions to physical partitions:

```
Client sees:     1000 virtual partitions  ($$$$$)
Kafka has:       100 physical partitions  ($)
Compression:     10:1 cost reduction
```

Clients connect to the proxy instead of Kafka directly. The proxy intercepts Kafka protocol messages and translates partition IDs and offsets transparently.

## Features

- **Transparent remapping** — No client code changes required
- **High performance** — Written in async Rust with Tokio
- **Low latency** — Target ≤1-3ms additional p99 latency
- **Stateless** — Horizontally scalable, no coordination needed
- **Observable** — Prometheus metrics built-in
- **Confluent Cloud compatible** — Tested with Kafka 3.6+

## How It Works

```
┌─────────────┐     ┌─────────────────────┐     ┌─────────────┐
│   Client    │────▶│  Partition Proxy    │────▶│    Kafka    │
│ (v.part 42) │     │  42 → phys.part 2   │     │  (10 parts) │
└─────────────┘     └─────────────────────┘     └─────────────┘
```

**Partition Mapping:**
```
physical_partition = virtual_partition % physical_partitions
```

**Offset Translation:**
```
physical_offset = (virtual_group * offset_range) + virtual_offset
```

Each virtual partition group gets its own offset space (default: 2^40 offsets), ensuring isolation.

## Installation

### From Source

```bash
git clone https://github.com/osodevops/kafka-partition-remapper.git
cd kafka-partition-remapper
cargo build --release
./target/release/kafka-partition-proxy --help
```

### Docker

```bash
docker pull osodevops/kafka-partition-remapper
docker run --rm -v /path/to/config:/config osodevops/kafka-partition-remapper --config /config/config.yaml
```

## Quick Start

### 1. Create Configuration

```yaml
listen:
  address: "0.0.0.0:9092"

kafka:
  bootstrap_servers:
    - "your-kafka:9092"

mapping:
  virtual_partitions: 100
  physical_partitions: 10

metrics:
  enabled: true
  address: "0.0.0.0:9090"
```

### 2. Start the Proxy

```bash
kafka-partition-proxy --config config.yaml
```

### 3. Connect Clients

Point your Kafka clients to the proxy instead of Kafka:

```bash
# Before (direct to Kafka)
kafka-console-producer --bootstrap-server kafka:9092 --topic my-topic

# After (through proxy)
kafka-console-producer --bootstrap-server proxy:9092 --topic my-topic
```

### Docker Compose

```bash
cd docker
docker compose up -d
```

See the [Quickstart Guide](docs/quickstart.md) for detailed instructions.

## Configuration

| Section | Option | Description | Default |
|---------|--------|-------------|---------|
| `listen` | `address` | Proxy listen address | `0.0.0.0:9092` |
| `listen` | `max_connections` | Max client connections | `1000` |
| `kafka` | `bootstrap_servers` | Kafka broker addresses | Required |
| `kafka` | `connection_timeout_ms` | Connection timeout | `10000` |
| `kafka` | `request_timeout_ms` | Request timeout | `30000` |
| `mapping` | `virtual_partitions` | Virtual partition count | Required |
| `mapping` | `physical_partitions` | Physical partition count | Required |
| `mapping` | `offset_range` | Offset space per virtual group | `2^40` |
| `metrics` | `enabled` | Enable Prometheus metrics | `true` |
| `metrics` | `address` | Metrics server address | `0.0.0.0:9090` |
| `logging` | `level` | Log level | `info` |
| `logging` | `json` | JSON log format | `false` |

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `kafka_proxy_requests_total` | Counter | Requests by API key |
| `kafka_proxy_request_duration_seconds` | Histogram | Request latency |
| `kafka_proxy_active_client_connections` | Gauge | Client connections |
| `kafka_proxy_partitions_remapped_total` | Counter | Remapped partitions |
| `kafka_proxy_bytes_received_total` | Counter | Bytes from clients |
| `kafka_proxy_bytes_sent_total` | Counter | Bytes to clients |

## Supported Kafka APIs

| API | Support | Notes |
|-----|---------|-------|
| ApiVersions | Full | Returns supported versions |
| Metadata | Full | Virtualizes partition count |
| Produce | Full | Remaps partitions and offsets |
| Fetch | Full | Remaps partitions and filters by offset range |
| Others | Passthrough | Forwarded without modification |

## Performance

| Metric | Target |
|--------|--------|
| Additional latency | ≤1-3ms p99 |
| Throughput | ≥80-90% of direct Kafka |
| Memory usage | <500MB typical |

## Limitations (MVP)

- **Global mapping only** — Same V:P ratio for all topics
- **PLAINTEXT only** — No SASL/TLS passthrough (deploy in VPC)
- **Kafka 3.6+** — Tested with Confluent Cloud
- **Fixed partition counts** — Topics must have exactly `physical_partitions` partitions

## Project Structure

```
kafka-partition-remapper/
├── crates/
│   ├── kafka-remapper-core/     # Core library
│   │   ├── src/
│   │   │   ├── config.rs        # Configuration
│   │   │   ├── error.rs         # Error types
│   │   │   ├── remapper/        # Partition mapping logic
│   │   │   ├── network/         # TCP listener, codec
│   │   │   ├── broker/          # Broker connections
│   │   │   ├── handlers/        # Protocol handlers
│   │   │   └── metrics/         # Prometheus metrics
│   │   └── tests/
│   └── kafka-remapper-cli/      # CLI binary
├── config/                      # Example configs
├── docker/                      # Docker setup
└── docs/                        # Documentation
```

## Building

```bash
# Build
cargo build --release

# Test
cargo test

# Run with debug logging
RUST_LOG=debug cargo run -p kafka-remapper-cli -- --config config.yaml
```

## Contributing

Contributions are welcome! Please read our contributing guidelines before submitting a PR.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

kafka-partition-remapper is licensed under the [MIT License](LICENSE).

---

<p align="center">
  Made with Rust by <a href="https://oso.sh">OSO</a>
</p>
