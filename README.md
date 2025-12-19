<p align="center">
  <h1 align="center">Kafka Partition Remapping Proxy</h1>
  <p align="center">
    A high-performance Kafka proxy that virtualizes partition counts, enabling cost reduction and seamless partition management
  </p>
</p>

<p align="center">
  <a href="https://github.com/osodevops/kafka-partition-remapper/actions/workflows/test.yml">
    <img src="https://github.com/osodevops/kafka-partition-remapper/actions/workflows/test.yml/badge.svg" alt="CI Status">
  </a>
  <a href="https://github.com/osodevops/kafka-partition-remapper/blob/main/LICENSE">
    <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License: Apache-2.0">
  </a>
  <a href="https://github.com/osodevops/kafka-partition-remapper/releases">
    <img src="https://img.shields.io/github/v/release/osodevops/kafka-partition-remapper" alt="Release">
  </a>
</p>

---

A lightweight, developer-friendly proxy for Apache Kafka that transparently remaps partitions between clients and brokers. Designed to make partition migrations, traffic rebalancing, and topology changes simple, safe, and zero-touch for your producers and consumers.

## Why Kafka Partition Remapping Proxy?

Running Kafka at scale often means dealing with partition constraints:

- **Cost pressure**: Managed Kafka services like Confluent Cloud charge per partition. High-cardinality use cases (multi-tenant SaaS, IoT, event sourcing) need thousands of logical partitions but this becomes prohibitively expensive.
- **Migration complexity**: Changing partition layouts requires complex coordination, custom tooling, or application changes.
- **Operational overhead**: Rebalancing hot partitions or evolving topology disrupts clients.

**Kafka Partition Remapping Proxy solves this** by sitting between your clients and your Kafka cluster, handling all partition remapping logic transparently. Producers and consumers connect to the proxy as if it were Kafka, while the proxy takes care of routing and remapping behind the scenes.

```
Client sees:     1000 virtual partitions  ($$$$$)
Kafka has:       100 physical partitions  ($)
Compression:     10:1 cost reduction
```

## Key Benefits

### Zero Code Changes
Keep your client config and application code untouched. Point them at the proxy and let it handle partition remapping and routing logic.

### Simple, Declarative Configuration
Define your partition remapping rules with a clean YAML configuration. No scripting or operational gymnastics required.

### Safe Migrations and Rebalancing
Move partitions, rebalance load, or change topic layouts with predictable behavior and minimal risk of client disruptions.

### Transparent to Clients
The proxy maintains Kafka semantics, so your existing tooling, metrics, and consumers continue to work as expected.

### High Performance
Written in async Rust with Tokio for minimal latency overhead (≤1-3ms p99) and high throughput.

### Optimized for Operations
Built to be easy to deploy, observe, and operate in modern Kubernetes and cloud environments. Prometheus metrics built-in.

## Use Cases

### Reduce Managed Kafka Costs
Use 1000 virtual partitions while paying for only 100 physical partitions. Achieve 10:1 cost reduction on partition-based pricing.

### Partition Migration Without Downtime
Move partitions between brokers or clusters without coordinating rolling client restarts or application releases.

### Load Rebalancing for Hot Partitions
Smooth out hot spots by transparently remapping partitions, while clients continue to write/read using the same partition IDs.

### Progressive Topology Changes
Evolve your Kafka topology over time with a proxy layer that decouples client expectations from physical partition layouts.

### Multi-Tenant Isolation
Give each tenant their own virtual partition space while efficiently sharing physical resources.

## Why Kafka Partition Remapper?

| Feature | Partition Remapper | MirrorMaker 2 | Custom Routing | TCP Load Balancer |
|---------|-------------------|---------------|----------------|-------------------|
| Zero code changes | ✅ | ❌ | ❌ | ✅ |
| Partition-level control | ✅ | ❌ | ✅ | ❌ |
| Offset translation | ✅ | ✅ | ❌ | ❌ |
| Protocol transparency | ✅ | ❌ | ❌ | ❌ |
| No additional infrastructure | ✅ | ❌ | ❌ | ❌ |
| Multi-tenant isolation | ✅ | ❌ | ✅ | ❌ |
| SASL/TLS support | ✅ | ✅ | ✅ | ✅ |

## How It Works

```
┌─────────────┐     ┌─────────────────────┐     ┌─────────────┐
│   Client    │────▶│  Partition Proxy    │────▶│    Kafka    │
│ (v.part 42) │     │  42 → phys.part 2   │     │  (10 parts) │
└─────────────┘     └─────────────────────┘     └─────────────┘
```

1. **Clients connect to the proxy** instead of directly to the Kafka cluster
2. **The proxy intercepts metadata and data traffic**
3. **Based on configuration**, it remaps logical partitions to physical partitions
4. **To the client**, everything still looks like a regular Kafka cluster

**Partition Mapping:**
```
physical_partition = virtual_partition % physical_partitions
```

**Offset Translation:**
```
physical_offset = (virtual_group * offset_range) + virtual_offset
```

Each virtual partition group gets its own offset space (default: 2^40 offsets), ensuring complete isolation.

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
# Using Docker
docker run --rm \
  -v /path/to/config.yaml:/config/config.yaml \
  osodevops/kafka-partition-remapper --config /config/config.yaml

# Or from source
cargo build --release
./target/release/kafka-partition-proxy --config config.yaml
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

## Try It Yourself

Get hands-on with example configurations in the [`config/`](./config/) directory:

| Configuration | Description |
|--------------|-------------|
| [`example.yaml`](config/example.yaml) | Basic configuration for getting started |
| [`development-local.yaml`](config/development-local.yaml) | Local development setup |
| [`sasl-ssl-confluent-cloud.yaml`](config/sasl-ssl-confluent-cloud.yaml) | Confluent Cloud with SASL/SSL |
| [`sasl-scram-internal.yaml`](config/sasl-scram-internal.yaml) | Internal Kafka with SCRAM authentication |
| [`mtls-enterprise.yaml`](config/mtls-enterprise.yaml) | Enterprise mTLS configuration |
| [`multi-tenant.yaml`](config/multi-tenant.yaml) | Multi-tenant isolation setup |
| [`high-throughput.yaml`](config/high-throughput.yaml) | Optimized for high throughput |

```bash
# Run with an example config
docker run -v $(pwd)/config:/config osodevops/kafka-partition-remapper \
  --config /config/example.yaml
```

## Installation

### macOS (Homebrew)

```bash
brew install osodevops/tap/kafka-partition-proxy
```

### Linux/macOS (Shell Installer)

```bash
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/osodevops/kafka-partition-remapper/releases/latest/download/kafka-remapper-cli-installer.sh | sh
```

### Windows (PowerShell Installer)

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://github.com/osodevops/kafka-partition-remapper/releases/latest/download/kafka-remapper-cli-installer.ps1 | iex"
```

### Windows (Scoop)

```powershell
scoop bucket add oso https://github.com/osodevops/scoop-bucket.git
scoop install kafka-partition-proxy
```

### Docker

```bash
docker pull osodevops/kafka-partition-remapper
docker run --rm osodevops/kafka-partition-remapper --help
```

### From Source

```bash
cargo install --git https://github.com/osodevops/kafka-partition-remapper kafka-remapper-cli
```

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

## Features

- **Transparent Kafka proxy** for producers and consumers
- **Partition-level remapping** with declarative configuration
- **Offset translation** maintaining consumer group semantics
- **High performance** async Rust with Tokio
- **Observable** with Prometheus metrics
- **Containerized** for Kubernetes deployments
- **Minimal dependencies** and small operational footprint

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
| OffsetCommit | Full | Translates virtual offsets to physical |
| OffsetFetch | Full | Translates physical offsets back to virtual |
| Others | Passthrough | Forwarded without modification |

## Performance

| Metric | Target |
|--------|--------|
| Additional latency | ≤1-3ms p99 |
| Throughput | ≥80-90% of direct Kafka |
| Memory usage | <500MB typical |

Benchmarks available via `cargo bench`.

## Documentation

| Document | Description |
|----------|-------------|
| [Quick Start](docs/quickstart.md) | Get running in 5 minutes |
| [Configuration Reference](docs/configuration.md) | Complete configuration options |
| [Architecture](docs/architecture.md) | System design and internals |
| [Authentication](docs/authentication.md) | TLS, SASL, mTLS setup guide |
| [Protocol Mapping](docs/protocol-mapping.md) | Kafka protocol implementation details |
| [Getting Started](docs/getting-started.md) | Detailed setup and deployment |

## Limitations

- **Global mapping by default** — Same V:P ratio for all topics (per-topic overrides available)
- **Kafka 3.6+** — Tested with Confluent Cloud and Apache Kafka 3.6+
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
│   │   │   ├── metrics/         # Prometheus metrics
│   │   │   └── testing/         # Test harness & mock broker
│   │   ├── tests/               # Integration tests
│   │   └── benches/             # Performance benchmarks
│   └── kafka-remapper-cli/      # CLI binary
├── config/                      # Example configs
├── docker/                      # Docker setup
└── docs/                        # Documentation
```

## Building

```bash
# Build
cargo build --release

# Test (131 tests)
cargo test --features testing

# Run benchmarks
cargo bench

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

Kafka Partition Remapping Proxy is licensed under the [Apache License 2.0](LICENSE).

---

## Summary

Kafka Partition Remapping Proxy is an easy-to-deploy Kafka proxy that simplifies partition management, reduces costs on managed Kafka services, and enables seamless topology changes without requiring changes to your client applications. By centralizing partition remapping logic in a lightweight Rust proxy, it helps Kafka platform teams deliver safer, faster infrastructure changes while keeping the developer experience simple.

---

<p align="center">
  Made with Rust by <a href="https://oso.sh">OSO</a>
</p>
