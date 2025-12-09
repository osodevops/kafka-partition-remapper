# Kafka Partition Remapper Quickstart Guide

Get started with the Kafka Partition Remapping Proxy in 5 minutes. This guide covers installation, configuration, and running the proxy.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Running the Proxy](#running-the-proxy)
5. [Docker Compose Setup](#docker-compose-setup)
6. [Verifying It Works](#verifying-it-works)
7. [Metrics](#metrics)
8. [Next Steps](#next-steps)

---

## Prerequisites

- **Rust 1.75+** (for building from source)
- **Apache Kafka** cluster accessible (or use Docker Compose)
- **Docker** (optional, for containerized deployment)

---

## Installation

### Option 1: Build from Source

```bash
# Clone the repository
git clone https://github.com/osodevops/kafka-partition-remapper.git
cd kafka-partition-remapper

# Build in release mode
cargo build --release

# Binary is at target/release/kafka-partition-proxy
./target/release/kafka-partition-proxy --help
```

### Option 2: Install with Cargo

```bash
cargo install --path crates/kafka-remapper-cli

# Now available in your PATH
kafka-partition-proxy --help
```

### Option 3: Docker

```bash
docker build -t kafka-partition-proxy -f docker/Dockerfile .
docker run --rm kafka-partition-proxy --help
```

### Verify Installation

```bash
kafka-partition-proxy --version
# kafka-partition-proxy 0.1.0
```

---

## Configuration

Create a configuration file `config.yaml`:

```yaml
listen:
  address: "0.0.0.0:9092"
  max_connections: 1000

kafka:
  bootstrap_servers:
    - "your-kafka-broker:9092"
  connection_timeout_ms: 10000
  request_timeout_ms: 30000

mapping:
  virtual_partitions: 100   # Partitions clients see
  physical_partitions: 10   # Actual Kafka partitions
  offset_range: 1099511627776  # 2^40 (default)

metrics:
  enabled: true
  address: "0.0.0.0:9090"

logging:
  level: "info"
  json: false
```

### Understanding the Mapping

The proxy compresses partitions using a simple formula:

```
physical_partition = virtual_partition % physical_partitions
```

**Example with 100 virtual → 10 physical (10:1 compression):**

| Virtual Partition | Physical Partition | Virtual Group |
|-------------------|-------------------|---------------|
| 0                 | 0                 | 0             |
| 1                 | 1                 | 0             |
| 10                | 0                 | 1             |
| 11                | 1                 | 1             |
| 99                | 9                 | 9             |

Offsets are translated to maintain isolation between virtual partitions sharing the same physical partition.

### Configuration Requirements

- `virtual_partitions` must be divisible by `physical_partitions`
- All topics use the same compression ratio (global mapping)
- Physical Kafka topics must have exactly `physical_partitions` partitions

---

## Running the Proxy

### Basic Usage

```bash
kafka-partition-proxy --config config.yaml
```

**Expected Output:**

```
INFO starting kafka partition proxy version=0.1.0 listen=0.0.0.0:9092 virtual_partitions=100 physical_partitions=10 compression_ratio=10
INFO connecting to kafka cluster bootstrap_servers=["your-kafka-broker:9092"]
INFO connected to kafka cluster
INFO metrics server started address=0.0.0.0:9090
```

### CLI Options

```bash
kafka-partition-proxy --help

Options:
  -c, --config <CONFIG>  Path to configuration file [default: config.yaml]
      --listen <LISTEN>  Override listen address
  -v, --verbose...       Increase logging verbosity (-v for debug, -vv for trace)
  -h, --help             Print help
  -V, --version          Print version
```

### Enable Debug Logging

```bash
# Via CLI flag
kafka-partition-proxy --config config.yaml -v

# Via environment variable
RUST_LOG=debug kafka-partition-proxy --config config.yaml
```

---

## Docker Compose Setup

The easiest way to try the proxy locally is with Docker Compose.

### Step 1: Start the Stack

```bash
cd docker

# Pull the Kafka image
docker pull confluentinc/cp-server:latest

# Start Kafka + Proxy
docker compose up -d
```

### Step 2: Verify Services

```bash
docker compose ps
```

**Expected Output:**

```
NAME          STATUS         PORTS
kafka         Up (healthy)   0.0.0.0:29092->29092/tcp
kafka-proxy   Up             0.0.0.0:19092->9092/tcp, 0.0.0.0:9090->9090/tcp
```

### Step 3: Connect Clients to the Proxy

Point your Kafka clients to the proxy instead of Kafka directly:

```bash
# Direct to Kafka (bypassing proxy)
kafka-console-producer --bootstrap-server localhost:29092 --topic test

# Through the proxy (with partition remapping)
kafka-console-producer --bootstrap-server localhost:19092 --topic test
```

### Enable Monitoring (Optional)

```bash
# Start with Prometheus
docker compose --profile monitoring up -d

# Access Prometheus at http://localhost:9091
```

### Stop the Stack

```bash
docker compose down
```

---

## Verifying It Works

### Step 1: Create a Test Topic

Create a topic with the physical partition count:

```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic test-topic \
  --partitions 10
```

### Step 2: Check Metadata Through Proxy

```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server kafka-proxy:9092 \
  --describe \
  --topic test-topic
```

The proxy should report 100 virtual partitions (if configured with 100 virtual → 10 physical).

### Step 3: Produce Messages

```bash
# Produce to virtual partition 0 (maps to physical 0)
echo "message-1" | docker exec -i kafka kafka-console-producer \
  --bootstrap-server kafka-proxy:9092 \
  --topic test-topic \
  --property "parse.key=false"

# Produce to virtual partition 15 (maps to physical 5)
echo "message-2" | docker exec -i kafka kafka-console-producer \
  --bootstrap-server kafka-proxy:9092 \
  --topic test-topic \
  --property "parse.key=false"
```

### Step 4: Consume Messages

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka-proxy:9092 \
  --topic test-topic \
  --from-beginning
```

---

## Metrics

The proxy exposes Prometheus metrics on the configured metrics port (default: 9090).

### Available Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `kafka_proxy_requests_total` | Counter | Total requests by API key |
| `kafka_proxy_request_duration_seconds` | Histogram | Request latency by API key |
| `kafka_proxy_active_client_connections` | Gauge | Current client connections |
| `kafka_proxy_active_broker_connections` | Gauge | Current broker connections |
| `kafka_proxy_partitions_remapped_total` | Counter | Partitions remapped |
| `kafka_proxy_bytes_received_total` | Counter | Bytes received from clients |
| `kafka_proxy_bytes_sent_total` | Counter | Bytes sent to clients |
| `kafka_proxy_errors_total` | Counter | Errors by type |

### Scraping Metrics

```bash
curl http://localhost:9090/
```

### Prometheus Configuration

```yaml
scrape_configs:
  - job_name: 'kafka-proxy'
    static_configs:
      - targets: ['kafka-proxy:9090']
```

---

## Next Steps

### 1. Configure for Production

For production deployments:

```yaml
listen:
  address: "0.0.0.0:9092"
  max_connections: 10000

kafka:
  bootstrap_servers:
    - "broker-1.kafka.svc:9092"
    - "broker-2.kafka.svc:9092"
    - "broker-3.kafka.svc:9092"

mapping:
  virtual_partitions: 1000
  physical_partitions: 100

logging:
  level: "info"
  json: true  # Structured logs for production
```

### 2. Set Up Monitoring

Deploy with Prometheus and Grafana for observability:

```bash
cd docker
docker compose --profile monitoring up -d
```

### 3. Horizontal Scaling

The proxy is stateless and can be horizontally scaled:

```yaml
# kubernetes deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-proxy
spec:
  replicas: 3
  # ...
```

### 4. Create Topics with Correct Partition Count

Ensure all topics have exactly `physical_partitions` partitions:

```bash
kafka-topics --bootstrap-server kafka:9092 \
  --create \
  --topic my-topic \
  --partitions 10 \
  --replication-factor 3
```

---

## Troubleshooting

### "Connection refused" to Kafka

1. Verify Kafka is running:
   ```bash
   kafka-broker-api-versions --bootstrap-server localhost:9092
   ```

2. Check bootstrap servers in config

3. Ensure network connectivity between proxy and Kafka

### "Invalid compression ratio"

The configuration validation failed. Ensure:
- `virtual_partitions % physical_partitions == 0`
- Both values are greater than 0

### High Latency

1. Check network latency between proxy and Kafka
2. Monitor `kafka_proxy_request_duration_seconds` histogram
3. Consider increasing `max_connections` if connection pooling is saturated

### Enable Debug Logging

```bash
RUST_LOG=debug kafka-partition-proxy --config config.yaml
```

---

## Getting Help

- **Documentation**: See the [docs](.) directory
- **Issues**: Report bugs on [GitHub](https://github.com/osodevops/kafka-partition-remapper/issues)
- **Discussions**: Ask questions on GitHub Discussions
