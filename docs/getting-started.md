# Getting Started

This guide walks you through installing, configuring, and running the Kafka Partition Remapper.

## What is the Kafka Partition Remapper?

The Kafka Partition Remapper is a transparent proxy that presents **virtual partitions** to Kafka clients while using fewer **physical partitions** on the actual Kafka cluster. This enables significant cost savings on managed Kafka services that charge per partition.

**Example:** A 10:1 compression ratio means clients see 1000 partitions while the cluster only has 100, reducing costs by 90%.

## Prerequisites

- Rust 1.75+ (for building from source)
- A running Kafka cluster
- (Optional) Docker for containerized deployment

## Installation

### From Binary Releases

Download the latest release for your platform from the [releases page](https://github.com/your-org/kafka-partition-remapper/releases):

```bash
# Linux (x86_64)
curl -LO https://github.com/your-org/kafka-partition-remapper/releases/latest/download/kafka-partition-proxy-linux-amd64.tar.gz
tar xzf kafka-partition-proxy-linux-amd64.tar.gz
sudo mv kafka-partition-proxy /usr/local/bin/

# macOS (Apple Silicon)
curl -LO https://github.com/your-org/kafka-partition-remapper/releases/latest/download/kafka-partition-proxy-darwin-arm64.tar.gz
tar xzf kafka-partition-proxy-darwin-arm64.tar.gz
sudo mv kafka-partition-proxy /usr/local/bin/
```

### From Source

```bash
# Clone the repository
git clone https://github.com/your-org/kafka-partition-remapper.git
cd kafka-partition-remapper

# Build release binary
cargo build --release

# Binary is at ./target/release/kafka-partition-proxy
./target/release/kafka-partition-proxy --version
```

### Using Docker

```bash
# Pull the image
docker pull ghcr.io/your-org/kafka-partition-proxy:latest

# Run with a config file
docker run -v /path/to/config.yaml:/config.yaml \
  -p 9092:9092 \
  ghcr.io/your-org/kafka-partition-proxy:latest \
  -c /config.yaml
```

## Quick Start

### 1. Create a Configuration File

Create `config.yaml`:

```yaml
listen:
  address: "0.0.0.0:9092"

kafka:
  bootstrap_servers:
    - "your-kafka-broker:9092"

mapping:
  virtual_partitions: 100
  physical_partitions: 10
```

This configuration:
- Listens on port 9092 for client connections
- Connects to your Kafka broker
- Presents 100 virtual partitions for every 10 physical partitions (10:1 ratio)

### 2. Ensure Your Topic Exists

The topic must exist on Kafka with the physical partition count:

```bash
# Create topic with 10 partitions
kafka-topics.sh --bootstrap-server your-kafka-broker:9092 \
  --create --topic my-topic \
  --partitions 10 \
  --replication-factor 1
```

### 3. Start the Proxy

```bash
kafka-partition-proxy -c config.yaml
```

You should see:
```
INFO Starting Kafka Partition Remapper v0.5.4
INFO Listening on 0.0.0.0:9092
INFO Connected to Kafka cluster
INFO Mapping: 100 virtual -> 10 physical partitions (10:1)
```

### 4. Connect Your Clients

Point your Kafka clients to the proxy instead of the broker:

```python
# Python example with kafka-python
from kafka import KafkaProducer, KafkaConsumer

# Producer - connect to proxy
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Produce to virtual partition 50
producer.send('my-topic', key=b'key', value=b'value', partition=50)

# Consumer - connect to proxy
consumer = KafkaConsumer(
    'my-topic',
    bootstrap_servers=['localhost:9092'],
    group_id='my-group'
)

# Consumer sees 100 partitions and gets assigned some
for message in consumer:
    print(f"Partition: {message.partition}, Offset: {message.offset}")
```

## Understanding Partition Mapping

### How It Works

```
Virtual Partitions (what clients see):
  0   1   2   3   4   5   6   7   8   9  10  11 ... 99
  │   │   │   │   │   │   │   │   │   │   │   │      │
  └───┼───┼───┼───┼───┼───┼───┼───┼───┼───┘   │      │
      │   │   │   │   │   │   │   │   │       │      │
      └───┼───┼───┼───┼───┼───┼───┼───┼───────┘      │
          └───┴───┴───┴───┴───┴───┴───┴──────────────┘
                          │
                          ▼
Physical Partitions (on broker):
  0   1   2   3   4   5   6   7   8   9
```

- Virtual partition 0 → Physical partition 0
- Virtual partition 10 → Physical partition 0
- Virtual partition 50 → Physical partition 0
- Virtual partition 99 → Physical partition 9

### Offset Mapping

Each virtual partition gets a dedicated offset range within the physical partition:

```
Physical Partition 0 offset space:
├── VP 0:  offsets 0 to 1,099,511,627,775
├── VP 10: offsets 1,099,511,627,776 to 2,199,023,255,551
├── VP 20: offsets 2,199,023,255,552 to 3,298,534,883,327
└── ...
```

Clients see clean offsets (0, 1, 2, ...) in their virtual partitions.

## Configuration Examples

### Development (No Security)

```yaml
listen:
  address: "0.0.0.0:9092"

kafka:
  bootstrap_servers:
    - "localhost:9092"

mapping:
  virtual_partitions: 100
  physical_partitions: 10

logging:
  level: debug
  format: text
```

### Production with SASL/TLS

```yaml
listen:
  address: "0.0.0.0:9093"
  advertised_address: "kafka-proxy.example.com:9093"
  max_connections: 5000
  security:
    protocol: SASL_SSL
    tls:
      cert_path: "/etc/certs/server.crt"
      key_path: "/etc/certs/server.key"
    sasl:
      enabled_mechanisms:
        - SCRAM-SHA-256
      credentials:
        users:
          app-user: "${APP_USER_PASSWORD}"
          admin-user: "${ADMIN_USER_PASSWORD}"

kafka:
  bootstrap_servers:
    - "broker1.internal:9093"
    - "broker2.internal:9093"
  security_protocol: SASL_SSL
  tls:
    ca_cert_path: "/etc/certs/ca.crt"
  sasl:
    mechanism: SCRAM-SHA-256
    username: "proxy-service"
    password: "${KAFKA_PASSWORD}"

mapping:
  virtual_partitions: 1000
  physical_partitions: 100

metrics:
  enabled: true
  address: "0.0.0.0:9090"

logging:
  level: info
  format: json
```

### Per-Topic Configuration

```yaml
listen:
  address: "0.0.0.0:9092"

kafka:
  bootstrap_servers:
    - "localhost:9092"

mapping:
  # Default: 10:1 ratio
  virtual_partitions: 100
  physical_partitions: 10

  topics:
    # High-volume topic: 50:1 ratio
    events:
      virtual_partitions: 500
      physical_partitions: 10

    # Low-volume config topic: no remapping
    config:
      virtual_partitions: 3
      physical_partitions: 3

    # Pattern matching: all audit.* topics
    "audit\\..*":
      virtual_partitions: 50
      physical_partitions: 5
```

## Verifying the Setup

### Check Metadata

Use kafka-metadata or any client to verify virtual partitions:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic my-topic
```

You should see 100 partitions (virtual) instead of 10 (physical).

### Check Metrics

If metrics are enabled:

```bash
curl http://localhost:9090/metrics
```

Key metrics:
- `kafka_proxy_connections_active` - Current connections
- `kafka_proxy_requests_total` - Requests by API type
- `kafka_proxy_remapping_operations_total` - Remapping operations

### Debug Logging

For troubleshooting, enable debug logging:

```yaml
logging:
  level: debug
```

Or via command line:

```bash
kafka-partition-proxy -c config.yaml -v   # debug
kafka-partition-proxy -c config.yaml -vv  # trace
```

## Common Use Cases

### Cost Optimization on AWS MSK

MSK charges per partition-hour. With 10:1 remapping:

| Scenario | Physical Partitions | Virtual Partitions | Cost Reduction |
|----------|--------------------|--------------------|----------------|
| Before | 1000 | 1000 | - |
| After | 100 | 1000 | ~90% |

### Consumer Parallelism

Need 100 concurrent consumers but only have 10 partitions?

```yaml
mapping:
  virtual_partitions: 100
  physical_partitions: 10
```

Now 100 consumers can each get their own virtual partition.

### Gradual Migration

Start with low compression and increase:

```yaml
# Week 1: 2:1 ratio (safe)
mapping:
  virtual_partitions: 20
  physical_partitions: 10

# Week 2: 5:1 ratio
mapping:
  virtual_partitions: 50
  physical_partitions: 10

# Week 3: 10:1 ratio (target)
mapping:
  virtual_partitions: 100
  physical_partitions: 10
```

## Troubleshooting

### Connection Refused

```
Error: Connection refused (os error 111)
```

**Fix:** Verify Kafka broker is running and accessible:
```bash
nc -zv your-broker 9092
```

### Authentication Failed

```
Error: SASL authentication failed
```

**Fix:**
1. Verify credentials in config
2. Check mechanism is enabled on broker
3. Enable debug logging to see handshake details

### Partition Out of Range

```
Error: Virtual partition 150 out of range (max: 99)
```

**Fix:** Client is requesting a partition beyond configured virtual count. Check `virtual_partitions` setting.

### Offset Overflow

```
Error: Offset overflow for partition 50
```

**Fix:** Virtual partition has exceeded `offset_range`. Increase `offset_range` in config or compact the topic.

## Next Steps

- [Architecture Documentation](./architecture.md) - Understand the system design
- [Configuration Reference](./configuration.md) - All configuration options
- [Authentication Guide](./authentication.md) - Secure your deployment
- [Protocol Mapping](./protocol-mapping.md) - Deep dive into remapping algorithms

## Getting Help

- GitHub Issues: [Report bugs or request features](https://github.com/your-org/kafka-partition-remapper/issues)
- Discussions: [Ask questions](https://github.com/your-org/kafka-partition-remapper/discussions)
