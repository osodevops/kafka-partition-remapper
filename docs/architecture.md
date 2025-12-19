# Kafka Partition Remapper - Architecture Documentation

## Overview

The Kafka Partition Remapper is a transparent proxy that sits between Kafka clients and a Kafka cluster, providing **virtual partition expansion**. This enables significant cost savings on managed Kafka services by allowing clients to work with many virtual partitions while the underlying cluster uses fewer physical partitions.

```
┌──────────────┐     ┌─────────────────────┐     ┌──────────────────┐
│              │     │                     │     │                  │
│  Kafka       │────▶│  Partition Remapper │────▶│  Kafka Cluster   │
│  Clients     │◀────│  Proxy              │◀────│  (N partitions)  │
│              │     │                     │     │                  │
└──────────────┘     └─────────────────────┘     └──────────────────┘
   Sees M virtual         Translates              Has N physical
   partitions             M ←→ N                  partitions
```

## Core Concept: Virtual Partition Expansion

### The Problem

Managed Kafka services (AWS MSK, Confluent Cloud, etc.) charge per partition. Applications often need many partitions for:
- Parallelism in consumer groups
- Key-based partitioning for ordering guarantees
- Workload distribution

This creates a conflict between cost and scalability.

### The Solution

The remapper presents **virtual partitions** to clients while using fewer **physical partitions** on the broker:

| Configuration | Value | Example |
|---------------|-------|---------|
| Virtual Partitions (V) | What clients see | 100 |
| Physical Partitions (P) | What's on the broker | 10 |
| Compression Ratio (C) | V ÷ P | 10:1 |

A 10:1 compression ratio means 90% cost savings on partition-based pricing.

## Workspace Structure

```
kafka-partition-remapper/
├── Cargo.toml                         # Workspace manifest
├── crates/
│   ├── kafka-remapper-core/           # Core library
│   │   ├── src/
│   │   │   ├── lib.rs                 # Library entry point
│   │   │   ├── auth/                  # SASL authentication
│   │   │   │   ├── mod.rs
│   │   │   │   └── sasl/              # SASL mechanisms
│   │   │   │       ├── mod.rs         # SaslServer
│   │   │   │       ├── plain.rs       # PLAIN mechanism
│   │   │   │       ├── scram.rs       # SCRAM-SHA-256/512
│   │   │   │       ├── oauthbearer.rs # OAUTHBEARER
│   │   │   │       └── credentials.rs # Credential stores
│   │   │   ├── broker/                # Broker connection management
│   │   │   │   ├── mod.rs
│   │   │   │   └── connection.rs      # BrokerPool, BrokerConnection
│   │   │   ├── config.rs              # Configuration types
│   │   │   ├── error.rs               # Error types
│   │   │   ├── handlers/              # Protocol handlers
│   │   │   │   ├── mod.rs
│   │   │   │   ├── metadata.rs        # Metadata API
│   │   │   │   ├── produce.rs         # Produce API
│   │   │   │   ├── fetch.rs           # Fetch API
│   │   │   │   ├── offset_commit.rs   # OffsetCommit API
│   │   │   │   ├── offset_fetch.rs    # OffsetFetch API
│   │   │   │   └── sasl.rs            # SASL handshake
│   │   │   ├── metrics/               # Prometheus metrics
│   │   │   ├── network/               # Network layer
│   │   │   │   ├── mod.rs
│   │   │   │   ├── listener.rs        # ProxyListener
│   │   │   │   ├── connection.rs      # ConnectionHandler
│   │   │   │   ├── codec.rs           # KafkaCodec
│   │   │   │   └── client_stream.rs   # ClientStream
│   │   │   ├── remapper/              # Core remapping logic
│   │   │   │   ├── mod.rs
│   │   │   │   ├── partition_remapper.rs
│   │   │   │   └── topic_registry.rs
│   │   │   ├── testing/               # Test utilities
│   │   │   └── tls/                   # TLS support
│   │   │       ├── mod.rs
│   │   │       └── server.rs          # TlsServerAcceptor
│   │   └── tests/
│   │       ├── e2e_tests.rs
│   │       ├── metadata_tests.rs
│   │       └── per_topic_mapping_tests.rs
│   └── kafka-remapper-cli/            # CLI binary
│       └── src/main.rs
└── docs/
```

## Component Architecture

### High-Level Request Flow

```
                                    ┌─────────────────────────────────────────┐
                                    │           ProxyListener                 │
                                    │  - Accepts TCP connections              │
                                    │  - Optional TLS handshake               │
                                    │  - Connection limit enforcement         │
                                    └────────────────┬────────────────────────┘
                                                     │
                                                     ▼
                                    ┌─────────────────────────────────────────┐
                                    │         ConnectionHandler               │
                                    │  - Frames with KafkaCodec              │
                                    │  - SASL authentication                  │
                                    │  - Request dispatch                     │
                                    └────────────────┬────────────────────────┘
                                                     │
                    ┌────────────────────────────────┼────────────────────────────────┐
                    │                                │                                │
                    ▼                                ▼                                ▼
    ┌───────────────────────────┐  ┌───────────────────────────┐  ┌───────────────────────────┐
    │     MetadataHandler       │  │     ProduceHandler        │  │      FetchHandler         │
    │  - Rewrites broker addrs  │  │  - Maps V→P partitions    │  │  - Maps V→P partitions    │
    │  - Virtualizes partitions │  │  - Translates offsets     │  │  - Filters responses      │
    └───────────────────────────┘  └───────────────────────────┘  └───────────────────────────┘
                    │                                │                                │
                    └────────────────────────────────┼────────────────────────────────┘
                                                     │
                                                     ▼
                                    ┌─────────────────────────────────────────┐
                                    │            BrokerPool                   │
                                    │  - Connection management                │
                                    │  - Leader discovery                     │
                                    │  - Optional TLS/SASL to broker          │
                                    └────────────────┬────────────────────────┘
                                                     │
                                                     ▼
                                              Kafka Cluster
```

### Network Layer (`src/network/`)

#### ProxyListener

The main entry point for client connections:

```rust
pub struct ProxyListener {
    config: Arc<ProxyConfig>,
    shutdown_tx: broadcast::Sender<()>,
    active_connections: Arc<AtomicUsize>,
    tls_acceptor: Option<Arc<TlsServerAcceptor>>,
    sasl_server: Option<Arc<SaslServer>>,
}
```

Responsibilities:
- TCP socket binding (default `0.0.0.0:9092`)
- Connection limit enforcement (default 1000)
- TLS termination for SSL/SASL_SSL protocols
- Graceful shutdown propagation
- Connection lifecycle management

#### ConnectionHandler

Handles individual client connections:

```rust
pub struct ConnectionHandler {
    config: Arc<ProxyConfig>,
    shutdown_rx: broadcast::Receiver<()>,
    sasl_server: Option<Arc<SaslServer>>,
}
```

Responsibilities:
- Stream framing with `KafkaCodec`
- SASL authentication flow orchestration
- API request routing to handlers
- Request/response correlation

#### KafkaCodec

Wire protocol encoding/decoding:

```rust
pub struct KafkaCodec {
    max_frame_size: usize,  // Default: 100 MB
}
```

Frame format:
```
┌──────────────────┬──────────────────────────────────┐
│  Length (4 bytes)│  Message Body (N bytes)          │
│  Big-endian u32  │  [API Key][API Version][...]     │
└──────────────────┴──────────────────────────────────┘
```

#### ClientStream

Unified stream abstraction:

```rust
pub enum ClientStream {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}
```

- Implements `AsyncRead` and `AsyncWrite`
- Provides peer certificate extraction for mTLS principal mapping

### Broker Layer (`src/broker/`)

#### BrokerPool

Manages connections to Kafka brokers:

```rust
pub struct BrokerPool {
    connections: DashMap<i32, BrokerConnection>,
    bootstrap_servers: Vec<String>,
    config: Arc<ProxyConfig>,
    tls_connector: Option<TlsConnector>,
}
```

Responsibilities:
- Connection pooling by broker ID
- Automatic reconnection
- TLS/SASL establishment to brokers
- Metadata-based discovery

#### BrokerConnection

Individual broker connection:

```rust
pub struct BrokerConnection {
    stream: BrokerStream,
    broker_id: i32,
    address: String,
}
```

- Supports both plain TCP and TLS streams
- Request/response correlation by correlation ID
- Timeout handling

### Handler Layer (`src/handlers/`)

Each Kafka API has a dedicated handler implementing the `ProtocolHandler` trait:

```rust
#[async_trait]
pub trait ProtocolHandler: Send + Sync {
    async fn handle(&self, frame: &KafkaFrame) -> Result<BytesMut>;
}
```

| Handler | API Key | Remapping Logic |
|---------|---------|-----------------|
| `MetadataHandler` | 3 | Virtualizes partitions, rewrites broker addresses |
| `ProduceHandler` | 0 | Maps V→P partitions, translates response offsets |
| `FetchHandler` | 1 | Maps V→P partitions/offsets, filters response records |
| `OffsetCommitHandler` | 8 | Maps V→P partitions for committed offsets |
| `OffsetFetchHandler` | 9 | Maps V→P partitions in requests, P→V in responses |
| `SaslHandler` | 17, 36 | Authentication flow (no remapping) |
| `PassthroughHandler` | Various | Direct forwarding (group coordination, etc.) |

### Remapper Layer (`src/remapper/`)

#### PartitionRemapper

The mathematical core of partition/offset translation:

```rust
pub struct PartitionRemapper {
    virtual_partitions: u32,   // V
    physical_partitions: u32,  // P
    compression_ratio: u32,    // C = V/P
    offset_range: u64,         // Default: 2^40
}
```

Key algorithms (detailed in [Protocol Mapping Documentation](./protocol-mapping.md)):
- `virtual_to_physical()` - Maps virtual partition to physical
- `virtual_to_physical_offset()` - Maps virtual partition+offset to physical
- `physical_to_virtual()` - Reverse mapping for responses
- `offset_belongs_to_virtual()` - Determines if offset belongs to virtual partition

#### TopicRemapperRegistry

Per-topic configuration support:

```rust
pub struct TopicRemapperRegistry {
    default_remapper: Arc<PartitionRemapper>,
    topic_remappers: DashMap<String, Arc<PartitionRemapper>>,
    config: MappingConfig,
    compiled_patterns: Vec<(String, Regex)>,
}
```

Resolution order:
1. Exact topic name match
2. Regex pattern match (e.g., `events.*`)
3. Default global configuration

### Authentication Layer (`src/auth/`)

Full SASL support with pluggable mechanisms:

```rust
pub trait SaslAuthenticator: Send + Sync + Debug {
    fn mechanism_name(&self) -> &'static str;
    fn authenticate_step(&self, client_message: &[u8], session: &mut SaslSession)
        -> SaslStepResult;
    fn is_complete(&self, session: &SaslSession) -> bool;
}
```

Supported mechanisms:
- **PLAIN** - Simple username/password
- **SCRAM-SHA-256** - RFC 7677 challenge-response
- **SCRAM-SHA-512** - RFC 7677 challenge-response
- **OAUTHBEARER** - OAuth 2.0 tokens (KIP-255)

See [Authentication Documentation](./authentication.md) for details.

### TLS Layer (`src/tls/`)

Dual TLS support:
- **Server-side** (`TlsServerAcceptor`) - For client connections
- **Client-side** (`TlsConnector`) - For broker connections

Features:
- Certificate-based authentication
- mTLS with client certificate validation
- Principal extraction from certificates
- SNI support

## Security Model

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Security Boundaries                             │
│                                                                          │
│  ┌─────────────┐      ┌─────────────────────┐      ┌─────────────────┐ │
│  │   Client    │      │       Proxy         │      │    Broker       │ │
│  │             │      │                     │      │                 │ │
│  │  ┌───────┐  │      │  ┌───────────────┐  │      │  ┌───────────┐  │ │
│  │  │ SASL  │──┼──────┼─▶│  SaslServer   │  │      │  │  SASL     │  │ │
│  │  └───────┘  │      │  └───────────────┘  │      │  └───────────┘  │ │
│  │             │      │          │          │      │        ▲        │ │
│  │  ┌───────┐  │      │          ▼          │      │        │        │ │
│  │  │  TLS  │──┼──────┼─▶│ TlsAcceptor   │  │      │  ┌───────────┐  │ │
│  │  └───────┘  │      │  └───────────────┘  │      │  │  TLS      │  │ │
│  │             │      │                     │      │  └───────────┘  │ │
│  │             │      │  ┌───────────────┐  │      │        ▲        │ │
│  │             │      │  │ TlsConnector  │──┼──────┼────────┘        │ │
│  │             │      │  └───────────────┘  │      │                 │ │
│  │             │      │          │          │      │                 │ │
│  │             │      │          ▼          │      │                 │ │
│  │             │      │  ┌───────────────┐  │      │                 │ │
│  │             │      │  │ SaslClient    │──┼──────┼─▶│   SASL    │  │ │
│  │             │      │  └───────────────┘  │      │                 │ │
│  └─────────────┘      └─────────────────────┘      └─────────────────┘ │
│                                                                          │
│  Protocol: SASL_SSL          Protocol: SASL_SSL                         │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

The proxy acts as both:
- A **TLS/SASL server** for clients
- A **TLS/SASL client** for brokers

This enables independent security configurations for each side.

## Connection Lifecycle

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Client Connection Lifecycle                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  1. TCP Accept                                                       │
│     └─▶ Check max_connections limit                                 │
│         └─▶ Reject if exceeded                                      │
│                                                                      │
│  2. TLS Handshake (if SSL/SASL_SSL)                                 │
│     └─▶ TlsServerAcceptor::accept()                                │
│         └─▶ Verify client certificate (if mTLS)                    │
│             └─▶ Extract principal                                   │
│                                                                      │
│  3. SASL Authentication (if SASL_*/SASL_SSL)                        │
│     └─▶ Receive SaslHandshake request                              │
│         └─▶ Send mechanism list                                     │
│     └─▶ Receive SaslAuthenticate request(s)                        │
│         └─▶ PLAIN: Single message                                   │
│         └─▶ SCRAM: Multi-step challenge-response                   │
│         └─▶ OAUTHBEARER: Token validation                          │
│                                                                      │
│  4. Request Loop                                                     │
│     └─▶ Decode frame with KafkaCodec                               │
│         └─▶ Extract API key, version, correlation ID               │
│     └─▶ Dispatch to appropriate handler                            │
│     └─▶ Encode response                                             │
│     └─▶ Send to client                                              │
│     └─▶ Repeat until disconnect/shutdown                           │
│                                                                      │
│  5. Cleanup                                                          │
│     └─▶ Decrement active_connections                               │
│     └─▶ Close streams                                               │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Graceful Shutdown

The proxy supports graceful shutdown through a broadcast channel:

```rust
// Shutdown signal propagation
shutdown_tx.send(())  // Triggers shutdown

// Each component listens
select! {
    _ = shutdown_rx.recv() => return Ok(()),
    frame = stream.next() => { /* handle request */ }
}
```

Shutdown sequence:
1. Stop accepting new connections
2. Signal all `ConnectionHandler` tasks
3. Wait for in-flight requests to complete
4. Close broker connections
5. Exit cleanly

## Metrics

Prometheus metrics are exposed at `/metrics` (configurable port):

| Metric | Type | Description |
|--------|------|-------------|
| `kafka_proxy_connections_active` | Gauge | Current active connections |
| `kafka_proxy_connections_total` | Counter | Total connections accepted |
| `kafka_proxy_requests_total` | Counter | Requests by API key |
| `kafka_proxy_request_duration_seconds` | Histogram | Request latency |
| `kafka_proxy_bytes_sent_total` | Counter | Bytes sent to clients |
| `kafka_proxy_bytes_received_total` | Counter | Bytes received from clients |
| `kafka_proxy_remapping_operations_total` | Counter | Partition remapping operations |
| `kafka_proxy_broker_connections_active` | Gauge | Active broker connections |

## Error Handling

Errors are categorized into distinct types:

```rust
pub enum ProxyError {
    Connection(io::Error),           // Network errors
    ProtocolDecode { message },      // Malformed requests
    ProtocolEncode { message },      // Encoding failures
    BrokerUnavailable { broker_id }, // Broker connection issues
    NoBrokersAvailable,              // No reachable brokers
    TopicNotFound { topic },         // Unknown topic
    PartitionOutOfRange { ... },     // Invalid partition
    OffsetOverflow { ... },          // Offset too large
    CorrelationIdMismatch { ... },   // Protocol violation
    UnsupportedApi { ... },          // Unknown API
    Remap(RemapError),               // Remapping failures
    Tls(TlsError),                   // TLS errors
    Auth(AuthError),                 // Authentication failures
    Shutdown,                        // Clean shutdown
}
```

Error handling philosophy:
- Client errors return appropriate Kafka error codes
- Internal errors are logged and connections may be closed
- Authentication failures return `SASL_AUTHENTICATION_FAILED`
- Partition errors return `UNKNOWN_TOPIC_OR_PARTITION`

## Performance Considerations

### Memory Management
- Zero-copy where possible using `BytesMut`
- Connection pooling to brokers
- Lazy remapper creation per topic

### Concurrency
- Each client connection runs in its own tokio task
- Broker connections are shared via `BrokerPool`
- Lock-free data structures (`DashMap`) for hot paths

### Scalability Limits
- Default max connections: 1000 (configurable)
- Max frame size: 100 MB
- Offset range: 2^40 per virtual partition

## Dependencies

| Dependency | Version | Purpose |
|------------|---------|---------|
| tokio | 1.42 | Async runtime |
| kafka-protocol | 0.17 | Kafka protocol types |
| tokio-rustls | 0.26 | TLS implementation |
| rustls | 0.23 | TLS library |
| serde/serde_yaml | Latest | Configuration |
| prometheus | 0.13 | Metrics |
| thiserror | 2.0 | Error definitions |
| dashmap | Latest | Concurrent maps |
| sha2/hmac/pbkdf2 | Latest | SCRAM authentication |

## Related Documentation

- [Authentication & Security](./authentication.md) - Detailed auth configuration
- [Protocol Mapping](./protocol-mapping.md) - Partition/offset translation algorithms
- [Configuration Reference](./configuration.md) - All configuration options
- [Getting Started](./getting-started.md) - Quick start guide
