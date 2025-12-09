Product overview
A Rust-based Kafka Partition Remapping Proxy that sits between Kafka clients and a managed Kafka cluster (e.g., Confluent Cloud), exposing a large number of virtual partitions while using a much smaller number of physical partitions, to reduce per‑partition costs without changing client applications.​

The proxy uses kafka-protocol-rs for full Kafka wire-protocol support (encode/decode of all request/response types and versions) and implements the routing, remapping, and offset translation logic itself.​

Goals and non-goals
Goals
Reduce effective Confluent/Kafka partition count by 5–10x via virtual→physical partition remapping while preserving application behavior (ordering per key, consumer group semantics).​

Be transparent to existing Kafka clients (no code changes, standard Kafka protocol) by acting as a binary proxy implementing the Kafka protocol.​​

Support a configurable mapping strategy:

virtual_partitions (what clients see)

physical_partitions (what the broker actually has)

fixed compression ratio (e.g., 10:1).​

Ship an MVP suitable for production use for a single Kafka cluster:

Basic HA via stateless proxy instances behind a load balancer.

Metrics and logging sufficient for SRE operation.​

Non-goals (v1)
Full virtual clusters (per-tenant cluster abstraction, multi-cluster routing).

Field-level encryption or content-aware routing.

UI/console; v1 is config‑file + metrics only.

Deep enterprise features (RBAC UI, chargeback dashboards, audits) beyond basic logs.

Personas and use cases
Persona 1: Platform engineer (Confluent / MSK / Kafka Cloud)
Runs a shared Kafka cluster for many teams.

Faces high per-partition bills and internal pressure to cut costs.

Wants a drop‑in component that reduces partition counts without forcing app changes.​

Key use cases:

Expose 1,000 partitions logically to apps while paying for 100 physical partitions (10:1 compression).​

Onboard new tenants without increasing physical partitions linearly.

Run cost experiments by changing compression ratio per topic.

Persona 2: SaaS infra lead
Multi-tenant SaaS mapping customers → Kafka topics/partitions.

Needs strong isolation semantics (per-tenant partition key range) but cannot afford linear partition growth.​

Key use cases:

Map “500 partitions per customer” design to far fewer physical partitions while keeping per-customer ordering and consumer semantics.​

Show internal/external stakeholders clear partition-cost savings.

High-level design
Architecture
Clients → Proxy → Kafka Broker:

Clients talk Kafka over TCP to the proxy (binary protocol).

Proxy uses kafka-protocol-rs to parse requests and encode responses.

Proxy remaps topic, partition, offset from virtual to physical and forwards to the real cluster.​​

Core concept:

Clients see V virtual partitions; cluster actually uses P physical partitions.

Compression ratio 
C
=
V
/
P
C=V/P must be an integer.​

Mapping:

physical_partition = virtual_partition / C

virtual_group = virtual_partition % C

physical_offset = virtual_group * OFFSET_RANGE + virtual_offset

virtual_offset = physical_offset % OFFSET_RANGE.​

Main components
TCP front-end

Tokio-based TCP listener (e.g., 0.0.0.0:9092).

Frame parsing: 4‑byte length prefix + Kafka request bytes.​

Per-connection loop that:

Parses the request header (API key, version, correlation id).

Deserializes the body via kafka-protocol-rs.

Dispatches to appropriate handler.

Encodes and writes back response.​​

PartitionRemapper

Core data structure controlling mapping:​

Config:

virtual_partitions: u32

physical_partitions: u32

compression_ratio: u32

offset_range: u64 (e.g., 
10
12
10 
12
 ).​

Functions:

virtual_to_physical(v_part) -> p_part

virtual_to_physical_offset(v_part, v_offset) -> (p_part, p_offset)

physical_to_virtual(p_part, p_offset) -> (v_part, v_offset).​

Validation:

Ensure virtual_partitions % physical_partitions == 0.

Protocol handlers (using kafka-protocol-rs types)

a. Produce​​

API key 0: ProduceRequest / ProduceResponse.

For each topic, partition:

Interpret partition as virtual.

Map to physical partition and translate offsets via PartitionRemapper.

Build a new request to the broker with physical partition and transformed record batch.

On the response, map physical base offsets back to virtual offsets before returning to the client.​

b. Fetch

API key 1: FetchRequest / FetchResponse.

For each topic, partition:

Map virtual partition to physical.

Translate requested fetch offset to physical offset.

Compute offset range belonging to that virtual partition (virtual_group * offset_range .. (virtual_group+1)*offset_range).​

Fetch from broker; filter records to that range.

Translate returned offsets back to virtual offsets.​

c. Metadata

API key 3: MetadataRequest / MetadataResponse.

Request is usually pass-through, but response is transformed:

For each topic, expand physical partition list 0..P-1 into 0..V-1 by mapping virtual→physical and copying leader/replica metadata.​

Clients thus see V partitions while cluster actually has P.

d. Group coordination & offsets (MVP+)​

JoinGroup / SyncGroup / OffsetCommit / ListOffsets:

Initially: pass-through for basic support.

MVP plus: maintain mapping so group assignments and committed offsets align with virtual partitions.

Broker connection pool

Manages async connections to the real Kafka cluster.

Simple routing (single cluster, no virtual clusters in v1).

Reuses connections per broker; handles reconnection and backoff.

Configuration and ops

YAML or env-based config:

Proxy listen address.

Mapping parameters per topic (eventually per-topic; MVP may support global mapping or small set).​

Broker bootstrap list.

Logging via tracing.

Metrics via Prometheus (produce/fetch counts, latency histograms, mapping errors).​

Functional requirements
F1: Transparent Kafka protocol support (subset)
The proxy MUST support at least:

ApiVersions, Metadata, Produce, Fetch.

For MVP+ group features: JoinGroup, SyncGroup, OffsetCommit, ListOffsets.​​

It MUST preserve:

Message ordering per key.

Consumer group semantics (no double-consumption on rebalance).

Error codes expected by clients (mapping internal broker errors to client-visible ones).

F2: Virtual→physical partition mapping
Product MUST expose a configurable number of virtual partitions per topic and map them deterministically to physical partitions via PartitionRemapper.​

Mapping MUST:

Ensure each virtual partition maps to exactly one physical partition.

Ensure there is no offset-range overlap between virtual partitions on the same physical partition (using fixed, large offset_range windows).​

Clients MUST be able to use normal partition selection logic (hash(key) % V) unchanged.

F3: Offset translation
For all produce and fetch paths, the proxy MUST:

Translate virtual offsets to physical before sending to broker.

Translate physical offsets to virtual before returning to clients.

Offset translation MUST be reversible and deterministic, validated by round-trip tests.​

F4: Metadata virtualization
MetadataResponse MUST report virtual partition counts to clients.

Leader/replica assignments MAY mirror the physical ones; no need to create new logical broker IDs in v1.

F5: Configuration
Admin MUST be able to configure:

virtual_partitions, physical_partitions, compression_ratio, offset_range.

One or more topics to which remapping applies; other topics may be pass-through.​

Broker bootstrap servers.

F6: Observability
The system MUST expose:

Counters: number of produce/fetch/metadata/group requests.

Histograms: per-op latency (produce/fetch).

Gauges: active connections, error rates.​

Logs MUST include correlation id, API key, topic, and mapping decisions for debugging.

Non-functional requirements
Performance
Additional p99 latency introduced by proxy:

Target: ≤ 1–3 ms per request at typical loads.​

Throughput:

Target: ≥ 80–90% of direct Kafka throughput for produce/fetch.​

Memory:

Target: ≤ 300 MB for 100–200 concurrent connections on a modest instance.​

Reliability
Proxy MUST handle broker reconnect transparently.

Should be stateless enough to run multiple instances behind a TCP load balancer.

Security
MVP: transparent PLAINTEXT and SASL pass-through (no termination).

Later: TLS termination and mTLS can be added.

Dependencies
tokio for async networking.

kafka-protocol-rs for Kafka message structs and Encodable/Decodable traits against Kafka 4.1.0 and compatible versions.​

bytes / BytesMut (already used by kafka-protocol-rs).​

serde/serde_yaml for config.

tracing, prometheus for logging/metrics.

Release scope (MVP)
In scope
Single-cluster Kafka Partition Remapping Proxy:

Produce, Fetch, Metadata implemented with full remapping.

Config-driven mapping 
V
,
P
,
C
V,P,C.

Basic error handling and metrics.

Docker image and minimal deployment docs.

Out of scope
Multi-cluster routing, full virtual clusters.

UI console.

Complex auth, multi-tenant policy engine.

Open questions
Per-topic vs global mapping: start with a global (V, P) or allow per-topic overrides from day one?

How strictly to validate max offsets vs offset_range to prevent overlap in pathological traffic?

How to version mapping changes: what happens if admin changes V or P at runtime?
