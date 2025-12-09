//! End-to-end tests for the Kafka partition remapping proxy.
//!
//! This module contains two types of tests:
//! 1. Mock-based scenario tests using ProxyTestHarness
//! 2. Real Kafka tests using testcontainers-rs

use std::collections::HashMap;
use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::{ClientContext, DefaultClientContext};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::Message;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::{Kafka, KAFKA_PORT};

use kafka_remapper_core::config::MappingConfig;
use kafka_remapper_core::remapper::PartitionRemapper;
use kafka_remapper_core::testing::{ProxyTestHarness, TestHarnessBuilder};

// =============================================================================
// Mock-based Scenario Tests
// =============================================================================

/// Test that a produce/consume roundtrip preserves message ordering.
#[tokio::test]
async fn test_produce_consume_roundtrip_ordering() {
    let harness = ProxyTestHarness::new().await;

    let virtual_partition = 42;
    let message_count = 1000;

    let mut physical_offsets = Vec::new();

    for i in 0..message_count {
        let physical = harness
            .remapper
            .virtual_to_physical_offset(virtual_partition, i)
            .unwrap();
        physical_offsets.push(physical.physical_offset);
    }

    // Physical offsets should be strictly increasing within same partition
    for i in 1..physical_offsets.len() {
        assert!(
            physical_offsets[i] > physical_offsets[i - 1],
            "Physical offsets must be strictly increasing: {} vs {}",
            physical_offsets[i - 1],
            physical_offsets[i]
        );
    }

    // Verify roundtrip for all offsets
    for (i, &phys_offset) in physical_offsets.iter().enumerate() {
        let mapping = harness
            .remapper
            .virtual_to_physical(virtual_partition)
            .unwrap();
        let back = harness
            .remapper
            .physical_to_virtual(mapping.physical_partition, phys_offset)
            .unwrap();

        assert_eq!(back.virtual_partition, virtual_partition);
        assert_eq!(back.virtual_offset, i as i64);
    }
}

/// Test key-based routing consistency.
#[tokio::test]
async fn test_key_ordering_preserved() {
    let harness = ProxyTestHarness::new().await;

    let keys = vec![
        "customer:123",
        "order:abc-456",
        "user@example.com",
        "payment-key-789",
    ];

    for key in &keys {
        let hash = key.bytes().fold(0u32, |acc, b| acc.wrapping_add(b as u32));
        let virtual_partition = (hash % 100) as i32;
        let mapping = harness
            .remapper
            .virtual_to_physical(virtual_partition)
            .unwrap();

        // Same key should always map to same partitions
        for _ in 0..100 {
            let hash2 = key.bytes().fold(0u32, |acc, b| acc.wrapping_add(b as u32));
            let vp2 = (hash2 % 100) as i32;
            assert_eq!(vp2, virtual_partition, "Key routing should be consistent");

            let mapping2 = harness.remapper.virtual_to_physical(vp2).unwrap();
            assert_eq!(
                mapping.physical_partition, mapping2.physical_partition,
                "Physical partition should be consistent for same key"
            );
        }
    }
}

/// Test multiple partitions roundtrip.
#[tokio::test]
async fn test_multiple_partitions_roundtrip() {
    let harness = TestHarnessBuilder::new()
        .virtual_partitions(1000)
        .physical_partitions(100)
        .build()
        .await;

    let test_partitions: Vec<i32> = (0..10).map(|i| i * 100 + 50).collect();

    for &v_part in &test_partitions {
        for offset in 0..100 {
            let physical = harness
                .remapper
                .virtual_to_physical_offset(v_part, offset)
                .unwrap();

            let back = harness
                .remapper
                .physical_to_virtual(physical.physical_partition, physical.physical_offset)
                .unwrap();

            assert_eq!(back.virtual_partition, v_part);
            assert_eq!(back.virtual_offset, offset);
        }
    }
}

/// Test consumer group offset tracking.
#[tokio::test]
async fn test_consumer_group_offset_tracking() {
    let harness = ProxyTestHarness::new().await;

    struct ConsumerState {
        committed_offsets: HashMap<i32, i64>,
    }

    let mut state = ConsumerState {
        committed_offsets: HashMap::new(),
    };

    // Consumer consumes partitions 0-9
    for v_part in 0..10 {
        let consumed_offset = 1000i64;
        let physical = harness
            .remapper
            .virtual_to_physical_offset(v_part, consumed_offset)
            .unwrap();
        state
            .committed_offsets
            .insert(v_part, physical.physical_offset);
    }

    // Later, consumer restarts and fetches committed offsets
    for v_part in 0..10 {
        let stored_physical = state.committed_offsets.get(&v_part).unwrap();
        let mapping = harness.remapper.virtual_to_physical(v_part).unwrap();
        let back = harness
            .remapper
            .physical_to_virtual(mapping.physical_partition, *stored_physical)
            .unwrap();

        assert_eq!(back.virtual_partition, v_part);
        assert_eq!(back.virtual_offset, 1000);
    }
}

/// Test high throughput scenario.
#[tokio::test]
async fn test_high_throughput_remapping() {
    let harness = TestHarnessBuilder::new()
        .virtual_partitions(10000)
        .physical_partitions(100)
        .build()
        .await;

    let messages_per_partition = 1000;
    let partitions = 100;

    for p in 0..partitions {
        let v_part = p * 100;

        for offset in 0..messages_per_partition {
            let physical = harness
                .remapper
                .virtual_to_physical_offset(v_part, offset)
                .unwrap();

            assert!(physical.physical_partition < 100);

            let back = harness
                .remapper
                .physical_to_virtual(physical.physical_partition, physical.physical_offset)
                .unwrap();

            assert_eq!(back.virtual_partition, v_part);
            assert_eq!(back.virtual_offset, offset);
        }
    }
}

/// Test partition rebalancing scenario.
#[tokio::test]
async fn test_consumer_rebalance_offset_continuity() {
    let harness = ProxyTestHarness::new().await;

    let v_part = 25;
    let committed = 500i64;

    let physical_before = harness
        .remapper
        .virtual_to_physical_offset(v_part, committed)
        .unwrap();

    let mapping = harness.remapper.virtual_to_physical(v_part).unwrap();
    let back = harness
        .remapper
        .physical_to_virtual(mapping.physical_partition, physical_before.physical_offset)
        .unwrap();

    assert_eq!(back.virtual_partition, v_part);
    assert_eq!(back.virtual_offset, committed);
}

/// Test that all virtual partitions are correctly distributed.
#[tokio::test]
async fn test_partition_distribution() {
    let harness = ProxyTestHarness::new().await;

    let mut distribution: HashMap<i32, Vec<i32>> = HashMap::new();

    for v in 0..100 {
        let mapping = harness.remapper.virtual_to_physical(v).unwrap();
        distribution
            .entry(mapping.physical_partition)
            .or_default()
            .push(v);
    }

    assert_eq!(distribution.len(), 10);

    for (phys, virtuals) in &distribution {
        assert_eq!(
            virtuals.len(),
            10,
            "Physical partition {} should have 10 virtual partitions",
            phys
        );
    }
}

/// Test boundary conditions.
#[tokio::test]
async fn test_boundary_conditions() {
    let harness = ProxyTestHarness::new().await;

    let offset_range = harness.remapper.offset_range();

    for v_part in [0, 10, 50, 99] {
        let first = harness
            .remapper
            .virtual_to_physical_offset(v_part, 0)
            .unwrap();

        let last_virtual = (offset_range - 1) as i64;
        let last = harness
            .remapper
            .virtual_to_physical_offset(v_part, last_virtual)
            .unwrap();

        let back_first = harness
            .remapper
            .physical_to_virtual(first.physical_partition, first.physical_offset)
            .unwrap();
        assert_eq!(back_first.virtual_partition, v_part);
        assert_eq!(back_first.virtual_offset, 0);

        let back_last = harness
            .remapper
            .physical_to_virtual(last.physical_partition, last.physical_offset)
            .unwrap();
        assert_eq!(back_last.virtual_partition, v_part);
        assert_eq!(back_last.virtual_offset, last_virtual);
    }
}

// =============================================================================
// Testcontainers-based Real Kafka Tests
// =============================================================================

/// Test environment with a real Kafka container.
pub struct KafkaTestEnvironment {
    pub bootstrap_servers: String,
    pub remapper: PartitionRemapper,
    _container: testcontainers::ContainerAsync<Kafka>,
}

impl KafkaTestEnvironment {
    /// Create a new test environment with Kafka container.
    pub async fn new() -> Self {
        let container = Kafka::default().start().await.unwrap();
        let port = container.get_host_port_ipv4(KAFKA_PORT).await.unwrap();
        // Use 127.0.0.1 explicitly to avoid IPv6 resolution issues
        let bootstrap_servers = format!("127.0.0.1:{}", port);

        let remapper = PartitionRemapper::new(&MappingConfig {
            virtual_partitions: 100,
            physical_partitions: 10,
            offset_range: 1 << 40,
        });

        // Wait for Kafka to be ready - try to connect with retries
        let mut attempts = 0;
        loop {
            let producer_result: Result<BaseProducer, _> = ClientConfig::new()
                .set("bootstrap.servers", &bootstrap_servers)
                .set("message.timeout.ms", "5000")
                .create();

            if let Ok(producer) = producer_result {
                // Try a metadata request to verify Kafka is truly ready
                if producer
                    .client()
                    .fetch_metadata(None, Duration::from_secs(5))
                    .is_ok()
                {
                    break;
                }
            }

            attempts += 1;
            if attempts >= 30 {
                panic!("Kafka did not become ready within 30 seconds");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Self {
            bootstrap_servers,
            remapper,
            _container: container,
        }
    }

    /// Create an admin client for this environment.
    pub fn create_admin_client(&self) -> AdminClient<DefaultClientContext> {
        ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .create()
            .expect("Failed to create admin client")
    }

    /// Create a producer for this environment.
    pub fn create_producer(&self) -> BaseProducer {
        ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Failed to create producer")
    }

    /// Create a consumer for this environment.
    pub fn create_consumer(&self, group_id: &str) -> BaseConsumer {
        ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("Failed to create consumer")
    }

    /// Create a topic with the specified number of partitions.
    pub async fn create_topic(&self, name: &str, num_partitions: i32) {
        let admin = self.create_admin_client();
        let topic = NewTopic::new(name, num_partitions, TopicReplication::Fixed(1));
        let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));

        admin
            .create_topics(&[topic], &opts)
            .await
            .expect("Failed to create topic");

        // Wait for topic to be created
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Test that we can produce and consume messages through Kafka.
///
/// Note: This test requires Docker and may be flaky due to Kafka startup timing.
/// Run with: cargo test --test e2e_tests test_kafka -- --ignored
#[tokio::test]
#[ignore = "Requires Docker and stable Kafka container networking"]
async fn test_kafka_produce_consume_basic() {
    let env = KafkaTestEnvironment::new().await;

    env.create_topic("test-basic", 10).await;

    let producer = env.create_producer();

    producer
        .send(
            BaseRecord::to("test-basic")
                .payload("test message")
                .key("test-key")
                .partition(0),
        )
        .expect("Failed to send message");

    producer.flush(Duration::from_secs(5)).unwrap();

    let consumer = env.create_consumer("test-group-basic");
    consumer.subscribe(&["test-basic"]).unwrap();

    let message = consumer.poll(Duration::from_secs(10)).unwrap().unwrap();
    let payload = message.payload().unwrap();

    assert_eq!(payload, b"test message");
}

/// Test that virtual partition mapping works correctly.
#[tokio::test]
#[ignore = "Requires Docker and stable Kafka container networking"]
async fn test_virtual_partition_mapping_with_kafka() {
    let env = KafkaTestEnvironment::new().await;

    env.create_topic("test-mapping", 10).await;

    let producer = env.create_producer();

    for virtual_partition in [0, 15, 27, 42, 58, 63, 74, 89, 95, 99] {
        let mapping = env.remapper.virtual_to_physical(virtual_partition).unwrap();
        let physical_partition = mapping.physical_partition;

        assert!(
            physical_partition < 10,
            "Physical partition {} should be < 10",
            physical_partition
        );

        let message = format!("message for virtual partition {}", virtual_partition);
        producer
            .send(
                BaseRecord::to("test-mapping")
                    .payload(message.as_bytes())
                    .key(&virtual_partition.to_string())
                    .partition(physical_partition),
            )
            .expect("Failed to send message");
    }

    producer.flush(Duration::from_secs(5)).unwrap();

    let consumer = env.create_consumer("test-group-mapping");
    consumer.subscribe(&["test-mapping"]).unwrap();

    let mut received = 0;
    while received < 10 {
        if let Some(Ok(_message)) = consumer.poll(Duration::from_secs(5)) {
            received += 1;
        } else {
            break;
        }
    }

    assert_eq!(received, 10, "Should have received all 10 messages");
}

/// Test offset translation with real Kafka offsets.
#[tokio::test]
#[ignore = "Requires Docker and stable Kafka container networking"]
async fn test_offset_translation_with_kafka() {
    let env = KafkaTestEnvironment::new().await;

    env.create_topic("test-offsets", 10).await;

    let producer = env.create_producer();

    for i in 0..10 {
        producer
            .send(
                BaseRecord::<(), _>::to("test-offsets")
                    .payload(format!("message {}", i).as_bytes())
                    .partition(0),
            )
            .expect("Failed to send message");
    }

    producer.flush(Duration::from_secs(5)).unwrap();

    let virtual_partition = 0;
    for kafka_offset in 0i64..10 {
        let physical = env
            .remapper
            .virtual_to_physical_offset(virtual_partition, kafka_offset)
            .unwrap();

        let back = env
            .remapper
            .physical_to_virtual(physical.physical_partition, physical.physical_offset)
            .unwrap();

        assert_eq!(back.virtual_partition, virtual_partition);
        assert_eq!(back.virtual_offset, kafka_offset);
    }
}

/// Test multiple consumers reading from different virtual partitions.
#[tokio::test]
#[ignore = "Requires Docker and stable Kafka container networking"]
async fn test_multi_consumer_virtual_partitions() {
    let env = KafkaTestEnvironment::new().await;

    env.create_topic("test-multi-consumer", 10).await;

    let producer = env.create_producer();

    for phys_partition in 0..10 {
        for i in 0..5 {
            producer
                .send(
                    BaseRecord::<(), _>::to("test-multi-consumer")
                        .payload(format!("p{}m{}", phys_partition, i).as_bytes())
                        .partition(phys_partition),
                )
                .expect("Failed to send message");
        }
    }

    producer.flush(Duration::from_secs(5)).unwrap();

    for phys in 0..10 {
        let virtuals = env.remapper.virtual_partitions_for_physical(phys);
        assert_eq!(
            virtuals.len(),
            10,
            "Each physical should have 10 virtual partitions"
        );

        for &v in &virtuals {
            let mapping = env.remapper.virtual_to_physical(v).unwrap();
            assert_eq!(mapping.physical_partition, phys);
        }
    }
}

/// Test that key-based routing is consistent.
#[tokio::test]
#[ignore = "Requires Docker and stable Kafka container networking"]
async fn test_key_routing_consistency() {
    let env = KafkaTestEnvironment::new().await;

    env.create_topic("test-key-routing", 10).await;

    let producer = env.create_producer();

    let test_key = "consistent-key";
    let mut target_partition = None;

    for i in 0..10 {
        producer
            .send(
                BaseRecord::to("test-key-routing")
                    .payload(format!("message {}", i).as_bytes())
                    .key(test_key),
            )
            .expect("Failed to send message");
    }

    producer.flush(Duration::from_secs(5)).unwrap();

    let consumer = env.create_consumer("test-group-key-routing");
    consumer.subscribe(&["test-key-routing"]).unwrap();

    let mut received = 0;
    while received < 10 {
        if let Some(Ok(message)) = consumer.poll(Duration::from_secs(5)) {
            let partition = message.partition();
            if target_partition.is_none() {
                target_partition = Some(partition);
            } else {
                assert_eq!(
                    Some(partition),
                    target_partition,
                    "All messages with same key should go to same partition"
                );
            }
            received += 1;
        } else {
            break;
        }
    }

    assert!(received > 0, "Should have received at least one message");
}

/// Test high-throughput message production and consumption.
#[tokio::test]
#[ignore = "Requires Docker and stable Kafka container networking"]
async fn test_high_throughput_kafka() {
    let env = KafkaTestEnvironment::new().await;

    env.create_topic("test-throughput", 10).await;

    let producer = env.create_producer();

    let message_count = 1000;

    for i in 0..message_count {
        let partition = (i % 10) as i32;
        producer
            .send(
                BaseRecord::<(), _>::to("test-throughput")
                    .payload(format!("msg-{}", i).as_bytes())
                    .partition(partition),
            )
            .expect("Failed to send message");

        if i % 100 == 99 {
            producer.flush(Duration::from_secs(5)).unwrap();
        }
    }

    producer.flush(Duration::from_secs(5)).unwrap();

    let consumer = env.create_consumer("test-group-throughput");
    consumer.subscribe(&["test-throughput"]).unwrap();

    let mut received = 0;
    let start = std::time::Instant::now();

    while received < message_count && start.elapsed() < Duration::from_secs(30) {
        if let Some(Ok(_)) = consumer.poll(Duration::from_millis(100)) {
            received += 1;
        }
    }

    assert_eq!(
        received, message_count,
        "Should have received all {} messages, got {}",
        message_count, received
    );
}
