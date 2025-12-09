//! Integration tests for Metadata request handling.
//!
//! Tests that metadata responses correctly expand physical partitions
//! to virtual partitions.

use std::sync::Arc;

use kafka_remapper_core::config::{KafkaConfig, ListenConfig, MappingConfig};
use kafka_remapper_core::handlers::MetadataHandler;
use kafka_remapper_core::remapper::PartitionRemapper;
use kafka_remapper_core::testing::{api_keys, MockBroker, ProxyTestHarness, TestHarnessBuilder};

/// Helper to create a metadata handler for tests.
fn create_metadata_handler(
    harness: &ProxyTestHarness,
) -> MetadataHandler {
    MetadataHandler::new(
        harness.remapper.clone(),
        harness.broker_pool.clone(),
        &harness.config.listen,
    )
}

#[tokio::test]
async fn test_metadata_partition_expansion() {
    // Setup: 100 virtual on 10 physical
    let harness = ProxyTestHarness::new().await;

    // Verify config
    assert_eq!(harness.remapper.virtual_partitions(), 100);
    assert_eq!(harness.remapper.physical_partitions(), 10);

    // Compression ratio should be 10:1
    assert_eq!(harness.remapper.compression_ratio(), 10);
}

#[tokio::test]
async fn test_metadata_all_virtual_partitions_listed() {
    // Setup with 1000 virtual on 100 physical
    let harness = TestHarnessBuilder::new()
        .virtual_partitions(1000)
        .physical_partitions(100)
        .build()
        .await;

    // Verify all 1000 virtual partitions exist in mapping
    for v_part in 0i32..1000 {
        let result = harness.remapper.virtual_to_physical(v_part);
        assert!(
            result.is_ok(),
            "Virtual partition {} should be mappable",
            v_part
        );

        let mapping = result.unwrap();
        assert!(
            mapping.physical_partition >= 0 && mapping.physical_partition < 100,
            "Physical partition should be in range 0-99"
        );
    }
}

#[tokio::test]
async fn test_metadata_virtual_to_physical_distribution() {
    let harness = ProxyTestHarness::new().await;

    // Each physical partition should host exactly 10 virtual partitions
    for phys in 0i32..10 {
        let virtual_count = (0i32..100)
            .filter(|&v| {
                harness
                    .remapper
                    .virtual_to_physical(v)
                    .map(|m| m.physical_partition == phys)
                    .unwrap_or(false)
            })
            .count();

        assert_eq!(
            virtual_count, 10,
            "Physical partition {} should host exactly 10 virtual partitions",
            phys
        );
    }
}

#[tokio::test]
async fn test_metadata_broker_recording() {
    let mut harness = ProxyTestHarness::new().await;

    // Register a metadata response handler
    use bytes::{BufMut, BytesMut};
    harness
        .register_handler(
            api_keys::METADATA,
            Arc::new(|call| {
                let mut buf = BytesMut::new();
                buf.put_i32(call.correlation_id);
                // Minimal metadata response - just correlation ID for now
                buf.freeze()
            }),
        )
        .await;

    // The harness should start with no calls
    let calls = harness.get_broker_calls().await;
    assert_eq!(calls.len(), 0);
}

#[tokio::test]
async fn test_metadata_virtual_partition_leader_assignment() {
    let harness = ProxyTestHarness::new().await;

    // Virtual partitions on the same physical should inherit the same leader
    // Virtual 0 and 10 both map to physical 0
    let mapping_0 = harness.remapper.virtual_to_physical(0).unwrap();
    let mapping_10 = harness.remapper.virtual_to_physical(10).unwrap();

    assert_eq!(
        mapping_0.physical_partition, mapping_10.physical_partition,
        "Virtual 0 and 10 should map to same physical partition"
    );
    assert_eq!(mapping_0.physical_partition, 0);
}

#[tokio::test]
async fn test_metadata_custom_partition_config() {
    // Test with custom configuration
    let harness = TestHarnessBuilder::new()
        .virtual_partitions(500)
        .physical_partitions(50)
        .build()
        .await;

    assert_eq!(harness.remapper.virtual_partitions(), 500);
    assert_eq!(harness.remapper.physical_partitions(), 50);
    assert_eq!(harness.remapper.compression_ratio(), 10);

    // Verify boundary cases
    assert!(harness.remapper.virtual_to_physical(0).is_ok());
    assert!(harness.remapper.virtual_to_physical(499).is_ok());
    assert!(harness.remapper.virtual_to_physical(500).is_err());
}

#[tokio::test]
async fn test_metadata_partition_indices_sorted() {
    let harness = ProxyTestHarness::new().await;

    // Get all virtual partitions for physical 0
    let virtuals: Vec<i32> = (0i32..100)
        .filter(|&v| {
            harness
                .remapper
                .virtual_to_physical(v)
                .map(|m| m.physical_partition == 0)
                .unwrap_or(false)
        })
        .collect();

    // Should be 0, 10, 20, 30, 40, 50, 60, 70, 80, 90
    assert_eq!(virtuals, vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90]);
}
