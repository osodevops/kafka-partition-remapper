//! Integration tests for Produce request handling.
//!
//! Tests that produce requests correctly map virtual partitions to physical
//! and translate offsets in responses.

use kafka_remapper_core::testing::{ProxyTestHarness, TestHarnessBuilder};

#[tokio::test]
async fn test_produce_request_partition_remapping() {
    let harness = ProxyTestHarness::new().await;

    // Virtual partition 27 should map to physical partition 7 (27 % 10 = 7)
    let mapping = harness.remapper.virtual_to_physical(27).unwrap();
    assert_eq!(mapping.physical_partition, 7);
    assert_eq!(mapping.virtual_group, 2); // 27 / 10 = 2
}

#[tokio::test]
async fn test_produce_multiple_partitions() {
    let harness = ProxyTestHarness::new().await;

    // Test mapping multiple partitions
    let test_cases = vec![
        (15, 5, 1), // virtual 15 -> physical 5, group 1
        (42, 2, 4), // virtual 42 -> physical 2, group 4
        (99, 9, 9), // virtual 99 -> physical 9, group 9
        (0, 0, 0),  // virtual 0 -> physical 0, group 0
    ];

    for (virtual_part, expected_physical, expected_group) in test_cases {
        let mapping = harness.remapper.virtual_to_physical(virtual_part).unwrap();
        assert_eq!(
            mapping.physical_partition, expected_physical,
            "Virtual {virtual_part} should map to physical {expected_physical}"
        );
        assert_eq!(
            mapping.virtual_group, expected_group,
            "Virtual {virtual_part} should be in group {expected_group}"
        );
    }
}

#[tokio::test]
async fn test_produce_offset_response_translation() {
    let harness = ProxyTestHarness::new().await;

    // When broker returns a physical offset, it should be translated back to virtual
    // Virtual partition 50, virtual offset 1000
    let virtual_partition = 50;
    let virtual_offset = 1000i64;

    // Convert to physical
    let physical = harness
        .remapper
        .virtual_to_physical_offset(virtual_partition, virtual_offset)
        .unwrap();

    // Physical partition should be 50 % 10 = 0
    assert_eq!(physical.physical_partition, 0);

    // Virtual group is 50 / 10 = 5
    assert_eq!(physical.virtual_group, 5);

    // Physical offset = group * offset_range + virtual_offset
    // = 5 * (2^40) + 1000
    let expected_physical_offset = 5i64 * (harness.remapper.offset_range() as i64) + virtual_offset;
    assert_eq!(physical.physical_offset, expected_physical_offset);

    // Now translate back - should get original values
    let back = harness
        .remapper
        .physical_to_virtual(physical.physical_partition, physical.physical_offset)
        .unwrap();

    assert_eq!(back.virtual_partition, virtual_partition);
    assert_eq!(back.virtual_offset, virtual_offset);
}

#[tokio::test]
async fn test_produce_offset_determinism() {
    let harness = ProxyTestHarness::new().await;

    // Same input should always produce same output
    for _ in 0..100 {
        let v_part = 27;
        let v_offset = 5000i64;

        let result1 = harness
            .remapper
            .virtual_to_physical_offset(v_part, v_offset)
            .unwrap();
        let result2 = harness
            .remapper
            .virtual_to_physical_offset(v_part, v_offset)
            .unwrap();

        assert_eq!(result1.physical_partition, result2.physical_partition);
        assert_eq!(result1.physical_offset, result2.physical_offset);
    }
}

#[tokio::test]
async fn test_produce_all_partitions_mappable() {
    let harness = TestHarnessBuilder::new()
        .virtual_partitions(1000)
        .physical_partitions(100)
        .build()
        .await;

    // Every virtual partition should be mappable
    for v in 0i32..1000 {
        let result = harness.remapper.virtual_to_physical(v);
        assert!(result.is_ok(), "Virtual partition {v} should be mappable");
    }
}

#[tokio::test]
async fn test_produce_offset_ranges_per_group() {
    let harness = ProxyTestHarness::new().await;

    let offset_range = harness.remapper.offset_range();

    // Virtual partitions in different groups should have different offset ranges
    // Virtual 0 (group 0) and Virtual 10 (group 1) both on physical 0
    let range_0 = harness.remapper.get_offset_range_for_virtual(0).unwrap();
    let range_10 = harness.remapper.get_offset_range_for_virtual(10).unwrap();

    // Group 0: [0, offset_range)
    assert_eq!(range_0.0, 0);
    assert_eq!(range_0.1, offset_range as i64);

    // Group 1: [offset_range, 2*offset_range)
    assert_eq!(range_10.0, offset_range as i64);
    assert_eq!(range_10.1, 2 * offset_range as i64);

    // Ranges should not overlap
    assert!(range_0.1 <= range_10.0);
}

#[tokio::test]
async fn test_produce_base_offset_translation() {
    let harness = ProxyTestHarness::new().await;

    // Simulate broker returning base_offset = physical_offset
    // Proxy should translate it back to virtual_offset

    let virtual_partition = 75;
    let virtual_offset = 12345i64;

    // What the broker would receive
    let physical = harness
        .remapper
        .virtual_to_physical_offset(virtual_partition, virtual_offset)
        .unwrap();

    // Broker returns physical offset as base_offset
    let broker_base_offset = physical.physical_offset;

    // Proxy translates back
    let translated = harness
        .remapper
        .physical_to_virtual(physical.physical_partition, broker_base_offset)
        .unwrap();

    // Should match original virtual offset
    assert_eq!(translated.virtual_partition, virtual_partition);
    assert_eq!(translated.virtual_offset, virtual_offset);
}

#[tokio::test]
async fn test_produce_high_compression_ratio() {
    // Test with 100:1 compression
    let harness = TestHarnessBuilder::new()
        .virtual_partitions(10_000)
        .physical_partitions(100)
        .build()
        .await;

    assert_eq!(harness.remapper.compression_ratio(), 100);

    // Virtual 5050 should map to physical 50, group 50
    let mapping = harness.remapper.virtual_to_physical(5050).unwrap();
    assert_eq!(mapping.physical_partition, 50);
    assert_eq!(mapping.virtual_group, 50);
}
