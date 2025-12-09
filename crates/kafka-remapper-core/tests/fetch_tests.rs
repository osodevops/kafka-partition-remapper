//! Integration tests for Fetch request handling.
//!
//! Tests that fetch requests correctly map virtual partitions and offsets
//! to physical, and translate responses back to virtual.

use kafka_remapper_core::testing::{ProxyTestHarness, TestHarnessBuilder};

#[tokio::test]
async fn test_fetch_request_partition_remapping() {
    let harness = ProxyTestHarness::new().await;

    // Client fetches from virtual partition 30
    // Should map to physical partition 0 (30 % 10 = 0), group 3 (30 / 10 = 3)
    let mapping = harness.remapper.virtual_to_physical(30).unwrap();
    assert_eq!(mapping.physical_partition, 0);
    assert_eq!(mapping.virtual_group, 3);
}

#[tokio::test]
async fn test_fetch_offset_translation() {
    let harness = ProxyTestHarness::new().await;

    // Client wants to fetch from virtual partition 27, starting at virtual offset 1000
    let virtual_partition = 27;
    let fetch_offset = 1000i64;

    // Proxy translates to physical
    let physical = harness
        .remapper
        .virtual_to_physical_offset(virtual_partition, fetch_offset)
        .unwrap();

    // Physical partition: 27 % 10 = 7
    assert_eq!(physical.physical_partition, 7);

    // Virtual group: 27 / 10 = 2
    assert_eq!(physical.virtual_group, 2);

    // Physical offset: group * offset_range + virtual_offset
    let offset_range = harness.remapper.offset_range() as i64;
    let expected_offset = 2 * offset_range + fetch_offset;
    assert_eq!(physical.physical_offset, expected_offset);
}

#[tokio::test]
async fn test_fetch_across_virtual_partition_boundary() {
    let harness = ProxyTestHarness::new().await;

    // Virtual partitions 0-9 all map to physical 0 (but different groups)
    // Messages from virtual 0 should NOT appear in fetch for virtual 5

    let offset_range = harness.remapper.offset_range();

    // Virtual partition 0 offset range
    let range_0 = harness.remapper.get_offset_range_for_virtual(0).unwrap();
    assert_eq!(range_0.0, 0);
    assert_eq!(range_0.1, offset_range as i64);

    // Virtual partition 5 offset range (still physical 0, but group 0)
    let range_5 = harness.remapper.get_offset_range_for_virtual(5).unwrap();
    assert_eq!(range_5.0, 0);
    assert_eq!(range_5.1, offset_range as i64);

    // Wait - these are both group 0? Let's verify:
    // vp 0: phys 0, group 0
    // vp 5: phys 5, group 0
    // So they're on DIFFERENT physical partitions

    let mapping_0 = harness.remapper.virtual_to_physical(0).unwrap();
    let mapping_5 = harness.remapper.virtual_to_physical(5).unwrap();

    assert_eq!(mapping_0.physical_partition, 0);
    assert_eq!(mapping_5.physical_partition, 5);

    // Both are group 0
    assert_eq!(mapping_0.virtual_group, 0);
    assert_eq!(mapping_5.virtual_group, 0);
}

#[tokio::test]
async fn test_fetch_same_physical_different_virtual() {
    let harness = ProxyTestHarness::new().await;

    // Virtual 0 and 10 both map to physical 0, but different groups
    let mapping_0 = harness.remapper.virtual_to_physical(0).unwrap();
    let mapping_10 = harness.remapper.virtual_to_physical(10).unwrap();

    assert_eq!(mapping_0.physical_partition, 0);
    assert_eq!(mapping_10.physical_partition, 0);

    // Different groups
    assert_eq!(mapping_0.virtual_group, 0);
    assert_eq!(mapping_10.virtual_group, 1);

    // Their offset ranges should not overlap
    let range_0 = harness.remapper.get_offset_range_for_virtual(0).unwrap();
    let range_10 = harness.remapper.get_offset_range_for_virtual(10).unwrap();

    assert!(
        range_0.1 <= range_10.0,
        "Offset ranges should not overlap: {:?} vs {:?}",
        range_0,
        range_10
    );
}

#[tokio::test]
async fn test_fetch_offset_belongs_to_virtual() {
    let harness = ProxyTestHarness::new().await;

    // Get offset range for virtual partition 0
    let (start_0, end_0) = harness.remapper.get_offset_range_for_virtual(0).unwrap();

    // An offset in the middle of the range should belong to virtual 0
    let mid_offset = (start_0 + end_0) / 2;
    let belongs = harness
        .remapper
        .offset_belongs_to_virtual(0, mid_offset, 0)
        .unwrap();
    assert!(belongs, "Mid-range offset should belong to virtual 0");

    // The same offset should NOT belong to virtual 10 (same physical, different group)
    let belongs_10 = harness
        .remapper
        .offset_belongs_to_virtual(0, mid_offset, 10)
        .unwrap();
    assert!(
        !belongs_10,
        "Offset from group 0 should not belong to virtual 10 (group 1)"
    );
}

#[tokio::test]
async fn test_fetch_response_offset_translation() {
    let harness = ProxyTestHarness::new().await;

    // Broker returns records with physical offsets
    // Proxy should translate to virtual offsets

    let virtual_partition = 50;
    let offset_range = harness.remapper.offset_range();

    // Physical offset in the range for virtual partition 50
    // Virtual 50 -> physical 0, group 5
    let mapping = harness
        .remapper
        .virtual_to_physical(virtual_partition)
        .unwrap();
    assert_eq!(mapping.physical_partition, 0);
    assert_eq!(mapping.virtual_group, 5);

    // A physical offset in group 5's range
    let physical_offset = 5 * offset_range as i64 + 12345;

    // Translate back to virtual
    let virtual_mapping = harness
        .remapper
        .physical_to_virtual(mapping.physical_partition, physical_offset)
        .unwrap();

    assert_eq!(virtual_mapping.virtual_partition, virtual_partition);
    assert_eq!(virtual_mapping.virtual_offset, 12345);
}

#[tokio::test]
async fn test_fetch_high_watermark_translation() {
    let harness = ProxyTestHarness::new().await;

    // High watermark is just another offset that needs translation
    let virtual_partition = 75;

    // Simulate broker's high watermark (physical offset)
    let physical = harness
        .remapper
        .virtual_to_physical_offset(virtual_partition, 50000)
        .unwrap();

    // Broker's high watermark
    let broker_hw = physical.physical_offset;

    // Translate back
    let translated = harness
        .remapper
        .physical_to_virtual(physical.physical_partition, broker_hw)
        .unwrap();

    assert_eq!(translated.virtual_partition, virtual_partition);
    assert_eq!(translated.virtual_offset, 50000);
}

#[tokio::test]
async fn test_fetch_large_offset_range() {
    // Test with custom offset range
    let harness = TestHarnessBuilder::new()
        .virtual_partitions(100)
        .physical_partitions(10)
        .offset_range(1 << 30) // 1 billion
        .build()
        .await;

    let offset_range = harness.remapper.offset_range();
    assert_eq!(offset_range, 1 << 30);

    // Large but valid offset
    let large_offset = (1i64 << 29) + 999;
    let physical = harness
        .remapper
        .virtual_to_physical_offset(42, large_offset)
        .unwrap();

    let back = harness
        .remapper
        .physical_to_virtual(physical.physical_partition, physical.physical_offset)
        .unwrap();

    assert_eq!(back.virtual_partition, 42);
    assert_eq!(back.virtual_offset, large_offset);
}

#[tokio::test]
async fn test_fetch_zero_offset() {
    let harness = ProxyTestHarness::new().await;

    // Fetch from offset 0 for various virtual partitions
    for v_part in [0, 25, 50, 75, 99] {
        let physical = harness
            .remapper
            .virtual_to_physical_offset(v_part, 0)
            .unwrap();

        let back = harness
            .remapper
            .physical_to_virtual(physical.physical_partition, physical.physical_offset)
            .unwrap();

        assert_eq!(
            back.virtual_partition, v_part,
            "Partition roundtrip failed for {v_part}"
        );
        assert_eq!(
            back.virtual_offset, 0,
            "Offset 0 roundtrip failed for {v_part}"
        );
    }
}
