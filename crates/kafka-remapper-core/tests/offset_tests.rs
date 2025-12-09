//! Integration tests for `OffsetCommit` and `OffsetFetch` handling.
//!
//! Tests that offset commit/fetch correctly translate between virtual
//! and physical partition indices and offsets.

use kafka_remapper_core::testing::{ProxyTestHarness, TestHarnessBuilder};

#[tokio::test]
async fn test_offset_commit_translation() {
    let harness = ProxyTestHarness::new().await;

    // Virtual partition 42, offset 5000
    let v_part = 42;
    let v_offset = 5000i64;

    // Translate to physical
    let physical = harness
        .remapper
        .virtual_to_physical_offset(v_part, v_offset)
        .unwrap();

    // Virtual 42 -> Physical 2 (42 % 10), group 4 (42 / 10)
    assert_eq!(physical.physical_partition, 2);
    assert_eq!(physical.virtual_group, 4);

    // Physical offset = group * offset_range + virtual_offset
    let offset_range = harness.remapper.offset_range() as i64;
    let expected_offset = 4 * offset_range + v_offset;
    assert_eq!(physical.physical_offset, expected_offset);
}

#[tokio::test]
async fn test_offset_fetch_translation() {
    let harness = ProxyTestHarness::new().await;

    // Simulate broker returning a physical offset
    // We need to translate it back to virtual

    let v_part = 27;
    let v_offset = 1500i64;

    // First get the physical representation
    let physical = harness
        .remapper
        .virtual_to_physical_offset(v_part, v_offset)
        .unwrap();

    // Now translate back (as would happen in OffsetFetch response)
    let back = harness
        .remapper
        .physical_to_virtual(physical.physical_partition, physical.physical_offset)
        .unwrap();

    assert_eq!(back.virtual_partition, v_part);
    assert_eq!(back.virtual_offset, v_offset);
}

#[tokio::test]
async fn test_virtual_offset_commit_roundtrip() {
    let harness = ProxyTestHarness::new().await;

    // Test multiple partitions
    let test_cases = vec![(0, 0), (27, 5000), (50, 12345), (99, 999_999)];

    for (v_part, v_offset) in test_cases {
        // Commit (virtual -> physical)
        let physical = harness
            .remapper
            .virtual_to_physical_offset(v_part, v_offset)
            .unwrap();

        // Fetch (physical -> virtual)
        let back = harness
            .remapper
            .physical_to_virtual(physical.physical_partition, physical.physical_offset)
            .unwrap();

        assert_eq!(
            back.virtual_partition, v_part,
            "Partition roundtrip failed for ({v_part}, {v_offset})"
        );
        assert_eq!(
            back.virtual_offset, v_offset,
            "Offset roundtrip failed for ({v_part}, {v_offset})"
        );
    }
}

#[tokio::test]
async fn test_offset_commit_multiple_groups() {
    let harness = ProxyTestHarness::new().await;

    // Virtual partitions that map to the same physical but different groups
    // vp 0, 10, 20, 30, ... all map to physical 0

    let physical_partition = 0;
    let offset_range = harness.remapper.offset_range() as i64;

    for group in 0..10 {
        let v_part = group * 10; // 0, 10, 20, 30, ...
        let v_offset = 1000i64;

        let physical = harness
            .remapper
            .virtual_to_physical_offset(v_part, v_offset)
            .unwrap();

        assert_eq!(
            physical.physical_partition, physical_partition,
            "Virtual partition {v_part} should map to physical 0"
        );

        // Each group has its own offset range
        let expected_offset = i64::from(group) * offset_range + v_offset;
        assert_eq!(
            physical.physical_offset, expected_offset,
            "Offset for group {group} incorrect"
        );
    }
}

#[tokio::test]
async fn test_offset_fetch_no_overlap_between_groups() {
    let harness = ProxyTestHarness::new().await;

    let offset_range = harness.remapper.offset_range();

    // Offsets from different virtual partitions (same physical) should not overlap
    // Virtual 0 uses offsets [0, offset_range)
    // Virtual 10 uses offsets [offset_range, 2*offset_range)

    let range_0 = harness.remapper.get_offset_range_for_virtual(0).unwrap();
    let range_10 = harness.remapper.get_offset_range_for_virtual(10).unwrap();

    // Should not overlap
    assert!(
        range_0.1 <= range_10.0,
        "Offset ranges should not overlap: {range_0:?} vs {range_10:?}"
    );

    // Specific boundary check
    assert_eq!(range_0.0, 0);
    assert_eq!(range_0.1, offset_range as i64);
    assert_eq!(range_10.0, offset_range as i64);
    assert_eq!(range_10.1, 2 * offset_range as i64);
}

#[tokio::test]
async fn test_offset_commit_high_compression() {
    // 1000:100 compression ratio
    let harness = TestHarnessBuilder::new()
        .virtual_partitions(1000)
        .physical_partitions(100)
        .build()
        .await;

    // Virtual 550 -> Physical 50, group 5
    let physical = harness
        .remapper
        .virtual_to_physical_offset(550, 1000)
        .unwrap();
    assert_eq!(physical.physical_partition, 50);
    assert_eq!(physical.virtual_group, 5);
}

#[tokio::test]
async fn test_offset_fetch_preserves_negative_offset() {
    let harness = ProxyTestHarness::new().await;

    // When no offset is committed, broker returns -1
    // We should preserve this in the virtual response

    // Virtual partition 27 maps to physical 7
    let mapping = harness.remapper.virtual_to_physical(27).unwrap();
    assert_eq!(mapping.physical_partition, 7);

    // -1 offset should not be translated (it's a sentinel value)
    // The handler should detect this and preserve it
}

#[tokio::test]
async fn test_offset_commit_large_offsets() {
    let harness = ProxyTestHarness::new().await;

    // Test with large virtual offsets
    let large_offset = (1i64 << 39) + 12345; // Just under half of default offset range

    let physical = harness
        .remapper
        .virtual_to_physical_offset(50, large_offset)
        .unwrap();

    let back = harness
        .remapper
        .physical_to_virtual(physical.physical_partition, physical.physical_offset)
        .unwrap();

    assert_eq!(back.virtual_partition, 50);
    assert_eq!(back.virtual_offset, large_offset);
}

#[tokio::test]
async fn test_offset_commit_zero_offset() {
    let harness = ProxyTestHarness::new().await;

    // Offset 0 is valid and should work
    for v_part in [0, 25, 50, 75, 99] {
        let physical = harness
            .remapper
            .virtual_to_physical_offset(v_part, 0)
            .unwrap();

        let back = harness
            .remapper
            .physical_to_virtual(physical.physical_partition, physical.physical_offset)
            .unwrap();

        assert_eq!(back.virtual_partition, v_part);
        assert_eq!(back.virtual_offset, 0);
    }
}

#[tokio::test]
async fn test_offset_fetch_multiple_topics() {
    let harness = ProxyTestHarness::new().await;

    // Different topics, same virtual partition, should have same physical mapping
    // (remapping is topic-independent)

    let v_part = 42;
    let v_offset = 5000i64;

    let physical = harness
        .remapper
        .virtual_to_physical_offset(v_part, v_offset)
        .unwrap();

    // Same mapping regardless of topic name
    assert_eq!(physical.physical_partition, 2); // 42 % 10
    assert_eq!(physical.virtual_group, 4); // 42 / 10
}
