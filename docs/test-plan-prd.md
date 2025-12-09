# Kafka Partition Remapping Proxy: Comprehensive Test Plan PRD

## Executive Summary

This PRD defines a complete testing strategy for the **Kafka Partition Remapping Proxy**—a critical system that intercepts Kafka protocol messages and remaps virtual partitions to physical partitions for cost optimization.

The test plan mirrors Apache Kafka's testing approach ([web:129][web:130][web:132][web:135][web:145]) while adding partition-specific validation to ensure:
- ✅ Offset translation correctness (virtual ↔ physical)
- ✅ Consumer group semantics preserved across remapping
- ✅ Message ordering maintained per key
- ✅ No data loss or corruption
- ✅ Latency acceptable for production use

---

## Testing Strategy Overview

### Test Pyramid

```
                    ▲
                   /|\
                  / | \
                 /  |  \ E2E Tests (Scenario validation)
                /   |   \ 5-10 tests
               /    |    \
              /_____|_____\
              
             ╱ │ ╲ Integration Tests
            ╱  │  ╲ (Kafka cluster simulation)
           ╱   │   ╲ 50-75 tests
          ╱____|____╲
          
    ┌────────────────────┐ Unit Tests
    │   Unit Tests       │ (Core logic)
    │   100+ tests       │
    └────────────────────┘
```

### Testing Layers

| Layer | Focus | Tools | Coverage Target |
|-------|-------|-------|-----------------|
| **Unit** | PartitionRemapper, offset translation logic | Rust unit tests (`cargo test`) | 90%+ |
| **Integration** | Kafka protocol handling, broker interaction | Docker + testcontainers + embedded Kafka | 75%+ |
| **E2E** | Real production scenarios | Real Kafka cluster + producer/consumer tools | 60%+ |
| **Performance** | Latency, throughput under load | kafka-producer-perf-test, custom benchmarks | vs baseline |

---

## Unit Tests

### 1. PartitionRemapper Core Logic

**Goal**: Validate deterministic offset/partition mapping without I/O

#### Test 1.1: Virtual to Physical Partition Mapping

```rust
#[test]
fn test_virtual_to_physical_partition_basic() {
    // 100 virtual on 10 physical = 10:1 ratio
    let remapper = PartitionRemapper::new(100, 10);
    
    // Partition 0-9 → physical 0
    assert_eq!(remapper.virtual_to_physical(0), 0);
    assert_eq!(remapper.virtual_to_physical(9), 0);
    
    // Partition 10-19 → physical 1
    assert_eq!(remapper.virtual_to_physical(10), 1);
    assert_eq!(remapper.virtual_to_physical(19), 1);
    
    // Partition 90-99 → physical 9
    assert_eq!(remapper.virtual_to_physical(90), 9);
    assert_eq!(remapper.virtual_to_physical(99), 9);
}

#[test]
fn test_virtual_to_physical_large_compression() {
    // 1000 virtual on 100 physical = 10:1
    let remapper = PartitionRemapper::new(1000, 100);
    
    for v_part in 0..1000 {
        let p_part = remapper.virtual_to_physical(v_part);
        assert!(p_part < 100, "Physical partition out of range");
        
        // Each physical partition should map exactly 10 virtual partitions
        let count = (0..1000)
            .filter(|v| remapper.virtual_to_physical(*v) == p_part)
            .count();
        assert_eq!(count, 10);
    }
}

#[test]
fn test_virtual_partition_uniqueness() {
    // Each virtual partition maps to exactly one physical partition
    let remapper = PartitionRemapper::new(100, 10);
    
    for v_part in 0..100 {
        let p_part = remapper.virtual_to_physical(v_part);
        
        // Reverse mapping should give same result
        let v_parts_in_p = (0..100)
            .filter(|v| remapper.virtual_to_physical(*v) == p_part)
            .collect::<Vec<_>>();
        
        assert!(v_parts_in_p.contains(&v_part));
    }
}

#[test]
#[should_panic]
fn test_non_divisible_partition_count() {
    // Should panic: 100 % 7 != 0
    PartitionRemapper::new(100, 7);
}
```

#### Test 1.2: Offset Translation Roundtrip

```rust
#[test]
fn test_offset_translation_roundtrip() {
    let remapper = PartitionRemapper::new(1000, 100);
    
    let test_cases = vec![
        (0, 0),           // First partition, offset 0
        (127, 5000),      // Mid partition
        (999, 999_999),   // Last partition, large offset
        (500, 1_000_000_000_000 - 1), // Near boundary
    ];
    
    for (v_part, v_offset) in test_cases {
        // Virtual → Physical
        let phys_offset = remapper.virtual_to_physical_offset(v_part, v_offset);
        
        // Physical → Virtual
        let (v_part_back, v_offset_back) = remapper.physical_to_virtual_offset(phys_offset);
        
        // Should match original
        assert_eq!(v_part_back, v_part, "Partition roundtrip failed");
        assert_eq!(v_offset_back, v_offset, "Offset roundtrip failed");
    }
}

#[test]
fn test_offset_no_overlap() {
    // Virtual partitions on same physical partition must have non-overlapping offsets
    let remapper = PartitionRemapper::new(100, 10);
    
    let phys_part = 5u32;
    let virtual_parts: Vec<u32> = (0..100)
        .filter(|v| remapper.virtual_to_physical(*v) == phys_part)
        .collect();
    
    // For each virtual partition in this physical partition:
    for i in 0..virtual_parts.len() {
        for j in (i+1)..virtual_parts.len() {
            let v_part_i = virtual_parts[i];
            let v_part_j = virtual_parts[j];
            
            // Offset ranges should not overlap
            let (offset_i_start, offset_i_end) = (
                (v_part_i % 10) as u64 * remapper.offset_range,
                ((v_part_i % 10) as u64 + 1) * remapper.offset_range,
            );
            let (offset_j_start, offset_j_end) = (
                (v_part_j % 10) as u64 * remapper.offset_range,
                ((v_part_j % 10) as u64 + 1) * remapper.offset_range,
            );
            
            // No overlap
            assert!(offset_i_end <= offset_j_start || offset_j_end <= offset_i_start);
        }
    }
}

#[test]
fn test_offset_determinism() {
    // Same input → same output, always
    let remapper = PartitionRemapper::new(100, 10);
    
    for _ in 0..1000 {
        let v_part = rand::random::<u32>() % 100;
        let v_offset = rand::random::<u64>() % (1_000_000_000_000);
        
        let result1 = remapper.virtual_to_physical_offset(v_part, v_offset);
        let result2 = remapper.virtual_to_physical_offset(v_part, v_offset);
        
        assert_eq!(result1, result2, "Offset translation not deterministic");
    }
}
```

#### Test 1.3: Edge Cases

```rust
#[test]
fn test_zero_offset() {
    let remapper = PartitionRemapper::new(100, 10);
    
    for v_part in 0..100 {
        let (p_part, p_offset) = remapper.virtual_to_physical_offset(v_part, 0);
        
        // Offset 0 in virtual should map to (v_part % 10) * offset_range
        let expected_offset = ((v_part % 10) as u64) * remapper.offset_range;
        assert_eq!(p_offset, expected_offset);
        
        // Should reverse correctly
        let (v_part_back, v_offset_back) = remapper.physical_to_virtual_offset(p_offset);
        assert_eq!(v_part_back, v_part);
        assert_eq!(v_offset_back, 0);
    }
}

#[test]
fn test_max_offset_boundary() {
    let remapper = PartitionRemapper::new(100, 10);
    
    // Test at boundary of offset range
    let v_part = 27;
    let boundary_offset = remapper.offset_range - 1;
    
    let (p_part, p_offset) = remapper.virtual_to_physical_offset(v_part, boundary_offset);
    
    // Should be just before the next virtual partition's range
    let next_v_part = v_part + 10;
    let (next_p_part, next_p_offset) = remapper.virtual_to_physical_offset(next_v_part, 0);
    
    assert!(p_offset < next_p_offset || (p_part != next_p_part));
}

#[test]
fn test_single_partition_mapping() {
    // Edge case: 10 virtual on 10 physical = 1:1 (no compression)
    let remapper = PartitionRemapper::new(10, 10);
    
    for v_part in 0..10 {
        assert_eq!(remapper.virtual_to_physical(v_part), v_part);
    }
}

#[test]
fn test_high_compression_ratio() {
    // Edge case: 10,000 virtual on 100 physical = 100:1
    let remapper = PartitionRemapper::new(10_000, 100);
    
    for v_part in 0..10_000 {
        let p_part = remapper.virtual_to_physical(v_part);
        assert!(p_part < 100);
    }
}
```

### 2. Message Key Hashing Preservation

**Goal**: Ensure same key routes to same partition before and after remapping

```rust
#[test]
fn test_key_routing_consistency() {
    let remapper = PartitionRemapper::new(100, 10);
    
    let test_keys = vec![
        "customer:123",
        "order:abc-456",
        "user@example.com",
        "payment-key-789",
    ];
    
    for key in test_keys {
        // Original Kafka hash
        let hash = fnv_hash(key);
        let virtual_partition = (hash as u32) % 100;
        
        // That virtual partition should always map to same physical
        let physical_partition = remapper.virtual_to_physical(virtual_partition);
        
        // If we hash again, we get same route
        let hash2 = fnv_hash(key);
        let virtual_partition2 = (hash2 as u32) % 100;
        assert_eq!(virtual_partition, virtual_partition2);
        
        // Same physical partition
        assert_eq!(physical_partition, remapper.virtual_to_physical(virtual_partition2));
    }
}

#[test]
fn test_all_keys_eventually_land() {
    // Every possible hash value should land somewhere
    let remapper = PartitionRemapper::new(1000, 100);
    
    let mut partitions_used = std::collections::HashSet::new();
    
    for i in 0..10_000 {
        let virtual_partition = i % 1000;
        let physical_partition = remapper.virtual_to_physical(virtual_partition as u32);
        partitions_used.insert(physical_partition);
    }
    
    // Should use all 100 physical partitions
    assert_eq!(partitions_used.len(), 100);
}
```

---

## Integration Tests

### 1. Protocol Request/Response Handlers

**Goal**: Validate Kafka protocol handling with full remapping

#### Test 1.1: ProduceRequest Remapping

```rust
#[tokio::test]
async fn test_produce_request_partition_remapping() {
    let mut proxy = ProxyTestHarness::new().await;
    let remapper = PartitionRemapper::new(100, 10);
    
    // Client produces to virtual partition 27
    let produce_request = ProduceRequest {
        topics: vec![TopicProduceData {
            topic: "orders".to_string(),
            partitions: vec![PartitionProduceData {
                partition: 27,  // VIRTUAL
                records: vec![RecordData {
                    offset: 0,
                    key: Some(b"order-123".to_vec()),
                    value: Some(b"payment data".to_vec()),
                }],
            }],
        }],
    };
    
    let response = proxy.handle_produce_request(produce_request).await.unwrap();
    
    // Verify:
    // 1. Response shows virtual partition 27
    assert_eq!(response.topics[0].partitions[0].partition, 27);
    
    // 2. Response has valid offset
    let returned_offset = response.topics[0].partitions[0].base_offset;
    assert!(returned_offset >= 0);
    
    // 3. Broker actually received on physical partition 2 (27 / 10 = 2)
    let broker_calls = proxy.broker_call_log();
    assert_eq!(broker_calls.last().unwrap().partition, 2);
    
    // 4. Offset was translated
    let expected_phys_offset = (27 % 10) as u64 * OFFSET_RANGE;
    assert_eq!(broker_calls.last().unwrap().offset, expected_phys_offset);
}

#[tokio::test]
async fn test_produce_multiple_partitions() {
    let mut proxy = ProxyTestHarness::new().await;
    
    let produce_request = ProduceRequest {
        topics: vec![TopicProduceData {
            topic: "orders".to_string(),
            partitions: vec![
                PartitionProduceData {
                    partition: 15,  // → physical 1
                    records: vec![RecordData {
                        key: Some(b"key1".to_vec()),
                        value: Some(b"val1".to_vec()),
                    }],
                },
                PartitionProduceData {
                    partition: 42,  // → physical 4
                    records: vec![RecordData {
                        key: Some(b"key2".to_vec()),
                        value: Some(b"val2".to_vec()),
                    }],
                },
            ],
        }],
    };
    
    let response = proxy.handle_produce_request(produce_request).await.unwrap();
    
    // Both should succeed
    assert_eq!(response.topics[0].partitions.len(), 2);
    assert_eq!(response.topics[0].partitions[0].error_code, 0);
    assert_eq!(response.topics[0].partitions[1].error_code, 0);
}

#[tokio::test]
async fn test_produce_offset_response_translation() {
    let mut proxy = ProxyTestHarness::new().await;
    
    // Produce to virtual partition 50, expecting certain offset back
    let produce_request = /* ... */;
    
    let response = proxy.handle_produce_request(produce_request).await.unwrap();
    let returned_virtual_offset = response.topics[0].partitions[0].base_offset;
    
    // Offset should be what the client expects (as if they wrote to virtual partition)
    // Not the physical offset the broker actually stored
    
    // Verify with subsequent fetch
    let fetch_response = proxy.fetch_from_virtual_partition(50, returned_virtual_offset).await;
    
    // Should get the message back
    assert_eq!(fetch_response.records.len(), 1);
}
```

#### Test 1.2: FetchRequest Remapping

```rust
#[tokio::test]
async fn test_fetch_request_partition_remapping() {
    let mut proxy = ProxyTestHarness::new().await;
    
    // First, seed some messages to physical partition 3
    // (which maps to virtual partitions 30-39)
    proxy.seed_broker_data(3, vec![
        (0, RecordData { offset: 0, key: b"k30", value: b"v30" }),
        (0, RecordData { offset: 1, key: b"k31", value: b"v31" }),
    ]).await;
    
    // Now fetch from virtual partition 30
    let fetch_request = FetchRequest {
        topics: vec![TopicFetchData {
            topic: "orders".to_string(),
            partitions: vec![PartitionFetchData {
                partition: 30,  // VIRTUAL → physical 3
                fetch_offset: 0,
                max_bytes: 10_000,
            }],
        }],
    };
    
    let response = proxy.handle_fetch_request(fetch_request).await.unwrap();
    
    // Verify:
    // 1. Response shows virtual partition 30
    assert_eq!(response.topics[0].partitions[0].partition, 30);
    
    // 2. Got messages
    assert_eq!(response.topics[0].partitions[0].records.len(), 2);
    
    // 3. Offsets are translated to virtual (0, 1 instead of physical)
    assert_eq!(response.topics[0].partitions[0].records[0].offset, 0);
    assert_eq!(response.topics[0].partitions[0].records[1].offset, 1);
    
    // 4. Messages are correct
    assert_eq!(response.topics[0].partitions[0].records[0].key, Some(b"k30".to_vec()));
}

#[tokio::test]
async fn test_fetch_across_virtual_partition_boundary() {
    // When fetching from a physical partition, should only return
    // messages belonging to requested virtual partition
    
    let mut proxy = ProxyTestHarness::new().await;
    
    // Seed physical partition 0 with messages from:
    // - Virtual partition 0-4 (offset range 0..50B)
    // - Virtual partition 5-9 (offset range 50B..100B)
    proxy.seed_broker_data(0, vec![
        (0, RecordData { offset: 10, key: b"vp0", value: b"data" }),      // vp 0
        (0, RecordData { offset: 40, key: b"vp4", value: b"data" }),      // vp 4
        (60, RecordData { offset: 60, key: b"vp5", value: b"data" }),     // vp 5
        (90, RecordData { offset: 90, key: b"vp9", value: b"data" }),     // vp 9
    ]).await;
    
    // Fetch from virtual partition 0
    let response = proxy.fetch_from_virtual_partition(0, 0).await;
    
    // Should only get messages from vp 0 and 4 (same physical, same offset range)
    // NOT messages from vp 5-9
    let records = response.records;
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].key, Some(b"vp0".to_vec()));
    assert_eq!(records[1].key, Some(b"vp4".to_vec()));
}

#[tokio::test]
async fn test_fetch_offset_translation() {
    let mut proxy = ProxyTestHarness::new().await;
    
    // Client wants to fetch from virtual partition 27, starting at virtual offset 1000
    let fetch_request = FetchRequest {
        topics: vec![TopicFetchData {
            topic: "orders".to_string(),
            partitions: vec![PartitionFetchData {
                partition: 27,
                fetch_offset: 1000,  // VIRTUAL offset
                max_bytes: 10_000,
            }],
        }],
    };
    
    let response = proxy.handle_fetch_request(fetch_request).await.unwrap();
    
    // Proxy should have sent to broker with:
    // Physical partition: 27 / 10 = 2
    // Physical offset: (27 % 10) * OFFSET_RANGE + 1000
    
    let broker_call = proxy.broker_call_log().last().unwrap();
    assert_eq!(broker_call.partition, 2);
    assert_eq!(
        broker_call.offset,
        (27 % 10) as u64 * OFFSET_RANGE + 1000
    );
}
```

#### Test 1.3: MetadataRequest Expansion

```rust
#[tokio::test]
async fn test_metadata_partition_expansion() {
    let mut proxy = ProxyTestHarness::new().await;
    
    // Broker has 10 physical partitions
    // Proxy configured for 100 virtual partitions
    
    let metadata_request = MetadataRequest {
        topics: vec!["orders".to_string()],
    };
    
    let response = proxy.handle_metadata_request(metadata_request).await.unwrap();
    
    // Should report 100 partitions, not 10
    assert_eq!(response.topics[0].partitions.len(), 100);
    
    // Partition IDs should be 0..99
    for i in 0..100 {
        assert_eq!(response.topics[0].partitions[i].partition, i as i32);
    }
    
    // Leader/replica info should be copied from physical partitions
    for i in 0..100 {
        let physical_idx = i / 10;  // 100 virtual → 10 physical
        
        let virtual_leader = response.topics[0].partitions[i].leader;
        let physical_leader = response.topics[0].partitions[physical_idx].leader;
        
        // Should mirror the physical partition's leader
        assert_eq!(virtual_leader, physical_leader);
    }
}

#[tokio::test]
async fn test_metadata_all_virtual_partitions_listed() {
    let mut proxy = ProxyTestHarness::new().await;
    let remapper = PartitionRemapper::new(1000, 100);
    
    let response = proxy.get_metadata("orders").await;
    
    // Should have 1000 partitions
    assert_eq!(response.partitions.len(), 1000);
    
    // Check that all virtual partitions are represented
    let partition_ids: Vec<i32> = response.partitions.iter()
        .map(|p| p.partition)
        .collect();
    
    for i in 0..1000 {
        assert!(partition_ids.contains(&(i as i32)));
    }
}
```

### 2. Consumer Group Tests

**Goal**: Validate group coordination and partition assignment

#### Test 2.1: Consumer Group Rebalancing

```rust
#[tokio::test]
async fn test_single_consumer_gets_all_partitions() {
    let mut proxy = ProxyTestHarness::new().await;
    
    // Consumer joins group
    let join_response = proxy.join_group("orders-group").await;
    
    // Should be assigned all 100 virtual partitions
    let assignments = proxy.get_consumer_assignments("orders-group").await;
    
    assert_eq!(assignments.len(), 100);
    assert!(assignments.contains(&0));
    assert!(assignments.contains(&99));
}

#[tokio::test]
async fn test_two_consumers_split_partitions() {
    let mut proxy = ProxyTestHarness::new().await;
    
    // Consumer 1 joins
    let consumer1 = proxy.join_consumer_group("group", "consumer-1").await;
    let mut assignments1 = proxy.get_consumer_assignments("group").await;
    
    // Consumer 2 joins → rebalance
    let consumer2 = proxy.join_consumer_group("group", "consumer-2").await;
    
    // Assignments should change
    let new_assignments1 = proxy.get_consumer_assignments_for_consumer("group", "consumer-1").await;
    let new_assignments2 = proxy.get_consumer_assignments_for_consumer("group", "consumer-2").await;
    
    // Each should have roughly half
    assert_eq!(new_assignments1.len() + new_assignments2.len(), 100);
    
    // No overlap
    let mut all: Vec<_> = new_assignments1.iter().chain(new_assignments2.iter()).collect();
    all.sort();
    for i in 0..all.len()-1 {
        assert_ne!(all[i], all[i+1], "Duplicate partition assignment");
    }
}

#[tokio::test]
async fn test_consumer_scales_up_then_down() {
    let mut proxy = ProxyTestHarness::new().await;
    
    // Start with 1 consumer
    let c1 = proxy.join_consumer_group("group", "c1").await;
    let assign1 = proxy.get_assignments("group").await;
    
    // Add 2 more consumers
    let c2 = proxy.join_consumer_group("group", "c2").await;
    let c3 = proxy.join_consumer_group("group", "c3").await;
    
    let assign3 = proxy.get_assignments("group").await;
    
    // Should be more evenly distributed
    assert!(assign3[0].len() <= assign1[0].len());
    
    // Remove 1 consumer
    proxy.leave_consumer_group("group", "c3").await;
    
    let assign2 = proxy.get_assignments("group").await;
    
    // C1 and C2 should pick up C3's partitions
    assert!(assign2.len() == 2);
}

#[tokio::test]
async fn test_consumer_group_offset_tracking() {
    let mut proxy = ProxyTestHarness::new().await;
    
    let consumer = proxy.join_consumer_group("group", "consumer-1").await;
    
    // Consume some messages from virtual partition 0
    let messages = proxy.consume_from_partition("group", "consumer-1", 0, 0, 100).await;
    
    // Commit offset
    let last_offset = messages.last().unwrap().offset;
    proxy.commit_offset("group", "consumer-1", 0, last_offset).await;
    
    // Check committed offset
    let committed = proxy.get_committed_offset("group", "consumer-1", 0).await;
    assert_eq!(committed, last_offset);
    
    // Consumer should resume from here on next join
    let next_consumer = proxy.join_consumer_group("group", "consumer-2").await;
    let next_messages = proxy.consume_from_partition("group", "consumer-2", 0, committed + 1, 100).await;
    
    // Should start from after committed offset
    assert!(next_messages[0].offset > last_offset);
}

#[tokio::test]
async fn test_virtual_offset_commit_translation() {
    // When consumer commits virtual offset, proxy stores physical offset internally
    // but consumer always sees virtual offsets
    
    let mut proxy = ProxyTestHarness::new().await;
    
    let consumer = proxy.join_consumer_group("group", "c1").await;
    
    // Commit virtual offset 5000 to virtual partition 27
    proxy.commit_offset("group", "c1", 27, 5000).await;
    
    // Get committed offset (should be virtual)
    let committed_virtual = proxy.get_committed_offset("group", "c1", 27).await;
    assert_eq!(committed_virtual, 5000);
    
    // Broker should have received physical offset
    let broker_commit = proxy.broker_call_log()
        .iter()
        .find(|c| c.method == "OffsetCommit")
        .unwrap();
    
    let expected_phys_offset = (27 % 10) as u64 * OFFSET_RANGE + 5000;
    assert_eq!(broker_commit.offset, expected_phys_offset);
}
```

#### Test 2.2: Partition Assignment Strategies

```rust
#[tokio::test]
async fn test_range_assignment() {
    // 100 virtual partitions, 4 consumers
    // Should assign 25 partitions per consumer
    
    let mut proxy = ProxyTestHarness::new()
        .with_assignment_strategy("range")
        .await;
    
    for i in 1..=4 {
        proxy.join_consumer_group("group", &format!("c{}", i)).await;
    }
    
    let assignments = proxy.get_all_assignments("group").await;
    
    // Each consumer gets 25
    for assignment in assignments.values() {
        assert_eq!(assignment.len(), 25);
    }
    
    // C1 gets 0-24, C2 gets 25-49, etc.
    let c1 = assignments.get("c1").unwrap();
    assert_eq!(c1[0], 0);
    assert_eq!(c1[24], 24);
}

#[tokio::test]
async fn test_roundrobin_assignment() {
    let mut proxy = ProxyTestHarness::new()
        .with_assignment_strategy("roundrobin")
        .await;
    
    for i in 1..=4 {
        proxy.join_consumer_group("group", &format!("c{}", i)).await;
    }
    
    let assignments = proxy.get_all_assignments("group").await;
    
    // Each consumer should have 25 partitions
    // Distributed round-robin style
    let c1 = assignments.get("c1").unwrap();
    
    // C1 should get: 0, 4, 8, 12, ...
    for (idx, &partition) in c1.iter().enumerate() {
        assert_eq!(partition % 4, 0); // C1 is index 0
    }
}

#[tokio::test]
async fn test_sticky_assignment_minimizes_movement() {
    let mut proxy = ProxyTestHarness::new()
        .with_assignment_strategy("sticky")
        .await;
    
    // Initial: 2 consumers
    proxy.join_consumer_group("group", "c1").await;
    proxy.join_consumer_group("group", "c2").await;
    
    let initial = proxy.get_all_assignments("group").await;
    
    // Add 1 more consumer
    proxy.join_consumer_group("group", "c3").await;
    
    let after = proxy.get_all_assignments("group").await;
    
    // Sticky should minimize changes
    // C1 and C2 should retain most of their partitions
    let c1_before = initial.get("c1").unwrap();
    let c1_after = after.get("c1").unwrap();
    
    let retained = c1_before.iter()
        .filter(|p| c1_after.contains(p))
        .count();
    
    // Should retain at least 60% of partitions
    assert!(retained as f32 > 0.6 * c1_before.len() as f32);
}
```

### 3. Offset Commit/Fetch Tests

```rust
#[tokio::test]
async fn test_offset_commit_request() {
    let mut proxy = ProxyTestHarness::new().await;
    
    let consumer = proxy.join_consumer_group("orders", "c1").await;
    
    // Commit virtual offset 1500 for virtual partition 42
    let commit_request = OffsetCommitRequest {
        group_id: "orders".to_string(),
        offsets: vec![OffsetCommitData {
            topic: "orders".to_string(),
            partitions: vec![OffsetCommitPartitionData {
                partition: 42,
                offset: 1500,
            }],
        }],
    };
    
    let response = proxy.handle_offset_commit(commit_request).await.unwrap();
    
    // Should succeed
    assert_eq!(response.topics[0].partitions[0].error_code, 0);
}

#[tokio::test]
async fn test_offset_fetch_request() {
    let mut proxy = ProxyTestHarness::new().await;
    
    // First commit an offset
    proxy.commit_offset("group", "c1", 27, 5000).await;
    
    // Now fetch it
    let fetch_request = OffsetFetchRequest {
        group_id: "group".to_string(),
        topics: vec![OffsetFetchTopicData {
            topic: "orders".to_string(),
            partitions: vec![27],
        }],
    };
    
    let response = proxy.handle_offset_fetch(fetch_request).await.unwrap();
    
    // Should return virtual offset 5000, not physical
    assert_eq!(response.topics[0].partitions[0].committed_offset, 5000);
}
```

### 4. Docker Compose Integration Setup

```yaml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_LOG_RETENTION_HOURS: 24
    ports:
      - "9093:9093"

  kafka-proxy:
    build:
      context: .
      dockerfile: Dockerfile
    depends_on:
      - kafka
    environment:
      KAFKA_BROKERS: kafka:9093
      VIRTUAL_PARTITIONS: 100
      PHYSICAL_PARTITIONS: 10
      PROXY_PORT: 9092
    ports:
      - "9092:9092"

  testcontainer:
    image: rust:latest
    working_dir: /app
    volumes:
      - .:/app
    command: cargo test --test integration_tests -- --test-threads=1
    depends_on:
      - kafka-proxy
```

---

## E2E Scenario Tests

### Scenario 1: Produce → Consume Roundtrip

```rust
#[tokio::test]
async fn test_produce_consume_roundtrip() {
    // Full test: Producer sends to virtual partition, consumer reads back
    
    let mut test_env = KafkaE2EEnvironment::new().await;
    
    // Start producer
    let producer = test_env.create_producer().await;
    
    // Produce 1000 messages to virtual partition 42
    for i in 0..1000 {
        let key = format!("key-{}", i);
        let value = format!("value-{}", i);
        
        producer.send_to_partition(42, key, value).await;
    }
    
    // Start consumer
    let consumer = test_env.create_consumer("group").await;
    consumer.subscribe_to_partition(42).await;
    
    // Consume messages
    let mut consumed = Vec::new();
    for _ in 0..1000 {
        if let Some(message) = consumer.next_message(Duration::from_secs(5)).await {
            consumed.push(message);
        }
    }
    
    // Verify
    assert_eq!(consumed.len(), 1000);
    assert_eq!(consumed[0].key, "key-0");
    assert_eq!(consumed[999].key, "key-999");
}

#[tokio::test]
async fn test_key_ordering_preserved() {
    // Same key always goes to same partition, ordering preserved
    
    let test_env = KafkaE2EEnvironment::new().await;
    let producer = test_env.create_producer().await;
    
    // Send 100 messages with same key
    for i in 0..100 {
        producer.send("order-123", format!("event-{}", i)).await;
    }
    
    // Consume
    let consumer = test_env.create_consumer("group").await;
    let messages = consumer.consume_all(Duration::from_secs(10)).await;
    
    // Should get them in order
    for (i, msg) in messages.iter().enumerate() {
        assert_eq!(msg.value, format!("event-{}", i));
    }
}

#[tokio::test]
async fn test_multiple_partitions_roundtrip() {
    // Producers to different virtual partitions, consumers consume independently
    
    let test_env = KafkaE2EEnvironment::new().await;
    let producer = test_env.create_producer().await;
    
    // Produce to 10 different virtual partitions
    for partition in 0..10 {
        for i in 0..100 {
            producer.send_to_partition(
                partition,
                format!("p{}-k{}", partition, i),
                format!("data"),
            ).await;
        }
    }
    
    // Create consumers for each partition
    let mut consumers = Vec::new();
    for partition in 0..10 {
        let consumer = test_env.create_consumer(&format!("group-{}", partition)).await;
        consumer.subscribe_to_partition(partition as i32).await;
        consumers.push(consumer);
    }
    
    // Consume from all
    for (partition, consumer) in consumers.iter().enumerate() {
        let messages = consumer.consume_all(Duration::from_secs(10)).await;
        
        assert_eq!(messages.len(), 100);
        for (i, msg) in messages.iter().enumerate() {
            assert_eq!(msg.key, format!("p{}-k{}", partition, i));
        }
    }
}
```

---

## Performance Tests

### Latency Benchmarks

```rust
#[test]
fn bench_produce_latency() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    
    rt.block_on(async {
        let mut proxy = ProxyTestHarness::new().await;
        
        let mut latencies = Vec::new();
        
        for _ in 0..1000 {
            let start = Instant::now();
            
            let produce_req = create_produce_request();
            proxy.handle_produce_request(produce_req).await.ok();
            
            latencies.push(start.elapsed());
        }
        
        let p50 = percentile(&latencies, 50);
        let p99 = percentile(&latencies, 99);
        let p999 = percentile(&latencies, 99.9);
        
        println!("Produce latency - P50: {:?}, P99: {:?}, P999: {:?}", p50, p99, p999);
        
        // Assert acceptable
        assert!(p99.as_millis() < 5, "P99 latency too high");
    });
}

#[test]
fn bench_fetch_latency() {
    // Similar to produce latency test
}

#[test]
fn bench_metadata_latency() {
    // Metadata request latency (should be very fast)
}

#[test]
fn bench_throughput() {
    // Measure messages/sec produced and consumed
}
```

---

## Test Infrastructure

### TestHarness Interface

```rust
pub struct ProxyTestHarness {
    proxy: KafkaPartitionRemappingProxy,
    mock_broker: MockBrokerServer,
    broker_call_log: Arc<Mutex<Vec<BrokerCall>>>,
}

impl ProxyTestHarness {
    pub async fn new() -> Self {
        // Spin up proxy + mock broker
    }
    
    pub async fn handle_produce_request(&mut self, req: ProduceRequest) -> Result<ProduceResponse> {
        // Send request through proxy
    }
    
    pub async fn handle_fetch_request(&mut self, req: FetchRequest) -> Result<FetchResponse> {
        // Send request through proxy
    }
    
    pub fn broker_call_log(&self) -> Vec<BrokerCall> {
        // Return all calls made to broker
    }
    
    pub async fn seed_broker_data(&mut self, partition: u32, data: Vec<RecordData>) {
        // Pre-populate broker with test data
    }
}
```

---

## Coverage Targets

| Component | Target | Method |
|-----------|--------|--------|
| PartitionRemapper | 95%+ | Unit tests |
| ProduceRequest handler | 90%+ | Integration tests |
| FetchRequest handler | 90%+ | Integration tests |
| MetadataRequest handler | 85%+ | Integration tests |
| Consumer group logic | 85%+ | Integration + E2E tests |
| Offset translation | 95%+ | Unit + integration |
| Error handling | 80%+ | All test types |

---

## Test Execution

### CI/CD Pipeline

```yaml
test:
  unit:
    script:
      - cargo test --lib -- --test-threads=$(nproc)
      - cargo test --doc
    coverage: 90%
    
  integration:
    script:
      - docker-compose up -d
      - cargo test --test integration_tests -- --test-threads=1
      - docker-compose down
    timeout: 10m
    
  e2e:
    script:
      - ./scripts/deploy-test-env.sh
      - cargo test --test e2e_tests -- --test-threads=1
      - ./scripts/teardown-test-env.sh
    timeout: 30m
    
  performance:
    script:
      - cargo bench --bench partition_remapping
    artifacts:
      - target/criterion/
```

### Run Commands

```bash
# Unit tests only
cargo test --lib

# Integration tests
docker-compose -f tests/docker-compose.yml up -d
cargo test --test '*'
docker-compose -f tests/docker-compose.yml down

# All tests with coverage
cargo tarpaulin --out Html --timeout 300

# Specific test
cargo test test_virtual_to_physical_partition_mapping -- --nocapture
```

---

## Validation Checklist

Before deploying to production:

- [ ] All unit tests pass (90%+ coverage)
- [ ] All integration tests pass
- [ ] All E2E scenario tests pass
- [ ] Performance benchmarks meet targets (P99 <5ms)
- [ ] Load testing with 100+ concurrent connections
- [ ] Offset translation verified in both directions
- [ ] Consumer group rebalancing tested (add/remove consumers)
- [ ] Error scenarios handled (broker failures, timeouts)
- [ ] Memory leaks checked (valgrind/heaptrack)
- [ ] No data loss in 24-hour burn test

---

## Summary

This test plan ensures:

✅ **Correctness** - Offset translation is mathematically verified  
✅ **Compatibility** - All Kafka protocol semantics preserved  
✅ **Performance** - <5ms P99 latency added by proxy  
✅ **Reliability** - 90%+ code coverage, all failure modes tested  
✅ **Scalability** - Tested with 100-1000 virtual partitions  

Follow Apache Kafka's testing best practices ([web:129][web:130][web:132][web:135][web:145]) while adding partition remapping validation.
