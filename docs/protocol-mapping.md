# Protocol Mapping Documentation

## Overview

The Kafka Partition Remapper intercepts Kafka protocol messages and transforms partition and offset references between the **virtual** space (what clients see) and **physical** space (what exists on brokers). This document explains the mathematical formulas, algorithms, and per-API handling.

## Core Concepts

### Virtual vs Physical Partitions

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Virtual Partition Space                          │
│  (What clients see)                                                  │
│                                                                      │
│  ┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬─────┬────┬────┬────┐   │
│  │ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │ 6 │ 7 │ 8 │ 9 │ ... │ 97 │ 98 │ 99 │   │
│  └───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴─────┴────┴────┴────┘   │
│    │   │   │   │   │   │   │   │   │   │         │    │    │       │
│    └───┼───┼───┼───┼───┼───┼───┼───┼───┼─────────┼────┼────┘       │
│        │   │   │   │   │   │   │   │   │         │    │            │
│        ▼   ▼   ▼   ▼   ▼   ▼   ▼   ▼   ▼         ▼    ▼            │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                Physical Partition Space                      │   │
│  │  (What exists on brokers)                                   │   │
│  │                                                              │   │
│  │  ┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐                  │   │
│  │  │ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │ 6 │ 7 │ 8 │ 9 │                  │   │
│  │  └───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘                  │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘

Example: 100 virtual partitions → 10 physical partitions (10:1 ratio)
```

### Key Parameters

| Parameter | Symbol | Description | Default |
|-----------|--------|-------------|---------|
| Virtual Partitions | V | Partition count clients see | Configured |
| Physical Partitions | P | Partition count on broker | Configured |
| Compression Ratio | C | V ÷ P (must be integer) | V/P |
| Offset Range | R | Max offsets per virtual partition | 2^40 |

### Constraints

- `P >= 1` (at least one physical partition)
- `V >= P` (virtual count >= physical count)
- `V % P == 0` (compression ratio must be integer)
- `R >= 2^20` (offset range minimum ~1 million)

## Partition Mapping

### Virtual to Physical Formula

```
physical_partition = virtual_partition % physical_partitions
virtual_group = virtual_partition / physical_partitions
```

### Mapping Example (100 Virtual → 10 Physical)

| Virtual Partition | Physical Partition | Virtual Group |
|-------------------|-------------------|---------------|
| 0 | 0 | 0 |
| 1 | 1 | 0 |
| 5 | 5 | 0 |
| 9 | 9 | 0 |
| 10 | 0 | 1 |
| 11 | 1 | 1 |
| 50 | 0 | 5 |
| 99 | 9 | 9 |

### Physical to Virtual (Reverse Mapping)

Given a physical partition, multiple virtual partitions map to it:

```
virtual_partitions_for_physical(P) = [P, P+10, P+20, ..., P+90]
```

For physical partition 0: virtual partitions [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]

## Offset Mapping

Offsets need transformation because multiple virtual partitions share a physical partition. Each virtual partition gets a dedicated **offset range** within the physical partition's offset space.

### Virtual to Physical Offset Formula

```
physical_offset = (virtual_group * offset_range) + virtual_offset
```

Where:
- `virtual_group` = `virtual_partition / physical_partitions`
- `offset_range` = configurable (default 2^40)

### Physical to Virtual Offset Formula

```
virtual_group = physical_offset / offset_range
virtual_offset = physical_offset % offset_range
virtual_partition = physical_partition + (virtual_group * physical_partitions)
```

### Offset Space Visualization

```
Physical Partition 0 Offset Space:
┌───────────────────────────────────────────────────────────────────────────┐
│                                                                           │
│  Virtual Partition 0      Virtual Partition 10     Virtual Partition 20   │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐    │
│  │ Offsets 0 to R-1 │    │ Offsets R to 2R-1│    │ Offsets 2R to 3R-1│   │
│  │ (Group 0)        │    │ (Group 1)        │    │ (Group 2)         │   │
│  └──────────────────┘    └──────────────────┘    └──────────────────┘    │
│  │◄── offset_range ──►│                                                   │
│                                                                           │
│  ... continues for each virtual group ...                                 │
│                                                                           │
│  Virtual Partition 90                                                     │
│  ┌──────────────────┐                                                    │
│  │ Offsets 9R to 10R-1│                                                  │
│  │ (Group 9)         │                                                   │
│  └──────────────────┘                                                    │
└───────────────────────────────────────────────────────────────────────────┘
```

### Offset Mapping Example

Configuration: 100 virtual, 10 physical, offset_range = 1,000,000,000,000 (10^12)

| Virtual Partition | Virtual Offset | Physical Partition | Physical Offset |
|-------------------|----------------|-------------------|-----------------|
| 0 | 0 | 0 | 0 |
| 0 | 100 | 0 | 100 |
| 10 | 0 | 0 | 1,000,000,000,000 |
| 10 | 100 | 0 | 1,000,000,000,100 |
| 50 | 500 | 0 | 5,000,000,000,500 |
| 99 | 0 | 9 | 9,000,000,000,000 |

## API-Specific Handling

### Metadata API (Key: 3)

The Metadata response is the most complex transformation, as it defines the partition topology clients see.

**Request Handling:**
- Pass through unchanged (topic list)

**Response Transformation:**

1. **Broker Address Rewriting:**
   All broker endpoints are rewritten to the proxy's advertised address:
   ```
   Original: broker-1.kafka.svc:9092, broker-2.kafka.svc:9092
   Rewritten: proxy.example.com:9092, proxy.example.com:9092
   ```
   This ensures all client connections route through the proxy.

2. **Partition Expansion:**
   For each physical partition, create C (compression ratio) virtual partitions:
   ```rust
   for physical_partition in 0..physical_partitions {
       for group in 0..compression_ratio {
           let virtual_partition = physical_partition + (group * physical_partitions);
           // Create virtual partition metadata with same leader/ISR as physical
       }
   }
   ```

**Example Transformation:**

Physical Response (10 partitions):
```json
{
  "topics": [{
    "name": "orders",
    "partitions": [
      {"partition": 0, "leader": 1, "isr": [1, 2]},
      {"partition": 1, "leader": 2, "isr": [2, 1]},
      // ... partitions 2-9
    ]
  }]
}
```

Virtual Response (100 partitions):
```json
{
  "topics": [{
    "name": "orders",
    "partitions": [
      {"partition": 0, "leader": 1, "isr": [1, 2]},   // Physical 0
      {"partition": 1, "leader": 2, "isr": [2, 1]},   // Physical 1
      // ... partitions 2-9 (same as physical)
      {"partition": 10, "leader": 1, "isr": [1, 2]},  // Physical 0 again
      {"partition": 11, "leader": 2, "isr": [2, 1]},  // Physical 1 again
      // ... up to partition 99
    ]
  }]
}
```

### Produce API (Key: 0)

**Request Transformation:**

1. Map virtual partition to physical:
   ```rust
   let mapping = remapper.virtual_to_physical(virtual_partition)?;
   request.partition = mapping.physical_partition;
   ```

2. Track mapping for response correlation:
   ```rust
   struct PartitionMapping {
       virtual_partition: i32,
       physical_partition: i32,
       virtual_group: u32,
   }
   ```

**Response Transformation:**

1. Translate base_offset from physical to virtual:
   ```rust
   let virtual_mapping = remapper.physical_to_virtual(
       physical_partition,
       response.base_offset
   )?;
   response.base_offset = virtual_mapping.virtual_offset;
   ```

2. Restore virtual partition in response

**Flow Diagram:**
```
Client Request               Proxy                    Broker
─────────────────           ─────                    ──────
Produce to VP 50            Map to PP 0              Receive PP 0
Offset: N/A                 Group: 5                 Offset: N/A
        │                       │                        │
        └──────────────────────►│                        │
                                └───────────────────────►│
                                                         │
                                │◄───────────────────────┘
                                │ Offset: 5,000,000,000,042
        │◄──────────────────────┘
        │ Translate to VP 50
        │ Offset: 42
```

### Fetch API (Key: 1)

The most complex handler due to offset translation and response filtering.

**Request Transformation:**

1. Map virtual partition to physical:
   ```rust
   let offset_mapping = remapper.virtual_to_physical_offset(
       virtual_partition,
       fetch_offset
   )?;
   request.partition = offset_mapping.physical_partition;
   request.fetch_offset = offset_mapping.physical_offset;
   ```

2. Track mapping for response processing:
   ```rust
   struct FetchMapping {
       virtual_partition: i32,
       physical_partition: i32,
       virtual_group: u32,
       original_offset: i64,
   }
   ```

**Response Transformation:**

1. **Offset Translation:**
   ```rust
   response.high_watermark = remapper.physical_to_virtual(
       physical_partition, high_watermark
   )?.virtual_offset;

   response.log_start_offset = remapper.physical_to_virtual(
       physical_partition, log_start_offset
   )?.virtual_offset;

   response.last_stable_offset = remapper.physical_to_virtual(
       physical_partition, last_stable_offset
   )?.virtual_offset;
   ```

2. **Record Filtering:**
   Only return records belonging to the requested virtual partition:
   ```rust
   for record_batch in response.records {
       let base_offset = record_batch.base_offset;
       if remapper.offset_belongs_to_virtual(
           physical_partition,
           base_offset,
           virtual_partition
       )? {
           // Include this batch
           virtual_response.records.push(translated_batch);
       }
   }
   ```

3. **Duplicate Responses:**
   If multiple virtual partitions map to the same physical partition and are requested together, the physical response is duplicated and filtered for each.

**Flow Diagram:**
```
Client Request               Proxy                    Broker
─────────────────           ─────                    ──────
Fetch from VP 50            Map to PP 0              Fetch from PP 0
Offset: 100                 Physical offset:         Offset: 5T+100
                            5,000,000,000,100
        │                       │                        │
        └──────────────────────►│                        │
                                └───────────────────────►│
                                                         │
                                │◄───────────────────────┘
                                │ Records from PP 0
                                │ Filter for Group 5 only
        │◄──────────────────────┘
        │ Records for VP 50
        │ Offset: 100, 101, 102...
```

### OffsetCommit API (Key: 8)

**Request Transformation:**
```rust
for partition in request.partitions {
    let offset_mapping = remapper.virtual_to_physical_offset(
        partition.virtual_partition,
        partition.committed_offset
    )?;
    partition.partition = offset_mapping.physical_partition;
    partition.committed_offset = offset_mapping.physical_offset;
}
```

**Response Transformation:**
- Restore virtual partition indices
- No offset translation needed (response only has error codes)

### OffsetFetch API (Key: 9)

**Request Transformation:**
```rust
for partition in request.partitions {
    let mapping = remapper.virtual_to_physical(partition.virtual_partition)?;
    partition.partition = mapping.physical_partition;
}
```

**Response Transformation:**
```rust
for partition in response.partitions {
    let virtual_mapping = remapper.physical_to_virtual(
        partition.physical_partition,
        partition.committed_offset
    )?;
    partition.partition = virtual_mapping.virtual_partition;
    partition.committed_offset = virtual_mapping.virtual_offset;
}
```

### Passthrough APIs

These APIs are forwarded without modification:

| API Key | Name | Reason |
|---------|------|--------|
| 10 | FindCoordinator | Group coordination uses group ID, not partitions |
| 11 | JoinGroup | Consumer group protocol |
| 12 | Heartbeat | Consumer group liveness |
| 13 | LeaveGroup | Consumer group departure |
| 14 | SyncGroup | Consumer group state sync |
| 2 | ListOffsets | Could be remapped but currently passthrough |

**Note:** ListOffsets may need remapping in future versions for full virtual partition support.

### ApiVersions (Key: 18)

Returns the proxy's supported API versions. No remapping - informs clients of capabilities.

## Per-Topic Configuration

Different topics can have different remapping configurations:

```yaml
mapping:
  # Global defaults
  virtual_partitions: 100
  physical_partitions: 10

  # Per-topic overrides
  topics:
    high-volume-events:
      virtual_partitions: 1000
      physical_partitions: 100

    # Regex pattern matching
    "metrics.*":
      virtual_partitions: 50
      physical_partitions: 5
```

### TopicRemapperRegistry

```rust
pub struct TopicRemapperRegistry {
    default_remapper: Arc<PartitionRemapper>,
    topic_remappers: DashMap<String, Arc<PartitionRemapper>>,
    config: MappingConfig,
    compiled_patterns: Vec<(String, Regex)>,
}

impl TopicRemapperRegistry {
    pub fn get_remapper(&self, topic: &str) -> Arc<PartitionRemapper> {
        // 1. Check exact match cache
        if let Some(remapper) = self.topic_remappers.get(topic) {
            return remapper.clone();
        }

        // 2. Check exact match in config
        if let Some(config) = self.config.topics.get(topic) {
            let remapper = create_remapper(config);
            self.topic_remappers.insert(topic.to_string(), remapper.clone());
            return remapper;
        }

        // 3. Check regex patterns
        for (pattern, regex) in &self.compiled_patterns {
            if regex.is_match(topic) {
                let config = &self.config.topics[pattern];
                let remapper = create_remapper(config);
                self.topic_remappers.insert(topic.to_string(), remapper.clone());
                return remapper;
            }
        }

        // 4. Return default
        self.default_remapper.clone()
    }
}
```

## PartitionRemapper Implementation

### Core Structure

```rust
pub struct PartitionRemapper {
    virtual_partitions: u32,
    physical_partitions: u32,
    compression_ratio: u32,
    offset_range: u64,
}
```

### Key Methods

```rust
impl PartitionRemapper {
    /// Map virtual partition to physical
    pub fn virtual_to_physical(&self, virtual_partition: i32)
        -> RemapResult<PhysicalMapping>
    {
        self.validate_virtual_partition(virtual_partition)?;

        let vp = virtual_partition as u32;
        Ok(PhysicalMapping {
            physical_partition: (vp % self.physical_partitions) as i32,
            virtual_group: vp / self.physical_partitions,
        })
    }

    /// Map virtual partition + offset to physical
    pub fn virtual_to_physical_offset(
        &self,
        virtual_partition: i32,
        virtual_offset: i64
    ) -> RemapResult<PhysicalOffsetMapping> {
        let mapping = self.virtual_to_physical(virtual_partition)?;
        self.validate_virtual_offset(virtual_offset)?;

        let physical_offset = (mapping.virtual_group as i64 * self.offset_range as i64)
            + virtual_offset;

        Ok(PhysicalOffsetMapping {
            physical_partition: mapping.physical_partition,
            physical_offset,
            virtual_group: mapping.virtual_group,
        })
    }

    /// Map physical partition + offset back to virtual
    pub fn physical_to_virtual(
        &self,
        physical_partition: i32,
        physical_offset: i64
    ) -> RemapResult<VirtualMapping> {
        self.validate_physical_partition(physical_partition)?;
        self.validate_physical_offset(physical_offset)?;

        let virtual_group = (physical_offset as u64 / self.offset_range) as u32;
        let virtual_offset = (physical_offset as u64 % self.offset_range) as i64;
        let virtual_partition = physical_partition
            + (virtual_group * self.physical_partitions as u32) as i32;

        Ok(VirtualMapping {
            virtual_partition,
            virtual_offset,
        })
    }

    /// Check if a physical offset belongs to a specific virtual partition
    pub fn offset_belongs_to_virtual(
        &self,
        physical_partition: i32,
        physical_offset: i64,
        virtual_partition: i32
    ) -> RemapResult<bool> {
        let mapping = self.physical_to_virtual(physical_partition, physical_offset)?;
        Ok(mapping.virtual_partition == virtual_partition)
    }

    /// Get all virtual partitions that map to a physical partition
    pub fn virtual_partitions_for_physical(&self, physical_partition: i32)
        -> Vec<i32>
    {
        (0..self.compression_ratio)
            .map(|group| physical_partition + (group * self.physical_partitions as u32) as i32)
            .collect()
    }

    /// Get the physical offset range for a virtual partition
    pub fn get_offset_range_for_virtual(&self, virtual_partition: i32)
        -> RemapResult<(i64, i64)>
    {
        let mapping = self.virtual_to_physical(virtual_partition)?;
        let start = mapping.virtual_group as i64 * self.offset_range as i64;
        let end = start + self.offset_range as i64 - 1;
        Ok((start, end))
    }
}
```

### Validation

```rust
fn validate_virtual_partition(&self, partition: i32) -> RemapResult<()> {
    if partition < 0 {
        return Err(RemapError::NegativePartition(partition));
    }
    if partition as u32 >= self.virtual_partitions {
        return Err(RemapError::VirtualPartitionOutOfRange {
            partition,
            max_partition: self.virtual_partitions as i32 - 1,
        });
    }
    Ok(())
}

fn validate_virtual_offset(&self, offset: i64) -> RemapResult<()> {
    if offset < 0 {
        return Err(RemapError::NegativeOffset(offset));
    }
    if offset as u64 >= self.offset_range {
        return Err(RemapError::OffsetOverflow {
            partition: 0,
            offset,
        });
    }
    Ok(())
}
```

## Edge Cases and Limitations

### Offset Range Overflow

If a virtual partition produces more than `offset_range` messages, the offset will overflow into the next virtual group's space. The default offset_range of 2^40 (~1 trillion) makes this unlikely.

**Mitigation:** Configure appropriate offset_range based on expected throughput.

### Consumer Group Coordination

Consumer group assignment algorithms in the Kafka protocol use partition counts. The proxy virtualizes the partition count in Metadata responses, so consumer groups see virtual partitions.

**Note:** Consumer rebalancing works correctly because:
1. Clients see 100 partitions in metadata
2. They request assignment to virtual partitions
3. The proxy translates to physical partitions

### Transaction Support

Transactional produces and fetches work because:
- Producer ID is not remapped
- Sequence numbers are per-physical-partition
- Transaction markers are correctly filtered per virtual partition

### Compacted Topics

Log compaction works but with a caveat:
- Keys are compacted within the physical partition
- Different virtual partitions sharing a physical partition may have key collisions
- Use appropriate key design to avoid cross-virtual-partition key collisions

## Property-Based Testing

The remapping logic is validated with property-based tests:

```rust
#[test]
fn prop_offset_round_trip() {
    // virtual -> physical -> virtual should be identity
    for v in 0..virtual_partitions {
        for offset in sample_offsets {
            let physical = remapper.virtual_to_physical_offset(v, offset);
            let back = remapper.physical_to_virtual(
                physical.physical_partition,
                physical.physical_offset
            );
            assert_eq!(back.virtual_partition, v);
            assert_eq!(back.virtual_offset, offset);
        }
    }
}

#[test]
fn prop_disjoint_offset_ranges() {
    // Offset ranges for different virtual partitions must not overlap
    for v1 in 0..virtual_partitions {
        for v2 in 0..virtual_partitions {
            if v1 != v2 && v1 % physical == v2 % physical {
                let (start1, end1) = remapper.get_offset_range_for_virtual(v1);
                let (start2, end2) = remapper.get_offset_range_for_virtual(v2);
                assert!(end1 < start2 || end2 < start1);
            }
        }
    }
}
```

## Related Documentation

- [Architecture](./architecture.md) - Overall system design
- [Configuration Reference](./configuration.md) - Mapping configuration options
- [Getting Started](./getting-started.md) - Quick start guide
