//! Core partition remapping logic.
//!
//! Maps V virtual partitions to P physical partitions where V/P = C (compression ratio).
//! Each physical partition hosts C virtual partitions, each with a dedicated offset range.
//!
//! # Offset Layout
//!
//! Physical partition `p` hosts virtual partitions `p, p+P, p+2P, ...`
//!
//! Within a physical partition, offsets are segmented:
//! - Virtual partition V uses offsets `[virtual_group * offset_range, (virtual_group + 1) * offset_range)`
//! - Where `virtual_group = V / P`
//!
//! # Example
//!
//! With 100 virtual partitions and 10 physical partitions (10:1 ratio):
//! - Virtual partition 0 → Physical partition 0, group 0
//! - Virtual partition 10 → Physical partition 0, group 1
//! - Virtual partition 5 → Physical partition 5, group 0
//! - Virtual partition 95 → Physical partition 5, group 9

use crate::config::MappingConfig;
use crate::error::{RemapError, RemapResult};

/// Core partition remapping logic.
#[derive(Debug, Clone)]
pub struct PartitionRemapper {
    virtual_partitions: u32,
    physical_partitions: u32,
    compression_ratio: u32,
    offset_range: u64,
}

/// Result of mapping a virtual partition to physical.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhysicalMapping {
    /// The physical partition index.
    pub physical_partition: i32,
    /// Which "slot" within the physical partition (0 to compression_ratio-1).
    pub virtual_group: u32,
}

/// Result of mapping a virtual partition + offset to physical.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhysicalOffsetMapping {
    /// The physical partition index.
    pub physical_partition: i32,
    /// The translated physical offset.
    pub physical_offset: i64,
    /// Which "slot" within the physical partition.
    pub virtual_group: u32,
}

/// Result of reverse mapping from physical to virtual.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VirtualMapping {
    /// The virtual partition index.
    pub virtual_partition: i32,
    /// The translated virtual offset.
    pub virtual_offset: i64,
}

impl PartitionRemapper {
    /// Create a new remapper from configuration.
    ///
    /// The configuration should already be validated via [`MappingConfig::validate`].
    #[must_use]
    pub fn new(config: &MappingConfig) -> Self {
        Self {
            virtual_partitions: config.virtual_partitions,
            physical_partitions: config.physical_partitions,
            compression_ratio: config.compression_ratio(),
            offset_range: config.offset_range,
        }
    }

    /// Map a virtual partition to its physical partition.
    ///
    /// # Errors
    ///
    /// Returns an error if the virtual partition is negative or exceeds the configured maximum.
    pub fn virtual_to_physical(&self, virtual_partition: i32) -> RemapResult<PhysicalMapping> {
        // Validate input
        if virtual_partition < 0 {
            return Err(RemapError::NegativePartition(virtual_partition));
        }

        let v = virtual_partition as u32;
        if v >= self.virtual_partitions {
            return Err(RemapError::VirtualPartitionOutOfRange {
                partition: virtual_partition,
                max_partition: self.virtual_partitions,
            });
        }

        // physical_partition = virtual_partition % physical_partitions
        // virtual_group = virtual_partition / physical_partitions
        Ok(PhysicalMapping {
            physical_partition: (v % self.physical_partitions) as i32,
            virtual_group: v / self.physical_partitions,
        })
    }

    /// Map a virtual partition + offset to physical partition + offset.
    ///
    /// # Errors
    ///
    /// Returns an error if the virtual partition is out of range or the offset is negative.
    pub fn virtual_to_physical_offset(
        &self,
        virtual_partition: i32,
        virtual_offset: i64,
    ) -> RemapResult<PhysicalOffsetMapping> {
        if virtual_offset < 0 {
            return Err(RemapError::NegativeOffset(virtual_offset));
        }

        let mapping = self.virtual_to_physical(virtual_partition)?;

        // physical_offset = virtual_group * offset_range + virtual_offset
        let physical_offset = u64::from(mapping.virtual_group)
            .saturating_mul(self.offset_range)
            .saturating_add(virtual_offset as u64);

        Ok(PhysicalOffsetMapping {
            physical_partition: mapping.physical_partition,
            physical_offset: physical_offset as i64,
            virtual_group: mapping.virtual_group,
        })
    }

    /// Reverse map from physical partition + offset to virtual partition + offset.
    ///
    /// # Errors
    ///
    /// Returns an error if the offset is negative or does not map to a valid virtual partition.
    pub fn physical_to_virtual(
        &self,
        physical_partition: i32,
        physical_offset: i64,
    ) -> RemapResult<VirtualMapping> {
        if physical_offset < 0 {
            return Err(RemapError::NegativeOffset(physical_offset));
        }

        if physical_partition < 0 {
            return Err(RemapError::NegativePartition(physical_partition));
        }

        let offset = physical_offset as u64;

        // Determine which virtual group this offset belongs to
        let virtual_group = (offset / self.offset_range) as u32;
        let virtual_offset = (offset % self.offset_range) as i64;

        // virtual_partition = physical_partition + virtual_group * physical_partitions
        let virtual_partition =
            physical_partition as u32 + virtual_group * self.physical_partitions;

        // Check if this virtual partition is valid
        if virtual_partition >= self.virtual_partitions {
            return Err(RemapError::InvalidPhysicalOffset {
                offset: physical_offset,
                partition: physical_partition,
            });
        }

        Ok(VirtualMapping {
            virtual_partition: virtual_partition as i32,
            virtual_offset,
        })
    }

    /// Get the physical offset range for a specific virtual partition.
    ///
    /// Returns (start_offset, end_offset) where start is inclusive and end is exclusive.
    ///
    /// # Errors
    ///
    /// Returns an error if the virtual partition is out of range.
    pub fn get_offset_range_for_virtual(
        &self,
        virtual_partition: i32,
    ) -> RemapResult<(i64, i64)> {
        let mapping = self.virtual_to_physical(virtual_partition)?;
        let start = (u64::from(mapping.virtual_group) * self.offset_range) as i64;
        let end = start + self.offset_range as i64;
        Ok((start, end))
    }

    /// Check if a physical offset belongs to a specific virtual partition.
    ///
    /// # Errors
    ///
    /// Returns an error if the virtual partition is out of range.
    pub fn offset_belongs_to_virtual(
        &self,
        physical_partition: i32,
        physical_offset: i64,
        virtual_partition: i32,
    ) -> RemapResult<bool> {
        if physical_offset < 0 {
            return Ok(false);
        }

        let expected = self.virtual_to_physical(virtual_partition)?;
        if expected.physical_partition != physical_partition {
            return Ok(false);
        }

        let (start, end) = self.get_offset_range_for_virtual(virtual_partition)?;
        Ok(physical_offset >= start && physical_offset < end)
    }

    /// Get the number of virtual partitions.
    #[must_use]
    pub fn virtual_partitions(&self) -> u32 {
        self.virtual_partitions
    }

    /// Get the number of physical partitions.
    #[must_use]
    pub fn physical_partitions(&self) -> u32 {
        self.physical_partitions
    }

    /// Get the compression ratio (virtual / physical).
    #[must_use]
    pub fn compression_ratio(&self) -> u32 {
        self.compression_ratio
    }

    /// Get the offset range per virtual partition.
    #[must_use]
    pub fn offset_range(&self) -> u64 {
        self.offset_range
    }

    /// Get all virtual partitions that map to a given physical partition.
    #[must_use]
    pub fn virtual_partitions_for_physical(&self, physical_partition: i32) -> Vec<i32> {
        if physical_partition < 0 || physical_partition as u32 >= self.physical_partitions {
            return Vec::new();
        }

        let p = physical_partition as u32;
        (0..self.compression_ratio)
            .map(|group| (p + group * self.physical_partitions) as i32)
            .filter(|&v| (v as u32) < self.virtual_partitions)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> MappingConfig {
        MappingConfig {
            virtual_partitions: 100,
            physical_partitions: 10,
            offset_range: 1 << 20, // 1M offsets per virtual partition
        }
    }

    fn remapper() -> PartitionRemapper {
        PartitionRemapper::new(&test_config())
    }

    #[test]
    fn test_virtual_to_physical_basic() {
        let r = remapper();

        // Virtual 0 -> Physical 0, group 0
        let m = r.virtual_to_physical(0).unwrap();
        assert_eq!(m.physical_partition, 0);
        assert_eq!(m.virtual_group, 0);

        // Virtual 10 -> Physical 0, group 1
        let m = r.virtual_to_physical(10).unwrap();
        assert_eq!(m.physical_partition, 0);
        assert_eq!(m.virtual_group, 1);

        // Virtual 5 -> Physical 5, group 0
        let m = r.virtual_to_physical(5).unwrap();
        assert_eq!(m.physical_partition, 5);
        assert_eq!(m.virtual_group, 0);

        // Virtual 95 -> Physical 5, group 9
        let m = r.virtual_to_physical(95).unwrap();
        assert_eq!(m.physical_partition, 5);
        assert_eq!(m.virtual_group, 9);
    }

    #[test]
    fn test_virtual_to_physical_out_of_range() {
        let r = remapper();

        assert!(matches!(
            r.virtual_to_physical(100),
            Err(RemapError::VirtualPartitionOutOfRange { .. })
        ));

        assert!(matches!(
            r.virtual_to_physical(-1),
            Err(RemapError::NegativePartition(-1))
        ));
    }

    #[test]
    fn test_offset_round_trip() {
        let r = remapper();

        // Test various virtual partitions and offsets
        for v_part in [0, 5, 10, 50, 99] {
            for v_offset in [0i64, 100, 1000, (1 << 19)] {
                let physical = r.virtual_to_physical_offset(v_part, v_offset).unwrap();
                let back = r
                    .physical_to_virtual(physical.physical_partition, physical.physical_offset)
                    .unwrap();

                assert_eq!(
                    back.virtual_partition, v_part,
                    "partition mismatch for v_part={v_part}, v_offset={v_offset}"
                );
                assert_eq!(
                    back.virtual_offset, v_offset,
                    "offset mismatch for v_part={v_part}, v_offset={v_offset}"
                );
            }
        }
    }

    #[test]
    fn test_negative_offset() {
        let r = remapper();

        assert!(matches!(
            r.virtual_to_physical_offset(0, -1),
            Err(RemapError::NegativeOffset(-1))
        ));

        assert!(matches!(
            r.physical_to_virtual(0, -1),
            Err(RemapError::NegativeOffset(-1))
        ));
    }

    #[test]
    fn test_offset_range_boundaries() {
        let r = remapper();

        // Virtual partition 0 should have offset range [0, 2^20)
        let (start, end) = r.get_offset_range_for_virtual(0).unwrap();
        assert_eq!(start, 0);
        assert_eq!(end, 1 << 20);

        // Virtual partition 10 (group 1 on physical 0) should have range [2^20, 2^21)
        let (start, end) = r.get_offset_range_for_virtual(10).unwrap();
        assert_eq!(start, 1 << 20);
        assert_eq!(end, 2 << 20);
    }

    #[test]
    fn test_offset_belongs_to_virtual() {
        let r = remapper();

        // Offset 0 belongs to virtual partition 0
        assert!(r.offset_belongs_to_virtual(0, 0, 0).unwrap());

        // Offset 0 does not belong to virtual partition 10 (different group)
        assert!(!r.offset_belongs_to_virtual(0, 0, 10).unwrap());

        // Offset at 2^20 belongs to virtual partition 10
        assert!(r.offset_belongs_to_virtual(0, 1 << 20, 10).unwrap());

        // Offset at 2^20 does not belong to virtual partition 0
        assert!(!r.offset_belongs_to_virtual(0, 1 << 20, 0).unwrap());

        // Different physical partition
        assert!(!r.offset_belongs_to_virtual(1, 0, 0).unwrap());
    }

    #[test]
    fn test_virtual_partitions_for_physical() {
        let r = remapper();

        // Physical partition 0 hosts virtual partitions 0, 10, 20, ..., 90
        let vparts = r.virtual_partitions_for_physical(0);
        assert_eq!(vparts.len(), 10);
        assert_eq!(vparts, vec![0, 10, 20, 30, 40, 50, 60, 70, 80, 90]);

        // Physical partition 5 hosts virtual partitions 5, 15, 25, ..., 95
        let vparts = r.virtual_partitions_for_physical(5);
        assert_eq!(vparts.len(), 10);
        assert_eq!(vparts, vec![5, 15, 25, 35, 45, 55, 65, 75, 85, 95]);

        // Invalid physical partition
        let vparts = r.virtual_partitions_for_physical(10);
        assert!(vparts.is_empty());
    }

    #[test]
    fn test_invalid_physical_offset() {
        let r = remapper();

        // Physical offset too large (would map to virtual partition >= 100)
        let result = r.physical_to_virtual(0, 10 << 20); // 10 * 2^20, group 10
        assert!(matches!(
            result,
            Err(RemapError::InvalidPhysicalOffset { .. })
        ));
    }

    #[test]
    fn test_accessors() {
        let r = remapper();
        assert_eq!(r.virtual_partitions(), 100);
        assert_eq!(r.physical_partitions(), 10);
        assert_eq!(r.compression_ratio(), 10);
        assert_eq!(r.offset_range(), 1 << 20);
    }

    #[test]
    fn test_exhaustive_partition_mapping() {
        // Test ALL virtual partitions map correctly
        let r = remapper();

        for v in 0..100i32 {
            let m = r.virtual_to_physical(v).unwrap();

            // Verify formula: physical = virtual % physical_partitions
            assert_eq!(
                m.physical_partition,
                v % 10,
                "physical partition mismatch for virtual {v}"
            );

            // Verify formula: group = virtual / physical_partitions
            assert_eq!(
                m.virtual_group,
                (v / 10) as u32,
                "virtual group mismatch for virtual {v}"
            );
        }
    }

    #[test]
    fn test_different_compression_ratios() {
        // Test 2:1 ratio
        let config_2_1 = MappingConfig {
            virtual_partitions: 20,
            physical_partitions: 10,
            offset_range: 1 << 20,
        };
        let r = PartitionRemapper::new(&config_2_1);

        assert_eq!(r.virtual_to_physical(0).unwrap().physical_partition, 0);
        assert_eq!(r.virtual_to_physical(0).unwrap().virtual_group, 0);
        assert_eq!(r.virtual_to_physical(10).unwrap().physical_partition, 0);
        assert_eq!(r.virtual_to_physical(10).unwrap().virtual_group, 1);
        assert_eq!(r.virtual_to_physical(19).unwrap().physical_partition, 9);
        assert_eq!(r.virtual_to_physical(19).unwrap().virtual_group, 1);

        // Test 100:1 ratio
        let config_100_1 = MappingConfig {
            virtual_partitions: 1000,
            physical_partitions: 10,
            offset_range: 1 << 20,
        };
        let r = PartitionRemapper::new(&config_100_1);

        assert_eq!(r.virtual_to_physical(0).unwrap().physical_partition, 0);
        assert_eq!(r.virtual_to_physical(0).unwrap().virtual_group, 0);
        assert_eq!(r.virtual_to_physical(500).unwrap().physical_partition, 0);
        assert_eq!(r.virtual_to_physical(500).unwrap().virtual_group, 50);
        assert_eq!(r.virtual_to_physical(999).unwrap().physical_partition, 9);
        assert_eq!(r.virtual_to_physical(999).unwrap().virtual_group, 99);
    }

    #[test]
    fn test_offset_at_range_boundaries() {
        let r = remapper();
        let offset_range = r.offset_range() as i64;

        // Test at exact boundary - last valid offset for group 0
        let last_offset_group_0 = offset_range - 1;
        let m = r.virtual_to_physical_offset(0, last_offset_group_0).unwrap();
        assert_eq!(m.physical_offset, last_offset_group_0);

        let back = r.physical_to_virtual(m.physical_partition, m.physical_offset).unwrap();
        assert_eq!(back.virtual_partition, 0);
        assert_eq!(back.virtual_offset, last_offset_group_0);

        // Test first offset of group 1 (virtual partition 10)
        let m = r.virtual_to_physical_offset(10, 0).unwrap();
        assert_eq!(m.physical_offset, offset_range);
        assert_eq!(m.physical_partition, 0);

        let back = r.physical_to_virtual(m.physical_partition, m.physical_offset).unwrap();
        assert_eq!(back.virtual_partition, 10);
        assert_eq!(back.virtual_offset, 0);
    }

    #[test]
    fn test_large_offsets() {
        // Use default 2^40 offset range
        let config = MappingConfig {
            virtual_partitions: 100,
            physical_partitions: 10,
            offset_range: 1 << 40,
        };
        let r = PartitionRemapper::new(&config);

        // Test with large but valid offset
        let large_offset = (1i64 << 39) + 12345;
        let m = r.virtual_to_physical_offset(5, large_offset).unwrap();
        let back = r.physical_to_virtual(m.physical_partition, m.physical_offset).unwrap();

        assert_eq!(back.virtual_partition, 5);
        assert_eq!(back.virtual_offset, large_offset);
    }

    #[test]
    fn test_all_physical_partitions_have_correct_virtual_count() {
        let r = remapper();

        for p in 0..10 {
            let virtuals = r.virtual_partitions_for_physical(p);
            assert_eq!(
                virtuals.len(),
                10,
                "physical partition {p} should host exactly 10 virtual partitions"
            );

            // Verify each virtual partition maps back to this physical
            for v in virtuals {
                let m = r.virtual_to_physical(v).unwrap();
                assert_eq!(m.physical_partition, p);
            }
        }
    }

    // ========================================
    // Additional tests from PRD Test Plan
    // ========================================

    #[test]
    fn test_virtual_to_physical_large_compression() {
        // 1000 virtual on 100 physical = 10:1 ratio
        let config = MappingConfig {
            virtual_partitions: 1000,
            physical_partitions: 100,
            offset_range: 1 << 20,
        };
        let r = PartitionRemapper::new(&config);

        for v_part in 0i32..1000 {
            let p_part = r.virtual_to_physical(v_part).unwrap().physical_partition;
            assert!(
                p_part < 100,
                "Physical partition {p_part} out of range for virtual {v_part}"
            );

            // Each physical partition should map exactly 10 virtual partitions
            let count = (0i32..1000)
                .filter(|&v| r.virtual_to_physical(v).unwrap().physical_partition == p_part)
                .count();
            assert_eq!(
                count, 10,
                "Physical partition {p_part} should host exactly 10 virtual partitions"
            );
        }
    }

    #[test]
    fn test_virtual_partition_uniqueness() {
        // Each virtual partition maps to exactly one physical partition
        let r = remapper();

        for v_part in 0i32..100 {
            let p_part = r.virtual_to_physical(v_part).unwrap().physical_partition;

            // Collect all virtual partitions that map to this physical partition
            let v_parts_in_p: Vec<i32> = (0i32..100)
                .filter(|&v| r.virtual_to_physical(v).unwrap().physical_partition == p_part)
                .collect();

            // This virtual partition should be in the list
            assert!(
                v_parts_in_p.contains(&v_part),
                "Virtual partition {v_part} should be found in physical partition {p_part}'s mapping"
            );

            // All virtual partitions in the same physical should have non-overlapping offset ranges
            for other in &v_parts_in_p {
                if *other != v_part {
                    let (start_v, end_v) = r.get_offset_range_for_virtual(v_part).unwrap();
                    let (start_o, end_o) = r.get_offset_range_for_virtual(*other).unwrap();
                    assert!(
                        end_v <= start_o || end_o <= start_v,
                        "Offset ranges overlap for virtual partitions {v_part} and {other}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_non_divisible_partition_count_returns_error() {
        // 100 % 7 != 0, so validation should fail
        let config = MappingConfig {
            virtual_partitions: 100,
            physical_partitions: 7,
            offset_range: 1 << 20,
        };

        let result = config.validate();
        assert!(
            result.is_err(),
            "Config with non-divisible partition count should fail validation"
        );
    }

    #[test]
    fn test_zero_offset_all_partitions() {
        let r = remapper();

        for v_part in 0i32..100 {
            let mapping = r.virtual_to_physical_offset(v_part, 0).unwrap();

            // Offset 0 in virtual should map to (virtual_group * offset_range)
            let expected_offset =
                (v_part as i64 / 10) * (r.offset_range() as i64);
            assert_eq!(
                mapping.physical_offset, expected_offset,
                "Zero offset for virtual partition {v_part} should map to {expected_offset}"
            );

            // Should reverse correctly
            let back = r
                .physical_to_virtual(mapping.physical_partition, mapping.physical_offset)
                .unwrap();
            assert_eq!(back.virtual_partition, v_part);
            assert_eq!(back.virtual_offset, 0);
        }
    }

    #[test]
    fn test_single_partition_mapping_1_to_1() {
        // Edge case: 10 virtual on 10 physical = 1:1 (no compression)
        let config = MappingConfig {
            virtual_partitions: 10,
            physical_partitions: 10,
            offset_range: 1 << 20,
        };
        let r = PartitionRemapper::new(&config);

        for v_part in 0i32..10 {
            let mapping = r.virtual_to_physical(v_part).unwrap();
            assert_eq!(
                mapping.physical_partition, v_part,
                "In 1:1 mapping, virtual {v_part} should map to physical {v_part}"
            );
            assert_eq!(
                mapping.virtual_group, 0,
                "In 1:1 mapping, all virtual partitions should be in group 0"
            );
        }
    }

    #[test]
    fn test_high_compression_ratio_100_to_1() {
        // Edge case: 10,000 virtual on 100 physical = 100:1
        let config = MappingConfig {
            virtual_partitions: 10_000,
            physical_partitions: 100,
            offset_range: 1 << 20,
        };
        let r = PartitionRemapper::new(&config);

        for v_part in 0i32..10_000 {
            let mapping = r.virtual_to_physical(v_part).unwrap();
            assert!(
                mapping.physical_partition < 100,
                "Physical partition should be < 100"
            );
            assert!(
                mapping.virtual_group < 100,
                "Virtual group should be < 100"
            );
        }

        // Verify each physical partition hosts exactly 100 virtual partitions
        for p in 0i32..100 {
            let count = (0i32..10_000)
                .filter(|&v| r.virtual_to_physical(v).unwrap().physical_partition == p)
                .count();
            assert_eq!(
                count, 100,
                "Physical partition {p} should host exactly 100 virtual partitions"
            );
        }
    }

    #[test]
    fn test_offset_determinism() {
        // Same input should always produce same output
        let r = remapper();

        let test_cases: Vec<(i32, i64)> = vec![
            (0, 0),
            (27, 5000),
            (99, 999_999),
            (50, 12345),
        ];

        for (v_part, v_offset) in test_cases {
            let result1 = r.virtual_to_physical_offset(v_part, v_offset).unwrap();
            let result2 = r.virtual_to_physical_offset(v_part, v_offset).unwrap();

            assert_eq!(
                result1.physical_partition, result2.physical_partition,
                "Physical partition should be deterministic for ({v_part}, {v_offset})"
            );
            assert_eq!(
                result1.physical_offset, result2.physical_offset,
                "Physical offset should be deterministic for ({v_part}, {v_offset})"
            );

            // Also test reverse mapping determinism
            let back1 = r.physical_to_virtual(result1.physical_partition, result1.physical_offset).unwrap();
            let back2 = r.physical_to_virtual(result2.physical_partition, result2.physical_offset).unwrap();

            assert_eq!(back1.virtual_partition, back2.virtual_partition);
            assert_eq!(back1.virtual_offset, back2.virtual_offset);
        }
    }

    #[test]
    fn test_all_virtual_partitions_are_reachable() {
        // Every virtual partition should be assigned to some physical partition
        let r = remapper();

        let mut seen_virtual = std::collections::HashSet::new();

        for p in 0i32..10 {
            let virtuals = r.virtual_partitions_for_physical(p);
            for v in virtuals {
                assert!(
                    !seen_virtual.contains(&v),
                    "Virtual partition {v} assigned to multiple physical partitions"
                );
                seen_virtual.insert(v);
            }
        }

        // All 100 virtual partitions should be accounted for
        assert_eq!(
            seen_virtual.len(),
            100,
            "All virtual partitions should be reachable"
        );
        for v in 0i32..100 {
            assert!(
                seen_virtual.contains(&v),
                "Virtual partition {v} is not reachable"
            );
        }
    }

    #[test]
    fn test_offset_translation_roundtrip_exhaustive() {
        // Comprehensive roundtrip test from PRD
        let config = MappingConfig {
            virtual_partitions: 1000,
            physical_partitions: 100,
            offset_range: 1_000_000_000_000, // 1 trillion
        };
        let r = PartitionRemapper::new(&config);

        let test_cases: Vec<(i32, i64)> = vec![
            (0, 0),              // First partition, offset 0
            (127, 5000),         // Mid partition
            (999, 999_999),      // Last partition, large offset
            (500, 999_999_999_999), // Near boundary
        ];

        for (v_part, v_offset) in test_cases {
            // Virtual → Physical
            let phys = r.virtual_to_physical_offset(v_part, v_offset).unwrap();

            // Physical → Virtual
            let back = r.physical_to_virtual(phys.physical_partition, phys.physical_offset).unwrap();

            // Should match original
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
}

/// Property-based tests for the partition remapper
#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    fn arb_config() -> impl Strategy<Value = MappingConfig> {
        // Generate valid configs where virtual is divisible by physical
        (1u32..=10, 1u32..=100).prop_map(|(physical, ratio)| MappingConfig {
            virtual_partitions: physical * ratio,
            physical_partitions: physical,
            offset_range: 1 << 30, // 1B offsets per group
        })
    }

    proptest! {
        /// Round-trip property: v→p→v should return the original partition and offset
        #[test]
        fn prop_offset_round_trip(
            config in arb_config(),
            v_part_idx in 0usize..1000,
            v_offset in 0i64..1_000_000_000
        ) {
            let r = PartitionRemapper::new(&config);
            let v_part = (v_part_idx % config.virtual_partitions as usize) as i32;

            let physical = r.virtual_to_physical_offset(v_part, v_offset).unwrap();
            let back = r.physical_to_virtual(physical.physical_partition, physical.physical_offset).unwrap();

            prop_assert_eq!(back.virtual_partition, v_part);
            prop_assert_eq!(back.virtual_offset, v_offset);
        }

        /// Partition mapping property: physical = virtual % physical_partitions
        #[test]
        fn prop_partition_formula(
            config in arb_config(),
            v_part_idx in 0usize..1000
        ) {
            let r = PartitionRemapper::new(&config);
            let v_part = (v_part_idx % config.virtual_partitions as usize) as i32;

            let m = r.virtual_to_physical(v_part).unwrap();

            prop_assert_eq!(
                m.physical_partition,
                v_part % config.physical_partitions as i32
            );
            prop_assert_eq!(
                m.virtual_group,
                (v_part as u32) / config.physical_partitions
            );
        }

        /// Offset range property: offsets in range [start, end) belong to the virtual partition
        #[test]
        fn prop_offset_range_membership(
            config in arb_config(),
            v_part_idx in 0usize..1000,
            offset_in_range in 0i64..1_000_000
        ) {
            let r = PartitionRemapper::new(&config);
            let v_part = (v_part_idx % config.virtual_partitions as usize) as i32;

            let (start, end) = r.get_offset_range_for_virtual(v_part).unwrap();
            let physical_partition = r.virtual_to_physical(v_part).unwrap().physical_partition;

            // Any offset in [start, end) should belong to this virtual partition
            let test_offset = start + (offset_in_range % (end - start));
            prop_assert!(r.offset_belongs_to_virtual(physical_partition, test_offset, v_part).unwrap());

            // Offset just before start should NOT belong (unless start is 0)
            if start > 0 {
                prop_assert!(!r.offset_belongs_to_virtual(physical_partition, start - 1, v_part).unwrap());
            }
        }

        /// Disjoint ranges property: two different virtual partitions on same physical have disjoint offset ranges
        #[test]
        fn prop_disjoint_offset_ranges(
            config in arb_config(),
            physical in 0i32..10
        ) {
            let r = PartitionRemapper::new(&config);
            let phys = physical % config.physical_partitions as i32;

            let virtuals = r.virtual_partitions_for_physical(phys);

            // Check all pairs have disjoint ranges
            for i in 0..virtuals.len() {
                for j in (i + 1)..virtuals.len() {
                    let (start_i, end_i) = r.get_offset_range_for_virtual(virtuals[i]).unwrap();
                    let (start_j, end_j) = r.get_offset_range_for_virtual(virtuals[j]).unwrap();

                    // Ranges should not overlap
                    prop_assert!(end_i <= start_j || end_j <= start_i,
                        "Ranges overlap: [{}, {}) and [{}, {})",
                        start_i, end_i, start_j, end_j);
                }
            }
        }

        /// Compression ratio property: each physical partition hosts exactly compression_ratio virtual partitions
        #[test]
        fn prop_compression_ratio_correct(config in arb_config()) {
            let r = PartitionRemapper::new(&config);
            let expected_count = config.virtual_partitions / config.physical_partitions;

            for p in 0..config.physical_partitions as i32 {
                let virtuals = r.virtual_partitions_for_physical(p);
                prop_assert_eq!(virtuals.len() as u32, expected_count);
            }
        }
    }
}
