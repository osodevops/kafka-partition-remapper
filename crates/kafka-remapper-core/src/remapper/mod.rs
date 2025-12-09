//! Partition remapping logic.
//!
//! This module contains the core logic for mapping virtual partitions and offsets
//! to physical partitions and offsets, and vice versa.

mod partition_remapper;

pub use partition_remapper::{
    PartitionRemapper, PhysicalMapping, PhysicalOffsetMapping, VirtualMapping,
};
