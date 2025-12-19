//! Partition remapping logic.
//!
//! This module contains the core logic for mapping virtual partitions and offsets
//! to physical partitions and offsets, and vice versa.
//!
//! # Per-Topic Mapping
//!
//! The [`TopicRemapperRegistry`] provides topic-specific remappers based on
//! configuration. Topics can have custom partition mapping that overrides
//! global defaults, following Kafka's `TopicConfig` pattern.

mod partition_remapper;
mod topic_registry;

pub use partition_remapper::{
    PartitionRemapper, PhysicalMapping, PhysicalOffsetMapping, VirtualMapping,
};
pub use topic_registry::TopicRemapperRegistry;
