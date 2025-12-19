//! Integration tests for per-topic partition mapping.
//!
//! Tests that topics can have different partition configurations that
//! override the global defaults.

use std::collections::HashMap;
use std::sync::Arc;

use kafka_remapper_core::config::{MappingConfig, TopicMappingConfig};
use kafka_remapper_core::remapper::TopicRemapperRegistry;

/// Test that unknown topics use the default remapper.
#[test]
fn test_unknown_topic_uses_default() {
    let config = MappingConfig {
        virtual_partitions: 100,
        physical_partitions: 10,
        offset_range: 1 << 40,
        topics: HashMap::new(),
    };

    let registry = TopicRemapperRegistry::new(&config);

    // Unknown topic should use global defaults
    let remapper = registry.get_remapper("unknown-topic");
    assert_eq!(remapper.virtual_partitions(), 100);
    assert_eq!(remapper.physical_partitions(), 10);

    // Verify this is the same instance as the default remapper
    assert!(Arc::ptr_eq(&remapper, registry.default_remapper()));
}

/// Test that topics with exact name matches get custom configs.
#[test]
fn test_exact_topic_match() {
    let mut topics = HashMap::new();
    topics.insert(
        "high-throughput".to_string(),
        TopicMappingConfig {
            virtual_partitions: Some(200),
            physical_partitions: Some(20),
            offset_range: None, // Inherit from global
        },
    );
    topics.insert(
        "low-volume".to_string(),
        TopicMappingConfig {
            virtual_partitions: Some(20),
            physical_partitions: Some(4),
            offset_range: Some(1 << 30),
        },
    );

    let config = MappingConfig {
        virtual_partitions: 100,
        physical_partitions: 10,
        offset_range: 1 << 40,
        topics,
    };

    let registry = TopicRemapperRegistry::new(&config);

    // high-throughput topic should have custom config
    let ht_remapper = registry.get_remapper("high-throughput");
    assert_eq!(ht_remapper.virtual_partitions(), 200);
    assert_eq!(ht_remapper.physical_partitions(), 20);
    assert_eq!(ht_remapper.offset_range(), 1 << 40); // Inherited from global

    // low-volume topic should have custom config
    let lv_remapper = registry.get_remapper("low-volume");
    assert_eq!(lv_remapper.virtual_partitions(), 20);
    assert_eq!(lv_remapper.physical_partitions(), 4);
    assert_eq!(lv_remapper.offset_range(), 1 << 30);

    // Unknown topic should still use global defaults
    let unknown_remapper = registry.get_remapper("unknown");
    assert_eq!(unknown_remapper.virtual_partitions(), 100);
    assert_eq!(unknown_remapper.physical_partitions(), 10);
}

/// Test that regex patterns match multiple topics.
#[test]
fn test_regex_pattern_matching() {
    let mut topics = HashMap::new();
    // Use a more specific pattern that requires at least a dot
    topics.insert(
        "events\\..*".to_string(),
        TopicMappingConfig {
            virtual_partitions: Some(50),
            physical_partitions: None, // Inherit from global
            offset_range: None,
        },
    );

    let config = MappingConfig {
        virtual_partitions: 100,
        physical_partitions: 10,
        offset_range: 1 << 40,
        topics,
    };

    let registry = TopicRemapperRegistry::new(&config);

    // events\..* pattern should match events.clicks, events.orders, etc.
    let clicks_remapper = registry.get_remapper("events.clicks");
    assert_eq!(clicks_remapper.virtual_partitions(), 50);
    assert_eq!(clicks_remapper.physical_partitions(), 10); // Inherited

    let orders_remapper = registry.get_remapper("events.orders");
    assert_eq!(orders_remapper.virtual_partitions(), 50);
    assert_eq!(orders_remapper.physical_partitions(), 10);

    // "events" without the dot should NOT match the pattern
    let events_remapper = registry.get_remapper("events");
    assert_eq!(events_remapper.virtual_partitions(), 100); // Uses global
}

/// Test that topic remappers are cached correctly.
#[test]
fn test_remapper_caching() {
    let mut topics = HashMap::new();
    topics.insert(
        "cached-topic".to_string(),
        TopicMappingConfig {
            virtual_partitions: Some(50),
            physical_partitions: Some(5),
            offset_range: None,
        },
    );

    let config = MappingConfig {
        virtual_partitions: 100,
        physical_partitions: 10,
        offset_range: 1 << 40,
        topics,
    };

    let registry = TopicRemapperRegistry::new(&config);

    // First call creates and caches
    let remapper1 = registry.get_remapper("cached-topic");

    // Second call should return cached instance
    let remapper2 = registry.get_remapper("cached-topic");

    // Should be the same Arc (pointer equality)
    assert!(Arc::ptr_eq(&remapper1, &remapper2));
}

/// Test that partition remapping works correctly with per-topic config.
#[test]
fn test_partition_remapping_per_topic() {
    let mut topics = HashMap::new();
    topics.insert(
        "small-topic".to_string(),
        TopicMappingConfig {
            virtual_partitions: Some(20),
            physical_partitions: Some(5),
            offset_range: None,
        },
    );

    let config = MappingConfig {
        virtual_partitions: 100,
        physical_partitions: 10,
        offset_range: 1 << 40,
        topics,
    };

    let registry = TopicRemapperRegistry::new(&config);

    // small-topic: 20 virtual on 5 physical = 4:1 compression
    let small_remapper = registry.get_remapper("small-topic");
    assert_eq!(small_remapper.compression_ratio(), 4);

    // Verify partition mapping
    let mapping = small_remapper.virtual_to_physical(15).unwrap();
    // Virtual 15 maps to physical 15 % 5 = 0, group 15 / 5 = 3
    assert_eq!(mapping.physical_partition, 0);

    // default topic: 100 virtual on 10 physical = 10:1 compression
    let default_remapper = registry.get_remapper("default-topic");
    assert_eq!(default_remapper.compression_ratio(), 10);

    // Virtual 15 maps to physical 15 % 10 = 5, group 15 / 10 = 1
    let default_mapping = default_remapper.virtual_to_physical(15).unwrap();
    assert_eq!(default_mapping.physical_partition, 5);
}

/// Test offset translation with per-topic config.
#[test]
fn test_offset_translation_per_topic() {
    let mut topics = HashMap::new();
    topics.insert(
        "custom-offset".to_string(),
        TopicMappingConfig {
            virtual_partitions: Some(10),
            physical_partitions: Some(2),
            offset_range: Some(1_000_000), // 1M offsets per virtual partition
        },
    );

    let config = MappingConfig {
        virtual_partitions: 100,
        physical_partitions: 10,
        offset_range: 1 << 40,
        topics,
    };

    let registry = TopicRemapperRegistry::new(&config);
    let remapper = registry.get_remapper("custom-offset");

    // 10 virtual on 2 physical = 5:1 compression
    assert_eq!(remapper.compression_ratio(), 5);

    // Virtual partition 5 with offset 500
    // Group = 5 / 2 = 2, physical partition = 5 % 2 = 1
    // Physical offset = 2 * 1_000_000 + 500 = 2_000_500
    let mapping = remapper.virtual_to_physical_offset(5, 500).unwrap();
    assert_eq!(mapping.physical_partition, 1);
    assert_eq!(mapping.physical_offset, 2_000_500);

    // Reverse translation
    let reverse = remapper.physical_to_virtual(1, 2_000_500).unwrap();
    assert_eq!(reverse.virtual_partition, 5);
    assert_eq!(reverse.virtual_offset, 500);
}

/// Test that helper methods return correct topic-specific values.
#[test]
fn test_registry_helper_methods() {
    let mut topics = HashMap::new();
    topics.insert(
        "custom-topic".to_string(),
        TopicMappingConfig {
            virtual_partitions: Some(50),
            physical_partitions: Some(5),
            offset_range: Some(1 << 30),
        },
    );

    let config = MappingConfig {
        virtual_partitions: 100,
        physical_partitions: 10,
        offset_range: 1 << 40,
        topics,
    };

    let registry = TopicRemapperRegistry::new(&config);

    // Custom topic
    assert_eq!(registry.get_virtual_partitions("custom-topic"), 50);
    assert_eq!(registry.get_physical_partitions("custom-topic"), 5);
    assert_eq!(registry.get_offset_range("custom-topic"), 1 << 30);

    // Default (unknown) topic
    assert_eq!(registry.get_virtual_partitions("unknown"), 100);
    assert_eq!(registry.get_physical_partitions("unknown"), 10);
    assert_eq!(registry.get_offset_range("unknown"), 1 << 40);
}

/// Test has_topic_override method.
#[test]
fn test_has_topic_override() {
    let mut topics = HashMap::new();
    topics.insert(
        "overridden".to_string(),
        TopicMappingConfig {
            virtual_partitions: Some(50),
            physical_partitions: None,
            offset_range: None,
        },
    );
    // Use a pattern that requires at least a dot after "events"
    topics.insert(
        "events\\..*".to_string(),
        TopicMappingConfig {
            virtual_partitions: Some(25),
            physical_partitions: None,
            offset_range: None,
        },
    );

    let config = MappingConfig {
        virtual_partitions: 100,
        physical_partitions: 10,
        offset_range: 1 << 40,
        topics,
    };

    let registry = TopicRemapperRegistry::new(&config);

    assert!(registry.has_topic_override("overridden"));
    assert!(registry.has_topic_override("events.clicks")); // Matches pattern
    assert!(!registry.has_topic_override("unknown"));
    assert!(!registry.has_topic_override("events")); // Doesn't match pattern (needs .something)
}

/// Test that partial overrides inherit from global config.
#[test]
fn test_partial_override_inheritance() {
    let mut topics = HashMap::new();
    // Only override virtual_partitions
    topics.insert(
        "partial-override".to_string(),
        TopicMappingConfig {
            virtual_partitions: Some(200),
            physical_partitions: None, // Should inherit 10
            offset_range: None,        // Should inherit 1 << 40
        },
    );

    let config = MappingConfig {
        virtual_partitions: 100,
        physical_partitions: 10,
        offset_range: 1 << 40,
        topics,
    };

    let registry = TopicRemapperRegistry::new(&config);
    let remapper = registry.get_remapper("partial-override");

    assert_eq!(remapper.virtual_partitions(), 200); // Overridden
    assert_eq!(remapper.physical_partitions(), 10); // Inherited
    assert_eq!(remapper.offset_range(), 1 << 40); // Inherited
}
