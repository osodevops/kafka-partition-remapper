//! Topic-aware partition remapper registry.
//!
//! Provides per-topic `PartitionRemapper` instances based on configuration.
//! Topics can have custom partition mapping configurations that override the
//! global defaults, following Kafka's `TopicConfig` pattern.
//!
//! # Usage
//!
//! ```ignore
//! let registry = TopicRemapperRegistry::new(&mapping_config);
//!
//! // Get remapper for a topic (creates and caches if needed)
//! let remapper = registry.get_remapper("my-topic");
//!
//! // Get virtual partition count for metadata response
//! let virtual_count = registry.get_virtual_partitions("my-topic");
//! ```

use std::sync::Arc;

use dashmap::DashMap;
use regex::Regex;

use crate::config::MappingConfig;

use super::PartitionRemapper;

/// Registry that provides topic-specific `PartitionRemapper` instances.
///
/// Implements lazy caching of remappers - creates them on first access
/// and reuses for subsequent requests.
#[derive(Debug)]
pub struct TopicRemapperRegistry {
    /// Default remapper for topics without specific config.
    default_remapper: Arc<PartitionRemapper>,

    /// Cached remappers for topics with overrides.
    /// Key is the topic name, value is the remapper.
    topic_remappers: DashMap<String, Arc<PartitionRemapper>>,

    /// The full mapping configuration (for accessing topic overrides).
    config: MappingConfig,

    /// Pre-compiled regex patterns for pattern matching.
    /// Tuple of (pattern string, compiled regex, topic config).
    compiled_patterns: Vec<(String, Regex)>,
}

impl TopicRemapperRegistry {
    /// Create a new topic remapper registry from configuration.
    #[must_use]
    pub fn new(config: &MappingConfig) -> Self {
        let default_remapper = Arc::new(PartitionRemapper::new(config));

        // Pre-compile regex patterns
        let compiled_patterns: Vec<(String, Regex)> = config
            .topics
            .keys()
            .filter(|pattern| Self::is_regex_pattern(pattern))
            .filter_map(|pattern| {
                // Convert glob-like patterns to regex
                let regex_pattern = format!("^{}$", pattern);
                Regex::new(&regex_pattern)
                    .ok()
                    .map(|re| (pattern.clone(), re))
            })
            .collect();

        Self {
            default_remapper,
            topic_remappers: DashMap::new(),
            config: config.clone(),
            compiled_patterns,
        }
    }

    /// Check if a pattern string contains regex-like characters.
    fn is_regex_pattern(pattern: &str) -> bool {
        pattern.contains('*')
            || pattern.contains('?')
            || pattern.contains('[')
            || pattern.contains('.')
            || pattern.contains('+')
            || pattern.contains('|')
    }

    /// Get the remapper for a specific topic.
    ///
    /// Returns the cached remapper if available, otherwise creates a new one
    /// based on the topic's configuration (or global defaults if no override).
    pub fn get_remapper(&self, topic: &str) -> Arc<PartitionRemapper> {
        // Fast path: check cache first
        if let Some(remapper) = self.topic_remappers.get(topic) {
            return Arc::clone(remapper.value());
        }

        // Get the effective config for this topic
        let topic_config = self.get_effective_config(topic);

        // Check if it matches the global config (use default remapper)
        if self.is_default_config(&topic_config) {
            return Arc::clone(&self.default_remapper);
        }

        // Create and cache a new remapper for this topic
        let remapper = Arc::new(PartitionRemapper::new(&topic_config));
        self.topic_remappers
            .insert(topic.to_string(), Arc::clone(&remapper));
        remapper
    }

    /// Get the number of virtual partitions for a topic.
    ///
    /// Used for generating metadata responses.
    #[must_use]
    pub fn get_virtual_partitions(&self, topic: &str) -> u32 {
        self.get_effective_config(topic).virtual_partitions
    }

    /// Get the number of physical partitions for a topic.
    #[must_use]
    pub fn get_physical_partitions(&self, topic: &str) -> u32 {
        self.get_effective_config(topic).physical_partitions
    }

    /// Get the offset range for a topic.
    #[must_use]
    pub fn get_offset_range(&self, topic: &str) -> u64 {
        self.get_effective_config(topic).offset_range
    }

    /// Get the default remapper (for topics without overrides).
    #[must_use]
    pub fn default_remapper(&self) -> &Arc<PartitionRemapper> {
        &self.default_remapper
    }

    /// Get the effective configuration for a topic.
    ///
    /// Priority:
    /// 1. Exact topic name match
    /// 2. First matching regex pattern
    /// 3. Global defaults
    fn get_effective_config(&self, topic: &str) -> MappingConfig {
        // Check for exact match first
        if let Some(topic_config) = self.config.topics.get(topic) {
            return topic_config.merge_with_defaults(&self.config);
        }

        // Check regex patterns
        for (pattern, regex) in &self.compiled_patterns {
            if regex.is_match(topic) {
                if let Some(topic_config) = self.config.topics.get(pattern) {
                    return topic_config.merge_with_defaults(&self.config);
                }
            }
        }

        // Return global defaults
        self.config.get_topic_config(topic)
    }

    /// Check if a config matches the global defaults.
    fn is_default_config(&self, config: &MappingConfig) -> bool {
        config.virtual_partitions == self.config.virtual_partitions
            && config.physical_partitions == self.config.physical_partitions
            && config.offset_range == self.config.offset_range
    }

    /// Check if a topic has custom configuration.
    #[must_use]
    pub fn has_topic_override(&self, topic: &str) -> bool {
        // Check exact match
        if self.config.topics.contains_key(topic) {
            return true;
        }

        // Check regex patterns
        for (_, regex) in &self.compiled_patterns {
            if regex.is_match(topic) {
                return true;
            }
        }

        false
    }

    /// Get the number of configured topic overrides.
    #[must_use]
    pub fn topic_override_count(&self) -> usize {
        self.config.topics.len()
    }

    /// Get the global/default configuration.
    #[must_use]
    pub fn global_config(&self) -> &MappingConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::config::TopicMappingConfig;

    fn default_config() -> MappingConfig {
        MappingConfig {
            virtual_partitions: 100,
            physical_partitions: 10,
            offset_range: 1 << 40,
            topics: HashMap::new(),
        }
    }

    fn config_with_overrides() -> MappingConfig {
        let mut topics = HashMap::new();
        topics.insert(
            "high-throughput".to_string(),
            TopicMappingConfig {
                virtual_partitions: Some(200),
                physical_partitions: Some(20),
                offset_range: None,
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
        topics.insert(
            "events.*".to_string(),
            TopicMappingConfig {
                virtual_partitions: Some(50),
                physical_partitions: None,
                offset_range: None,
            },
        );

        MappingConfig {
            virtual_partitions: 100,
            physical_partitions: 10,
            offset_range: 1 << 40,
            topics,
        }
    }

    #[test]
    fn test_registry_new() {
        let config = default_config();
        let registry = TopicRemapperRegistry::new(&config);

        assert_eq!(registry.topic_override_count(), 0);
        assert_eq!(
            registry.get_virtual_partitions("any-topic"),
            config.virtual_partitions
        );
    }

    #[test]
    fn test_default_remapper_for_unknown_topic() {
        let config = config_with_overrides();
        let registry = TopicRemapperRegistry::new(&config);

        // Unknown topic should use default remapper
        let remapper = registry.get_remapper("unknown-topic");
        assert_eq!(remapper.virtual_partitions(), 100);
        assert_eq!(remapper.physical_partitions(), 10);
    }

    #[test]
    fn test_exact_topic_match() {
        let config = config_with_overrides();
        let registry = TopicRemapperRegistry::new(&config);

        // Exact match should use topic-specific config
        assert_eq!(registry.get_virtual_partitions("high-throughput"), 200);
        assert_eq!(registry.get_physical_partitions("high-throughput"), 20);

        let remapper = registry.get_remapper("high-throughput");
        assert_eq!(remapper.virtual_partitions(), 200);
        assert_eq!(remapper.physical_partitions(), 20);
    }

    #[test]
    fn test_topic_with_partial_override() {
        let config = config_with_overrides();
        let registry = TopicRemapperRegistry::new(&config);

        // low-volume has custom virtual_partitions, physical_partitions, and offset_range
        assert_eq!(registry.get_virtual_partitions("low-volume"), 20);
        assert_eq!(registry.get_physical_partitions("low-volume"), 4);
        assert_eq!(registry.get_offset_range("low-volume"), 1 << 30);
    }

    #[test]
    fn test_regex_pattern_matching() {
        let config = config_with_overrides();
        let registry = TopicRemapperRegistry::new(&config);

        // events.* pattern should match events.clicks, events.orders, etc.
        assert_eq!(registry.get_virtual_partitions("events.clicks"), 50);
        assert_eq!(registry.get_virtual_partitions("events.orders"), 50);
        // Physical partitions should inherit from global (not specified in override)
        assert_eq!(registry.get_physical_partitions("events.clicks"), 10);
    }

    #[test]
    fn test_remapper_caching() {
        let config = config_with_overrides();
        let registry = TopicRemapperRegistry::new(&config);

        // First call creates and caches
        let remapper1 = registry.get_remapper("high-throughput");
        // Second call should return cached instance
        let remapper2 = registry.get_remapper("high-throughput");

        // Should be the same Arc (pointer equality)
        assert!(Arc::ptr_eq(&remapper1, &remapper2));
    }

    #[test]
    fn test_default_remapper_not_cached() {
        let config = default_config();
        let registry = TopicRemapperRegistry::new(&config);

        // Topics using default config should return the default remapper
        let remapper1 = registry.get_remapper("topic1");
        let remapper2 = registry.get_remapper("topic2");

        // Both should point to the same default remapper
        assert!(Arc::ptr_eq(&remapper1, &remapper2));
        assert!(Arc::ptr_eq(&remapper1, registry.default_remapper()));
    }

    #[test]
    fn test_has_topic_override() {
        let config = config_with_overrides();
        let registry = TopicRemapperRegistry::new(&config);

        assert!(registry.has_topic_override("high-throughput"));
        assert!(registry.has_topic_override("low-volume"));
        assert!(registry.has_topic_override("events.anything"));
        assert!(!registry.has_topic_override("unknown"));
    }

    #[test]
    fn test_topic_override_count() {
        let config = config_with_overrides();
        let registry = TopicRemapperRegistry::new(&config);

        assert_eq!(registry.topic_override_count(), 3);
    }

    #[test]
    fn test_global_config() {
        let config = config_with_overrides();
        let registry = TopicRemapperRegistry::new(&config);

        let global = registry.global_config();
        assert_eq!(global.virtual_partitions, 100);
        assert_eq!(global.physical_partitions, 10);
    }
}
