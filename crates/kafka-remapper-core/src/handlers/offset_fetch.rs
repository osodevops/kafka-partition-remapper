//! OffsetFetch request handler.
//!
//! # DEPRECATED - DO NOT USE
//!
//! **WARNING:** This handler is BROKEN and should NOT be wired into the connection handler.
//!
//! ## The Problem
//!
//! This handler translates virtual partition numbers to physical partition numbers in requests,
//! and attempts to translate back in responses. However, this approach is fundamentally broken
//! because multiple virtual partitions map to the same physical partition.
//!
//! When we request offsets for a physical partition, the broker returns the offset stored
//! at that key - but we cannot know WHICH virtual partition that offset belongs to. The
//! saved mapping only tells us which virtual partition we REQUESTED, not which one the
//! stored offset actually came from.
//!
//! ## Example
//!
//! ```text
//! With 100 virtual → 10 physical:
//!
//! 1. Consumer commits virtual partition 10, offset 200
//!    → With translation: stored at (group, topic, partition=0)
//!
//! 2. Consumer fetches offset for virtual partition 0
//!    → Translated to physical partition 0
//!    → Broker returns offset 200 (from virtual partition 10!)
//!    → Handler restores "virtual partition 0" from saved mapping
//!    → Result: virtual partition 0 has offset 200 - WRONG!
//! ```
//!
//! ## Correct Approach
//!
//! Use **passthrough** (in `connection.rs`). Store and retrieve offsets using virtual
//! partition numbers directly. The broker accepts any partition number in `__consumer_offsets`.
//!
//! See `docs/implementation-plan.md` Appendix A for detailed analysis.
//!
//! ---
//!
//! Original description (for historical reference):
//! Translates virtual partition indices to physical in the request,
//! and physical offsets back to virtual in the response.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::BytesMut;
use kafka_protocol::messages::{OffsetFetchRequest, OffsetFetchResponse};
use kafka_protocol::protocol::{Decodable, Encodable};
use tracing::debug;

use crate::broker::BrokerPool;
use crate::error::{ProxyError, Result};
use crate::network::codec::KafkaFrame;
use crate::remapper::TopicRemapperRegistry;

use super::ProtocolHandler;

/// Stores the original virtual partition info for translation back.
#[derive(Debug, Clone)]
struct PartitionMapping {
    virtual_partition: i32,
    physical_partition: i32,
}

/// Handler for OffsetFetch requests.
///
/// # Deprecated
///
/// **DO NOT USE** - This handler returns incorrect offsets due to partition key collisions.
/// Use passthrough instead. See module-level documentation for details.
#[deprecated(
    since = "0.6.0",
    note = "This handler is broken - cannot determine which virtual partition a stored offset belongs to when multiple map to the same physical partition. Use passthrough instead."
)]
pub struct OffsetFetchHandler {
    registry: Arc<TopicRemapperRegistry>,
    broker_pool: Arc<BrokerPool>,
}

#[allow(deprecated)]
impl OffsetFetchHandler {
    /// Create a new OffsetFetch handler.
    #[must_use]
    pub fn new(registry: Arc<TopicRemapperRegistry>, broker_pool: Arc<BrokerPool>) -> Self {
        Self {
            registry,
            broker_pool,
        }
    }

    /// Translate virtual partitions to physical in the request.
    /// Returns a mapping of virtual -> physical for response translation.
    fn translate_request(
        &self,
        request: &mut OffsetFetchRequest,
    ) -> Result<Vec<(String, Vec<PartitionMapping>)>> {
        let mut mappings = Vec::new();

        if let Some(topics) = &mut request.topics {
            for topic in topics {
                let topic_name = topic.name.to_string();
                let remapper = self.registry.get_remapper(&topic_name);
                let mut topic_mappings = Vec::new();

                for partition in &mut topic.partition_indexes {
                    let virtual_partition = *partition;

                    // Translate to physical
                    let physical = remapper
                        .virtual_to_physical(virtual_partition)
                        .map_err(ProxyError::Remap)?;

                    debug!(
                        virtual_partition,
                        physical_partition = physical.physical_partition,
                        "translating offset fetch partition"
                    );

                    topic_mappings.push(PartitionMapping {
                        virtual_partition,
                        physical_partition: physical.physical_partition,
                    });

                    *partition = physical.physical_partition;
                }

                mappings.push((topic_name, topic_mappings));
            }
        }

        Ok(mappings)
    }

    /// Translate physical offsets back to virtual in the response.
    fn translate_response(
        &self,
        response: &mut OffsetFetchResponse,
        mappings: &[(String, Vec<PartitionMapping>)],
    ) -> Result<()> {
        for topic in &mut response.topics {
            let topic_name = topic.name.to_string();
            let remapper = self.registry.get_remapper(&topic_name);

            // Find the mappings for this topic
            if let Some((_, topic_mappings)) = mappings.iter().find(|(name, _)| name == &topic_name)
            {
                for partition in &mut topic.partitions {
                    let physical_partition = partition.partition_index;
                    let physical_offset = partition.committed_offset;

                    // Find the virtual partition that maps to this physical
                    if let Some(mapping) = topic_mappings
                        .iter()
                        .find(|m| m.physical_partition == physical_partition)
                    {
                        // Translate the offset back to virtual
                        if physical_offset >= 0 {
                            let virtual_mapping = remapper
                                .physical_to_virtual(physical_partition, physical_offset)
                                .map_err(ProxyError::Remap)?;

                            debug!(
                                physical_partition,
                                physical_offset,
                                virtual_partition = virtual_mapping.virtual_partition,
                                virtual_offset = virtual_mapping.virtual_offset,
                                "translating offset fetch response"
                            );

                            partition.partition_index = mapping.virtual_partition;
                            partition.committed_offset = virtual_mapping.virtual_offset;
                        } else {
                            // -1 means no offset committed yet, just restore virtual partition
                            partition.partition_index = mapping.virtual_partition;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
#[allow(deprecated)]
impl ProtocolHandler for OffsetFetchHandler {
    async fn handle(&self, frame: &KafkaFrame) -> Result<BytesMut> {
        debug!(
            correlation_id = frame.correlation_id,
            api_version = frame.api_version,
            "handling OffsetFetch"
        );

        // Decode the request
        let mut request_bytes = bytes::Bytes::copy_from_slice(&frame.bytes[8..]);
        let mut request = OffsetFetchRequest::decode(&mut request_bytes, frame.api_version)
            .map_err(|e| ProxyError::ProtocolDecode {
                message: format!("failed to decode OffsetFetchRequest: {}", e),
            })?;

        // Translate virtual -> physical and save mappings
        let mappings = self.translate_request(&mut request)?;

        // Re-encode the request
        let mut encoded_request = BytesMut::new();
        encoded_request.extend_from_slice(&frame.bytes[..8]);
        request
            .encode(&mut encoded_request, frame.api_version)
            .map_err(|e| ProxyError::ProtocolEncode {
                message: format!("failed to encode OffsetFetchRequest: {}", e),
            })?;

        // Send to broker
        let response_body = self.broker_pool.send_request(&encoded_request).await?;

        // Decode response (skip correlation ID)
        let mut response_bytes = response_body.slice(4..);
        let mut response = OffsetFetchResponse::decode(&mut response_bytes, frame.api_version)
            .map_err(|e| ProxyError::ProtocolDecode {
                message: format!("failed to decode OffsetFetchResponse: {}", e),
            })?;

        // Translate physical offsets back to virtual
        self.translate_response(&mut response, &mappings)?;

        // Re-encode the response
        let mut response_buf = BytesMut::new();
        response
            .encode(&mut response_buf, frame.api_version)
            .map_err(|e| ProxyError::ProtocolEncode {
                message: format!("failed to encode OffsetFetchResponse: {}", e),
            })?;

        Ok(response_buf)
    }
}

#[cfg(test)]
#[allow(deprecated)] // Tests kept for historical reference - handler is deprecated
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::config::{KafkaConfig, MappingConfig, SecurityProtocol};
    use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
    use kafka_protocol::messages::offset_fetch_response::{
        OffsetFetchResponsePartition, OffsetFetchResponseTopic,
    };
    use kafka_protocol::protocol::StrBytes;

    fn test_registry() -> Arc<TopicRemapperRegistry> {
        Arc::new(TopicRemapperRegistry::new(&MappingConfig {
            virtual_partitions: 100,
            physical_partitions: 10,
            offset_range: 1 << 40,
            topics: HashMap::new(),
        }))
    }

    fn test_pool() -> Arc<BrokerPool> {
        Arc::new(BrokerPool::new(KafkaConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            connection_timeout_ms: 100,
            request_timeout_ms: 1000,
            metadata_refresh_interval_secs: 0,
            security_protocol: SecurityProtocol::Plaintext,
            tls: None,
            sasl: None,
        }))
    }

    #[test]
    fn test_translate_request() {
        let handler = OffsetFetchHandler::new(test_registry(), test_pool());

        let mut request = OffsetFetchRequest::default();
        request.group_id =
            kafka_protocol::messages::GroupId::from(StrBytes::from_static_str("test-group"));

        let mut topic = OffsetFetchRequestTopic::default();
        topic.name =
            kafka_protocol::messages::TopicName::from(StrBytes::from_static_str("test-topic"));
        topic.partition_indexes = vec![27, 50]; // Virtual partitions

        request.topics = Some(vec![topic]);

        let mappings = handler.translate_request(&mut request).unwrap();

        // Check request was modified
        assert_eq!(request.topics.as_ref().unwrap()[0].partition_indexes[0], 7); // 27 % 10
        assert_eq!(request.topics.as_ref().unwrap()[0].partition_indexes[1], 0); // 50 % 10

        // Check mappings were saved
        assert_eq!(mappings.len(), 1);
        assert_eq!(mappings[0].1.len(), 2);
        assert_eq!(mappings[0].1[0].virtual_partition, 27);
        assert_eq!(mappings[0].1[0].physical_partition, 7);
    }

    #[test]
    fn test_translate_response() {
        let handler = OffsetFetchHandler::new(test_registry(), test_pool());

        // Simulate mappings from a previous request
        let mappings = vec![(
            "test-topic".to_string(),
            vec![PartitionMapping {
                virtual_partition: 27,
                physical_partition: 7,
            }],
        )];

        let mut response = OffsetFetchResponse::default();
        let mut topic = OffsetFetchResponseTopic::default();
        topic.name =
            kafka_protocol::messages::TopicName::from(StrBytes::from_static_str("test-topic"));

        // Physical offset for virtual partition 27
        // Virtual 27 is in group 2, so physical offset = 2 * offset_range + virtual_offset
        let virtual_offset = 5000i64;
        let physical_offset = 2i64 * (1i64 << 40) + virtual_offset;

        let mut partition = OffsetFetchResponsePartition::default();
        partition.partition_index = 7; // Physical partition
        partition.committed_offset = physical_offset;
        topic.partitions.push(partition);

        response.topics.push(topic);

        // Translate response
        handler
            .translate_response(&mut response, &mappings)
            .unwrap();

        // Partition index should be back to virtual
        assert_eq!(response.topics[0].partitions[0].partition_index, 27);

        // Offset should be virtual
        assert_eq!(
            response.topics[0].partitions[0].committed_offset,
            virtual_offset
        );
    }

    #[test]
    fn test_translate_response_no_offset() {
        let handler = OffsetFetchHandler::new(test_registry(), test_pool());

        let mappings = vec![(
            "test-topic".to_string(),
            vec![PartitionMapping {
                virtual_partition: 42,
                physical_partition: 2,
            }],
        )];

        let mut response = OffsetFetchResponse::default();
        let mut topic = OffsetFetchResponseTopic::default();
        topic.name =
            kafka_protocol::messages::TopicName::from(StrBytes::from_static_str("test-topic"));

        let mut partition = OffsetFetchResponsePartition::default();
        partition.partition_index = 2;
        partition.committed_offset = -1; // No offset committed
        topic.partitions.push(partition);

        response.topics.push(topic);

        handler
            .translate_response(&mut response, &mappings)
            .unwrap();

        // Partition index should be back to virtual
        assert_eq!(response.topics[0].partitions[0].partition_index, 42);

        // Offset should still be -1
        assert_eq!(response.topics[0].partitions[0].committed_offset, -1);
    }
}
