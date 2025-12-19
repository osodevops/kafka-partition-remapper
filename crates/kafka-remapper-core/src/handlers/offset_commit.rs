//! OffsetCommit request handler.
//!
//! # DEPRECATED - DO NOT USE
//!
//! **WARNING:** This handler is BROKEN and should NOT be wired into the connection handler.
//!
//! ## The Problem
//!
//! This handler translates virtual partition numbers to physical partition numbers.
//! However, multiple virtual partitions map to the same physical partition (e.g., with
//! 100 virtual → 10 physical, virtual partitions 0, 10, 20, ..., 90 all map to physical 0).
//!
//! Offset storage in `__consumer_offsets` is keyed by `(group_id, topic, partition)`.
//! If we translate partition numbers, all virtual partitions mapping to the same physical
//! partition will **overwrite each other's offsets**, causing **DATA LOSS**.
//!
//! ## Example
//!
//! ```text
//! Consumer commits virtual partition 0, offset 100
//!   → Translated to (group, topic, partition=0)
//!   → Stored at key: (group, topic, 0)
//!
//! Consumer commits virtual partition 10, offset 200
//!   → Translated to (group, topic, partition=0)  // SAME KEY!
//!   → Overwrites previous offset!
//!
//! Consumer fetches offset for virtual partition 0
//!   → Returns offset from partition 10 - WRONG!
//! ```
//!
//! ## Correct Approach
//!
//! Use **passthrough** (in `connection.rs`). The broker accepts any partition number
//! in `__consumer_offsets`, so we store offsets keyed by virtual partition number directly.
//! This avoids collisions between virtual partitions on the same physical partition.
//!
//! See `docs/implementation-plan.md` Appendix A for detailed analysis.
//!
//! ---
//!
//! Original description (for historical reference):
//! Translates virtual partition indices and offsets to physical before forwarding
//! to the broker. The broker stores the physical offset, but consumers always
//! work with virtual offsets.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::BytesMut;
use kafka_protocol::messages::OffsetCommitRequest;
use kafka_protocol::protocol::{Decodable, Encodable};
use tracing::debug;

use crate::broker::BrokerPool;
use crate::error::{ProxyError, Result};
use crate::network::codec::KafkaFrame;
use crate::remapper::TopicRemapperRegistry;

use super::ProtocolHandler;

/// Handler for OffsetCommit requests.
///
/// # Deprecated
///
/// **DO NOT USE** - This handler causes data loss due to partition key collisions.
/// Use passthrough instead. See module-level documentation for details.
#[deprecated(
    since = "0.6.0",
    note = "This handler is broken - multiple virtual partitions collide at the same physical partition key, causing offset overwrites. Use passthrough instead."
)]
pub struct OffsetCommitHandler {
    registry: Arc<TopicRemapperRegistry>,
    broker_pool: Arc<BrokerPool>,
}

#[allow(deprecated)]
impl OffsetCommitHandler {
    /// Create a new OffsetCommit handler.
    #[must_use]
    pub fn new(registry: Arc<TopicRemapperRegistry>, broker_pool: Arc<BrokerPool>) -> Self {
        Self {
            registry,
            broker_pool,
        }
    }

    /// Translate virtual partitions and offsets to physical in the request.
    fn translate_request(&self, request: &mut OffsetCommitRequest) -> Result<()> {
        for topic in &mut request.topics {
            let topic_name = topic.name.to_string();
            let remapper = self.registry.get_remapper(&topic_name);

            for partition in &mut topic.partitions {
                let virtual_partition = partition.partition_index;
                let virtual_offset = partition.committed_offset;

                // Translate to physical
                let physical = remapper
                    .virtual_to_physical_offset(virtual_partition, virtual_offset)
                    .map_err(ProxyError::Remap)?;

                debug!(
                    virtual_partition,
                    virtual_offset,
                    physical_partition = physical.physical_partition,
                    physical_offset = physical.physical_offset,
                    "translating offset commit"
                );

                // Update the request with physical values
                partition.partition_index = physical.physical_partition;
                partition.committed_offset = physical.physical_offset;
            }
        }

        Ok(())
    }
}

#[async_trait]
#[allow(deprecated)]
impl ProtocolHandler for OffsetCommitHandler {
    async fn handle(&self, frame: &KafkaFrame) -> Result<BytesMut> {
        debug!(
            correlation_id = frame.correlation_id,
            api_version = frame.api_version,
            "handling OffsetCommit"
        );

        // Decode the request (skip the 8-byte header: api_key, api_version, correlation_id)
        let mut request_bytes = bytes::Bytes::copy_from_slice(&frame.bytes[8..]);
        let mut request = OffsetCommitRequest::decode(&mut request_bytes, frame.api_version)
            .map_err(|e| ProxyError::ProtocolDecode {
                message: format!("failed to decode OffsetCommitRequest: {}", e),
            })?;

        // Translate virtual -> physical
        self.translate_request(&mut request)?;

        // Re-encode the request with physical values
        let mut encoded_request = BytesMut::new();

        // Add header: api_key (2) + api_version (2) + correlation_id (4)
        encoded_request.extend_from_slice(&frame.bytes[..8]);

        // Encode the modified request body
        request
            .encode(&mut encoded_request, frame.api_version)
            .map_err(|e| ProxyError::ProtocolEncode {
                message: format!("failed to encode OffsetCommitRequest: {}", e),
            })?;

        // Send to broker
        let response_body = self.broker_pool.send_request(&encoded_request).await?;

        // Return response body without correlation ID (first 4 bytes)
        // Note: The response partition indices should be translated back to virtual,
        // but since OffsetCommitResponse only contains error codes and the partition
        // indices mirror the request, the client already knows which virtual partitions
        // it committed. We can pass through the response as-is for now.
        Ok(BytesMut::from(&response_body[4..]))
    }
}

#[cfg(test)]
#[allow(deprecated)] // Tests kept for historical reference - handler is deprecated
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::config::{KafkaConfig, MappingConfig, SecurityProtocol};
    use kafka_protocol::messages::offset_commit_request::{
        OffsetCommitRequestPartition, OffsetCommitRequestTopic,
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
        let handler = OffsetCommitHandler::new(test_registry(), test_pool());

        let mut request = OffsetCommitRequest::default();
        request.group_id =
            kafka_protocol::messages::GroupId::from(StrBytes::from_static_str("test-group"));

        let mut topic = OffsetCommitRequestTopic::default();
        topic.name =
            kafka_protocol::messages::TopicName::from(StrBytes::from_static_str("test-topic"));

        let mut partition = OffsetCommitRequestPartition::default();
        partition.partition_index = 27; // Virtual partition
        partition.committed_offset = 5000; // Virtual offset
        topic.partitions.push(partition);

        request.topics.push(topic);

        // Translate
        handler.translate_request(&mut request).unwrap();

        // Virtual 27 -> Physical 7, group 2
        assert_eq!(request.topics[0].partitions[0].partition_index, 7);

        // Physical offset = group * offset_range + virtual_offset
        // = 2 * (1 << 40) + 5000
        let expected_offset = 2i64 * (1i64 << 40) + 5000;
        assert_eq!(
            request.topics[0].partitions[0].committed_offset,
            expected_offset
        );
    }

    #[test]
    fn test_translate_multiple_partitions() {
        let handler = OffsetCommitHandler::new(test_registry(), test_pool());

        let mut request = OffsetCommitRequest::default();
        request.group_id =
            kafka_protocol::messages::GroupId::from(StrBytes::from_static_str("test-group"));

        let mut topic = OffsetCommitRequestTopic::default();
        topic.name =
            kafka_protocol::messages::TopicName::from(StrBytes::from_static_str("test-topic"));

        // Add multiple partitions
        for (v_part, v_offset) in [(0, 100), (50, 5000), (99, 999)] {
            let mut partition = OffsetCommitRequestPartition::default();
            partition.partition_index = v_part;
            partition.committed_offset = v_offset;
            topic.partitions.push(partition);
        }

        request.topics.push(topic);

        handler.translate_request(&mut request).unwrap();

        // Verify translations
        // Virtual 0 -> Physical 0, group 0
        assert_eq!(request.topics[0].partitions[0].partition_index, 0);
        assert_eq!(request.topics[0].partitions[0].committed_offset, 100);

        // Virtual 50 -> Physical 0, group 5
        assert_eq!(request.topics[0].partitions[1].partition_index, 0);
        let expected_50 = 5i64 * (1i64 << 40) + 5000;
        assert_eq!(
            request.topics[0].partitions[1].committed_offset,
            expected_50
        );

        // Virtual 99 -> Physical 9, group 9
        assert_eq!(request.topics[0].partitions[2].partition_index, 9);
        let expected_99 = 9i64 * (1i64 << 40) + 999;
        assert_eq!(
            request.topics[0].partitions[2].committed_offset,
            expected_99
        );
    }
}
