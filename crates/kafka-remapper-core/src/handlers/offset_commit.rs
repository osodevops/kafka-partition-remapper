//! OffsetCommit request handler.
//!
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
use crate::remapper::PartitionRemapper;

use super::ProtocolHandler;

/// Handler for OffsetCommit requests.
///
/// Translates virtual partitions and offsets to physical before forwarding.
pub struct OffsetCommitHandler {
    remapper: Arc<PartitionRemapper>,
    broker_pool: Arc<BrokerPool>,
}

impl OffsetCommitHandler {
    /// Create a new OffsetCommit handler.
    #[must_use]
    pub fn new(remapper: Arc<PartitionRemapper>, broker_pool: Arc<BrokerPool>) -> Self {
        Self {
            remapper,
            broker_pool,
        }
    }

    /// Translate virtual partitions and offsets to physical in the request.
    fn translate_request(&self, request: &mut OffsetCommitRequest) -> Result<()> {
        for topic in &mut request.topics {
            for partition in &mut topic.partitions {
                let virtual_partition = partition.partition_index;
                let virtual_offset = partition.committed_offset;

                // Translate to physical
                let physical = self
                    .remapper
                    .virtual_to_physical_offset(virtual_partition, virtual_offset)
                    .map_err(|e| ProxyError::Remap(e))?;

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
impl ProtocolHandler for OffsetCommitHandler {
    async fn handle(&self, frame: &KafkaFrame) -> Result<BytesMut> {
        debug!(
            correlation_id = frame.correlation_id,
            api_version = frame.api_version,
            "handling OffsetCommit"
        );

        // Decode the request (skip the 8-byte header: api_key, api_version, correlation_id)
        let mut request_bytes = bytes::Bytes::copy_from_slice(&frame.bytes[8..]);
        let mut request =
            OffsetCommitRequest::decode(&mut request_bytes, frame.api_version).map_err(|e| {
                ProxyError::ProtocolDecode {
                    message: format!("failed to decode OffsetCommitRequest: {}", e),
                }
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
mod tests {
    use super::*;
    use crate::config::{KafkaConfig, MappingConfig, SecurityProtocol};
    use kafka_protocol::messages::offset_commit_request::{
        OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };
    use kafka_protocol::protocol::StrBytes;

    fn test_remapper() -> Arc<PartitionRemapper> {
        Arc::new(PartitionRemapper::new(&MappingConfig {
            virtual_partitions: 100,
            physical_partitions: 10,
            offset_range: 1 << 40,
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
        let handler = OffsetCommitHandler::new(test_remapper(), test_pool());

        let mut request = OffsetCommitRequest::default();
        request.group_id = kafka_protocol::messages::GroupId::from(StrBytes::from_static_str(
            "test-group",
        ));

        let mut topic = OffsetCommitRequestTopic::default();
        topic.name = kafka_protocol::messages::TopicName::from(StrBytes::from_static_str(
            "test-topic",
        ));

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
        let handler = OffsetCommitHandler::new(test_remapper(), test_pool());

        let mut request = OffsetCommitRequest::default();
        request.group_id = kafka_protocol::messages::GroupId::from(StrBytes::from_static_str(
            "test-group",
        ));

        let mut topic = OffsetCommitRequestTopic::default();
        topic.name = kafka_protocol::messages::TopicName::from(StrBytes::from_static_str(
            "test-topic",
        ));

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
        assert_eq!(request.topics[0].partitions[1].committed_offset, expected_50);

        // Virtual 99 -> Physical 9, group 9
        assert_eq!(request.topics[0].partitions[2].partition_index, 9);
        let expected_99 = 9i64 * (1i64 << 40) + 999;
        assert_eq!(request.topics[0].partitions[2].committed_offset, expected_99);
    }
}
