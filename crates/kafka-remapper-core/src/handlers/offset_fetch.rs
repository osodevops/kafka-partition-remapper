//! OffsetFetch request handler.
//!
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
use crate::remapper::PartitionRemapper;

use super::ProtocolHandler;

/// Stores the original virtual partition info for translation back.
#[derive(Debug, Clone)]
struct PartitionMapping {
    virtual_partition: i32,
    physical_partition: i32,
}

/// Handler for OffsetFetch requests.
///
/// Translates virtual partitions to physical in the request,
/// and translates physical offsets back to virtual in the response.
pub struct OffsetFetchHandler {
    remapper: Arc<PartitionRemapper>,
    broker_pool: Arc<BrokerPool>,
}

impl OffsetFetchHandler {
    /// Create a new OffsetFetch handler.
    #[must_use]
    pub fn new(remapper: Arc<PartitionRemapper>, broker_pool: Arc<BrokerPool>) -> Self {
        Self {
            remapper,
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
                let mut topic_mappings = Vec::new();

                for partition in &mut topic.partition_indexes {
                    let virtual_partition = *partition;

                    // Translate to physical
                    let physical = self
                        .remapper
                        .virtual_to_physical(virtual_partition)
                        .map_err(|e| ProxyError::Remap(e))?;

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
                            let virtual_mapping = self
                                .remapper
                                .physical_to_virtual(physical_partition, physical_offset)
                                .map_err(|e| ProxyError::Remap(e))?;

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
impl ProtocolHandler for OffsetFetchHandler {
    async fn handle(&self, frame: &KafkaFrame) -> Result<BytesMut> {
        debug!(
            correlation_id = frame.correlation_id,
            api_version = frame.api_version,
            "handling OffsetFetch"
        );

        // Decode the request
        let mut request_bytes = bytes::Bytes::copy_from_slice(&frame.bytes[8..]);
        let mut request =
            OffsetFetchRequest::decode(&mut request_bytes, frame.api_version).map_err(|e| {
                ProxyError::ProtocolDecode {
                    message: format!("failed to decode OffsetFetchRequest: {}", e),
                }
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
        let mut response =
            OffsetFetchResponse::decode(&mut response_bytes, frame.api_version).map_err(|e| {
                ProxyError::ProtocolDecode {
                    message: format!("failed to decode OffsetFetchResponse: {}", e),
                }
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
mod tests {
    use super::*;
    use crate::config::{KafkaConfig, MappingConfig};
    use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
    use kafka_protocol::messages::offset_fetch_response::{
        OffsetFetchResponsePartition, OffsetFetchResponseTopic,
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
        }))
    }

    #[test]
    fn test_translate_request() {
        let handler = OffsetFetchHandler::new(test_remapper(), test_pool());

        let mut request = OffsetFetchRequest::default();
        request.group_id = kafka_protocol::messages::GroupId::from(StrBytes::from_static_str(
            "test-group",
        ));

        let mut topic = OffsetFetchRequestTopic::default();
        topic.name = kafka_protocol::messages::TopicName::from(StrBytes::from_static_str(
            "test-topic",
        ));
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
        let handler = OffsetFetchHandler::new(test_remapper(), test_pool());

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
        topic.name = kafka_protocol::messages::TopicName::from(StrBytes::from_static_str(
            "test-topic",
        ));

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
        handler.translate_response(&mut response, &mappings).unwrap();

        // Partition index should be back to virtual
        assert_eq!(response.topics[0].partitions[0].partition_index, 27);

        // Offset should be virtual
        assert_eq!(response.topics[0].partitions[0].committed_offset, virtual_offset);
    }

    #[test]
    fn test_translate_response_no_offset() {
        let handler = OffsetFetchHandler::new(test_remapper(), test_pool());

        let mappings = vec![(
            "test-topic".to_string(),
            vec![PartitionMapping {
                virtual_partition: 42,
                physical_partition: 2,
            }],
        )];

        let mut response = OffsetFetchResponse::default();
        let mut topic = OffsetFetchResponseTopic::default();
        topic.name = kafka_protocol::messages::TopicName::from(StrBytes::from_static_str(
            "test-topic",
        ));

        let mut partition = OffsetFetchResponsePartition::default();
        partition.partition_index = 2;
        partition.committed_offset = -1; // No offset committed
        topic.partitions.push(partition);

        response.topics.push(topic);

        handler.translate_response(&mut response, &mappings).unwrap();

        // Partition index should be back to virtual
        assert_eq!(response.topics[0].partitions[0].partition_index, 42);

        // Offset should still be -1
        assert_eq!(response.topics[0].partitions[0].committed_offset, -1);
    }
}
