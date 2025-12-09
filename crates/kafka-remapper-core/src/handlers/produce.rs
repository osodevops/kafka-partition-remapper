//! Produce request handler.
//!
//! Transforms Produce requests by mapping virtual partitions to physical partitions
//! and translating offsets in the response.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::BytesMut;
use kafka_protocol::messages::{ProduceRequest, ProduceResponse};
use kafka_protocol::protocol::{Decodable, Encodable};
use tracing::debug;

use crate::broker::BrokerPool;
use crate::error::{ProxyError, Result};
use crate::network::codec::KafkaFrame;
use crate::remapper::PartitionRemapper;

use super::ProtocolHandler;

/// Handler for Produce requests.
pub struct ProduceHandler {
    remapper: Arc<PartitionRemapper>,
    broker_pool: Arc<BrokerPool>,
}

impl ProduceHandler {
    /// Create a new Produce handler.
    #[must_use]
    pub fn new(remapper: Arc<PartitionRemapper>, broker_pool: Arc<BrokerPool>) -> Self {
        Self {
            remapper,
            broker_pool,
        }
    }

    /// Transform virtual partitions to physical in the request.
    ///
    /// Returns the modified request and a mapping for response translation.
    fn physicalize_request(
        &self,
        mut request: ProduceRequest,
    ) -> Result<(ProduceRequest, PartitionMapping)> {
        let mut mapping = PartitionMapping::new();

        for topic_data in &mut request.topic_data {
            let topic_name = topic_data.name.to_string();

            for partition_data in &mut topic_data.partition_data {
                let virtual_partition = partition_data.index;

                // Map virtual to physical
                let physical = self
                    .remapper
                    .virtual_to_physical(virtual_partition)
                    .map_err(|e| ProxyError::Remap(e))?;

                debug!(
                    topic = %topic_name,
                    virtual_partition,
                    physical_partition = physical.physical_partition,
                    "mapping produce partition"
                );

                // Store mapping for response translation
                mapping.add(&topic_name, virtual_partition, physical.physical_partition);

                // Update partition index to physical
                partition_data.index = physical.physical_partition;

                // Note: We're not modifying the record batch offsets here.
                // The broker assigns offsets, and we translate them in the response.
            }
        }

        Ok((request, mapping))
    }

    /// Transform physical offsets back to virtual in the response.
    fn virtualize_response(
        &self,
        mut response: ProduceResponse,
        mapping: &PartitionMapping,
    ) -> Result<ProduceResponse> {
        for topic_response in &mut response.responses {
            let topic_name = topic_response.name.to_string();
            let original_partitions = std::mem::take(&mut topic_response.partition_responses);
            let mut virtual_responses = Vec::new();

            for partition_response in original_partitions {
                let physical_partition = partition_response.index;

                // Get all virtual partitions that were sent to this physical partition
                let virtual_partitions = mapping.get_virtual(&topic_name, physical_partition);

                for virtual_partition in virtual_partitions {
                    let mut virtual_response = partition_response.clone();
                    virtual_response.index = virtual_partition;

                    // Translate base offset back to virtual
                    if virtual_response.base_offset >= 0 {
                        if let Ok(virtual_mapping) = self.remapper.physical_to_virtual(
                            physical_partition,
                            virtual_response.base_offset,
                        ) {
                            virtual_response.base_offset = virtual_mapping.virtual_offset;
                        }
                    }

                    virtual_responses.push(virtual_response);
                }
            }

            topic_response.partition_responses = virtual_responses;
        }

        Ok(response)
    }
}

#[async_trait]
impl ProtocolHandler for ProduceHandler {
    async fn handle(&self, frame: &KafkaFrame) -> Result<BytesMut> {
        debug!(
            correlation_id = frame.correlation_id,
            api_version = frame.api_version,
            "handling Produce"
        );

        // Decode the request (skip header - first 8 bytes: api_key, version, correlation_id)
        // Actually we need to properly skip the request header
        let header_size = self.calculate_header_size(frame.api_version);
        let request_body = bytes::Bytes::copy_from_slice(&frame.bytes[header_size..]);

        let request =
            ProduceRequest::decode(&mut request_body.clone(), frame.api_version).map_err(|e| {
                ProxyError::ProtocolDecode {
                    message: format!("failed to decode ProduceRequest: {e}"),
                }
            })?;

        // Transform to physical partitions
        let (physical_request, mapping) = self.physicalize_request(request)?;

        // Encode the physical request
        let mut physical_bytes = BytesMut::new();

        // Write request header
        physical_bytes.extend_from_slice(&frame.bytes[..header_size]);

        // Write request body
        physical_request
            .encode(&mut physical_bytes, frame.api_version)
            .map_err(|e| ProxyError::ProtocolEncode {
                message: e.to_string(),
            })?;

        // Forward to broker
        let response_body = self.broker_pool.send_request(&physical_bytes).await?;

        // Decode broker response (skip correlation ID - first 4 bytes)
        let mut response_bytes = response_body.slice(4..);
        let response =
            ProduceResponse::decode(&mut response_bytes, frame.api_version).map_err(|e| {
                ProxyError::ProtocolDecode {
                    message: e.to_string(),
                }
            })?;

        // Virtualize response
        let virtualized = self.virtualize_response(response, &mapping)?;

        // Encode response
        let mut buf = BytesMut::new();
        virtualized
            .encode(&mut buf, frame.api_version)
            .map_err(|e| ProxyError::ProtocolEncode {
                message: e.to_string(),
            })?;

        Ok(buf)
    }
}

impl ProduceHandler {
    /// Calculate the request header size for a given API version.
    fn calculate_header_size(&self, api_version: i16) -> usize {
        // Request header v0-v1: api_key(2) + api_version(2) + correlation_id(4) = 8
        // Request header v2+: adds client_id (compact string)
        // For simplicity, we'll use 8 as the minimum header size
        // A more robust implementation would parse the actual header
        8
    }
}

/// Tracks virtual -> physical partition mappings for request/response correlation.
struct PartitionMapping {
    /// topic -> (virtual_partition -> physical_partition)
    mappings: HashMap<String, HashMap<i32, i32>>,
}

impl PartitionMapping {
    fn new() -> Self {
        Self {
            mappings: HashMap::new(),
        }
    }

    fn add(&mut self, topic: &str, virtual_partition: i32, physical_partition: i32) {
        self.mappings
            .entry(topic.to_string())
            .or_default()
            .insert(virtual_partition, physical_partition);
    }

    fn get_virtual(&self, topic: &str, physical_partition: i32) -> Vec<i32> {
        self.mappings
            .get(topic)
            .map(|m| {
                m.iter()
                    .filter(|(_, &p)| p == physical_partition)
                    .map(|(&v, _)| v)
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{KafkaConfig, MappingConfig};

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
    fn test_partition_mapping() {
        let mut mapping = PartitionMapping::new();

        mapping.add("topic1", 0, 0);
        mapping.add("topic1", 10, 0); // Both map to physical 0
        mapping.add("topic1", 5, 5);

        let virtuals = mapping.get_virtual("topic1", 0);
        assert_eq!(virtuals.len(), 2);
        assert!(virtuals.contains(&0));
        assert!(virtuals.contains(&10));

        let virtuals = mapping.get_virtual("topic1", 5);
        assert_eq!(virtuals.len(), 1);
        assert!(virtuals.contains(&5));

        // Non-existent
        let virtuals = mapping.get_virtual("topic1", 9);
        assert!(virtuals.is_empty());
    }
}
