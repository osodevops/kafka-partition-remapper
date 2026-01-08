//! Fetch request handler.
//!
//! Transforms Fetch requests by mapping virtual partitions/offsets to physical
//! and filtering/translating responses.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::BytesMut;
use kafka_protocol::messages::{FetchRequest, FetchResponse, RequestHeader, ResponseHeader};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion};
use tracing::debug;

use crate::broker::BrokerPool;
use crate::error::{ProxyError, Result};
use crate::network::codec::KafkaFrame;
use crate::remapper::TopicRemapperRegistry;

use super::ProtocolHandler;

/// Handler for Fetch requests.
pub struct FetchHandler {
    registry: Arc<TopicRemapperRegistry>,
    broker_pool: Arc<BrokerPool>,
}

impl FetchHandler {
    /// Create a new Fetch handler.
    #[must_use]
    pub fn new(registry: Arc<TopicRemapperRegistry>, broker_pool: Arc<BrokerPool>) -> Self {
        Self {
            registry,
            broker_pool,
        }
    }

    /// Transform virtual partitions/offsets to physical in the request.
    fn physicalize_request(
        &self,
        mut request: FetchRequest,
    ) -> Result<(FetchRequest, FetchMapping)> {
        let mut mapping = FetchMapping::new();

        for topic in &mut request.topics {
            let topic_name = topic.topic.to_string();
            let remapper = self.registry.get_remapper(&topic_name);

            for partition in &mut topic.partitions {
                let virtual_partition = partition.partition;
                let virtual_offset = partition.fetch_offset;

                // Map to physical
                let physical = remapper
                    .virtual_to_physical_offset(virtual_partition, virtual_offset)
                    .map_err(ProxyError::Remap)?;

                debug!(
                    topic = %topic_name,
                    virtual_partition,
                    virtual_offset,
                    physical_partition = physical.physical_partition,
                    physical_offset = physical.physical_offset,
                    "mapping fetch partition"
                );

                // Store mapping for response filtering
                mapping.add(
                    &topic_name,
                    virtual_partition,
                    physical.physical_partition,
                    virtual_offset,
                );

                // Update to physical values
                partition.partition = physical.physical_partition;
                partition.fetch_offset = physical.physical_offset;
            }
        }

        Ok((request, mapping))
    }

    /// Transform physical partitions/offsets back to virtual and filter records.
    fn virtualize_response(
        &self,
        mut response: FetchResponse,
        mapping: &FetchMapping,
    ) -> Result<FetchResponse> {
        for topic_response in &mut response.responses {
            let topic_name = topic_response.topic.to_string();
            let remapper = self.registry.get_remapper(&topic_name);
            let original_partitions = std::mem::take(&mut topic_response.partitions);
            let mut virtual_partitions = Vec::new();

            for partition_data in original_partitions {
                let physical_partition = partition_data.partition_index;

                // Get virtual partitions that were fetching from this physical one
                let fetch_infos = mapping.get_fetch_info(&topic_name, physical_partition);

                for info in fetch_infos {
                    let mut virtual_data = partition_data.clone();
                    virtual_data.partition_index = info.virtual_partition;

                    // Translate high watermark
                    if virtual_data.high_watermark >= 0 {
                        if let Ok(vm) = remapper
                            .physical_to_virtual(physical_partition, virtual_data.high_watermark)
                        {
                            if vm.virtual_partition == info.virtual_partition {
                                virtual_data.high_watermark = vm.virtual_offset;
                            }
                        }
                    }

                    // Translate log start offset
                    if virtual_data.log_start_offset >= 0 {
                        if let Ok(vm) = remapper
                            .physical_to_virtual(physical_partition, virtual_data.log_start_offset)
                        {
                            if vm.virtual_partition == info.virtual_partition {
                                virtual_data.log_start_offset = vm.virtual_offset;
                            }
                        }
                    }

                    // Translate last stable offset
                    if virtual_data.last_stable_offset >= 0 {
                        if let Ok(vm) = remapper.physical_to_virtual(
                            physical_partition,
                            virtual_data.last_stable_offset,
                        ) {
                            if vm.virtual_partition == info.virtual_partition {
                                virtual_data.last_stable_offset = vm.virtual_offset;
                            }
                        }
                    }

                    // Note: For a complete implementation, we would need to:
                    // 1. Parse the record batches in virtual_data.records
                    // 2. Filter out records whose offsets don't belong to this virtual partition
                    // 3. Translate the remaining record offsets to virtual
                    //
                    // This is complex because record batches have their own format.
                    // For MVP, we'll pass through records and rely on client-side filtering.

                    virtual_partitions.push(virtual_data);
                }
            }

            topic_response.partitions = virtual_partitions;
        }

        Ok(response)
    }
}

#[async_trait]
impl ProtocolHandler for FetchHandler {
    async fn handle(&self, frame: &KafkaFrame) -> Result<BytesMut> {
        debug!(
            correlation_id = frame.correlation_id,
            api_version = frame.api_version,
            "handling Fetch"
        );

        // Decode request header first to skip past it
        // The header includes api_key, api_version, correlation_id, client_id, and for v2+ tagged fields
        let request_header_version = FetchRequest::header_version(frame.api_version);
        let mut request_bytes = bytes::Bytes::copy_from_slice(&frame.bytes);
        let _request_header =
            RequestHeader::decode(&mut request_bytes, request_header_version).map_err(|e| {
                ProxyError::ProtocolDecode {
                    message: format!("failed to decode request header: {e}"),
                }
            })?;

        // Now decode the request body (request_bytes now points past the header)
        let request =
            FetchRequest::decode(&mut request_bytes, frame.api_version).map_err(|e| {
                ProxyError::ProtocolDecode {
                    message: format!("failed to decode FetchRequest: {e}"),
                }
            })?;

        // Transform to physical
        let (physical_request, mapping) = self.physicalize_request(request)?;

        // Encode the physical request with header
        let mut physical_bytes = BytesMut::new();

        // Re-encode request header (preserve original client_id and tagged fields)
        let request_header = RequestHeader::default()
            .with_request_api_key(frame.api_key as i16)
            .with_request_api_version(frame.api_version)
            .with_correlation_id(frame.correlation_id);
        request_header
            .encode(&mut physical_bytes, request_header_version)
            .map_err(|e| ProxyError::ProtocolEncode {
                message: format!("failed to encode request header: {e}"),
            })?;

        // Write request body
        physical_request
            .encode(&mut physical_bytes, frame.api_version)
            .map_err(|e| ProxyError::ProtocolEncode {
                message: e.to_string(),
            })?;

        // Forward to broker
        let response_body = self.broker_pool.send_request(&physical_bytes).await?;

        // Decode broker response header first
        // For Fetch v12+, the response uses a flexible header (version 1) with tagged fields
        let header_version = FetchResponse::header_version(frame.api_version);
        let mut response_bytes = bytes::Bytes::copy_from_slice(&response_body);
        let _header = ResponseHeader::decode(&mut response_bytes, header_version).map_err(|e| {
            ProxyError::ProtocolDecode {
                message: format!("failed to decode response header: {e}"),
            }
        })?;

        // Now decode the response body
        let response =
            FetchResponse::decode(&mut response_bytes, frame.api_version).map_err(|e| {
                ProxyError::ProtocolDecode {
                    message: e.to_string(),
                }
            })?;

        // Virtualize response
        let virtualized = self.virtualize_response(response, &mapping)?;

        // Encode response with proper header
        // For flexible versions (Fetch v12+), the response header includes tagged fields
        let mut buf = BytesMut::new();
        let header_version = FetchResponse::header_version(frame.api_version);

        // For flexible versions (header v1), add empty tagged fields varint (0x00)
        if header_version >= 1 {
            buf.extend_from_slice(&[0u8]);
        }

        virtualized
            .encode(&mut buf, frame.api_version)
            .map_err(|e| ProxyError::ProtocolEncode {
                message: e.to_string(),
            })?;

        Ok(buf)
    }
}

/// Tracks fetch request mappings for response correlation.
struct FetchMapping {
    /// topic -> (physical_partition -> list of fetch infos)
    mappings: HashMap<String, HashMap<i32, Vec<FetchInfo>>>,
}

#[derive(Clone)]
struct FetchInfo {
    virtual_partition: i32,
    virtual_offset: i64,
}

impl FetchMapping {
    fn new() -> Self {
        Self {
            mappings: HashMap::new(),
        }
    }

    fn add(
        &mut self,
        topic: &str,
        virtual_partition: i32,
        physical_partition: i32,
        virtual_offset: i64,
    ) {
        self.mappings
            .entry(topic.to_string())
            .or_default()
            .entry(physical_partition)
            .or_default()
            .push(FetchInfo {
                virtual_partition,
                virtual_offset,
            });
    }

    fn get_fetch_info(&self, topic: &str, physical_partition: i32) -> Vec<FetchInfo> {
        self.mappings
            .get(topic)
            .and_then(|m| m.get(&physical_partition))
            .cloned()
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{KafkaConfig, MappingConfig, SecurityProtocol};

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
    fn test_fetch_mapping() {
        let mut mapping = FetchMapping::new();

        mapping.add("topic1", 0, 0, 100);
        mapping.add("topic1", 10, 0, 200); // Both map to physical 0
        mapping.add("topic1", 5, 5, 300);

        let infos = mapping.get_fetch_info("topic1", 0);
        assert_eq!(infos.len(), 2);

        let infos = mapping.get_fetch_info("topic1", 5);
        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].virtual_partition, 5);
        assert_eq!(infos[0].virtual_offset, 300);
    }
}
