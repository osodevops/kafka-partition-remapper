//! Metadata request handler.
//!
//! Transforms the Metadata response to show virtual partitions instead of
//! physical partitions. Each physical partition is expanded to multiple
//! virtual partitions based on the compression ratio.
//!
//! Also rewrites broker addresses so clients connect back to the proxy
//! instead of directly to Kafka brokers.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::BytesMut;
use kafka_protocol::messages::MetadataResponse;
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use tracing::{debug, warn};

use crate::broker::BrokerPool;
use crate::config::ListenConfig;
use crate::error::{ProxyError, Result};
use crate::network::codec::KafkaFrame;
use crate::remapper::PartitionRemapper;

use super::ProtocolHandler;

/// Handler for Metadata requests.
pub struct MetadataHandler {
    remapper: Arc<PartitionRemapper>,
    broker_pool: Arc<BrokerPool>,
    /// Advertised host for broker address rewriting.
    advertised_host: String,
    /// Advertised port for broker address rewriting.
    advertised_port: i32,
}

impl MetadataHandler {
    /// Create a new Metadata handler.
    ///
    /// # Arguments
    ///
    /// * `remapper` - Partition remapping logic
    /// * `broker_pool` - Pool of broker connections
    /// * `listen_config` - Listen configuration with advertised address
    ///
    /// # Panics
    ///
    /// Panics if the advertised address cannot be parsed.
    #[must_use]
    pub fn new(
        remapper: Arc<PartitionRemapper>,
        broker_pool: Arc<BrokerPool>,
        listen_config: &ListenConfig,
    ) -> Self {
        let (host, port) = listen_config
            .parse_advertised_address()
            .expect("advertised address must be valid");

        Self {
            remapper,
            broker_pool,
            advertised_host: host,
            advertised_port: port,
        }
    }

    /// Rewrite broker addresses in the response to point to the proxy.
    ///
    /// All brokers are rewritten to use the proxy's advertised address.
    /// Clients will connect to the proxy for all requests.
    fn rewrite_broker_addresses(&self, response: &mut MetadataResponse) {
        debug!(
            original_brokers = response.brokers.len(),
            advertised_host = %self.advertised_host,
            advertised_port = self.advertised_port,
            "rewriting broker addresses to proxy"
        );

        for broker in &mut response.brokers {
            let original_host = broker.host.to_string();
            let original_port = broker.port;

            broker.host = StrBytes::from_string(self.advertised_host.clone());
            broker.port = self.advertised_port;

            debug!(
                broker_id = broker.node_id.0,
                original = %format!("{}:{}", original_host, original_port),
                rewritten = %format!("{}:{}", self.advertised_host, self.advertised_port),
                "rewrote broker address"
            );
        }
    }

    /// Expand physical partitions to virtual partitions in the response.
    ///
    /// For each topic, replaces the physical partition list with virtual partitions.
    /// Each physical partition's metadata (leader, replicas, ISR) is copied to all
    /// virtual partitions that map to it.
    fn virtualize_response(&self, mut response: MetadataResponse) -> MetadataResponse {
        for topic in &mut response.topics {
            let original_partitions = std::mem::take(&mut topic.partitions);
            let mut virtual_partitions = Vec::new();

            // For each virtual partition, find its physical partition and copy metadata
            for v_idx in 0..self.remapper.virtual_partitions() {
                let v_idx_i32 = v_idx as i32;

                // Map virtual to physical
                if let Ok(mapping) = self.remapper.virtual_to_physical(v_idx_i32) {
                    // Find the physical partition's metadata
                    if let Some(physical) = original_partitions
                        .iter()
                        .find(|p| p.partition_index == mapping.physical_partition)
                    {
                        // Clone physical partition metadata for this virtual partition
                        let mut virtual_partition = physical.clone();
                        virtual_partition.partition_index = v_idx_i32;
                        virtual_partitions.push(virtual_partition);
                    }
                }
            }

            // Sort by partition index
            virtual_partitions.sort_by_key(|p| p.partition_index);
            topic.partitions = virtual_partitions;
        }

        response
    }
}

#[async_trait]
impl ProtocolHandler for MetadataHandler {
    async fn handle(&self, frame: &KafkaFrame) -> Result<BytesMut> {
        debug!(
            correlation_id = frame.correlation_id,
            api_version = frame.api_version,
            "handling Metadata"
        );

        // Forward request to broker
        let response_body = self.broker_pool.send_request(&frame.bytes).await?;

        // Decode broker response (skip correlation ID - first 4 bytes)
        let mut response_bytes = response_body.slice(4..);
        let mut response =
            MetadataResponse::decode(&mut response_bytes, frame.api_version).map_err(|e| {
                ProxyError::ProtocolDecode {
                    message: e.to_string(),
                }
            })?;

        debug!(
            brokers = response.brokers.len(),
            topics = response.topics.len(),
            "received broker metadata"
        );

        // Update broker pool with discovered brokers (use REAL addresses for backend)
        let brokers: Vec<_> = response
            .brokers
            .iter()
            .map(|b| crate::broker::pool::BrokerInfo::new(b.node_id.0, b.host.to_string(), b.port))
            .collect();
        self.broker_pool.update_brokers(brokers).await;

        // Rewrite broker addresses to point to proxy (BEFORE returning to client)
        self.rewrite_broker_addresses(&mut response);

        // Virtualize partitions
        let virtualized = self.virtualize_response(response);

        debug!(
            topics = virtualized.topics.len(),
            virtual_partitions = self.remapper.virtual_partitions(),
            "virtualized metadata response"
        );

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{KafkaConfig, ListenConfig, MappingConfig, SecurityProtocol};
    use kafka_protocol::messages::metadata_response::{
        MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    };
    use kafka_protocol::messages::BrokerId;
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

    fn test_listen_config() -> ListenConfig {
        ListenConfig {
            address: "0.0.0.0:9092".to_string(),
            advertised_address: Some("proxy.example.com:9092".to_string()),
            max_connections: 1000,
        }
    }

    #[test]
    fn test_virtualize_response() {
        let handler = MetadataHandler::new(test_remapper(), test_pool(), &test_listen_config());

        // Create a response with 10 physical partitions
        let mut response = MetadataResponse::default();

        let mut topic = MetadataResponseTopic::default();
        topic.name = Some(kafka_protocol::messages::TopicName::from(
            StrBytes::from_static_str("test-topic"),
        ));

        for p_idx in 0..10 {
            let mut partition = MetadataResponsePartition::default();
            partition.partition_index = p_idx;
            partition.leader_id = kafka_protocol::messages::BrokerId(1);
            topic.partitions.push(partition);
        }

        response.topics.push(topic);

        // Virtualize
        let virtualized = handler.virtualize_response(response);

        // Should now have 100 virtual partitions
        assert_eq!(virtualized.topics.len(), 1);
        assert_eq!(virtualized.topics[0].partitions.len(), 100);

        // Verify partition indices are 0-99
        for (i, partition) in virtualized.topics[0].partitions.iter().enumerate() {
            assert_eq!(partition.partition_index, i as i32);
        }

        // Verify leader is copied correctly
        // Virtual partition 0 maps to physical 0
        assert_eq!(
            virtualized.topics[0].partitions[0].leader_id,
            kafka_protocol::messages::BrokerId(1)
        );
    }

    #[test]
    fn test_rewrite_broker_addresses() {
        let handler = MetadataHandler::new(test_remapper(), test_pool(), &test_listen_config());

        // Create a response with real broker addresses
        let mut response = MetadataResponse::default();

        // Add some brokers with real Kafka addresses
        let mut broker1 = MetadataResponseBroker::default();
        broker1.node_id = BrokerId(1);
        broker1.host = StrBytes::from_static_str("kafka-1.internal");
        broker1.port = 9092;
        response.brokers.push(broker1);

        let mut broker2 = MetadataResponseBroker::default();
        broker2.node_id = BrokerId(2);
        broker2.host = StrBytes::from_static_str("kafka-2.internal");
        broker2.port = 9092;
        response.brokers.push(broker2);

        // Rewrite addresses
        handler.rewrite_broker_addresses(&mut response);

        // All brokers should now point to proxy
        assert_eq!(response.brokers.len(), 2);
        for broker in &response.brokers {
            assert_eq!(broker.host.to_string(), "proxy.example.com");
            assert_eq!(broker.port, 9092);
        }
    }

    #[test]
    fn test_rewrite_preserves_broker_ids() {
        let handler = MetadataHandler::new(test_remapper(), test_pool(), &test_listen_config());

        let mut response = MetadataResponse::default();

        let mut broker = MetadataResponseBroker::default();
        broker.node_id = BrokerId(42);
        broker.host = StrBytes::from_static_str("kafka.internal");
        broker.port = 9092;
        response.brokers.push(broker);

        handler.rewrite_broker_addresses(&mut response);

        // Broker ID should be preserved
        assert_eq!(response.brokers[0].node_id, BrokerId(42));
    }

    #[test]
    fn test_listen_config_fallback_to_address() {
        // When advertised_address is not set, use address
        let config = ListenConfig {
            address: "0.0.0.0:9092".to_string(),
            advertised_address: None,
            max_connections: 1000,
        };

        let (host, port) = config.parse_advertised_address().unwrap();
        assert_eq!(host, "0.0.0.0");
        assert_eq!(port, 9092);
    }

    #[test]
    fn test_listen_config_uses_advertised() {
        let config = ListenConfig {
            address: "0.0.0.0:9092".to_string(),
            advertised_address: Some("kafka-proxy.prod:19092".to_string()),
            max_connections: 1000,
        };

        let (host, port) = config.parse_advertised_address().unwrap();
        assert_eq!(host, "kafka-proxy.prod");
        assert_eq!(port, 19092);
    }
}
