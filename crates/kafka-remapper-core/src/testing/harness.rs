//! Test harness for integration testing of the partition remapping proxy.
//!
//! Provides a complete test environment with:
//! - Mock Kafka broker
//! - Partition remapper
//! - Helper methods for sending requests and verifying responses

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::RwLock;

use crate::broker::BrokerPool;
use crate::config::{KafkaConfig, ListenConfig, MappingConfig, ProxyConfig, SecurityProtocol};
use crate::remapper::PartitionRemapper;

use super::mock_broker::{BrokerCall, MockBroker, ResponseGenerator};

/// Test harness for integration testing.
pub struct ProxyTestHarness {
    /// The mock broker
    mock_broker: MockBroker,
    /// The mock broker address
    mock_broker_addr: String,
    /// The partition remapper
    pub remapper: Arc<PartitionRemapper>,
    /// The broker pool (connected to mock broker)
    pub broker_pool: Arc<BrokerPool>,
    /// Test configuration
    pub config: Arc<ProxyConfig>,
}

impl ProxyTestHarness {
    /// Create a new test harness with default configuration.
    ///
    /// Default config: 100 virtual partitions, 10 physical partitions
    pub async fn new() -> Self {
        Self::with_config(100, 10).await
    }

    /// Create a new test harness with custom partition configuration.
    pub async fn with_config(virtual_partitions: u32, physical_partitions: u32) -> Self {
        // Start mock broker
        let mut mock_broker = MockBroker::new("127.0.0.1:0");
        let mock_broker_addr = mock_broker
            .start()
            .await
            .expect("Failed to start mock broker");

        // Create config
        let config = ProxyConfig {
            listen: ListenConfig {
                address: "127.0.0.1:0".to_string(),
                advertised_address: Some("localhost:19092".to_string()),
                max_connections: 100,
            },
            kafka: KafkaConfig {
                bootstrap_servers: vec![mock_broker_addr.clone()],
                connection_timeout_ms: 5000,
                request_timeout_ms: 10000,
                metadata_refresh_interval_secs: 0, // Disable auto refresh
                security_protocol: SecurityProtocol::Plaintext,
                tls: None,
                sasl: None,
            },
            mapping: MappingConfig {
                virtual_partitions,
                physical_partitions,
                offset_range: 1 << 40, // 1 trillion
            },
            metrics: Default::default(),
            logging: Default::default(),
        };

        let config = Arc::new(config);

        // Create remapper
        let remapper = Arc::new(PartitionRemapper::new(&config.mapping));

        // Create broker pool
        let broker_pool = Arc::new(BrokerPool::new(config.kafka.clone()));

        Self {
            mock_broker,
            mock_broker_addr,
            remapper,
            broker_pool,
            config,
        }
    }

    /// Get the mock broker address.
    #[must_use]
    pub fn broker_address(&self) -> &str {
        &self.mock_broker_addr
    }

    /// Register a custom response handler for an API key.
    pub async fn register_handler(&self, api_key: i16, handler: ResponseGenerator) {
        self.mock_broker.register_handler(api_key, handler).await;
    }

    /// Get all recorded broker calls.
    pub async fn get_broker_calls(&self) -> Vec<BrokerCall> {
        self.mock_broker.get_calls().await
    }

    /// Get broker calls filtered by API key.
    pub async fn get_broker_calls_for_api(&self, api_key: i16) -> Vec<BrokerCall> {
        self.mock_broker.get_calls_for_api(api_key).await
    }

    /// Clear all recorded broker calls.
    pub async fn clear_broker_calls(&self) {
        self.mock_broker.clear_calls().await;
    }

    /// Get the last broker call.
    pub async fn last_broker_call(&self) -> Option<BrokerCall> {
        self.mock_broker.get_calls().await.pop()
    }

    /// Connect the broker pool to the mock broker.
    pub async fn connect(&self) -> crate::error::Result<()> {
        self.broker_pool.connect().await
    }

    /// Send a raw request to the mock broker.
    pub async fn send_request(&self, request_bytes: &[u8]) -> crate::error::Result<Bytes> {
        self.broker_pool.send_request(request_bytes).await
    }

    /// Shutdown the test harness.
    pub async fn shutdown(&mut self) {
        self.mock_broker.stop().await;
    }
}

/// Builder for creating test harness with specific configuration.
pub struct TestHarnessBuilder {
    virtual_partitions: u32,
    physical_partitions: u32,
    offset_range: u64,
}

impl TestHarnessBuilder {
    /// Create a new builder with default values.
    #[must_use]
    pub fn new() -> Self {
        Self {
            virtual_partitions: 100,
            physical_partitions: 10,
            offset_range: 1 << 40,
        }
    }

    /// Set the number of virtual partitions.
    #[must_use]
    pub fn virtual_partitions(mut self, count: u32) -> Self {
        self.virtual_partitions = count;
        self
    }

    /// Set the number of physical partitions.
    #[must_use]
    pub fn physical_partitions(mut self, count: u32) -> Self {
        self.physical_partitions = count;
        self
    }

    /// Set the offset range per virtual partition.
    #[must_use]
    pub fn offset_range(mut self, range: u64) -> Self {
        self.offset_range = range;
        self
    }

    /// Build the test harness.
    pub async fn build(self) -> ProxyTestHarness {
        let mut harness =
            ProxyTestHarness::with_config(self.virtual_partitions, self.physical_partitions).await;

        // If custom offset range, recreate remapper
        if self.offset_range != (1 << 40) {
            let config = MappingConfig {
                virtual_partitions: self.virtual_partitions,
                physical_partitions: self.physical_partitions,
                offset_range: self.offset_range,
            };
            harness.remapper = Arc::new(PartitionRemapper::new(&config));
        }

        harness
    }
}

impl Default for TestHarnessBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_harness_creation() {
        let harness = ProxyTestHarness::new().await;

        assert_eq!(harness.remapper.virtual_partitions(), 100);
        assert_eq!(harness.remapper.physical_partitions(), 10);
        assert!(!harness.broker_address().is_empty());
    }

    #[tokio::test]
    async fn test_harness_with_custom_config() {
        let harness = ProxyTestHarness::with_config(1000, 100).await;

        assert_eq!(harness.remapper.virtual_partitions(), 1000);
        assert_eq!(harness.remapper.physical_partitions(), 100);
    }

    #[tokio::test]
    async fn test_harness_builder() {
        let harness = TestHarnessBuilder::new()
            .virtual_partitions(500)
            .physical_partitions(50)
            .build()
            .await;

        assert_eq!(harness.remapper.virtual_partitions(), 500);
        assert_eq!(harness.remapper.physical_partitions(), 50);
    }
}
