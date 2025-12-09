//! Background metadata refresh task.
//!
//! Periodically fetches metadata from Kafka to discover broker changes
//! (new brokers, address changes, broker removals) without relying on
//! client requests.

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use kafka_protocol::messages::{ApiKey, MetadataRequest, MetadataResponse, RequestHeader};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use super::pool::{BrokerInfo, BrokerPool};

/// Background task that periodically refreshes metadata from Kafka.
pub struct MetadataRefresher {
    broker_pool: Arc<BrokerPool>,
    interval: Duration,
    shutdown_rx: watch::Receiver<bool>,
}

impl MetadataRefresher {
    /// Create a new metadata refresher.
    ///
    /// # Arguments
    ///
    /// * `broker_pool` - The broker pool to update with discovered brokers
    /// * `interval_secs` - Refresh interval in seconds (0 to disable)
    /// * `shutdown_rx` - Shutdown signal receiver
    #[must_use]
    pub fn new(
        broker_pool: Arc<BrokerPool>,
        interval_secs: u64,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            broker_pool,
            interval: Duration::from_secs(interval_secs),
            shutdown_rx,
        }
    }

    /// Run the background refresh loop.
    ///
    /// This method runs until a shutdown signal is received.
    pub async fn run(mut self) {
        if self.interval.is_zero() {
            info!("metadata refresh disabled (interval=0)");
            return;
        }

        info!(
            interval_secs = self.interval.as_secs(),
            "starting background metadata refresh"
        );

        let mut interval = tokio::time::interval(self.interval);
        // Don't refresh immediately on startup - let the proxy connect first
        interval.tick().await;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.refresh_metadata().await;
                }
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        info!("metadata refresher shutting down");
                        break;
                    }
                }
            }
        }
    }

    /// Perform a single metadata refresh.
    async fn refresh_metadata(&self) {
        debug!("refreshing metadata from kafka");

        // Build a metadata request for all topics
        let request = self.build_metadata_request();

        match self.broker_pool.send_request(&request).await {
            Ok(response_bytes) => {
                if let Err(e) = self.process_metadata_response(response_bytes).await {
                    warn!(error = %e, "failed to process metadata response");
                }
            }
            Err(e) => {
                warn!(error = %e, "failed to fetch metadata");
            }
        }
    }

    /// Build a metadata request.
    fn build_metadata_request(&self) -> bytes::Bytes {
        let mut buf = BytesMut::new();

        // Use API version 9 (common version)
        let api_version = 9i16;

        // Build request header
        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::Metadata as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(0)
            .with_client_id(Some(StrBytes::from_static_str("kafka-partition-proxy")));

        // Build metadata request (empty topics = all topics)
        let request = MetadataRequest::default()
            .with_allow_auto_topic_creation(false);

        // Encode header and request
        let mut body = BytesMut::new();
        header.encode(&mut body, api_version).ok();
        request.encode(&mut body, api_version).ok();

        // Prepend length
        let len = body.len() as i32;
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&body);

        buf.freeze()
    }

    /// Process a metadata response and update the broker pool.
    async fn process_metadata_response(
        &self,
        response_bytes: bytes::Bytes,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Skip correlation ID (4 bytes)
        if response_bytes.len() < 4 {
            return Err("response too short".into());
        }

        let mut response_data = response_bytes.slice(4..);
        let response = MetadataResponse::decode(&mut response_data, 9)?;

        let brokers: Vec<BrokerInfo> = response
            .brokers
            .iter()
            .map(|b| BrokerInfo::new(b.node_id.0, b.host.to_string(), b.port))
            .collect();

        let broker_count = brokers.len();
        self.broker_pool.update_brokers(brokers).await;

        debug!(
            broker_count,
            topic_count = response.topics.len(),
            "metadata refresh complete"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_metadata_request() {
        let pool = Arc::new(BrokerPool::new(crate::config::KafkaConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            connection_timeout_ms: 1000,
            request_timeout_ms: 5000,
            metadata_refresh_interval_secs: 30,
        }));

        let (_tx, rx) = watch::channel(false);
        let refresher = MetadataRefresher::new(pool, 30, rx);

        let request = refresher.build_metadata_request();

        // Should have at least length prefix + header
        assert!(request.len() > 8);

        // First 4 bytes are length
        let len = i32::from_be_bytes([request[0], request[1], request[2], request[3]]);
        assert_eq!(len as usize, request.len() - 4);
    }

    #[tokio::test]
    async fn test_disabled_when_interval_zero() {
        let pool = Arc::new(BrokerPool::new(crate::config::KafkaConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            connection_timeout_ms: 1000,
            request_timeout_ms: 5000,
            metadata_refresh_interval_secs: 0,
        }));

        let (_tx, rx) = watch::channel(false);
        let refresher = MetadataRefresher::new(pool, 0, rx);

        // Should return immediately when interval is 0
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            refresher.run(),
        )
        .await;

        assert!(result.is_ok(), "should complete immediately when disabled");
    }
}
