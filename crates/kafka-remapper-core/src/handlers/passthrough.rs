//! Passthrough request handler.
//!
//! Forwards requests to the broker without modification.
//! Used for group coordination APIs that don't require remapping.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::BytesMut;
use tracing::debug;

use crate::broker::BrokerPool;
use crate::error::Result;
use crate::network::codec::KafkaFrame;

use super::ProtocolHandler;

/// Handler that passes requests through to the broker unchanged.
pub struct PassthroughHandler {
    broker_pool: Arc<BrokerPool>,
}

impl PassthroughHandler {
    /// Create a new passthrough handler.
    #[must_use]
    pub fn new(broker_pool: Arc<BrokerPool>) -> Self {
        Self { broker_pool }
    }
}

#[async_trait]
impl ProtocolHandler for PassthroughHandler {
    async fn handle(&self, frame: &KafkaFrame) -> Result<BytesMut> {
        debug!(
            api_key = ?frame.api_key,
            correlation_id = frame.correlation_id,
            "passthrough request"
        );

        // Forward request as-is
        let response_body = self.broker_pool.send_request(&frame.bytes).await?;

        // Return response body without correlation ID (first 4 bytes)
        Ok(BytesMut::from(&response_body[4..]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::KafkaConfig;

    fn test_pool() -> Arc<BrokerPool> {
        Arc::new(BrokerPool::new(KafkaConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            connection_timeout_ms: 100,
            request_timeout_ms: 1000,
            metadata_refresh_interval_secs: 0,
        }))
    }

    #[test]
    fn test_passthrough_creation() {
        let handler = PassthroughHandler::new(test_pool());
        // Just verify it can be created
        assert!(Arc::strong_count(&handler.broker_pool) > 0);
    }
}
