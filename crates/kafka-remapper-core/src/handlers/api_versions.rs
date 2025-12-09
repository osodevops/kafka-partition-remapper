//! ApiVersions request handler.
//!
//! Forwards the ApiVersions request to the broker and filters the response
//! to only include APIs that the proxy supports.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::BytesMut;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::{ApiKey, ApiVersionsResponse};
use kafka_protocol::protocol::{Decodable, Encodable};
use tracing::debug;

use crate::broker::BrokerPool;
use crate::error::{ProxyError, Result};
use crate::network::codec::KafkaFrame;

use super::ProtocolHandler;

/// Supported API keys and their version ranges.
///
/// The proxy only supports these APIs - others will be rejected.
const SUPPORTED_APIS: &[(ApiKey, i16, i16)] = &[
    (ApiKey::ApiVersions, 0, 3),
    (ApiKey::Metadata, 1, 12),
    (ApiKey::Produce, 3, 9),
    (ApiKey::Fetch, 4, 13),
    // Group coordination - passthrough
    (ApiKey::FindCoordinator, 0, 4),
    (ApiKey::JoinGroup, 0, 7),
    (ApiKey::SyncGroup, 0, 5),
    (ApiKey::Heartbeat, 0, 4),
    (ApiKey::LeaveGroup, 0, 4),
    (ApiKey::OffsetCommit, 0, 8),
    (ApiKey::OffsetFetch, 0, 8),
    (ApiKey::ListOffsets, 0, 7),
];

/// Handler for ApiVersions requests.
pub struct ApiVersionsHandler {
    broker_pool: Arc<BrokerPool>,
}

impl ApiVersionsHandler {
    /// Create a new ApiVersions handler.
    #[must_use]
    pub fn new(broker_pool: Arc<BrokerPool>) -> Self {
        Self { broker_pool }
    }

    /// Build a response without forwarding to broker.
    ///
    /// This is used when we can't reach a broker or want to respond locally.
    fn build_local_response(&self, api_version: i16) -> Result<BytesMut> {
        let mut response = ApiVersionsResponse::default();
        response.error_code = 0; // No error

        for &(api_key, min_version, max_version) in SUPPORTED_APIS {
            let mut api = ApiVersion::default();
            api.api_key = api_key as i16;
            api.min_version = min_version;
            api.max_version = max_version;
            response.api_keys.push(api);
        }

        let mut buf = BytesMut::new();
        response
            .encode(&mut buf, api_version)
            .map_err(|e| ProxyError::ProtocolEncode {
                message: e.to_string(),
            })?;

        Ok(buf)
    }
}

#[async_trait]
impl ProtocolHandler for ApiVersionsHandler {
    async fn handle(&self, frame: &KafkaFrame) -> Result<BytesMut> {
        debug!(
            correlation_id = frame.correlation_id,
            api_version = frame.api_version,
            "handling ApiVersions"
        );

        // Try to forward to broker to get actual supported versions
        let response_body = match self.broker_pool.send_request(&frame.bytes).await {
            Ok(response) => response,
            Err(e) => {
                debug!(error = %e, "broker unavailable, using local response");
                return self.build_local_response(frame.api_version);
            }
        };

        // Decode broker response (skip correlation ID - first 4 bytes)
        let mut response_bytes = response_body.slice(4..);
        let mut broker_response =
            ApiVersionsResponse::decode(&mut response_bytes, frame.api_version).map_err(|e| {
                ProxyError::ProtocolDecode {
                    message: e.to_string(),
                }
            })?;

        // Filter to only APIs we support
        broker_response.api_keys.retain(|api| {
            SUPPORTED_APIS
                .iter()
                .any(|(key, _, _)| *key as i16 == api.api_key)
        });

        // Clamp versions to what we support
        for api in &mut broker_response.api_keys {
            if let Some((_, min, max)) = SUPPORTED_APIS
                .iter()
                .find(|(key, _, _)| *key as i16 == api.api_key)
            {
                api.min_version = api.min_version.max(*min);
                api.max_version = api.max_version.min(*max);
            }
        }

        // Encode response
        let mut buf = BytesMut::new();
        broker_response
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
    use bytes::Bytes;
    use crate::config::{KafkaConfig, SecurityProtocol};

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

    #[tokio::test]
    async fn test_build_local_response() {
        let handler = ApiVersionsHandler::new(test_pool());
        let response = handler.build_local_response(3).unwrap();
        assert!(!response.is_empty());

        // Decode and verify
        let mut bytes = Bytes::from(response.to_vec());
        let decoded = ApiVersionsResponse::decode(&mut bytes, 3).unwrap();
        assert_eq!(decoded.error_code, 0);
        assert!(!decoded.api_keys.is_empty());

        // Should include ApiVersions
        assert!(decoded
            .api_keys
            .iter()
            .any(|a| a.api_key == ApiKey::ApiVersions as i16));
    }
}
