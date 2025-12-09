//! Per-connection request handler.
//!
//! Manages a single client connection, parsing requests, dispatching to handlers,
//! and sending responses.

use std::sync::Arc;

use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use kafka_protocol::messages::ApiKey;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio_util::codec::Framed;
use tracing::{debug, instrument};

use crate::config::ProxyConfig;
use crate::error::{ProxyError, Result};

use super::codec::{KafkaCodec, KafkaFrame, ResponseFrame};

/// Handles requests for a single client connection.
pub struct ConnectionHandler {
    config: Arc<ProxyConfig>,
    shutdown_rx: broadcast::Receiver<()>,
}

impl ConnectionHandler {
    /// Create a new connection handler.
    #[must_use]
    pub fn new(config: Arc<ProxyConfig>, shutdown_rx: broadcast::Receiver<()>) -> Self {
        Self {
            config,
            shutdown_rx,
        }
    }

    /// Handle the connection, processing requests until disconnect or shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if there's a protocol error or connection failure.
    #[instrument(skip(self, socket), fields(peer = %socket.peer_addr().unwrap_or_else(|_| "unknown".parse().unwrap())))]
    pub async fn handle(mut self, socket: TcpStream) -> Result<()> {
        let mut framed = Framed::new(socket, KafkaCodec::new());

        loop {
            tokio::select! {
                result = framed.next() => {
                    match result {
                        Some(Ok(frame)) => {
                            debug!(
                                api_key = ?frame.api_key,
                                api_version = frame.api_version,
                                correlation_id = frame.correlation_id,
                                "received request"
                            );

                            let response = self.dispatch_request(frame).await?;
                            framed.send(response).await.map_err(ProxyError::from)?;
                        }
                        Some(Err(e)) => {
                            return Err(ProxyError::ProtocolDecode {
                                message: e.to_string(),
                            });
                        }
                        None => {
                            debug!("client disconnected");
                            break;
                        }
                    }
                }
                _ = self.shutdown_rx.recv() => {
                    debug!("shutdown during connection handling");
                    return Err(ProxyError::Shutdown);
                }
            }
        }

        Ok(())
    }

    /// Dispatch a request to the appropriate handler.
    async fn dispatch_request(&self, frame: KafkaFrame) -> Result<ResponseFrame> {
        match frame.api_key {
            ApiKey::ApiVersions => self.handle_api_versions(frame).await,
            ApiKey::Metadata => self.handle_metadata(frame).await,
            ApiKey::Produce => self.handle_produce(frame).await,
            ApiKey::Fetch => self.handle_fetch(frame).await,
            // Group coordination - passthrough for now
            ApiKey::FindCoordinator
            | ApiKey::JoinGroup
            | ApiKey::SyncGroup
            | ApiKey::Heartbeat
            | ApiKey::LeaveGroup
            | ApiKey::OffsetCommit
            | ApiKey::OffsetFetch
            | ApiKey::ListOffsets => self.handle_passthrough(frame).await,
            _ => Err(ProxyError::UnsupportedApi {
                api_key: frame.api_key as i16,
                api_version: frame.api_version,
            }),
        }
    }

    /// Handle ApiVersions request.
    ///
    /// For now, return a stub response. Will be implemented in Phase 5.
    async fn handle_api_versions(&self, frame: KafkaFrame) -> Result<ResponseFrame> {
        debug!(correlation_id = frame.correlation_id, "handling ApiVersions");

        // TODO: Implement proper ApiVersions response in Phase 5
        // For now, return a minimal valid response

        // ApiVersions response format (simplified v0):
        // - error_code (2 bytes): 0 = no error
        // - api_keys array length (4 bytes)
        // - (empty array for now)

        let mut body = BytesMut::new();
        body.extend_from_slice(&[0, 0]); // error_code = 0
        body.extend_from_slice(&[0, 0, 0, 0]); // empty array

        Ok(ResponseFrame {
            correlation_id: frame.correlation_id,
            body,
        })
    }

    /// Handle Metadata request.
    ///
    /// Stub - will be implemented in Phase 5.
    async fn handle_metadata(&self, frame: KafkaFrame) -> Result<ResponseFrame> {
        debug!(correlation_id = frame.correlation_id, "handling Metadata");

        // TODO: Implement proper Metadata response with partition virtualization
        let mut body = BytesMut::new();
        // Minimal response with empty brokers and topics
        body.extend_from_slice(&[0, 0, 0, 0]); // empty brokers array
        body.extend_from_slice(&[0, 0, 0, 0]); // empty topics array

        Ok(ResponseFrame {
            correlation_id: frame.correlation_id,
            body,
        })
    }

    /// Handle Produce request.
    ///
    /// Stub - will be implemented in Phase 5.
    async fn handle_produce(&self, frame: KafkaFrame) -> Result<ResponseFrame> {
        debug!(correlation_id = frame.correlation_id, "handling Produce");

        // TODO: Implement partition remapping for produce
        Err(ProxyError::BrokerUnavailable {
            broker_id: -1,
            message: "produce handler not implemented".to_string(),
        })
    }

    /// Handle Fetch request.
    ///
    /// Stub - will be implemented in Phase 5.
    async fn handle_fetch(&self, frame: KafkaFrame) -> Result<ResponseFrame> {
        debug!(correlation_id = frame.correlation_id, "handling Fetch");

        // TODO: Implement partition remapping for fetch
        Err(ProxyError::BrokerUnavailable {
            broker_id: -1,
            message: "fetch handler not implemented".to_string(),
        })
    }

    /// Handle passthrough request (forward to broker unchanged).
    ///
    /// Stub - will be implemented in Phase 4/5.
    async fn handle_passthrough(&self, frame: KafkaFrame) -> Result<ResponseFrame> {
        debug!(
            api_key = ?frame.api_key,
            correlation_id = frame.correlation_id,
            "handling passthrough"
        );

        // TODO: Forward to broker connection pool
        Err(ProxyError::BrokerUnavailable {
            broker_id: -1,
            message: "passthrough handler not implemented".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{KafkaConfig, ListenConfig, LoggingConfig, MappingConfig, MetricsConfig};

    fn test_config() -> Arc<ProxyConfig> {
        Arc::new(ProxyConfig {
            listen: ListenConfig::default(),
            kafka: KafkaConfig {
                bootstrap_servers: vec!["localhost:9092".to_string()],
                connection_timeout_ms: 10000,
                request_timeout_ms: 30000,
                metadata_refresh_interval_secs: 0,
            },
            mapping: MappingConfig {
                virtual_partitions: 100,
                physical_partitions: 10,
                offset_range: 1 << 40,
            },
            metrics: MetricsConfig::default(),
            logging: LoggingConfig::default(),
        })
    }

    #[tokio::test]
    async fn test_handle_api_versions_stub() {
        let (tx, rx) = broadcast::channel(1);
        let handler = ConnectionHandler::new(test_config(), rx);

        let frame = KafkaFrame {
            api_key: ApiKey::ApiVersions,
            api_version: 3,
            correlation_id: 12345,
            bytes: BytesMut::new(),
        };

        let response = handler.handle_api_versions(frame).await.unwrap();
        assert_eq!(response.correlation_id, 12345);
        // Should have some response body
        assert!(!response.body.is_empty());

        drop(tx);
    }

    #[tokio::test]
    async fn test_handle_metadata_stub() {
        let (tx, rx) = broadcast::channel(1);
        let handler = ConnectionHandler::new(test_config(), rx);

        let frame = KafkaFrame {
            api_key: ApiKey::Metadata,
            api_version: 9,
            correlation_id: 54321,
            bytes: BytesMut::new(),
        };

        let response = handler.handle_metadata(frame).await.unwrap();
        assert_eq!(response.correlation_id, 54321);

        drop(tx);
    }

    #[tokio::test]
    async fn test_unsupported_api() {
        let (tx, rx) = broadcast::channel(1);
        let handler = ConnectionHandler::new(test_config(), rx);

        let frame = KafkaFrame {
            api_key: ApiKey::CreateTopics, // Not supported
            api_version: 0,
            correlation_id: 1,
            bytes: BytesMut::new(),
        };

        let result = handler.dispatch_request(frame).await;
        assert!(matches!(result, Err(ProxyError::UnsupportedApi { .. })));

        drop(tx);
    }
}
