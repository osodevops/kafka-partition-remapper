//! Per-connection request handler.
//!
//! Manages a single client connection, parsing requests, dispatching to handlers,
//! and sending responses.
//!
//! When SASL authentication is enabled, the handler performs SASL handshake
//! before allowing other Kafka operations.
//!
//! # Principal Handling
//!
//! The handler maintains a `ConnectionContext` that carries the authenticated
//! principal throughout the connection lifecycle. The principal is:
//!
//! 1. Initially set from TLS certificates (for mTLS) or anonymous
//! 2. Upgraded after successful SASL authentication
//! 3. Available for logging, authorization, and audit

use std::sync::Arc;

use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use kafka_protocol::messages::ApiKey;
use tokio::sync::broadcast;
use tokio_util::codec::Framed;
use tracing::{debug, info, instrument, warn};

use crate::auth::{Principal, SaslServer};
use crate::config::ProxyConfig;
use crate::error::{ProxyError, Result};
use crate::handlers::SaslHandler;

use super::client_stream::ClientStream;
use super::codec::{KafkaCodec, KafkaFrame, ResponseFrame};
use super::context::ConnectionContext;

/// Handles requests for a single client connection.
///
/// The handler maintains a `ConnectionContext` that carries authentication
/// information and connection metadata. The principal in the context can be
/// upgraded after SASL authentication completes.
pub struct ConnectionHandler {
    config: Arc<ProxyConfig>,
    shutdown_rx: broadcast::Receiver<()>,
    /// Optional SASL server for client authentication.
    sasl_server: Option<Arc<SaslServer>>,
    /// Connection context with principal and metadata.
    context: ConnectionContext,
    /// Flag to track if principal has been upgraded after SASL auth.
    principal_upgraded: bool,
}

impl ConnectionHandler {
    /// Create a new connection handler with context.
    ///
    /// # Arguments
    ///
    /// * `config` - Proxy configuration
    /// * `shutdown_rx` - Receiver for shutdown signals
    /// * `sasl_server` - Optional SASL server for authentication
    /// * `context` - Connection context with initial principal
    #[must_use]
    pub fn new(
        config: Arc<ProxyConfig>,
        shutdown_rx: broadcast::Receiver<()>,
        sasl_server: Option<Arc<SaslServer>>,
        context: ConnectionContext,
    ) -> Self {
        Self {
            config,
            shutdown_rx,
            sasl_server,
            context,
            principal_upgraded: false,
        }
    }

    /// Check if SASL authentication is required.
    #[must_use]
    pub fn requires_sasl(&self) -> bool {
        self.sasl_server.is_some()
    }

    /// Get the connection context.
    #[must_use]
    pub fn context(&self) -> &ConnectionContext {
        &self.context
    }

    /// Get the authenticated principal.
    #[must_use]
    pub fn principal(&self) -> &Principal {
        self.context.principal()
    }

    /// Handle the connection, processing requests until disconnect or shutdown.
    ///
    /// If SASL authentication is configured, the handler will require
    /// successful authentication before allowing other Kafka operations.
    ///
    /// # Errors
    ///
    /// Returns an error if there's a protocol error or connection failure.
    #[instrument(skip(self, stream), fields(
        peer = %stream.peer_addr().map(|a| a.to_string()).unwrap_or_else(|_| "unknown".to_string()),
        connection_id = %self.context.connection_id(),
        principal = %self.context.principal()
    ))]
    pub async fn handle(mut self, stream: ClientStream) -> Result<()> {
        info!(
            principal = %self.context.principal(),
            protocol = ?self.context.security_protocol(),
            "client connected"
        );

        let mut framed = Framed::new(stream, KafkaCodec::new());

        // Create SASL handler if authentication is required
        let mut sasl_handler = self
            .sasl_server
            .as_ref()
            .map(|server| SaslHandler::new(Arc::clone(server)));

        loop {
            tokio::select! {
                result = framed.next() => {
                    match result {
                        Some(Ok(frame)) => {
                            debug!(
                                api_key = ?frame.api_key,
                                api_version = frame.api_version,
                                correlation_id = frame.correlation_id,
                                principal = %self.context.principal(),
                                "received request"
                            );

                            // Handle SASL authentication flow
                            let response = if let Some(ref mut handler) = sasl_handler {
                                let response = self.dispatch_with_sasl(frame, handler).await?;

                                // Upgrade principal after successful SASL authentication
                                if !self.principal_upgraded && handler.is_authenticated() {
                                    if let Some(username) = handler.authenticated_user() {
                                        self.context = self.context.clone().with_sasl_principal(username.to_string());
                                        self.principal_upgraded = true;
                                        info!(
                                            principal = %self.context.principal(),
                                            "principal upgraded after SASL authentication"
                                        );
                                    }
                                }

                                response
                            } else {
                                self.dispatch_request(frame).await?
                            };

                            framed.send(response).await.map_err(ProxyError::from)?;
                        }
                        Some(Err(e)) => {
                            warn!(
                                principal = %self.context.principal(),
                                error = %e,
                                "protocol decode error"
                            );
                            return Err(ProxyError::ProtocolDecode {
                                message: e.to_string(),
                            });
                        }
                        None => {
                            debug!(principal = %self.context.principal(), "client disconnected");
                            break;
                        }
                    }
                }
                _ = self.shutdown_rx.recv() => {
                    debug!(principal = %self.context.principal(), "shutdown during connection handling");
                    return Err(ProxyError::Shutdown);
                }
            }
        }

        Ok(())
    }

    /// Dispatch a request with SASL authentication enforcement.
    ///
    /// Before authentication is complete, only ApiVersions, SaslHandshake,
    /// and SaslAuthenticate requests are allowed.
    async fn dispatch_with_sasl(
        &self,
        frame: KafkaFrame,
        sasl_handler: &mut SaslHandler,
    ) -> Result<ResponseFrame> {
        // ApiVersions is always allowed (clients need this to negotiate)
        if frame.api_key == ApiKey::ApiVersions {
            return self.handle_api_versions(frame).await;
        }

        // If already authenticated, proceed with normal dispatch
        if sasl_handler.is_authenticated() {
            return self.dispatch_request(frame).await;
        }

        // Before authentication, only SASL requests are allowed
        match frame.api_key {
            ApiKey::SaslHandshake => {
                debug!(principal = %self.context.principal(), "handling SaslHandshake");
                sasl_handler.handle_sasl_handshake(
                    frame.correlation_id,
                    frame.api_version,
                    &frame.bytes,
                )
            }
            ApiKey::SaslAuthenticate => {
                debug!(principal = %self.context.principal(), "handling SaslAuthenticate");
                sasl_handler.handle_sasl_authenticate(
                    frame.correlation_id,
                    frame.api_version,
                    &frame.bytes,
                )
            }
            _ => {
                warn!(
                    api_key = ?frame.api_key,
                    principal = %self.context.principal(),
                    "request rejected: SASL authentication required"
                );
                Err(ProxyError::Auth(crate::error::AuthError::AuthenticationFailed(
                    "SASL authentication required".to_string(),
                )))
            }
        }
    }

    /// Dispatch a request to the appropriate handler.
    async fn dispatch_request(&self, frame: KafkaFrame) -> Result<ResponseFrame> {
        match frame.api_key {
            ApiKey::ApiVersions => self.handle_api_versions(frame).await,
            ApiKey::Metadata => self.handle_metadata(frame).await,
            ApiKey::Produce => self.handle_produce(frame).await,
            ApiKey::Fetch => self.handle_fetch(frame).await,
            // Group coordination - passthrough is CORRECT for these APIs.
            // JoinGroup/SyncGroup: Partition assignments happen client-side using
            // Metadata (which returns virtual partitions). The broker treats
            // ConsumerProtocolAssignment as opaque bytes, so no transformation needed.
            //
            // OffsetCommit/OffsetFetch: Passthrough is REQUIRED to avoid data loss.
            // If we translated partition numbers, multiple virtual partitions would
            // map to the same physical partition key in __consumer_offsets, causing
            // offset overwrites. Passthrough stores offsets keyed by virtual partition.
            // WARNING: Do NOT wire up OffsetCommitHandler/OffsetFetchHandler - they are broken.
            //
            // See docs/implementation-plan.md Appendix A for detailed analysis.
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
        debug!(
            correlation_id = frame.correlation_id,
            "handling ApiVersions"
        );

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
    use std::collections::HashMap;
    use std::net::SocketAddr;

    use super::*;
    use crate::auth::AuthMethod;
    use crate::config::{
        KafkaConfig, ListenConfig, LoggingConfig, MappingConfig, MetricsConfig, SecurityProtocol,
    };

    fn test_config() -> Arc<ProxyConfig> {
        Arc::new(ProxyConfig {
            listen: ListenConfig::default(),
            kafka: KafkaConfig {
                bootstrap_servers: vec!["localhost:9092".to_string()],
                connection_timeout_ms: 10000,
                request_timeout_ms: 30000,
                metadata_refresh_interval_secs: 0,
                security_protocol: SecurityProtocol::Plaintext,
                tls: None,
                sasl: None,
            },
            mapping: MappingConfig {
                virtual_partitions: 100,
                physical_partitions: 10,
                offset_range: 1 << 40,
                topics: HashMap::new(),
            },
            metrics: MetricsConfig::default(),
            logging: LoggingConfig::default(),
        })
    }

    fn test_context() -> ConnectionContext {
        let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        ConnectionContext::anonymous(addr, "test-conn".to_string())
    }

    #[tokio::test]
    async fn test_handle_api_versions_stub() {
        let (tx, rx) = broadcast::channel(1);
        let handler = ConnectionHandler::new(test_config(), rx, None, test_context());

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
        let handler = ConnectionHandler::new(test_config(), rx, None, test_context());

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
        let handler = ConnectionHandler::new(test_config(), rx, None, test_context());

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

    #[test]
    fn test_handler_principal_access() {
        let (tx, rx) = broadcast::channel(1);
        let addr: SocketAddr = "192.168.1.100:9000".parse().unwrap();
        let context = ConnectionContext::new(
            Principal::new("test-user", AuthMethod::Ssl),
            addr,
            "conn-test".to_string(),
            SecurityProtocol::Ssl,
        );
        let handler = ConnectionHandler::new(test_config(), rx, None, context);

        assert_eq!(handler.principal().name, "test-user");
        assert_eq!(handler.principal().auth_method, AuthMethod::Ssl);
        assert_eq!(handler.context().connection_id(), "conn-test");

        drop(tx);
    }
}
