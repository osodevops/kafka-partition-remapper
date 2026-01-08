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
use crate::broker::BrokerPool;
use crate::config::ProxyConfig;
use crate::error::{ProxyError, Result};
use crate::handlers::{
    ApiVersionsHandler, FetchHandler, MetadataHandler, PassthroughHandler, ProduceHandler,
    ProtocolHandler, SaslHandler,
};
use crate::remapper::TopicRemapperRegistry;

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
    /// Handler for ApiVersions requests.
    api_versions_handler: ApiVersionsHandler,
    /// Handler for Metadata requests.
    metadata_handler: MetadataHandler,
    /// Handler for Produce requests.
    produce_handler: ProduceHandler,
    /// Handler for Fetch requests.
    fetch_handler: FetchHandler,
    /// Handler for passthrough requests (group coordination, offsets).
    passthrough_handler: PassthroughHandler,
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
    /// * `broker_pool` - Pool of broker connections for forwarding requests
    /// * `registry` - Topic-aware partition remapper registry
    #[must_use]
    pub fn new(
        config: Arc<ProxyConfig>,
        shutdown_rx: broadcast::Receiver<()>,
        sasl_server: Option<Arc<SaslServer>>,
        context: ConnectionContext,
        broker_pool: Arc<BrokerPool>,
        registry: Arc<TopicRemapperRegistry>,
    ) -> Self {
        // Create handlers
        let api_versions_handler = ApiVersionsHandler::new(Arc::clone(&broker_pool));
        let metadata_handler =
            MetadataHandler::new(Arc::clone(&registry), Arc::clone(&broker_pool), &config.listen);
        let produce_handler = ProduceHandler::new(Arc::clone(&registry), Arc::clone(&broker_pool));
        let fetch_handler = FetchHandler::new(Arc::clone(&registry), Arc::clone(&broker_pool));
        let passthrough_handler = PassthroughHandler::new(broker_pool);

        Self {
            config,
            shutdown_rx,
            sasl_server,
            context,
            principal_upgraded: false,
            api_versions_handler,
            metadata_handler,
            produce_handler,
            fetch_handler,
            passthrough_handler,
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
                Err(ProxyError::Auth(
                    crate::error::AuthError::AuthenticationFailed(
                        "SASL authentication required".to_string(),
                    ),
                ))
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
    /// Forwards request to broker and filters response to supported APIs.
    async fn handle_api_versions(&self, frame: KafkaFrame) -> Result<ResponseFrame> {
        let body = self.api_versions_handler.handle(&frame).await?;
        Ok(ResponseFrame {
            correlation_id: frame.correlation_id,
            body,
        })
    }

    /// Handle Metadata request.
    ///
    /// Expands physical partitions to virtual partitions and rewrites broker
    /// addresses to point to the proxy.
    async fn handle_metadata(&self, frame: KafkaFrame) -> Result<ResponseFrame> {
        let body = self.metadata_handler.handle(&frame).await?;
        Ok(ResponseFrame {
            correlation_id: frame.correlation_id,
            body,
        })
    }

    /// Handle Produce request.
    ///
    /// Maps virtual partitions to physical partitions in the request and
    /// translates offsets in the response.
    async fn handle_produce(&self, frame: KafkaFrame) -> Result<ResponseFrame> {
        let body = self.produce_handler.handle(&frame).await?;
        Ok(ResponseFrame {
            correlation_id: frame.correlation_id,
            body,
        })
    }

    /// Handle Fetch request.
    ///
    /// Maps virtual partitions to physical partitions in the request and
    /// translates offsets in the response.
    async fn handle_fetch(&self, frame: KafkaFrame) -> Result<ResponseFrame> {
        let body = self.fetch_handler.handle(&frame).await?;
        Ok(ResponseFrame {
            correlation_id: frame.correlation_id,
            body,
        })
    }

    /// Handle passthrough request (forward to broker unchanged).
    ///
    /// Used for group coordination APIs (JoinGroup, SyncGroup, Heartbeat,
    /// LeaveGroup, FindCoordinator) and offset APIs (OffsetCommit, OffsetFetch,
    /// ListOffsets) which don't require partition remapping.
    async fn handle_passthrough(&self, frame: KafkaFrame) -> Result<ResponseFrame> {
        let body = self.passthrough_handler.handle(&frame).await?;
        Ok(ResponseFrame {
            correlation_id: frame.correlation_id,
            body,
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

    fn test_broker_pool(config: &Arc<ProxyConfig>) -> Arc<BrokerPool> {
        Arc::new(BrokerPool::new(config.kafka.clone()))
    }

    fn test_registry(config: &Arc<ProxyConfig>) -> Arc<TopicRemapperRegistry> {
        Arc::new(TopicRemapperRegistry::new(&config.mapping))
    }

    fn test_context() -> ConnectionContext {
        let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        ConnectionContext::anonymous(addr, "test-conn".to_string())
    }

    #[tokio::test]
    async fn test_handle_api_versions() {
        let (tx, rx) = broadcast::channel(1);
        let config = test_config();
        let broker_pool = test_broker_pool(&config);
        let registry = test_registry(&config);
        let handler = ConnectionHandler::new(config, rx, None, test_context(), broker_pool, registry);

        let frame = KafkaFrame {
            api_key: ApiKey::ApiVersions,
            api_version: 3,
            correlation_id: 12345,
            bytes: BytesMut::new(),
        };

        // This will attempt to connect to broker and fail, falling back to local response
        let response = handler.handle_api_versions(frame).await.unwrap();
        assert_eq!(response.correlation_id, 12345);
        // Should have some response body with supported APIs
        assert!(!response.body.is_empty());

        drop(tx);
    }

    #[tokio::test]
    async fn test_unsupported_api() {
        let (tx, rx) = broadcast::channel(1);
        let config = test_config();
        let broker_pool = test_broker_pool(&config);
        let registry = test_registry(&config);
        let handler = ConnectionHandler::new(config, rx, None, test_context(), broker_pool, registry);

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
        let config = test_config();
        let broker_pool = test_broker_pool(&config);
        let registry = test_registry(&config);
        let handler = ConnectionHandler::new(config, rx, None, context, broker_pool, registry);

        assert_eq!(handler.principal().name, "test-user");
        assert_eq!(handler.principal().auth_method, AuthMethod::Ssl);
        assert_eq!(handler.context().connection_id(), "conn-test");

        drop(tx);
    }
}
