//! TCP listener for accepting Kafka client connections.
//!
//! The listener accepts connections and spawns a task for each one,
//! delegating to the connection handler for request processing.
//!
//! Supports optional TLS termination and SASL authentication for client
//! connections based on the security configuration.
//!
//! # Principal Extraction
//!
//! When mTLS is enabled, the listener extracts the client's principal from
//! the TLS certificate using the configured mapping rules. The principal
//! is stored in the `ConnectionContext` and passed to the handler.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tracing::{debug, error, info, instrument, warn};

use crate::auth::{
    AuthenticationContext, DefaultPrincipalBuilder, Principal, PrincipalBuilder, SaslServer,
    SslAuthenticationContext,
};
use crate::config::{ProxyConfig, SecurityProtocol};
use crate::error::{ProxyError, Result};
use crate::tls::TlsServerAcceptor;

use super::client_stream::ClientStream;
use super::connection::ConnectionHandler;
use super::context::ConnectionContext;

/// TCP listener that accepts client connections.
///
/// Supports optional TLS termination and SASL authentication based on
/// the security configuration.
///
/// # Principal Extraction
///
/// The listener uses a `DefaultPrincipalBuilder` to extract principals from
/// TLS certificates. The builder is configured with mapping rules from the
/// TLS configuration.
pub struct ProxyListener {
    config: Arc<ProxyConfig>,
    shutdown_tx: broadcast::Sender<()>,
    active_connections: Arc<AtomicUsize>,
    /// Optional TLS acceptor for client connections.
    tls_acceptor: Option<Arc<TlsServerAcceptor>>,
    /// Optional SASL server for client authentication.
    sasl_server: Option<Arc<SaslServer>>,
    /// Principal builder for extracting identity from certificates.
    principal_builder: Arc<DefaultPrincipalBuilder>,
    /// Security protocol for client connections.
    security_protocol: SecurityProtocol,
}

impl ProxyListener {
    /// Create a new proxy listener.
    ///
    /// # Errors
    ///
    /// Returns an error if TLS is configured but the TLS acceptor cannot be created,
    /// or if SASL is configured but the SASL server cannot be created.
    pub fn new(config: ProxyConfig) -> Result<Self> {
        let (shutdown_tx, _) = broadcast::channel(1);

        // Determine security protocol
        let security_protocol = config
            .listen
            .security
            .as_ref()
            .map(|s| s.protocol)
            .unwrap_or(SecurityProtocol::Plaintext);

        // Create TLS acceptor if TLS is configured
        let tls_acceptor = if let Some(security) = &config.listen.security {
            if security.protocol.requires_tls() {
                let tls_config = security.tls.as_ref().ok_or_else(|| {
                    ProxyError::Tls(crate::error::TlsError::Config(
                        "TLS configuration required for SSL/SASL_SSL protocol".to_string(),
                    ))
                })?;
                let acceptor = TlsServerAcceptor::new(tls_config)?;
                info!("TLS enabled for client connections");
                Some(Arc::new(acceptor))
            } else {
                None
            }
        } else {
            None
        };

        // Create SASL server if SASL is configured
        let sasl_server = if let Some(security) = &config.listen.security {
            if security.protocol.requires_sasl() {
                let sasl_config = security.sasl.as_ref().ok_or_else(|| {
                    ProxyError::Auth(crate::error::AuthError::MissingConfig(
                        "SASL configuration required for SASL_PLAINTEXT/SASL_SSL protocol"
                            .to_string(),
                    ))
                })?;
                let server = SaslServer::new(sasl_config)?;
                info!(
                    mechanisms = ?server.enabled_mechanisms(),
                    "SASL authentication enabled for client connections"
                );
                Some(Arc::new(server))
            } else {
                None
            }
        } else {
            None
        };

        // Create principal builder from TLS config mapping rules
        let principal_builder = if let Some(security) = &config.listen.security {
            if let Some(tls) = &security.tls {
                DefaultPrincipalBuilder::with_ssl_rules(&tls.principal.mapping_rules).map_err(
                    |e| {
                        ProxyError::Tls(crate::error::TlsError::Config(format!(
                            "Invalid principal mapping rules: {e}"
                        )))
                    },
                )?
            } else {
                DefaultPrincipalBuilder::new()
            }
        } else {
            DefaultPrincipalBuilder::new()
        };

        if security_protocol.requires_tls() {
            info!(
                rules = config
                    .listen
                    .security
                    .as_ref()
                    .and_then(|s| s.tls.as_ref())
                    .map(|t| t.principal.mapping_rules.as_str())
                    .unwrap_or("DEFAULT"),
                "Principal extraction configured"
            );
        }

        Ok(Self {
            config: Arc::new(config),
            shutdown_tx,
            active_connections: Arc::new(AtomicUsize::new(0)),
            tls_acceptor,
            sasl_server,
            principal_builder: Arc::new(principal_builder),
            security_protocol,
        })
    }

    /// Check if TLS is enabled for client connections.
    #[must_use]
    pub fn is_tls_enabled(&self) -> bool {
        self.tls_acceptor.is_some()
    }

    /// Check if SASL authentication is enabled for client connections.
    #[must_use]
    pub fn is_sasl_enabled(&self) -> bool {
        self.sasl_server.is_some()
    }

    /// Get a shutdown handle to signal the listener to stop.
    #[must_use]
    pub fn shutdown_handle(&self) -> broadcast::Sender<()> {
        self.shutdown_tx.clone()
    }

    /// Get the current number of active connections.
    #[must_use]
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }

    /// Run the listener, accepting connections until shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if binding to the listen address fails.
    #[instrument(skip(self), fields(address = %self.config.listen.address))]
    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.config.listen.address).await?;
        info!(address = %self.config.listen.address, "proxy listening");

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        // Connection counter for generating unique IDs
        let mut connection_counter: u64 = 0;

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((socket, addr)) => {
                            let current = self.active_connections.load(Ordering::Relaxed);

                            // Check connection limit
                            if current >= self.config.listen.max_connections {
                                warn!(
                                    peer = %addr,
                                    active = current,
                                    max = self.config.listen.max_connections,
                                    "connection rejected: limit reached"
                                );
                                // Socket will be dropped, closing the connection
                                continue;
                            }

                            self.active_connections.fetch_add(1, Ordering::Relaxed);
                            connection_counter += 1;
                            let connection_id = format!("conn-{connection_counter}");
                            debug!(peer = %addr, connection_id = %connection_id, active = current + 1, "accepted connection");

                            let config = Arc::clone(&self.config);
                            let shutdown_rx = self.shutdown_tx.subscribe();
                            let active_connections = Arc::clone(&self.active_connections);
                            let tls_acceptor = self.tls_acceptor.clone();
                            let sasl_server = self.sasl_server.clone();
                            let principal_builder = Arc::clone(&self.principal_builder);
                            let security_protocol = self.security_protocol;

                            tokio::spawn(async move {
                                // Perform TLS handshake if TLS is configured
                                let stream: ClientStream = if let Some(acceptor) = tls_acceptor {
                                    debug!(peer = %addr, "performing TLS handshake");
                                    match acceptor.accept(socket).await {
                                        Ok(tls_stream) => {
                                            debug!(peer = %addr, "TLS handshake successful");
                                            ClientStream::tls(tls_stream)
                                        }
                                        Err(e) => {
                                            warn!(peer = %addr, error = %e, "TLS handshake failed");
                                            active_connections.fetch_sub(1, Ordering::Relaxed);
                                            return;
                                        }
                                    }
                                } else {
                                    ClientStream::plain(socket)
                                };

                                // Extract principal from TLS certificates (for mTLS)
                                let initial_principal = if let Some(certs) = stream.peer_certificates() {
                                    if certs.is_empty() {
                                        debug!(peer = %addr, "No client certificate provided");
                                        Principal::anonymous()
                                    } else {
                                        let ssl_context = SslAuthenticationContext {
                                            peer_certificates: certs,
                                            client_address: addr,
                                        };
                                        match principal_builder.build(AuthenticationContext::Ssl(ssl_context)) {
                                            Ok(principal) => {
                                                info!(
                                                    peer = %addr,
                                                    principal = %principal,
                                                    "Extracted principal from client certificate"
                                                );
                                                principal
                                            }
                                            Err(e) => {
                                                warn!(
                                                    peer = %addr,
                                                    error = %e,
                                                    "Failed to extract principal from certificate"
                                                );
                                                Principal::anonymous()
                                            }
                                        }
                                    }
                                } else {
                                    Principal::anonymous()
                                };

                                // Create connection context
                                let context = ConnectionContext::new(
                                    initial_principal,
                                    addr,
                                    connection_id,
                                    security_protocol,
                                );

                                // Create connection handler with context
                                let handler = ConnectionHandler::new(
                                    config,
                                    shutdown_rx,
                                    sasl_server,
                                    context,
                                );

                                if let Err(e) = handler.handle(stream).await {
                                    match &e {
                                        ProxyError::Shutdown => {
                                            debug!(peer = %addr, "connection closed: shutdown");
                                        }
                                        ProxyError::Connection(io_err)
                                            if io_err.kind() == std::io::ErrorKind::UnexpectedEof =>
                                        {
                                            debug!(peer = %addr, "client disconnected");
                                        }
                                        _ => {
                                            error!(peer = %addr, error = %e, "connection error");
                                        }
                                    }
                                }
                                active_connections.fetch_sub(1, Ordering::Relaxed);
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "accept error");
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("shutdown signal received");
                    break;
                }
            }
        }

        // Wait for active connections to drain (with timeout)
        let active = self.active_connections.load(Ordering::Relaxed);
        if active > 0 {
            info!(active, "waiting for connections to close");
            // In production, we'd wait with a timeout here
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use super::*;
    use crate::config::{
        KafkaConfig, ListenConfig, LoggingConfig, MappingConfig, MetricsConfig, SecurityProtocol,
    };
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;
    use tokio::time::timeout;

    fn test_config(port: u16) -> ProxyConfig {
        ProxyConfig {
            listen: ListenConfig {
                address: format!("127.0.0.1:{port}"),
                advertised_address: None,
                max_connections: 10,
                security: None,
            },
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
        }
    }

    #[tokio::test]
    async fn test_listener_accepts_connection() {
        let config = test_config(19092);
        let listener = ProxyListener::new(config).unwrap();
        let shutdown_handle = listener.shutdown_handle();

        // Spawn listener
        let listener_task = tokio::spawn(async move { listener.run().await });

        // Give listener time to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Connect
        let mut client = TcpStream::connect("127.0.0.1:19092").await.unwrap();

        // Send some bytes (will fail to parse as valid Kafka, but connection works)
        client.write_all(&[0, 0, 0, 4, 0, 0, 0, 0]).await.unwrap();

        // Shutdown
        let _ = shutdown_handle.send(());
        let _ = timeout(Duration::from_secs(1), listener_task).await;
    }

    #[tokio::test]
    async fn test_listener_shutdown() {
        let config = test_config(19093);
        let listener = ProxyListener::new(config).unwrap();
        let shutdown_handle = listener.shutdown_handle();

        let listener_task = tokio::spawn(async move { listener.run().await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Send shutdown
        let _ = shutdown_handle.send(());

        // Should complete quickly
        let result = timeout(Duration::from_secs(1), listener_task).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_listener_tls_not_enabled_by_default() {
        let config = test_config(19094);
        let listener = ProxyListener::new(config).unwrap();
        assert!(!listener.is_tls_enabled());
    }
}
