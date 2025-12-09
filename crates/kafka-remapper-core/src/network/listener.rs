//! TCP listener for accepting Kafka client connections.
//!
//! The listener accepts connections and spawns a task for each one,
//! delegating to the connection handler for request processing.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tracing::{debug, error, info, instrument, warn};

use crate::config::ProxyConfig;
use crate::error::{ProxyError, Result};

use super::connection::ConnectionHandler;

/// TCP listener that accepts client connections.
pub struct ProxyListener {
    config: Arc<ProxyConfig>,
    shutdown_tx: broadcast::Sender<()>,
    active_connections: Arc<AtomicUsize>,
}

impl ProxyListener {
    /// Create a new proxy listener.
    #[must_use]
    pub fn new(config: ProxyConfig) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        Self {
            config: Arc::new(config),
            shutdown_tx,
            active_connections: Arc::new(AtomicUsize::new(0)),
        }
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
                            debug!(peer = %addr, active = current + 1, "accepted connection");

                            let config = Arc::clone(&self.config);
                            let shutdown_rx = self.shutdown_tx.subscribe();
                            let active_connections = Arc::clone(&self.active_connections);

                            tokio::spawn(async move {
                                let handler = ConnectionHandler::new(config, shutdown_rx);
                                if let Err(e) = handler.handle(socket).await {
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
    use super::*;
    use crate::config::{KafkaConfig, ListenConfig, LoggingConfig, MappingConfig, MetricsConfig};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time::timeout;

    fn test_config(port: u16) -> ProxyConfig {
        ProxyConfig {
            listen: ListenConfig {
                address: format!("127.0.0.1:{port}"),
                advertised_address: None,
                max_connections: 10,
            },
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
        }
    }

    #[tokio::test]
    async fn test_listener_accepts_connection() {
        let config = test_config(19092);
        let listener = ProxyListener::new(config);
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
        let listener = ProxyListener::new(config);
        let shutdown_handle = listener.shutdown_handle();

        let listener_task = tokio::spawn(async move { listener.run().await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Send shutdown
        let _ = shutdown_handle.send(());

        // Should complete quickly
        let result = timeout(Duration::from_secs(1), listener_task).await;
        assert!(result.is_ok());
    }
}
