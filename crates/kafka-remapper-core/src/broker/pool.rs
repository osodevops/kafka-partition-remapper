//! Broker connection pool.
//!
//! Manages connections to all known Kafka brokers with automatic discovery
//! from metadata responses.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use tracing::{debug, info, warn};

use crate::config::KafkaConfig;
use crate::error::{ProxyError, Result};

use super::connection::BrokerConnection;

/// Manages connections to Kafka brokers.
pub struct BrokerPool {
    config: KafkaConfig,
    /// Connections indexed by broker ID.
    connections: DashMap<i32, Arc<BrokerConnection>>,
    /// Bootstrap connection (broker_id = -1).
    bootstrap: Arc<tokio::sync::RwLock<Option<Arc<BrokerConnection>>>>,
}

impl BrokerPool {
    /// Create a new broker pool.
    #[must_use]
    pub fn new(config: KafkaConfig) -> Self {
        Self {
            config,
            connections: DashMap::new(),
            bootstrap: Arc::new(tokio::sync::RwLock::new(None)),
        }
    }

    /// Connect to the Kafka cluster using bootstrap servers.
    ///
    /// Tries each bootstrap server in order until one succeeds.
    /// Applies TLS and SASL configuration based on the security protocol.
    ///
    /// # Errors
    ///
    /// Returns an error if all bootstrap servers fail.
    pub async fn connect(&self) -> Result<()> {
        let connect_timeout = Duration::from_millis(self.config.connection_timeout_ms);
        let request_timeout = Duration::from_millis(self.config.request_timeout_ms);

        for server in &self.config.bootstrap_servers {
            let conn = self.create_connection(-1, server.clone(), connect_timeout, request_timeout)?;

            match conn.connect().await {
                Ok(()) => {
                    info!(
                        server = %server,
                        protocol = ?self.config.security_protocol,
                        "connected to bootstrap server"
                    );
                    *self.bootstrap.write().await = Some(Arc::new(conn));
                    return Ok(());
                }
                Err(e) => {
                    warn!(server = %server, error = %e, "failed to connect to bootstrap server");
                }
            }
        }

        Err(ProxyError::NoBrokersAvailable)
    }

    /// Create a broker connection with the appropriate security configuration.
    fn create_connection(
        &self,
        broker_id: i32,
        address: String,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<BrokerConnection> {
        // Use security-aware connection if TLS or SASL is configured
        if self.config.security_protocol != crate::config::SecurityProtocol::Plaintext {
            BrokerConnection::with_security(
                broker_id,
                address,
                self.config.security_protocol,
                self.config.tls.as_ref(),
                self.config.sasl.clone(),
                connect_timeout,
                request_timeout,
            )
        } else {
            // Use simple connection for plaintext
            Ok(BrokerConnection::with_timeouts(
                broker_id,
                address,
                connect_timeout,
                request_timeout,
            ))
        }
    }

    /// Get a connection to a specific broker by ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker is not known.
    pub async fn get_broker(&self, broker_id: i32) -> Result<Arc<BrokerConnection>> {
        if let Some(conn) = self.connections.get(&broker_id) {
            return Ok(Arc::clone(conn.value()));
        }

        Err(ProxyError::BrokerUnavailable {
            broker_id,
            message: "broker not in pool".to_string(),
        })
    }

    /// Get any available connection (for metadata requests).
    ///
    /// Prefers the bootstrap connection, falls back to any known broker.
    ///
    /// # Errors
    ///
    /// Returns an error if no connections are available.
    pub async fn get_any(&self) -> Result<Arc<BrokerConnection>> {
        // Try bootstrap first
        if let Some(conn) = self.bootstrap.read().await.as_ref() {
            if conn.is_connected().await {
                return Ok(Arc::clone(conn));
            }
        }

        // Try any known broker
        for entry in self.connections.iter() {
            if entry.value().is_connected().await {
                return Ok(Arc::clone(entry.value()));
            }
        }

        Err(ProxyError::NoBrokersAvailable)
    }

    /// Send a request to any available broker.
    ///
    /// # Errors
    ///
    /// Returns an error if no brokers are available or the request fails.
    pub async fn send_request(&self, request_bytes: &[u8]) -> Result<Bytes> {
        let conn = self.get_any().await?;
        conn.send_request(request_bytes).await
    }

    /// Send a request to a specific broker.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker is not available or the request fails.
    pub async fn send_request_to_broker(
        &self,
        broker_id: i32,
        request_bytes: &[u8],
    ) -> Result<Bytes> {
        let conn = self.get_broker(broker_id).await?;
        conn.send_request(request_bytes).await
    }

    /// Update the pool with discovered brokers.
    ///
    /// Call this after receiving a Metadata response to register all known brokers.
    /// Applies the same TLS and SASL configuration as the bootstrap connection.
    pub async fn update_brokers(&self, brokers: Vec<BrokerInfo>) {
        let connect_timeout = Duration::from_millis(self.config.connection_timeout_ms);
        let request_timeout = Duration::from_millis(self.config.request_timeout_ms);

        for broker in brokers {
            // Skip if already connected
            if self.connections.contains_key(&broker.node_id) {
                continue;
            }

            let address = format!("{}:{}", broker.host, broker.port);
            let conn = match self.create_connection(
                broker.node_id,
                address.clone(),
                connect_timeout,
                request_timeout,
            ) {
                Ok(conn) => conn,
                Err(e) => {
                    warn!(
                        broker_id = broker.node_id,
                        address = %address,
                        error = %e,
                        "failed to create broker connection"
                    );
                    continue;
                }
            };

            match conn.connect().await {
                Ok(()) => {
                    info!(
                        broker_id = broker.node_id,
                        address = %address,
                        protocol = ?self.config.security_protocol,
                        "connected to broker"
                    );
                    self.connections.insert(broker.node_id, Arc::new(conn));
                }
                Err(e) => {
                    warn!(
                        broker_id = broker.node_id,
                        address = %address,
                        error = %e,
                        "failed to connect to broker"
                    );
                }
            }
        }
    }

    /// Get the number of connected brokers (excluding bootstrap).
    #[must_use]
    pub fn broker_count(&self) -> usize {
        self.connections.len()
    }

    /// Check if the pool has any connections.
    pub async fn is_connected(&self) -> bool {
        if self.bootstrap.read().await.is_some() {
            return true;
        }
        !self.connections.is_empty()
    }

    /// Disconnect all brokers.
    pub async fn disconnect_all(&self) {
        // Disconnect bootstrap
        if let Some(conn) = self.bootstrap.write().await.take() {
            conn.disconnect().await;
        }

        // Disconnect all brokers
        for entry in self.connections.iter() {
            entry.value().disconnect().await;
        }
        self.connections.clear();
    }
}

/// Information about a Kafka broker.
#[derive(Debug, Clone)]
pub struct BrokerInfo {
    /// The broker node ID.
    pub node_id: i32,
    /// The broker hostname.
    pub host: String,
    /// The broker port.
    pub port: i32,
}

impl BrokerInfo {
    /// Create a new broker info.
    #[must_use]
    pub fn new(node_id: i32, host: String, port: i32) -> Self {
        Self { node_id, host, port }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SecurityProtocol;

    fn test_config() -> KafkaConfig {
        KafkaConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            connection_timeout_ms: 100,
            request_timeout_ms: 1000,
            metadata_refresh_interval_secs: 0,
            security_protocol: SecurityProtocol::Plaintext,
            tls: None,
            sasl: None,
        }
    }

    #[tokio::test]
    async fn test_pool_creation() {
        let pool = BrokerPool::new(test_config());
        assert_eq!(pool.broker_count(), 0);
        assert!(!pool.is_connected().await);
    }

    #[tokio::test]
    async fn test_get_broker_not_found() {
        let pool = BrokerPool::new(test_config());
        let result = pool.get_broker(1).await;
        assert!(matches!(result, Err(ProxyError::BrokerUnavailable { .. })));
    }

    #[tokio::test]
    async fn test_get_any_no_connections() {
        let pool = BrokerPool::new(test_config());
        let result = pool.get_any().await;
        assert!(matches!(result, Err(ProxyError::NoBrokersAvailable)));
    }

    #[tokio::test]
    async fn test_connect_to_invalid_bootstrap() {
        let config = KafkaConfig {
            bootstrap_servers: vec!["127.0.0.1:59999".to_string()],
            connection_timeout_ms: 100,
            request_timeout_ms: 1000,
            metadata_refresh_interval_secs: 0,
            security_protocol: SecurityProtocol::Plaintext,
            tls: None,
            sasl: None,
        };
        let pool = BrokerPool::new(config);
        let result = pool.connect().await;
        assert!(matches!(result, Err(ProxyError::NoBrokersAvailable)));
    }

    #[test]
    fn test_broker_info() {
        let info = BrokerInfo::new(1, "broker1.example.com".to_string(), 9092);
        assert_eq!(info.node_id, 1);
        assert_eq!(info.host, "broker1.example.com");
        assert_eq!(info.port, 9092);
    }
}
