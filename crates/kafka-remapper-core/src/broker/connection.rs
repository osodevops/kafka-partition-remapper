//! Single Kafka broker connection.
//!
//! Manages a TCP connection to a single Kafka broker with correlation ID tracking
//! for request/response matching. Supports both plain TCP and TLS connections.

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, RequestHeader, SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest,
    SaslHandshakeResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::{debug, info, instrument, warn};

use crate::config::{BrokerSaslConfig, BrokerTlsConfig, SaslMechanism, SecurityProtocol};
use crate::error::{ProxyError, Result};
use crate::tls::TlsConnector;

use super::stream::BrokerStream;

/// Default connection timeout.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Default request timeout.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// A connection to a single Kafka broker.
///
/// Supports plain TCP, TLS, and SASL authentication.
pub struct BrokerConnection {
    broker_id: i32,
    address: String,
    stream: Mutex<Option<BrokerStream>>,
    correlation_id: AtomicI32,
    connect_timeout: Duration,
    request_timeout: Duration,
    security_protocol: SecurityProtocol,
    tls_connector: Option<Arc<TlsConnector>>,
    sasl_config: Option<BrokerSaslConfig>,
}

impl BrokerConnection {
    /// Create a new broker connection (not yet connected).
    #[must_use]
    pub fn new(broker_id: i32, address: String) -> Self {
        Self {
            broker_id,
            address,
            stream: Mutex::new(None),
            correlation_id: AtomicI32::new(0),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            security_protocol: SecurityProtocol::Plaintext,
            tls_connector: None,
            sasl_config: None,
        }
    }

    /// Create a new broker connection with custom timeouts.
    #[must_use]
    pub fn with_timeouts(
        broker_id: i32,
        address: String,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Self {
        Self {
            broker_id,
            address,
            stream: Mutex::new(None),
            correlation_id: AtomicI32::new(0),
            connect_timeout,
            request_timeout,
            security_protocol: SecurityProtocol::Plaintext,
            tls_connector: None,
            sasl_config: None,
        }
    }

    /// Create a new broker connection with TLS and SASL configuration.
    ///
    /// # Arguments
    ///
    /// * `broker_id` - The Kafka broker ID
    /// * `address` - The broker address (host:port)
    /// * `security_protocol` - The security protocol to use
    /// * `tls_config` - Optional TLS configuration
    /// * `sasl_config` - Optional SASL configuration
    /// * `connect_timeout` - Connection timeout
    /// * `request_timeout` - Request timeout
    ///
    /// # Errors
    ///
    /// Returns an error if TLS configuration is required but invalid.
    pub fn with_security(
        broker_id: i32,
        address: String,
        security_protocol: SecurityProtocol,
        tls_config: Option<&BrokerTlsConfig>,
        sasl_config: Option<BrokerSaslConfig>,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self> {
        // Create TLS connector if needed
        let tls_connector = if security_protocol.requires_tls() {
            let config = tls_config.cloned().unwrap_or_default();
            let connector = TlsConnector::new(&config)
                .map_err(|e| ProxyError::Connection(std::io::Error::other(e.to_string())))?;
            Some(Arc::new(connector))
        } else {
            None
        };

        Ok(Self {
            broker_id,
            address,
            stream: Mutex::new(None),
            correlation_id: AtomicI32::new(0),
            connect_timeout,
            request_timeout,
            security_protocol,
            tls_connector,
            sasl_config,
        })
    }

    /// Get the broker ID.
    #[must_use]
    pub fn broker_id(&self) -> i32 {
        self.broker_id
    }

    /// Get the broker address.
    #[must_use]
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Check if the connection is established.
    pub async fn is_connected(&self) -> bool {
        self.stream.lock().await.is_some()
    }

    /// Connect to the broker.
    ///
    /// Establishes a TCP connection and optionally performs TLS handshake
    /// and SASL authentication based on the security protocol.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails, times out, or authentication fails.
    #[instrument(skip(self), fields(broker_id = self.broker_id, address = %self.address, protocol = ?self.security_protocol))]
    pub async fn connect(&self) -> Result<()> {
        // Step 1: Establish TCP connection
        let tcp_stream =
            match timeout(self.connect_timeout, TcpStream::connect(&self.address)).await {
                Ok(Ok(stream)) => stream,
                Ok(Err(e)) => {
                    warn!(error = %e, "failed to connect to broker");
                    return Err(ProxyError::BrokerUnavailable {
                        broker_id: self.broker_id,
                        message: e.to_string(),
                    });
                }
                Err(_) => {
                    warn!("connection timeout");
                    return Err(ProxyError::BrokerUnavailable {
                        broker_id: self.broker_id,
                        message: "connection timeout".to_string(),
                    });
                }
            };

        debug!("TCP connection established");

        // Step 2: Perform TLS handshake if required
        let stream = if self.security_protocol.requires_tls() {
            let connector =
                self.tls_connector
                    .as_ref()
                    .ok_or_else(|| ProxyError::BrokerUnavailable {
                        broker_id: self.broker_id,
                        message: "TLS required but no connector configured".to_string(),
                    })?;

            // Extract hostname from address for SNI
            let server_name = self.address.split(':').next().unwrap_or(&self.address);

            debug!(server_name, "performing TLS handshake");

            let tls_stream = connector
                .connect(server_name, tcp_stream)
                .await
                .map_err(|e| ProxyError::BrokerUnavailable {
                    broker_id: self.broker_id,
                    message: format!("TLS handshake failed: {e}"),
                })?;

            debug!("TLS handshake completed");
            BrokerStream::tls(tls_stream)
        } else {
            BrokerStream::plain(tcp_stream)
        };

        // Step 3: Store the stream
        *self.stream.lock().await = Some(stream);

        // Step 4: Perform SASL authentication if required
        if self.security_protocol.requires_sasl() {
            self.perform_sasl_handshake().await?;
        }

        debug!("connected to broker");
        Ok(())
    }

    /// Perform SASL authentication handshake.
    ///
    /// This sends SaslHandshake and SaslAuthenticate requests to the broker.
    /// Supports SASL/PLAIN mechanism.
    async fn perform_sasl_handshake(&self) -> Result<()> {
        let sasl_config =
            self.sasl_config
                .as_ref()
                .ok_or_else(|| ProxyError::BrokerUnavailable {
                    broker_id: self.broker_id,
                    message: "SASL required but no configuration provided".to_string(),
                })?;

        let mechanism_name = match sasl_config.mechanism {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
        };

        debug!(mechanism = mechanism_name, "performing SASL authentication");

        // Step 1: Send SaslHandshake request
        let handshake_response = self.send_sasl_handshake(mechanism_name).await?;

        // Check for errors in handshake response
        if handshake_response.error_code != 0 {
            return Err(ProxyError::BrokerUnavailable {
                broker_id: self.broker_id,
                message: format!(
                    "SASL handshake failed with error code: {}",
                    handshake_response.error_code
                ),
            });
        }

        // Verify the mechanism is supported
        let supported_mechanisms: Vec<String> = handshake_response
            .mechanisms
            .iter()
            .map(|m| m.to_string())
            .collect();

        if !supported_mechanisms.iter().any(|m| m == mechanism_name) {
            return Err(ProxyError::BrokerUnavailable {
                broker_id: self.broker_id,
                message: format!(
                    "SASL mechanism '{}' not supported by broker. Supported: {:?}",
                    mechanism_name, supported_mechanisms
                ),
            });
        }

        debug!(
            supported_mechanisms = ?supported_mechanisms,
            "SASL handshake successful"
        );

        // Step 2: Send SaslAuthenticate request with credentials
        match sasl_config.mechanism {
            SaslMechanism::Plain => {
                self.authenticate_plain(&sasl_config.username, &sasl_config.password)
                    .await?;
            }
            SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => {
                // SCRAM requires multiple round-trips, not yet implemented
                return Err(ProxyError::BrokerUnavailable {
                    broker_id: self.broker_id,
                    message: format!("SASL mechanism '{}' not yet implemented", mechanism_name),
                });
            }
        }

        info!("SASL authentication completed successfully");
        Ok(())
    }

    /// Send a SaslHandshake request and receive the response.
    async fn send_sasl_handshake(&self, mechanism: &str) -> Result<SaslHandshakeResponse> {
        let correlation_id = self.next_correlation_id();
        let api_version = 1i16; // Use version 1 for modern Kafka
        let header_version = SaslHandshakeRequest::header_version(api_version);

        // Build the request header
        let mut header = RequestHeader::default();
        header.request_api_key = ApiKey::SaslHandshake as i16;
        header.request_api_version = api_version;
        header.correlation_id = correlation_id;
        header.client_id = Some(StrBytes::from_static_str("kafka-remapper-proxy"));

        // Build the request body
        let mut request = SaslHandshakeRequest::default();
        request.mechanism = StrBytes::from_string(mechanism.to_string());

        // Encode the request with the correct header version
        let mut buf = BytesMut::new();
        header
            .encode(&mut buf, header_version)
            .map_err(|e| ProxyError::ProtocolEncode {
                message: format!("failed to encode SASL handshake header: {e}"),
            })?;
        request
            .encode(&mut buf, api_version)
            .map_err(|e| ProxyError::ProtocolEncode {
                message: format!("failed to encode SASL handshake request: {e}"),
            })?;

        // Send the request and receive response
        let response_bytes = self.send_raw_bytes(&buf).await?;

        // Skip correlation ID (first 4 bytes) and decode response
        let mut response_data = Bytes::copy_from_slice(&response_bytes[4..]);
        let response =
            SaslHandshakeResponse::decode(&mut response_data, api_version).map_err(|e| {
                ProxyError::ProtocolDecode {
                    message: format!("failed to decode SASL handshake response: {e}"),
                }
            })?;

        Ok(response)
    }

    /// Authenticate using SASL/PLAIN mechanism.
    ///
    /// SASL/PLAIN auth bytes format: \0username\0password
    async fn authenticate_plain(&self, username: &str, password: &str) -> Result<()> {
        let correlation_id = self.next_correlation_id();
        let api_version = 2i16; // Use version 2 for modern Kafka
        let header_version = SaslAuthenticateRequest::header_version(api_version);

        // Build the request header
        let mut header = RequestHeader::default();
        header.request_api_key = ApiKey::SaslAuthenticate as i16;
        header.request_api_version = api_version;
        header.correlation_id = correlation_id;
        header.client_id = Some(StrBytes::from_static_str("kafka-remapper-proxy"));

        // Build SASL/PLAIN auth bytes: \0username\0password
        let auth_bytes = format!("\0{}\0{}", username, password);

        // Build the request body
        let mut request = SaslAuthenticateRequest::default();
        request.auth_bytes = Bytes::from(auth_bytes);

        // Encode the request with the correct header version
        let mut buf = BytesMut::new();
        header
            .encode(&mut buf, header_version)
            .map_err(|e| ProxyError::ProtocolEncode {
                message: format!("failed to encode SASL authenticate header: {e}"),
            })?;
        request
            .encode(&mut buf, api_version)
            .map_err(|e| ProxyError::ProtocolEncode {
                message: format!("failed to encode SASL authenticate request: {e}"),
            })?;

        // Send the request and receive response
        let response_bytes = self.send_raw_bytes(&buf).await?;

        // Skip correlation ID (first 4 bytes) and decode response
        // Note: Response header version follows the same rules
        let mut response_data = Bytes::copy_from_slice(&response_bytes[4..]);
        let response =
            SaslAuthenticateResponse::decode(&mut response_data, api_version).map_err(|e| {
                ProxyError::ProtocolDecode {
                    message: format!("failed to decode SASL authenticate response: {e}"),
                }
            })?;

        // Check for authentication errors
        if response.error_code != 0 {
            let error_message = response
                .error_message
                .map(|s| s.to_string())
                .unwrap_or_else(|| "unknown error".to_string());
            return Err(ProxyError::BrokerUnavailable {
                broker_id: self.broker_id,
                message: format!(
                    "SASL authentication failed (error code {}): {}",
                    response.error_code, error_message
                ),
            });
        }

        debug!("SASL/PLAIN authentication successful");
        Ok(())
    }

    /// Send raw bytes to the broker and receive the response.
    ///
    /// This is a low-level method used during SASL handshake before
    /// the connection is fully established.
    async fn send_raw_bytes(&self, request_bytes: &[u8]) -> Result<Vec<u8>> {
        let mut guard = self.stream.lock().await;
        let stream = guard.as_mut().ok_or(ProxyError::BrokerUnavailable {
            broker_id: self.broker_id,
            message: "not connected".to_string(),
        })?;

        // Write request with length prefix
        let mut write_buf = BytesMut::with_capacity(4 + request_bytes.len());
        write_buf.put_u32(request_bytes.len() as u32);
        write_buf.extend_from_slice(request_bytes);

        stream
            .write_all(&write_buf)
            .await
            .map_err(|e| ProxyError::Connection(e))?;
        stream.flush().await.map_err(ProxyError::Connection)?;

        // Read response length
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .map_err(ProxyError::Connection)?;
        let response_len = u32::from_be_bytes(len_buf) as usize;

        // Read response body
        let mut response_buf = vec![0u8; response_len];
        stream
            .read_exact(&mut response_buf)
            .await
            .map_err(ProxyError::Connection)?;

        Ok(response_buf)
    }

    /// Disconnect from the broker.
    pub async fn disconnect(&self) {
        *self.stream.lock().await = None;
    }

    /// Send a raw request and receive the response.
    ///
    /// This method handles:
    /// - Correlation ID generation and verification
    /// - Request framing (4-byte length prefix)
    /// - Response reading with timeout
    ///
    /// The `request_bytes` should contain the full Kafka request body (header + payload),
    /// but without the length prefix.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The connection is not established
    /// - Writing or reading fails
    /// - The response correlation ID doesn't match
    /// - The request times out
    #[instrument(skip(self, request_bytes), fields(broker_id = self.broker_id))]
    pub async fn send_request(&self, request_bytes: &[u8]) -> Result<Bytes> {
        let mut guard = self.stream.lock().await;
        let stream = guard.as_mut().ok_or(ProxyError::BrokerUnavailable {
            broker_id: self.broker_id,
            message: "not connected".to_string(),
        })?;

        // Extract correlation ID from request (bytes 4-7)
        if request_bytes.len() < 8 {
            return Err(ProxyError::ProtocolEncode {
                message: "request too short".to_string(),
            });
        }
        let correlation_id = i32::from_be_bytes([
            request_bytes[4],
            request_bytes[5],
            request_bytes[6],
            request_bytes[7],
        ]);

        debug!(
            correlation_id,
            request_len = request_bytes.len(),
            "sending request"
        );

        // Write request with length prefix
        let mut write_buf = BytesMut::with_capacity(4 + request_bytes.len());
        write_buf.put_u32(request_bytes.len() as u32);
        write_buf.extend_from_slice(request_bytes);

        let write_result = timeout(self.request_timeout, async {
            stream.write_all(&write_buf).await?;
            stream.flush().await
        })
        .await;

        match write_result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                // Connection may be broken, clear it
                *guard = None;
                return Err(ProxyError::Connection(e));
            }
            Err(_) => {
                return Err(ProxyError::BrokerUnavailable {
                    broker_id: self.broker_id,
                    message: "write timeout".to_string(),
                });
            }
        }

        // Read response length
        let read_result = timeout(self.request_timeout, async {
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).await?;
            let response_len = u32::from_be_bytes(len_buf) as usize;

            // Read response body
            let mut response_buf = vec![0u8; response_len];
            stream.read_exact(&mut response_buf).await?;

            Ok::<_, std::io::Error>(response_buf)
        })
        .await;

        let response_buf = match read_result {
            Ok(Ok(buf)) => buf,
            Ok(Err(e)) => {
                // Connection may be broken, clear it
                *guard = None;
                return Err(ProxyError::Connection(e));
            }
            Err(_) => {
                return Err(ProxyError::BrokerUnavailable {
                    broker_id: self.broker_id,
                    message: "read timeout".to_string(),
                });
            }
        };

        // Verify correlation ID in response (first 4 bytes)
        if response_buf.len() < 4 {
            return Err(ProxyError::ProtocolDecode {
                message: "response too short".to_string(),
            });
        }

        let response_correlation_id = i32::from_be_bytes([
            response_buf[0],
            response_buf[1],
            response_buf[2],
            response_buf[3],
        ]);

        if response_correlation_id != correlation_id {
            return Err(ProxyError::CorrelationIdMismatch {
                expected: correlation_id,
                actual: response_correlation_id,
            });
        }

        debug!(
            correlation_id,
            response_len = response_buf.len(),
            "received response"
        );

        Ok(Bytes::from(response_buf))
    }

    /// Generate a new correlation ID.
    #[must_use]
    pub fn next_correlation_id(&self) -> i32 {
        self.correlation_id.fetch_add(1, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_connection_new() {
        let conn = BrokerConnection::new(1, "localhost:9092".to_string());
        assert_eq!(conn.broker_id(), 1);
        assert_eq!(conn.address(), "localhost:9092");
    }

    #[test]
    fn test_correlation_id_generation() {
        let conn = BrokerConnection::new(1, "localhost:9092".to_string());
        let id1 = conn.next_correlation_id();
        let id2 = conn.next_correlation_id();
        let id3 = conn.next_correlation_id();

        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(id3, 2);
    }

    #[tokio::test]
    async fn test_not_connected() {
        let conn = BrokerConnection::new(1, "localhost:9092".to_string());
        assert!(!conn.is_connected().await);
    }

    #[tokio::test]
    async fn test_connect_to_invalid_address() {
        let conn = BrokerConnection::with_timeouts(
            1,
            "127.0.0.1:59999".to_string(), // Non-existent port
            Duration::from_millis(100),
            Duration::from_secs(1),
        );

        let result = conn.connect().await;
        assert!(result.is_err());
    }
}
