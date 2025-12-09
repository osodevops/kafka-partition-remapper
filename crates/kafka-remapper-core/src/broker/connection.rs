//! Single Kafka broker connection.
//!
//! Manages a TCP connection to a single Kafka broker with correlation ID tracking
//! for request/response matching.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::{debug, instrument, warn};

use crate::error::{ProxyError, Result};

/// Default connection timeout.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Default request timeout.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// A connection to a single Kafka broker.
pub struct BrokerConnection {
    broker_id: i32,
    address: String,
    stream: Mutex<Option<TcpStream>>,
    correlation_id: AtomicI32,
    connect_timeout: Duration,
    request_timeout: Duration,
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
        }
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
    /// # Errors
    ///
    /// Returns an error if the connection fails or times out.
    #[instrument(skip(self), fields(broker_id = self.broker_id, address = %self.address))]
    pub async fn connect(&self) -> Result<()> {
        let result = timeout(self.connect_timeout, TcpStream::connect(&self.address)).await;

        match result {
            Ok(Ok(stream)) => {
                debug!("connected to broker");
                *self.stream.lock().await = Some(stream);
                Ok(())
            }
            Ok(Err(e)) => {
                warn!(error = %e, "failed to connect to broker");
                Err(ProxyError::BrokerUnavailable {
                    broker_id: self.broker_id,
                    message: e.to_string(),
                })
            }
            Err(_) => {
                warn!("connection timeout");
                Err(ProxyError::BrokerUnavailable {
                    broker_id: self.broker_id,
                    message: "connection timeout".to_string(),
                })
            }
        }
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
        let correlation_id =
            i32::from_be_bytes([request_bytes[4], request_bytes[5], request_bytes[6], request_bytes[7]]);

        debug!(correlation_id, request_len = request_bytes.len(), "sending request");

        // Write request with length prefix
        let mut write_buf = BytesMut::with_capacity(4 + request_bytes.len());
        write_buf.put_u32(request_bytes.len() as u32);
        write_buf.extend_from_slice(request_bytes);

        let write_result = timeout(
            self.request_timeout,
            async {
                stream.write_all(&write_buf).await?;
                stream.flush().await
            },
        )
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
