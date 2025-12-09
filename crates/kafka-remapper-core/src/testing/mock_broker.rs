//! Mock Kafka broker for integration testing.
//!
//! A lightweight mock broker that:
//! - Accepts Kafka protocol connections
//! - Records all requests received
//! - Returns configurable responses

use bytes::{BufMut, Bytes, BytesMut};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Mutex, RwLock};

/// API keys for Kafka protocol
pub mod api_keys {
    pub const PRODUCE: i16 = 0;
    pub const FETCH: i16 = 1;
    pub const LIST_OFFSETS: i16 = 2;
    pub const METADATA: i16 = 3;
    pub const OFFSET_COMMIT: i16 = 8;
    pub const OFFSET_FETCH: i16 = 9;
    pub const FIND_COORDINATOR: i16 = 10;
    pub const JOIN_GROUP: i16 = 11;
    pub const HEARTBEAT: i16 = 12;
    pub const LEAVE_GROUP: i16 = 13;
    pub const SYNC_GROUP: i16 = 14;
    pub const API_VERSIONS: i16 = 18;
}

/// A recorded broker call.
#[derive(Debug, Clone)]
pub struct BrokerCall {
    /// The API key of the request.
    pub api_key: i16,
    /// The API version of the request.
    pub api_version: i16,
    /// The correlation ID.
    pub correlation_id: i32,
    /// The raw request bytes (without length prefix).
    pub request_bytes: Bytes,
}

/// Response generator function type.
pub type ResponseGenerator = Arc<dyn Fn(&BrokerCall) -> Bytes + Send + Sync>;

/// Mock Kafka broker for testing.
pub struct MockBroker {
    address: String,
    listener: Option<TcpListener>,
    shutdown_tx: Option<broadcast::Sender<()>>,
    call_log: Arc<RwLock<Vec<BrokerCall>>>,
    response_handlers: Arc<RwLock<HashMap<i16, ResponseGenerator>>>,
}

impl MockBroker {
    /// Create a new mock broker that will bind to the given address.
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            listener: None,
            shutdown_tx: None,
            call_log: Arc::new(RwLock::new(Vec::new())),
            response_handlers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Start the mock broker.
    ///
    /// Returns the actual address the broker is listening on.
    ///
    /// # Errors
    ///
    /// Returns an error if binding to the address fails.
    pub async fn start(&mut self) -> std::io::Result<String> {
        let listener = TcpListener::bind(&self.address).await?;
        let actual_address = listener.local_addr()?.to_string();
        self.listener = Some(listener);

        let (shutdown_tx, _) = broadcast::channel::<()>(1);
        self.shutdown_tx = Some(shutdown_tx.clone());

        let listener = self.listener.take().unwrap();
        let call_log = self.call_log.clone();
        let response_handlers = self.response_handlers.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();

        // Spawn the accept loop
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                    result = listener.accept() => {
                        match result {
                            Ok((stream, _addr)) => {
                                let call_log = call_log.clone();
                                let response_handlers = response_handlers.clone();
                                let shutdown_rx = shutdown_tx.subscribe();

                                tokio::spawn(async move {
                                    Self::handle_connection(stream, call_log, response_handlers, shutdown_rx).await;
                                });
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        });

        Ok(actual_address)
    }

    /// Stop the mock broker.
    pub async fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }

    /// Register a response handler for a specific API key.
    pub async fn register_handler(&self, api_key: i16, handler: ResponseGenerator) {
        self.response_handlers
            .write()
            .await
            .insert(api_key, handler);
    }

    /// Get all recorded calls.
    pub async fn get_calls(&self) -> Vec<BrokerCall> {
        self.call_log.read().await.clone()
    }

    /// Get calls filtered by API key.
    pub async fn get_calls_for_api(&self, api_key: i16) -> Vec<BrokerCall> {
        self.call_log
            .read()
            .await
            .iter()
            .filter(|c| c.api_key == api_key)
            .cloned()
            .collect()
    }

    /// Clear the call log.
    pub async fn clear_calls(&self) {
        self.call_log.write().await.clear();
    }

    /// Get the broker address.
    #[must_use]
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Handle a single client connection.
    async fn handle_connection(
        mut stream: TcpStream,
        call_log: Arc<RwLock<Vec<BrokerCall>>>,
        response_handlers: Arc<RwLock<HashMap<i16, ResponseGenerator>>>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => break,
                result = Self::read_frame(&mut stream) => {
                    match result {
                        Ok(Some(frame)) => {
                            // Parse request header
                            if frame.len() < 8 {
                                continue;
                            }

                            let api_key = i16::from_be_bytes([frame[0], frame[1]]);
                            let api_version = i16::from_be_bytes([frame[2], frame[3]]);
                            let correlation_id = i32::from_be_bytes([
                                frame[4], frame[5], frame[6], frame[7],
                            ]);

                            let call = BrokerCall {
                                api_key,
                                api_version,
                                correlation_id,
                                request_bytes: Bytes::copy_from_slice(&frame),
                            };

                            // Record the call
                            call_log.write().await.push(call.clone());

                            // Generate response
                            let response = {
                                let handlers = response_handlers.read().await;
                                if let Some(handler) = handlers.get(&api_key) {
                                    handler(&call)
                                } else {
                                    Self::default_response(&call)
                                }
                            };

                            // Write response with length prefix
                            if Self::write_frame(&mut stream, &response).await.is_err() {
                                break;
                            }
                        }
                        Ok(None) => break, // Connection closed
                        Err(_) => break,   // Error reading
                    }
                }
            }
        }
    }

    /// Read a Kafka frame (4-byte length prefix + body).
    async fn read_frame(stream: &mut TcpStream) -> std::io::Result<Option<Vec<u8>>> {
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        let len = u32::from_be_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await?;
        Ok(Some(buf))
    }

    /// Write a Kafka frame (4-byte length prefix + body).
    async fn write_frame(stream: &mut TcpStream, data: &[u8]) -> std::io::Result<()> {
        let mut buf = BytesMut::with_capacity(4 + data.len());
        buf.put_u32(data.len() as u32);
        buf.extend_from_slice(data);
        stream.write_all(&buf).await?;
        stream.flush().await
    }

    /// Generate a default response (correlation ID only).
    fn default_response(call: &BrokerCall) -> Bytes {
        let mut buf = BytesMut::with_capacity(4);
        buf.put_i32(call.correlation_id);
        buf.freeze()
    }
}

/// Default response generators for common API types.
pub mod responses {
    use super::*;

    /// Create a simple metadata response for testing.
    ///
    /// Returns a response with:
    /// - One broker at the given address
    /// - One topic with the specified number of partitions
    pub fn metadata_response(
        broker_id: i32,
        broker_host: &str,
        broker_port: i32,
        topic_name: &str,
        partition_count: i32,
    ) -> ResponseGenerator {
        let host = broker_host.to_string();
        let topic = topic_name.to_string();

        Arc::new(move |call: &BrokerCall| {
            // Build a minimal MetadataResponse
            // This is a simplified version - production would use kafka-protocol crate
            let mut buf = BytesMut::with_capacity(256);

            // Correlation ID
            buf.put_i32(call.correlation_id);

            // For API version >= 9, there's a throttle_time_ms
            if call.api_version >= 9 {
                buf.put_i32(0); // throttle_time_ms
            }

            // Brokers array (compact for v9+)
            if call.api_version >= 9 {
                buf.put_u8(2); // compact array length = 1 + 1
            } else {
                buf.put_i32(1); // array length = 1
            }

            // Broker entry
            buf.put_i32(broker_id);

            // Host (compact string for v9+)
            let host_bytes = host.as_bytes();
            if call.api_version >= 9 {
                buf.put_u8((host_bytes.len() + 1) as u8);
            } else {
                buf.put_i16(host_bytes.len() as i16);
            }
            buf.extend_from_slice(host_bytes);

            buf.put_i32(broker_port);

            // Rack (nullable, compact for v9+)
            if call.api_version >= 9 {
                buf.put_u8(0); // null compact string
            } else if call.api_version >= 1 {
                buf.put_i16(-1); // null string
            }

            // Tagged fields for broker (v9+)
            if call.api_version >= 9 {
                buf.put_u8(0);
            }

            // Cluster ID (v2+)
            if call.api_version >= 2 {
                if call.api_version >= 9 {
                    buf.put_u8(0); // null compact string
                } else {
                    buf.put_i16(-1); // null string
                }
            }

            // Controller ID (v1+)
            if call.api_version >= 1 {
                buf.put_i32(broker_id);
            }

            // Topics array
            if call.api_version >= 9 {
                buf.put_u8(2); // compact array length = 1 + 1
            } else {
                buf.put_i32(1);
            }

            // Topic entry
            buf.put_i16(0); // error_code = 0

            // Topic name
            let topic_bytes = topic.as_bytes();
            if call.api_version >= 9 {
                buf.put_u8((topic_bytes.len() + 1) as u8);
            } else {
                buf.put_i16(topic_bytes.len() as i16);
            }
            buf.extend_from_slice(topic_bytes);

            // Topic ID (v10+)
            if call.api_version >= 10 {
                buf.extend_from_slice(&[0u8; 16]); // null UUID
            }

            // Is internal (v1+)
            if call.api_version >= 1 {
                buf.put_u8(0); // false
            }

            // Partitions array
            if call.api_version >= 9 {
                buf.put_u8((partition_count + 1) as u8);
            } else {
                buf.put_i32(partition_count);
            }

            // Partition entries
            for partition_id in 0..partition_count {
                buf.put_i16(0); // error_code

                buf.put_i32(partition_id);
                buf.put_i32(broker_id); // leader_id

                // Leader epoch (v7+)
                if call.api_version >= 7 {
                    buf.put_i32(0);
                }

                // Replicas
                if call.api_version >= 9 {
                    buf.put_u8(2); // 1 replica + 1
                } else {
                    buf.put_i32(1);
                }
                buf.put_i32(broker_id);

                // ISR
                if call.api_version >= 9 {
                    buf.put_u8(2);
                } else {
                    buf.put_i32(1);
                }
                buf.put_i32(broker_id);

                // Offline replicas (v5+)
                if call.api_version >= 5 {
                    if call.api_version >= 9 {
                        buf.put_u8(1); // empty array
                    } else {
                        buf.put_i32(0);
                    }
                }

                // Tagged fields for partition (v9+)
                if call.api_version >= 9 {
                    buf.put_u8(0);
                }
            }

            // Topic authorized operations (v8+)
            if call.api_version >= 8 {
                buf.put_i32(-2147483648); // unknown
            }

            // Tagged fields for topic (v9+)
            if call.api_version >= 9 {
                buf.put_u8(0);
            }

            // Tagged fields for response (v9+)
            if call.api_version >= 9 {
                buf.put_u8(0);
            }

            buf.freeze()
        })
    }

    /// Create a simple produce response.
    pub fn produce_response(base_offset: i64) -> ResponseGenerator {
        Arc::new(move |call: &BrokerCall| {
            let mut buf = BytesMut::with_capacity(64);
            buf.put_i32(call.correlation_id);

            // Responses array (1 topic)
            buf.put_i32(1);

            // Topic name (we just echo back a simple name)
            let topic = b"test-topic";
            buf.put_i16(topic.len() as i16);
            buf.extend_from_slice(topic);

            // Partition responses (1 partition)
            buf.put_i32(1);

            // Partition response
            buf.put_i32(0); // partition index
            buf.put_i16(0); // error code
            buf.put_i64(base_offset); // base offset
            buf.put_i64(-1); // log append time (v2+)

            // Throttle time (v1+)
            if call.api_version >= 1 {
                buf.put_i32(0);
            }

            buf.freeze()
        })
    }

    /// Create an empty fetch response.
    pub fn fetch_response() -> ResponseGenerator {
        Arc::new(|call: &BrokerCall| {
            let mut buf = BytesMut::with_capacity(32);
            buf.put_i32(call.correlation_id);

            // Throttle time (v1+)
            if call.api_version >= 1 {
                buf.put_i32(0);
            }

            // Error code (v7+)
            if call.api_version >= 7 {
                buf.put_i16(0);
            }

            // Session ID (v7+)
            if call.api_version >= 7 {
                buf.put_i32(0);
            }

            // Responses array (empty)
            buf.put_i32(0);

            buf.freeze()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn test_mock_broker_start_stop() {
        let mut broker = MockBroker::new("127.0.0.1:0");
        let addr = broker.start().await.unwrap();
        assert!(!addr.is_empty());

        // Should be able to connect
        let result = TcpStream::connect(&addr).await;
        assert!(result.is_ok());

        broker.stop().await;
    }

    #[tokio::test]
    async fn test_mock_broker_records_calls() {
        let mut broker = MockBroker::new("127.0.0.1:0");
        let addr = broker.start().await.unwrap();

        let mut stream = TcpStream::connect(&addr).await.unwrap();

        // Send a fake request
        // API key: 3 (Metadata), Version: 1, Correlation ID: 42
        let mut request = BytesMut::new();
        request.put_i16(3); // api key
        request.put_i16(1); // api version
        request.put_i32(42); // correlation id
        request.put_i16(0); // client id length (empty)

        // Send with length prefix
        let mut frame = BytesMut::with_capacity(4 + request.len());
        frame.put_u32(request.len() as u32);
        frame.extend_from_slice(&request);
        stream.write_all(&frame).await.unwrap();
        stream.flush().await.unwrap();

        // Read response
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut response_buf = vec![0u8; len];
        stream.read_exact(&mut response_buf).await.unwrap();

        // Wait a bit for the call to be recorded
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Check call was recorded
        let calls = broker.get_calls().await;
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].api_key, 3);
        assert_eq!(calls[0].api_version, 1);
        assert_eq!(calls[0].correlation_id, 42);

        broker.stop().await;
    }

    #[tokio::test]
    async fn test_mock_broker_custom_handler() {
        let mut broker = MockBroker::new("127.0.0.1:0");
        let addr = broker.start().await.unwrap();

        // Register a custom handler for API key 3 (Metadata)
        broker
            .register_handler(
                3,
                Arc::new(|call| {
                    let mut buf = BytesMut::new();
                    buf.put_i32(call.correlation_id);
                    buf.put_i32(12345); // custom data
                    buf.freeze()
                }),
            )
            .await;

        let mut stream = TcpStream::connect(&addr).await.unwrap();

        // Send metadata request
        let mut request = BytesMut::new();
        request.put_i16(3); // api key
        request.put_i16(1); // api version
        request.put_i32(100); // correlation id

        let mut frame = BytesMut::with_capacity(4 + request.len());
        frame.put_u32(request.len() as u32);
        frame.extend_from_slice(&request);
        stream.write_all(&frame).await.unwrap();
        stream.flush().await.unwrap();

        // Read response
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut response_buf = vec![0u8; len];
        stream.read_exact(&mut response_buf).await.unwrap();

        // Check correlation ID
        assert_eq!(
            i32::from_be_bytes([
                response_buf[0],
                response_buf[1],
                response_buf[2],
                response_buf[3]
            ]),
            100
        );
        // Check custom data
        assert_eq!(
            i32::from_be_bytes([
                response_buf[4],
                response_buf[5],
                response_buf[6],
                response_buf[7]
            ]),
            12345
        );

        broker.stop().await;
    }
}
