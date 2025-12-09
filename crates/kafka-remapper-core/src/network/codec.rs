//! Kafka protocol frame codec.
//!
//! Implements tokio's `Decoder` and `Encoder` traits for Kafka wire protocol frames.
//! Kafka uses a simple framing format: 4-byte big-endian length prefix followed by
//! the message bytes.

use bytes::{Buf, BufMut, BytesMut};
use kafka_protocol::messages::ApiKey;
use std::io;
use tokio_util::codec::{Decoder, Encoder};

/// Maximum frame size (100 MB by default).
const DEFAULT_MAX_FRAME_SIZE: usize = 100 * 1024 * 1024;

/// A parsed Kafka frame containing the raw bytes and metadata.
#[derive(Debug)]
pub struct KafkaFrame {
    /// The API key for this request.
    pub api_key: ApiKey,
    /// The API version.
    pub api_version: i16,
    /// The correlation ID for request/response matching.
    pub correlation_id: i32,
    /// The complete frame bytes (after the length prefix).
    pub bytes: BytesMut,
}

/// Codec for Kafka wire protocol frames.
///
/// Kafka messages are framed as:
/// - 4 bytes: message length (big-endian, excludes these 4 bytes)
/// - N bytes: message content
///
/// The first 8 bytes of the message content contain:
/// - 2 bytes: API key
/// - 2 bytes: API version
/// - 4 bytes: correlation ID
#[derive(Debug, Clone)]
pub struct KafkaCodec {
    max_frame_size: usize,
}

impl KafkaCodec {
    /// Create a new codec with default max frame size.
    #[must_use]
    pub fn new() -> Self {
        Self {
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        }
    }

    /// Create a new codec with custom max frame size.
    #[must_use]
    pub fn with_max_frame_size(max_frame_size: usize) -> Self {
        Self { max_frame_size }
    }
}

impl Default for KafkaCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for KafkaCodec {
    type Item = KafkaFrame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Need at least 4 bytes for the length prefix
        if src.len() < 4 {
            return Ok(None);
        }

        // Read length without consuming
        let length = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

        // Validate frame size
        if length > self.max_frame_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "frame size {length} exceeds maximum {}",
                    self.max_frame_size
                ),
            ));
        }

        // Need length prefix + message body
        if src.len() < 4 + length {
            // Reserve space for the full frame
            src.reserve(4 + length - src.len());
            return Ok(None);
        }

        // Consume length prefix
        src.advance(4);

        // Extract frame bytes
        let bytes = src.split_to(length);

        // Parse header (first 8 bytes: api_key + api_version + correlation_id)
        if bytes.len() < 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame too small for request header",
            ));
        }

        let api_key_raw = i16::from_be_bytes([bytes[0], bytes[1]]);
        let api_version = i16::from_be_bytes([bytes[2], bytes[3]]);
        let correlation_id = i32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);

        let api_key = ApiKey::try_from(api_key_raw).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown API key: {api_key_raw}"),
            )
        })?;

        Ok(Some(KafkaFrame {
            api_key,
            api_version,
            correlation_id,
            bytes,
        }))
    }
}

/// A response frame to be encoded.
#[derive(Debug)]
pub struct ResponseFrame {
    /// The correlation ID (must match the request).
    pub correlation_id: i32,
    /// The response body bytes.
    pub body: BytesMut,
}

impl Encoder<ResponseFrame> for KafkaCodec {
    type Error = io::Error;

    fn encode(&mut self, item: ResponseFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Response format:
        // 4 bytes: length (correlation_id + body)
        // 4 bytes: correlation_id
        // N bytes: body

        let total_len = 4 + item.body.len();

        dst.reserve(4 + total_len);
        dst.put_u32(total_len as u32);
        dst.put_i32(item.correlation_id);
        dst.extend_from_slice(&item.body);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_request_frame(api_key: i16, api_version: i16, correlation_id: i32) -> BytesMut {
        let mut buf = BytesMut::new();
        // Length prefix (8 bytes for header + some body)
        buf.put_u32(12);
        // API key
        buf.put_i16(api_key);
        // API version
        buf.put_i16(api_version);
        // Correlation ID
        buf.put_i32(correlation_id);
        // Some body bytes
        buf.put_u32(0);
        buf
    }

    #[test]
    fn test_decode_valid_frame() {
        let mut codec = KafkaCodec::new();
        let mut buf = make_request_frame(18, 3, 12345); // ApiVersions v3

        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame.api_key, ApiKey::ApiVersions);
        assert_eq!(frame.api_version, 3);
        assert_eq!(frame.correlation_id, 12345);
        assert_eq!(frame.bytes.len(), 12); // 8 header + 4 body
    }

    #[test]
    fn test_decode_incomplete_length() {
        let mut codec = KafkaCodec::new();
        let mut buf = BytesMut::from(&[0u8, 0, 0][..]); // Only 3 bytes

        let result = codec.decode(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_incomplete_body() {
        let mut codec = KafkaCodec::new();
        let mut buf = BytesMut::new();
        buf.put_u32(100); // Expect 100 bytes
        buf.put_u32(0); // Only 4 bytes of body

        let result = codec.decode(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_frame_too_large() {
        let mut codec = KafkaCodec::with_max_frame_size(100);
        let mut buf = BytesMut::new();
        buf.put_u32(200); // Larger than max

        let result = codec.decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_unknown_api_key() {
        let mut codec = KafkaCodec::new();
        let mut buf = make_request_frame(9999, 0, 1); // Invalid API key

        let result = codec.decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_encode_response() {
        let mut codec = KafkaCodec::new();
        let mut dst = BytesMut::new();

        let response = ResponseFrame {
            correlation_id: 12345,
            body: BytesMut::from(&[1u8, 2, 3, 4][..]),
        };

        codec.encode(response, &mut dst).unwrap();

        // Should have: 4 (length) + 4 (correlation_id) + 4 (body) = 12 bytes
        assert_eq!(dst.len(), 12);
        // Length should be 8 (correlation_id + body)
        assert_eq!(u32::from_be_bytes([dst[0], dst[1], dst[2], dst[3]]), 8);
        // Correlation ID
        assert_eq!(i32::from_be_bytes([dst[4], dst[5], dst[6], dst[7]]), 12345);
        // Body
        assert_eq!(&dst[8..12], &[1, 2, 3, 4]);
    }

    #[test]
    fn test_multiple_frames() {
        let mut codec = KafkaCodec::new();
        let mut buf = BytesMut::new();

        // First frame
        buf.extend_from_slice(&make_request_frame(18, 3, 1));
        // Second frame
        buf.extend_from_slice(&make_request_frame(3, 9, 2));

        // Decode first
        let frame1 = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame1.correlation_id, 1);

        // Decode second
        let frame2 = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame2.correlation_id, 2);

        // No more frames
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }
}
