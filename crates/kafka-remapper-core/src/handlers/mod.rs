//! Kafka protocol request handlers.
//!
//! Each handler is responsible for processing a specific Kafka API request,
//! applying partition/offset remapping where necessary, and forwarding to
//! the real Kafka cluster.

pub mod api_versions;
pub mod fetch;
pub mod metadata;
pub mod offset_commit;
pub mod offset_fetch;
pub mod passthrough;
pub mod produce;

pub use api_versions::ApiVersionsHandler;
pub use fetch::FetchHandler;
pub use metadata::MetadataHandler;
pub use offset_commit::OffsetCommitHandler;
pub use offset_fetch::OffsetFetchHandler;
pub use passthrough::PassthroughHandler;
pub use produce::ProduceHandler;

use async_trait::async_trait;
use bytes::BytesMut;

use crate::error::Result;
use crate::network::codec::KafkaFrame;

/// Trait for protocol handlers.
#[async_trait]
pub trait ProtocolHandler: Send + Sync {
    /// Handle a Kafka request and produce a response body.
    ///
    /// The `frame` contains the parsed request metadata and raw bytes.
    /// Returns the response body (without length prefix or correlation ID header).
    async fn handle(&self, frame: &KafkaFrame) -> Result<BytesMut>;
}
