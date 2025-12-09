//! Domain error types for the Kafka partition remapping proxy.
//!
//! Uses `thiserror` for ergonomic error definitions with proper context.

use thiserror::Error;

/// Errors related to configuration parsing and validation.
#[derive(Error, Debug)]
pub enum ConfigError {
    /// Virtual partitions must be evenly divisible by physical partitions.
    #[error("invalid compression ratio: virtual_partitions ({virtual_partitions}) must be divisible by physical_partitions ({physical_partitions})")]
    InvalidCompressionRatio {
        virtual_partitions: u32,
        physical_partitions: u32,
    },

    /// Offset range must be large enough to prevent overlap.
    #[error("offset_range must be at least 2^20 (1048576), got {0}")]
    OffsetRangeTooSmall(u64),

    /// Physical partitions must be at least 1.
    #[error("physical_partitions must be at least 1, got {0}")]
    InvalidPhysicalPartitions(u32),

    /// Virtual partitions must be at least equal to physical partitions.
    #[error("virtual_partitions ({virtual_partitions}) must be >= physical_partitions ({physical_partitions})")]
    VirtualLessThanPhysical {
        virtual_partitions: u32,
        physical_partitions: u32,
    },

    /// Failed to read configuration file.
    #[error("failed to read config file '{path}': {source}")]
    IoError {
        path: String,
        #[source]
        source: std::io::Error,
    },

    /// Failed to parse YAML configuration.
    #[error("failed to parse config: {0}")]
    ParseError(#[from] serde_yaml::Error),

    /// Invalid address format.
    #[error("invalid address format: {0} (expected 'host:port')")]
    InvalidAddress(String),
}

/// Errors that occur during proxy operation.
#[derive(Error, Debug)]
pub enum ProxyError {
    /// TCP/IO connection error.
    #[error("connection error: {0}")]
    Connection(#[from] std::io::Error),

    /// Failed to decode Kafka protocol message.
    #[error("protocol decode error: {message}")]
    ProtocolDecode { message: String },

    /// Failed to encode Kafka protocol message.
    #[error("protocol encode error: {message}")]
    ProtocolEncode { message: String },

    /// Broker is not available or connection failed.
    #[error("broker {broker_id} unavailable: {message}")]
    BrokerUnavailable { broker_id: i32, message: String },

    /// No brokers available to handle request.
    #[error("no brokers available")]
    NoBrokersAvailable,

    /// Topic was not found in metadata.
    #[error("topic not found: {topic}")]
    TopicNotFound { topic: String },

    /// Partition index out of valid range.
    #[error("partition {partition} out of range for topic {topic} (max: {max_partition})")]
    PartitionOutOfRange {
        topic: String,
        partition: i32,
        max_partition: i32,
    },

    /// Offset would overflow the configured range.
    #[error("offset overflow: virtual offset {offset} exceeds range for partition {partition}")]
    OffsetOverflow { partition: i32, offset: i64 },

    /// Request correlation ID mismatch.
    #[error("correlation ID mismatch: expected {expected}, got {actual}")]
    CorrelationIdMismatch { expected: i32, actual: i32 },

    /// Unsupported API key/version.
    #[error("unsupported API: key={api_key}, version={api_version}")]
    UnsupportedApi { api_key: i16, api_version: i16 },

    /// Remapping error.
    #[error("remapping error: {0}")]
    Remap(#[from] RemapError),

    /// Shutdown signal received.
    #[error("proxy shutting down")]
    Shutdown,
}

/// Errors specific to partition/offset remapping operations.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum RemapError {
    /// Virtual partition index exceeds configured maximum.
    #[error("virtual partition {partition} exceeds maximum {max_partition}")]
    VirtualPartitionOutOfRange { partition: i32, max_partition: u32 },

    /// Negative partition index is invalid.
    #[error("negative partition index: {0}")]
    NegativePartition(i32),

    /// Physical offset does not map to any valid virtual partition.
    #[error("physical offset {offset} does not belong to any virtual partition in physical partition {partition}")]
    InvalidPhysicalOffset { offset: i64, partition: i32 },

    /// Negative offset is invalid for mapping.
    #[error("negative offset: {0}")]
    NegativeOffset(i64),
}

/// Result type alias for proxy operations.
pub type Result<T> = std::result::Result<T, ProxyError>;

/// Result type alias for configuration operations.
pub type ConfigResult<T> = std::result::Result<T, ConfigError>;

/// Result type alias for remapping operations.
pub type RemapResult<T> = std::result::Result<T, RemapError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_error_display() {
        let err = ConfigError::InvalidCompressionRatio {
            virtual_partitions: 100,
            physical_partitions: 30,
        };
        assert!(err.to_string().contains("100"));
        assert!(err.to_string().contains("30"));
    }

    #[test]
    fn test_remap_error_display() {
        let err = RemapError::VirtualPartitionOutOfRange {
            partition: 150,
            max_partition: 100,
        };
        assert!(err.to_string().contains("150"));
        assert!(err.to_string().contains("100"));
    }

    #[test]
    fn test_proxy_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "test");
        let proxy_err: ProxyError = io_err.into();
        assert!(matches!(proxy_err, ProxyError::Connection(_)));
    }

    #[test]
    fn test_proxy_error_from_remap() {
        let remap_err = RemapError::NegativePartition(-1);
        let proxy_err: ProxyError = remap_err.into();
        assert!(matches!(proxy_err, ProxyError::Remap(_)));
    }
}
