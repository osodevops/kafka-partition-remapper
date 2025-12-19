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

    /// General validation error.
    #[error("validation error: {0}")]
    ValidationError(String),
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

    /// TLS error.
    #[error("TLS error: {0}")]
    Tls(#[from] TlsError),

    /// SASL authentication error.
    #[error("authentication error: {0}")]
    Auth(#[from] AuthError),

    /// Shutdown signal received.
    #[error("proxy shutting down")]
    Shutdown,
}

/// Errors related to TLS/SSL operations.
#[derive(Error, Debug)]
pub enum TlsError {
    /// Failed to load certificate file.
    #[error("failed to load certificate from '{path}': {message}")]
    CertificateLoad { path: String, message: String },

    /// Failed to load private key file.
    #[error("failed to load private key from '{path}': {message}")]
    PrivateKeyLoad { path: String, message: String },

    /// Failed to build TLS configuration.
    #[error("TLS configuration error: {0}")]
    Config(String),

    /// TLS handshake failed.
    #[error("TLS handshake failed: {0}")]
    Handshake(String),

    /// Invalid certificate.
    #[error("invalid certificate: {0}")]
    InvalidCertificate(String),

    /// No certificates found in file.
    #[error("no certificates found in '{0}'")]
    NoCertificates(String),

    /// No private keys found in file.
    #[error("no private keys found in '{0}'")]
    NoPrivateKeys(String),
}

/// Errors related to SASL authentication.
#[derive(Error, Debug)]
pub enum AuthError {
    /// SASL mechanism not supported.
    #[error("unsupported SASL mechanism: {0}")]
    UnsupportedMechanism(String),

    /// Authentication failed (invalid credentials).
    #[error("authentication failed: {0}")]
    AuthenticationFailed(String),

    /// Invalid credentials provided.
    #[error("invalid credentials")]
    InvalidCredentials,

    /// SASL handshake failed.
    #[error("SASL handshake error: {0}")]
    HandshakeError(String),

    /// Missing required configuration.
    #[error("missing SASL configuration: {0}")]
    MissingConfig(String),

    /// Invalid SASL protocol message.
    #[error("invalid SASL message: {0}")]
    InvalidMessage(String),

    /// Credential store error.
    #[error("credential store error: {0}")]
    CredentialStore(String),

    /// Configuration error (e.g., OAUTHBEARER setup).
    #[error("authentication configuration error: {0}")]
    Configuration(String),

    /// OAuth token validation failed.
    #[error("OAuth token validation failed: {0}")]
    TokenValidationFailed(String),

    /// OAuth token has expired.
    #[error("OAuth token expired")]
    TokenExpired,

    /// OAuth token signature verification failed.
    #[error("OAuth token signature invalid")]
    TokenSignatureInvalid,

    /// Required scope missing from OAuth token.
    #[error("missing required OAuth scope: {0}")]
    MissingScope(String),

    /// Unexpected error during authentication.
    #[error("unexpected authentication error: {0}")]
    Unexpected(String),
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

/// Result type alias for TLS operations.
pub type TlsResult<T> = std::result::Result<T, TlsError>;

/// Result type alias for authentication operations.
pub type AuthResult<T> = std::result::Result<T, AuthError>;

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

    #[test]
    fn test_proxy_error_from_tls() {
        let tls_err = TlsError::Config("test error".to_string());
        let proxy_err: ProxyError = tls_err.into();
        assert!(matches!(proxy_err, ProxyError::Tls(_)));
    }

    #[test]
    fn test_proxy_error_from_auth() {
        let auth_err = AuthError::AuthenticationFailed("invalid credentials".to_string());
        let proxy_err: ProxyError = auth_err.into();
        assert!(matches!(proxy_err, ProxyError::Auth(_)));
    }

    #[test]
    fn test_tls_error_display() {
        let err = TlsError::CertificateLoad {
            path: "/etc/ssl/cert.pem".to_string(),
            message: "file not found".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("/etc/ssl/cert.pem"));
        assert!(msg.contains("file not found"));
    }

    #[test]
    fn test_auth_error_display() {
        let err = AuthError::UnsupportedMechanism("GSSAPI".to_string());
        assert!(err.to_string().contains("GSSAPI"));
    }
}
