//! Configuration types for the Kafka partition remapping proxy.
//!
//! Configuration is loaded from YAML files and validated before use.

use regex::Regex;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::error::{ConfigError, ConfigResult};

/// Root configuration for the proxy.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProxyConfig {
    /// TCP listener configuration.
    pub listen: ListenConfig,

    /// Kafka cluster connection configuration.
    pub kafka: KafkaConfig,

    /// Partition remapping configuration.
    pub mapping: MappingConfig,

    /// Prometheus metrics configuration.
    #[serde(default)]
    pub metrics: MetricsConfig,

    /// Logging configuration.
    #[serde(default)]
    pub logging: LoggingConfig,
}

/// TCP listener configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ListenConfig {
    /// Address to bind to, e.g., "0.0.0.0:9092".
    #[serde(default = "default_listen_address")]
    pub address: String,

    /// Advertised address that clients should use to connect to this proxy.
    ///
    /// This address is returned in Metadata responses so clients reconnect
    /// to the proxy instead of directly to Kafka brokers.
    ///
    /// If not set, defaults to the listen address.
    /// Example: "proxy.example.com:9092" or "localhost:19092"
    pub advertised_address: Option<String>,

    /// Maximum number of concurrent client connections.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
}

/// Kafka cluster connection configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KafkaConfig {
    /// Bootstrap server addresses.
    pub bootstrap_servers: Vec<String>,

    /// Connection timeout in milliseconds.
    #[serde(default = "default_connection_timeout_ms")]
    pub connection_timeout_ms: u64,

    /// Request timeout in milliseconds.
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,

    /// Interval in seconds for background metadata refresh.
    /// Set to 0 to disable background refresh (only refresh on client requests).
    /// Default: 30 seconds.
    #[serde(default = "default_metadata_refresh_interval_secs")]
    pub metadata_refresh_interval_secs: u64,

    /// Security protocol for broker connections.
    #[serde(default)]
    pub security_protocol: SecurityProtocol,

    /// TLS configuration for broker connections (when using SSL or SASL_SSL).
    #[serde(default)]
    pub tls: Option<BrokerTlsConfig>,

    /// SASL authentication configuration (when using SASL_PLAINTEXT or SASL_SSL).
    #[serde(default)]
    pub sasl: Option<BrokerSaslConfig>,
}

/// Security protocol for Kafka connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SecurityProtocol {
    /// Plain TCP without encryption or authentication.
    #[default]
    Plaintext,
    /// TLS encryption without SASL authentication.
    Ssl,
    /// SASL authentication without TLS encryption.
    SaslPlaintext,
    /// TLS encryption with SASL authentication.
    SaslSsl,
}

impl SecurityProtocol {
    /// Check if TLS is required for this protocol.
    #[must_use]
    pub fn requires_tls(&self) -> bool {
        matches!(self, Self::Ssl | Self::SaslSsl)
    }

    /// Check if SASL is required for this protocol.
    #[must_use]
    pub fn requires_sasl(&self) -> bool {
        matches!(self, Self::SaslPlaintext | Self::SaslSsl)
    }
}

/// TLS configuration for broker connections.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BrokerTlsConfig {
    /// Path to CA certificate file (PEM format) for verifying broker certificates.
    /// If not set, uses the system's root certificates.
    pub ca_cert_path: Option<PathBuf>,

    /// Path to client certificate file (PEM format) for mTLS authentication.
    pub cert_path: Option<PathBuf>,

    /// Path to client private key file (PEM format) for mTLS authentication.
    pub key_path: Option<PathBuf>,

    /// Whether to skip server certificate verification (INSECURE - for testing only).
    #[serde(default)]
    pub insecure_skip_verify: bool,
}

impl Default for BrokerTlsConfig {
    fn default() -> Self {
        Self {
            ca_cert_path: None,
            cert_path: None,
            key_path: None,
            insecure_skip_verify: false,
        }
    }
}

/// SASL authentication mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
pub enum SaslMechanism {
    /// SASL/PLAIN - simple username/password authentication.
    #[default]
    #[serde(rename = "PLAIN")]
    Plain,
    /// SASL/SCRAM-SHA-256 - salted challenge-response authentication.
    #[serde(rename = "SCRAM-SHA-256")]
    ScramSha256,
    /// SASL/SCRAM-SHA-512 - salted challenge-response authentication.
    #[serde(rename = "SCRAM-SHA-512")]
    ScramSha512,
}

impl SaslMechanism {
    /// Get the Kafka mechanism name as used in the SASL handshake.
    #[must_use]
    pub fn mechanism_name(&self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

/// SASL authentication configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BrokerSaslConfig {
    /// SASL mechanism to use.
    #[serde(default)]
    pub mechanism: SaslMechanism,

    /// Username for authentication.
    /// Supports environment variable expansion: "${KAFKA_USERNAME}"
    pub username: String,

    /// Password for authentication.
    /// Supports environment variable expansion: "${KAFKA_PASSWORD}"
    pub password: String,
}

impl BrokerSaslConfig {
    /// Get the username with environment variables expanded.
    #[must_use]
    pub fn username(&self) -> String {
        expand_env_vars(&self.username)
    }

    /// Get the password with environment variables expanded.
    #[must_use]
    pub fn password(&self) -> String {
        expand_env_vars(&self.password)
    }
}

/// Expand environment variables in a string.
///
/// Replaces `${VAR_NAME}` with the value of the environment variable `VAR_NAME`.
/// If the variable is not set, replaces with an empty string.
fn expand_env_vars(s: &str) -> String {
    let re = Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}").expect("valid regex");
    re.replace_all(s, |caps: &regex::Captures| {
        std::env::var(&caps[1]).unwrap_or_default()
    })
    .to_string()
}

/// Partition remapping configuration.
///
/// Defines the mapping from virtual partitions (what clients see) to
/// physical partitions (what the Kafka cluster has).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MappingConfig {
    /// Number of virtual partitions exposed to clients.
    pub virtual_partitions: u32,

    /// Number of physical partitions in the Kafka cluster.
    pub physical_partitions: u32,

    /// Offset range allocated to each virtual partition within a physical partition.
    ///
    /// Default: 2^40 (~1 trillion offsets per virtual partition).
    /// This determines how many messages can be stored per virtual partition
    /// before potential offset collision.
    #[serde(default = "default_offset_range")]
    pub offset_range: u64,
}

/// Prometheus metrics configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MetricsConfig {
    /// Whether to enable the metrics endpoint.
    #[serde(default = "default_metrics_enabled")]
    pub enabled: bool,

    /// Address for the metrics HTTP server.
    #[serde(default = "default_metrics_address")]
    pub address: String,
}

/// Logging configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoggingConfig {
    /// Log level: trace, debug, info, warn, error.
    #[serde(default = "default_log_level")]
    pub level: String,

    /// Output logs in JSON format (for production).
    #[serde(default)]
    pub json: bool,
}

// Default value functions

fn default_listen_address() -> String {
    "0.0.0.0:9092".to_string()
}

fn default_max_connections() -> usize {
    1000
}

fn default_connection_timeout_ms() -> u64 {
    10_000
}

fn default_request_timeout_ms() -> u64 {
    30_000
}

fn default_metadata_refresh_interval_secs() -> u64 {
    30
}

fn default_offset_range() -> u64 {
    1 << 40 // 2^40 = 1,099,511,627,776
}

fn default_metrics_enabled() -> bool {
    true
}

fn default_metrics_address() -> String {
    "0.0.0.0:9090".to_string()
}

fn default_log_level() -> String {
    "info".to_string()
}

// Default implementations

impl Default for ListenConfig {
    fn default() -> Self {
        Self {
            address: default_listen_address(),
            advertised_address: None,
            max_connections: default_max_connections(),
        }
    }
}

impl ListenConfig {
    /// Get the advertised address for clients.
    ///
    /// Returns the configured `advertised_address` if set,
    /// otherwise falls back to the `address`.
    #[must_use]
    pub fn get_advertised_address(&self) -> &str {
        self.advertised_address.as_deref().unwrap_or(&self.address)
    }

    /// Parse the advertised address into host and port.
    ///
    /// # Errors
    ///
    /// Returns an error if the address cannot be parsed.
    pub fn parse_advertised_address(&self) -> ConfigResult<(String, i32)> {
        let addr = self.get_advertised_address();
        let parts: Vec<&str> = addr.rsplitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(ConfigError::InvalidAddress(addr.to_string()));
        }
        let port: i32 = parts[0]
            .parse()
            .map_err(|_| ConfigError::InvalidAddress(addr.to_string()))?;
        let host = parts[1].to_string();
        Ok((host, port))
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_metrics_enabled(),
            address: default_metrics_address(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            json: false,
        }
    }
}

// Configuration loading and validation

impl ProxyConfig {
    /// Load configuration from a YAML file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or parsed, or if
    /// validation fails.
    pub fn from_file<P: AsRef<Path>>(path: P) -> ConfigResult<Self> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path).map_err(|e| ConfigError::IoError {
            path: path.display().to_string(),
            source: e,
        })?;

        let config: Self = serde_yaml::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    /// Load configuration from a YAML string.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing or validation fails.
    pub fn from_str(content: &str) -> ConfigResult<Self> {
        let config: Self = serde_yaml::from_str(content)?;
        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if any validation check fails.
    pub fn validate(&self) -> ConfigResult<()> {
        self.mapping.validate()
    }
}

impl MappingConfig {
    /// Validate the mapping configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `physical_partitions` is zero
    /// - `virtual_partitions` is less than `physical_partitions`
    /// - `virtual_partitions` is not evenly divisible by `physical_partitions`
    /// - `offset_range` is too small
    pub fn validate(&self) -> ConfigResult<()> {
        // Physical partitions must be at least 1
        if self.physical_partitions == 0 {
            return Err(ConfigError::InvalidPhysicalPartitions(self.physical_partitions));
        }

        // Virtual partitions must be >= physical partitions
        if self.virtual_partitions < self.physical_partitions {
            return Err(ConfigError::VirtualLessThanPhysical {
                virtual_partitions: self.virtual_partitions,
                physical_partitions: self.physical_partitions,
            });
        }

        // Virtual partitions must be evenly divisible by physical partitions
        if self.virtual_partitions % self.physical_partitions != 0 {
            return Err(ConfigError::InvalidCompressionRatio {
                virtual_partitions: self.virtual_partitions,
                physical_partitions: self.physical_partitions,
            });
        }

        // Offset range must be large enough
        const MIN_OFFSET_RANGE: u64 = 1 << 20; // 2^20 = 1,048,576
        if self.offset_range < MIN_OFFSET_RANGE {
            return Err(ConfigError::OffsetRangeTooSmall(self.offset_range));
        }

        Ok(())
    }

    /// Calculate the compression ratio (virtual / physical).
    #[must_use]
    pub fn compression_ratio(&self) -> u32 {
        self.virtual_partitions / self.physical_partitions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> ProxyConfig {
        ProxyConfig {
            listen: ListenConfig::default(),
            kafka: KafkaConfig {
                bootstrap_servers: vec!["localhost:9092".to_string()],
                connection_timeout_ms: 10_000,
                request_timeout_ms: 30_000,
                metadata_refresh_interval_secs: 30,
                security_protocol: SecurityProtocol::default(),
                tls: None,
                sasl: None,
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

    #[test]
    fn test_valid_config_passes_validation() {
        let config = valid_config();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_compression_ratio() {
        let config = valid_config();
        assert_eq!(config.mapping.compression_ratio(), 10);
    }

    #[test]
    fn test_invalid_compression_ratio() {
        let mut config = valid_config();
        config.mapping.virtual_partitions = 100;
        config.mapping.physical_partitions = 30; // 100 % 30 != 0

        let result = config.validate();
        assert!(matches!(
            result,
            Err(ConfigError::InvalidCompressionRatio { .. })
        ));
    }

    #[test]
    fn test_zero_physical_partitions() {
        let mut config = valid_config();
        config.mapping.physical_partitions = 0;

        let result = config.validate();
        assert!(matches!(
            result,
            Err(ConfigError::InvalidPhysicalPartitions(0))
        ));
    }

    #[test]
    fn test_virtual_less_than_physical() {
        let mut config = valid_config();
        config.mapping.virtual_partitions = 5;
        config.mapping.physical_partitions = 10;

        let result = config.validate();
        assert!(matches!(
            result,
            Err(ConfigError::VirtualLessThanPhysical { .. })
        ));
    }

    #[test]
    fn test_offset_range_too_small() {
        let mut config = valid_config();
        config.mapping.offset_range = 1000; // Less than 2^20

        let result = config.validate();
        assert!(matches!(
            result,
            Err(ConfigError::OffsetRangeTooSmall(1000))
        ));
    }

    #[test]
    fn test_from_yaml_string() {
        let yaml = r"
listen:
  address: '0.0.0.0:9092'
kafka:
  bootstrap_servers:
    - 'localhost:9092'
mapping:
  virtual_partitions: 1000
  physical_partitions: 100
";
        let config = ProxyConfig::from_str(yaml).unwrap();
        assert_eq!(config.mapping.virtual_partitions, 1000);
        assert_eq!(config.mapping.physical_partitions, 100);
        assert_eq!(config.mapping.compression_ratio(), 10);
    }

    #[test]
    fn test_default_values_applied() {
        let yaml = r"
listen:
  address: '0.0.0.0:9092'
kafka:
  bootstrap_servers:
    - 'localhost:9092'
mapping:
  virtual_partitions: 100
  physical_partitions: 10
";
        let config = ProxyConfig::from_str(yaml).unwrap();
        // offset_range should have default value
        assert_eq!(config.mapping.offset_range, 1 << 40);
        // metrics should have defaults
        assert!(config.metrics.enabled);
        assert_eq!(config.metrics.address, "0.0.0.0:9090");
        // security_protocol should default to PLAINTEXT
        assert_eq!(config.kafka.security_protocol, SecurityProtocol::Plaintext);
    }

    #[test]
    fn test_security_protocol_parsing() {
        let yaml = r"
listen:
  address: '0.0.0.0:9092'
kafka:
  bootstrap_servers:
    - 'kafka.example.com:9093'
  security_protocol: SASL_SSL
  sasl:
    mechanism: PLAIN
    username: '${KAFKA_API_KEY}'
    password: '${KAFKA_API_SECRET}'
mapping:
  virtual_partitions: 100
  physical_partitions: 10
";
        let config = ProxyConfig::from_str(yaml).unwrap();
        assert_eq!(config.kafka.security_protocol, SecurityProtocol::SaslSsl);
        assert!(config.kafka.security_protocol.requires_tls());
        assert!(config.kafka.security_protocol.requires_sasl());

        let sasl = config.kafka.sasl.unwrap();
        assert_eq!(sasl.mechanism, SaslMechanism::Plain);
    }

    #[test]
    fn test_sasl_scram_mechanism_parsing() {
        let yaml = r"
listen:
  address: '0.0.0.0:9092'
kafka:
  bootstrap_servers:
    - 'kafka.example.com:9093'
  security_protocol: SASL_SSL
  sasl:
    mechanism: SCRAM-SHA-256
    username: 'user'
    password: 'pass'
mapping:
  virtual_partitions: 100
  physical_partitions: 10
";
        let config = ProxyConfig::from_str(yaml).unwrap();
        let sasl = config.kafka.sasl.unwrap();
        assert_eq!(sasl.mechanism, SaslMechanism::ScramSha256);
        assert_eq!(sasl.mechanism.mechanism_name(), "SCRAM-SHA-256");
    }

    #[test]
    fn test_tls_config_parsing() {
        let yaml = r"
listen:
  address: '0.0.0.0:9092'
kafka:
  bootstrap_servers:
    - 'kafka.example.com:9093'
  security_protocol: SSL
  tls:
    ca_cert_path: '/etc/ssl/ca.crt'
    cert_path: '/etc/ssl/client.crt'
    key_path: '/etc/ssl/client.key'
mapping:
  virtual_partitions: 100
  physical_partitions: 10
";
        let config = ProxyConfig::from_str(yaml).unwrap();
        assert_eq!(config.kafka.security_protocol, SecurityProtocol::Ssl);
        assert!(config.kafka.security_protocol.requires_tls());
        assert!(!config.kafka.security_protocol.requires_sasl());

        let tls = config.kafka.tls.unwrap();
        assert_eq!(
            tls.ca_cert_path,
            Some(PathBuf::from("/etc/ssl/ca.crt"))
        );
        assert_eq!(
            tls.cert_path,
            Some(PathBuf::from("/etc/ssl/client.crt"))
        );
    }

    #[test]
    fn test_env_var_expansion() {
        std::env::set_var("TEST_KAFKA_USER", "my-user");
        std::env::set_var("TEST_KAFKA_PASS", "my-password");

        let config = BrokerSaslConfig {
            mechanism: SaslMechanism::Plain,
            username: "${TEST_KAFKA_USER}".to_string(),
            password: "${TEST_KAFKA_PASS}".to_string(),
        };

        assert_eq!(config.username(), "my-user");
        assert_eq!(config.password(), "my-password");

        std::env::remove_var("TEST_KAFKA_USER");
        std::env::remove_var("TEST_KAFKA_PASS");
    }

    #[test]
    fn test_env_var_expansion_missing_var() {
        let config = BrokerSaslConfig {
            mechanism: SaslMechanism::Plain,
            username: "${NONEXISTENT_VAR}".to_string(),
            password: "literal".to_string(),
        };

        assert_eq!(config.username(), "");
        assert_eq!(config.password(), "literal");
    }

    #[test]
    fn test_security_protocol_methods() {
        assert!(!SecurityProtocol::Plaintext.requires_tls());
        assert!(!SecurityProtocol::Plaintext.requires_sasl());

        assert!(SecurityProtocol::Ssl.requires_tls());
        assert!(!SecurityProtocol::Ssl.requires_sasl());

        assert!(!SecurityProtocol::SaslPlaintext.requires_tls());
        assert!(SecurityProtocol::SaslPlaintext.requires_sasl());

        assert!(SecurityProtocol::SaslSsl.requires_tls());
        assert!(SecurityProtocol::SaslSsl.requires_sasl());
    }

    #[test]
    fn test_sasl_mechanism_names() {
        assert_eq!(SaslMechanism::Plain.mechanism_name(), "PLAIN");
        assert_eq!(SaslMechanism::ScramSha256.mechanism_name(), "SCRAM-SHA-256");
        assert_eq!(SaslMechanism::ScramSha512.mechanism_name(), "SCRAM-SHA-512");
    }
}
