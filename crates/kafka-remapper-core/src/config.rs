//! Configuration types for the Kafka partition remapping proxy.
//!
//! Configuration is loaded from YAML files and validated before use.

use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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

    /// Client-facing security configuration.
    ///
    /// Configures TLS termination and SASL authentication for client connections.
    /// If not set, clients connect using plaintext (no encryption or authentication).
    #[serde(default)]
    pub security: Option<ClientSecurityConfig>,
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
    /// SASL/OAUTHBEARER - OAuth 2.0 bearer token authentication.
    #[serde(rename = "OAUTHBEARER")]
    OAuthBearer,
}

impl SaslMechanism {
    /// Get the Kafka mechanism name as used in the SASL handshake.
    #[must_use]
    pub fn mechanism_name(&self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
            Self::OAuthBearer => "OAUTHBEARER",
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

// =============================================================================
// Client-Side Security Configuration
// =============================================================================

/// Security configuration for client connections to the proxy.
///
/// This allows the proxy to authenticate clients before allowing them
/// to access Kafka through the remapping proxy.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClientSecurityConfig {
    /// Security protocol for client connections.
    /// Options: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
    #[serde(default)]
    pub protocol: SecurityProtocol,

    /// TLS configuration for client connections (server-side TLS).
    /// Required when protocol is SSL or SASL_SSL.
    #[serde(default)]
    pub tls: Option<ClientTlsConfig>,

    /// SASL authentication configuration for clients.
    /// Required when protocol is SASL_PLAINTEXT or SASL_SSL.
    #[serde(default)]
    pub sasl: Option<ClientSaslConfig>,
}

impl Default for ClientSecurityConfig {
    fn default() -> Self {
        Self {
            protocol: SecurityProtocol::Plaintext,
            tls: None,
            sasl: None,
        }
    }
}

impl ClientSecurityConfig {
    /// Validate the client security configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - TLS is required but no TLS config is provided
    /// - TLS config is missing required certificate paths
    /// - SASL is required but no SASL config is provided
    pub fn validate(&self) -> ConfigResult<()> {
        // Check TLS config if TLS is required
        if self.protocol.requires_tls() {
            let tls = self.tls.as_ref().ok_or_else(|| {
                ConfigError::ValidationError(
                    "TLS configuration required for SSL/SASL_SSL protocol".to_string(),
                )
            })?;
            tls.validate()?;
        }

        // Check SASL config if SASL is required
        if self.protocol.requires_sasl() {
            let sasl = self.sasl.as_ref().ok_or_else(|| {
                ConfigError::ValidationError(
                    "SASL configuration required for SASL_PLAINTEXT/SASL_SSL protocol".to_string(),
                )
            })?;
            sasl.validate()?;
        }

        Ok(())
    }
}

/// TLS configuration for client-facing connections (server-side TLS).
///
/// The proxy acts as a TLS server, presenting certificates to clients.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClientTlsConfig {
    /// Path to server certificate file (PEM format). Required.
    pub cert_path: PathBuf,

    /// Path to server private key file (PEM format). Required.
    pub key_path: PathBuf,

    /// Path to CA certificate for client certificate verification (mTLS).
    /// If provided, client certificates will be validated against this CA.
    pub ca_cert_path: Option<PathBuf>,

    /// Require client certificates (mTLS mode).
    /// If true, clients must present a valid certificate signed by the CA.
    #[serde(default)]
    pub require_client_cert: bool,

    /// Principal extraction configuration for mTLS connections.
    ///
    /// Configures how to extract the principal identity from client
    /// X.509 certificates. Only used when mTLS is enabled.
    #[serde(default)]
    pub principal: SslPrincipalConfig,
}

/// SSL principal mapping configuration.
///
/// Configures how to extract a principal name from X.509 certificate
/// Distinguished Names (DNs). This follows Apache Kafka's
/// `ssl.principal.mapping.rules` configuration pattern.
///
/// # Rule Syntax
///
/// Rules are comma-separated and processed in order:
/// - `DEFAULT` - Return the DN unchanged
/// - `RULE:pattern/replacement/[LU]` - Regex replacement with optional case transform
///
/// # Examples
///
/// ```yaml
/// principal:
///   # Extract CN and lowercase it; fallback to full DN
///   mapping_rules: "RULE:^CN=([^,]+),.*$/$1/L,DEFAULT"
/// ```
///
/// Common patterns:
/// - `"RULE:^CN=([^,]+).*$/$1/"` - Extract CN, keep case
/// - `"RULE:^CN=([^,]+).*$/$1/L"` - Extract CN, lowercase
/// - `"DEFAULT"` - Use full DN as principal name
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SslPrincipalConfig {
    /// Mapping rules for extracting principal from certificate DN.
    ///
    /// Format: Comma-separated list of rules processed in order.
    /// First matching rule wins.
    ///
    /// Default: `"DEFAULT"` (use full DN unchanged)
    #[serde(default = "default_principal_rules")]
    pub mapping_rules: String,
}

impl Default for SslPrincipalConfig {
    fn default() -> Self {
        Self {
            mapping_rules: default_principal_rules(),
        }
    }
}

fn default_principal_rules() -> String {
    "DEFAULT".to_string()
}

impl ClientTlsConfig {
    /// Validate the client TLS configuration.
    pub fn validate(&self) -> ConfigResult<()> {
        // Check that cert file exists
        if !self.cert_path.exists() {
            return Err(ConfigError::ValidationError(format!(
                "Server certificate file not found: {}",
                self.cert_path.display()
            )));
        }

        // Check that key file exists
        if !self.key_path.exists() {
            return Err(ConfigError::ValidationError(format!(
                "Server private key file not found: {}",
                self.key_path.display()
            )));
        }

        // If requiring client cert, CA cert path must be provided
        if self.require_client_cert && self.ca_cert_path.is_none() {
            return Err(ConfigError::ValidationError(
                "CA certificate path required when require_client_cert is true".to_string(),
            ));
        }

        // Check CA cert exists if provided
        if let Some(ca_path) = &self.ca_cert_path {
            if !ca_path.exists() {
                return Err(ConfigError::ValidationError(format!(
                    "CA certificate file not found: {}",
                    ca_path.display()
                )));
            }
        }

        Ok(())
    }
}

/// SASL configuration for client authentication.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClientSaslConfig {
    /// Enabled SASL mechanisms (clients can choose from these).
    #[serde(default = "default_client_sasl_mechanisms")]
    pub enabled_mechanisms: Vec<SaslMechanism>,

    /// Credential configuration for authenticating clients (for PLAIN/SCRAM).
    pub credentials: CredentialConfig,

    /// OAUTHBEARER-specific configuration.
    ///
    /// Required when `OAUTHBEARER` is in `enabled_mechanisms`.
    #[serde(default)]
    pub oauthbearer: Option<OAuthBearerConfig>,
}

fn default_client_sasl_mechanisms() -> Vec<SaslMechanism> {
    vec![SaslMechanism::Plain]
}

impl ClientSaslConfig {
    /// Validate the client SASL configuration.
    pub fn validate(&self) -> ConfigResult<()> {
        if self.enabled_mechanisms.is_empty() {
            return Err(ConfigError::ValidationError(
                "At least one SASL mechanism must be enabled".to_string(),
            ));
        }

        // Validate OAUTHBEARER config if OAUTHBEARER is enabled
        if self.enabled_mechanisms.contains(&SaslMechanism::OAuthBearer) {
            if let Some(ref oauth_config) = self.oauthbearer {
                oauth_config.validate()?;
            }
            // Note: oauthbearer config is optional - if not provided,
            // defaults to passthrough mode (validation: none)
        }

        self.credentials.validate()
    }

    /// Check if a mechanism is enabled.
    #[must_use]
    pub fn is_mechanism_enabled(&self, mechanism: SaslMechanism) -> bool {
        self.enabled_mechanisms.contains(&mechanism)
    }

    /// Get the list of enabled mechanism names for SASL handshake response.
    #[must_use]
    pub fn enabled_mechanism_names(&self) -> Vec<&'static str> {
        self.enabled_mechanisms
            .iter()
            .map(|m| m.mechanism_name())
            .collect()
    }
}

/// Credential storage configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum CredentialConfig {
    /// Inline user credentials.
    Inline {
        /// List of user credentials.
        users: Vec<UserCredential>,
    },
    /// File-based credentials.
    File {
        /// Path to credentials file (YAML format).
        file: PathBuf,
    },
}

impl CredentialConfig {
    /// Validate the credential configuration.
    pub fn validate(&self) -> ConfigResult<()> {
        match self {
            Self::Inline { users } => {
                if users.is_empty() {
                    return Err(ConfigError::ValidationError(
                        "At least one user credential must be configured".to_string(),
                    ));
                }
                for user in users {
                    if user.username.is_empty() {
                        return Err(ConfigError::ValidationError(
                            "Username cannot be empty".to_string(),
                        ));
                    }
                }
                Ok(())
            }
            Self::File { file } => {
                if !file.exists() {
                    return Err(ConfigError::ValidationError(format!(
                        "Credentials file not found: {}",
                        file.display()
                    )));
                }
                Ok(())
            }
        }
    }
}

/// A single user credential for client authentication.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UserCredential {
    /// Username for authentication.
    pub username: String,

    /// Password for authentication.
    /// Supports environment variable expansion: "${VARIABLE_NAME}"
    pub password: String,
}

impl UserCredential {
    /// Get the password with environment variables expanded.
    #[must_use]
    pub fn password(&self) -> String {
        expand_env_vars(&self.password)
    }
}

// =============================================================================
// OAUTHBEARER Configuration
// =============================================================================

/// Configuration for OAUTHBEARER authentication.
///
/// OAUTHBEARER uses OAuth 2.0 bearer tokens for authentication (KIP-255).
/// The proxy can either pass through tokens without validation (for testing
/// or when the broker validates tokens) or validate tokens locally using JWT.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct OAuthBearerConfig {
    /// Token validation mode.
    ///
    /// - `none`: Accept tokens without validation (passthrough mode)
    /// - `jwt`: Validate JWT signature and claims using JWKS
    #[serde(default)]
    pub validation: TokenValidationMode,

    /// JWT validation configuration (required when validation is `jwt`).
    #[serde(default)]
    pub jwt: Option<JwtValidationConfig>,
}

impl OAuthBearerConfig {
    /// Validate the OAUTHBEARER configuration.
    pub fn validate(&self) -> ConfigResult<()> {
        match self.validation {
            TokenValidationMode::None => Ok(()),
            TokenValidationMode::Jwt => {
                let jwt = self.jwt.as_ref().ok_or_else(|| {
                    ConfigError::ValidationError(
                        "JWT configuration required when validation mode is 'jwt'".to_string(),
                    )
                })?;
                jwt.validate()
            }
        }
    }
}

/// Token validation mode for OAUTHBEARER.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenValidationMode {
    /// Accept tokens without validation (passthrough mode).
    ///
    /// The token's `sub` claim is extracted for the principal name,
    /// but no signature or expiration validation is performed.
    /// Use this when the broker performs token validation.
    #[default]
    None,

    /// Validate JWT signature and claims using JWKS.
    ///
    /// Requires `jwt` configuration with JWKS URL or inline keys.
    Jwt,
}

/// JWT validation configuration for OAUTHBEARER.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JwtValidationConfig {
    /// URL to fetch JWKS (JSON Web Key Set) for signature verification.
    ///
    /// Example: `https://login.microsoftonline.com/{tenant}/discovery/v2.0/keys`
    pub jwks_url: Option<String>,

    /// Inline JWKS JSON (alternative to URL for air-gapped environments).
    pub jwks_inline: Option<String>,

    /// Expected token issuer (`iss` claim).
    ///
    /// If set, tokens with a different issuer will be rejected.
    pub issuer: Option<String>,

    /// Expected audience (`aud` claim).
    ///
    /// If set, tokens not intended for this audience will be rejected.
    pub audience: Option<String>,

    /// Required scopes (space-separated in token's `scope` claim).
    ///
    /// If set, tokens missing any of these scopes will be rejected.
    #[serde(default)]
    pub required_scopes: Vec<String>,

    /// Clock skew tolerance in seconds for expiration checks.
    ///
    /// Default: 60 seconds.
    #[serde(default = "default_clock_skew_seconds")]
    pub clock_skew_seconds: u64,

    /// JWKS refresh interval in seconds.
    ///
    /// Default: 3600 seconds (1 hour).
    #[serde(default = "default_jwks_refresh_interval_secs")]
    pub jwks_refresh_interval_secs: u64,
}

impl JwtValidationConfig {
    /// Validate the JWT configuration.
    pub fn validate(&self) -> ConfigResult<()> {
        if self.jwks_url.is_none() && self.jwks_inline.is_none() {
            return Err(ConfigError::ValidationError(
                "Either jwks_url or jwks_inline must be provided for JWT validation".to_string(),
            ));
        }
        Ok(())
    }
}

fn default_clock_skew_seconds() -> u64 {
    60
}

fn default_jwks_refresh_interval_secs() -> u64 {
    3600
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
///
/// Supports per-topic overrides via the `topics` field, following Kafka's
/// two-tier configuration model where topic-level settings override defaults.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MappingConfig {
    /// Number of virtual partitions exposed to clients (default for all topics).
    pub virtual_partitions: u32,

    /// Number of physical partitions in the Kafka cluster (default for all topics).
    pub physical_partitions: u32,

    /// Offset range allocated to each virtual partition within a physical partition.
    ///
    /// Default: 2^40 (~1 trillion offsets per virtual partition).
    /// This determines how many messages can be stored per virtual partition
    /// before potential offset collision.
    #[serde(default = "default_offset_range")]
    pub offset_range: u64,

    /// Per-topic mapping overrides.
    ///
    /// Keys can be exact topic names or regex patterns (e.g., "events.*").
    /// Values override the global defaults above for matching topics.
    ///
    /// Example:
    /// ```yaml
    /// topics:
    ///   high-throughput-topic:
    ///     virtual_partitions: 128
    ///     physical_partitions: 16
    ///   "events.*":
    ///     virtual_partitions: 32
    /// ```
    #[serde(default)]
    pub topics: HashMap<String, TopicMappingConfig>,
}

/// Per-topic mapping configuration.
///
/// All fields are optional and override the global `MappingConfig` defaults.
/// This follows Kafka's `TopicConfig` pattern where topic-level settings
/// take precedence over broker-level defaults.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct TopicMappingConfig {
    /// Number of virtual partitions for this topic.
    /// If not set, uses the global `virtual_partitions` value.
    #[serde(default)]
    pub virtual_partitions: Option<u32>,

    /// Number of physical partitions for this topic.
    /// If not set, uses the global `physical_partitions` value.
    #[serde(default)]
    pub physical_partitions: Option<u32>,

    /// Offset range for this topic.
    /// If not set, uses the global `offset_range` value.
    #[serde(default)]
    pub offset_range: Option<u64>,
}

impl TopicMappingConfig {
    /// Merge this topic config with global defaults to produce a complete config.
    #[must_use]
    pub fn merge_with_defaults(&self, global: &MappingConfig) -> MappingConfig {
        MappingConfig {
            virtual_partitions: self.virtual_partitions.unwrap_or(global.virtual_partitions),
            physical_partitions: self.physical_partitions.unwrap_or(global.physical_partitions),
            offset_range: self.offset_range.unwrap_or(global.offset_range),
            topics: HashMap::new(), // Per-topic config doesn't have nested topics
        }
    }

    /// Check if this config has any overrides.
    #[must_use]
    pub fn has_overrides(&self) -> bool {
        self.virtual_partitions.is_some()
            || self.physical_partitions.is_some()
            || self.offset_range.is_some()
    }
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
            security: None,
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
        self.mapping.validate()?;

        // Validate client security configuration if present
        if let Some(security) = &self.listen.security {
            security.validate()?;
        }

        Ok(())
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
    /// - Any per-topic configuration is invalid when merged with defaults
    pub fn validate(&self) -> ConfigResult<()> {
        // Validate global defaults using specific error types
        Self::validate_global_config(
            self.virtual_partitions,
            self.physical_partitions,
            self.offset_range,
        )?;

        // Validate each per-topic configuration
        for (topic_pattern, topic_config) in &self.topics {
            let merged = topic_config.merge_with_defaults(self);
            Self::validate_topic_config(
                &merged,
                topic_pattern,
            )?;
        }

        Ok(())
    }

    /// Validate global config using specific error types for backwards compatibility.
    fn validate_global_config(
        virtual_partitions: u32,
        physical_partitions: u32,
        offset_range: u64,
    ) -> ConfigResult<()> {
        // Physical partitions must be at least 1
        if physical_partitions == 0 {
            return Err(ConfigError::InvalidPhysicalPartitions(physical_partitions));
        }

        // Virtual partitions must be >= physical partitions
        if virtual_partitions < physical_partitions {
            return Err(ConfigError::VirtualLessThanPhysical {
                virtual_partitions,
                physical_partitions,
            });
        }

        // Virtual partitions must be evenly divisible by physical partitions
        if virtual_partitions % physical_partitions != 0 {
            return Err(ConfigError::InvalidCompressionRatio {
                virtual_partitions,
                physical_partitions,
            });
        }

        // Offset range must be large enough
        const MIN_OFFSET_RANGE: u64 = 1 << 20; // 2^20 = 1,048,576
        if offset_range < MIN_OFFSET_RANGE {
            return Err(ConfigError::OffsetRangeTooSmall(offset_range));
        }

        Ok(())
    }

    /// Validate per-topic config using ValidationError with topic context.
    fn validate_topic_config(config: &MappingConfig, topic: &str) -> ConfigResult<()> {
        // Physical partitions must be at least 1
        if config.physical_partitions == 0 {
            return Err(ConfigError::ValidationError(format!(
                "physical_partitions must be at least 1 for topic '{topic}'"
            )));
        }

        // Virtual partitions must be >= physical partitions
        if config.virtual_partitions < config.physical_partitions {
            return Err(ConfigError::ValidationError(format!(
                "virtual_partitions ({}) must be >= physical_partitions ({}) for topic '{topic}'",
                config.virtual_partitions, config.physical_partitions
            )));
        }

        // Virtual partitions must be evenly divisible by physical partitions
        if config.virtual_partitions % config.physical_partitions != 0 {
            return Err(ConfigError::ValidationError(format!(
                "virtual_partitions ({}) must be evenly divisible by physical_partitions ({}) for topic '{topic}'",
                config.virtual_partitions, config.physical_partitions
            )));
        }

        // Offset range must be large enough
        const MIN_OFFSET_RANGE: u64 = 1 << 20; // 2^20 = 1,048,576
        if config.offset_range < MIN_OFFSET_RANGE {
            return Err(ConfigError::ValidationError(format!(
                "offset_range ({}) must be at least {MIN_OFFSET_RANGE} for topic '{topic}'",
                config.offset_range
            )));
        }

        Ok(())
    }

    /// Calculate the compression ratio (virtual / physical).
    #[must_use]
    pub fn compression_ratio(&self) -> u32 {
        self.virtual_partitions / self.physical_partitions
    }

    /// Get the effective mapping config for a specific topic.
    ///
    /// Checks for exact match first, then regex patterns.
    /// Returns the merged config if a match is found, otherwise returns global defaults.
    #[must_use]
    pub fn get_topic_config(&self, topic: &str) -> MappingConfig {
        // Check for exact topic match first
        if let Some(topic_config) = self.topics.get(topic) {
            return topic_config.merge_with_defaults(self);
        }

        // Check regex patterns
        for (pattern, topic_config) in &self.topics {
            // Skip non-regex patterns (already checked above)
            if !pattern.contains('*') && !pattern.contains('?') && !pattern.contains('[') {
                continue;
            }

            // Try to compile as regex
            if let Ok(re) = Regex::new(&format!("^{pattern}$")) {
                if re.is_match(topic) {
                    return topic_config.merge_with_defaults(self);
                }
            }
        }

        // No match, return global defaults
        MappingConfig {
            virtual_partitions: self.virtual_partitions,
            physical_partitions: self.physical_partitions,
            offset_range: self.offset_range,
            topics: HashMap::new(),
        }
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
                topics: HashMap::new(),
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
        assert_eq!(tls.ca_cert_path, Some(PathBuf::from("/etc/ssl/ca.crt")));
        assert_eq!(tls.cert_path, Some(PathBuf::from("/etc/ssl/client.crt")));
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

    // ==========================================================================
    // Client Security Configuration Tests
    // ==========================================================================

    #[test]
    fn test_client_security_config_default() {
        let config = ClientSecurityConfig::default();
        assert_eq!(config.protocol, SecurityProtocol::Plaintext);
        assert!(config.tls.is_none());
        assert!(config.sasl.is_none());
    }

    #[test]
    fn test_client_security_config_plaintext_validates() {
        let config = ClientSecurityConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_client_security_config_ssl_requires_tls() {
        let config = ClientSecurityConfig {
            protocol: SecurityProtocol::Ssl,
            tls: None,
            sasl: None,
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("TLS configuration required"));
    }

    #[test]
    fn test_client_security_config_sasl_requires_sasl_config() {
        let config = ClientSecurityConfig {
            protocol: SecurityProtocol::SaslPlaintext,
            tls: None,
            sasl: None,
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("SASL configuration required"));
    }

    #[test]
    fn test_client_sasl_config_enabled_mechanisms() {
        let config = ClientSaslConfig {
            enabled_mechanisms: vec![SaslMechanism::Plain, SaslMechanism::ScramSha256],
            credentials: CredentialConfig::Inline {
                users: vec![UserCredential {
                    username: "test".to_string(),
                    password: "pass".to_string(),
                }],
            },
            oauthbearer: None,
        };
        assert!(config.is_mechanism_enabled(SaslMechanism::Plain));
        assert!(config.is_mechanism_enabled(SaslMechanism::ScramSha256));
        assert!(!config.is_mechanism_enabled(SaslMechanism::ScramSha512));
    }

    #[test]
    fn test_client_sasl_config_mechanism_names() {
        let config = ClientSaslConfig {
            enabled_mechanisms: vec![
                SaslMechanism::Plain,
                SaslMechanism::ScramSha256,
                SaslMechanism::ScramSha512,
            ],
            credentials: CredentialConfig::Inline {
                users: vec![UserCredential {
                    username: "test".to_string(),
                    password: "pass".to_string(),
                }],
            },
            oauthbearer: None,
        };
        let names = config.enabled_mechanism_names();
        assert_eq!(names, vec!["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]);
    }

    #[test]
    fn test_credential_config_inline_validation() {
        let config = CredentialConfig::Inline {
            users: vec![UserCredential {
                username: "user1".to_string(),
                password: "pass1".to_string(),
            }],
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_credential_config_inline_empty_fails() {
        let config = CredentialConfig::Inline { users: vec![] };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("At least one user credential"));
    }

    #[test]
    fn test_credential_config_inline_empty_username_fails() {
        let config = CredentialConfig::Inline {
            users: vec![UserCredential {
                username: "".to_string(),
                password: "pass".to_string(),
            }],
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Username cannot be empty"));
    }

    #[test]
    fn test_user_credential_env_expansion() {
        std::env::set_var("TEST_CLIENT_PASS", "secret123");
        let cred = UserCredential {
            username: "user".to_string(),
            password: "${TEST_CLIENT_PASS}".to_string(),
        };
        assert_eq!(cred.password(), "secret123");
        std::env::remove_var("TEST_CLIENT_PASS");
    }

    #[test]
    fn test_client_security_config_yaml_parsing() {
        let yaml = r"
listen:
  address: '0.0.0.0:9092'
  security:
    protocol: SASL_PLAINTEXT
    sasl:
      enabled_mechanisms:
        - PLAIN
        - SCRAM-SHA-256
      credentials:
        users:
          - username: 'client1'
            password: 'password1'
          - username: 'client2'
            password: 'password2'
kafka:
  bootstrap_servers:
    - 'localhost:9092'
mapping:
  virtual_partitions: 100
  physical_partitions: 10
";
        let config = ProxyConfig::from_str(yaml).unwrap();
        let security = config.listen.security.unwrap();
        assert_eq!(security.protocol, SecurityProtocol::SaslPlaintext);
        assert!(security.tls.is_none());
        let sasl = security.sasl.unwrap();
        assert_eq!(sasl.enabled_mechanisms.len(), 2);
        assert!(sasl.is_mechanism_enabled(SaslMechanism::Plain));
        assert!(sasl.is_mechanism_enabled(SaslMechanism::ScramSha256));
    }

    #[test]
    fn test_listen_config_with_security_default() {
        let config = ListenConfig::default();
        assert!(config.security.is_none());
    }
}
