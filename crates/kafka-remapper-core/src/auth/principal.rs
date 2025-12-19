//! Principal representation for authenticated identities.
//!
//! This module provides types for representing authenticated principals,
//! following Apache Kafka's `KafkaPrincipal` design pattern.
//!
//! # Overview
//!
//! A principal represents an authenticated identity with:
//! - A type (usually "User")
//! - A name (username, certificate CN, etc.)
//! - The authentication method used
//!
//! # Example
//!
//! ```
//! use kafka_remapper_core::auth::{Principal, AuthMethod};
//!
//! // Create a principal from SASL authentication
//! let principal = Principal::new("alice", AuthMethod::Sasl);
//! assert_eq!(principal.to_string(), "User:alice");
//!
//! // Create an anonymous principal
//! let anon = Principal::anonymous();
//! assert!(anon.is_anonymous());
//! ```

use std::fmt;
use std::net::SocketAddr;

use rustls::pki_types::CertificateDer;

use super::ssl_principal_mapper::{MapperError, SslPrincipalMapper};

/// Type of principal - matches Kafka's `KafkaPrincipal.USER_TYPE`.
pub const USER_TYPE: &str = "User";

/// Represents an authenticated principal.
///
/// Follows Apache Kafka's `KafkaPrincipal` design:
/// - `principal_type`: Always "User" for standard auth
/// - `name`: The identity name (username, CN, etc.)
/// - `auth_method`: How the principal was authenticated
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Principal {
    /// The type of principal (usually "User").
    pub principal_type: String,
    /// The name/identity of the principal.
    pub name: String,
    /// How this principal was authenticated.
    pub auth_method: AuthMethod,
}

/// How the principal was authenticated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthMethod {
    /// No authentication (PLAINTEXT connections).
    Anonymous,
    /// TLS with client certificate (mTLS).
    Ssl,
    /// SASL authentication.
    Sasl,
    /// Both TLS certificate and SASL authentication.
    SaslSsl,
}

impl Principal {
    /// Create a new principal with the given name and authentication method.
    pub fn new(name: impl Into<String>, auth_method: AuthMethod) -> Self {
        Self {
            principal_type: USER_TYPE.to_string(),
            name: name.into(),
            auth_method,
        }
    }

    /// Create an anonymous principal for unauthenticated connections.
    pub fn anonymous() -> Self {
        Self {
            principal_type: USER_TYPE.to_string(),
            name: "ANONYMOUS".to_string(),
            auth_method: AuthMethod::Anonymous,
        }
    }

    /// Check if this is an anonymous principal.
    pub fn is_anonymous(&self) -> bool {
        self.name == "ANONYMOUS" && matches!(self.auth_method, AuthMethod::Anonymous)
    }

    /// Upgrade this principal with SASL authentication.
    ///
    /// If the principal was authenticated via SSL (mTLS), this upgrades it to
    /// `SaslSsl`. If it was anonymous, it becomes `Sasl` authenticated.
    pub fn with_sasl_auth(self, sasl_username: impl Into<String>) -> Self {
        let new_method = match self.auth_method {
            AuthMethod::Ssl => AuthMethod::SaslSsl,
            _ => AuthMethod::Sasl,
        };
        Self {
            principal_type: self.principal_type,
            name: sasl_username.into(),
            auth_method: new_method,
        }
    }
}

impl fmt::Display for Principal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.principal_type, self.name)
    }
}

impl Default for Principal {
    fn default() -> Self {
        Self::anonymous()
    }
}

/// Context for building principals from SSL/TLS authentication.
pub struct SslAuthenticationContext<'a> {
    /// Peer certificate chain (first cert is peer's).
    pub peer_certificates: &'a [CertificateDer<'a>],
    /// Client's IP address.
    pub client_address: SocketAddr,
}

/// Context for building principals from SASL authentication.
pub struct SaslAuthenticationContext<'a> {
    /// SASL mechanism used.
    pub mechanism: &'a str,
    /// Authenticated username.
    pub username: &'a str,
    /// Client's IP address.
    pub client_address: SocketAddr,
}

/// Context for building principals.
pub enum AuthenticationContext<'a> {
    /// SSL/TLS authentication context.
    Ssl(SslAuthenticationContext<'a>),
    /// SASL authentication context.
    Sasl(SaslAuthenticationContext<'a>),
}

/// Builder for extracting principals from authentication context.
///
/// Follows Apache Kafka's `KafkaPrincipalBuilder` interface.
pub trait PrincipalBuilder: Send + Sync {
    /// Build a principal from the authentication context.
    fn build(&self, context: AuthenticationContext<'_>) -> Result<Principal, PrincipalError>;
}

/// Default implementation of `PrincipalBuilder`.
///
/// Follows Apache Kafka's `DefaultKafkaPrincipalBuilder`:
/// - For SSL: Extract Subject DN from certificate, apply `SslPrincipalMapper`
/// - For SASL: Use authenticated username directly
pub struct DefaultPrincipalBuilder {
    ssl_mapper: SslPrincipalMapper,
}

impl DefaultPrincipalBuilder {
    /// Create with default rules (return full DN).
    pub fn new() -> Self {
        Self {
            ssl_mapper: SslPrincipalMapper::default(),
        }
    }

    /// Create with custom SSL mapping rules.
    ///
    /// # Errors
    ///
    /// Returns an error if the rules string is invalid.
    pub fn with_ssl_rules(rules: &str) -> Result<Self, MapperError> {
        Ok(Self {
            ssl_mapper: SslPrincipalMapper::from_rules(rules)?,
        })
    }

    /// Extract principal from SSL context.
    fn build_ssl_principal(
        &self,
        ctx: &SslAuthenticationContext<'_>,
    ) -> Result<Principal, PrincipalError> {
        if ctx.peer_certificates.is_empty() {
            return Ok(Principal::anonymous());
        }

        // First certificate is the peer's certificate
        let cert_der = &ctx.peer_certificates[0];

        // Parse the X.509 certificate
        let (_, cert) = x509_parser::parse_x509_certificate(cert_der.as_ref())
            .map_err(|e| PrincipalError::CertificateParseError(e.to_string()))?;

        // Get the Subject DN as a string
        let subject_dn = cert.subject().to_string();

        // Apply the mapping rules
        let name = self
            .ssl_mapper
            .get_name(&subject_dn)
            .map_err(|e| PrincipalError::MappingError(e.to_string()))?;

        Ok(Principal::new(name, AuthMethod::Ssl))
    }

    /// Extract principal from SASL context.
    fn build_sasl_principal(
        &self,
        ctx: &SaslAuthenticationContext<'_>,
    ) -> Result<Principal, PrincipalError> {
        Ok(Principal::new(ctx.username, AuthMethod::Sasl))
    }
}

impl Default for DefaultPrincipalBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PrincipalBuilder for DefaultPrincipalBuilder {
    fn build(&self, context: AuthenticationContext<'_>) -> Result<Principal, PrincipalError> {
        match context {
            AuthenticationContext::Ssl(ssl_ctx) => self.build_ssl_principal(&ssl_ctx),
            AuthenticationContext::Sasl(sasl_ctx) => self.build_sasl_principal(&sasl_ctx),
        }
    }
}

/// Errors from principal extraction.
#[derive(Debug, Clone, thiserror::Error)]
pub enum PrincipalError {
    /// Failed to parse the X.509 certificate.
    #[error("failed to parse certificate: {0}")]
    CertificateParseError(String),
    /// Failed to map the DN to a principal name.
    #[error("failed to map principal: {0}")]
    MappingError(String),
    /// No peer certificate available.
    #[error("no peer certificate available")]
    NoCertificate,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_principal_new() {
        let principal = Principal::new("alice", AuthMethod::Sasl);
        assert_eq!(principal.principal_type, "User");
        assert_eq!(principal.name, "alice");
        assert_eq!(principal.auth_method, AuthMethod::Sasl);
    }

    #[test]
    fn test_principal_anonymous() {
        let principal = Principal::anonymous();
        assert_eq!(principal.principal_type, "User");
        assert_eq!(principal.name, "ANONYMOUS");
        assert_eq!(principal.auth_method, AuthMethod::Anonymous);
        assert!(principal.is_anonymous());
    }

    #[test]
    fn test_principal_not_anonymous() {
        let principal = Principal::new("alice", AuthMethod::Sasl);
        assert!(!principal.is_anonymous());
    }

    #[test]
    fn test_principal_display() {
        let principal = Principal::new("kafka-client", AuthMethod::Ssl);
        assert_eq!(principal.to_string(), "User:kafka-client");
    }

    #[test]
    fn test_principal_with_sasl_auth_from_ssl() {
        let principal = Principal::new("cert-user", AuthMethod::Ssl);
        let upgraded = principal.with_sasl_auth("sasl-user");
        assert_eq!(upgraded.name, "sasl-user");
        assert_eq!(upgraded.auth_method, AuthMethod::SaslSsl);
    }

    #[test]
    fn test_principal_with_sasl_auth_from_anonymous() {
        let principal = Principal::anonymous();
        let upgraded = principal.with_sasl_auth("alice");
        assert_eq!(upgraded.name, "alice");
        assert_eq!(upgraded.auth_method, AuthMethod::Sasl);
    }

    #[test]
    fn test_principal_equality() {
        let p1 = Principal::new("alice", AuthMethod::Sasl);
        let p2 = Principal::new("alice", AuthMethod::Sasl);
        let p3 = Principal::new("bob", AuthMethod::Sasl);
        let p4 = Principal::new("alice", AuthMethod::Ssl);

        assert_eq!(p1, p2);
        assert_ne!(p1, p3);
        assert_ne!(p1, p4);
    }

    #[test]
    fn test_principal_default() {
        let principal = Principal::default();
        assert!(principal.is_anonymous());
    }

    #[test]
    fn test_default_principal_builder_new() {
        let builder = DefaultPrincipalBuilder::new();
        // Just verify it creates without panic
        assert!(builder.ssl_mapper.get_name("CN=test").is_ok());
    }

    #[test]
    fn test_default_principal_builder_with_rules() {
        let builder =
            DefaultPrincipalBuilder::with_ssl_rules("RULE:^CN=([^,]+).*$/$1/,DEFAULT").unwrap();
        assert!(builder.ssl_mapper.get_name("CN=test,O=Org").is_ok());
    }

    #[test]
    fn test_sasl_context_builds_principal() {
        let builder = DefaultPrincipalBuilder::new();
        let addr: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = SaslAuthenticationContext {
            mechanism: "PLAIN",
            username: "alice",
            client_address: addr,
        };
        let principal = builder.build(AuthenticationContext::Sasl(ctx)).unwrap();
        assert_eq!(principal.name, "alice");
        assert_eq!(principal.auth_method, AuthMethod::Sasl);
    }
}
