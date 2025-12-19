//! Client authentication module.
//!
//! This module provides authentication for client connections to the proxy.
//! It supports:
//! - TLS termination (configured via the `tls` module)
//! - SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER)
//! - Principal extraction from X.509 certificates (mTLS)
//!
//! # Security Protocols
//!
//! The proxy supports four security protocols matching Kafka's security configuration:
//! - `PLAINTEXT` - No encryption or authentication
//! - `SSL` - TLS encryption only
//! - `SASL_PLAINTEXT` - SASL authentication without encryption
//! - `SASL_SSL` - Both TLS encryption and SASL authentication (recommended)
//!
//! # Principal Extraction
//!
//! For mTLS connections, the proxy can extract a principal identity from the
//! client's X.509 certificate using configurable mapping rules. This follows
//! Apache Kafka's `ssl.principal.mapping.rules` pattern.
//!
//! ```yaml
//! listen:
//!   security:
//!     protocol: SSL
//!     tls:
//!       cert_path: "/etc/ssl/proxy.crt"
//!       key_path: "/etc/ssl/proxy.key"
//!       ca_cert_path: "/etc/ssl/ca.crt"
//!       require_client_cert: true
//!       principal:
//!         # Extract CN and lowercase it
//!         mapping_rules: "RULE:^CN=([^,]+),.*$/$1/L,DEFAULT"
//! ```
//!
//! # Example Configuration
//!
//! ```yaml
//! listen:
//!   address: "0.0.0.0:9092"
//!   security:
//!     protocol: SASL_SSL
//!     tls:
//!       cert_path: "/etc/ssl/proxy.crt"
//!       key_path: "/etc/ssl/proxy.key"
//!     sasl:
//!       enabled_mechanisms: [PLAIN, SCRAM-SHA-256, OAUTHBEARER]
//!       credentials:
//!         users:
//!           - username: "client1"
//!             password: "secret"
//!       oauthbearer:
//!         validation: none  # passthrough mode
//! ```

pub mod principal;
pub mod sasl;
pub mod ssl_principal_mapper;

// Re-export principal types
pub use principal::{
    AuthMethod, AuthenticationContext, DefaultPrincipalBuilder, Principal, PrincipalBuilder,
    PrincipalError, SaslAuthenticationContext, SslAuthenticationContext,
};
pub use ssl_principal_mapper::{CaseTransform, MapperError, MappingRule, SslPrincipalMapper};

// Re-export SASL types
pub use sasl::{
    create_credential_store, CredentialStore, InMemoryCredentialStore, OAuthBearerAuthenticator,
    PlainAuthenticator, SaslAuthenticator, SaslServer, SaslSession, SaslStepResult, ScramHash,
    ScramSha256, ScramSha512, StoredCredentials, TokenValidator,
};
