//! Connection context carrying authentication and metadata.
//!
//! This module provides the `ConnectionContext` struct which carries
//! authentication information and connection metadata throughout the
//! request handling pipeline, following Apache Kafka's `RequestContext` pattern.
//!
//! # Overview
//!
//! The connection context is created when a client connects and carries:
//! - The authenticated principal (from TLS certificate or SASL)
//! - Client's remote address
//! - Connection identifier for logging/tracing
//! - Security protocol in use
//!
//! # Principal Lifecycle
//!
//! 1. **Initial**: Anonymous or extracted from TLS certificate (mTLS)
//! 2. **After SASL**: Upgraded with SASL username (auth method changes)
//!
//! # Example
//!
//! ```
//! use std::net::SocketAddr;
//! use kafka_remapper_core::auth::{Principal, AuthMethod};
//! use kafka_remapper_core::network::ConnectionContext;
//! use kafka_remapper_core::config::SecurityProtocol;
//!
//! // Create context for mTLS connection
//! let addr: SocketAddr = "192.168.1.100:45678".parse().unwrap();
//! let principal = Principal::new("kafka-client", AuthMethod::Ssl);
//! let context = ConnectionContext::new(
//!     principal,
//!     addr,
//!     "conn-1".to_string(),
//!     SecurityProtocol::Ssl,
//! );
//!
//! assert_eq!(context.principal().name, "kafka-client");
//! assert_eq!(context.client_address(), addr);
//! ```

use std::net::SocketAddr;

use crate::auth::{AuthMethod, Principal};
use crate::config::SecurityProtocol;

/// Context for a client connection.
///
/// Carries authentication information and connection metadata throughout
/// the request handling pipeline. Follows Apache Kafka's `RequestContext` pattern.
///
/// # Thread Safety
///
/// `ConnectionContext` is `Clone` and designed to be passed through async tasks.
/// The principal is set during connection establishment and can be upgraded
/// once during SASL authentication.
///
/// # Immutability
///
/// Following Kafka's design, the context is treated as immutable after creation.
/// The `with_sasl_principal()` method creates a new context rather than mutating.
#[derive(Debug, Clone)]
pub struct ConnectionContext {
    /// The authenticated principal for this connection.
    principal: Principal,
    /// Client's remote address.
    client_address: SocketAddr,
    /// Connection identifier (for logging/tracing).
    connection_id: String,
    /// Security protocol in use.
    security_protocol: SecurityProtocol,
}

impl ConnectionContext {
    /// Create a new connection context.
    ///
    /// # Arguments
    ///
    /// * `principal` - The authenticated principal
    /// * `client_address` - Client's remote socket address
    /// * `connection_id` - Unique identifier for this connection
    /// * `security_protocol` - Security protocol being used
    pub fn new(
        principal: Principal,
        client_address: SocketAddr,
        connection_id: String,
        security_protocol: SecurityProtocol,
    ) -> Self {
        Self {
            principal,
            client_address,
            connection_id,
            security_protocol,
        }
    }

    /// Create an anonymous context for unauthenticated connections.
    ///
    /// Used for PLAINTEXT connections where no authentication is configured.
    pub fn anonymous(client_address: SocketAddr, connection_id: String) -> Self {
        Self {
            principal: Principal::anonymous(),
            client_address,
            connection_id,
            security_protocol: SecurityProtocol::Plaintext,
        }
    }

    /// Get the authenticated principal.
    pub fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Get the client's remote address.
    pub fn client_address(&self) -> SocketAddr {
        self.client_address
    }

    /// Get the connection identifier.
    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }

    /// Get the security protocol.
    pub fn security_protocol(&self) -> SecurityProtocol {
        self.security_protocol
    }

    /// Check if this connection is authenticated (not anonymous).
    pub fn is_authenticated(&self) -> bool {
        !self.principal.is_anonymous()
    }

    /// Upgrade the principal after SASL authentication.
    ///
    /// This consumes the context and returns a new one with the upgraded
    /// principal. The auth method is updated to reflect both SSL and SASL
    /// authentication if applicable:
    ///
    /// - `Anonymous` → `Sasl`
    /// - `Ssl` → `SaslSsl`
    /// - `Sasl` → `Sasl` (username updated)
    /// - `SaslSsl` → `SaslSsl` (username updated)
    ///
    /// # Arguments
    ///
    /// * `sasl_username` - The username from SASL authentication
    ///
    /// # Example
    ///
    /// ```
    /// use std::net::SocketAddr;
    /// use kafka_remapper_core::auth::{Principal, AuthMethod};
    /// use kafka_remapper_core::network::ConnectionContext;
    /// use kafka_remapper_core::config::SecurityProtocol;
    ///
    /// let addr: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    /// let ssl_principal = Principal::new("cert-user", AuthMethod::Ssl);
    /// let context = ConnectionContext::new(
    ///     ssl_principal,
    ///     addr,
    ///     "conn-1".to_string(),
    ///     SecurityProtocol::SaslSsl,
    /// );
    ///
    /// // After SASL authentication completes
    /// let upgraded = context.with_sasl_principal("sasl-user".to_string());
    /// assert_eq!(upgraded.principal().name, "sasl-user");
    /// assert_eq!(upgraded.principal().auth_method, AuthMethod::SaslSsl);
    /// ```
    pub fn with_sasl_principal(self, sasl_username: String) -> Self {
        Self {
            principal: self.principal.with_sasl_auth(sasl_username),
            ..self
        }
    }
}

impl std::fmt::Display for ConnectionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConnectionContext(principal={}, client={}, protocol={:?})",
            self.principal, self.client_address, self.security_protocol
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_addr() -> SocketAddr {
        "192.168.1.100:45678".parse().unwrap()
    }

    #[test]
    fn test_context_new() {
        let principal = Principal::new("test-user", AuthMethod::Ssl);
        let context = ConnectionContext::new(
            principal,
            test_addr(),
            "conn-123".to_string(),
            SecurityProtocol::Ssl,
        );

        assert_eq!(context.principal().name, "test-user");
        assert_eq!(context.principal().auth_method, AuthMethod::Ssl);
        assert_eq!(context.client_address(), test_addr());
        assert_eq!(context.connection_id(), "conn-123");
        assert_eq!(context.security_protocol(), SecurityProtocol::Ssl);
    }

    #[test]
    fn test_context_anonymous() {
        let context = ConnectionContext::anonymous(test_addr(), "conn-456".to_string());

        assert!(context.principal().is_anonymous());
        assert_eq!(context.security_protocol(), SecurityProtocol::Plaintext);
        assert!(!context.is_authenticated());
    }

    #[test]
    fn test_context_is_authenticated() {
        let authenticated = ConnectionContext::new(
            Principal::new("user", AuthMethod::Sasl),
            test_addr(),
            "conn-1".to_string(),
            SecurityProtocol::SaslPlaintext,
        );
        assert!(authenticated.is_authenticated());

        let anonymous = ConnectionContext::anonymous(test_addr(), "conn-2".to_string());
        assert!(!anonymous.is_authenticated());
    }

    #[test]
    fn test_context_upgrade_from_ssl_to_sasl_ssl() {
        let ssl_principal = Principal::new("cert-user", AuthMethod::Ssl);
        let context = ConnectionContext::new(
            ssl_principal,
            test_addr(),
            "conn-789".to_string(),
            SecurityProtocol::SaslSsl,
        );

        let upgraded = context.with_sasl_principal("sasl-user".to_string());

        assert_eq!(upgraded.principal().name, "sasl-user");
        assert_eq!(upgraded.principal().auth_method, AuthMethod::SaslSsl);
        // Other fields preserved
        assert_eq!(upgraded.client_address(), test_addr());
        assert_eq!(upgraded.connection_id(), "conn-789");
        assert_eq!(upgraded.security_protocol(), SecurityProtocol::SaslSsl);
    }

    #[test]
    fn test_context_upgrade_from_anonymous_to_sasl() {
        let context = ConnectionContext::new(
            Principal::anonymous(),
            test_addr(),
            "conn-111".to_string(),
            SecurityProtocol::SaslPlaintext,
        );

        let upgraded = context.with_sasl_principal("alice".to_string());

        assert_eq!(upgraded.principal().name, "alice");
        assert_eq!(upgraded.principal().auth_method, AuthMethod::Sasl);
    }

    #[test]
    fn test_context_display() {
        let context = ConnectionContext::new(
            Principal::new("kafka-client", AuthMethod::Ssl),
            test_addr(),
            "conn-display".to_string(),
            SecurityProtocol::Ssl,
        );

        let display = format!("{}", context);
        assert!(display.contains("kafka-client"));
        assert!(display.contains("192.168.1.100"));
        assert!(display.contains("Ssl"));
    }

    #[test]
    fn test_context_clone() {
        let context = ConnectionContext::new(
            Principal::new("user", AuthMethod::Sasl),
            test_addr(),
            "conn-clone".to_string(),
            SecurityProtocol::SaslPlaintext,
        );

        let cloned = context.clone();
        assert_eq!(cloned.principal().name, context.principal().name);
        assert_eq!(cloned.connection_id(), context.connection_id());
    }
}
