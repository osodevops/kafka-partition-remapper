//! SASL authentication implementations.
//!
//! This module provides server-side SASL authentication for client connections.
//! Supported mechanisms:
//! - PLAIN - Simple username/password (should only be used over TLS)
//! - SCRAM-SHA-256 - Challenge-response authentication
//! - SCRAM-SHA-512 - Challenge-response authentication
//! - OAUTHBEARER - OAuth 2.0 bearer token authentication

pub mod credentials;
#[cfg(feature = "oauthbearer-jwt")]
pub mod jwt;
pub mod oauthbearer;
pub mod plain;
pub mod scram;

pub use credentials::{
    create_credential_store, CredentialStore, InMemoryCredentialStore, StoredCredentials,
};
pub use oauthbearer::{
    OAuthBearerAuthenticator, OAuthBearerClientMessage, TokenValidationResult, TokenValidator,
};
pub use plain::PlainAuthenticator;
pub use scram::{
    generate_scram_credentials, generate_scram_credentials_with_salt, ScramCredentials, ScramHash,
    ScramSessionState, ScramSha256, ScramSha256Authenticator, ScramSha512,
    ScramSha512Authenticator,
};

use std::sync::Arc;

use crate::config::{ClientSaslConfig, SaslMechanism};
use crate::error::{AuthError, Result};

/// SASL session state.
#[derive(Debug, Default)]
pub struct SaslSession {
    /// The mechanism being used.
    pub mechanism: Option<String>,
    /// The authenticated username (set when authentication completes).
    pub authenticated_user: Option<String>,
    /// Number of authentication steps completed.
    pub step_count: usize,
    /// SCRAM-specific session state for multi-step authentication.
    pub scram_state: Option<ScramSessionState>,
}

impl SaslSession {
    /// Create a new SASL session.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the mechanism being used.
    pub fn set_mechanism(&mut self, mechanism: &str) {
        self.mechanism = Some(mechanism.to_string());
    }

    /// Check if authentication is complete.
    #[must_use]
    pub fn is_authenticated(&self) -> bool {
        self.authenticated_user.is_some()
    }
}

/// Result of a SASL authentication step.
#[derive(Debug)]
pub enum SaslStepResult {
    /// Authentication completed successfully, with optional final response.
    Complete(Vec<u8>),
    /// More steps required, send this challenge to client.
    Continue(Vec<u8>),
    /// Authentication failed.
    Failed(AuthError),
}

/// Trait for SASL authenticators.
pub trait SaslAuthenticator: Send + Sync + std::fmt::Debug {
    /// Get the mechanism name (e.g., "PLAIN", "SCRAM-SHA-256").
    fn mechanism_name(&self) -> &'static str;

    /// Process a client authentication message.
    fn authenticate_step(&self, client_message: &[u8], session: &mut SaslSession)
        -> SaslStepResult;

    /// Check if authentication is complete for this session.
    fn is_complete(&self, session: &SaslSession) -> bool;
}

/// SASL server that manages multiple authenticators.
#[derive(Debug)]
pub struct SaslServer {
    authenticators: Vec<Arc<dyn SaslAuthenticator>>,
}

impl SaslServer {
    /// Create a new SASL server from configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the credential store cannot be created.
    pub fn new(config: &ClientSaslConfig) -> Result<Self> {
        let credential_store = create_credential_store(&config.credentials)?;
        let mut authenticators: Vec<Arc<dyn SaslAuthenticator>> = Vec::new();

        for mechanism in &config.enabled_mechanisms {
            match mechanism {
                SaslMechanism::Plain => {
                    authenticators.push(Arc::new(PlainAuthenticator::new(Arc::clone(
                        &credential_store,
                    ))));
                }
                SaslMechanism::ScramSha256 => {
                    authenticators.push(Arc::new(ScramSha256Authenticator::new(Arc::clone(
                        &credential_store,
                    ))));
                }
                SaslMechanism::ScramSha512 => {
                    authenticators.push(Arc::new(ScramSha512Authenticator::new(Arc::clone(
                        &credential_store,
                    ))));
                }
                SaslMechanism::OAuthBearer => {
                    let oauth_auth =
                        OAuthBearerAuthenticator::from_config(config.oauthbearer.as_ref())?;
                    authenticators.push(Arc::new(oauth_auth));
                }
            }
        }

        if authenticators.is_empty() {
            return Err(AuthError::UnsupportedMechanism(
                "No supported SASL mechanisms configured".to_string(),
            )
            .into());
        }

        Ok(Self { authenticators })
    }

    /// Get the list of enabled mechanism names.
    #[must_use]
    pub fn enabled_mechanisms(&self) -> Vec<&'static str> {
        self.authenticators
            .iter()
            .map(|a| a.mechanism_name())
            .collect()
    }

    /// Get an authenticator for the specified mechanism.
    #[must_use]
    pub fn get_authenticator(&self, mechanism: &str) -> Option<Arc<dyn SaslAuthenticator>> {
        self.authenticators
            .iter()
            .find(|a| a.mechanism_name().eq_ignore_ascii_case(mechanism))
            .cloned()
    }

    /// Start a new authentication session for a mechanism.
    ///
    /// # Errors
    ///
    /// Returns an error if the mechanism is not supported.
    pub fn start_session(
        &self,
        mechanism: &str,
    ) -> Result<(SaslSession, Arc<dyn SaslAuthenticator>)> {
        let authenticator = self
            .get_authenticator(mechanism)
            .ok_or_else(|| AuthError::UnsupportedMechanism(mechanism.to_string()))?;

        let mut session = SaslSession::new();
        session.set_mechanism(mechanism);

        Ok((session, authenticator))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CredentialConfig, UserCredential};

    fn test_config() -> ClientSaslConfig {
        ClientSaslConfig {
            enabled_mechanisms: vec![SaslMechanism::Plain],
            credentials: CredentialConfig::Inline {
                users: vec![UserCredential {
                    username: "test_user".to_string(),
                    password: "test_pass".to_string(),
                }],
            },
            oauthbearer: None,
        }
    }

    #[test]
    fn test_sasl_server_creation() {
        let config = test_config();
        let server = SaslServer::new(&config).unwrap();
        assert_eq!(server.enabled_mechanisms(), vec!["PLAIN"]);
    }

    #[test]
    fn test_get_authenticator() {
        let config = test_config();
        let server = SaslServer::new(&config).unwrap();

        assert!(server.get_authenticator("PLAIN").is_some());
        assert!(server.get_authenticator("plain").is_some()); // Case insensitive
        assert!(server.get_authenticator("SCRAM-SHA-256").is_none());
    }

    #[test]
    fn test_start_session() {
        let config = test_config();
        let server = SaslServer::new(&config).unwrap();

        let (session, auth) = server.start_session("PLAIN").unwrap();
        assert_eq!(session.mechanism, Some("PLAIN".to_string()));
        assert_eq!(auth.mechanism_name(), "PLAIN");
    }

    #[test]
    fn test_start_session_unsupported() {
        let config = test_config();
        let server = SaslServer::new(&config).unwrap();

        let result = server.start_session("UNKNOWN");
        assert!(result.is_err());
    }

    #[test]
    fn test_full_authentication_flow() {
        let config = test_config();
        let server = SaslServer::new(&config).unwrap();

        let (mut session, auth) = server.start_session("PLAIN").unwrap();

        // Authenticate with valid credentials
        let message = b"\0test_user\0test_pass";
        let result = auth.authenticate_step(message, &mut session);

        assert!(matches!(result, SaslStepResult::Complete(_)));
        assert!(session.is_authenticated());
        assert_eq!(session.authenticated_user, Some("test_user".to_string()));
    }
}
