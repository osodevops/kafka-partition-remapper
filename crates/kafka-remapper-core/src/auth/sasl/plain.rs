//! SASL/PLAIN authentication implementation.
//!
//! SASL/PLAIN transmits credentials as cleartext (should only be used over TLS).
//! The authentication message format is: `\0username\0password`

use std::sync::Arc;

use tracing::{debug, warn};

use crate::error::{AuthError, Result};

use super::credentials::CredentialStore;
use super::{SaslAuthenticator, SaslSession, SaslStepResult};

/// SASL/PLAIN authenticator.
#[derive(Debug)]
pub struct PlainAuthenticator {
    credential_store: Arc<dyn CredentialStore>,
}

impl PlainAuthenticator {
    /// Create a new PLAIN authenticator.
    #[must_use]
    pub fn new(credential_store: Arc<dyn CredentialStore>) -> Self {
        Self { credential_store }
    }
}

impl SaslAuthenticator for PlainAuthenticator {
    fn mechanism_name(&self) -> &'static str {
        "PLAIN"
    }

    fn authenticate_step(
        &self,
        client_message: &[u8],
        session: &mut SaslSession,
    ) -> SaslStepResult {
        // PLAIN format: \0username\0password
        // The message may optionally start with an authorization identity: authzid\0authcid\0password
        // We ignore the authzid if present.

        let parts: Vec<&[u8]> = client_message.split(|&b| b == 0).collect();

        let (username, password) = match parts.len() {
            // Format: \0username\0password (3 parts, first is empty)
            3 if parts[0].is_empty() => {
                let username = String::from_utf8_lossy(parts[1]).to_string();
                let password = String::from_utf8_lossy(parts[2]).to_string();
                (username, password)
            }
            // Format: authzid\0username\0password (3 parts, first is authzid)
            3 => {
                let username = String::from_utf8_lossy(parts[1]).to_string();
                let password = String::from_utf8_lossy(parts[2]).to_string();
                (username, password)
            }
            _ => {
                warn!(
                    "Invalid PLAIN message format: expected 3 parts, got {}",
                    parts.len()
                );
                return SaslStepResult::Failed(AuthError::InvalidCredentials);
            }
        };

        debug!(username = %username, "PLAIN authentication attempt");

        if self.credential_store.validate(&username, &password) {
            session.authenticated_user = Some(username.clone());
            debug!(username = %username, "PLAIN authentication successful");
            SaslStepResult::Complete(Vec::new())
        } else {
            warn!(username = %username, "PLAIN authentication failed: invalid credentials");
            SaslStepResult::Failed(AuthError::InvalidCredentials)
        }
    }

    fn is_complete(&self, session: &SaslSession) -> bool {
        session.authenticated_user.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::sasl::credentials::InMemoryCredentialStore;

    fn create_authenticator() -> PlainAuthenticator {
        let mut store = InMemoryCredentialStore::new();
        store.add_user("alice".to_string(), "secret".to_string());
        store.add_user("bob".to_string(), "password123".to_string());
        PlainAuthenticator::new(Arc::new(store))
    }

    #[test]
    fn test_mechanism_name() {
        let auth = create_authenticator();
        assert_eq!(auth.mechanism_name(), "PLAIN");
    }

    #[test]
    fn test_valid_credentials() {
        let auth = create_authenticator();
        let mut session = SaslSession::new();

        // Format: \0username\0password
        let message = b"\0alice\0secret";
        let result = auth.authenticate_step(message, &mut session);

        assert!(matches!(result, SaslStepResult::Complete(_)));
        assert_eq!(session.authenticated_user, Some("alice".to_string()));
    }

    #[test]
    fn test_valid_credentials_with_authzid() {
        let auth = create_authenticator();
        let mut session = SaslSession::new();

        // Format: authzid\0username\0password (authzid is ignored)
        let message = b"ignored\0bob\0password123";
        let result = auth.authenticate_step(message, &mut session);

        assert!(matches!(result, SaslStepResult::Complete(_)));
        assert_eq!(session.authenticated_user, Some("bob".to_string()));
    }

    #[test]
    fn test_invalid_password() {
        let auth = create_authenticator();
        let mut session = SaslSession::new();

        let message = b"\0alice\0wrongpassword";
        let result = auth.authenticate_step(message, &mut session);

        assert!(matches!(
            result,
            SaslStepResult::Failed(AuthError::InvalidCredentials)
        ));
        assert!(session.authenticated_user.is_none());
    }

    #[test]
    fn test_unknown_user() {
        let auth = create_authenticator();
        let mut session = SaslSession::new();

        let message = b"\0unknown\0password";
        let result = auth.authenticate_step(message, &mut session);

        assert!(matches!(
            result,
            SaslStepResult::Failed(AuthError::InvalidCredentials)
        ));
    }

    #[test]
    fn test_invalid_format_too_few_parts() {
        let auth = create_authenticator();
        let mut session = SaslSession::new();

        let message = b"username";
        let result = auth.authenticate_step(message, &mut session);

        assert!(matches!(
            result,
            SaslStepResult::Failed(AuthError::InvalidCredentials)
        ));
    }

    #[test]
    fn test_is_complete() {
        let auth = create_authenticator();
        let mut session = SaslSession::new();

        assert!(!auth.is_complete(&session));

        session.authenticated_user = Some("alice".to_string());
        assert!(auth.is_complete(&session));
    }
}
