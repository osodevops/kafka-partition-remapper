//! Credential storage and validation for SASL authentication.
//!
//! This module provides credential storage backends for validating
//! client credentials during SASL authentication.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::config::{CredentialConfig, UserCredential};
use crate::error::{AuthError, Result};

/// Stored credentials for a user.
#[derive(Debug, Clone)]
pub struct StoredCredentials {
    /// The username.
    pub username: String,
    /// The password (plaintext for PLAIN, or stored key for SCRAM).
    pub password: String,
}

/// Trait for credential storage backends.
pub trait CredentialStore: Send + Sync + std::fmt::Debug {
    /// Get credentials for a username.
    fn get_credentials(&self, username: &str) -> Option<StoredCredentials>;

    /// Check if credentials are valid.
    fn validate(&self, username: &str, password: &str) -> bool {
        self.get_credentials(username)
            .map(|creds| creds.password == password)
            .unwrap_or(false)
    }
}

/// In-memory credential store.
#[derive(Debug)]
pub struct InMemoryCredentialStore {
    credentials: HashMap<String, StoredCredentials>,
}

impl InMemoryCredentialStore {
    /// Create a new in-memory credential store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            credentials: HashMap::new(),
        }
    }

    /// Add credentials for a user.
    pub fn add_user(&mut self, username: String, password: String) {
        self.credentials
            .insert(username.clone(), StoredCredentials { username, password });
    }

    /// Create from a list of user credentials.
    #[must_use]
    pub fn from_users(users: Vec<UserCredential>) -> Self {
        let mut store = Self::new();
        for user in users {
            store.add_user(user.username, user.password);
        }
        store
    }
}

impl Default for InMemoryCredentialStore {
    fn default() -> Self {
        Self::new()
    }
}

impl CredentialStore for InMemoryCredentialStore {
    fn get_credentials(&self, username: &str) -> Option<StoredCredentials> {
        self.credentials.get(username).cloned()
    }
}

/// File-based credential store.
///
/// Loads credentials from a file in the format:
/// ```text
/// username1:password1
/// username2:password2
/// ```
#[derive(Debug)]
pub struct FileCredentialStore {
    inner: InMemoryCredentialStore,
}

impl FileCredentialStore {
    /// Create a new file-based credential store.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or parsed.
    pub fn load(path: &Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path).map_err(|e| {
            AuthError::CredentialStore(format!("Failed to read credentials file: {e}"))
        })?;

        let mut inner = InMemoryCredentialStore::new();

        for (line_num, line) in contents.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let parts: Vec<&str> = line.splitn(2, ':').collect();
            if parts.len() != 2 {
                return Err(AuthError::CredentialStore(format!(
                    "Invalid credentials file format at line {}: expected 'username:password'",
                    line_num + 1
                ))
                .into());
            }

            inner.add_user(parts[0].to_string(), parts[1].to_string());
        }

        Ok(Self { inner })
    }
}

impl CredentialStore for FileCredentialStore {
    fn get_credentials(&self, username: &str) -> Option<StoredCredentials> {
        self.inner.get_credentials(username)
    }
}

/// Create a credential store from configuration.
///
/// # Errors
///
/// Returns an error if the credential store cannot be created.
pub fn create_credential_store(config: &CredentialConfig) -> Result<Arc<dyn CredentialStore>> {
    match config {
        CredentialConfig::Inline { users } => {
            Ok(Arc::new(InMemoryCredentialStore::from_users(users.clone())))
        }
        CredentialConfig::File { file } => Ok(Arc::new(FileCredentialStore::load(file)?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_in_memory_store() {
        let mut store = InMemoryCredentialStore::new();
        store.add_user("user1".to_string(), "pass1".to_string());
        store.add_user("user2".to_string(), "pass2".to_string());

        assert!(store.validate("user1", "pass1"));
        assert!(store.validate("user2", "pass2"));
        assert!(!store.validate("user1", "wrong"));
        assert!(!store.validate("unknown", "pass"));
    }

    #[test]
    fn test_from_user_credentials() {
        let users = vec![
            UserCredential {
                username: "alice".to_string(),
                password: "secret1".to_string(),
            },
            UserCredential {
                username: "bob".to_string(),
                password: "secret2".to_string(),
            },
        ];

        let store = InMemoryCredentialStore::from_users(users);
        assert!(store.validate("alice", "secret1"));
        assert!(store.validate("bob", "secret2"));
    }

    #[test]
    fn test_file_credential_store() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "# Comment line").unwrap();
        writeln!(file, "user1:password1").unwrap();
        writeln!(file, "user2:password2").unwrap();
        writeln!(file).unwrap(); // Empty line
        writeln!(file, "user3:password:with:colons").unwrap();
        file.flush().unwrap();

        let store = FileCredentialStore::load(file.path()).unwrap();
        assert!(store.validate("user1", "password1"));
        assert!(store.validate("user2", "password2"));
        assert!(store.validate("user3", "password:with:colons"));
        assert!(!store.validate("user1", "wrong"));
    }

    #[test]
    fn test_file_credential_store_invalid_format() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "invalid_line_without_colon").unwrap();
        file.flush().unwrap();

        let result = FileCredentialStore::load(file.path());
        assert!(result.is_err());
    }
}
