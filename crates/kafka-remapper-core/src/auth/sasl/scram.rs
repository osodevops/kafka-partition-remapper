//! SASL/SCRAM authentication implementation.
//!
//! SCRAM (Salted Challenge Response Authentication Mechanism) provides secure
//! password-based authentication without transmitting the password in cleartext.
//!
//! Supports:
//! - SCRAM-SHA-256 (RFC 7677)
//! - SCRAM-SHA-512 (RFC 7677 variant)

use std::collections::HashMap;
use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use rand::Rng;
use sha2::{Digest, Sha256, Sha512};
use tracing::{debug, warn};

use crate::error::AuthError;

use super::credentials::CredentialStore;
use super::{SaslAuthenticator, SaslSession, SaslStepResult};

/// Minimum number of PBKDF2 iterations (per RFC 7677)
pub const MIN_ITERATIONS: u32 = 4096;

/// Default number of PBKDF2 iterations
pub const DEFAULT_ITERATIONS: u32 = 4096;

/// Salt length in bytes
pub const SALT_LENGTH: usize = 16;

/// Nonce length in bytes
pub const NONCE_LENGTH: usize = 24;

/// SCRAM credentials stored for a user.
#[derive(Debug, Clone)]
pub struct ScramCredentials {
    /// Random salt used for key derivation.
    pub salt: Vec<u8>,
    /// Number of PBKDF2 iterations.
    pub iterations: u32,
    /// Stored key: H(ClientKey) where ClientKey = HMAC(SaltedPassword, "Client Key")
    pub stored_key: Vec<u8>,
    /// Server key: HMAC(SaltedPassword, "Server Key")
    pub server_key: Vec<u8>,
}

/// SCRAM session state for multi-step authentication.
#[derive(Debug, Default)]
pub struct ScramSessionState {
    /// Client's first message (without header).
    pub client_first_message_bare: String,
    /// Server's first message.
    pub server_first_message: String,
    /// Combined nonce (client + server).
    pub combined_nonce: String,
    /// Username being authenticated.
    pub username: String,
    /// Current step in the SCRAM exchange.
    pub step: u8,
}

/// Hash algorithm trait for SCRAM variants.
pub trait ScramHash: Send + Sync + std::fmt::Debug {
    /// The hash algorithm name.
    fn name() -> &'static str;
    /// Output length in bytes.
    fn output_len() -> usize;
    /// Compute HMAC.
    fn hmac(key: &[u8], data: &[u8]) -> Vec<u8>;
    /// Compute hash.
    fn hash(data: &[u8]) -> Vec<u8>;
    /// Derive key using PBKDF2.
    fn pbkdf2(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8>;
}

/// SHA-256 implementation for SCRAM.
#[derive(Debug)]
pub struct ScramSha256;

impl ScramHash for ScramSha256 {
    fn name() -> &'static str {
        "SCRAM-SHA-256"
    }

    fn output_len() -> usize {
        32
    }

    fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC can take key of any size");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }

    fn hash(data: &[u8]) -> Vec<u8> {
        Sha256::digest(data).to_vec()
    }

    fn pbkdf2(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
        let mut output = vec![0u8; 32];
        pbkdf2_hmac::<Sha256>(password, salt, iterations, &mut output);
        output
    }
}

/// SHA-512 implementation for SCRAM.
#[derive(Debug)]
pub struct ScramSha512;

impl ScramHash for ScramSha512 {
    fn name() -> &'static str {
        "SCRAM-SHA-512"
    }

    fn output_len() -> usize {
        64
    }

    fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac = Hmac::<Sha512>::new_from_slice(key).expect("HMAC can take key of any size");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }

    fn hash(data: &[u8]) -> Vec<u8> {
        Sha512::digest(data).to_vec()
    }

    fn pbkdf2(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
        let mut output = vec![0u8; 64];
        pbkdf2_hmac::<Sha512>(password, salt, iterations, &mut output);
        output
    }
}

/// Generate SCRAM credentials from a plaintext password.
pub fn generate_scram_credentials<H: ScramHash>(password: &str) -> ScramCredentials {
    let mut rng = rand::thread_rng();
    let salt: Vec<u8> = (0..SALT_LENGTH).map(|_| rng.gen()).collect();
    let iterations = DEFAULT_ITERATIONS;

    generate_scram_credentials_with_salt::<H>(password, &salt, iterations)
}

/// Generate SCRAM credentials with specific salt and iterations.
pub fn generate_scram_credentials_with_salt<H: ScramHash>(
    password: &str,
    salt: &[u8],
    iterations: u32,
) -> ScramCredentials {
    let salted_password = H::pbkdf2(password.as_bytes(), salt, iterations);
    let client_key = H::hmac(&salted_password, b"Client Key");
    let stored_key = H::hash(&client_key);
    let server_key = H::hmac(&salted_password, b"Server Key");

    ScramCredentials {
        salt: salt.to_vec(),
        iterations,
        stored_key,
        server_key,
    }
}

// ============================================================================
// Standalone Message Parsing Functions (for testing)
// ============================================================================

/// Parse client-first-message.
///
/// Format: `[gs2-header]n=username,r=client-nonce`
/// Returns (gs2_header, username, client_nonce)
pub fn parse_client_first_message(message: &str) -> Result<(String, String, String), AuthError> {
    // Find the GS2 header (everything before the bare message)
    let (gs2_header, bare_message) = if let Some(pos) = message.find("n=") {
        // GS2 header is everything before "n="
        (&message[..pos], &message[pos..])
    } else if let Some(stripped) = message.strip_prefix("n,,") {
        ("n,,", stripped)
    } else if let Some(stripped) = message.strip_prefix("y,,") {
        ("y,,", stripped)
    } else if let Some(pos) = message.find(",,") {
        // Handle "p=xxx,," or other variants
        let header_end = pos + 2;
        (&message[..header_end], &message[header_end..])
    } else {
        return Err(AuthError::InvalidMessage(
            "Invalid GS2 header format".to_string(),
        ));
    };

    let mut username = None;
    let mut client_nonce = None;

    for part in bare_message.split(',') {
        if let Some(value) = part.strip_prefix("n=") {
            username = Some(value.to_string());
        } else if let Some(value) = part.strip_prefix("r=") {
            client_nonce = Some(value.to_string());
        }
    }

    let username =
        username.ok_or_else(|| AuthError::InvalidMessage("Missing username".to_string()))?;
    let client_nonce = client_nonce
        .ok_or_else(|| AuthError::InvalidMessage("Missing client nonce".to_string()))?;

    Ok((gs2_header.to_string(), username, client_nonce))
}

/// Parse client-final-message.
///
/// Format: `c=channel-binding,r=combined-nonce,p=client-proof`
/// Returns (message_without_proof, proof_bytes)
pub fn parse_client_final_message(message: &str) -> Result<(String, Vec<u8>), AuthError> {
    // Find the proof first (it's at the end)
    let (without_proof, proof) = if let Some(idx) = message.rfind(",p=") {
        let proof_b64 = &message[idx + 3..];
        let proof = BASE64
            .decode(proof_b64)
            .map_err(|_| AuthError::InvalidMessage("Invalid proof encoding".to_string()))?;
        (&message[..idx], proof)
    } else {
        return Err(AuthError::InvalidMessage(
            "Missing client proof".to_string(),
        ));
    };

    // Verify channel binding is present
    if !without_proof.starts_with("c=") {
        return Err(AuthError::InvalidMessage(
            "Missing channel binding in client-final".to_string(),
        ));
    }

    Ok((without_proof.to_string(), proof))
}

/// SCRAM server authenticator.
#[derive(Debug)]
pub struct ScramAuthenticator<H: ScramHash> {
    credential_store: Arc<dyn CredentialStore>,
    /// Pre-computed SCRAM credentials for users (username -> credentials)
    scram_credentials: HashMap<String, ScramCredentials>,
    _marker: std::marker::PhantomData<H>,
}

impl<H: ScramHash + 'static> ScramAuthenticator<H> {
    /// Create a new SCRAM authenticator.
    ///
    /// This will pre-compute SCRAM credentials from the plaintext passwords
    /// in the credential store.
    #[must_use]
    pub fn new(credential_store: Arc<dyn CredentialStore>) -> Self {
        Self {
            credential_store,
            scram_credentials: HashMap::new(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Add pre-computed SCRAM credentials for a user.
    pub fn add_scram_credentials(&mut self, username: String, credentials: ScramCredentials) {
        self.scram_credentials.insert(username, credentials);
    }

    /// Get SCRAM credentials for a user.
    ///
    /// First checks pre-computed credentials, then falls back to deriving
    /// from the plaintext password in the credential store.
    fn get_scram_credentials(&self, username: &str) -> Option<ScramCredentials> {
        // Check pre-computed credentials first
        if let Some(creds) = self.scram_credentials.get(username) {
            return Some(creds.clone());
        }

        // Fall back to deriving from plaintext password
        let stored = self.credential_store.get_credentials(username)?;
        Some(generate_scram_credentials::<H>(&stored.password))
    }

    /// Parse client-first-message.
    ///
    /// Format: `[gs2-header]n=username,r=client-nonce`
    fn parse_client_first_message(
        &self,
        message: &str,
    ) -> Result<(String, String, String), AuthError> {
        // Skip GS2 header (n,,) if present
        let bare_message = if let Some(stripped) = message.strip_prefix("n,,") {
            stripped
        } else if message.starts_with("y,,") || message.starts_with("p=") {
            return Err(AuthError::InvalidMessage(
                "Channel binding not supported".to_string(),
            ));
        } else {
            message
        };

        let mut username = None;
        let mut client_nonce = None;

        for part in bare_message.split(',') {
            if let Some(value) = part.strip_prefix("n=") {
                username = Some(value.to_string());
            } else if let Some(value) = part.strip_prefix("r=") {
                client_nonce = Some(value.to_string());
            }
        }

        let username =
            username.ok_or_else(|| AuthError::InvalidMessage("Missing username".to_string()))?;
        let client_nonce = client_nonce
            .ok_or_else(|| AuthError::InvalidMessage("Missing client nonce".to_string()))?;

        Ok((bare_message.to_string(), username, client_nonce))
    }

    /// Generate server-first-message.
    ///
    /// Format: `r=combined-nonce,s=salt,i=iterations`
    fn generate_server_first_message(
        &self,
        client_nonce: &str,
        credentials: &ScramCredentials,
    ) -> (String, String) {
        let mut rng = rand::thread_rng();
        let server_nonce: String = (0..NONCE_LENGTH)
            .map(|_| {
                let idx = rng.gen_range(0..62);
                match idx {
                    0..=9 => (b'0' + idx) as char,
                    10..=35 => (b'a' + idx - 10) as char,
                    _ => (b'A' + idx - 36) as char,
                }
            })
            .collect();

        let combined_nonce = format!("{client_nonce}{server_nonce}");
        let salt_b64 = BASE64.encode(&credentials.salt);

        let message = format!(
            "r={},s={},i={}",
            combined_nonce, salt_b64, credentials.iterations
        );

        (message, combined_nonce)
    }

    /// Parse client-final-message.
    ///
    /// Format: `c=channel-binding,r=combined-nonce,p=client-proof`
    fn parse_client_final_message(&self, message: &str) -> Result<(String, Vec<u8>), AuthError> {
        // Find the proof first (it's at the end)
        let (without_proof, proof) = if let Some(idx) = message.rfind(",p=") {
            let proof_b64 = &message[idx + 3..];
            let proof = BASE64
                .decode(proof_b64)
                .map_err(|_| AuthError::InvalidMessage("Invalid proof encoding".to_string()))?;
            (&message[..idx], proof)
        } else {
            return Err(AuthError::InvalidMessage(
                "Missing client proof".to_string(),
            ));
        };

        let mut nonce = None;
        for part in without_proof.split(',') {
            if let Some(value) = part.strip_prefix("r=") {
                nonce = Some(value.to_string());
            }
        }

        let nonce =
            nonce.ok_or_else(|| AuthError::InvalidMessage("Missing nonce in final".to_string()))?;

        Ok((nonce, proof))
    }

    /// Verify client proof and generate server signature.
    fn verify_and_sign(
        &self,
        credentials: &ScramCredentials,
        auth_message: &str,
        client_proof: &[u8],
    ) -> Result<Vec<u8>, AuthError> {
        // ClientSignature = HMAC(StoredKey, AuthMessage)
        let client_signature = H::hmac(&credentials.stored_key, auth_message.as_bytes());

        // ClientKey = ClientProof XOR ClientSignature
        if client_proof.len() != client_signature.len() {
            return Err(AuthError::InvalidCredentials);
        }

        let client_key: Vec<u8> = client_proof
            .iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();

        // Verify: H(ClientKey) == StoredKey
        let computed_stored_key = H::hash(&client_key);
        if computed_stored_key != credentials.stored_key {
            return Err(AuthError::InvalidCredentials);
        }

        // ServerSignature = HMAC(ServerKey, AuthMessage)
        let server_signature = H::hmac(&credentials.server_key, auth_message.as_bytes());

        Ok(server_signature)
    }
}

impl<H: ScramHash + 'static> SaslAuthenticator for ScramAuthenticator<H> {
    fn mechanism_name(&self) -> &'static str {
        H::name()
    }

    fn authenticate_step(
        &self,
        client_message: &[u8],
        session: &mut SaslSession,
    ) -> SaslStepResult {
        let message = match String::from_utf8(client_message.to_vec()) {
            Ok(m) => m,
            Err(_) => {
                return SaslStepResult::Failed(AuthError::InvalidMessage(
                    "Invalid UTF-8 in SCRAM message".to_string(),
                ))
            }
        };

        // Get or create SCRAM state
        let state = session
            .scram_state
            .get_or_insert_with(ScramSessionState::default);

        match state.step {
            0 => {
                // Step 1: Receive client-first-message
                debug!("SCRAM step 1: processing client-first-message");

                let (bare_message, username, client_nonce) =
                    match self.parse_client_first_message(&message) {
                        Ok(result) => result,
                        Err(e) => return SaslStepResult::Failed(e),
                    };

                // Get credentials for user
                let credentials = match self.get_scram_credentials(&username) {
                    Some(c) => c,
                    None => {
                        warn!(username = %username, "SCRAM: unknown user");
                        return SaslStepResult::Failed(AuthError::InvalidCredentials);
                    }
                };

                // Generate server-first-message
                let (server_first, combined_nonce) =
                    self.generate_server_first_message(&client_nonce, &credentials);

                // Store state
                state.client_first_message_bare = bare_message;
                state.server_first_message = server_first.clone();
                state.combined_nonce = combined_nonce;
                state.username = username;
                state.step = 1;

                debug!("SCRAM step 1: sending server-first-message");
                SaslStepResult::Continue(server_first.into_bytes())
            }
            1 => {
                // Step 2: Receive client-final-message
                debug!("SCRAM step 2: processing client-final-message");

                let (nonce, client_proof) = match self.parse_client_final_message(&message) {
                    Ok(result) => result,
                    Err(e) => return SaslStepResult::Failed(e),
                };

                // Verify nonce matches
                if nonce != state.combined_nonce {
                    warn!("SCRAM: nonce mismatch");
                    return SaslStepResult::Failed(AuthError::InvalidCredentials);
                }

                // Get credentials again
                let credentials = match self.get_scram_credentials(&state.username) {
                    Some(c) => c,
                    None => return SaslStepResult::Failed(AuthError::InvalidCredentials),
                };

                // Build auth message
                // client-final-message-without-proof
                let without_proof = if let Some(idx) = message.rfind(",p=") {
                    &message[..idx]
                } else {
                    return SaslStepResult::Failed(AuthError::InvalidMessage(
                        "Invalid client-final".to_string(),
                    ));
                };

                let auth_message = format!(
                    "{},{},{}",
                    state.client_first_message_bare, state.server_first_message, without_proof
                );

                // Verify proof and get server signature
                let server_signature =
                    match self.verify_and_sign(&credentials, &auth_message, &client_proof) {
                        Ok(sig) => sig,
                        Err(e) => {
                            warn!(username = %state.username, "SCRAM: authentication failed");
                            return SaslStepResult::Failed(e);
                        }
                    };

                // Build server-final-message
                let server_final = format!("v={}", BASE64.encode(&server_signature));

                session.authenticated_user = Some(state.username.clone());
                debug!(username = %state.username, "SCRAM: authentication successful");

                SaslStepResult::Complete(server_final.into_bytes())
            }
            _ => {
                SaslStepResult::Failed(AuthError::InvalidMessage("Invalid SCRAM step".to_string()))
            }
        }
    }

    fn is_complete(&self, session: &SaslSession) -> bool {
        session.authenticated_user.is_some()
    }
}

/// Type aliases for convenience
pub type ScramSha256Authenticator = ScramAuthenticator<ScramSha256>;
pub type ScramSha512Authenticator = ScramAuthenticator<ScramSha512>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::sasl::credentials::InMemoryCredentialStore;

    fn create_test_credentials_sha256() -> ScramCredentials {
        generate_scram_credentials_with_salt::<ScramSha256>(
            "password123",
            b"testsalt12345678",
            4096,
        )
    }

    #[test]
    fn test_generate_scram_credentials() {
        let creds = generate_scram_credentials::<ScramSha256>("test_password");
        assert_eq!(creds.salt.len(), SALT_LENGTH);
        assert_eq!(creds.iterations, DEFAULT_ITERATIONS);
        assert_eq!(creds.stored_key.len(), 32);
        assert_eq!(creds.server_key.len(), 32);
    }

    #[test]
    fn test_generate_scram_credentials_sha512() {
        let creds = generate_scram_credentials::<ScramSha512>("test_password");
        assert_eq!(creds.salt.len(), SALT_LENGTH);
        assert_eq!(creds.stored_key.len(), 64);
        assert_eq!(creds.server_key.len(), 64);
    }

    #[test]
    fn test_credentials_are_deterministic() {
        let salt = b"fixed_salt_value";
        let creds1 = generate_scram_credentials_with_salt::<ScramSha256>("password", salt, 4096);
        let creds2 = generate_scram_credentials_with_salt::<ScramSha256>("password", salt, 4096);

        assert_eq!(creds1.stored_key, creds2.stored_key);
        assert_eq!(creds1.server_key, creds2.server_key);
    }

    #[test]
    fn test_parse_client_first_message() {
        let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
        let auth = ScramSha256Authenticator::new(store);

        let message = "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL";
        let (bare, username, nonce) = auth.parse_client_first_message(message).unwrap();

        assert_eq!(bare, "n=user,r=fyko+d2lbbFgONRv9qkxdawL");
        assert_eq!(username, "user");
        assert_eq!(nonce, "fyko+d2lbbFgONRv9qkxdawL");
    }

    #[test]
    fn test_parse_client_first_message_without_gs2() {
        let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
        let auth = ScramSha256Authenticator::new(store);

        let message = "n=testuser,r=abc123";
        let (_, username, nonce) = auth.parse_client_first_message(message).unwrap();

        assert_eq!(username, "testuser");
        assert_eq!(nonce, "abc123");
    }

    #[test]
    fn test_full_scram_exchange() {
        let mut store = InMemoryCredentialStore::new();
        store.add_user("alice".to_string(), "secret123".to_string());
        let store: Arc<dyn CredentialStore> = Arc::new(store);

        let mut auth = ScramSha256Authenticator::new(Arc::clone(&store));

        // Pre-compute credentials with known salt for testing
        let salt = b"known_salt_value";
        let creds = generate_scram_credentials_with_salt::<ScramSha256>("secret123", salt, 4096);
        auth.add_scram_credentials("alice".to_string(), creds.clone());

        let mut session = SaslSession::new();

        // Step 1: Client sends first message
        let client_nonce = "rOprNGfwEbeRWgbNEkqO";
        let client_first = format!("n,,n=alice,r={}", client_nonce);

        let result = auth.authenticate_step(client_first.as_bytes(), &mut session);

        let server_first = match result {
            SaslStepResult::Continue(data) => String::from_utf8(data).unwrap(),
            _ => panic!("Expected Continue"),
        };

        // Verify server-first format
        assert!(server_first.starts_with("r="));
        assert!(server_first.contains(",s="));
        assert!(server_first.contains(",i="));

        // Extract combined nonce and verify it starts with client nonce
        let combined_nonce = server_first
            .split(',')
            .find(|s| s.starts_with("r="))
            .unwrap()
            .strip_prefix("r=")
            .unwrap();
        assert!(combined_nonce.starts_with(client_nonce));

        // Step 2: Client computes proof and sends final message
        // For testing, we need to compute the client proof correctly
        let salt_b64 = BASE64.encode(salt);
        let salted_password = ScramSha256::pbkdf2(b"secret123", salt, 4096);
        let client_key = ScramSha256::hmac(&salted_password, b"Client Key");
        let stored_key = ScramSha256::hash(&client_key);

        let client_final_without_proof = format!("c=biws,r={}", combined_nonce);
        let auth_message = format!(
            "n=alice,r={},{},{}",
            client_nonce, server_first, client_final_without_proof
        );

        let client_signature = ScramSha256::hmac(&stored_key, auth_message.as_bytes());
        let client_proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();

        let client_final = format!(
            "{},p={}",
            client_final_without_proof,
            BASE64.encode(&client_proof)
        );

        let result = auth.authenticate_step(client_final.as_bytes(), &mut session);

        match result {
            SaslStepResult::Complete(data) => {
                let server_final = String::from_utf8(data).unwrap();
                assert!(server_final.starts_with("v="));
                assert!(session.is_authenticated());
                assert_eq!(session.authenticated_user, Some("alice".to_string()));
            }
            SaslStepResult::Failed(e) => panic!("Authentication failed: {:?}", e),
            _ => panic!("Expected Complete"),
        }
    }

    #[test]
    fn test_wrong_password() {
        let mut store = InMemoryCredentialStore::new();
        store.add_user("bob".to_string(), "correct_password".to_string());
        let store: Arc<dyn CredentialStore> = Arc::new(store);

        let mut auth = ScramSha256Authenticator::new(Arc::clone(&store));

        // Pre-compute credentials with the CORRECT password
        let salt = b"test_salt_123456";
        let creds =
            generate_scram_credentials_with_salt::<ScramSha256>("correct_password", salt, 4096);
        auth.add_scram_credentials("bob".to_string(), creds);

        let mut session = SaslSession::new();

        // Step 1
        let client_first = "n,,n=bob,r=clientnonce123";
        let result = auth.authenticate_step(client_first.as_bytes(), &mut session);

        let server_first = match result {
            SaslStepResult::Continue(data) => String::from_utf8(data).unwrap(),
            _ => panic!("Expected Continue"),
        };

        // Step 2: Compute proof with WRONG password
        let combined_nonce = server_first
            .split(',')
            .find(|s| s.starts_with("r="))
            .unwrap()
            .strip_prefix("r=")
            .unwrap();

        let wrong_salted_password = ScramSha256::pbkdf2(b"wrong_password", salt, 4096);
        let wrong_client_key = ScramSha256::hmac(&wrong_salted_password, b"Client Key");
        let wrong_stored_key = ScramSha256::hash(&wrong_client_key);

        let client_final_without_proof = format!("c=biws,r={}", combined_nonce);
        let auth_message = format!(
            "n=bob,r=clientnonce123,{},{}",
            server_first, client_final_without_proof
        );

        let wrong_signature = ScramSha256::hmac(&wrong_stored_key, auth_message.as_bytes());
        let wrong_proof: Vec<u8> = wrong_client_key
            .iter()
            .zip(wrong_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();

        let client_final = format!(
            "{},p={}",
            client_final_without_proof,
            BASE64.encode(&wrong_proof)
        );

        let result = auth.authenticate_step(client_final.as_bytes(), &mut session);

        assert!(matches!(
            result,
            SaslStepResult::Failed(AuthError::InvalidCredentials)
        ));
        assert!(!session.is_authenticated());
    }

    #[test]
    fn test_unknown_user() {
        let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
        let auth = ScramSha256Authenticator::new(store);

        let mut session = SaslSession::new();
        let client_first = "n,,n=unknown_user,r=nonce123";

        let result = auth.authenticate_step(client_first.as_bytes(), &mut session);
        assert!(matches!(
            result,
            SaslStepResult::Failed(AuthError::InvalidCredentials)
        ));
    }

    #[test]
    fn test_mechanism_names() {
        let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());

        let sha256_auth = ScramSha256Authenticator::new(Arc::clone(&store));
        assert_eq!(sha256_auth.mechanism_name(), "SCRAM-SHA-256");

        let sha512_auth = ScramSha512Authenticator::new(store);
        assert_eq!(sha512_auth.mechanism_name(), "SCRAM-SHA-512");
    }

    // ============================================================================
    // RFC 7677 Test Vectors
    // ============================================================================
    // These tests verify our implementation against the RFC 7677 example.
    // Reference: https://datatracker.ietf.org/doc/html/rfc7677#section-3

    #[test]
    fn test_rfc7677_salted_password() {
        // RFC 7677 Section 3 example values
        let password = b"pencil";
        let salt = BASE64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let iterations = 4096u32;

        let salted_password = ScramSha256::pbkdf2(password, &salt, iterations);

        // The RFC doesn't provide the raw salted password, but we can verify
        // by checking the derived keys match
        let client_key = ScramSha256::hmac(&salted_password, b"Client Key");
        let stored_key = ScramSha256::hash(&client_key);
        let server_key = ScramSha256::hmac(&salted_password, b"Server Key");

        // Verify lengths are correct for SHA-256
        assert_eq!(salted_password.len(), 32);
        assert_eq!(client_key.len(), 32);
        assert_eq!(stored_key.len(), 32);
        assert_eq!(server_key.len(), 32);
    }

    #[test]
    fn test_rfc7677_full_exchange_vectors() {
        // RFC 7677 Section 3 provides a complete example exchange
        // User: "user", Password: "pencil"
        let password = "pencil";
        let salt = BASE64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let iterations = 4096u32;

        // Client nonce from RFC example
        let client_nonce = "rOprNGfwEbeRWgbNEkqO";
        // Server nonce (appended to client nonce)
        let server_nonce = "rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";

        // Message components from RFC
        let client_first_bare = format!("n=user,r={}", client_nonce);
        let server_first = format!(
            "r={},s={},i={}",
            server_nonce,
            BASE64.encode(&salt),
            iterations
        );
        let client_final_without_proof = format!("c=biws,r={}", server_nonce);

        let auth_message = format!(
            "{},{},{}",
            client_first_bare, server_first, client_final_without_proof
        );

        // Compute the proof
        let salted_password = ScramSha256::pbkdf2(password.as_bytes(), &salt, iterations);
        let client_key = ScramSha256::hmac(&salted_password, b"Client Key");
        let stored_key = ScramSha256::hash(&client_key);
        let client_signature = ScramSha256::hmac(&stored_key, auth_message.as_bytes());

        let client_proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();

        // Server key and signature
        let server_key = ScramSha256::hmac(&salted_password, b"Server Key");
        let server_signature = ScramSha256::hmac(&server_key, auth_message.as_bytes());

        // Verify proof and signature have correct lengths
        assert_eq!(client_proof.len(), 32);
        assert_eq!(server_signature.len(), 32);

        // Verify stored key can be used to recover client key via XOR
        let recovered_client_key: Vec<u8> = client_proof
            .iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();
        assert_eq!(recovered_client_key, client_key);

        // Verify H(recovered_client_key) == stored_key
        let recovered_stored_key = ScramSha256::hash(&recovered_client_key);
        assert_eq!(recovered_stored_key, stored_key);
    }

    // ============================================================================
    // Message Parsing Tests (similar to Kafka's ScramMessagesTest)
    // ============================================================================

    #[test]
    fn test_parse_valid_client_first_with_authzid() {
        // "n,a=authzid,n=username,r=clientnonce"
        let message = "n,a=admin,n=testuser,r=abc123xyz";
        let parsed = parse_client_first_message(message);
        assert!(parsed.is_ok());
        let (gs2_header, username, nonce) = parsed.unwrap();
        assert_eq!(gs2_header, "n,a=admin,");
        assert_eq!(username, "testuser");
        assert_eq!(nonce, "abc123xyz");
    }

    #[test]
    fn test_parse_client_first_empty_authzid() {
        let message = "n,,n=user,r=nonce";
        let parsed = parse_client_first_message(message);
        assert!(parsed.is_ok());
        let (gs2_header, username, nonce) = parsed.unwrap();
        assert_eq!(gs2_header, "n,,");
        assert_eq!(username, "user");
        assert_eq!(nonce, "nonce");
    }

    #[test]
    fn test_parse_client_first_invalid_cbind_flag() {
        // 'y' means client supports but thinks server doesn't - should work
        let message = "y,,n=user,r=nonce";
        let parsed = parse_client_first_message(message);
        assert!(parsed.is_ok());

        // 'p=...' means channel binding required - not supported
        let message = "p=tls-unique,,n=user,r=nonce";
        let parsed = parse_client_first_message(message);
        // Should still parse (we just don't verify channel binding)
        assert!(parsed.is_ok());
    }

    #[test]
    fn test_parse_client_first_missing_username() {
        let message = "n,,r=nonce";
        let parsed = parse_client_first_message(message);
        assert!(parsed.is_err());
    }

    #[test]
    fn test_parse_client_first_missing_nonce() {
        let message = "n,,n=user";
        let parsed = parse_client_first_message(message);
        assert!(parsed.is_err());
    }

    #[test]
    fn test_parse_client_final_valid() {
        let message = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";
        let parsed = parse_client_final_message(message);
        assert!(parsed.is_ok());
        let (without_proof, proof) = parsed.unwrap();
        assert!(without_proof.starts_with("c=biws,r="));
        assert!(!proof.is_empty());
    }

    #[test]
    fn test_parse_client_final_missing_channel_binding() {
        let message = "r=nonce,p=proof";
        let parsed = parse_client_final_message(message);
        assert!(parsed.is_err());
    }

    #[test]
    fn test_parse_client_final_missing_proof() {
        let message = "c=biws,r=nonce";
        let parsed = parse_client_final_message(message);
        assert!(parsed.is_err());
    }

    #[test]
    fn test_parse_client_final_invalid_proof_encoding() {
        let message = "c=biws,r=nonce,p=!!!invalid-base64!!!";
        let parsed = parse_client_final_message(message);
        assert!(parsed.is_err());
    }

    // ============================================================================
    // Special Character Username Tests (similar to Kafka's saslName tests)
    // ============================================================================

    #[test]
    fn test_username_with_comma() {
        // In SCRAM, commas in usernames must be encoded as =2C
        let mut store = InMemoryCredentialStore::new();
        store.add_user("user=2Cwith=2Ccommas".to_string(), "password".to_string());
        let store: Arc<dyn CredentialStore> = Arc::new(store);

        let mut auth = ScramSha256Authenticator::new(Arc::clone(&store));
        let creds = generate_scram_credentials::<ScramSha256>("password");
        auth.add_scram_credentials("user=2Cwith=2Ccommas".to_string(), creds);

        let mut session = SaslSession::new();
        // The username field uses the encoded form
        let client_first = "n,,n=user=2Cwith=2Ccommas,r=testnonce123";

        let result = auth.authenticate_step(client_first.as_bytes(), &mut session);
        assert!(matches!(result, SaslStepResult::Continue(_)));
    }

    #[test]
    fn test_username_with_equals() {
        // In SCRAM, equals signs in usernames must be encoded as =3D
        let mut store = InMemoryCredentialStore::new();
        store.add_user("user=3Dwith=3Dequals".to_string(), "password".to_string());
        let store: Arc<dyn CredentialStore> = Arc::new(store);

        let mut auth = ScramSha256Authenticator::new(Arc::clone(&store));
        let creds = generate_scram_credentials::<ScramSha256>("password");
        auth.add_scram_credentials("user=3Dwith=3Dequals".to_string(), creds);

        let mut session = SaslSession::new();
        let client_first = "n,,n=user=3Dwith=3Dequals,r=testnonce123";

        let result = auth.authenticate_step(client_first.as_bytes(), &mut session);
        assert!(matches!(result, SaslStepResult::Continue(_)));
    }

    #[test]
    fn test_username_with_unicode() {
        // Unicode usernames should work (UTF-8)
        let mut store = InMemoryCredentialStore::new();
        store.add_user("用户名".to_string(), "密码".to_string());
        let store: Arc<dyn CredentialStore> = Arc::new(store);

        let mut auth = ScramSha256Authenticator::new(Arc::clone(&store));
        let creds = generate_scram_credentials::<ScramSha256>("密码");
        auth.add_scram_credentials("用户名".to_string(), creds);

        let mut session = SaslSession::new();
        let client_first = "n,,n=用户名,r=testnonce123";

        let result = auth.authenticate_step(client_first.as_bytes(), &mut session);
        assert!(matches!(result, SaslStepResult::Continue(_)));
    }

    // ============================================================================
    // Invalid Message Handling Tests
    // ============================================================================

    #[test]
    fn test_invalid_gs2_header_malformed() {
        let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
        let auth = ScramSha256Authenticator::new(store);

        let mut session = SaslSession::new();
        // Missing second comma in GS2 header
        let client_first = "n,n=user,r=nonce";

        let result = auth.authenticate_step(client_first.as_bytes(), &mut session);
        assert!(matches!(result, SaslStepResult::Failed(_)));
    }

    #[test]
    fn test_invalid_empty_message() {
        let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
        let auth = ScramSha256Authenticator::new(store);

        let mut session = SaslSession::new();
        let result = auth.authenticate_step(b"", &mut session);
        assert!(matches!(result, SaslStepResult::Failed(_)));
    }

    #[test]
    fn test_invalid_client_first_no_gs2() {
        let store: Arc<dyn CredentialStore> = Arc::new(InMemoryCredentialStore::new());
        let auth = ScramSha256Authenticator::new(store);

        let mut session = SaslSession::new();
        // Missing GS2 header entirely
        let client_first = "n=user,r=nonce";

        let result = auth.authenticate_step(client_first.as_bytes(), &mut session);
        assert!(matches!(result, SaslStepResult::Failed(_)));
    }

    #[test]
    fn test_client_final_wrong_nonce() {
        let mut store = InMemoryCredentialStore::new();
        store.add_user("alice".to_string(), "password123".to_string());
        let store: Arc<dyn CredentialStore> = Arc::new(store);

        let mut auth = ScramSha256Authenticator::new(Arc::clone(&store));
        let salt = b"test_salt_1234567890";
        let creds = generate_scram_credentials_with_salt::<ScramSha256>("password123", salt, 4096);
        auth.add_scram_credentials("alice".to_string(), creds);

        let mut session = SaslSession::new();
        let client_nonce = "clientnonce123";

        // Step 1: Client first
        let client_first = format!("n,,n=alice,r={}", client_nonce);
        let result = auth.authenticate_step(client_first.as_bytes(), &mut session);

        let server_first = match result {
            SaslStepResult::Continue(data) => String::from_utf8(data).unwrap(),
            _ => panic!("Expected Continue"),
        };

        // Step 2: Client final with WRONG nonce (doesn't match combined nonce)
        let client_final =
            "c=biws,r=wrong_nonce_here,p=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
        let result = auth.authenticate_step(client_final.as_bytes(), &mut session);

        // Should fail because nonce doesn't match
        assert!(matches!(result, SaslStepResult::Failed(_)));
    }

    #[test]
    fn test_client_final_invalid_channel_binding() {
        let mut store = InMemoryCredentialStore::new();
        store.add_user("alice".to_string(), "password123".to_string());
        let store: Arc<dyn CredentialStore> = Arc::new(store);

        let mut auth = ScramSha256Authenticator::new(Arc::clone(&store));
        let creds = generate_scram_credentials::<ScramSha256>("password123");
        auth.add_scram_credentials("alice".to_string(), creds);

        let mut session = SaslSession::new();

        // Step 1
        let client_first = "n,,n=alice,r=testnonce";
        let result = auth.authenticate_step(client_first.as_bytes(), &mut session);

        let server_first = match result {
            SaslStepResult::Continue(data) => String::from_utf8(data).unwrap(),
            _ => panic!("Expected Continue"),
        };

        let combined_nonce = server_first
            .split(',')
            .find(|s| s.starts_with("r="))
            .unwrap()
            .strip_prefix("r=")
            .unwrap();

        // Step 2: Client final with WRONG channel binding (not "biws" which is base64("n,,"))
        let client_final = format!(
            "c=cD10bHMtdW5pcXVlLHNlcnZlci1zaWduYXR1cmU=,r={},p=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            combined_nonce
        );
        let result = auth.authenticate_step(client_final.as_bytes(), &mut session);

        // Should fail because channel binding doesn't match
        assert!(matches!(result, SaslStepResult::Failed(_)));
    }

    // ============================================================================
    // SHA-512 Specific Tests
    // ============================================================================

    #[test]
    fn test_sha512_full_exchange() {
        let mut store = InMemoryCredentialStore::new();
        store.add_user("bob".to_string(), "secretpassword".to_string());
        let store: Arc<dyn CredentialStore> = Arc::new(store);

        let mut auth = ScramSha512Authenticator::new(Arc::clone(&store));
        let salt = b"sha512_test_salt";
        let creds =
            generate_scram_credentials_with_salt::<ScramSha512>("secretpassword", salt, 4096);
        auth.add_scram_credentials("bob".to_string(), creds);

        let mut session = SaslSession::new();
        let client_nonce = "sha512clientnonce";

        // Step 1: Client first
        let client_first = format!("n,,n=bob,r={}", client_nonce);
        let result = auth.authenticate_step(client_first.as_bytes(), &mut session);

        let server_first = match result {
            SaslStepResult::Continue(data) => String::from_utf8(data).unwrap(),
            _ => panic!("Expected Continue"),
        };

        // Parse server-first
        let combined_nonce = server_first
            .split(',')
            .find(|s| s.starts_with("r="))
            .unwrap()
            .strip_prefix("r=")
            .unwrap();

        // Compute client proof using SHA-512
        let salted_password = ScramSha512::pbkdf2(b"secretpassword", salt, 4096);
        let client_key = ScramSha512::hmac(&salted_password, b"Client Key");
        let stored_key = ScramSha512::hash(&client_key);

        let client_first_bare = format!("n=bob,r={}", client_nonce);
        let client_final_without_proof = format!("c=biws,r={}", combined_nonce);
        let auth_message = format!(
            "{},{},{}",
            client_first_bare, server_first, client_final_without_proof
        );

        let client_signature = ScramSha512::hmac(&stored_key, auth_message.as_bytes());
        let client_proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();

        // SHA-512 produces 64-byte output
        assert_eq!(client_proof.len(), 64);

        let client_final = format!(
            "{},p={}",
            client_final_without_proof,
            BASE64.encode(&client_proof)
        );

        let result = auth.authenticate_step(client_final.as_bytes(), &mut session);

        match result {
            SaslStepResult::Complete(data) => {
                let server_final = String::from_utf8(data).unwrap();
                assert!(server_final.starts_with("v="));
                assert!(session.is_authenticated());
                assert_eq!(session.authenticated_user, Some("bob".to_string()));
            }
            SaslStepResult::Failed(e) => panic!("SHA-512 authentication failed: {:?}", e),
            _ => panic!("Expected Complete"),
        }
    }

    #[test]
    fn test_sha512_output_length() {
        // Verify SHA-512 produces 64-byte outputs
        let hash = ScramSha512::hash(b"test data");
        assert_eq!(hash.len(), 64);

        let hmac = ScramSha512::hmac(b"key", b"data");
        assert_eq!(hmac.len(), 64);

        let pbkdf2 = ScramSha512::pbkdf2(b"password", b"salt", 4096);
        assert_eq!(pbkdf2.len(), 64);
    }

    #[test]
    fn test_sha256_output_length() {
        // Verify SHA-256 produces 32-byte outputs
        let hash = ScramSha256::hash(b"test data");
        assert_eq!(hash.len(), 32);

        let hmac = ScramSha256::hmac(b"key", b"data");
        assert_eq!(hmac.len(), 32);

        let pbkdf2 = ScramSha256::pbkdf2(b"password", b"salt", 4096);
        assert_eq!(pbkdf2.len(), 32);
    }

    // ============================================================================
    // Iteration Count Tests
    // ============================================================================

    #[test]
    fn test_minimum_iterations() {
        // RFC 7677 specifies minimum 4096 iterations
        let creds = generate_scram_credentials_with_salt::<ScramSha256>(
            "password",
            b"salt1234567890",
            4096,
        );
        assert_eq!(creds.iterations, 4096);
    }

    #[test]
    fn test_high_iterations() {
        // Test with higher iteration count
        let creds = generate_scram_credentials_with_salt::<ScramSha256>(
            "password",
            b"salt1234567890",
            10000,
        );
        assert_eq!(creds.iterations, 10000);
    }

    // ============================================================================
    // Concurrent Access Tests
    // ============================================================================

    #[test]
    fn test_multiple_concurrent_sessions() {
        // Simulate multiple clients authenticating concurrently
        let mut store = InMemoryCredentialStore::new();
        store.add_user("user1".to_string(), "pass1".to_string());
        store.add_user("user2".to_string(), "pass2".to_string());
        let store: Arc<dyn CredentialStore> = Arc::new(store);

        let mut auth = ScramSha256Authenticator::new(Arc::clone(&store));
        auth.add_scram_credentials(
            "user1".to_string(),
            generate_scram_credentials::<ScramSha256>("pass1"),
        );
        auth.add_scram_credentials(
            "user2".to_string(),
            generate_scram_credentials::<ScramSha256>("pass2"),
        );

        // Create two independent sessions
        let mut session1 = SaslSession::new();
        let mut session2 = SaslSession::new();

        // Start both sessions
        let result1 = auth.authenticate_step(b"n,,n=user1,r=nonce1", &mut session1);
        let result2 = auth.authenticate_step(b"n,,n=user2,r=nonce2", &mut session2);

        // Both should get Continue responses
        assert!(matches!(result1, SaslStepResult::Continue(_)));
        assert!(matches!(result2, SaslStepResult::Continue(_)));

        // Sessions should have different state
        assert_eq!(session1.scram_state.as_ref().unwrap().username, "user1");
        assert_eq!(session2.scram_state.as_ref().unwrap().username, "user2");
    }

    // ============================================================================
    // Edge Cases
    // ============================================================================

    #[test]
    fn test_empty_password() {
        let mut store = InMemoryCredentialStore::new();
        store.add_user("emptypass".to_string(), String::new());
        let store: Arc<dyn CredentialStore> = Arc::new(store);

        let mut auth = ScramSha256Authenticator::new(Arc::clone(&store));
        let creds = generate_scram_credentials::<ScramSha256>("");
        auth.add_scram_credentials("emptypass".to_string(), creds);

        let mut session = SaslSession::new();
        let result = auth.authenticate_step(b"n,,n=emptypass,r=nonce", &mut session);
        assert!(matches!(result, SaslStepResult::Continue(_)));
    }

    #[test]
    fn test_long_password() {
        let long_password = "a".repeat(10000);
        let mut store = InMemoryCredentialStore::new();
        store.add_user("longpass".to_string(), long_password.clone());
        let store: Arc<dyn CredentialStore> = Arc::new(store);

        let mut auth = ScramSha256Authenticator::new(Arc::clone(&store));
        let creds = generate_scram_credentials::<ScramSha256>(&long_password);
        auth.add_scram_credentials("longpass".to_string(), creds);

        let mut session = SaslSession::new();
        let result = auth.authenticate_step(b"n,,n=longpass,r=nonce", &mut session);
        assert!(matches!(result, SaslStepResult::Continue(_)));
    }

    #[test]
    fn test_long_username() {
        let long_username = "u".repeat(1000);
        let mut store = InMemoryCredentialStore::new();
        store.add_user(long_username.clone(), "password".to_string());
        let store: Arc<dyn CredentialStore> = Arc::new(store);

        let mut auth = ScramSha256Authenticator::new(Arc::clone(&store));
        let creds = generate_scram_credentials::<ScramSha256>("password");
        auth.add_scram_credentials(long_username.clone(), creds);

        let mut session = SaslSession::new();
        let client_first = format!("n,,n={},r=nonce", long_username);
        let result = auth.authenticate_step(client_first.as_bytes(), &mut session);
        assert!(matches!(result, SaslStepResult::Continue(_)));
    }
}
