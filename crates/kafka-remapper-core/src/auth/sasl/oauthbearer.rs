//! SASL/OAUTHBEARER authentication implementation.
//!
//! OAUTHBEARER uses OAuth 2.0 bearer tokens for authentication (KIP-255).
//! The token is passed in a single message exchange (like PLAIN, unlike SCRAM).
//!
//! ## Wire Protocol
//!
//! The client message format follows RFC 7628 and Kafka's implementation:
//!
//! ```text
//! n,a=<authzid>,\x01auth=Bearer <token>\x01[extensions]\x01
//! ```
//!
//! - `n,` - GS2 header indicating no channel binding
//! - `a=<authzid>,` - Optional authorization identity
//! - `\x01` - SOH (Start of Header) delimiter
//! - `auth=Bearer <token>` - The bearer token (mandatory)
//! - Extensions: optional `key=value\x01` pairs
//!
//! ## Server Response
//!
//! - Success: empty bytes `[]`
//! - Error: JSON `{"status":"<error>","scope":"...","openid-configuration":"..."}`

use std::fmt::Debug;
use std::sync::Arc;

use tracing::{debug, warn};

use crate::config::{OAuthBearerConfig, TokenValidationMode};
use crate::error::{AuthError, AuthResult};

use super::{SaslAuthenticator, SaslSession, SaslStepResult};

/// SOH (Start of Header) delimiter used in OAUTHBEARER messages.
const SOH: char = '\x01';

/// Parsed OAUTHBEARER client message.
#[derive(Debug, Clone)]
pub struct OAuthBearerClientMessage {
    /// Authorization identity (the `a=` field, may be empty).
    pub authz_id: Option<String>,
    /// The bearer token extracted from `auth=Bearer <token>`.
    pub token: String,
    /// SASL extensions (key-value pairs).
    pub extensions: Vec<(String, String)>,
}

/// Token validation result.
#[derive(Debug, Clone)]
pub struct TokenValidationResult {
    /// Principal/subject extracted from token (e.g., `sub` claim).
    pub principal: String,
    /// Token lifetime in milliseconds (for session lifetime reporting).
    pub lifetime_ms: Option<i64>,
    /// Scopes granted by the token.
    pub scopes: Vec<String>,
}

/// Trait for validating OAuth bearer tokens.
///
/// Implementations can validate tokens against:
/// - No validation (accept all tokens - for testing/passthrough)
/// - JWT signature validation with JWKS
/// - Token introspection endpoint
pub trait TokenValidator: Send + Sync + Debug {
    /// Validate a bearer token and return the principal.
    fn validate(&self, token: &str) -> AuthResult<TokenValidationResult>;
}

/// Accept-all token validator (passthrough mode).
///
/// Extracts the principal from the JWT `sub` claim without verification.
/// Use this when the broker validates tokens or for testing.
#[derive(Debug, Default)]
pub struct NoOpTokenValidator;

impl TokenValidator for NoOpTokenValidator {
    fn validate(&self, token: &str) -> AuthResult<TokenValidationResult> {
        // Extract principal from JWT without verification
        let principal =
            extract_jwt_subject_unverified(token).unwrap_or_else(|| "oauth-user".to_string());

        debug!(principal = %principal, "OAUTHBEARER passthrough - extracted principal without validation");

        Ok(TokenValidationResult {
            principal,
            lifetime_ms: None,
            scopes: vec![],
        })
    }
}

/// Extract `sub` claim from JWT without signature verification.
///
/// JWT format: `header.payload.signature` (base64url encoded)
fn extract_jwt_subject_unverified(token: &str) -> Option<String> {
    // JWT format: header.payload.signature
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return None;
    }

    // Decode payload (base64url, may or may not have padding)
    let payload = base64_decode_url_safe(parts[1])?;
    let payload_str = std::str::from_utf8(&payload).ok()?;

    // Simple JSON parsing - look for "sub" field
    // Format: {"sub":"value",...} or {"sub": "value",...}
    extract_json_string_field(payload_str, "sub")
}

/// Decode base64url without padding.
fn base64_decode_url_safe(input: &str) -> Option<Vec<u8>> {
    // Add padding if needed
    let padding = (4 - input.len() % 4) % 4;
    let mut padded = input.to_string();
    for _ in 0..padding {
        padded.push('=');
    }

    // Convert from URL-safe alphabet
    let standard: String = padded
        .chars()
        .map(|c| match c {
            '-' => '+',
            '_' => '/',
            c => c,
        })
        .collect();

    // Use standard base64 decoding
    use std::io::Read;
    let mut decoder = base64::read::DecoderReader::new(
        standard.as_bytes(),
        &base64::engine::general_purpose::STANDARD,
    );
    let mut decoded = Vec::new();
    decoder.read_to_end(&mut decoded).ok()?;
    Some(decoded)
}

/// Extract a string field from JSON (simple parser, no dependencies).
fn extract_json_string_field(json: &str, field: &str) -> Option<String> {
    // Look for "field":"value" or "field": "value"
    let pattern = format!("\"{}\"", field);
    let start = json.find(&pattern)?;
    let rest = &json[start + pattern.len()..];

    // Skip whitespace and colon
    let rest = rest.trim_start();
    let rest = rest.strip_prefix(':')?;
    let rest = rest.trim_start();

    // Extract string value
    let rest = rest.strip_prefix('"')?;
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

/// SASL/OAUTHBEARER authenticator.
#[derive(Debug)]
pub struct OAuthBearerAuthenticator {
    validator: Arc<dyn TokenValidator>,
}

impl OAuthBearerAuthenticator {
    /// Create a new OAUTHBEARER authenticator with the given validator.
    pub fn new(validator: Arc<dyn TokenValidator>) -> Self {
        Self { validator }
    }

    /// Create an authenticator that accepts all tokens (passthrough mode).
    #[must_use]
    pub fn new_passthrough() -> Self {
        Self {
            validator: Arc::new(NoOpTokenValidator),
        }
    }

    /// Create an authenticator from configuration.
    pub fn from_config(config: Option<&OAuthBearerConfig>) -> AuthResult<Self> {
        let config = config.cloned().unwrap_or_default();

        match config.validation {
            TokenValidationMode::None => Ok(Self::new_passthrough()),
            TokenValidationMode::Jwt => {
                // JWT validation requires the oauthbearer-jwt feature
                #[cfg(feature = "oauthbearer-jwt")]
                {
                    let jwt_config = config.jwt.ok_or_else(|| {
                        AuthError::Configuration(
                            "JWT configuration required for JWT validation mode".to_string(),
                        )
                    })?;
                    let validator = super::jwt::JwtTokenValidator::new(jwt_config)?;
                    Ok(Self::new(Arc::new(validator)))
                }

                #[cfg(not(feature = "oauthbearer-jwt"))]
                {
                    Err(AuthError::Configuration(
                        "JWT validation requires the 'oauthbearer-jwt' feature to be enabled"
                            .to_string(),
                    ))
                }
            }
        }
    }

    /// Parse the OAUTHBEARER client message.
    ///
    /// Format: `n,a=<authzid>,\x01auth=Bearer <token>\x01[extensions]\x01`
    fn parse_client_message(message: &[u8]) -> AuthResult<OAuthBearerClientMessage> {
        let message_str = std::str::from_utf8(message).map_err(|_| {
            AuthError::InvalidMessage("Invalid UTF-8 in OAUTHBEARER message".to_string())
        })?;

        // Split by SOH delimiter
        let parts: Vec<&str> = message_str.split(SOH).collect();

        if parts.is_empty() {
            return Err(AuthError::InvalidMessage(
                "Empty OAUTHBEARER message".to_string(),
            ));
        }

        // First part is GS2 header: "n,a=<authzid>," or "n,,"
        let gs2_header = parts[0];
        let authz_id = Self::parse_gs2_authzid(gs2_header)?;

        // Find the auth= field containing the Bearer token
        let mut token: Option<String> = None;
        let mut extensions = Vec::new();

        for part in &parts[1..] {
            if part.is_empty() {
                continue;
            }

            if let Some(auth_value) = part.strip_prefix("auth=") {
                // auth=Bearer <token>
                if let Some(bearer_token) = auth_value
                    .strip_prefix("Bearer ")
                    .or_else(|| auth_value.strip_prefix("bearer "))
                {
                    token = Some(bearer_token.trim().to_string());
                } else {
                    return Err(AuthError::InvalidMessage(
                        "auth field must use 'Bearer' scheme".to_string(),
                    ));
                }
            } else if let Some(eq_pos) = part.find('=') {
                // Extension: key=value
                let key = &part[..eq_pos];
                let value = &part[eq_pos + 1..];
                extensions.push((key.to_string(), value.to_string()));
            }
        }

        let token = token.ok_or_else(|| {
            AuthError::InvalidMessage(
                "Missing auth=Bearer field in OAUTHBEARER message".to_string(),
            )
        })?;

        if token.is_empty() {
            return Err(AuthError::InvalidMessage("Empty bearer token".to_string()));
        }

        Ok(OAuthBearerClientMessage {
            authz_id,
            token,
            extensions,
        })
    }

    /// Parse authorization identity from GS2 header.
    ///
    /// Format: "n,a=<authzid>," or "n,," or "y,a=<authzid>,"
    fn parse_gs2_authzid(gs2_header: &str) -> AuthResult<Option<String>> {
        // Must start with 'n,' (no channel binding) or 'y,' (channel binding available)
        if !gs2_header.starts_with("n,") && !gs2_header.starts_with("y,") {
            // 'p' would indicate channel binding which we don't support
            if gs2_header.starts_with("p=") {
                return Err(AuthError::InvalidMessage(
                    "Channel binding not supported for OAUTHBEARER".to_string(),
                ));
            }
            return Err(AuthError::InvalidMessage(format!(
                "Invalid GS2 header: expected 'n,' or 'y,' prefix, got '{}'",
                gs2_header.chars().take(10).collect::<String>()
            )));
        }

        // Extract authzid if present
        let rest = &gs2_header[2..]; // Skip "n," or "y,"
        if let Some(authzid_part) = rest.strip_prefix("a=") {
            let authzid = authzid_part.trim_end_matches(',');
            if authzid.is_empty() {
                return Ok(None);
            }
            return Ok(Some(authzid.to_string()));
        }

        Ok(None)
    }

    /// Build an OAuth error response (JSON format per RFC 7628).
    fn build_error_response(status: &str, description: Option<&str>) -> Vec<u8> {
        let mut json = format!("{{\"status\":\"{status}\"");
        if let Some(desc) = description {
            // Escape quotes in description
            let escaped = desc.replace('\\', "\\\\").replace('"', "\\\"");
            json.push_str(&format!(",\"error_description\":\"{escaped}\""));
        }
        json.push('}');
        json.into_bytes()
    }
}

impl SaslAuthenticator for OAuthBearerAuthenticator {
    fn mechanism_name(&self) -> &'static str {
        "OAUTHBEARER"
    }

    fn authenticate_step(
        &self,
        client_message: &[u8],
        session: &mut SaslSession,
    ) -> SaslStepResult {
        // Check for error acknowledgment (single SOH byte)
        if client_message.len() == 1 && client_message[0] == SOH as u8 {
            debug!("OAUTHBEARER client acknowledged error");
            return SaslStepResult::Failed(AuthError::AuthenticationFailed(
                "Client acknowledged authentication error".to_string(),
            ));
        }

        // Parse the client message
        let parsed = match Self::parse_client_message(client_message) {
            Ok(msg) => msg,
            Err(e) => {
                warn!(error = %e, "Failed to parse OAUTHBEARER message");
                return SaslStepResult::Failed(e);
            }
        };

        debug!(
            authz_id = ?parsed.authz_id,
            token_len = parsed.token.len(),
            extensions = ?parsed.extensions,
            "OAUTHBEARER authentication attempt"
        );

        // Validate the token
        match self.validator.validate(&parsed.token) {
            Ok(result) => {
                // Use authz_id if provided, otherwise use token's principal
                let principal = parsed.authz_id.unwrap_or(result.principal);

                session.authenticated_user = Some(principal.clone());
                debug!(principal = %principal, "OAUTHBEARER authentication successful");

                // Success response is empty bytes
                SaslStepResult::Complete(Vec::new())
            }
            Err(e) => {
                warn!(error = %e, "OAUTHBEARER token validation failed");

                // Return error response in JSON format
                let error_response =
                    Self::build_error_response("invalid_token", Some(&e.to_string()));

                SaslStepResult::Continue(error_response)
            }
        }
    }

    fn is_complete(&self, session: &SaslSession) -> bool {
        session.authenticated_user.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mechanism_name() {
        let auth = OAuthBearerAuthenticator::new_passthrough();
        assert_eq!(auth.mechanism_name(), "OAUTHBEARER");
    }

    #[test]
    fn test_parse_valid_message_with_authzid() {
        let msg = b"n,a=user@example.com,\x01auth=Bearer eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.sig\x01\x01";
        let parsed = OAuthBearerAuthenticator::parse_client_message(msg).unwrap();

        assert_eq!(parsed.authz_id, Some("user@example.com".to_string()));
        assert!(parsed.token.starts_with("eyJhbGciOiJSUzI1NiJ9"));
        assert!(parsed.extensions.is_empty());
    }

    #[test]
    fn test_parse_valid_message_without_authzid() {
        let msg = b"n,,\x01auth=Bearer token123\x01\x01";
        let parsed = OAuthBearerAuthenticator::parse_client_message(msg).unwrap();

        assert_eq!(parsed.authz_id, None);
        assert_eq!(parsed.token, "token123");
    }

    #[test]
    fn test_parse_message_with_extensions() {
        let msg = b"n,,\x01auth=Bearer token123\x01scope=read write\x01\x01";
        let parsed = OAuthBearerAuthenticator::parse_client_message(msg).unwrap();

        assert_eq!(parsed.token, "token123");
        assert_eq!(parsed.extensions.len(), 1);
        assert_eq!(
            parsed.extensions[0],
            ("scope".to_string(), "read write".to_string())
        );
    }

    #[test]
    fn test_parse_message_case_insensitive_bearer() {
        let msg = b"n,,\x01auth=bearer token123\x01\x01";
        let parsed = OAuthBearerAuthenticator::parse_client_message(msg).unwrap();
        assert_eq!(parsed.token, "token123");
    }

    #[test]
    fn test_parse_invalid_missing_bearer() {
        let msg = b"n,,\x01auth=token123\x01\x01";
        let result = OAuthBearerAuthenticator::parse_client_message(msg);
        assert!(matches!(result, Err(AuthError::InvalidMessage(_))));
    }

    #[test]
    fn test_parse_invalid_empty_token() {
        let msg = b"n,,\x01auth=Bearer \x01\x01";
        let result = OAuthBearerAuthenticator::parse_client_message(msg);
        assert!(matches!(result, Err(AuthError::InvalidMessage(_))));
    }

    #[test]
    fn test_parse_invalid_channel_binding() {
        let msg = b"p=tls-unique,,\x01auth=Bearer token\x01\x01";
        let result = OAuthBearerAuthenticator::parse_client_message(msg);
        assert!(matches!(result, Err(AuthError::InvalidMessage(_))));
    }

    #[test]
    fn test_parse_invalid_gs2_header() {
        let msg = b"x,,\x01auth=Bearer token\x01\x01";
        let result = OAuthBearerAuthenticator::parse_client_message(msg);
        assert!(matches!(result, Err(AuthError::InvalidMessage(_))));
    }

    #[test]
    fn test_parse_missing_auth_field() {
        let msg = b"n,,\x01scope=read\x01\x01";
        let result = OAuthBearerAuthenticator::parse_client_message(msg);
        assert!(matches!(result, Err(AuthError::InvalidMessage(_))));
    }

    #[test]
    fn test_extract_jwt_subject() {
        // JWT with sub="test-user" (header: {"alg":"none"}, payload: {"sub":"test-user"})
        // header: eyJhbGciOiJub25lIn0
        // payload: eyJzdWIiOiJ0ZXN0LXVzZXIifQ
        let token = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.";
        let subject = extract_jwt_subject_unverified(token);
        assert_eq!(subject, Some("test-user".to_string()));
    }

    #[test]
    fn test_extract_jwt_subject_invalid_format() {
        let token = "not-a-jwt";
        let subject = extract_jwt_subject_unverified(token);
        assert_eq!(subject, None);
    }

    #[test]
    fn test_full_auth_passthrough() {
        let auth = OAuthBearerAuthenticator::new_passthrough();
        let mut session = SaslSession::new();

        // Create a simple JWT-like token (base64url encoded)
        // Header: {"alg":"none"}
        // Payload: {"sub":"alice"}
        let token = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJhbGljZSJ9.";
        let msg = format!("n,,\x01auth=Bearer {token}\x01\x01");

        let result = auth.authenticate_step(msg.as_bytes(), &mut session);

        assert!(matches!(result, SaslStepResult::Complete(_)));
        assert_eq!(session.authenticated_user, Some("alice".to_string()));
    }

    #[test]
    fn test_full_auth_with_authzid_override() {
        let auth = OAuthBearerAuthenticator::new_passthrough();
        let mut session = SaslSession::new();

        // Token has sub="alice" but authzid is "admin@example.com"
        let token = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJhbGljZSJ9.";
        let msg = format!("n,a=admin@example.com,\x01auth=Bearer {token}\x01\x01");

        let result = auth.authenticate_step(msg.as_bytes(), &mut session);

        assert!(matches!(result, SaslStepResult::Complete(_)));
        // authzid should override the token's subject
        assert_eq!(
            session.authenticated_user,
            Some("admin@example.com".to_string())
        );
    }

    #[test]
    fn test_error_acknowledgment() {
        let auth = OAuthBearerAuthenticator::new_passthrough();
        let mut session = SaslSession::new();

        // Single SOH byte = client acknowledging error
        let result = auth.authenticate_step(b"\x01", &mut session);

        assert!(matches!(result, SaslStepResult::Failed(_)));
    }

    #[test]
    fn test_is_complete() {
        let auth = OAuthBearerAuthenticator::new_passthrough();
        let mut session = SaslSession::new();

        assert!(!auth.is_complete(&session));

        session.authenticated_user = Some("alice".to_string());
        assert!(auth.is_complete(&session));
    }

    #[test]
    fn test_build_error_response() {
        let response =
            OAuthBearerAuthenticator::build_error_response("invalid_token", Some("Token expired"));
        let json = String::from_utf8(response).unwrap();

        assert!(json.contains("\"status\":\"invalid_token\""));
        assert!(json.contains("\"error_description\":\"Token expired\""));
    }

    #[test]
    fn test_extract_json_string_field() {
        let json = r#"{"sub":"alice","iss":"https://example.com"}"#;

        assert_eq!(
            extract_json_string_field(json, "sub"),
            Some("alice".to_string())
        );
        assert_eq!(
            extract_json_string_field(json, "iss"),
            Some("https://example.com".to_string())
        );
        assert_eq!(extract_json_string_field(json, "missing"), None);
    }

    #[test]
    fn test_base64_decode_url_safe() {
        // "test" in base64url without padding
        let encoded = "dGVzdA";
        let decoded = base64_decode_url_safe(encoded).unwrap();
        assert_eq!(decoded, b"test");

        // With URL-safe characters
        let encoded = "PDw_Pz4-"; // "<<??>>".to_vec() in base64url
        let decoded = base64_decode_url_safe(encoded).unwrap();
        assert_eq!(decoded, b"<<??>>");
    }
}
