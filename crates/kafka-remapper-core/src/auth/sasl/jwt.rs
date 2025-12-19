//! JWT token validation for OAUTHBEARER.
//!
//! This module provides JWT signature and claims validation for OAUTHBEARER
//! authentication. It is only compiled when the `oauthbearer-jwt` feature is enabled.
//!
//! Note: Full JWT validation requires additional dependencies (jsonwebtoken, reqwest)
//! for JWKS fetching and signature verification. This is a stub implementation
//! that will be expanded when those dependencies are added.

use std::fmt::Debug;

use crate::config::JwtValidationConfig;
use crate::error::{AuthError, AuthResult};

use super::oauthbearer::{TokenValidationResult, TokenValidator};

/// JWT token validator that validates OAuth bearer tokens using JWKS.
#[derive(Debug)]
pub struct JwtTokenValidator {
    #[allow(dead_code)]
    config: JwtValidationConfig,
}

impl JwtTokenValidator {
    /// Create a new JWT token validator from configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid or JWKS cannot be loaded.
    pub fn new(config: JwtValidationConfig) -> AuthResult<Self> {
        // Validate configuration
        if config.jwks_url.is_none() && config.jwks_inline.is_none() {
            return Err(AuthError::Configuration(
                "Either jwks_url or jwks_inline must be provided".to_string(),
            ));
        }

        // TODO: Add actual JWT validation dependencies (jsonwebtoken, reqwest)
        // and implement proper JWKS loading and caching.
        //
        // For now, this is a placeholder that allows the code to compile.
        // Full implementation would:
        // 1. Fetch JWKS from jwks_url or parse jwks_inline
        // 2. Cache the JWKS with configurable refresh
        // 3. Validate JWT signatures using the appropriate key
        // 4. Validate claims (iss, aud, exp, etc.)

        Ok(Self { config })
    }
}

impl TokenValidator for JwtTokenValidator {
    fn validate(&self, token: &str) -> AuthResult<TokenValidationResult> {
        // Extract principal from the token's sub claim without verification
        // This is a temporary implementation - full JWT validation would verify
        // the signature and all claims before extracting the principal.
        let principal = extract_jwt_subject(token).ok_or_else(|| {
            AuthError::AuthenticationFailed("Failed to extract subject from JWT".to_string())
        })?;

        // Validate issuer if configured
        if let Some(expected_issuer) = &self.config.issuer {
            let token_issuer = extract_jwt_claim(token, "iss");
            if token_issuer.as_ref() != Some(expected_issuer) {
                return Err(AuthError::AuthenticationFailed(format!(
                    "Invalid issuer: expected '{}', got {:?}",
                    expected_issuer, token_issuer
                )));
            }
        }

        // Validate audience if configured
        if let Some(expected_audience) = &self.config.audience {
            let token_audience = extract_jwt_claim(token, "aud");
            if token_audience.as_ref() != Some(expected_audience) {
                return Err(AuthError::AuthenticationFailed(format!(
                    "Invalid audience: expected '{}', got {:?}",
                    expected_audience, token_audience
                )));
            }
        }

        // TODO: Implement actual signature verification and expiration checks
        // when jsonwebtoken dependency is added.

        Ok(TokenValidationResult {
            principal,
            lifetime_ms: None,
            scopes: vec![],
        })
    }
}

/// Extract the `sub` claim from a JWT without signature verification.
fn extract_jwt_subject(token: &str) -> Option<String> {
    extract_jwt_claim(token, "sub")
}

/// Extract a claim from a JWT payload without signature verification.
fn extract_jwt_claim(token: &str, claim: &str) -> Option<String> {
    // JWT format: header.payload.signature
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return None;
    }

    // Decode payload (base64url)
    let payload = base64_decode_url_safe(parts[1])?;
    let payload_str = std::str::from_utf8(&payload).ok()?;

    // Simple JSON parsing for the claim
    extract_json_string_field(payload_str, claim)
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

/// Extract a string field from JSON (simple parser).
fn extract_json_string_field(json: &str, field: &str) -> Option<String> {
    let pattern = format!("\"{}\"", field);
    let start = json.find(&pattern)?;
    let rest = &json[start + pattern.len()..];

    let rest = rest.trim_start();
    let rest = rest.strip_prefix(':')?;
    let rest = rest.trim_start();

    let rest = rest.strip_prefix('"')?;
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jwt_validator_requires_jwks() {
        let config = JwtValidationConfig {
            jwks_url: None,
            jwks_inline: None,
            issuer: None,
            audience: None,
            required_scopes: vec![],
            clock_skew_seconds: 60,
            jwks_refresh_interval_secs: 3600,
        };

        let result = JwtTokenValidator::new(config);
        assert!(result.is_err());
    }

    #[test]
    fn test_jwt_validator_with_jwks_url() {
        let config = JwtValidationConfig {
            jwks_url: Some("https://example.com/.well-known/jwks.json".to_string()),
            jwks_inline: None,
            issuer: None,
            audience: None,
            required_scopes: vec![],
            clock_skew_seconds: 60,
            jwks_refresh_interval_secs: 3600,
        };

        let result = JwtTokenValidator::new(config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_extract_jwt_subject() {
        // JWT with sub="test-user" (header: {"alg":"none"}, payload: {"sub":"test-user"})
        let token = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.";
        let subject = extract_jwt_subject(token);
        assert_eq!(subject, Some("test-user".to_string()));
    }

    #[test]
    fn test_validate_token_basic() {
        let config = JwtValidationConfig {
            jwks_url: Some("https://example.com/.well-known/jwks.json".to_string()),
            jwks_inline: None,
            issuer: None,
            audience: None,
            required_scopes: vec![],
            clock_skew_seconds: 60,
            jwks_refresh_interval_secs: 3600,
        };

        let validator = JwtTokenValidator::new(config).unwrap();

        // Token with sub="alice"
        let token = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJhbGljZSJ9.";
        let result = validator.validate(token).unwrap();
        assert_eq!(result.principal, "alice");
    }

    #[test]
    fn test_validate_token_with_issuer_check() {
        let config = JwtValidationConfig {
            jwks_url: Some("https://example.com/.well-known/jwks.json".to_string()),
            jwks_inline: None,
            issuer: Some("https://auth.example.com".to_string()),
            audience: None,
            required_scopes: vec![],
            clock_skew_seconds: 60,
            jwks_refresh_interval_secs: 3600,
        };

        let validator = JwtTokenValidator::new(config).unwrap();

        // Token with sub="alice" but wrong issuer - should fail
        let token = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJhbGljZSJ9.";
        let result = validator.validate(token);
        assert!(result.is_err());
    }
}
