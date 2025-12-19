//! SASL authentication request handlers.
//!
//! Handles client-side SASL authentication requests (SaslHandshake, SaslAuthenticate).

use bytes::{BufMut, BytesMut};
use kafka_protocol::messages::{
    SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
};
use kafka_protocol::protocol::Decodable;
use std::sync::Arc;
use tracing::{debug, warn};

use crate::auth::{SaslAuthenticator, SaslServer, SaslSession, SaslStepResult};
use crate::error::{AuthError, ProxyError, Result};
use crate::network::codec::ResponseFrame;

/// SASL authentication handler for client connections.
#[derive(Debug)]
pub struct SaslHandler {
    sasl_server: Arc<SaslServer>,
    session: Option<SaslSession>,
    authenticator: Option<Arc<dyn SaslAuthenticator>>,
}

impl SaslHandler {
    /// Create a new SASL handler.
    #[must_use]
    pub fn new(sasl_server: Arc<SaslServer>) -> Self {
        Self {
            sasl_server,
            session: None,
            authenticator: None,
        }
    }

    /// Check if authentication is complete.
    #[must_use]
    pub fn is_authenticated(&self) -> bool {
        self.session
            .as_ref()
            .map(|s| s.is_authenticated())
            .unwrap_or(false)
    }

    /// Get the authenticated username.
    #[must_use]
    pub fn authenticated_user(&self) -> Option<&str> {
        self.session
            .as_ref()
            .and_then(|s| s.authenticated_user.as_deref())
    }

    /// Handle SaslHandshake request (API key 17).
    ///
    /// Returns the enabled mechanisms and starts a SASL session for the selected mechanism.
    pub fn handle_sasl_handshake(
        &mut self,
        correlation_id: i32,
        api_version: i16,
        request_bytes: &[u8],
    ) -> Result<ResponseFrame> {
        // Decode the request
        let request =
            SaslHandshakeRequest::decode(&mut BytesMut::from(request_bytes).freeze(), api_version)
                .map_err(|e| ProxyError::ProtocolDecode {
                    message: format!("Failed to decode SaslHandshake request: {e}"),
                })?;

        let mechanism = request.mechanism.to_string();
        debug!(mechanism = %mechanism, "SASL handshake request");

        // Check if mechanism is supported
        let enabled_mechanisms = self.sasl_server.enabled_mechanisms();
        let error_code = if self.sasl_server.get_authenticator(&mechanism).is_some() {
            // Start session for this mechanism
            match self.sasl_server.start_session(&mechanism) {
                Ok((session, auth)) => {
                    self.session = Some(session);
                    self.authenticator = Some(auth);
                    0 // Success
                }
                Err(e) => {
                    warn!(error = %e, "Failed to start SASL session");
                    33 // UNSUPPORTED_SASL_MECHANISM
                }
            }
        } else {
            warn!(mechanism = %mechanism, "Unsupported SASL mechanism requested");
            33 // UNSUPPORTED_SASL_MECHANISM
        };

        // Build response
        let mut response = SaslHandshakeResponse::default();
        response.error_code = error_code;
        for mech in &enabled_mechanisms {
            response.mechanisms.push((*mech).into());
        }

        // Encode response
        let body = encode_sasl_handshake_response(&response, api_version)?;

        Ok(ResponseFrame {
            correlation_id,
            body,
        })
    }

    /// Handle SaslAuthenticate request (API key 36).
    ///
    /// Performs the actual authentication using the mechanism selected in handshake.
    pub fn handle_sasl_authenticate(
        &mut self,
        correlation_id: i32,
        api_version: i16,
        request_bytes: &[u8],
    ) -> Result<ResponseFrame> {
        // Decode the request
        let request = SaslAuthenticateRequest::decode(
            &mut BytesMut::from(request_bytes).freeze(),
            api_version,
        )
        .map_err(|e| ProxyError::ProtocolDecode {
            message: format!("Failed to decode SaslAuthenticate request: {e}"),
        })?;

        let auth_bytes = request.auth_bytes.as_ref();
        debug!(
            auth_bytes_len = auth_bytes.len(),
            "SASL authenticate request"
        );

        // Check that we have an active session
        let (session, authenticator) = match (&mut self.session, &self.authenticator) {
            (Some(session), Some(auth)) => (session, auth),
            _ => {
                warn!("SaslAuthenticate received without prior SaslHandshake");
                return build_auth_error_response(
                    correlation_id,
                    api_version,
                    58, // SASL_AUTHENTICATION_FAILED
                    "No SASL session active - send SaslHandshake first",
                );
            }
        };

        // Perform authentication step
        let result = authenticator.authenticate_step(auth_bytes, session);

        match result {
            SaslStepResult::Complete(response_bytes) => {
                debug!(
                    user = ?session.authenticated_user,
                    "SASL authentication successful"
                );
                build_auth_success_response(correlation_id, api_version, &response_bytes)
            }
            SaslStepResult::Continue(challenge) => {
                debug!("SASL authentication continuing, sending challenge");
                build_auth_continue_response(correlation_id, api_version, &challenge)
            }
            SaslStepResult::Failed(err) => {
                warn!(error = %err, "SASL authentication failed");
                build_auth_error_response(
                    correlation_id,
                    api_version,
                    58, // SASL_AUTHENTICATION_FAILED
                    &err.to_string(),
                )
            }
        }
    }
}

/// Encode a SaslHandshakeResponse.
fn encode_sasl_handshake_response(
    response: &SaslHandshakeResponse,
    api_version: i16,
) -> Result<BytesMut> {
    let mut body = BytesMut::new();

    // Error code (2 bytes)
    body.put_i16(response.error_code);

    // Mechanisms array
    if api_version >= 1 {
        // Compact array format for v1+
        let len = response.mechanisms.len() as i32 + 1; // +1 for compact encoding
        body.put_u8(len as u8); // varint encoding (simplified for small arrays)
        for mech in &response.mechanisms {
            let mech_bytes = mech.as_str().as_bytes();
            body.put_u8((mech_bytes.len() + 1) as u8); // compact string length
            body.put_slice(mech_bytes);
        }
        body.put_u8(0); // tagged fields
    } else {
        // V0 format
        body.put_i32(response.mechanisms.len() as i32);
        for mech in &response.mechanisms {
            let mech_bytes = mech.as_str().as_bytes();
            body.put_i16(mech_bytes.len() as i16);
            body.put_slice(mech_bytes);
        }
    }

    Ok(body)
}

/// Build a successful authentication response.
fn build_auth_success_response(
    correlation_id: i32,
    api_version: i16,
    auth_bytes: &[u8],
) -> Result<ResponseFrame> {
    let mut body = BytesMut::new();

    // Error code = 0 (success)
    body.put_i16(0);

    if api_version >= 2 {
        // Compact bytes for v2+
        let len = auth_bytes.len() as i32 + 1;
        body.put_u8(len as u8);
        body.put_slice(auth_bytes);
    } else {
        // V0/V1 format
        body.put_i32(auth_bytes.len() as i32);
        body.put_slice(auth_bytes);
    }

    // Error message (null/empty)
    if api_version >= 2 {
        body.put_u8(1); // compact nullable string = null (length 1 means empty)
    } else {
        body.put_i16(-1); // nullable string = null
    }

    // Session lifetime (v1+)
    if api_version >= 1 {
        body.put_i64(0); // No session lifetime
    }

    // Tagged fields (v2+)
    if api_version >= 2 {
        body.put_u8(0);
    }

    Ok(ResponseFrame {
        correlation_id,
        body,
    })
}

/// Build a continue response (for multi-step auth like SCRAM).
fn build_auth_continue_response(
    correlation_id: i32,
    api_version: i16,
    challenge: &[u8],
) -> Result<ResponseFrame> {
    // Same as success but with challenge bytes
    build_auth_success_response(correlation_id, api_version, challenge)
}

/// Build an authentication error response.
fn build_auth_error_response(
    correlation_id: i32,
    api_version: i16,
    error_code: i16,
    error_message: &str,
) -> Result<ResponseFrame> {
    let mut body = BytesMut::new();

    // Error code
    body.put_i16(error_code);

    // Auth bytes (empty on error)
    if api_version >= 2 {
        body.put_u8(1); // compact bytes = empty
    } else {
        body.put_i32(0);
    }

    // Error message
    let msg_bytes = error_message.as_bytes();
    if api_version >= 2 {
        let len = msg_bytes.len() as i32 + 1;
        body.put_u8(len as u8);
        body.put_slice(msg_bytes);
    } else {
        body.put_i16(msg_bytes.len() as i16);
        body.put_slice(msg_bytes);
    }

    // Session lifetime (v1+)
    if api_version >= 1 {
        body.put_i64(0);
    }

    // Tagged fields (v2+)
    if api_version >= 2 {
        body.put_u8(0);
    }

    Ok(ResponseFrame {
        correlation_id,
        body,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClientSaslConfig, CredentialConfig, SaslMechanism, UserCredential};

    fn test_sasl_server() -> Arc<SaslServer> {
        let config = ClientSaslConfig {
            enabled_mechanisms: vec![SaslMechanism::Plain],
            credentials: CredentialConfig::Inline {
                users: vec![UserCredential {
                    username: "test_user".to_string(),
                    password: "test_pass".to_string(),
                }],
            },
            oauthbearer: None,
        };
        Arc::new(SaslServer::new(&config).unwrap())
    }

    #[test]
    fn test_sasl_handler_creation() {
        let server = test_sasl_server();
        let handler = SaslHandler::new(server);
        assert!(!handler.is_authenticated());
        assert!(handler.authenticated_user().is_none());
    }

    #[test]
    fn test_sasl_handshake_supported_mechanism() {
        let server = test_sasl_server();
        let mut handler = SaslHandler::new(server);

        // Build a simple v0 SaslHandshake request for PLAIN
        let mut request_bytes = BytesMut::new();
        request_bytes.put_i16(5); // mechanism length
        request_bytes.put_slice(b"PLAIN");

        let response = handler.handle_sasl_handshake(1, 0, &request_bytes).unwrap();
        assert_eq!(response.correlation_id, 1);

        // Verify session was started
        assert!(handler.session.is_some());
        assert!(handler.authenticator.is_some());
    }

    #[test]
    fn test_full_authentication_flow() {
        let server = test_sasl_server();
        let mut handler = SaslHandler::new(server);

        // Step 1: Handshake
        let mut handshake_bytes = BytesMut::new();
        handshake_bytes.put_i16(5);
        handshake_bytes.put_slice(b"PLAIN");

        let _ = handler
            .handle_sasl_handshake(1, 0, &handshake_bytes)
            .unwrap();

        // Step 2: Authenticate
        let auth_data = b"\0test_user\0test_pass";
        let mut auth_bytes = BytesMut::new();
        auth_bytes.put_i32(auth_data.len() as i32);
        auth_bytes.put_slice(auth_data);

        let response = handler.handle_sasl_authenticate(2, 0, &auth_bytes).unwrap();
        assert_eq!(response.correlation_id, 2);

        // Verify authentication succeeded
        assert!(handler.is_authenticated());
        assert_eq!(handler.authenticated_user(), Some("test_user"));
    }

    #[test]
    fn test_authenticate_without_handshake() {
        let server = test_sasl_server();
        let mut handler = SaslHandler::new(server);

        // Try to authenticate without handshake
        let auth_data = b"\0test_user\0test_pass";
        let mut auth_bytes = BytesMut::new();
        auth_bytes.put_i32(auth_data.len() as i32);
        auth_bytes.put_slice(auth_data);

        let response = handler.handle_sasl_authenticate(1, 0, &auth_bytes).unwrap();

        // Should get error response (check first 2 bytes for error code)
        // Error code 58 as big-endian i16 is [0, 58]
        assert_eq!(response.body[0..2], [0, 58]);
    }
}
