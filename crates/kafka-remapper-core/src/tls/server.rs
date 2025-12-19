//! TLS server acceptor for inbound client connections.
//!
//! Provides TLS support for the proxy's connections from Kafka clients.
//! The proxy acts as a TLS server, presenting certificates to clients.

use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rustls::crypto::ring::default_provider;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::danger::ClientCertVerifier;
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor as TokioTlsAcceptor;
use tracing::{debug, warn};

use crate::config::ClientTlsConfig;
use crate::error::{TlsError, TlsResult};

/// Install the ring crypto provider if not already installed.
fn ensure_crypto_provider() {
    // Try to install the ring provider, ignore errors if already installed
    let _ = CryptoProvider::install_default(default_provider());
}

/// TLS acceptor for inbound connections from Kafka clients.
///
/// This acceptor wraps TCP connections in TLS, handling certificate presentation
/// and optional client certificate verification (mTLS).
#[derive(Clone)]
pub struct TlsServerAcceptor {
    inner: TokioTlsAcceptor,
}

impl TlsServerAcceptor {
    /// Create a new TLS acceptor from configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - TLS configuration specifying server certificate and optional client auth
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Server certificate or key files cannot be loaded
    /// - CA certificate for client verification cannot be loaded
    /// - TLS configuration is invalid
    pub fn new(config: &ClientTlsConfig) -> TlsResult<Self> {
        ensure_crypto_provider();
        let server_config = build_server_config(config)?;
        Ok(Self {
            inner: TokioTlsAcceptor::from(Arc::new(server_config)),
        })
    }

    /// Accept a TLS connection from a client.
    ///
    /// # Arguments
    ///
    /// * `stream` - The underlying TCP stream to wrap
    ///
    /// # Errors
    ///
    /// Returns an error if the TLS handshake fails.
    pub async fn accept(&self, stream: TcpStream) -> TlsResult<TlsStream<TcpStream>> {
        debug!("accepting TLS connection from client");

        self.inner
            .accept(stream)
            .await
            .map_err(|e| TlsError::Handshake(e.to_string()))
    }
}

/// Build a rustls `ServerConfig` from our configuration.
fn build_server_config(config: &ClientTlsConfig) -> TlsResult<ServerConfig> {
    // Load server certificate chain
    let certs = load_certificates(&config.cert_path)?;
    let key = load_private_key(&config.key_path)?;

    // Build the server config
    let builder = ServerConfig::builder();

    let server_config = if config.require_client_cert {
        // mTLS mode: require and verify client certificates
        let ca_path = config.ca_cert_path.as_ref().ok_or_else(|| {
            TlsError::Config(
                "CA certificate path required when require_client_cert is true".to_string(),
            )
        })?;

        debug!(ca_path = %ca_path.display(), "configuring mTLS with client certificate verification");

        let client_cert_verifier = build_client_verifier(ca_path)?;

        builder
            .with_client_cert_verifier(client_cert_verifier)
            .with_single_cert(certs, key)
            .map_err(|e| TlsError::Config(format!("failed to configure server cert: {e}")))?
    } else if let Some(ca_path) = &config.ca_cert_path {
        // Optional client cert: verify if provided
        debug!(ca_path = %ca_path.display(), "configuring optional client certificate verification");

        let client_cert_verifier = build_optional_client_verifier(ca_path)?;

        builder
            .with_client_cert_verifier(client_cert_verifier)
            .with_single_cert(certs, key)
            .map_err(|e| TlsError::Config(format!("failed to configure server cert: {e}")))?
    } else {
        // No client certificate verification
        debug!("configuring TLS without client certificate verification");

        builder
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| TlsError::Config(format!("failed to configure server cert: {e}")))?
    };

    Ok(server_config)
}

/// Build a client certificate verifier that requires client certs.
fn build_client_verifier(ca_path: &Path) -> TlsResult<Arc<dyn ClientCertVerifier>> {
    let root_store = build_root_store(ca_path)?;

    WebPkiClientVerifier::builder(Arc::new(root_store))
        .build()
        .map_err(|e| TlsError::Config(format!("failed to build client verifier: {e}")))
}

/// Build a client certificate verifier that optionally verifies client certs.
fn build_optional_client_verifier(
    ca_path: &Path,
) -> TlsResult<Arc<dyn ClientCertVerifier>> {
    let root_store = build_root_store(ca_path)?;

    WebPkiClientVerifier::builder(Arc::new(root_store))
        .allow_unauthenticated()
        .build()
        .map_err(|e| TlsError::Config(format!("failed to build client verifier: {e}")))
}

/// Build the root certificate store for client verification.
fn build_root_store(ca_path: &Path) -> TlsResult<RootCertStore> {
    let mut root_store = RootCertStore::empty();

    debug!(path = %ca_path.display(), "loading CA certificate for client verification");
    let certs = load_certificates(ca_path)?;
    let (added, _ignored) = root_store.add_parsable_certificates(certs);
    debug!(added, "added CA certificates to client verification store");

    if added == 0 {
        return Err(TlsError::NoCertificates(ca_path.display().to_string()));
    }

    Ok(root_store)
}

/// Load certificates from a PEM file.
fn load_certificates(path: &Path) -> TlsResult<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(path).map_err(|e| TlsError::CertificateLoad {
        path: path.display().to_string(),
        message: e.to_string(),
    })?;

    let mut reader = BufReader::new(file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .filter_map(|result| match result {
            Ok(cert) => Some(cert),
            Err(e) => {
                warn!(error = %e, "skipping invalid certificate");
                None
            }
        })
        .collect();

    if certs.is_empty() {
        return Err(TlsError::NoCertificates(path.display().to_string()));
    }

    debug!(count = certs.len(), path = %path.display(), "loaded certificates");
    Ok(certs)
}

/// Load a private key from a PEM file.
fn load_private_key(path: &Path) -> TlsResult<PrivateKeyDer<'static>> {
    let file = std::fs::File::open(path).map_err(|e| TlsError::PrivateKeyLoad {
        path: path.display().to_string(),
        message: e.to_string(),
    })?;

    let mut reader = BufReader::new(file);

    // Try to read any type of private key (RSA, PKCS8, EC)
    loop {
        match rustls_pemfile::read_one(&mut reader) {
            Ok(Some(rustls_pemfile::Item::Pkcs1Key(key))) => {
                debug!(path = %path.display(), "loaded PKCS#1 RSA private key");
                return Ok(PrivateKeyDer::Pkcs1(key));
            }
            Ok(Some(rustls_pemfile::Item::Pkcs8Key(key))) => {
                debug!(path = %path.display(), "loaded PKCS#8 private key");
                return Ok(PrivateKeyDer::Pkcs8(key));
            }
            Ok(Some(rustls_pemfile::Item::Sec1Key(key))) => {
                debug!(path = %path.display(), "loaded SEC1 EC private key");
                return Ok(PrivateKeyDer::Sec1(key));
            }
            Ok(Some(_)) => {
                // Skip non-key items (certificates, etc.)
                continue;
            }
            Ok(None) => {
                // End of file
                break;
            }
            Err(e) => {
                return Err(TlsError::PrivateKeyLoad {
                    path: path.display().to_string(),
                    message: e.to_string(),
                });
            }
        }
    }

    Err(TlsError::NoPrivateKeys(path.display().to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;
    use tempfile::NamedTempFile;

    // Self-signed test certificate and key (for testing only)
    const TEST_CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIIC/zCCAeegAwIBAgIUHZciHaWd7ShdIRd77iIRL+AQ+eswDQYJKoZIhvcNAQEL
BQAwDzENMAsGA1UEAwwEdGVzdDAeFw0yNTEyMDkyMTA0MTZaFw0yNjEyMDkyMTA0
MTZaMA8xDTALBgNVBAMMBHRlc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK
AoIBAQC/P2tCibhR7rmIYqozEgCCWeKiMEw+TQNVQsjWIV/IV5eovbQ/+VwjUfXW
q7Hn51njAZ71NA0gJJ9dsThe6CbsqFuovjYkJhp62RQNbGq4Uw55cyqnKzYeW7e3
uLH7bgXvStsWoAvR+IZs0bKl6k48EyfILqhTNgcwoPGNpQi7wi5RKIC8nBsjLDKY
svcpUa2De0czrScLi+ihhiEY1HftxBbwBrjtVuYho8K5D+KshxHGxHcdwM2UnnlF
Gj219q0hLjkWT/xJA9QU5eOL5nZ+PQwmH4Scq1m3OX8tobeb1gyt+a2Y4D88kTLq
QSKfERIiWlTmWMsKeD5scLh+hwvTAgMBAAGjUzBRMB0GA1UdDgQWBBQeaF4xjsT0
o66q57PjKd6c7vQ6/zAfBgNVHSMEGDAWgBQeaF4xjsT0o66q57PjKd6c7vQ6/zAP
BgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQC9Mb0xwAXX0Ypo4BaC
C024DEpXMBzJkFShm3bCShUqZXpubfFiRcwtal5mfMBzWRxZIWLcxgRXfNhJWM8v
6fqb7WaREipGF9gOc0QvTxLIfO0V5DjD6j2LJQVhPVBdcGZIE+e628qAHkzpiPcU
BFvXNWPXOabDR/sx+Q224RPlNEsBIohtkAdL3AmvNlf+M0/KR5wp59VQDj6Ubabl
I109v8uD6JRc+P+HyaOgY97XNgBnIb9R2RPCd3/dacXXveCs27y7u+YuKW2nYRc6
6i7Riip2hupqP7Lx6Z9jOlsWpIsabZGJAwFoHL9FUjhlZH/rdEzo84/h3jOtaSD4
b/te
-----END CERTIFICATE-----"#;

    const TEST_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC/P2tCibhR7rmI
YqozEgCCWeKiMEw+TQNVQsjWIV/IV5eovbQ/+VwjUfXWq7Hn51njAZ71NA0gJJ9d
sThe6CbsqFuovjYkJhp62RQNbGq4Uw55cyqnKzYeW7e3uLH7bgXvStsWoAvR+IZs
0bKl6k48EyfILqhTNgcwoPGNpQi7wi5RKIC8nBsjLDKYsvcpUa2De0czrScLi+ih
hiEY1HftxBbwBrjtVuYho8K5D+KshxHGxHcdwM2UnnlFGj219q0hLjkWT/xJA9QU
5eOL5nZ+PQwmH4Scq1m3OX8tobeb1gyt+a2Y4D88kTLqQSKfERIiWlTmWMsKeD5s
cLh+hwvTAgMBAAECggEADrjeE+gwJTaAV8xol7faDC7JMH0RUXZyPD0A4uL80ZpU
lWvNFWOnwRxNFXJwJo77r2rvhqa0H/ZRwk+jLEMow+0N6UaDOnModK6DSak/6eKS
6ayA6w97ggjDcsQoB1fn4wzbIrm9TzOXfYcC/pyz2xIKbPGSiZ1OHmM1VRcQPgvJ
lmWWlrTzJYRmW6KjSVQzP0p3V/OdTsxgENOXQEmMq0dKJaUvFSZ2HYGZJmQgg8VY
TjI/TGIbdvGx/UyTjnFO0OPq4xhVgYXrABDMvAUDXkljEY61sFtCsevEXWQnW8Ym
W3ZdvbUqvEavn7LLoYr+dlMWyezQ3gcoNhkn/Kn0UQKBgQDmpyVYkQfAPZRf2Qea
o3unoc/13f4z82sIVRmeedfPuC1O7NafI1uUSiLC94aI1lUlQOd/StC/92TGlgNc
8lUMC8Vlr4mxcMPX3GQyqUrGHbAWbXUKExqKA/F1QbwqWbeeZfxStL9lHnUaC/7L
2m4X1R5DiVW7KoW+USo1iPbMGwKBgQDUQ7R0bCX+7SBHQOmtnL9PvSYImSyTrQZ/
HWb5q8jMs9cnKNKYOW/qEslgXy6Tb39ns0AYa4CT7dkwBSwLly/mfYxbfo/dcwvY
ZZOqC0QwFTWP1OP1VTN95JSYjYnfD2aHxibNUERZj/TTr4DWhcjh+r+wslTe6lkx
VwhLwnfKqQKBgBMqtJnFg4VgGKJWYKFjEHV/ps5hoiwjADPzDmvy6BIk1e8HE1aq
E4QhHP5in1VjqjOsTxBu4SXyovc1pBXnNVYI7GBk0+Zg3oVjlRf4pXQNJ4LVmbI6
oCvz4+7AhahnSDDrfKpKxtTaURTXBldeUWO9nAQ0t2EUSYTlLcLBHPEdAoGBAJB7
WVyZtK82Nu9pRuYOuMYNCNN3d7k5YB+sIsi1XmO/0iZsihRlnEDm8r2vbCOdFErA
31L/8bA/iMM/8gAds9QfByfMGR7yTVDJq15mds6H0UKK9XOrv/XkXiUMypjTgcXP
YeAEz9FqxIpGftsGi3sOU+ZxLIXjXDzSceonf6SpAoGAVg0dD9XmBFzHAMWxpf/X
NpMPmVcZspBoI9V62B3AohZQcCXvYAF5HE6HOR8+lF7/2mu0utQVhTRR57taXDTl
5PhKQItP6NfRgBjgiCA/m9GOUw3t3+9nVKW8KWBmNQXuMMdX2J0rRrvuuljdtQwf
z6oCYD97ZaLrS2AUbvCJZAw=
-----END PRIVATE KEY-----"#;

    fn create_temp_cert_file() -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(TEST_CERT.as_bytes()).unwrap();
        file.flush().unwrap();
        file
    }

    fn create_temp_key_file() -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(TEST_KEY.as_bytes()).unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn test_load_server_certificates() {
        let cert_file = create_temp_cert_file();
        let certs = load_certificates(cert_file.path()).unwrap();
        assert_eq!(certs.len(), 1);
    }

    #[test]
    fn test_load_server_private_key() {
        let key_file = create_temp_key_file();
        let key = load_private_key(key_file.path());
        assert!(key.is_ok());
    }

    #[test]
    fn test_acceptor_without_client_auth() {
        let cert_file = create_temp_cert_file();
        let key_file = create_temp_key_file();

        let config = ClientTlsConfig {
            cert_path: cert_file.path().to_path_buf(),
            key_path: key_file.path().to_path_buf(),
            ca_cert_path: None,
            require_client_cert: false,
            principal: Default::default(),
        };

        let acceptor = TlsServerAcceptor::new(&config);
        assert!(acceptor.is_ok(), "Expected Ok, got: {:?}", acceptor.err());
    }

    #[test]
    fn test_acceptor_with_optional_client_auth() {
        let cert_file = create_temp_cert_file();
        let key_file = create_temp_key_file();

        let config = ClientTlsConfig {
            cert_path: cert_file.path().to_path_buf(),
            key_path: key_file.path().to_path_buf(),
            ca_cert_path: Some(cert_file.path().to_path_buf()),
            require_client_cert: false,
            principal: Default::default(),
        };

        let acceptor = TlsServerAcceptor::new(&config);
        assert!(acceptor.is_ok(), "Expected Ok, got: {:?}", acceptor.err());
    }

    #[test]
    fn test_acceptor_with_required_client_auth() {
        let cert_file = create_temp_cert_file();
        let key_file = create_temp_key_file();

        let config = ClientTlsConfig {
            cert_path: cert_file.path().to_path_buf(),
            key_path: key_file.path().to_path_buf(),
            ca_cert_path: Some(cert_file.path().to_path_buf()),
            require_client_cert: true,
            principal: Default::default(),
        };

        let acceptor = TlsServerAcceptor::new(&config);
        assert!(acceptor.is_ok(), "Expected Ok, got: {:?}", acceptor.err());
    }

    #[test]
    fn test_acceptor_missing_ca_for_mtls() {
        let cert_file = create_temp_cert_file();
        let key_file = create_temp_key_file();

        let config = ClientTlsConfig {
            cert_path: cert_file.path().to_path_buf(),
            key_path: key_file.path().to_path_buf(),
            ca_cert_path: None,
            require_client_cert: true,
            principal: Default::default(),
        };

        let acceptor = TlsServerAcceptor::new(&config);
        assert!(acceptor.is_err());
    }

    #[test]
    fn test_acceptor_missing_cert() {
        let key_file = create_temp_key_file();

        let config = ClientTlsConfig {
            cert_path: PathBuf::from("/nonexistent/cert.pem"),
            key_path: key_file.path().to_path_buf(),
            ca_cert_path: None,
            require_client_cert: false,
            principal: Default::default(),
        };

        let acceptor = TlsServerAcceptor::new(&config);
        assert!(acceptor.is_err());
    }

    #[test]
    fn test_acceptor_missing_key() {
        let cert_file = create_temp_cert_file();

        let config = ClientTlsConfig {
            cert_path: cert_file.path().to_path_buf(),
            key_path: PathBuf::from("/nonexistent/key.pem"),
            ca_cert_path: None,
            require_client_cert: false,
            principal: Default::default(),
        };

        let acceptor = TlsServerAcceptor::new(&config);
        assert!(acceptor.is_err());
    }
}
