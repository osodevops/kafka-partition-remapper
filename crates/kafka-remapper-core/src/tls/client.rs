//! TLS client connector for outbound broker connections.
//!
//! Provides TLS support for the proxy's connections to Kafka brokers.

use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rustls::crypto::ring::default_provider;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector as TokioTlsConnector;
use tracing::{debug, warn};

use crate::config::BrokerTlsConfig;
use crate::error::{TlsError, TlsResult};

/// Install the ring crypto provider if not already installed.
fn ensure_crypto_provider() {
    // Try to install the ring provider, ignore errors if already installed
    let _ = CryptoProvider::install_default(default_provider());
}

/// TLS connector for outbound connections to Kafka brokers.
///
/// This connector wraps TCP connections in TLS, handling certificate loading
/// and verification according to the provided configuration.
#[derive(Clone)]
pub struct TlsConnector {
    inner: TokioTlsConnector,
}

impl TlsConnector {
    /// Create a new TLS connector from configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - TLS configuration specifying certificates and verification options
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Certificate files cannot be loaded
    /// - Private key files cannot be loaded
    /// - TLS configuration is invalid
    pub fn new(config: &BrokerTlsConfig) -> TlsResult<Self> {
        ensure_crypto_provider();
        let client_config = build_client_config(config)?;
        Ok(Self {
            inner: TokioTlsConnector::from(Arc::new(client_config)),
        })
    }

    /// Create a TLS connector using system root certificates.
    ///
    /// This is a convenience method for connecting to brokers with
    /// certificates signed by well-known CAs.
    ///
    /// # Errors
    ///
    /// Returns an error if the TLS configuration cannot be built.
    pub fn with_native_roots() -> TlsResult<Self> {
        let config = BrokerTlsConfig::default();
        Self::new(&config)
    }

    /// Connect to a server over TLS.
    ///
    /// # Arguments
    ///
    /// * `server_name` - The hostname for SNI and certificate verification
    /// * `stream` - The underlying TCP stream to wrap
    ///
    /// # Errors
    ///
    /// Returns an error if the TLS handshake fails.
    pub async fn connect(
        &self,
        server_name: &str,
        stream: TcpStream,
    ) -> TlsResult<TlsStream<TcpStream>> {
        let server_name = ServerName::try_from(server_name.to_string())
            .map_err(|e| TlsError::Config(format!("invalid server name: {e}")))?;

        debug!("initiating TLS handshake");

        self.inner
            .connect(server_name, stream)
            .await
            .map_err(|e| TlsError::Handshake(e.to_string()))
    }
}

/// Build a rustls `ClientConfig` from our configuration.
fn build_client_config(config: &BrokerTlsConfig) -> TlsResult<ClientConfig> {
    let root_store = build_root_store(config)?;

    let builder = ClientConfig::builder().with_root_certificates(root_store);

    // Check if we need client certificates (mTLS to broker)
    let client_config = if let (Some(cert_path), Some(key_path)) =
        (&config.cert_path, &config.key_path)
    {
        debug!("loading client certificate for mTLS");
        let certs = load_certificates(cert_path)?;
        let key = load_private_key(key_path)?;

        builder
            .with_client_auth_cert(certs, key)
            .map_err(|e| TlsError::Config(format!("failed to configure client auth: {e}")))?
    } else {
        builder.with_no_client_auth()
    };

    Ok(client_config)
}

/// Build the root certificate store.
fn build_root_store(config: &BrokerTlsConfig) -> TlsResult<RootCertStore> {
    let mut root_store = RootCertStore::empty();

    if let Some(ca_path) = &config.ca_cert_path {
        // Load custom CA certificate
        debug!(path = %ca_path.display(), "loading custom CA certificate");
        let certs = load_certificates(ca_path)?;
        let (added, _ignored) = root_store.add_parsable_certificates(certs);
        debug!(added, "added CA certificates to trust store");

        if added == 0 {
            return Err(TlsError::NoCertificates(ca_path.display().to_string()));
        }
    } else {
        // Use webpki roots (Mozilla's root certificates)
        debug!("using system root certificates");
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
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
    use tempfile::NamedTempFile;

    // Self-signed test certificate and key (for testing only)
    // Generated with: openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 365 -nodes -subj "/CN=test"
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
    fn test_load_certificates() {
        let cert_file = create_temp_cert_file();
        let certs = load_certificates(cert_file.path()).unwrap();
        assert_eq!(certs.len(), 1);
    }

    #[test]
    fn test_load_private_key() {
        let key_file = create_temp_key_file();
        let key = load_private_key(key_file.path());
        assert!(key.is_ok());
    }

    #[test]
    fn test_load_certificates_file_not_found() {
        let result = load_certificates(Path::new("/nonexistent/path/cert.pem"));
        assert!(matches!(result, Err(TlsError::CertificateLoad { .. })));
    }

    #[test]
    fn test_load_private_key_file_not_found() {
        let result = load_private_key(Path::new("/nonexistent/path/key.pem"));
        assert!(matches!(result, Err(TlsError::PrivateKeyLoad { .. })));
    }

    #[test]
    fn test_connector_with_native_roots() {
        let connector = TlsConnector::with_native_roots();
        assert!(connector.is_ok());
    }

    #[test]
    fn test_connector_with_custom_ca() {
        let cert_file = create_temp_cert_file();
        let config = BrokerTlsConfig {
            ca_cert_path: Some(cert_file.path().to_path_buf()),
            cert_path: None,
            key_path: None,
            insecure_skip_verify: false,
        };
        let connector = TlsConnector::new(&config);
        // The test cert is a valid PEM cert, so it should load successfully.
        // Rustls adds it to the root store even without CA:TRUE extension.
        assert!(
            connector.is_ok(),
            "Expected Ok, got: {:?}",
            connector.err()
        );
    }

    #[test]
    fn test_connector_with_client_cert() {
        let cert_file = create_temp_cert_file();
        let key_file = create_temp_key_file();
        let config = BrokerTlsConfig {
            ca_cert_path: Some(cert_file.path().to_path_buf()),
            cert_path: Some(cert_file.path().to_path_buf()),
            key_path: Some(key_file.path().to_path_buf()),
            insecure_skip_verify: false,
        };
        let connector = TlsConnector::new(&config);
        assert!(
            connector.is_ok(),
            "Expected Ok, got: {:?}",
            connector.err()
        );
    }

    #[test]
    fn test_empty_cert_file() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"not a certificate").unwrap();
        let result = load_certificates(file.path());
        assert!(matches!(result, Err(TlsError::NoCertificates(_))));
    }

    #[test]
    fn test_empty_key_file() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"not a key").unwrap();
        let result = load_private_key(file.path());
        assert!(matches!(result, Err(TlsError::NoPrivateKeys(_))));
    }
}
