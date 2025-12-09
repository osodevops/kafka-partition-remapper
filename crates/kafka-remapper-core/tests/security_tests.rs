//! Security integration tests for TLS and SASL authentication.
//!
//! These tests verify that the proxy can connect to Kafka brokers using
//! various security protocols (SSL, `SASL_PLAINTEXT`, `SASL_SSL`).
//!
//! Note: These tests require a properly configured Kafka cluster and are
//! marked as ignored by default. Run with: cargo test --test `security_tests` -- --ignored

use std::time::Duration;

use kafka_remapper_core::broker::BrokerConnection;
use kafka_remapper_core::config::{
    BrokerSaslConfig, BrokerTlsConfig, SaslMechanism, SecurityProtocol,
};

/// Test that a plaintext connection can be established.
#[tokio::test]
async fn test_plaintext_connection_config() {
    // Just verify we can create a connection with plaintext config
    let conn = BrokerConnection::new(1, "localhost:9092".to_string());
    assert_eq!(conn.broker_id(), 1);
    assert_eq!(conn.address(), "localhost:9092");
}

/// Test that a TLS connection configuration can be created.
#[tokio::test]
async fn test_ssl_connection_config() {
    let tls_config = BrokerTlsConfig::default();

    let result = BrokerConnection::with_security(
        1,
        "localhost:9093".to_string(),
        SecurityProtocol::Ssl,
        Some(&tls_config),
        None,
        Duration::from_secs(10),
        Duration::from_secs(30),
    );

    assert!(
        result.is_ok(),
        "Should be able to create SSL connection config"
    );
    let conn = result.unwrap();
    assert_eq!(conn.broker_id(), 1);
}

/// Test that a `SASL_PLAINTEXT` connection configuration can be created.
#[tokio::test]
async fn test_sasl_plaintext_connection_config() {
    let sasl_config = BrokerSaslConfig {
        mechanism: SaslMechanism::Plain,
        username: "test-user".to_string(),
        password: "test-password".to_string(),
    };

    let result = BrokerConnection::with_security(
        1,
        "localhost:9092".to_string(),
        SecurityProtocol::SaslPlaintext,
        None,
        Some(sasl_config),
        Duration::from_secs(10),
        Duration::from_secs(30),
    );

    assert!(
        result.is_ok(),
        "Should be able to create SASL_PLAINTEXT connection config"
    );
}

/// Test that a `SASL_SSL` connection configuration can be created.
#[tokio::test]
async fn test_sasl_ssl_connection_config() {
    let tls_config = BrokerTlsConfig::default();
    let sasl_config = BrokerSaslConfig {
        mechanism: SaslMechanism::Plain,
        username: "test-user".to_string(),
        password: "test-password".to_string(),
    };

    let result = BrokerConnection::with_security(
        1,
        "localhost:9093".to_string(),
        SecurityProtocol::SaslSsl,
        Some(&tls_config),
        Some(sasl_config),
        Duration::from_secs(10),
        Duration::from_secs(30),
    );

    assert!(
        result.is_ok(),
        "Should be able to create SASL_SSL connection config"
    );
}

/// Test SASL/PLAIN credential format.
#[test]
fn test_sasl_plain_credential_format() {
    // SASL/PLAIN format: \0username\0password
    let username = "kafka-api-key";
    let password = "kafka-api-secret";

    let auth_bytes = format!("\0{username}\0{password}");

    let parts: Vec<&str> = auth_bytes.split('\0').collect();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0], ""); // Empty prefix
    assert_eq!(parts[1], username);
    assert_eq!(parts[2], password);
}

/// Test that `SecurityProtocol` correctly identifies TLS requirements.
#[test]
fn test_security_protocol_requires_tls() {
    assert!(!SecurityProtocol::Plaintext.requires_tls());
    assert!(SecurityProtocol::Ssl.requires_tls());
    assert!(!SecurityProtocol::SaslPlaintext.requires_tls());
    assert!(SecurityProtocol::SaslSsl.requires_tls());
}

/// Test that `SecurityProtocol` correctly identifies SASL requirements.
#[test]
fn test_security_protocol_requires_sasl() {
    assert!(!SecurityProtocol::Plaintext.requires_sasl());
    assert!(!SecurityProtocol::Ssl.requires_sasl());
    assert!(SecurityProtocol::SaslPlaintext.requires_sasl());
    assert!(SecurityProtocol::SaslSsl.requires_sasl());
}

// =============================================================================
// Real Kafka Tests (Require configured Kafka cluster)
// =============================================================================

/// Test connecting to a real Kafka broker with `SASL_SSL`.
///
/// This test requires:
/// - A Kafka broker running with `SASL_SSL` enabled
/// - Environment variables:
///   - `KAFKA_BOOTSTRAP_SERVERS`: The broker address (e.g., "localhost:9093")
///   - `KAFKA_API_KEY`: The SASL username
///   - `KAFKA_API_SECRET`: The SASL password
///
/// Run with: cargo test --test `security_tests` `test_real_sasl_ssl_connection` -- --ignored
#[tokio::test]
#[ignore = "Requires configured Kafka cluster with SASL_SSL"]
async fn test_real_sasl_ssl_connection() {
    let bootstrap_servers =
        std::env::var("KAFKA_BOOTSTRAP_SERVERS").expect("KAFKA_BOOTSTRAP_SERVERS env var required");
    let api_key = std::env::var("KAFKA_API_KEY").expect("KAFKA_API_KEY env var required");
    let api_secret = std::env::var("KAFKA_API_SECRET").expect("KAFKA_API_SECRET env var required");

    let tls_config = BrokerTlsConfig::default(); // Use system root certificates
    let sasl_config = BrokerSaslConfig {
        mechanism: SaslMechanism::Plain,
        username: api_key,
        password: api_secret,
    };

    let conn = BrokerConnection::with_security(
        -1,
        bootstrap_servers,
        SecurityProtocol::SaslSsl,
        Some(&tls_config),
        Some(sasl_config),
        Duration::from_secs(30),
        Duration::from_secs(60),
    )
    .expect("Failed to create connection");

    let result = conn.connect().await;
    assert!(
        result.is_ok(),
        "Should connect to SASL_SSL broker: {:?}",
        result.err()
    );

    conn.disconnect().await;
}

/// Test connecting to Confluent Cloud.
///
/// This test requires:
/// - A Confluent Cloud cluster
/// - Environment variables:
///   - `CONFLUENT_BOOTSTRAP_SERVERS`: The bootstrap server (e.g., "pkc-xxx.region.gcp.confluent.cloud:9092")
///   - `CONFLUENT_API_KEY`: The API key
///   - `CONFLUENT_API_SECRET`: The API secret
///
/// Run with: cargo test --test `security_tests` `test_confluent_cloud_connection` -- --ignored
#[tokio::test]
#[ignore = "Requires Confluent Cloud credentials"]
async fn test_confluent_cloud_connection() {
    let bootstrap_servers = std::env::var("CONFLUENT_BOOTSTRAP_SERVERS")
        .expect("CONFLUENT_BOOTSTRAP_SERVERS env var required");
    let api_key = std::env::var("CONFLUENT_API_KEY").expect("CONFLUENT_API_KEY env var required");
    let api_secret =
        std::env::var("CONFLUENT_API_SECRET").expect("CONFLUENT_API_SECRET env var required");

    let tls_config = BrokerTlsConfig::default(); // Confluent uses well-known CAs
    let sasl_config = BrokerSaslConfig {
        mechanism: SaslMechanism::Plain,
        username: api_key,
        password: api_secret,
    };

    let conn = BrokerConnection::with_security(
        -1,
        bootstrap_servers,
        SecurityProtocol::SaslSsl,
        Some(&tls_config),
        Some(sasl_config),
        Duration::from_secs(30),
        Duration::from_secs(60),
    )
    .expect("Failed to create connection");

    let result = conn.connect().await;
    assert!(
        result.is_ok(),
        "Should connect to Confluent Cloud: {:?}",
        result.err()
    );

    conn.disconnect().await;
}
