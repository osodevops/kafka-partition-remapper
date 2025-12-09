//! Kafka Partition Remapper Core Library
//!
//! This library provides the core functionality for a Kafka partition remapping proxy.
//! It enables exposing a large number of virtual partitions to clients while using
//! fewer physical partitions on the actual Kafka cluster, reducing per-partition costs.
//!
//! # Architecture
//!
//! The library is organized into several modules:
//!
//! - [`config`] - Configuration loading and validation
//! - [`error`] - Domain-specific error types
//! - [`remapper`] - Core partition/offset mapping logic
//! - [`network`] - TCP listener and Kafka frame codec
//! - [`broker`] - Backend Kafka broker connection pool
//! - [`handlers`] - Kafka protocol request handlers
//! - [`metrics`] - Prometheus metrics collection
//! - [`tls`] - TLS/SSL support for secure connections
//!
//! # Example
//!
//! ```rust,ignore
//! use kafka_remapper_core::config::ProxyConfig;
//!
//! // Load configuration
//! let config = ProxyConfig::from_file("config.yaml")?;
//!
//! // Start the proxy
//! // ...
//! ```

#![forbid(unsafe_code)]
#![allow(dead_code, unused_variables, unused_imports, clippy::redundant_closure)]

pub mod broker;
pub mod config;
pub mod error;
pub mod handlers;
pub mod metrics;
pub mod network;
pub mod remapper;
pub mod tls;

/// Test utilities for integration testing.
///
/// This module is only available when compiling tests or when the `testing` feature is enabled.
#[cfg(any(test, feature = "testing"))]
pub mod testing;

// Re-export commonly used types
pub use broker::{BrokerConnection, BrokerPool, BrokerStream};
pub use config::{BrokerSaslConfig, BrokerTlsConfig, ProxyConfig, SaslMechanism, SecurityProtocol};
pub use error::{AuthError, ConfigError, ProxyError, RemapError, Result, TlsError};
pub use tls::TlsConnector;
