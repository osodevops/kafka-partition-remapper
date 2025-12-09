//! TLS/SSL support for Kafka connections.
//!
//! This module provides TLS functionality for both client-side connections
//! (proxy to broker) and server-side connections (client to proxy).
//!
//! # Features
//!
//! - **Client TLS** (`client` module): Connect to Kafka brokers over TLS
//! - **Server TLS** (future): Accept client connections over TLS
//!
//! # Usage
//!
//! ```rust,ignore
//! use kafka_remapper_core::tls::client::TlsConnector;
//! use kafka_remapper_core::config::BrokerTlsConfig;
//!
//! let config = BrokerTlsConfig::default();
//! let connector = TlsConnector::new(&config)?;
//! let tls_stream = connector.connect("kafka.example.com", tcp_stream).await?;
//! ```

pub mod client;

pub use client::TlsConnector;
