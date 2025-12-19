//! TLS/SSL support for Kafka connections.
//!
//! This module provides TLS functionality for both client-side connections
//! (proxy to broker) and server-side connections (client to proxy).
//!
//! # Features
//!
//! - **Client TLS** (`client` module): Connect to Kafka brokers over TLS
//! - **Server TLS** (`server` module): Accept client connections over TLS
//!
//! # Usage
//!
//! ## Client TLS (Proxy to Broker)
//!
//! ```rust,ignore
//! use kafka_remapper_core::tls::TlsConnector;
//! use kafka_remapper_core::config::BrokerTlsConfig;
//!
//! let config = BrokerTlsConfig::default();
//! let connector = TlsConnector::new(&config)?;
//! let tls_stream = connector.connect("kafka.example.com", tcp_stream).await?;
//! ```
//!
//! ## Server TLS (Client to Proxy)
//!
//! ```rust,ignore
//! use kafka_remapper_core::tls::TlsServerAcceptor;
//! use kafka_remapper_core::config::ClientTlsConfig;
//!
//! let config = ClientTlsConfig { cert_path: ..., key_path: ..., ... };
//! let acceptor = TlsServerAcceptor::new(&config)?;
//! let tls_stream = acceptor.accept(tcp_stream).await?;
//! ```

pub mod client;
pub mod server;

pub use client::TlsConnector;
pub use server::TlsServerAcceptor;
