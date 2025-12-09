//! Broker connection management.
//!
//! This module provides:
//! - Connection management to Kafka brokers
//! - Connection pooling with per-broker connections
//! - Request/response correlation
//! - Background metadata refresh
//! - TLS and SASL authentication support

pub mod connection;
pub mod metadata_refresh;
pub mod pool;
pub mod stream;

pub use connection::BrokerConnection;
pub use metadata_refresh::MetadataRefresher;
pub use pool::BrokerPool;
pub use stream::BrokerStream;
