//! Broker connection management.
//!
//! This module provides:
//! - Connection management to Kafka brokers
//! - Connection pooling with per-broker connections
//! - Request/response correlation
//! - Background metadata refresh

pub mod connection;
pub mod metadata_refresh;
pub mod pool;

pub use connection::BrokerConnection;
pub use metadata_refresh::MetadataRefresher;
pub use pool::BrokerPool;
