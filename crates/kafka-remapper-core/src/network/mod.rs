//! Network layer for the Kafka partition remapping proxy.
//!
//! This module provides:
//! - TCP listener for accepting client connections
//! - Kafka frame codec for parsing/encoding messages
//! - Connection handler for processing requests
//! - Client stream abstraction for TLS/plain TCP
//! - Connection context for carrying principal and metadata

pub mod client_stream;
pub mod codec;
pub mod connection;
pub mod context;
pub mod listener;

pub use client_stream::ClientStream;
pub use codec::{KafkaCodec, KafkaFrame};
pub use connection::ConnectionHandler;
pub use context::ConnectionContext;
pub use listener::ProxyListener;
