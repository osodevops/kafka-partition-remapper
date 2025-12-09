//! Network layer for the Kafka partition remapping proxy.
//!
//! This module provides:
//! - TCP listener for accepting client connections
//! - Kafka frame codec for parsing/encoding messages
//! - Connection handler for processing requests

pub mod codec;
pub mod connection;
pub mod listener;

pub use codec::{KafkaCodec, KafkaFrame};
pub use connection::ConnectionHandler;
pub use listener::ProxyListener;
