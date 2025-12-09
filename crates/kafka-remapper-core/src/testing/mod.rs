//! Test utilities for the Kafka partition remapping proxy.
//!
//! This module provides infrastructure for integration testing:
//!
//! - [`MockBroker`] - A lightweight mock Kafka broker for testing
//! - [`ProxyTestHarness`] - A complete test environment with mock broker and remapper
//! - [`TestHarnessBuilder`] - Builder pattern for custom test configurations
//!
//! # Example
//!
//! ```rust,ignore
//! use kafka_remapper_core::testing::{ProxyTestHarness, mock_broker::api_keys};
//!
//! #[tokio::test]
//! async fn test_produce_request() {
//!     let harness = ProxyTestHarness::new().await;
//!
//!     // Connect to mock broker
//!     harness.connect().await.unwrap();
//!
//!     // Send a request and verify
//!     let calls = harness.get_broker_calls_for_api(api_keys::PRODUCE).await;
//!     assert_eq!(calls.len(), 1);
//! }
//! ```

pub mod harness;
pub mod mock_broker;

pub use harness::{ProxyTestHarness, TestHarnessBuilder};
pub use mock_broker::{api_keys, BrokerCall, MockBroker, ResponseGenerator};
