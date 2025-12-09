//! Metrics collection for the Kafka partition remapping proxy.
//!
//! Provides Prometheus-compatible metrics for monitoring proxy performance,
//! request rates, and connection statistics.

pub mod prometheus;

pub use prometheus::ProxyMetrics;
