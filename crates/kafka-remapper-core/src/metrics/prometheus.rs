//! Prometheus metrics for the Kafka partition remapping proxy.
//!
//! Provides counters, histograms, and gauges for monitoring proxy health
//! and performance.

use prometheus::{
    Counter, CounterVec, HistogramOpts, HistogramVec, IntGauge, Opts, Registry, TextEncoder,
};

/// Proxy metrics collection.
///
/// Contains all metrics exposed by the proxy for monitoring.
pub struct ProxyMetrics {
    /// The Prometheus registry.
    pub registry: Registry,

    /// Total requests by API key.
    pub requests_total: CounterVec,

    /// Request errors by API key and error type.
    pub requests_errors: CounterVec,

    /// Request latency histogram by API key.
    pub request_duration_seconds: HistogramVec,

    /// Current active client connections.
    pub active_client_connections: IntGauge,

    /// Current active broker connections.
    pub active_broker_connections: IntGauge,

    /// Total partition remapping operations.
    pub partitions_remapped: Counter,

    /// Total offset translation operations.
    pub offsets_translated: Counter,

    /// Total bytes received from clients.
    pub bytes_received: Counter,

    /// Total bytes sent to clients.
    pub bytes_sent: Counter,
}

impl ProxyMetrics {
    /// Create a new metrics collection.
    ///
    /// # Panics
    ///
    /// Panics if metric registration fails (should not happen with unique names).
    #[must_use]
    pub fn new() -> Self {
        let registry = Registry::new();

        let requests_total = CounterVec::new(
            Opts::new(
                "kafka_proxy_requests_total",
                "Total number of requests processed by API key",
            ),
            &["api_key"],
        )
        .expect("metric creation should succeed");

        let requests_errors = CounterVec::new(
            Opts::new(
                "kafka_proxy_requests_errors_total",
                "Total number of request errors by API key and error type",
            ),
            &["api_key", "error_type"],
        )
        .expect("metric creation should succeed");

        let request_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "kafka_proxy_request_duration_seconds",
                "Request latency in seconds",
            )
            .buckets(vec![
                0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
            ]),
            &["api_key"],
        )
        .expect("metric creation should succeed");

        let active_client_connections = IntGauge::new(
            "kafka_proxy_active_client_connections",
            "Current number of active client connections",
        )
        .expect("metric creation should succeed");

        let active_broker_connections = IntGauge::new(
            "kafka_proxy_active_broker_connections",
            "Current number of active broker connections",
        )
        .expect("metric creation should succeed");

        let partitions_remapped = Counter::new(
            "kafka_proxy_partitions_remapped_total",
            "Total number of partition remapping operations",
        )
        .expect("metric creation should succeed");

        let offsets_translated = Counter::new(
            "kafka_proxy_offsets_translated_total",
            "Total number of offset translation operations",
        )
        .expect("metric creation should succeed");

        let bytes_received = Counter::new(
            "kafka_proxy_bytes_received_total",
            "Total bytes received from clients",
        )
        .expect("metric creation should succeed");

        let bytes_sent = Counter::new(
            "kafka_proxy_bytes_sent_total",
            "Total bytes sent to clients",
        )
        .expect("metric creation should succeed");

        // Register all metrics
        registry
            .register(Box::new(requests_total.clone()))
            .expect("metric registration should succeed");
        registry
            .register(Box::new(requests_errors.clone()))
            .expect("metric registration should succeed");
        registry
            .register(Box::new(request_duration_seconds.clone()))
            .expect("metric registration should succeed");
        registry
            .register(Box::new(active_client_connections.clone()))
            .expect("metric registration should succeed");
        registry
            .register(Box::new(active_broker_connections.clone()))
            .expect("metric registration should succeed");
        registry
            .register(Box::new(partitions_remapped.clone()))
            .expect("metric registration should succeed");
        registry
            .register(Box::new(offsets_translated.clone()))
            .expect("metric registration should succeed");
        registry
            .register(Box::new(bytes_received.clone()))
            .expect("metric registration should succeed");
        registry
            .register(Box::new(bytes_sent.clone()))
            .expect("metric registration should succeed");

        Self {
            registry,
            requests_total,
            requests_errors,
            request_duration_seconds,
            active_client_connections,
            active_broker_connections,
            partitions_remapped,
            offsets_translated,
            bytes_received,
            bytes_sent,
        }
    }

    /// Record a request being processed.
    pub fn record_request(&self, api_key: &str) {
        self.requests_total.with_label_values(&[api_key]).inc();
    }

    /// Record a request error.
    pub fn record_error(&self, api_key: &str, error_type: &str) {
        self.requests_errors
            .with_label_values(&[api_key, error_type])
            .inc();
    }

    /// Record request duration.
    pub fn record_duration(&self, api_key: &str, duration_seconds: f64) {
        self.request_duration_seconds
            .with_label_values(&[api_key])
            .observe(duration_seconds);
    }

    /// Increment active client connections.
    pub fn inc_client_connections(&self) {
        self.active_client_connections.inc();
    }

    /// Decrement active client connections.
    pub fn dec_client_connections(&self) {
        self.active_client_connections.dec();
    }

    /// Set the number of active broker connections.
    pub fn set_broker_connections(&self, count: i64) {
        self.active_broker_connections.set(count);
    }

    /// Record partition remapping.
    pub fn record_partition_remap(&self) {
        self.partitions_remapped.inc();
    }

    /// Record offset translation.
    pub fn record_offset_translation(&self) {
        self.offsets_translated.inc();
    }

    /// Record bytes received.
    pub fn record_bytes_received(&self, bytes: u64) {
        self.bytes_received.inc_by(bytes as f64);
    }

    /// Record bytes sent.
    pub fn record_bytes_sent(&self, bytes: u64) {
        self.bytes_sent.inc_by(bytes as f64);
    }

    /// Encode metrics in Prometheus text format.
    ///
    /// # Errors
    ///
    /// Returns an error if encoding fails.
    pub fn encode(&self) -> Result<String, prometheus::Error> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = String::new();
        encoder.encode_utf8(&metric_families, &mut buffer)?;
        Ok(buffer)
    }
}

impl Default for ProxyMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = ProxyMetrics::new();
        assert!(metrics.encode().is_ok());
    }

    #[test]
    fn test_record_request() {
        let metrics = ProxyMetrics::new();
        metrics.record_request("Metadata");
        metrics.record_request("Metadata");
        metrics.record_request("Produce");

        let output = metrics.encode().unwrap();
        assert!(output.contains("kafka_proxy_requests_total"));
    }

    #[test]
    fn test_record_error() {
        let metrics = ProxyMetrics::new();
        metrics.record_error("Fetch", "decode_error");

        let output = metrics.encode().unwrap();
        assert!(output.contains("kafka_proxy_requests_errors_total"));
    }

    #[test]
    fn test_record_duration() {
        let metrics = ProxyMetrics::new();
        metrics.record_duration("ApiVersions", 0.001);
        metrics.record_duration("ApiVersions", 0.002);

        let output = metrics.encode().unwrap();
        assert!(output.contains("kafka_proxy_request_duration_seconds"));
    }

    #[test]
    fn test_connection_gauges() {
        let metrics = ProxyMetrics::new();

        metrics.inc_client_connections();
        metrics.inc_client_connections();
        metrics.dec_client_connections();

        metrics.set_broker_connections(3);

        let output = metrics.encode().unwrap();
        assert!(output.contains("kafka_proxy_active_client_connections"));
        assert!(output.contains("kafka_proxy_active_broker_connections"));
    }

    #[test]
    fn test_bytes_counters() {
        let metrics = ProxyMetrics::new();
        metrics.record_bytes_received(1024);
        metrics.record_bytes_sent(2048);

        let output = metrics.encode().unwrap();
        assert!(output.contains("kafka_proxy_bytes_received_total"));
        assert!(output.contains("kafka_proxy_bytes_sent_total"));
    }
}
