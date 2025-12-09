//! Kafka Partition Remapping Proxy CLI
//!
//! A TCP proxy that remaps virtual Kafka partitions to physical partitions,
//! enabling cost reduction on managed Kafka services.

use std::sync::Arc;

use clap::Parser;
use tokio::signal;
use tracing::{info, Level};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use kafka_remapper_core::broker::{BrokerPool, MetadataRefresher};
use kafka_remapper_core::config::{LoggingConfig, ProxyConfig};
use kafka_remapper_core::metrics::ProxyMetrics;
use kafka_remapper_core::network::ProxyListener;
use kafka_remapper_core::remapper::PartitionRemapper;
use tokio::sync::watch;

/// Kafka partition remapping proxy.
#[derive(Parser)]
#[command(name = "kafka-partition-proxy")]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to configuration file.
    #[arg(short, long, default_value = "config.yaml")]
    config: String,

    /// Override listen address.
    #[arg(long)]
    listen: Option<String>,

    /// Increase logging verbosity (-v for debug, -vv for trace).
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Load configuration
    let mut config = ProxyConfig::from_file(&args.config)?;

    // Apply CLI overrides
    if let Some(listen) = args.listen {
        config.listen.address = listen;
    }

    // Override log level from verbosity flag
    let log_config = match args.verbose {
        0 => config.logging.clone(),
        1 => LoggingConfig {
            level: "debug".to_string(),
            ..config.logging.clone()
        },
        _ => LoggingConfig {
            level: "trace".to_string(),
            ..config.logging.clone()
        },
    };

    // Setup tracing
    setup_tracing(&log_config);

    info!(
        version = env!("CARGO_PKG_VERSION"),
        listen = %config.listen.address,
        virtual_partitions = config.mapping.virtual_partitions,
        physical_partitions = config.mapping.physical_partitions,
        compression_ratio = config.mapping.compression_ratio(),
        "starting kafka partition proxy"
    );

    // Run the async runtime
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async move { run_proxy(config).await })
}

fn setup_tracing(config: &LoggingConfig) {
    let level = match config.level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let filter = EnvFilter::from_default_env().add_directive(level.into());

    let subscriber = tracing_subscriber::registry().with(filter);

    if config.json {
        subscriber.with(fmt::layer().json()).init();
    } else {
        subscriber.with(fmt::layer()).init();
    }
}

async fn run_proxy(config: ProxyConfig) -> anyhow::Result<()> {
    // Initialize components
    let metrics = Arc::new(ProxyMetrics::new());
    let _remapper = Arc::new(PartitionRemapper::new(&config.mapping));
    let broker_pool = Arc::new(BrokerPool::new(config.kafka.clone()));

    info!(
        bootstrap_servers = ?config.kafka.bootstrap_servers,
        "connecting to kafka cluster"
    );

    // Connect to Kafka
    broker_pool.connect().await?;
    info!("connected to kafka cluster");

    // Start metrics server if enabled
    if config.metrics.enabled {
        let metrics_clone = Arc::clone(&metrics);
        let metrics_addr = config.metrics.address.clone();
        tokio::spawn(async move {
            if let Err(e) = start_metrics_server(&metrics_addr, metrics_clone).await {
                tracing::error!(error = %e, "metrics server error");
            }
        });
        info!(address = %config.metrics.address, "metrics server started");
    }

    // Create shutdown channel for metadata refresher
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Start background metadata refresh
    let metadata_refresh_interval = config.kafka.metadata_refresh_interval_secs;
    if metadata_refresh_interval > 0 {
        let refresher = MetadataRefresher::new(
            Arc::clone(&broker_pool),
            metadata_refresh_interval,
            shutdown_rx,
        );
        tokio::spawn(async move {
            refresher.run().await;
        });
        info!(
            interval_secs = metadata_refresh_interval,
            "background metadata refresh started"
        );
    }

    // Start proxy listener
    let listener = ProxyListener::new(config);
    let shutdown_handle = listener.shutdown_handle();

    // Handle shutdown signals
    tokio::spawn(async move {
        shutdown_signal().await;
        info!("shutdown signal received, stopping proxy");
        let _ = shutdown_handle.send(());
        // Signal metadata refresher to stop
        let _ = shutdown_tx.send(true);
    });

    // Run the proxy
    listener.run().await?;

    info!("proxy shutdown complete");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl+c");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to listen for SIGTERM")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

async fn start_metrics_server(
    addr: &str,
    metrics: Arc<ProxyMetrics>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use http_body_util::Full;
    use hyper::body::Bytes;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request, Response};
    use hyper_util::rt::TokioIo;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;

    let addr: SocketAddr = addr.parse()?;
    let listener = TcpListener::bind(addr).await?;

    info!(address = %addr, "metrics server listening");

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let metrics = Arc::clone(&metrics);

        tokio::spawn(async move {
            let service = service_fn(move |_req: Request<hyper::body::Incoming>| {
                let metrics = Arc::clone(&metrics);
                async move {
                    let body = metrics.encode().unwrap_or_default();
                    Ok::<_, hyper::Error>(Response::new(Full::new(Bytes::from(body))))
                }
            });

            if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                tracing::debug!(error = %e, "metrics connection error");
            }
        });
    }
}
