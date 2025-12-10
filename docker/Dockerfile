# Build stage
FROM rust:1.75-slim-bookworm AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests first for dependency caching
COPY Cargo.toml Cargo.lock ./
COPY crates/kafka-remapper-core/Cargo.toml ./crates/kafka-remapper-core/
COPY crates/kafka-remapper-cli/Cargo.toml ./crates/kafka-remapper-cli/

# Create dummy source files to build dependencies
RUN mkdir -p crates/kafka-remapper-core/src crates/kafka-remapper-cli/src crates/kafka-remapper-core/benches && \
    echo "pub fn dummy() {}" > crates/kafka-remapper-core/src/lib.rs && \
    echo "fn main() {}" > crates/kafka-remapper-cli/src/main.rs && \
    echo "fn main() {}" > crates/kafka-remapper-core/benches/partition_remapping.rs

# Build dependencies
RUN cargo build --release --package kafka-remapper-cli

# Remove dummy source files
RUN rm -rf crates/kafka-remapper-core/src crates/kafka-remapper-cli/src crates/kafka-remapper-core/benches

# Copy actual source code
COPY crates/kafka-remapper-core/src ./crates/kafka-remapper-core/src
COPY crates/kafka-remapper-core/benches ./crates/kafka-remapper-core/benches
COPY crates/kafka-remapper-cli/src ./crates/kafka-remapper-cli/src

# Touch source files to invalidate cache
RUN touch crates/kafka-remapper-core/src/lib.rs crates/kafka-remapper-cli/src/main.rs

# Build the release binary
RUN cargo build --release --package kafka-remapper-cli

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -r -s /bin/false kafka-proxy

# Copy the binary
COPY --from=builder /app/target/release/kafka-partition-proxy /usr/local/bin/

# Copy default config
COPY config/example.yaml /etc/kafka-proxy/config.yaml

# Set ownership
RUN chown -R kafka-proxy:kafka-proxy /etc/kafka-proxy

# Switch to non-root user
USER kafka-proxy

# Expose ports
# 9092 - Kafka proxy port
# 9090 - Prometheus metrics port
EXPOSE 9092 9090

# Default command
ENTRYPOINT ["kafka-partition-proxy"]
CMD ["--config", "/etc/kafka-proxy/config.yaml"]
