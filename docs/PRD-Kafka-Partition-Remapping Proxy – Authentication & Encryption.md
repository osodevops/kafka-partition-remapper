# PRD: Kafka Partition Remapping Proxy – Authentication & Encryption

## Overview

The Kafka Partition Remapping Proxy currently lacks client authentication and TLS/SSL support, making it unsuitable for production use with Confluent Cloud or enterprise Kafka clusters. This PRD outlines support for industry-standard authentication mechanisms and encryption to match Confluent Cloud's security model.

## Goals

1. **Support all major Kafka authentication mechanisms** used by Confluent Cloud
2. **Enable secure proxy-to-broker communication** with TLS/SSL and SASL
3. **Allow transparent client authentication** (optional identity passthrough or swapping)
4. **Achieve feature parity with Confluent Gateway** for enterprise deployments
5. **Maintain backward compatibility** for development/test environments (optional auth)

---

## Authentication Mechanisms to Support

### Phase 1: Core (MVP)

These are the most common Confluent Cloud auth methods:

#### 1. **SASL/PLAIN**
- **Use case**: Simple username/password authentication, most common for Confluent Cloud dev clusters
- **Client config**:
  ```
  security.protocol=SASL_SSL
  sasl.mechanism=PLAIN
  sasl.username=<api-key>
  sasl.password=<api-secret>
  ```
- **Proxy responsibility**: 
  - Receive PLAIN credentials from clients
  - Forward them to backing Kafka cluster (identity passthrough), OR
  - Swap them for different credentials (identity swapping) [Phase 2]

#### 2. **SASL/SCRAM-SHA-256 & SCRAM-SHA-512**
- **Use case**: Salted challenge-response, more secure than PLAIN
- **Client config**:
  ```
  security.protocol=SASL_SSL
  sasl.mechanism=SCRAM-SHA-256
  sasl.username=<username>
  sasl.password=<password>
  ```
- **Proxy responsibility**: 
  - Validate SCRAM authentication against credential store or backing cluster
  - Forward to broker or swap credentials

#### 3. **mTLS (Mutual TLS / Certificate-Based)**
- **Use case**: Certificate-based authentication, enterprise standard
- **Client config**:
  ```
  security.protocol=SSL
  ssl.keystore.location=/path/to/client.keystore.p12
  ssl.keystore.type=PKCS12
  ssl.keystore.password=<password>
  ssl.key.password=<password>
  ```
- **Proxy responsibility**:
  - Validate client certificates against configured CA
  - Extract principal from certificate (DN, subject alternative name, custom mapping rules)
  - Optionally map to identity pool for fine-grained access control

#### 4. **SASL/OAUTHBEARER (Phase 1.5)**
- **Use case**: OAuth 2.0 token-based auth, for SSO and cloud-native deployments
- **Client config**:
  ```
  security.protocol=SASL_SSL
  sasl.mechanism=OAUTHBEARER
  sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required <oauth-params>
  ```
- **Proxy responsibility**:
  - Accept bearer tokens from clients
  - Optionally validate tokens against OAuth provider
  - Forward to broker or swap for different credentials

---

## TLS/SSL Encryption

### Proxy-to-Client TLS

The proxy must present a certificate to clients, supporting two modes:

#### **TLS Encryption Only (Server-side authentication)**
- Client validates proxy certificate against trusted CA
- **Config**:
  ```yaml
  proxy:
    tls:
      enabled: true
      cert_path: /path/to/proxy.crt
      key_path: /path/to/proxy.key
      # Optional: enable client certificate validation
      client_auth: optional  # or 'required'
      ca_path: /path/to/ca.crt
  ```

#### **mTLS (Mutual TLS)**
- Proxy validates client certificates in addition to server TLS
- **Config**:
  ```yaml
  proxy:
    tls:
      enabled: true
      cert_path: /path/to/proxy.crt
      key_path: /path/to/proxy.key
      client_auth: required
      ca_path: /path/to/client-ca.crt
  ```

### Proxy-to-Broker TLS

The proxy itself acts as a Kafka client when connecting to brokers. It must support:

#### **TLS with Server Verification (1-way)**
```yaml
broker_connection:
  bootstrap_servers: kafka:9093
  security_protocol: SSL
  ssl:
    truststore_path: /path/to/broker.truststore.jks
    truststore_password: <password>
    # OR use PEM cert
    ca_cert_path: /path/to/broker-ca.crt
```

#### **mTLS (2-way)**
```yaml
broker_connection:
  bootstrap_servers: kafka:9093
  security_protocol: SSL
  ssl:
    truststore_path: /path/to/broker.truststore.jks
    truststore_password: <password>
    keystore_path: /path/to/proxy-client.keystore.jks
    keystore_password: <password>
    key_password: <password>
    # OR use PEM
    ca_cert_path: /path/to/broker-ca.crt
    cert_path: /path/to/proxy-client.crt
    key_path: /path/to/proxy-client.key
```

---

## Configuration Structure

### `proxy-auth-config.yaml` (or environment variable overrides)

```yaml
# Listen on secure port with TLS
proxy:
  listen_address: "0.0.0.0:19093"
  tls:
    enabled: true
    cert_path: "/etc/proxy/certs/proxy.crt"
    key_path: "/etc/proxy/certs/proxy.key"
    # Client certificate validation (for mTLS from clients)
    client_auth: optional  # 'optional', 'required', or 'none'
    ca_path: "/etc/proxy/certs/client-ca.crt"

# Authenticate clients connecting to proxy
client_auth:
  # List of enabled mechanisms in order of preference
  enabled_mechanisms:
    - SASL_PLAIN
    - SASL_SCRAM_SHA256
    - SASL_SCRAM_SHA512
    - MTLS
    # - OAUTHBEARER  # Phase 1.5
  
  # PLAIN authentication
  sasl_plain:
    enabled: true
    # Credential store type: 'memory', 'file', 'broker'
    credential_store: "broker"  # Validate against backing Kafka cluster
    
  # SCRAM authentication  
  sasl_scram:
    enabled: true
    credential_store: "broker"  # Fetch SCRAM users from metadata
    
  # mTLS client authentication
  mtls:
    enabled: true
    principal_mapping_rules:
      # Extract principal from certificate Subject DN
      # Examples: "CN=(.*)$", "CN=(.+?),.*$"
      - "CN=([^,]+)$"  # Extract common name
    # Optional: map certificates to identity pools (Confluent Cloud style)
    identity_pools: []

# Connect proxy to backing Kafka cluster
broker_connection:
  bootstrap_servers: "kafka.example.com:9093"
  security_protocol: "SASL_SSL"  # PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
  
  # Proxy authentication to broker (SASL)
  sasl:
    mechanism: "PLAIN"  # PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER
    username: "${KAFKA_API_KEY}"
    password: "${KAFKA_API_SECRET}"
  
  # Proxy TLS to broker
  ssl:
    # Server certificate validation
    truststore_path: "/etc/proxy/certs/broker-ca.crt"  # PEM format preferred
    truststore_type: "PEM"  # or JKS, PKCS12
    truststore_password: null  # Not needed for PEM
    
    # Client mTLS to broker (if required)
    keystore_path: "/etc/proxy/certs/proxy-client.p12"
    keystore_type: "PKCS12"
    keystore_password: "${KEYSTORE_PASSWORD}"
    key_password: "${KEY_PASSWORD}"
    
    # Alternative: direct PEM paths
    # ca_cert_path: "/etc/proxy/certs/broker-ca.crt"
    # cert_path: "/etc/proxy/certs/proxy-client.crt"
    # key_path: "/etc/proxy/certs/proxy-client.key"

# Identity handling (Phase 2)
identity:
  mode: "passthrough"  # 'passthrough' or 'swap'
  # passthrough: use client's credentials to connect to broker
  # swap: transform client credentials into broker credentials
  
  # Swap configuration (for future use)
  swap_config:
    rules: []
    # Example: map client PLAIN user "app1" to broker user "broker-app1"
    # - from: "app1"
    #   to: "broker-app1"
```

### Environment Variable Overrides

```bash
PROXY_TLS_ENABLED=true
PROXY_TLS_CERT_PATH=/etc/proxy/certs/proxy.crt
PROXY_TLS_KEY_PATH=/etc/proxy/certs/proxy.key

BROKER_SECURITY_PROTOCOL=SASL_SSL
BROKER_SASL_MECHANISM=PLAIN
BROKER_SASL_USERNAME=$KAFKA_API_KEY
BROKER_SASL_PASSWORD=$KAFKA_API_SECRET

BROKER_SSL_TRUSTSTORE_PATH=/etc/proxy/certs/broker-ca.crt
BROKER_SSL_TRUSTSTORE_TYPE=PEM

CLIENT_AUTH_ENABLED_MECHANISMS=SASL_PLAIN,SASL_SCRAM_SHA256,MTLS
```

---

## Implementation Roadmap

### Phase 1: MVP – Core Authentication (Weeks 1–3)
- [ ] **TLS/SSL infrastructure**
  - Server-side TLS on proxy listener
  - Client TLS validation (optional/required)
  - Proxy-to-broker TLS with truststore support
  
- [ ] **SASL/PLAIN**
  - Parse PLAIN auth from clients
  - Forward to broker (identity passthrough)
  - Credential validation against broker metadata store
  
- [ ] **SASL/SCRAM-SHA-256 & SHA-512**
  - Parse SCRAM auth from clients
  - Fetch SCRAM credentials from broker
  - Validate SCRAM exchange
  
- [ ] **Configuration & documentation**
  - YAML config schema with sensible defaults
  - Env var overrides for containerized deployments
  - Examples: connecting to Confluent Cloud, self-managed clusters, mTLS setups

### Phase 1.5: OAUTHBEARER (Weeks 3–4)
- [ ] Accept and forward OAuth bearer tokens
- [ ] Optional: validate tokens against OAuth provider
- [ ] Cloud-native deployments (GCP IAM, AWS STS, Azure AD)

### Phase 2: Advanced Features (Weeks 5–6)
- [ ] **mTLS with identity pools**
  - Extract principal from certificate (CN, SAN, custom regex)
  - Map to Confluent Cloud identity pools
  - Fine-grained ACL support
  
- [ ] **Authentication swapping**
  - Transform client credentials → broker credentials
  - Useful for migrating between Kafka clusters with different auth schemes
  
- [ ] **Credential reloading**
  - Hot-reload config without restarting proxy
  - Dynamic identity pool updates

### Phase 3: Observability & Enterprise (Weeks 7–8)
- [ ] Audit logging (who authenticated, when, outcome)
- [ ] Metrics (auth success/failure rate, latency)
- [ ] Certificate expiry warnings
- [ ] CRL (Certificate Revocation List) support for mTLS

---

## Technical Stack (Rust)

### Crates Required

```toml
# TLS/SSL
rustls = "0.21"          # Modern, async-friendly TLS
rustls-pemfile = "2.0"   # Load PEM certificates
x509-parser = "0.15"     # Parse X.509 certificates for principal extraction

# SASL
# For SASL/PLAIN, SCRAM: hand-roll parsing using tokio for async, or use rdkafka for compatibility
rdkafka = { version = "0.35", features = ["ssl", "gssapi"] }  # Fallback for broker connection

# Configuration
serde = { version = "1.0", features = ["derive"] }
serde_yaml = "0.9"
serde_json = "1.0"

# Async runtime
tokio = { version = "1.0", features = ["full"] }

# Logging & observability
tracing = "0.1"
tracing-subscriber = "0.3"
prometheus = "0.13"  # Metrics for auth events
```

### Module Structure

```
src/
├── auth/
│   ├── mod.rs                 # Auth trait & registry
│   ├── sasl_plain.rs          # SASL/PLAIN parser & handler
│   ├── sasl_scram.rs          # SASL/SCRAM (SHA-256, SHA-512)
│   ├── mtls.rs                # Certificate validation & principal extraction
│   ├── oauthbearer.rs         # OAuth token handling [Phase 1.5]
│   └── identity.rs            # Passthrough vs. swapping logic
├── tls/
│   ├── mod.rs
│   ├── server.rs              # Server-side TLS (proxy listener)
│   ├── client.rs              # Client-side TLS (proxy → broker)
│   └── certificate.rs         # Certificate loading & validation
├── config/
│   ├── mod.rs
│   └── loader.rs              # YAML + env var loading
├── broker_connection.rs        # Enhanced with SASL/TLS support
└── proxy.rs                    # Main proxy logic (unchanged)
```

---

## API & Protocol Changes

### Protocol: No Changes
- Kafka protocol remains unchanged
- Proxy is transparent to clients

### Configuration Changes
- New `proxy-auth-config.yaml` structure (see above)
- Environment variable namespace: `PROXY_*`, `BROKER_*`, `CLIENT_AUTH_*`

### Runtime Changes
- Proxy listens on TLS port by default (e.g., `:19093` instead of `:19092`)
- Backward compat: support non-TLS for dev (proxy_tls_enabled=false)

---

## Testing Strategy

### Unit Tests
- PLAIN credential parsing and validation
- SCRAM challenge-response logic
- X.509 certificate principal extraction (regex patterns)
- Identity mapping rules

### Integration Tests
- Confluent Cloud test cluster with API key auth
- Self-managed Kafka with SCRAM users
- Self-signed mTLS certificates (test-ca, test-client, test-broker)
- Multiple auth mechanisms in sequence

### E2E Test Scenarios
1. **Confluent Cloud via SASL/PLAIN + TLS**
   - Client: `kafka-console-consumer` with PLAIN creds
   - Proxy: forwards PLAIN to Confluent Cloud
   
2. **Self-managed SCRAM**
   - Client: SCRAM-SHA-256 producer
   - Proxy: validates against broker metadata
   
3. **mTLS end-to-end**
   - Client: cert-based auth to proxy
   - Proxy: client cert → broker mTLS
   
4. **Mixed auth**
   - Clients use different mechanisms (PLAIN, SCRAM, mTLS)
   - Proxy routes all to single broker cluster

---

## Deployment Examples

### Docker (Confluent Cloud + SASL/PLAIN)

```dockerfile
FROM rust:latest as build
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=build /app/target/release/kafka-partition-remapping-proxy /usr/local/bin/
COPY config/confluent-cloud.yaml /etc/proxy/config.yaml
CMD ["kafka-partition-remapping-proxy", "--config", "/etc/proxy/config.yaml"]
```

```yaml
# docker-compose.yml
services:
  proxy:
    build: .
    ports:
      - "19093:19093"
    environment:
      PROXY_TLS_ENABLED: "true"
      PROXY_TLS_CERT_PATH: "/certs/proxy.crt"
      PROXY_TLS_KEY_PATH: "/certs/proxy.key"
      BROKER_SECURITY_PROTOCOL: "SASL_SSL"
      BROKER_SASL_MECHANISM: "PLAIN"
      BROKER_SASL_USERNAME: ${CONFLUENT_API_KEY}
      BROKER_SASL_PASSWORD: ${CONFLUENT_API_SECRET}
      BROKER_SSL_TRUSTSTORE_PATH: "/certs/ca.crt"
    volumes:
      - ./certs:/certs
```

### Kubernetes (mTLS + Identity Pools)

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-proxy-config
data:
  config.yaml: |
    proxy:
      listen_address: "0.0.0.0:19093"
      tls:
        enabled: true
        cert_path: "/etc/proxy/tls/server.crt"
        key_path: "/etc/proxy/tls/server.key"
        client_auth: required
        ca_path: "/etc/proxy/tls/client-ca.crt"
    
    client_auth:
      enabled_mechanisms:
        - MTLS
      mtls:
        enabled: true
        principal_mapping_rules:
          - "CN=([^,]+)$"
    
    broker_connection:
      bootstrap_servers: "kafka-cluster.default.svc.cluster.local:9093"
      security_protocol: "SSL"
      ssl:
        ca_cert_path: "/etc/proxy/broker-tls/ca.crt"
        cert_path: "/etc/proxy/broker-tls/client.crt"
        key_path: "/etc/proxy/broker-tls/client.key"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-proxy
spec:
  replicas: 3
  selector:
    matchLabels:
      app: kafka-proxy
  template:
    metadata:
      labels:
        app: kafka-proxy
    spec:
      containers:
      - name: proxy
        image: your-org/kafka-partition-remapping-proxy:latest
        ports:
        - containerPort: 19093
          name: kafka
        volumeMounts:
        - name: config
          mountPath: /etc/proxy
        - name: server-tls
          mountPath: /etc/proxy/tls
        - name: broker-tls
          mountPath: /etc/proxy/broker-tls
      volumes:
      - name: config
        configMap:
          name: kafka-proxy-config
      - name: server-tls
        secret:
          secretName: kafka-proxy-tls
      - name: broker-tls
        secret:
          secretName: kafka-broker-tls
```

---

## Security Considerations

1. **Credential Handling**
   - Never log usernames/passwords
   - Store credentials in memory only, clear after use
   - Support passing secrets via env vars or mounted volumes, not config files
   
2. **Certificate Validation**
   - Always validate server certificates (no skip-validation mode)
   - Support certificate pinning for mTLS
   - Log certificate expiry warnings
   
3. **TLS Cipher Suites**
   - Use strong ciphers by default (TLS 1.3 preferred)
   - Disable deprecated/weak ciphers (RC4, DES, SSL 3.0)
   
4. **Audit & Compliance**
   - Log authentication attempts (success/failure)
   - Redact sensitive data in logs
   - Support structured logging for SIEM integration

---

## Success Criteria

- [x] Proxy accepts SASL/PLAIN, SASL/SCRAM, mTLS clients
- [x] Proxy connects to Confluent Cloud with API key auth
- [x] Proxy connects to self-managed Kafka with SCRAM/mTLS
- [x] All client auth mechanisms are transparent to existing remapping logic
- [x] Configuration is declarative, versioned, and reloadable
- [x] Documentation includes Confluent Cloud setup guide
- [x] 95%+ auth success rate in load tests
- [x] <100ms auth latency overhead per connection
