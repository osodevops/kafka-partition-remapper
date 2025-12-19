# Configuration Reference

## Overview

The Kafka Partition Remapper is configured via a YAML configuration file. All options with their defaults and descriptions are documented here.

## Complete Configuration Schema

```yaml
# Proxy listener configuration
listen:
  address: "0.0.0.0:9092"           # Bind address
  advertised_address: null          # Address clients use to reconnect (optional)
  max_connections: 1000             # Maximum concurrent connections
  security:                         # Client-facing security (optional)
    protocol: PLAINTEXT             # PLAINTEXT|SSL|SASL_PLAINTEXT|SASL_SSL
    tls:                            # TLS configuration (for SSL/SASL_SSL)
      cert_path: "/path/to/cert"
      key_path: "/path/to/key"
      ca_cert_path: null            # CA for client cert validation (mTLS)
      require_client_cert: false    # Require mTLS
      principal:
        mapping_rules: ["DEFAULT"]
    sasl:                           # SASL configuration (for SASL_*/SASL_SSL)
      enabled_mechanisms:
        - PLAIN
        - SCRAM-SHA-256
        - SCRAM-SHA-512
        - OAUTHBEARER
      credentials:
        users: {}                   # Username -> password/credentials
        file: null                  # External credentials file
      oauthbearer:                  # OAuth configuration (optional)
        jwks_url: null
        public_key_path: null
        issuer: null
        audience: null
        required_scopes: []
        clock_skew_seconds: 60

# Kafka broker configuration
kafka:
  bootstrap_servers:                # Required
    - "broker1:9092"
    - "broker2:9092"
  connection_timeout_ms: 10000
  request_timeout_ms: 30000
  metadata_refresh_interval_secs: 30
  security_protocol: PLAINTEXT      # PLAINTEXT|SSL|SASL_PLAINTEXT|SASL_SSL
  tls:                              # TLS to brokers (for SSL/SASL_SSL)
    ca_cert_path: null
    cert_path: null
    key_path: null
    insecure_skip_verify: false
  sasl:                             # SASL to brokers (for SASL_*/SASL_SSL)
    mechanism: PLAIN
    username: null
    password: null

# Partition mapping configuration
mapping:
  virtual_partitions: 100           # Required
  physical_partitions: 10           # Required
  offset_range: 1099511627776       # Default: 2^40
  topics: {}                        # Per-topic overrides

# Metrics configuration
metrics:
  enabled: true
  address: "0.0.0.0:9090"

# Logging configuration
logging:
  level: "info"                     # trace|debug|info|warn|error
  format: "json"                    # json|text
```

## Section Reference

### listen

Controls how the proxy accepts client connections.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `address` | String | `"0.0.0.0:9092"` | TCP bind address (host:port) |
| `advertised_address` | String | None | Address returned in Metadata responses. If not set, uses `address`. Set this when the proxy is behind a load balancer. |
| `max_connections` | Integer | `1000` | Maximum concurrent client connections. New connections are rejected when limit is reached. |
| `security` | Object | None | Security configuration for client connections |

#### listen.security

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `protocol` | Enum | `PLAINTEXT` | Security protocol: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `tls` | Object | None | TLS configuration (required for SSL/SASL_SSL) |
| `sasl` | Object | None | SASL configuration (required for SASL_*/SASL_SSL) |

#### listen.security.tls

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `cert_path` | Path | Required | Path to server certificate (PEM format) |
| `key_path` | Path | Required | Path to private key (PEM format) |
| `ca_cert_path` | Path | None | CA certificate for client validation (enables mTLS) |
| `require_client_cert` | Boolean | `false` | Require clients to present certificates |
| `principal.mapping_rules` | List | `["DEFAULT"]` | Rules for extracting principal from client certificates |

#### listen.security.sasl

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled_mechanisms` | List | Empty | SASL mechanisms to enable: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER` |
| `credentials` | Object | Required | Credential configuration |
| `oauthbearer` | Object | None | OAUTHBEARER-specific configuration |

#### listen.security.sasl.credentials

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `users` | Map | Empty | Username to credential mapping (inline) |
| `file` | Path | None | Path to external credentials file |

**Inline credential formats:**

```yaml
# Simple password (for PLAIN, auto-hashed for SCRAM)
users:
  alice: "password123"

# SCRAM with explicit settings
users:
  bob:
    password: "password123"
    iterations: 4096  # PBKDF2 iterations

# Pre-computed SCRAM credentials
users:
  carol:
    salt: "base64-salt"
    iterations: 4096
    stored_key: "base64-stored-key"
    server_key: "base64-server-key"
```

#### listen.security.sasl.oauthbearer

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `jwks_url` | URL | None | JWKS endpoint for key discovery |
| `public_key_path` | Path | None | Path to public key for token validation |
| `issuer` | String | None | Expected token issuer (`iss` claim) |
| `audience` | String | None | Expected token audience (`aud` claim) |
| `required_scopes` | List | Empty | Required scopes in token |
| `clock_skew_seconds` | Integer | `60` | Allowed clock skew for expiration |

---

### kafka

Configures connections to Kafka brokers.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `bootstrap_servers` | List | Required | Broker addresses for initial connection |
| `connection_timeout_ms` | Integer | `10000` | TCP connection timeout |
| `request_timeout_ms` | Integer | `30000` | Request/response timeout |
| `metadata_refresh_interval_secs` | Integer | `30` | How often to refresh cluster metadata |
| `security_protocol` | Enum | `PLAINTEXT` | Security protocol to brokers |
| `tls` | Object | None | TLS configuration (for SSL/SASL_SSL) |
| `sasl` | Object | None | SASL configuration (for SASL_*/SASL_SSL) |

#### kafka.tls

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `ca_cert_path` | Path | None | CA certificate for broker verification |
| `cert_path` | Path | None | Client certificate (for mTLS to brokers) |
| `key_path` | Path | None | Client private key (for mTLS to brokers) |
| `insecure_skip_verify` | Boolean | `false` | Skip certificate verification (NOT recommended) |

#### kafka.sasl

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `mechanism` | String | Required | SASL mechanism: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `username` | String | Required | Username for broker authentication |
| `password` | String | Required | Password for broker authentication |

---

### mapping

Configures partition remapping behavior.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `virtual_partitions` | Integer | Required | Number of virtual partitions clients see |
| `physical_partitions` | Integer | Required | Number of physical partitions on broker |
| `offset_range` | Integer | `1099511627776` (2^40) | Max offsets per virtual partition |
| `topics` | Map | Empty | Per-topic configuration overrides |

**Constraints:**
- `physical_partitions >= 1`
- `virtual_partitions >= physical_partitions`
- `virtual_partitions % physical_partitions == 0` (must be evenly divisible)
- `offset_range >= 1048576` (2^20)

#### mapping.topics

Per-topic overrides. Keys can be exact topic names or regex patterns.

```yaml
mapping:
  virtual_partitions: 100
  physical_partitions: 10

  topics:
    # Exact topic name
    high-volume-topic:
      virtual_partitions: 500
      physical_partitions: 50

    # Regex pattern (matches events, events-v2, events-prod, etc.)
    "events.*":
      virtual_partitions: 200
      physical_partitions: 20

    # Another pattern
    "metrics\\..*":
      virtual_partitions: 50
      physical_partitions: 5
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `virtual_partitions` | Integer | Inherits global | Virtual partitions for this topic |
| `physical_partitions` | Integer | Inherits global | Physical partitions for this topic |
| `offset_range` | Integer | Inherits global | Offset range for this topic |

---

### metrics

Prometheus metrics endpoint configuration.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | Boolean | `true` | Enable metrics endpoint |
| `address` | String | `"0.0.0.0:9090"` | Metrics HTTP bind address |

---

### logging

Logging configuration.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `level` | String | `"info"` | Log level: `trace`, `debug`, `info`, `warn`, `error` |
| `format` | String | `"json"` | Output format: `json`, `text` |

---

## Environment Variable Expansion

Configuration values can reference environment variables using `${VAR_NAME}` syntax:

```yaml
kafka:
  sasl:
    username: "${KAFKA_USERNAME}"
    password: "${KAFKA_PASSWORD}"

listen:
  security:
    sasl:
      credentials:
        users:
          service: "${SERVICE_PASSWORD}"
```

---

## Example Configurations

### Minimal Development Configuration

```yaml
listen:
  address: "0.0.0.0:9092"

kafka:
  bootstrap_servers:
    - "localhost:9092"

mapping:
  virtual_partitions: 100
  physical_partitions: 10
```

### Production with SASL_SSL

```yaml
listen:
  address: "0.0.0.0:9093"
  advertised_address: "kafka-proxy.example.com:9093"
  max_connections: 5000
  security:
    protocol: SASL_SSL
    tls:
      cert_path: "/etc/kafka-proxy/tls/server.crt"
      key_path: "/etc/kafka-proxy/tls/server.key"
    sasl:
      enabled_mechanisms:
        - SCRAM-SHA-256
      credentials:
        file: "/etc/kafka-proxy/credentials.yaml"

kafka:
  bootstrap_servers:
    - "broker1.kafka.internal:9093"
    - "broker2.kafka.internal:9093"
    - "broker3.kafka.internal:9093"
  connection_timeout_ms: 5000
  request_timeout_ms: 30000
  security_protocol: SASL_SSL
  tls:
    ca_cert_path: "/etc/kafka-proxy/tls/ca.crt"
  sasl:
    mechanism: SCRAM-SHA-256
    username: "${KAFKA_PROXY_USERNAME}"
    password: "${KAFKA_PROXY_PASSWORD}"

mapping:
  virtual_partitions: 1000
  physical_partitions: 100
  topics:
    "high-volume.*":
      virtual_partitions: 5000
      physical_partitions: 500
    low-volume-config:
      virtual_partitions: 10
      physical_partitions: 1

metrics:
  enabled: true
  address: "0.0.0.0:9090"

logging:
  level: "info"
  format: "json"
```

### mTLS with Principal Mapping

```yaml
listen:
  address: "0.0.0.0:9093"
  security:
    protocol: SSL
    tls:
      cert_path: "/etc/tls/server.crt"
      key_path: "/etc/tls/server.key"
      ca_cert_path: "/etc/tls/client-ca.crt"
      require_client_cert: true
      principal:
        mapping_rules:
          # Extract CN and lowercase it
          - "RULE:^CN=([^,]+).*$/$1/L"
          # Fallback to full DN
          - "DEFAULT"

kafka:
  bootstrap_servers:
    - "broker:9092"

mapping:
  virtual_partitions: 100
  physical_partitions: 10
```

### OAuth Bearer Authentication

```yaml
listen:
  address: "0.0.0.0:9093"
  security:
    protocol: SASL_SSL
    tls:
      cert_path: "/etc/tls/server.crt"
      key_path: "/etc/tls/server.key"
    sasl:
      enabled_mechanisms:
        - OAUTHBEARER
      oauthbearer:
        jwks_url: "https://auth.example.com/.well-known/jwks.json"
        issuer: "https://auth.example.com"
        audience: "kafka-proxy"
        required_scopes:
          - "kafka:produce"
          - "kafka:consume"
        clock_skew_seconds: 120

kafka:
  bootstrap_servers:
    - "broker:9092"

mapping:
  virtual_partitions: 100
  physical_partitions: 10
```

### Multi-Topic Configuration

```yaml
listen:
  address: "0.0.0.0:9092"

kafka:
  bootstrap_servers:
    - "broker:9092"

mapping:
  # Default mapping (10:1 ratio)
  virtual_partitions: 100
  physical_partitions: 10

  topics:
    # High-throughput event streams (50:1 ratio)
    "events":
      virtual_partitions: 500
      physical_partitions: 10

    # Pattern: all audit topics (20:1 ratio)
    "audit\\..*":
      virtual_partitions: 100
      physical_partitions: 5

    # Low-volume configuration topics (no remapping)
    "config":
      virtual_partitions: 3
      physical_partitions: 3

    # Metrics topics (10:1 ratio with smaller offset range)
    "metrics\\..*":
      virtual_partitions: 100
      physical_partitions: 10
      offset_range: 1073741824  # 2^30
```

---

## Configuration Validation

The proxy validates configuration on startup. Common validation errors:

| Error | Cause | Fix |
|-------|-------|-----|
| `InvalidCompressionRatio` | V not divisible by P | Ensure `virtual_partitions % physical_partitions == 0` |
| `VirtualLessThanPhysical` | V < P | Set `virtual_partitions >= physical_partitions` |
| `InvalidPhysicalPartitions` | P = 0 | Set `physical_partitions >= 1` |
| `OffsetRangeTooSmall` | R < 2^20 | Increase `offset_range` |
| `IoError` | File not found | Check file paths for TLS certs, credentials |
| `ParseError` | Invalid YAML | Check YAML syntax |

---

## CLI Options

Command-line options override configuration file values:

```bash
kafka-partition-proxy [OPTIONS]

Options:
  -c, --config <FILE>     Configuration file path [default: config.yaml]
  -v, --verbose           Increase verbosity (-v debug, -vv trace)
  --listen <ADDRESS>      Override listen address
  -h, --help              Print help
  -V, --version           Print version
```

**Example:**
```bash
# Use custom config and override listen address
kafka-partition-proxy -c /etc/kafka-proxy/config.yaml --listen 0.0.0.0:19092 -v
```

---

## Related Documentation

- [Architecture](./architecture.md) - System design overview
- [Authentication](./authentication.md) - Security configuration details
- [Protocol Mapping](./protocol-mapping.md) - Remapping algorithm details
- [Getting Started](./getting-started.md) - Quick start guide
