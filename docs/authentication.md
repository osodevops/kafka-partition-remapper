# Authentication & Security Documentation

## Overview

The Kafka Partition Remapper provides comprehensive security features including:

- **TLS/SSL encryption** for both client and broker connections
- **Mutual TLS (mTLS)** with client certificate authentication
- **SASL authentication** with multiple mechanism support
- **Principal extraction** from certificates and SASL credentials

The proxy acts as a security boundary, terminating client connections and establishing separate secured connections to brokers.

## Security Protocols

Four security protocols are supported, matching Kafka's native options:

| Protocol | TLS | SASL | Use Case |
|----------|-----|------|----------|
| `PLAINTEXT` | No | No | Development only |
| `SSL` | Yes | No | TLS encryption without SASL |
| `SASL_PLAINTEXT` | No | Yes | SASL without encryption (not recommended) |
| `SASL_SSL` | Yes | Yes | Full security (recommended for production) |

## SASL Authentication

### Supported Mechanisms

#### PLAIN

Simple username/password authentication. Credentials are sent in cleartext, so **always use with TLS**.

**Wire Format:**
```
\0<username>\0<password>
```

**Configuration:**
```yaml
listen:
  security:
    protocol: SASL_SSL
    sasl:
      enabled_mechanisms:
        - PLAIN
      credentials:
        users:
          alice: "alice-password"
          bob: "bob-password"
```

#### SCRAM-SHA-256 / SCRAM-SHA-512

Salted Challenge Response Authentication Mechanism (RFC 5802, RFC 7677). Passwords are never sent over the wire - uses cryptographic challenge-response.

**Authentication Flow:**
```
┌──────────┐                              ┌──────────┐
│  Client  │                              │  Server  │
└────┬─────┘                              └────┬─────┘
     │                                         │
     │  1. client-first-message                │
     │  n=username,r=client-nonce              │
     │────────────────────────────────────────▶│
     │                                         │
     │  2. server-first-message                │
     │  r=combined-nonce,s=salt,i=iterations   │
     │◀────────────────────────────────────────│
     │                                         │
     │  3. client-final-message                │
     │  c=channel-binding,r=nonce,p=proof      │
     │────────────────────────────────────────▶│
     │                                         │
     │  4. server-final-message                │
     │  v=server-signature                     │
     │◀────────────────────────────────────────│
     │                                         │
```

**SCRAM Features:**
- Password never sent over the wire
- Server proof prevents man-in-the-middle attacks
- Salt and iteration count prevent rainbow table attacks
- SHA-256: 32-byte hash output
- SHA-512: 64-byte hash output (more secure, larger overhead)

**Configuration with stored credentials:**
```yaml
listen:
  security:
    protocol: SASL_SSL
    sasl:
      enabled_mechanisms:
        - SCRAM-SHA-256
        - SCRAM-SHA-512
      credentials:
        users:
          alice:
            password: "alice-password"  # Will be hashed
          bob:
            # Pre-computed SCRAM credentials
            salt: "base64-encoded-salt"
            iterations: 4096
            stored_key: "base64-encoded-stored-key"
            server_key: "base64-encoded-server-key"
```

**Credential Storage Structure:**
```rust
pub struct ScramCredentials {
    pub salt: Vec<u8>,       // Random salt (16 bytes recommended)
    pub iterations: u32,     // PBKDF2 iterations (4096+ recommended)
    pub stored_key: Vec<u8>, // H(ClientKey)
    pub server_key: Vec<u8>, // HMAC(SaltedPassword, "Server Key")
}
```

#### OAUTHBEARER

OAuth 2.0 bearer token authentication per KIP-255. Supports JWT tokens with signature validation.

**Token Format (JWT):**
```
<header>.<payload>.<signature>
```

**Configuration:**
```yaml
listen:
  security:
    protocol: SASL_SSL
    sasl:
      enabled_mechanisms:
        - OAUTHBEARER
      oauthbearer:
        # Option 1: Validate with JWKS endpoint
        jwks_url: "https://auth.example.com/.well-known/jwks.json"

        # Option 2: Validate with public key
        public_key_path: "/path/to/public-key.pem"

        # Token validation options
        issuer: "https://auth.example.com"
        audience: "kafka-proxy"
        required_scopes:
          - "kafka:read"
          - "kafka:write"
        clock_skew_seconds: 60
```

**Token Validation:**
1. Signature verification (RS256, RS384, RS512, ES256, ES384, ES512)
2. Expiration check (`exp` claim)
3. Not-before check (`nbf` claim)
4. Issuer validation (`iss` claim)
5. Audience validation (`aud` claim)
6. Scope validation (custom `scope` claim)

### SASL Server Architecture

```rust
pub struct SaslServer {
    authenticators: Vec<Arc<dyn SaslAuthenticator>>,
}

pub trait SaslAuthenticator: Send + Sync + Debug {
    /// Returns the SASL mechanism name (e.g., "PLAIN", "SCRAM-SHA-256")
    fn mechanism_name(&self) -> &'static str;

    /// Process one step of authentication
    /// Returns Complete, Continue (for multi-step), or Failed
    fn authenticate_step(
        &self,
        client_message: &[u8],
        session: &mut SaslSession
    ) -> SaslStepResult;

    /// Check if authentication is complete
    fn is_complete(&self, session: &SaslSession) -> bool;
}

pub enum SaslStepResult {
    Complete(Vec<u8>),          // Authentication succeeded, optional final response
    Continue(Vec<u8>),          // More steps needed, response to client
    Failed(AuthError),          // Authentication failed
}
```

### SASL Session State

Each connection maintains a `SaslSession` for multi-step authentication:

```rust
pub struct SaslSession {
    mechanism: Option<String>,
    state: SessionState,
    principal: Option<String>,
    custom_data: HashMap<String, Vec<u8>>,
}

pub enum SessionState {
    Initial,
    InProgress,
    Complete,
    Failed,
}
```

### SASL Protocol Flow

```
┌──────────┐                              ┌──────────┐
│  Client  │                              │  Proxy   │
└────┬─────┘                              └────┬─────┘
     │                                         │
     │  SaslHandshake Request                  │
     │  mechanism: "SCRAM-SHA-256"             │
     │────────────────────────────────────────▶│
     │                                         │
     │  SaslHandshake Response                 │
     │  error_code: 0                          │
     │  enabled_mechanisms: [...]              │
     │◀────────────────────────────────────────│
     │                                         │
     │  SaslAuthenticate Request               │
     │  auth_bytes: <client-first>             │
     │────────────────────────────────────────▶│
     │                                         │
     │  SaslAuthenticate Response              │
     │  auth_bytes: <server-first>             │
     │◀────────────────────────────────────────│
     │                                         │
     │  SaslAuthenticate Request               │
     │  auth_bytes: <client-final>             │
     │────────────────────────────────────────▶│
     │                                         │
     │  SaslAuthenticate Response              │
     │  error_code: 0                          │
     │  auth_bytes: <server-final>             │
     │◀────────────────────────────────────────│
     │                                         │
     │  Regular Kafka requests...              │
     │────────────────────────────────────────▶│
```

## TLS Configuration

### Client-Side TLS (Proxy as Server)

The proxy terminates TLS connections from clients:

```yaml
listen:
  address: "0.0.0.0:9093"
  security:
    protocol: SSL  # or SASL_SSL
    tls:
      # Server certificate (required)
      cert_path: "/path/to/server.crt"
      key_path: "/path/to/server.key"

      # CA for client certificate validation (optional, for mTLS)
      ca_cert_path: "/path/to/ca.crt"

      # Require client certificates (mTLS)
      require_client_cert: false

      # Principal extraction from certificates
      principal:
        mapping_rules:
          - "DEFAULT"
```

### Broker-Side TLS (Proxy as Client)

The proxy establishes TLS connections to brokers:

```yaml
kafka:
  bootstrap_servers:
    - "broker1:9093"
    - "broker2:9093"
  security_protocol: SSL  # or SASL_SSL
  tls:
    # CA certificate to verify broker certificates
    ca_cert_path: "/path/to/ca.crt"

    # Client certificate for mTLS to brokers (optional)
    cert_path: "/path/to/client.crt"
    key_path: "/path/to/client.key"

    # Skip hostname verification (not recommended)
    insecure_skip_verify: false
```

### TLS Implementation Details

**TlsServerAcceptor** (for client connections):
```rust
pub struct TlsServerAcceptor {
    acceptor: tokio_rustls::TlsAcceptor,
}

impl TlsServerAcceptor {
    pub async fn accept(&self, stream: TcpStream) -> TlsResult<TlsStream<TcpStream>>;
}
```

**TlsConnector** (for broker connections):
```rust
pub struct TlsConnector {
    connector: tokio_rustls::TlsConnector,
}

impl TlsConnector {
    pub async fn connect(&self, domain: &str, stream: TcpStream)
        -> TlsResult<TlsStream<TcpStream>>;
}
```

## Principal Extraction

Principals identify authenticated users for authorization and auditing.

### From SASL

Each SASL mechanism extracts principals differently:

| Mechanism | Principal Format |
|-----------|------------------|
| PLAIN | `User:username` |
| SCRAM-SHA-* | `User:username` |
| OAUTHBEARER | `User:sub` (from JWT subject claim) |

### From Client Certificates (mTLS)

Certificate DN (Distinguished Name) can be transformed using mapping rules:

```yaml
tls:
  principal:
    mapping_rules:
      # Use DN unchanged
      - "DEFAULT"

      # Extract CN only
      - "RULE:^CN=([^,]+).*$/$1/"

      # Extract and lowercase
      - "RULE:^CN=([^,]+).*$/$1/L"

      # Extract and uppercase
      - "RULE:^CN=([^,]+).*$/$1/U"
```

**Mapping Rule Format:**
```
RULE:<pattern>/<replacement>/[LU]
```

- `pattern`: Regex to match against certificate DN
- `replacement`: Replacement pattern with capture groups
- `L`: Transform result to lowercase
- `U`: Transform result to uppercase

**Example Transformations:**

| Certificate DN | Rule | Result |
|----------------|------|--------|
| `CN=alice,OU=Engineering,O=ACME` | `DEFAULT` | `CN=alice,OU=Engineering,O=ACME` |
| `CN=alice,OU=Engineering,O=ACME` | `RULE:^CN=([^,]+).*$/$1/` | `alice` |
| `CN=Alice.Smith,OU=Eng` | `RULE:^CN=([^,]+).*$/$1/L` | `alice.smith` |

### SslPrincipalMapper Implementation

```rust
pub struct SslPrincipalMapper {
    rules: Vec<MappingRule>,
}

pub enum MappingRule {
    Default,
    Rule {
        pattern: Regex,
        replacement: String,
        case_transform: Option<CaseTransform>,
    },
}

pub enum CaseTransform {
    Lowercase,
    Uppercase,
}

impl SslPrincipalMapper {
    pub fn map(&self, dn: &str) -> String;
}
```

## Credential Storage

### Inline Credentials

Simple configuration with credentials in the YAML file:

```yaml
sasl:
  credentials:
    users:
      alice: "alice-secret-password"
      bob: "bob-secret-password"
```

### File-Based Credentials

Reference an external credentials file:

```yaml
sasl:
  credentials:
    file: "/etc/kafka-proxy/credentials.yaml"
```

**credentials.yaml:**
```yaml
users:
  alice:
    password: "alice-secret-password"
  bob:
    # Pre-computed SCRAM credentials
    mechanism: SCRAM-SHA-256
    salt: "dGhpcyBpcyBhIHNhbHQ="
    iterations: 4096
    stored_key: "c3RvcmVkIGtleSBoZXJl"
    server_key: "c2VydmVyIGtleSBoZXJl"
```

### Environment Variable Expansion

Credentials can reference environment variables:

```yaml
sasl:
  credentials:
    users:
      service-account: "${KAFKA_SERVICE_PASSWORD}"
```

### Credential Store Trait

```rust
pub trait CredentialStore: Send + Sync + Debug {
    /// Get plaintext password for PLAIN authentication
    fn get_plaintext(&self, username: &str) -> Option<String>;

    /// Get SCRAM credentials (salt, iterations, stored_key, server_key)
    fn get_scram_credentials(&self, username: &str) -> Option<ScramCredentials>;
}
```

## Broker Authentication

The proxy can authenticate to brokers using SASL:

```yaml
kafka:
  security_protocol: SASL_SSL
  sasl:
    mechanism: SCRAM-SHA-256
    username: "proxy-service"
    password: "${BROKER_PASSWORD}"
```

This establishes a single identity for all client requests when forwarded to brokers. The proxy authenticates clients independently using its own credential store.

## Security Best Practices

### 1. Always Use TLS in Production

```yaml
listen:
  security:
    protocol: SASL_SSL  # Not SASL_PLAINTEXT

kafka:
  security_protocol: SASL_SSL  # Not SASL_PLAINTEXT
```

### 2. Use SCRAM Over PLAIN

PLAIN sends passwords in cleartext (even over TLS). SCRAM provides:
- No password transmission
- Server verification
- Replay attack protection

### 3. Rotate Credentials Regularly

For SCRAM, you can update stored credentials without knowing the original password.

### 4. Use Strong SCRAM Iterations

Minimum 4096 iterations, recommended 10000+:

```yaml
credentials:
  users:
    alice:
      password: "secret"
      iterations: 10000
```

### 5. Enable mTLS for Service-to-Service

When the proxy serves other services (not end users):

```yaml
listen:
  security:
    tls:
      require_client_cert: true
      ca_cert_path: "/path/to/service-ca.crt"
```

### 6. Restrict SASL Mechanisms

Only enable mechanisms you actually use:

```yaml
sasl:
  enabled_mechanisms:
    - SCRAM-SHA-256  # Only SCRAM
  # Don't include PLAIN unless necessary
```

### 7. Use Environment Variables for Secrets

Never commit credentials to version control:

```yaml
kafka:
  sasl:
    username: "${KAFKA_USERNAME}"
    password: "${KAFKA_PASSWORD}"
```

## Error Handling

Authentication errors return appropriate Kafka error codes:

| Error | Kafka Error Code | Description |
|-------|------------------|-------------|
| Unsupported mechanism | `UNSUPPORTED_SASL_MECHANISM` (33) | Requested mechanism not enabled |
| Invalid credentials | `SASL_AUTHENTICATION_FAILED` (58) | Username/password incorrect |
| Token expired | `SASL_AUTHENTICATION_FAILED` (58) | OAuth token past expiration |
| Invalid signature | `SASL_AUTHENTICATION_FAILED` (58) | JWT signature verification failed |
| TLS handshake failure | Connection closed | Certificate issues, cipher mismatch |

## Debugging Authentication Issues

Enable debug logging:

```yaml
logging:
  level: debug
```

Common issues:

1. **"Unsupported SASL mechanism"** - Check `enabled_mechanisms` includes requested mechanism
2. **"Authentication failed"** - Verify credentials match, check username case sensitivity
3. **"TLS handshake failed"** - Verify certificates are valid, check CA trust chain
4. **"Certificate verification failed"** - Ensure CA certificate is correct, check hostname matches

## Related Documentation

- [Architecture](./architecture.md) - Overall system design
- [Configuration Reference](./configuration.md) - Complete configuration options
- [Protocol Mapping](./protocol-mapping.md) - Partition remapping details
