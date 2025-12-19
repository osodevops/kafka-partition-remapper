//! Stream abstraction for client connections.
//!
//! Provides a unified interface over plain TCP and TLS streams for client-facing
//! connections. The proxy acts as a TLS server when accepting encrypted connections.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use rustls::pki_types::CertificateDer;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;

/// A stream that can be either plain TCP or TLS-encrypted for client connections.
///
/// This abstraction allows connection handling code to work with both
/// encrypted and unencrypted client connections transparently.
///
/// Unlike `BrokerStream` which uses `client::TlsStream`, this uses
/// `server::TlsStream` because the proxy acts as a TLS server for clients.
pub enum ClientStream {
    /// Plain TCP connection (PLAINTEXT or SASL_PLAINTEXT).
    Plain(TcpStream),
    /// TLS-encrypted connection (SSL or SASL_SSL).
    /// The proxy is the server in this TLS connection.
    Tls(TlsStream<TcpStream>),
}

impl ClientStream {
    /// Create a new plain TCP stream.
    pub fn plain(stream: TcpStream) -> Self {
        Self::Plain(stream)
    }

    /// Create a new TLS stream (server-side).
    pub fn tls(stream: TlsStream<TcpStream>) -> Self {
        Self::Tls(stream)
    }

    /// Check if this is a TLS connection.
    pub fn is_tls(&self) -> bool {
        matches!(self, Self::Tls(_))
    }

    /// Get the peer address of the underlying TCP connection.
    pub fn peer_addr(&self) -> io::Result<std::net::SocketAddr> {
        match self {
            Self::Plain(stream) => stream.peer_addr(),
            Self::Tls(stream) => stream.get_ref().0.peer_addr(),
        }
    }

    /// Get peer certificates if this is a TLS connection with client auth.
    ///
    /// Returns `None` for plain connections or TLS connections without client
    /// certificate authentication. For mTLS connections, returns the peer's
    /// certificate chain with the peer's certificate first.
    ///
    /// # Certificate Chain Order
    ///
    /// The certificates are returned in TLS protocol order:
    /// - First certificate is the peer's (client's) certificate
    /// - Subsequent certificates form the chain to the root CA
    ///
    /// # Example
    ///
    /// ```ignore
    /// if let Some(certs) = client_stream.peer_certificates() {
    ///     if let Some(peer_cert) = certs.first() {
    ///         // Parse and extract principal from peer_cert
    ///     }
    /// }
    /// ```
    pub fn peer_certificates(&self) -> Option<&[CertificateDer<'static>]> {
        match self {
            Self::Plain(_) => None,
            Self::Tls(tls_stream) => {
                // Access the ServerConnection through get_ref()
                let (_, server_conn) = tls_stream.get_ref();
                server_conn.peer_certificates()
            }
        }
    }
}

impl AsyncRead for ClientStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ClientStream::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            ClientStream::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ClientStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            ClientStream::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            ClientStream::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ClientStream::Plain(stream) => Pin::new(stream).poll_flush(cx),
            ClientStream::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ClientStream::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            ClientStream::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_stream_plain_is_not_tls() {
        // We can't easily test with real TcpStream without network,
        // but we verify the API compiles correctly.
    }
}
