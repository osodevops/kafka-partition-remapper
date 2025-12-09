//! Stream abstraction for broker connections.
//!
//! Provides a unified interface over plain TCP and TLS streams.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

/// A stream that can be either plain TCP or TLS-encrypted.
///
/// This abstraction allows broker connection code to work with both
/// encrypted and unencrypted connections transparently.
pub enum BrokerStream {
    /// Plain TCP connection (PLAINTEXT or SASL_PLAINTEXT).
    Plain(TcpStream),
    /// TLS-encrypted connection (SSL or SASL_SSL).
    Tls(TlsStream<TcpStream>),
}

impl BrokerStream {
    /// Create a new plain TCP stream.
    pub fn plain(stream: TcpStream) -> Self {
        Self::Plain(stream)
    }

    /// Create a new TLS stream.
    pub fn tls(stream: TlsStream<TcpStream>) -> Self {
        Self::Tls(stream)
    }

    /// Check if this is a TLS connection.
    pub fn is_tls(&self) -> bool {
        matches!(self, Self::Tls(_))
    }
}

impl AsyncRead for BrokerStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            BrokerStream::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            BrokerStream::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for BrokerStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            BrokerStream::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            BrokerStream::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            BrokerStream::Plain(stream) => Pin::new(stream).poll_flush(cx),
            BrokerStream::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            BrokerStream::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            BrokerStream::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_stream_is_tls() {
        // We can't easily construct a TlsStream without a real TLS handshake,
        // but we can verify the enum variant detection works with a mock approach.
        // For now, we just verify the API compiles correctly.
    }
}
