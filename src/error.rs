//! Transfer-level errors, classified by whether retrying could possibly help.

use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum TransferError {
    /// Connection reset, DNS failure, TLS handshake failure, network down.
    #[error("network error: {0}")]
    Network(String),

    /// No bytes arrived within the read timeout.
    #[error("timed out after {0:?} with no data")]
    Timeout(Duration),

    /// The server answered, unhappily.
    #[error("server returned HTTP {status}")]
    Status {
        status: u16,
        retry_after: Option<Duration>,
    },

    /// Validators say we are no longer looking at the same bytes. Retrying
    /// cannot fix this and continuing would corrupt the file.
    #[error("remote resource changed: {0}")]
    RemoteChanged(String),

    /// The server broke the HTTP contract — ignored `Range`, returned a
    /// `Content-Range` that does not match what we asked for, sent more bytes
    /// than it promised.
    #[error("protocol violation: {0}")]
    Protocol(String),

    /// Local disk problem.
    #[error("write failed: {0}")]
    Io(String),

    /// Graceful shutdown, not really a failure.
    #[error("cancelled")]
    Cancelled,
}

impl TransferError {
    /// PRD §14: retry connection resets, timeouts, DNS failures, 408, 429, 5xx.
    /// Nothing else.
    pub fn is_retryable(&self) -> bool {
        match self {
            TransferError::Network(_) | TransferError::Timeout(_) => true,
            TransferError::Status { status, .. } => {
                *status == 408 || *status == 429 || (500..600).contains(status)
            }
            // A truthful server that ignored our Range will ignore it again;
            // the engine handles that by falling back to sequential, not by
            // retrying blindly.
            TransferError::Protocol(_)
            | TransferError::RemoteChanged(_)
            | TransferError::Io(_)
            | TransferError::Cancelled => false,
        }
    }

    /// Server-suggested delay, honoured when present (PRD §14).
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            TransferError::Status { retry_after, .. } => *retry_after,
            _ => None,
        }
    }

    pub fn from_reqwest(err: &reqwest::Error) -> Self {
        // Everything reqwest reports that is not a timeout — connect failures,
        // TLS errors, resets, truncated bodies — is a transient network fault as
        // far as our retry policy is concerned.
        if err.is_timeout() {
            TransferError::Timeout(Duration::ZERO)
        } else {
            TransferError::Network(sanitize_reqwest(err))
        }
    }
}

/// `reqwest`'s `Display` includes the full URL, which may carry a signed token
/// or basic-auth userinfo. Strip it before the message can reach a log.
fn sanitize_reqwest(err: &reqwest::Error) -> String {
    let mut msg = err.to_string();
    if let Some(url) = err.url() {
        let redacted = crate::fmt::short_url(url.as_str());
        msg = msg.replace(url.as_str(), &redacted);
    }
    // Chain the source for context, minus URLs.
    let mut source = std::error::Error::source(err);
    let mut depth = 0;
    while let Some(s) = source {
        if depth >= 3 {
            break;
        }
        msg.push_str(": ");
        msg.push_str(&s.to_string());
        source = s.source();
        depth += 1;
    }
    msg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_classification() {
        assert!(TransferError::Network("reset".into()).is_retryable());
        assert!(TransferError::Timeout(Duration::from_secs(1)).is_retryable());
        for s in [408, 429, 500, 502, 503, 504] {
            assert!(
                TransferError::Status {
                    status: s,
                    retry_after: None
                }
                .is_retryable(),
                "{s} should retry"
            );
        }
        for s in [400, 401, 403, 404, 416] {
            assert!(
                !TransferError::Status {
                    status: s,
                    retry_after: None
                }
                .is_retryable(),
                "{s} should not retry"
            );
        }
        assert!(!TransferError::RemoteChanged("etag".into()).is_retryable());
        assert!(!TransferError::Protocol("ignored range".into()).is_retryable());
        assert!(!TransferError::Cancelled.is_retryable());
    }
}
