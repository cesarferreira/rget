//! Exponential backoff with jitter (PRD §14).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::error::TransferError;

#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Total attempts per range, including the first one.
    pub max_attempts: u32,
    pub base: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 10,
            base: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Decision {
    /// Wait this long, then try again. `attempt` is the number of the attempt
    /// about to be made (1-based).
    Retry {
        delay: Duration,
        attempt: u32,
    },
    GiveUp,
}

impl RetryPolicy {
    /// `attempts_made` counts attempts already completed and failed.
    pub fn decide(&self, err: &TransferError, attempts_made: u32) -> Decision {
        if !err.is_retryable() || attempts_made >= self.max_attempts {
            return Decision::GiveUp;
        }
        let delay = err
            .retry_after()
            .map(|d| d.min(self.max_delay))
            .unwrap_or_else(|| self.backoff(attempts_made));
        Decision::Retry {
            delay,
            attempt: attempts_made + 1,
        }
    }

    /// Full jitter: `rand(0, min(max, base * 2^n))`. Full jitter beats
    /// equal jitter for de-synchronising a fleet of workers that all failed at
    /// the same instant, which is exactly our situation when a network drops.
    fn backoff(&self, attempts_made: u32) -> Duration {
        let exp = attempts_made.min(20);
        let ceiling = self
            .base
            .saturating_mul(1u32.checked_shl(exp).unwrap_or(u32::MAX))
            .min(self.max_delay);
        let ceil_ms = ceiling.as_millis() as u64;
        if ceil_ms == 0 {
            return Duration::ZERO;
        }
        // Keep at least half the ceiling so we do not hammer a struggling
        // server with a run of near-zero delays.
        let floor_ms = ceil_ms / 2;
        Duration::from_millis(floor_ms + jitter(ceil_ms - floor_ms + 1))
    }
}

/// xorshift64*, seeded once from the clock. We need spread, not
/// unpredictability, so pulling in a CSPRNG would be overkill.
fn jitter(modulo: u64) -> u64 {
    static STATE: AtomicU64 = AtomicU64::new(0);
    let mut x = STATE.load(Ordering::Relaxed);
    if x == 0 {
        x = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x9E3779B97F4A7C15)
            | 1;
    }
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    STATE.store(x, Ordering::Relaxed);
    x.wrapping_mul(0x2545F4914F6CDD1D) % modulo.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gives_up_on_permanent_errors() {
        let p = RetryPolicy::default();
        let err = TransferError::Status {
            status: 404,
            retry_after: None,
        };
        assert_eq!(p.decide(&err, 0), Decision::GiveUp);
    }

    #[test]
    fn gives_up_after_max_attempts() {
        let p = RetryPolicy {
            max_attempts: 3,
            ..Default::default()
        };
        let err = TransferError::Network("reset".into());
        assert!(matches!(p.decide(&err, 2), Decision::Retry { .. }));
        assert_eq!(p.decide(&err, 3), Decision::GiveUp);
    }

    #[test]
    fn delay_grows_and_is_capped() {
        let p = RetryPolicy {
            max_attempts: 30,
            base: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
        };
        let err = TransferError::Network("reset".into());
        let mut prev = Duration::ZERO;
        for n in 0..4 {
            let Decision::Retry { delay, attempt } = p.decide(&err, n) else {
                panic!("expected retry");
            };
            assert_eq!(attempt, n + 1);
            // Floor is half the ceiling, so growth is monotonic despite jitter.
            assert!(delay >= prev, "delay {delay:?} < prev {prev:?}");
            prev = delay;
        }
        for n in 10..20 {
            let Decision::Retry { delay, .. } = p.decide(&err, n) else {
                panic!("expected retry");
            };
            assert!(delay <= p.max_delay);
        }
    }

    #[test]
    fn honours_retry_after() {
        let p = RetryPolicy::default();
        let err = TransferError::Status {
            status: 429,
            retry_after: Some(Duration::from_secs(7)),
        };
        assert_eq!(
            p.decide(&err, 0),
            Decision::Retry {
                delay: Duration::from_secs(7),
                attempt: 1
            }
        );
    }

    #[test]
    fn clamps_absurd_retry_after() {
        let p = RetryPolicy::default();
        let err = TransferError::Status {
            status: 503,
            retry_after: Some(Duration::from_secs(86_400)),
        };
        let Decision::Retry { delay, .. } = p.decide(&err, 0) else {
            panic!("expected retry");
        };
        assert_eq!(delay, p.max_delay);
    }

    #[test]
    fn jitter_spreads() {
        let values: Vec<u64> = (0..50).map(|_| jitter(1000)).collect();
        let distinct: std::collections::HashSet<_> = values.iter().collect();
        assert!(distinct.len() > 20, "jitter is not spreading: {distinct:?}");
        assert!(values.iter().all(|v| *v < 1000));
    }
}
