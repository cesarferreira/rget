//! Global bandwidth limiting (PRD §23).
//!
//! One token bucket shared by every worker, so `--limit 20MiB/s` means the
//! download totals 20 MiB/s rather than each of eight connections getting it.
//! The bucket is a plain mutex + clock rather than a per-worker allowance,
//! which is what makes the limit global and what will let a future daemon
//! share one bucket across several downloads.

use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct RateLimiter {
    bytes_per_sec: f64,
    /// Bucket depth, i.e. the largest burst allowed. A quarter-second of budget
    /// is enough to absorb the granularity of many workers writing 64 KiB body
    /// chunks, while keeping the overshoot on a short download small — a full
    /// second of depth made `--limit 30MiB/s` average 35 MiB/s over 135 MiB,
    /// which reads as the flag being ignored.
    capacity: f64,
    state: Mutex<Bucket>,
}

struct Bucket {
    tokens: f64,
    last: Instant,
}

/// Never go below this, or a limit smaller than one body chunk would make every
/// single read block on a sleep.
const MIN_CAPACITY: f64 = 1.0 * 1024.0 * 1024.0;

impl RateLimiter {
    pub fn new(bytes_per_sec: u64) -> Self {
        let rate = bytes_per_sec as f64;
        // Start full: the alternative is a visible stall at the very start of
        // every download, which costs more in confusion than the burst does in
        // accuracy.
        let capacity = (rate / 4.0).max(MIN_CAPACITY).min(rate.max(1.0));
        Self {
            bytes_per_sec: rate,
            capacity,
            state: Mutex::new(Bucket {
                tokens: capacity,
                last: Instant::now(),
            }),
        }
    }

    /// Block until `n` bytes of budget are available. Called *before* issuing
    /// the read, so the limit shapes what we pull off the socket rather than
    /// what we have already buffered.
    pub async fn acquire(&self, n: u64) {
        let mut remaining = n as f64;
        while remaining > 0.0 {
            let wait = {
                let mut b = self.state.lock().expect("rate limiter poisoned");
                let now = Instant::now();
                let elapsed = now.duration_since(b.last).as_secs_f64();
                b.last = now;
                b.tokens = (b.tokens + elapsed * self.bytes_per_sec).min(self.capacity);

                if b.tokens >= remaining {
                    b.tokens -= remaining;
                    remaining = 0.0;
                    Duration::ZERO
                } else {
                    // Spend what is there and sleep for the rest. Partial
                    // spending keeps many waiters progressing fairly instead of
                    // starving whoever asks for the largest chunk.
                    remaining -= b.tokens.max(0.0);
                    b.tokens = 0.0;
                    Duration::from_secs_f64((remaining / self.bytes_per_sec).min(1.0))
                }
            };
            if wait > Duration::ZERO {
                tokio::time::sleep(wait).await;
            }
        }
    }
}

/// Parse `20MiB/s`, `20MB`, `1.5m`, `500k`, `1000` (bytes/s).
pub fn parse_rate(input: &str) -> Result<u64, String> {
    let s = input.trim().trim_end_matches("/s").trim_end_matches("/S");
    let s = s.trim();
    let split = s
        .find(|c: char| !c.is_ascii_digit() && c != '.' && c != ',')
        .unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let num: f64 = num
        .replace(',', "")
        .parse()
        .map_err(|_| format!("invalid rate `{input}`"))?;
    if num <= 0.0 {
        return Err(format!("rate must be positive, got `{input}`"));
    }
    let mult: f64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1.0,
        "k" | "kb" | "kib" => 1024.0,
        "m" | "mb" | "mib" => 1024.0 * 1024.0,
        "g" | "gb" | "gib" => 1024.0 * 1024.0 * 1024.0,
        other => return Err(format!("unknown rate unit `{other}` in `{input}`")),
    };
    Ok((num * mult) as u64)
}

/// Parse `30s`, `500ms`, `2m`, `1h`, or a bare number of seconds.
pub fn parse_duration(input: &str) -> Result<Duration, String> {
    let s = input.trim();
    let split = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let num: f64 = num
        .parse()
        .map_err(|_| format!("invalid duration `{input}`"))?;
    let secs = match unit.trim().to_ascii_lowercase().as_str() {
        "ms" => num / 1000.0,
        "" | "s" | "sec" | "secs" => num,
        "m" | "min" | "mins" => num * 60.0,
        "h" | "hr" | "hrs" => num * 3600.0,
        other => return Err(format!("unknown duration unit `{other}` in `{input}`")),
    };
    if secs <= 0.0 {
        return Err(format!("duration must be positive, got `{input}`"));
    }
    Ok(Duration::from_secs_f64(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_rates() {
        assert_eq!(parse_rate("1024"), Ok(1024));
        assert_eq!(parse_rate("1k"), Ok(1024));
        assert_eq!(parse_rate("20MiB/s"), Ok(20 * 1024 * 1024));
        assert_eq!(
            parse_rate("20 MB / s".replace(' ', "").as_str()),
            Ok(20 * 1024 * 1024)
        );
        assert_eq!(parse_rate("1.5m"), Ok(1_572_864));
        assert_eq!(parse_rate("2G"), Ok(2 * 1024 * 1024 * 1024));
        assert!(parse_rate("0").is_err());
        assert!(parse_rate("-5m").is_err());
        assert!(parse_rate("fast").is_err());
        assert!(parse_rate("10furlongs").is_err());
    }

    #[test]
    fn parses_durations() {
        assert_eq!(parse_duration("30"), Ok(Duration::from_secs(30)));
        assert_eq!(parse_duration("30s"), Ok(Duration::from_secs(30)));
        assert_eq!(parse_duration("500ms"), Ok(Duration::from_millis(500)));
        assert_eq!(parse_duration("2m"), Ok(Duration::from_secs(120)));
        assert_eq!(parse_duration("1h"), Ok(Duration::from_secs(3600)));
        assert!(parse_duration("0").is_err());
        assert!(parse_duration("soon").is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn limits_throughput_globally() {
        // A rate well above MIN_CAPACITY, so the burst allowance is rate/4.
        let rate = 8 * 1024 * 1024;
        let limiter = RateLimiter::new(rate);
        let start = tokio::time::Instant::now();

        // The initial burst is free, and it is a quarter-second's worth.
        limiter.acquire(rate / 4).await;
        assert_eq!(start.elapsed(), Duration::ZERO);

        // The next 2 seconds' worth must actually take ~2 seconds, no matter how
        // many callers ask or how they carve it up.
        limiter.acquire(rate).await;
        limiter.acquire(rate).await;
        assert!(
            start.elapsed() >= Duration::from_millis(1900),
            "elapsed {:?}",
            start.elapsed()
        );
    }

    #[test]
    fn burst_allowance_is_a_small_fraction_of_the_rate() {
        // A quarter second for a fast limit...
        let fast = RateLimiter::new(40 * 1024 * 1024);
        assert_eq!(fast.capacity, 10.0 * 1024.0 * 1024.0);

        // ...but never so small that a single body chunk cannot be granted, and
        // never larger than one second's worth for a very slow limit.
        let slow = RateLimiter::new(64 * 1024);
        assert_eq!(slow.capacity, 64.0 * 1024.0);
        let mid = RateLimiter::new(2 * 1024 * 1024);
        assert_eq!(mid.capacity, MIN_CAPACITY);
    }
}
