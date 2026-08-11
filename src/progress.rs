//! Progress telemetry (PRD §17, §18).
//!
//! The transfer engine never renders anything. It emits [`Event`]s and bumps
//! atomic counters in [`Stats`]; a consumer (CLI renderer, JSON writer, future
//! TUI or daemon) subscribes and decides what a human should see.
//!
//! Byte-level progress deliberately does *not* travel through the channel one
//! message per socket read — at multi-gigabit that is tens of thousands of
//! messages a second. Workers bump a relaxed atomic on every write and emit a
//! coalesced [`Event::BytesWritten`] at most every 100 ms. Consumers that want
//! smooth output sample [`Stats`] on their own clock.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    DownloadStarted {
        id: String,
        filename: String,
        url: String,
        total_size: Option<u64>,
        resumed_bytes: u64,
        connections: usize,
        parallel: bool,
    },
    RangeStarted {
        index: u64,
        start: u64,
        end: u64,
    },
    BytesWritten {
        index: u64,
        bytes: u64,
    },
    RangeCompleted {
        index: u64,
    },
    RangeSplit {
        index: u64,
        new_index: u64,
        at: u64,
    },
    RetryScheduled {
        index: Option<u64>,
        attempt: u32,
        delay_ms: u64,
        reason: String,
    },
    /// A committer barrier completed: this many bytes are now durable.
    Checkpointed {
        durable_bytes: u64,
    },
    DownloadPaused {
        downloaded: u64,
        total_size: Option<u64>,
    },
    VerificationStarted {
        algorithm: String,
        total_size: u64,
    },
    VerificationProgress {
        bytes: u64,
        total_size: u64,
    },
    VerificationCompleted {
        algorithm: String,
        ok: bool,
        expected: Option<String>,
        actual: String,
    },
    DownloadCompleted {
        downloaded: u64,
        elapsed_ms: u64,
        average_bps: u64,
    },
    DownloadFailed {
        error: String,
    },
    /// Human-facing note that is not a lifecycle transition.
    Note {
        level: NoteLevel,
        message: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NoteLevel {
    Info,
    Warn,
    Error,
}

/// Hot-path counters. Cheap to read, cheap to bump, safe to share.
#[derive(Debug, Default)]
pub struct Stats {
    /// Bytes written to the file this run, plus whatever we resumed with.
    downloaded: AtomicU64,
    /// Bytes confirmed durable by a committer barrier.
    durable: AtomicU64,
    active_connections: AtomicUsize,
    ranges_total: AtomicUsize,
    ranges_complete: AtomicUsize,
    retries: AtomicU64,
}

impl Stats {
    pub fn add_downloaded(&self, n: u64) {
        self.downloaded.fetch_add(n, Ordering::Relaxed);
    }
    pub fn set_downloaded(&self, n: u64) {
        self.downloaded.store(n, Ordering::Relaxed);
    }
    pub fn downloaded(&self) -> u64 {
        self.downloaded.load(Ordering::Relaxed)
    }
    pub fn set_durable(&self, n: u64) {
        self.durable.store(n, Ordering::Relaxed);
    }
    pub fn durable(&self) -> u64 {
        self.durable.load(Ordering::Relaxed)
    }
    pub fn connection_opened(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }
    pub fn connection_closed(&self) {
        // Saturating: a double-close must not wrap to usize::MAX and make the
        // UI claim 18 quintillion connections.
        let _ = self
            .active_connections
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            });
    }
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }
    pub fn set_ranges_total(&self, n: usize) {
        self.ranges_total.store(n, Ordering::Relaxed);
    }
    pub fn set_ranges_complete(&self, n: usize) {
        self.ranges_complete.store(n, Ordering::Relaxed);
    }
    pub fn ranges(&self) -> (usize, usize) {
        (
            self.ranges_complete.load(Ordering::Relaxed),
            self.ranges_total.load(Ordering::Relaxed),
        )
    }
    pub fn record_retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }
    pub fn retries(&self) -> u64 {
        self.retries.load(Ordering::Relaxed)
    }
}

/// The engine's handle for publishing telemetry. Cloneable into every worker.
#[derive(Clone)]
pub struct Reporter {
    tx: Option<mpsc::UnboundedSender<Event>>,
    pub stats: Arc<Stats>,
}

impl Reporter {
    pub fn new() -> (Self, mpsc::UnboundedReceiver<Event>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self {
                tx: Some(tx),
                stats: Arc::new(Stats::default()),
            },
            rx,
        )
    }

    /// A reporter that records counters but publishes nothing — used in tests
    /// and by code paths with no consumer attached.
    pub fn silent() -> Self {
        Self {
            tx: None,
            stats: Arc::new(Stats::default()),
        }
    }

    pub fn emit(&self, event: Event) {
        if let Some(tx) = &self.tx {
            // A closed consumer is not an error: the download outlives the UI.
            let _ = tx.send(event);
        }
    }

    pub fn note(&self, level: NoteLevel, message: impl Into<String>) {
        self.emit(Event::Note {
            level,
            message: message.into(),
        });
    }

    pub fn info(&self, message: impl Into<String>) {
        self.note(NoteLevel::Info, message);
    }

    pub fn warn(&self, message: impl Into<String>) {
        self.note(NoteLevel::Warn, message);
    }
}

/// Rolling-window throughput. PRD §17: `downloaded / runtime` is a bad speed
/// readout, because it cannot fall when the network stalls.
pub struct SpeedMeter {
    window: Duration,
    samples: VecDeque<(Instant, u64)>,
    /// Exponentially weighted average, used for ETA so the estimate does not
    /// jump around with every sample.
    smoothed_bps: f64,
    started: Instant,
    start_bytes: u64,
}

impl SpeedMeter {
    pub fn new(window: Duration, start_bytes: u64) -> Self {
        let now = Instant::now();
        let mut samples = VecDeque::with_capacity(64);
        samples.push_back((now, start_bytes));
        Self {
            window,
            samples,
            smoothed_bps: 0.0,
            started: now,
            start_bytes,
        }
    }

    pub fn record(&mut self, total_downloaded: u64) {
        let now = Instant::now();
        let instant_bps = self.instant_from(now, total_downloaded);
        self.samples.push_back((now, total_downloaded));
        while let Some(&(t, _)) = self.samples.front() {
            if now.duration_since(t) > self.window && self.samples.len() > 2 {
                self.samples.pop_front();
            } else {
                break;
            }
        }
        // ~3s time constant at a 10 Hz sample rate.
        const ALPHA: f64 = 0.15;
        self.smoothed_bps = if self.smoothed_bps == 0.0 {
            instant_bps
        } else {
            ALPHA * instant_bps + (1.0 - ALPHA) * self.smoothed_bps
        };
    }

    fn instant_from(&self, now: Instant, total: u64) -> f64 {
        let Some(&(t0, b0)) = self.samples.front() else {
            return 0.0;
        };
        let dt = now.duration_since(t0).as_secs_f64();
        if dt <= 0.0 {
            return 0.0;
        }
        (total.saturating_sub(b0)) as f64 / dt
    }

    /// Throughput over the rolling window.
    pub fn rolling_bps(&self) -> f64 {
        let (&(t0, b0), &(t1, b1)) = match (self.samples.front(), self.samples.back()) {
            (Some(a), Some(b)) => (a, b),
            _ => return 0.0,
        };
        let dt = t1.duration_since(t0).as_secs_f64();
        if dt <= 0.0 {
            return 0.0;
        }
        (b1.saturating_sub(b0)) as f64 / dt
    }

    /// Smoothed throughput — the right input for an ETA.
    pub fn smoothed_bps(&self) -> f64 {
        self.smoothed_bps
    }

    /// Whole-run average, for the completion summary only.
    pub fn average_bps(&self, total_downloaded: u64) -> f64 {
        let dt = self.started.elapsed().as_secs_f64();
        if dt <= 0.0 {
            return 0.0;
        }
        (total_downloaded.saturating_sub(self.start_bytes)) as f64 / dt
    }

    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }
}

/// What a renderer needs for one frame.
#[derive(Debug, Clone, Serialize)]
pub struct Snapshot {
    pub filename: String,
    pub downloaded: u64,
    pub total_size: Option<u64>,
    pub bps: f64,
    pub smoothed_bps: f64,
    pub eta_secs: Option<u64>,
    pub elapsed_ms: u64,
    pub active_connections: usize,
    pub ranges_complete: usize,
    pub ranges_total: usize,
    pub retries: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_count_never_wraps() {
        let s = Stats::default();
        s.connection_closed();
        assert_eq!(s.active_connections(), 0);
        s.connection_opened();
        s.connection_opened();
        assert_eq!(s.active_connections(), 2);
        s.connection_closed();
        assert_eq!(s.active_connections(), 1);
    }

    #[test]
    fn silent_reporter_still_counts() {
        let r = Reporter::silent();
        r.emit(Event::RangeCompleted { index: 1 });
        r.stats.add_downloaded(10);
        assert_eq!(r.stats.downloaded(), 10);
    }

    #[test]
    fn reporter_survives_dropped_consumer() {
        let (r, rx) = Reporter::new();
        drop(rx);
        r.info("nobody is listening");
        assert_eq!(r.stats.downloaded(), 0);
    }

    #[test]
    fn rolling_speed_drops_when_transfer_stalls() {
        let mut m = SpeedMeter::new(Duration::from_millis(300), 0);
        // Simulate a burst then a stall by advancing real time.
        m.record(1_000_000);
        std::thread::sleep(Duration::from_millis(50));
        m.record(2_000_000);
        let fast = m.rolling_bps();
        assert!(fast > 0.0);

        // Stall: no new bytes for longer than the window.
        for _ in 0..8 {
            std::thread::sleep(Duration::from_millis(60));
            m.record(2_000_000);
        }
        assert!(
            m.rolling_bps() < fast / 2.0,
            "rolling speed should collapse when stalled: {} vs {}",
            m.rolling_bps(),
            fast
        );
    }

    #[test]
    fn average_excludes_resumed_bytes() {
        let m = SpeedMeter::new(Duration::from_secs(3), 5_000);
        // Only the 1_000 bytes downloaded this run count towards the average.
        let avg = m.average_bps(6_000);
        assert!(avg > 0.0);
        assert!(m.average_bps(5_000) == 0.0);
    }
}
