//! One worker transferring one range at a time.
//!
//! Memory discipline (PRD §30): a worker holds one body chunk at a time and
//! writes it straight through to the file. Nothing accumulates, so peak memory
//! is `connections × chunk`, independent of file size — a 500 GB download costs
//! the same as a 5 GB one.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use reqwest::Client;
use tracing::{debug, warn};

use crate::error::TransferError;
use crate::file::DestFile;
use crate::limit::RateLimiter;
use crate::mirror::SourceSet;
use crate::progress::{Event, Reporter};
use crate::retry::{Decision, RetryPolicy};
use crate::scheduler::{Lease, Scheduler};
use crate::shutdown::Cancel;
use crate::storage::OPEN_END;

/// Everything a worker needs, shared by `Arc` across all of them.
pub struct WorkerCtx {
    pub client: Client,
    pub sources: Arc<SourceSet>,
    pub file: Arc<DestFile>,
    pub scheduler: Arc<Scheduler>,
    pub reporter: Reporter,
    pub retry: RetryPolicy,
    pub limiter: Option<Arc<RateLimiter>>,
    /// Maximum silence between two body chunks before we call it a timeout.
    pub read_timeout: Duration,
    pub expected_total: Option<u64>,
    /// When false we never send a `Range` header at all — asking a server that
    /// ignores ranges for one guarantees a 200 with the whole body, which we
    /// would (correctly) reject as a protocol violation.
    pub ranges_supported: bool,
    /// Filled in when an open-ended (unknown length) transfer discovers the
    /// real size by reaching the end of the body.
    pub discovered_size: Arc<AtomicU64>,
    pub cancel: Cancel,
    /// The response the priming probe left open, whose body starts at byte 0.
    /// Whichever worker first picks up a lease sitting at byte 0 transfers it
    /// instead of opening a redundant connection. Taken at most once.
    pub primed: Mutex<Option<PrimedBody>>,
}

/// A live response body from [`crate::http::probe_priming`], tagged with the URL
/// it came from so it can never be spliced into a different source's range.
pub struct PrimedBody {
    pub url: url::Url,
    pub response: reqwest::Response,
}

impl WorkerCtx {
    /// Claim the primed body, but only for a request that it actually answers:
    /// the same URL, starting at the same byte.
    fn take_primed(&self, url: &url::Url, start: u64) -> Option<reqwest::Response> {
        if start != 0 {
            return None;
        }
        let mut slot = self.primed.lock().unwrap_or_else(|e| e.into_inner());
        match slot.as_ref() {
            Some(primed) if &primed.url == url => slot.take().map(|p| p.response),
            _ => None,
        }
    }
}

/// Outcome of a worker's whole run.
pub enum WorkerOutcome {
    /// No work left, or cancelled.
    Finished,
    /// A range failed permanently; the download cannot complete.
    Fatal(TransferError),
}

/// Pull ranges until there are none left, the download is cancelled, or a
/// non-recoverable error occurs.
pub async fn run(ctx: Arc<WorkerCtx>, worker_id: usize) -> WorkerOutcome {
    loop {
        if ctx.cancel.is_cancelled() {
            return WorkerOutcome::Finished;
        }

        let Some((lease, split)) = ctx.scheduler.acquire() else {
            if ctx.scheduler.is_finished() || ctx.scheduler.has_failure() {
                return WorkerOutcome::Finished;
            }
            // No pending range and nothing splittable: wait for another worker
            // to finish or make enough progress to be worth stealing from.
            tokio::select! {
                _ = ctx.scheduler.wait_for_change(Duration::from_millis(250)) => {}
                _ = ctx.cancel.cancelled() => return WorkerOutcome::Finished,
            }
            continue;
        };

        if let Some(split) = split {
            ctx.reporter.emit(Event::RangeSplit {
                index: split.shrunk.idx,
                new_index: split.added.idx,
                at: split.added.start,
            });
            debug!(
                worker = worker_id,
                victim = split.shrunk.idx,
                new_range = split.added.idx,
                at = split.added.start,
                "split a slow range"
            );
        }

        ctx.reporter.emit(Event::RangeStarted {
            index: lease.idx,
            start: lease.cursor(),
            end: lease.end(),
        });

        match transfer(&ctx, &lease, worker_id).await {
            Ok(()) => {
                ctx.scheduler.complete(lease.idx);
                ctx.reporter
                    .emit(Event::RangeCompleted { index: lease.idx });
                let (done, total) = ctx.scheduler.counts();
                ctx.reporter.stats.set_ranges_complete(done);
                ctx.reporter.stats.set_ranges_total(total);
            }
            Err(TransferError::Cancelled) => {
                ctx.scheduler.release(lease.idx);
                return WorkerOutcome::Finished;
            }
            Err(err) => {
                // Retries are exhausted by the time we get here.
                ctx.scheduler.fail(lease.idx);
                warn!(worker = worker_id, range = lease.idx, %err, "range failed permanently");
                return WorkerOutcome::Fatal(err);
            }
        }
    }
}

/// Transfer one lease to completion, retrying transient failures in place so a
/// blip costs us a reconnect rather than the range's progress (PRD §14).
async fn transfer(ctx: &WorkerCtx, lease: &Lease, worker_id: usize) -> Result<(), TransferError> {
    let mut attempts = 0u32;
    loop {
        if ctx.cancel.is_cancelled() {
            return Err(TransferError::Cancelled);
        }
        if lease.remaining() == 0 {
            return Ok(());
        }

        let (source_idx, source) = ctx.sources.pick();
        let url = source.url.clone();
        // The validator belongs to this source, not to the download.
        let validator = source.validator.clone();
        let before = lease.progress();
        match attempt(ctx, lease, &url, validator.as_deref()).await {
            Ok(()) => {
                ctx.sources.reward(source_idx);
                return Ok(());
            }
            Err(err) => {
                // A connection that delivered bytes before dying is flaky, not
                // broken. Charging it an attempt anyway would make us give up on
                // a link that drops every few MiB but is otherwise progressing.
                if lease.progress() > before {
                    attempts = 0;
                }
                attempts += 1;
                ctx.sources.penalise(source_idx);
                match ctx.retry.decide(&err, attempts) {
                    Decision::Retry { delay, attempt } => {
                        ctx.reporter.stats.record_retry();
                        ctx.reporter.emit(Event::RetryScheduled {
                            index: Some(lease.idx),
                            attempt,
                            delay_ms: delay.as_millis() as u64,
                            reason: err.to_string(),
                        });
                        debug!(
                            worker = worker_id,
                            range = lease.idx,
                            attempt,
                            ?delay,
                            %err,
                            "retrying range"
                        );
                        tokio::select! {
                            _ = tokio::time::sleep(delay) => {}
                            _ = ctx.cancel.cancelled() => return Err(TransferError::Cancelled),
                        }
                    }
                    Decision::GiveUp => return Err(err),
                }
            }
        }
    }
}

/// One HTTP request for the outstanding part of a lease.
async fn attempt(
    ctx: &WorkerCtx,
    lease: &Lease,
    url: &url::Url,
    validator: Option<&str>,
) -> Result<(), TransferError> {
    let start_cursor = lease.cursor();
    let open_ended = lease.is_open_ended();

    let (req_start, req_end) = if ctx.ranges_supported {
        (
            start_cursor,
            if open_ended { None } else { Some(lease.end()) },
        )
    } else {
        if start_cursor != 0 {
            return Err(TransferError::Protocol(
                "cannot resume from the middle: this server does not support range requests".into(),
            ));
        }
        (0, None)
    };

    // The priming probe already opened a body starting at byte 0. Using it here
    // is what makes a fresh download cost exactly as many requests as wget's.
    let mut resp = match ctx.take_primed(url, req_start) {
        Some(primed) => primed,
        None => {
            crate::http::get_range(
                &ctx.client,
                url,
                req_start,
                req_end,
                validator,
                ctx.expected_total,
            )
            .await?
        }
    };

    ctx.reporter.stats.connection_opened();
    // Guard so every early return below decrements the gauge exactly once.
    let _conn = ConnectionGuard(&ctx.reporter);

    let mut cursor = start_cursor;
    let mut pending_event_bytes = 0u64;
    let mut last_event = Instant::now();

    loop {
        if ctx.cancel.is_cancelled() {
            return Err(TransferError::Cancelled);
        }

        // Cancellation is checked *inside* the read wait, not just around it:
        // a stalled socket must not hold Ctrl+C hostage for a whole timeout.
        let chunk = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => return Err(TransferError::Cancelled),
            read = tokio::time::timeout(ctx.read_timeout, resp.chunk()) => match read {
                Err(_) => return Err(TransferError::Timeout(ctx.read_timeout)),
                Ok(Err(e)) => return Err(TransferError::from_reqwest(&e)),
                Ok(Ok(None)) => break,
                Ok(Ok(Some(chunk))) => chunk,
            },
        };
        if chunk.is_empty() {
            continue;
        }

        // Re-read the ceiling every chunk: the scheduler may have handed our
        // tail to an idle worker while this chunk was in flight.
        let end = lease.end();
        let writable = if open_ended && end >= OPEN_END {
            chunk.len() as u64
        } else {
            let room = end.saturating_sub(cursor).saturating_add(1);
            (chunk.len() as u64).min(room)
        };
        if writable == 0 {
            // Our range shrank to nothing; the rest belongs to someone else.
            break;
        }

        if let Some(limiter) = &ctx.limiter {
            limiter.acquire(writable).await;
        }

        let slice = &chunk[..writable as usize];
        ctx.file
            .write_at(slice, cursor)
            .map_err(|e| TransferError::Io(e.to_string()))?;

        cursor += writable;
        // Publishing progress is what makes the bytes eligible for the next
        // durability barrier. It never claims durability by itself.
        lease.publish_progress(cursor - lease.start);
        ctx.reporter.stats.add_downloaded(writable);

        pending_event_bytes += writable;
        if last_event.elapsed() >= Duration::from_millis(100) {
            ctx.reporter.emit(Event::BytesWritten {
                index: lease.idx,
                bytes: pending_event_bytes,
            });
            pending_event_bytes = 0;
            last_event = Instant::now();
        }

        if !open_ended && cursor > end {
            break;
        }
    }

    if pending_event_bytes > 0 {
        ctx.reporter.emit(Event::BytesWritten {
            index: lease.idx,
            bytes: pending_event_bytes,
        });
    }

    if open_ended && lease.end() >= OPEN_END {
        // The body ended, so now we know how big the resource actually was.
        ctx.discovered_size.store(cursor, Ordering::Release);
        ctx.scheduler
            .set_end(lease.idx, cursor.saturating_sub(1).max(lease.start));
        return Ok(());
    }

    if cursor > lease.end() {
        return Ok(());
    }

    // The body ended before the range did. Almost always a dropped connection
    // mid-transfer; retryable, and we keep every byte we did get.
    Err(TransferError::Network(format!(
        "connection closed with {} bytes of the range still missing",
        lease.end() + 1 - cursor
    )))
}

/// Keeps the active-connection gauge honest across every exit path.
struct ConnectionGuard<'a>(&'a Reporter);

impl Drop for ConnectionGuard<'_> {
    fn drop(&mut self) {
        self.0.stats.connection_closed();
    }
}
