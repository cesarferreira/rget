//! Download orchestration.
//!
//! The engine is a sequence of decisions, each of which can refuse to proceed:
//! probe → destination → resume validation → reconcile → plan → transfer →
//! barrier → verify. It owns no rendering and no argument parsing; it takes a
//! [`DownloadRequest`] and publishes [`Event`]s.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};
use url::Url;

use crate::error::TransferError;
use crate::file::DestFile;
use crate::http::{self, HttpConfig, RemoteInfo};
use crate::integrity::{self, Checksum};
use crate::limit::RateLimiter;
use crate::mirror::{self, Admission, Source, SourceSet};
use crate::naming;
use crate::progress::{Event, Reporter};
use crate::resume::{self, Identity, Validation};
use crate::retry::RetryPolicy;
use crate::scheduler::{self, Scheduler};
use crate::shutdown::Cancel;
use crate::storage::{
    DownloadRecord, ProgressUpdate, RangeRecord, RangeState, Status, Store, mint_cookie, now,
};
use crate::worker::{self, WorkerCtx, WorkerOutcome};

/// How often the committer runs its durability barrier. This is the maximum
/// amount of work a crash can cost us.
const COMMIT_INTERVAL: Duration = Duration::from_millis(500);

/// Benchmark-only override for [`COMMIT_INTERVAL`], in milliseconds. Lets a
/// measurement price the durability barrier without a rebuild; unset or
/// unparseable means the default.
fn commit_interval() -> Duration {
    match std::env::var("RGET_BENCH_COMMIT_INTERVAL_MS") {
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(ms) => Duration::from_millis(ms),
            Err(_) => COMMIT_INTERVAL,
        },
        Err(_) => COMMIT_INTERVAL,
    }
}
/// How long we wait for workers to notice cancellation before committing
/// anyway. PRD §26: do not wait indefinitely.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct DownloadRequest {
    /// First entry is the primary; the rest are mirrors.
    pub urls: Vec<Url>,
    pub output: Option<String>,
    pub dir: Option<String>,
    pub connections: usize,
    pub checksum: Option<Checksum>,
    pub limit: Option<u64>,
    pub http: HttpConfig,
    pub retries: u32,
    pub overwrite: bool,
    /// Discard existing progress and start over.
    pub restart: bool,
    pub preallocate: bool,
}

impl DownloadRequest {
    pub fn primary(&self) -> &Url {
        &self.urls[0]
    }
}

#[derive(Debug, Clone)]
pub struct DownloadReport {
    pub id: String,
    pub path: PathBuf,
    pub filename: String,
    pub downloaded: u64,
    pub total: Option<u64>,
    pub elapsed: Duration,
    /// `None` when no checksum was requested.
    pub verified: Option<bool>,
    /// True when we stopped early because of Ctrl+C rather than finishing.
    pub paused: bool,
}

pub async fn download(
    store: Arc<Store>,
    req: DownloadRequest,
    reporter: Reporter,
    cancel: Cancel,
) -> Result<DownloadReport> {
    let started = Instant::now();
    let client = http::build_client(&req.http)?;

    // -- 1. inspect the remote ------------------------------------------
    let primary = req.primary().clone();
    let policy = RetryPolicy {
        max_attempts: req.retries.max(1),
        ..RetryPolicy::default()
    };
    // Ask for the whole file when one connection is going to transfer it all,
    // and for just the first range when we intend to fan out -- an open-ended
    // primed body in a parallel download streams bytes no worker will ever read.
    let prime = (req.connections > 1).then_some(scheduler::MIN_CHUNK);
    let primed = probe_with_retry(&client, &primary, prime, &policy, &reporter, &cancel)
        .await
        .with_context(|| format!("cannot reach {}", http::redact(&primary)))?;
    let info = primed.info.clone();
    let primed_len = primed.body_len;
    // Held until the plan exists, because only then do we know whether anything
    // still needs byte 0. On a resume that is already past byte 0 this gets
    // dropped, which costs one aborted response and saves nothing -- fresh
    // downloads are the case worth optimising.
    let mut primed_body = primed.body;
    debug!(
        url = %http::redact(&info.final_url),
        size = ?info.size,
        ranges = info.accept_ranges,
        etag = ?info.etag,
        "probed remote"
    );
    if let Some(encoding) = &info.content_encoding {
        reporter.warn(format!(
            "server is sending `Content-Encoding: {encoding}`; the saved file will be in that \
             encoded form"
        ));
    }

    // -- 2. decide where it goes ----------------------------------------
    let dest = naming::choose(
        req.output.as_deref(),
        req.dir.as_deref(),
        info.content_disposition.as_deref(),
        &info.final_url,
    )?;
    let parent = dest
        .path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    naming::assert_within(&parent, &dest.path)?;
    debug!(path = %dest.path.display(), source = ?dest.source, "chose destination");

    // -- 3. find or create the record -----------------------------------
    let existing = store.find_for(primary.as_str(), &dest.path)?;
    let mut record = match existing {
        Some(rec) => rec,
        None => {
            guard_existing_file(&store, &dest.path, &req)?;
            let id = store.mint_id(primary.as_str())?;
            let rec = DownloadRecord {
                id,
                original_url: primary.to_string(),
                resolved_url: Some(info.final_url.to_string()),
                mirrors: req.urls.iter().skip(1).map(|u| u.to_string()).collect(),
                destination: dest.path.to_string_lossy().to_string(),
                filename: dest.filename.clone(),
                total_size: info.size,
                etag: info.etag.clone(),
                last_modified: info.last_modified.clone(),
                content_type: info.content_type.clone(),
                accept_ranges: info.accept_ranges,
                expected_checksum: req.checksum.as_ref().map(|c| c.expected.clone()),
                checksum_algorithm: req.checksum.as_ref().map(|c| c.algorithm.to_string()),
                file_cookie: mint_cookie(),
                file_dev: None,
                file_ino: None,
                durable_bytes: 0,
                status: Status::Pending,
                error: None,
                created_at: now(),
                updated_at: now(),
                completed_at: None,
            };
            store.insert(&rec)?;
            rec
        }
    };

    if req.restart {
        reporter.info("--restart: discarding previous progress");
        store.reset(&record.id)?;
        record = store
            .get(&record.id)?
            .ok_or_else(|| anyhow!("download record vanished"))?;
        if dest.path.exists() {
            std::fs::remove_file(&dest.path)
                .with_context(|| format!("cannot remove {}", dest.path.display()))?;
        }
    }

    // -- 4. open the file, then check it is *our* file -------------------
    //
    // Look at what is on disk *before* opening: opening creates the file, which
    // would make "the user deleted it" indistinguishable from "something
    // replaced it".
    let bytes_on_disk = std::fs::metadata(&dest.path).ok().map(|m| m.len());
    let file = Arc::new(DestFile::open(&dest.path)?);
    let file_len = file.size()?;
    let mut had_progress = record.durable_bytes > 0;

    if had_progress && bytes_on_disk.unwrap_or(0) == 0 {
        // The file is gone, or empty. Either way there are no bytes to protect,
        // so deleting it is a perfectly good way of saying "start over" — and
        // refusing would leave the user stuck with a download they cannot run
        // and cannot obviously fix.
        reporter.warn(format!(
            "{} {}, so there is nothing to resume; starting over",
            dest.path.display(),
            if bytes_on_disk.is_none() {
                "was removed"
            } else {
                "is empty"
            }
        ));
        store.reset(&record.id)?;
        record = store
            .get(&record.id)?
            .ok_or_else(|| anyhow!("download record vanished"))?;
        had_progress = false;
    }

    if had_progress {
        match resume::check_identity(&record, &file) {
            Identity::Same => {}
            // The file exists, holds data, and is not the one we were writing
            // to. Its contents are somebody's, so we do not touch them.
            Identity::Replaced => bail!(
                "{} holds {} but is not the file this download was writing to.\n\
                 It was replaced or recreated by something else since the last run.\n\
                 Use --restart to download it again, --output <different-name> to keep both,\n\
                 or delete the file if you do not need it.",
                dest.path.display(),
                crate::fmt::bytes(bytes_on_disk.unwrap_or(0)),
            ),
            Identity::Unrecorded => {
                reporter.warn("no recorded file identity for this download; relying on size checks")
            }
        }
    }

    // -- 5. is the remote still the same object? ------------------------
    if had_progress {
        match resume::validate(&record, &info) {
            Validation::Unchanged => reporter.info("remote file unchanged"),
            Validation::Unverifiable(reason) => reporter.warn(format!(
                "cannot confirm the remote file is unchanged: {reason}"
            )),
            Validation::Changed {
                reason,
                previous,
                current,
            } => {
                store.set_status(&record.id, Status::Failed, Some(&reason))?;
                bail!(
                    "Remote file changed since the previous download.\n\
                     Previous:\n  ETag: {}\n  Size: {}\n\
                     Current:\n  ETag: {}\n  Size: {}\n\
                     Refusing to resume because this could corrupt the file.\n\
                     Re-download from scratch with --restart.",
                    previous.etag.as_deref().unwrap_or("(none)"),
                    previous
                        .size
                        .map(crate::fmt::bytes)
                        .unwrap_or_else(|| "(unknown)".into()),
                    current.etag.as_deref().unwrap_or("(none)"),
                    current
                        .size
                        .map(crate::fmt::bytes)
                        .unwrap_or_else(|| "(unknown)".into()),
                );
            }
        }
    }

    // Refresh what we know, including the file's identity now that it exists.
    let identity = file.identity().ok();
    record.resolved_url = Some(info.final_url.to_string());
    record.total_size = info.size;
    record.etag = info.etag.clone();
    record.last_modified = info.last_modified.clone();
    record.content_type = info.content_type.clone();
    record.accept_ranges = info.accept_ranges;
    record.mirrors = req.urls.iter().skip(1).map(|u| u.to_string()).collect();
    record.expected_checksum = req.checksum.as_ref().map(|c| c.expected.clone());
    record.checksum_algorithm = req.checksum.as_ref().map(|c| c.algorithm.to_string());
    record.file_dev = identity.map(|i| i.dev);
    record.file_ino = identity.map(|i| i.ino);
    store.update_remote_metadata(&record)?;

    // -- 6. reconcile and plan -----------------------------------------
    let parallel = info.supports_parallel() && req.connections > 1;
    let ranges = build_plan(
        &store,
        &record,
        &info,
        &req,
        file_len,
        PlanShape {
            parallel,
            primed_len,
        },
        &reporter,
    )?;
    let resumed_bytes: u64 = ranges
        .iter()
        .map(|r| {
            if r.state == RangeState::Complete {
                r.size()
            } else {
                r.bytes_written
            }
        })
        .sum();

    if req.preallocate && parallel {
        if let Some(total) = info.size {
            file.preallocate(total).with_context(|| {
                format!(
                    "cannot reserve {} for the download",
                    crate::fmt::bytes(total)
                )
            })?;
        }
    }

    // -- 7. mirrors -----------------------------------------------------
    let sources = Arc::new(
        resolve_sources(&client, &req, &info, &reporter)
            .await
            .context("no usable source for this download")?,
    );

    // -- 8. transfer ----------------------------------------------------
    let scheduler = Arc::new(Scheduler::from_ranges(&ranges));
    let (done, total_ranges) = scheduler.counts();
    reporter.stats.set_ranges_complete(done);
    reporter.stats.set_ranges_total(total_ranges);
    reporter.stats.set_downloaded(resumed_bytes);
    reporter.stats.set_durable(resumed_bytes);

    store.set_status(&record.id, Status::Downloading, None)?;
    reporter.emit(Event::DownloadStarted {
        id: record.id.clone(),
        filename: record.filename.clone(),
        url: http::redact(&info.final_url),
        total_size: info.size,
        resumed_bytes,
        connections: req.connections,
        parallel,
    });

    // Only hand the primed body to the workers if byte 0 is still outstanding.
    // A resume whose first range already has bytes on disk cannot splice this
    // body in, so drop it and let the workers request what they actually need.
    let needs_byte_zero = ranges
        .iter()
        .any(|r| r.start == 0 && r.state != RangeState::Complete && r.bytes_written == 0);
    if !needs_byte_zero {
        primed_body = None;
    }
    let primed_body = primed_body.map(|response| worker::PrimedBody {
        url: info.final_url.clone(),
        response,
    });
    debug!(primed = primed_body.is_some(), "priming probe body");

    let ctx = Arc::new(WorkerCtx {
        client: client.clone(),
        sources: sources.clone(),
        file: file.clone(),
        scheduler: scheduler.clone(),
        reporter: reporter.clone(),
        retry: policy,
        limiter: req.limit.map(|bps| Arc::new(RateLimiter::new(bps))),
        read_timeout: req.http.timeout,
        expected_total: info.size,
        ranges_supported: info.accept_ranges,
        discovered_size: Arc::new(AtomicU64::new(0)),
        cancel: cancel.clone(),
        primed: std::sync::Mutex::new(primed_body),
    });

    let committer = tokio::spawn(commit_loop(
        store.clone(),
        record.id.clone(),
        file.clone(),
        scheduler.clone(),
        reporter.clone(),
        cancel.clone(),
        ranges.len(),
    ));

    let worker_count = if parallel { req.connections } else { 1 };
    let mut workers = JoinSet::new();
    for id in 0..worker_count {
        workers.spawn(worker::run(ctx.clone(), id));
    }

    let mut fatal: Option<TransferError> = None;
    while let Some(joined) = workers.join_next().await {
        match joined {
            Ok(WorkerOutcome::Finished) => {}
            Ok(WorkerOutcome::Fatal(err)) => {
                if fatal.is_none() {
                    fatal = Some(err);
                }
                // One range is unrecoverable, so the download cannot finish.
                // Stop the others rather than let them keep burning bandwidth.
                cancel.cancel();
            }
            Err(join_err) => {
                if fatal.is_none() {
                    fatal = Some(TransferError::Io(format!("worker panicked: {join_err}")));
                }
                cancel.cancel();
            }
        }
    }

    // -- 9. final barrier ----------------------------------------------
    committer.abort();
    let _ = committer.await;
    let durable = checkpoint(
        &store,
        &record.id,
        &file,
        &scheduler,
        &reporter,
        ranges.len(),
    )
    .await?;
    debug!(durable, "final checkpoint written");

    let cancelled = cancel.is_cancelled() && fatal.is_none();
    let downloaded = reporter.stats.downloaded();

    if let Some(err) = fatal {
        store.set_status(&record.id, Status::Failed, Some(&err.to_string()))?;
        reporter.emit(Event::DownloadFailed {
            error: err.to_string(),
        });
        return Err(anyhow!(err));
    }

    if cancelled {
        store.set_status(&record.id, Status::Paused, None)?;
        reporter.emit(Event::DownloadPaused {
            downloaded: durable,
            total_size: info.size,
        });
        return Ok(DownloadReport {
            id: record.id,
            path: dest.path,
            filename: record.filename,
            downloaded: durable,
            total: info.size,
            elapsed: started.elapsed(),
            verified: None,
            paused: true,
        });
    }

    if !scheduler.is_finished() {
        store.set_status(&record.id, Status::Paused, None)?;
        bail!("download stopped with ranges outstanding; run the same command to continue");
    }

    // -- 10. finalise the file -----------------------------------------
    let final_size = match info.size {
        Some(total) => total,
        None => {
            // Unknown length: the transfer itself told us how big it was.
            let discovered = ctx.discovered_size.load(Ordering::Acquire);
            if discovered > 0 {
                discovered
            } else {
                scheduler.written_bytes()
            }
        }
    };
    // Trim preallocation slack if the resource turned out shorter than planned.
    if file.size()? > final_size {
        file.truncate(final_size)?;
    }
    file.sync_data()?;

    // -- 11. verify -----------------------------------------------------
    let mut verified = None;
    if let Some(checksum) = req.checksum.clone() {
        store.set_status(&record.id, Status::Verifying, None)?;
        let outcome = integrity::verify(&dest.path, checksum, reporter.clone(), cancel.clone())
            .await
            .context("verification failed to run")?;
        verified = Some(outcome.ok());
        if !outcome.ok() {
            let msg = match &outcome {
                integrity::Outcome::Mismatch { expected, actual } => {
                    format!("checksum mismatch: expected {expected}, got {actual}")
                }
                integrity::Outcome::Match { .. } => unreachable!(),
            };
            // PRD Invariant 5: never report a mismatch as success.
            store.set_status(&record.id, Status::Failed, Some(&msg))?;
            reporter.emit(Event::DownloadFailed { error: msg.clone() });
            bail!("{msg}");
        }
    }

    store.set_status(&record.id, Status::Complete, None)?;
    let elapsed = started.elapsed();
    let this_run = downloaded.saturating_sub(resumed_bytes);
    reporter.emit(Event::DownloadCompleted {
        downloaded: final_size,
        elapsed_ms: elapsed.as_millis() as u64,
        average_bps: if elapsed.as_secs_f64() > 0.0 {
            (this_run as f64 / elapsed.as_secs_f64()) as u64
        } else {
            0
        },
    });
    info!(id = %record.id, path = %dest.path.display(), "download complete");

    Ok(DownloadReport {
        id: record.id,
        path: dest.path,
        filename: record.filename,
        downloaded: final_size,
        total: Some(final_size),
        elapsed,
        verified,
        paused: false,
    })
}

/// Probe, retrying transient failures. A 503 or a dropped connection on the
/// very first request is exactly as transient as one halfway through, and
/// failing the whole command over it would be the wrong call (PRD §14).
async fn probe_with_retry(
    client: &reqwest::Client,
    url: &Url,
    prime: Option<u64>,
    policy: &RetryPolicy,
    reporter: &Reporter,
    cancel: &Cancel,
) -> Result<http::Primed, TransferError> {
    let mut attempts = 0u32;
    loop {
        if cancel.is_cancelled() {
            return Err(TransferError::Cancelled);
        }
        match http::probe_priming(client, url, prime).await {
            Ok(primed) => return Ok(primed),
            Err(err) => {
                attempts += 1;
                match policy.decide(&err, attempts) {
                    crate::retry::Decision::Retry { delay, attempt } => {
                        reporter.stats.record_retry();
                        reporter.emit(Event::RetryScheduled {
                            index: None,
                            attempt,
                            delay_ms: delay.as_millis() as u64,
                            reason: err.to_string(),
                        });
                        tokio::select! {
                            _ = tokio::time::sleep(delay) => {}
                            _ = cancel.cancelled() => return Err(TransferError::Cancelled),
                        }
                    }
                    crate::retry::Decision::GiveUp => return Err(err),
                }
            }
        }
    }
}

/// PRD §22: never clobber a file we know nothing about.
fn guard_existing_file(store: &Store, path: &Path, req: &DownloadRequest) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    if req.overwrite || req.restart {
        return Ok(());
    }
    // A record for this destination under a different URL is still someone
    // else's download; refusing protects both.
    let owner = store.find_by_destination(path)?;
    let hint = match owner {
        Some(rec) if rec.status.is_resumable() => format!(
            "\n{} is the destination of download {} ({}).",
            path.display(),
            rec.id,
            rec.original_url
        ),
        _ => String::new(),
    };
    bail!(
        "{} already exists.{hint}\nUse:\n  --overwrite\n  --output <different-name>",
        path.display()
    );
}

/// How the fresh plan should be shaped: whether ranges may be split across
/// connections at all, and how many leading bytes the priming probe already has
/// in flight for the first range to pin itself to.
#[derive(Debug, Clone, Copy)]
struct PlanShape {
    parallel: bool,
    primed_len: u64,
}

/// Load, reconcile and if necessary rebuild the range plan.
fn build_plan(
    store: &Store,
    record: &DownloadRecord,
    info: &RemoteInfo,
    req: &DownloadRequest,
    file_len: u64,
    shape: PlanShape,
    reporter: &Reporter,
) -> Result<Vec<RangeRecord>> {
    let persisted = store.load_ranges(&record.id)?;

    let fresh_plan = || -> Vec<RangeRecord> {
        if shape.parallel {
            // Pin the first range to the bytes the probe already has in flight.
            scheduler::plan_primed(info.size.unwrap_or(0), req.connections, shape.primed_len)
        } else {
            scheduler::plan_sequential(info.size)
        }
    };

    if persisted.is_empty() {
        // Persist immediately: an unpersisted plan means a crash a second later
        // would find no ranges and start the whole file again.
        let plan = fresh_plan();
        store.replace_ranges(&record.id, &plan)?;
        return Ok(plan);
    }

    // Without range support we cannot restart mid-file, so any partial progress
    // has to be thrown away rather than resumed into.
    if !info.accept_ranges {
        let progress: u64 = persisted.iter().map(|r| r.bytes_written).sum();
        if progress > 0 {
            reporter.warn(format!(
                "server does not support resuming, so the {} already downloaded must be fetched \
                 again",
                crate::fmt::bytes(progress)
            ));
        }
        let plan = fresh_plan();
        store.replace_ranges(&record.id, &plan)?;
        return Ok(plan);
    }

    let reconciled = resume::reconcile(&persisted, file_len, info.size);
    for note in &reconciled.notes {
        reporter.warn(note.clone());
    }
    if reconciled.discarded_bytes > 0 {
        reporter.warn(format!(
            "re-downloading {} that could not be trusted",
            crate::fmt::bytes(reconciled.discarded_bytes)
        ));
    }

    // If the plan no longer describes the resource, rebuild it rather than
    // patch it — a patched plan is how gaps get created.
    let intact = match info.size {
        Some(total) => resume::plan_is_intact(&reconciled.ranges, total),
        None => reconciled.ranges.len() == 1,
    };
    if !intact {
        reporter.warn("previous range plan no longer matches the remote file; replanning");
        let plan = fresh_plan();
        store.replace_ranges(&record.id, &plan)?;
        return Ok(plan);
    }

    store.replace_ranges(&record.id, &reconciled.ranges)?;
    Ok(reconciled.ranges)
}

/// Probe every mirror and admit only those provably serving the same bytes.
async fn resolve_sources(
    client: &reqwest::Client,
    req: &DownloadRequest,
    primary_info: &RemoteInfo,
    reporter: &Reporter,
) -> Result<SourceSet> {
    let mut sources = vec![Source::new(
        req.primary().clone(),
        Admission::Primary,
        primary_info.validator(),
    )];
    let has_checksum = req.checksum.is_some();

    for url in req.urls.iter().skip(1) {
        match http::probe(client, url).await {
            Ok(info) => {
                let admission = mirror::classify(primary_info, &info, has_checksum);
                match &admission {
                    Admission::Rejected(reason) => {
                        reporter.warn(format!("ignoring mirror {}: {reason}", http::redact(url)))
                    }
                    Admission::ChecksumGuarded => reporter.info(format!(
                        "using mirror {} on the strength of the supplied checksum",
                        http::redact(url)
                    )),
                    _ => reporter.info(format!("mirror {} verified", http::redact(url))),
                }
                let validator = info.validator();
                sources.push(Source::new(url.clone(), admission, validator));
            }
            Err(err) => reporter.warn(format!(
                "ignoring unreachable mirror {}: {err}",
                http::redact(url)
            )),
        }
    }

    let set = SourceSet::new(sources);
    if set.is_empty() {
        bail!("every source was rejected");
    }
    Ok(set)
}

/// Periodic durability barrier. See `docs/CRASH_CONSISTENCY.md`.
async fn commit_loop(
    store: Arc<Store>,
    id: String,
    file: Arc<DestFile>,
    scheduler: Arc<Scheduler>,
    reporter: Reporter,
    cancel: Cancel,
    initial_range_count: usize,
) {
    let mut known_ranges = initial_range_count;
    let interval = commit_interval();
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = cancel.cancelled() => break,
        }
        match checkpoint(&store, &id, &file, &scheduler, &reporter, known_ranges).await {
            Ok(_) => known_ranges = scheduler.snapshot().len(),
            Err(err) => {
                // A failing store is serious but not a reason to corrupt the
                // file; keep transferring and try again next tick.
                warn!(%err, "checkpoint failed");
                reporter.warn(format!("could not save progress: {err}"));
            }
        }
    }
}

/// Snapshot → fsync → commit. The ordering is the whole point: nothing is
/// recorded as durable until the bytes it describes are on stable storage.
async fn checkpoint(
    store: &Arc<Store>,
    id: &str,
    file: &Arc<DestFile>,
    scheduler: &Arc<Scheduler>,
    reporter: &Reporter,
    known_ranges: usize,
) -> Result<u64> {
    // 1. Snapshot what workers have handed to the kernel.
    let snapshot = scheduler.snapshot();

    // 2. Barrier. On a blocking thread: fsync can take seconds and must not
    //    stall the runtime that is servicing the sockets.
    let f = file.clone();
    tokio::task::spawn_blocking(move || f.sync_data())
        .await
        .context("fsync task panicked")?
        .context("fsync of the destination file failed")?;

    // 3. Claim, in one transaction, only what the snapshot covered.
    let store = store.clone();
    let id = id.to_string();
    let structural_change = snapshot.len() != known_ranges;
    let durable = tokio::task::spawn_blocking(move || -> Result<u64> {
        if structural_change {
            // Ranges were split; rewrite the whole plan so the partition on
            // disk stays gapless.
            store.replace_ranges(&id, &snapshot)?;
            Ok(snapshot.iter().map(|r| r.bytes_written).sum())
        } else {
            let updates: Vec<ProgressUpdate> = snapshot
                .iter()
                .map(|r| ProgressUpdate {
                    idx: r.idx,
                    bytes_written: r.bytes_written.min(r.size()),
                    state: match r.state {
                        RangeState::Complete => RangeState::Complete,
                        _ if r.bytes_written > 0 => RangeState::Downloading,
                        other => other,
                    },
                })
                .collect();
            store.commit_progress(&id, &updates)
        }
    })
    .await
    .context("checkpoint task panicked")??;

    reporter.stats.set_durable(durable);
    reporter.emit(Event::Checkpointed {
        durable_bytes: durable,
    });
    Ok(durable)
}

/// Wait for cancellation, then give workers a bounded grace period. Used by the
/// CLI so Ctrl+C cannot hang the process (PRD §26).
pub async fn grace_period(cancel: &Cancel) {
    cancel.cancelled().await;
    tokio::time::sleep(SHUTDOWN_GRACE).await;
}
