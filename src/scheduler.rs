//! Byte-range planning and worker assignment (PRD §7, §8).
//!
//! ## Ownership model
//!
//! A range is owned by at most one worker at a time. Ownership is the window
//! `[start + progress, end]`, where `end` lives in an `AtomicU64` the scheduler
//! may *lower* but never raise. A worker writes only below its own `end`.
//!
//! Dynamic splitting (PRD §8) exploits exactly that asymmetry: to hand an idle
//! worker part of a slow worker's range, the scheduler lowers the victim's
//! `end` and creates a new range starting above it. Because the split point is
//! chosen at least [`SPLIT_MARGIN`] bytes ahead of the victim's current write
//! position, and a single body chunk is far smaller than that margin, the
//! victim cannot have written into the new owner's territory even if it read
//! the old `end` a moment before the split. No overlap, no locking on the hot
//! path (PRD Invariant 2).

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::Notify;

use crate::storage::{OPEN_END, RangeRecord, RangeState};

/// Smallest range worth creating. Below this, per-request overhead and the
/// extra connection cost more than the parallelism gains.
pub const MIN_CHUNK: u64 = 4 << 20;
/// Largest range we plan up front. Bigger ranges mean coarser recovery after a
/// crash and less room for the scheduler to rebalance.
pub const MAX_CHUNK: u64 = 128 << 20;
/// How far ahead of a victim's write position a split point must sit. Must
/// exceed the largest single body chunk a worker can write (tens of KiB in
/// practice), with a wide safety factor.
pub const SPLIT_MARGIN: u64 = 4 << 20;
/// Do not bother splitting unless both halves are worth having.
pub const MIN_SPLIT_TAIL: u64 = 8 << 20;

/// Chunk size for a fresh plan: one range per connection.
///
/// This used to plan four ranges per connection so that a slow connection would
/// naturally pick up less work. That predates work stealing, and it costs a full
/// round trip per extra wave of ranges: a client with `n` connections and `4n`
/// ranges pays `4 × RTT` in request setup before the last byte can start moving,
/// on every download. Measured against a server with 200 ms of per-request
/// latency, that oversubscription made `-c8` 2.5× *slower* than a single
/// request, because latency, not bandwidth, was the limit.
///
/// Rebalancing is now [`Scheduler::acquire`]'s job: it splits a range that is
/// running behind and hands the tail to an idle worker, which adapts to real
/// connection speed rather than guessing at it up front. So plan the minimum
/// number of ranges and let stealing do the rest.
///
/// [`MAX_CHUNK`] still applies, so very large files get more ranges than there
/// are connections -- which is what keeps crash recovery granular.
pub fn chunk_size(total: u64, connections: usize) -> u64 {
    let target_chunks = (connections as u64).max(1);
    (total / target_chunks).clamp(MIN_CHUNK, MAX_CHUNK)
}

/// Partition `[0, total)` into contiguous inclusive ranges.
pub fn plan(total: u64, connections: usize) -> Vec<RangeRecord> {
    if total == 0 {
        return Vec::new();
    }
    let chunk = chunk_size(total, connections);
    let mut ranges = Vec::new();
    let mut start = 0u64;
    let mut idx = 0u64;
    while start < total {
        let end = (start + chunk - 1).min(total - 1);
        ranges.push(RangeRecord {
            idx,
            start,
            end,
            state: RangeState::Pending,
            bytes_written: 0,
        });
        start = end + 1;
        idx += 1;
    }
    ranges
}

/// A plan whose first range is exactly the bytes the priming probe already has
/// in flight, so none of them are wasted and none are fetched twice.
///
/// The probe opens a bounded range rather than the whole file precisely so this
/// is possible: an open-ended primed body would stream the entire file down a
/// connection whose worker stops at the first chunk boundary, throwing away
/// everything the server raced ahead. Pinning the boundary to what was actually
/// requested makes the waste zero.
///
/// The remainder is split across the *other* connections, so the total request
/// count stays at one per connection — the whole point of [`chunk_size`].
pub fn plan_primed(total: u64, connections: usize, primed: u64) -> Vec<RangeRecord> {
    // A primed body covering everything (or nothing) has no boundary to pin.
    if total == 0 || primed == 0 || primed >= total {
        return plan(total, connections);
    }

    let mut ranges = vec![RangeRecord {
        idx: 0,
        start: 0,
        end: primed - 1,
        state: RangeState::Pending,
        bytes_written: 0,
    }];

    let rest = total - primed;
    let chunk = chunk_size(rest, connections.saturating_sub(1).max(1));
    let mut start = primed;
    let mut idx = 1u64;
    while start < total {
        let end = (start + chunk - 1).min(total - 1);
        ranges.push(RangeRecord {
            idx,
            start,
            end,
            state: RangeState::Pending,
            bytes_written: 0,
        });
        start = end + 1;
        idx += 1;
    }
    ranges
}

/// The single-range plan used when the server has no usable `Range` support or
/// never told us the size (PRD §6: the fallback needs no user intervention).
pub fn plan_sequential(total: Option<u64>) -> Vec<RangeRecord> {
    vec![RangeRecord {
        idx: 0,
        start: 0,
        end: total.map(|t| t.saturating_sub(1)).unwrap_or(OPEN_END),
        state: RangeState::Pending,
        bytes_written: 0,
    }]
}

/// A worker's exclusive claim on part of the file.
#[derive(Clone)]
pub struct Lease {
    pub idx: u64,
    pub start: u64,
    /// Inclusive upper bound. The scheduler may lower this; re-read it before
    /// every write and stop when it is passed.
    pub end: Arc<AtomicU64>,
    /// Bytes written from `start`, published by the owning worker.
    pub progress: Arc<AtomicU64>,
}

impl Lease {
    pub fn end(&self) -> u64 {
        self.end.load(Ordering::Acquire)
    }

    pub fn progress(&self) -> u64 {
        self.progress.load(Ordering::Acquire)
    }

    /// Absolute file offset of the next byte to fetch.
    pub fn cursor(&self) -> u64 {
        self.start + self.progress()
    }

    pub fn is_open_ended(&self) -> bool {
        self.end() >= OPEN_END
    }

    /// Bytes still owed on this lease, given the current (possibly lowered) end.
    pub fn remaining(&self) -> u64 {
        let end = self.end();
        let cursor = self.cursor();
        if cursor > end { 0 } else { end - cursor + 1 }
    }

    pub fn publish_progress(&self, bytes_from_start: u64) {
        self.progress.store(bytes_from_start, Ordering::Release);
    }
}

struct Live {
    start: u64,
    end: Arc<AtomicU64>,
    progress: Arc<AtomicU64>,
    state: RangeState,
    leased: bool,
}

struct State {
    ranges: BTreeMap<u64, Live>,
    pending: VecDeque<u64>,
    next_idx: u64,
}

pub struct Scheduler {
    state: Mutex<State>,
    /// Woken when work appears or the last worker finishes, so idle workers do
    /// not poll.
    wake: Notify,
}

/// A split the scheduler performed, for the caller to persist.
#[derive(Debug, Clone, Copy)]
pub struct Split {
    pub shrunk: RangeRecord,
    pub added: RangeRecord,
}

impl Scheduler {
    /// Build from persisted ranges. Completed ranges stay completed; anything
    /// else becomes pending from its durable prefix — including ranges left in
    /// `downloading` by a process that died (PRD §12 step 4).
    pub fn from_ranges(ranges: &[RangeRecord]) -> Self {
        let mut map = BTreeMap::new();
        let mut pending = VecDeque::new();
        let mut next_idx = 0;
        for r in ranges {
            next_idx = next_idx.max(r.idx + 1);
            let complete = r.state == RangeState::Complete;
            map.insert(
                r.idx,
                Live {
                    start: r.start,
                    end: Arc::new(AtomicU64::new(r.end)),
                    progress: Arc::new(AtomicU64::new(if complete {
                        r.size()
                    } else {
                        r.bytes_written
                    })),
                    state: if complete {
                        RangeState::Complete
                    } else {
                        RangeState::Pending
                    },
                    leased: false,
                },
            );
            if !complete {
                pending.push_back(r.idx);
            }
        }
        Self {
            state: Mutex::new(State {
                ranges: map,
                pending,
                next_idx,
            }),
            wake: Notify::new(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Claim work: a pending range if one exists, otherwise steal the tail of
    /// the slowest active range. `None` means "nothing to do right now".
    pub fn acquire(&self) -> Option<(Lease, Option<Split>)> {
        let mut st = self.lock();
        while let Some(idx) = st.pending.pop_front() {
            let Some(live) = st.ranges.get_mut(&idx) else {
                continue;
            };
            if live.state == RangeState::Complete || live.leased {
                continue;
            }
            live.leased = true;
            live.state = RangeState::Downloading;
            return Some((
                Lease {
                    idx,
                    start: live.start,
                    end: live.end.clone(),
                    progress: live.progress.clone(),
                },
                None,
            ));
        }
        self.split_locked(&mut st).map(|(l, s)| (l, Some(s)))
    }

    /// Subdivide the active range with the largest remaining tail so an idle
    /// worker has something to do (PRD §8).
    fn split_locked(&self, st: &mut State) -> Option<(Lease, Split)> {
        let mut best: Option<(u64, u64)> = None; // (idx, tail length)
        for (idx, live) in st.ranges.iter() {
            if !live.leased || live.state == RangeState::Complete {
                continue;
            }
            let end = live.end.load(Ordering::Acquire);
            if end >= OPEN_END {
                // An open-ended range has no known midpoint to split at.
                continue;
            }
            let cursor = live.start + live.progress.load(Ordering::Acquire);
            let split_at = cursor + SPLIT_MARGIN;
            if end < split_at {
                continue;
            }
            let tail = end - split_at + 1;
            if tail < MIN_SPLIT_TAIL {
                continue;
            }
            if best.is_none_or(|(_, best_tail)| tail > best_tail) {
                best = Some((*idx, tail));
            }
        }

        let (victim_idx, _) = best?;
        let (victim_start, old_end, cursor) = {
            let live = st.ranges.get(&victim_idx)?;
            (
                live.start,
                live.end.load(Ordering::Acquire),
                live.start + live.progress.load(Ordering::Acquire),
            )
        };

        // Split at the midpoint of what is left, but never closer than
        // SPLIT_MARGIN to where the victim is writing right now.
        let midpoint = cursor + (old_end - cursor) / 2;
        let split_at = midpoint.max(cursor + SPLIT_MARGIN);
        if split_at > old_end || old_end - split_at + 1 < MIN_SPLIT_TAIL {
            return None;
        }

        // Lower the victim's ceiling first. From this instant the victim can no
        // longer write at or past `split_at`.
        let new_victim_end = split_at - 1;
        let victim_progress = {
            let live = st.ranges.get_mut(&victim_idx)?;
            live.end.store(new_victim_end, Ordering::Release);
            live.progress.load(Ordering::Acquire)
        };

        let new_idx = st.next_idx;
        st.next_idx += 1;
        let end = Arc::new(AtomicU64::new(old_end));
        let progress = Arc::new(AtomicU64::new(0));
        st.ranges.insert(
            new_idx,
            Live {
                start: split_at,
                end: end.clone(),
                progress: progress.clone(),
                state: RangeState::Downloading,
                leased: true,
            },
        );

        Some((
            Lease {
                idx: new_idx,
                start: split_at,
                end,
                progress,
            },
            Split {
                shrunk: RangeRecord {
                    idx: victim_idx,
                    start: victim_start,
                    end: new_victim_end,
                    state: RangeState::Downloading,
                    bytes_written: victim_progress,
                },
                added: RangeRecord {
                    idx: new_idx,
                    start: split_at,
                    end: old_end,
                    state: RangeState::Pending,
                    bytes_written: 0,
                },
            },
        ))
    }

    /// Mark a lease finished. Idempotent.
    pub fn complete(&self, idx: u64) {
        let mut st = self.lock();
        if let Some(live) = st.ranges.get_mut(&idx) {
            live.state = RangeState::Complete;
            live.leased = false;
            let end = live.end.load(Ordering::Acquire);
            live.progress.store(
                end.saturating_sub(live.start).saturating_add(1),
                Ordering::Release,
            );
        }
        drop(st);
        self.wake.notify_waiters();
    }

    /// Release a lease without completing it. The range keeps its durable
    /// prefix and goes back in the queue, so one worker's failure costs only
    /// that range (PRD Invariant 6).
    pub fn release(&self, idx: u64) {
        let mut st = self.lock();
        if let Some(live) = st.ranges.get_mut(&idx) {
            if live.state != RangeState::Complete {
                live.state = RangeState::Pending;
                live.leased = false;
                st.pending.push_back(idx);
            }
        }
        drop(st);
        self.wake.notify_waiters();
    }

    /// Give up on a range permanently. The download as a whole fails, but we
    /// keep every other range's progress.
    pub fn fail(&self, idx: u64) {
        let mut st = self.lock();
        if let Some(live) = st.ranges.get_mut(&idx) {
            live.state = RangeState::Failed;
            live.leased = false;
        }
        drop(st);
        self.wake.notify_waiters();
    }

    /// When a range's real length turns out to differ from the plan — an
    /// open-ended sequential transfer that just ended — record the true end.
    pub fn set_end(&self, idx: u64, end: u64) {
        let st = self.lock();
        if let Some(live) = st.ranges.get(&idx) {
            live.end.store(end, Ordering::Release);
        }
    }

    pub fn is_finished(&self) -> bool {
        let st = self.lock();
        st.ranges.values().all(|l| l.state == RangeState::Complete)
    }

    pub fn has_failure(&self) -> bool {
        let st = self.lock();
        st.ranges.values().any(|l| l.state == RangeState::Failed)
    }

    /// True when no work is available and none will become available — every
    /// range is either complete or failed.
    pub fn is_drained(&self) -> bool {
        let st = self.lock();
        st.pending.is_empty()
            && st
                .ranges
                .values()
                .all(|l| matches!(l.state, RangeState::Complete | RangeState::Failed) || l.leased)
            && !st.ranges.values().any(|l| l.leased)
    }

    pub async fn wait_for_change(&self, timeout: std::time::Duration) {
        let _ = tokio::time::timeout(timeout, self.wake.notified()).await;
    }

    pub fn notify(&self) {
        self.wake.notify_waiters();
    }

    /// Current view of every range, for the committer and for progress.
    pub fn snapshot(&self) -> Vec<RangeRecord> {
        let st = self.lock();
        st.ranges
            .iter()
            .map(|(idx, live)| RangeRecord {
                idx: *idx,
                start: live.start,
                end: live.end.load(Ordering::Acquire),
                state: live.state,
                bytes_written: live.progress.load(Ordering::Acquire),
            })
            .collect()
    }

    pub fn counts(&self) -> (usize, usize) {
        let st = self.lock();
        let complete = st
            .ranges
            .values()
            .filter(|l| l.state == RangeState::Complete)
            .count();
        (complete, st.ranges.len())
    }

    /// Sum of every range's written prefix — the engine's view of "downloaded".
    pub fn written_bytes(&self) -> u64 {
        let st = self.lock();
        st.ranges
            .values()
            .map(|l| l.progress.load(Ordering::Acquire))
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The invariant from PRD §34: ranges partition the file exactly.
    fn assert_partition(ranges: &[RangeRecord], total: u64) {
        let mut sorted: Vec<_> = ranges.to_vec();
        sorted.sort_by_key(|r| r.start);
        let mut cursor = 0u64;
        for r in &sorted {
            assert_eq!(
                r.start, cursor,
                "gap or overlap at range {} ({}..={})",
                r.idx, r.start, r.end
            );
            assert!(r.end >= r.start, "inverted range {}", r.idx);
            assert!(r.end < total, "range {} extends past {total}", r.idx);
            cursor = r.end + 1;
        }
        assert_eq!(cursor, total, "ranges do not cover the whole file");
    }

    #[test]
    fn plan_partitions_the_file() {
        for total in [1u64, 2, MIN_CHUNK - 1, MIN_CHUNK, MIN_CHUNK + 1, 1 << 30] {
            for conns in [1usize, 4, 8, 32] {
                let ranges = plan(total, conns);
                assert!(!ranges.is_empty());
                assert_partition(&ranges, total);
            }
        }
    }

    #[test]
    fn plan_of_empty_file_is_empty() {
        assert!(plan(0, 8).is_empty());
    }

    #[test]
    fn chunk_size_is_bounded() {
        assert_eq!(chunk_size(1, 8), MIN_CHUNK);
        assert_eq!(chunk_size(u64::MAX, 8), MAX_CHUNK);
        let c = chunk_size(10 << 30, 8);
        assert!((MIN_CHUNK..=MAX_CHUNK).contains(&c));
    }

    #[test]
    fn an_idle_worker_steals_from_the_range_furthest_behind() {
        // With one range per connection there is no spare pending work, so
        // stealing is the *entire* rebalancing mechanism -- this is what the
        // plan's 4x oversubscription used to provide. A fast connection that
        // finishes early must be able to take work off a slow one, or a single
        // straggler decides the download's wall clock.
        let total = 256 << 20;
        let s = Scheduler::from_ranges(&plan(total, 4));
        let mut leases = Vec::new();
        for _ in 0..4 {
            let (lease, split) = s.acquire().expect("a pending range");
            assert!(split.is_none(), "no split while pending work remains");
            leases.push(lease);
        }

        // Three connections finish. The fourth has barely started.
        for lease in &leases[..3] {
            s.complete(lease.idx);
        }
        let straggler = &leases[3];
        straggler.publish_progress(1 << 20);

        let (stolen, split) = s
            .acquire()
            .expect("an idle worker must be able to steal from the straggler");
        let split = split.expect("the only work left is inside a leased range");
        assert_eq!(split.shrunk.idx, straggler.idx);

        // The stolen tail must start beyond where the victim could still be
        // writing, and stay inside what the victim originally owned.
        assert!(
            stolen.start > straggler.start + straggler.progress(),
            "stole bytes the victim may still write"
        );
        assert!(stolen.end() <= straggler.start + (total / 4) - 1);
        assert_eq!(
            straggler.end(),
            stolen.start - 1,
            "the victim's ceiling must drop to meet the stolen tail"
        );
    }

    #[test]
    fn primed_plan_pins_its_first_range_to_the_primed_bytes() {
        let total = 64 << 20;
        let primed = MIN_CHUNK;
        let ranges = plan_primed(total, 4, primed);

        // The first range must be exactly what the probe already has in flight,
        // or those bytes are either wasted or fetched twice.
        assert_eq!(ranges[0].start, 0);
        assert_eq!(ranges[0].end, primed - 1);

        // Still a contiguous, gapless partition of the whole file.
        for pair in ranges.windows(2) {
            assert_eq!(pair[1].start, pair[0].end + 1, "gap or overlap in the plan");
        }
        assert_eq!(ranges.last().unwrap().end, total - 1);
        for (i, r) in ranges.iter().enumerate() {
            assert_eq!(r.idx, i as u64);
        }
    }

    #[test]
    fn primed_plan_falls_back_when_there_is_no_boundary_to_pin() {
        let total = 64 << 20;
        // A body covering the whole file, or none of it, leaves nothing to pin.
        assert_eq!(plan_primed(total, 4, 0), plan(total, 4));
        assert_eq!(plan_primed(total, 4, total), plan(total, 4));
        assert_eq!(plan_primed(total, 4, total + 1), plan(total, 4));
        assert!(plan_primed(0, 4, MIN_CHUNK).is_empty());
    }

    #[test]
    fn sequential_plan_handles_unknown_size() {
        let r = plan_sequential(None);
        assert_eq!(r.len(), 1);
        assert!(r[0].is_open_ended());

        let r = plan_sequential(Some(1000));
        assert_eq!(r[0].end, 999);
    }

    #[test]
    fn acquire_hands_out_each_range_once() {
        let total = 100 << 20;
        let s = Scheduler::from_ranges(&plan(total, 4));
        let (_, count) = s.counts();
        let mut seen = Vec::new();
        for _ in 0..count {
            let (lease, split) = s.acquire().expect("a pending range");
            assert!(split.is_none(), "should not split while ranges are pending");
            assert!(
                !seen.contains(&lease.idx),
                "range {} leased twice",
                lease.idx
            );
            seen.push(lease.idx);
        }
        assert_eq!(seen.len(), count);

        // The plan is fully leased now, so the only way to satisfy more demand
        // is to steal the tail of a range already in flight. A plan of one range
        // per connection relies on exactly that.
        if let Some((lease, split)) = s.acquire() {
            assert!(
                split.is_some(),
                "a lease beyond the plan must come from a split"
            );
            assert!(
                !seen.contains(&lease.idx),
                "split handed back an existing range"
            );
        }
    }

    #[test]
    fn resumes_only_incomplete_ranges() {
        let mut ranges = plan(100 << 20, 4);
        ranges[0].state = RangeState::Complete;
        ranges[0].bytes_written = ranges[0].size();
        // A range the previous process was mid-way through.
        ranges[1].state = RangeState::Downloading;
        ranges[1].bytes_written = 1024;

        let s = Scheduler::from_ranges(&ranges);
        let mut leases = Vec::new();
        while let Some((lease, _)) = s.acquire() {
            leases.push(lease);
        }
        assert!(
            !leases.iter().any(|l| l.idx == 0),
            "completed range must not be re-leased"
        );
        let resumed = leases
            .iter()
            .find(|l| l.idx == 1)
            .expect("range 1 re-leased");
        assert_eq!(resumed.cursor(), ranges[1].start + 1024);
        assert_eq!(s.written_bytes(), ranges[0].size() + 1024);
    }

    #[test]
    fn split_never_overlaps_and_preserves_the_partition() {
        let total = 512 << 20;
        let s = Scheduler::from_ranges(&plan_sequential(Some(total)));
        let (victim, _) = s.acquire().expect("one range to lease");

        // Victim has written a little; an idle worker steals the tail.
        victim.publish_progress(16 << 20);
        let (thief, split) = s.acquire().expect("split should produce work");
        let split = split.expect("expected a split record");

        assert!(
            thief.start > victim.start + victim.progress() + SPLIT_MARGIN - 1,
            "split point {} too close to victim cursor {}",
            thief.start,
            victim.cursor()
        );
        assert_eq!(victim.end() + 1, thief.start, "split left a gap");
        assert_eq!(thief.end(), total - 1);
        assert_eq!(split.shrunk.end + 1, split.added.start);

        assert_partition(&s.snapshot(), total);
    }

    #[test]
    fn split_refuses_when_the_tail_is_small() {
        // 8 MiB total: after the margin there is nothing worth splitting.
        let s = Scheduler::from_ranges(&plan_sequential(Some(8 << 20)));
        let (lease, _) = s.acquire().unwrap();
        lease.publish_progress(1 << 20);
        assert!(s.acquire().is_none(), "should not split a tiny tail");
    }

    #[test]
    fn split_refuses_open_ended_ranges() {
        let s = Scheduler::from_ranges(&plan_sequential(None));
        let (lease, _) = s.acquire().unwrap();
        lease.publish_progress(64 << 20);
        assert!(s.acquire().is_none(), "cannot split an unknown length");
    }

    #[test]
    fn repeated_splits_keep_the_partition_intact() {
        let total = 4u64 << 30;
        let s = Scheduler::from_ranges(&plan(total, 2));
        let mut leases = Vec::new();
        while let Some((lease, _)) = s.acquire() {
            leases.push(lease);
        }
        // Everyone makes some progress, then we keep stealing tails.
        for round in 0..6 {
            for l in &leases {
                l.publish_progress((round + 1) * (8 << 20));
            }
            if let Some((lease, _)) = s.acquire() {
                leases.push(lease);
            }
            assert_partition(&s.snapshot(), total);
        }
    }

    #[test]
    fn release_requeues_with_progress_kept() {
        let s = Scheduler::from_ranges(&plan(100 << 20, 2));
        let (lease, _) = s.acquire().unwrap();
        lease.publish_progress(4096);
        s.release(lease.idx);

        let mut found = None;
        while let Some((l, _)) = s.acquire() {
            if l.idx == lease.idx {
                found = Some(l);
                break;
            }
        }
        let again = found.expect("released range should be handed out again");
        assert_eq!(again.progress(), 4096);
        assert!(!s.is_finished());
    }

    #[test]
    fn completion_is_tracked() {
        let ranges = plan(100 << 20, 2);
        let s = Scheduler::from_ranges(&ranges);
        let mut leases = Vec::new();
        while let Some((l, _)) = s.acquire() {
            leases.push(l);
        }
        for l in &leases {
            s.complete(l.idx);
        }
        assert!(s.is_finished());
        let (done, total) = s.counts();
        assert_eq!(done, total);
        assert_eq!(s.written_bytes(), 100 << 20);
        assert!(!s.has_failure());
    }

    #[test]
    fn failure_is_isolated() {
        let s = Scheduler::from_ranges(&plan(100 << 20, 2));
        let (a, _) = s.acquire().unwrap();
        let (b, _) = s.acquire().unwrap();
        b.publish_progress(1000);
        s.complete(b.idx);
        s.fail(a.idx);

        assert!(s.has_failure());
        assert!(!s.is_finished());
        // The completed range keeps its bytes.
        let snap = s.snapshot();
        let completed = snap.iter().find(|r| r.idx == b.idx).unwrap();
        assert_eq!(completed.state, RangeState::Complete);
    }

    #[test]
    fn lease_remaining_shrinks_with_the_ceiling() {
        let s = Scheduler::from_ranges(&plan_sequential(Some(1000)));
        let (lease, _) = s.acquire().unwrap();
        assert_eq!(lease.remaining(), 1000);
        lease.publish_progress(400);
        assert_eq!(lease.remaining(), 600);
        s.set_end(lease.idx, 499);
        assert_eq!(lease.remaining(), 100);
        lease.publish_progress(500);
        assert_eq!(lease.remaining(), 0);
    }
}
