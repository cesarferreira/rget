//! Property tests for range management (PRD §34).
//!
//! Randomised sequences of complete / fail / release / split / restart, checking
//! the invariants that make parallel writes safe:
//!
//! * ranges never overlap
//! * completed + pending + active covers the whole file, with no gaps
//! * no range extends past the content length
//!
//! Driven by a seeded xorshift rather than a property-testing crate: the whole
//! generator is fifteen lines, and a fixed seed list means a failure is
//! reproducible from the test name alone.

use rget::scheduler::{MIN_SPLIT_TAIL, SPLIT_MARGIN, Scheduler, plan};
use rget::storage::{RangeRecord, RangeState};

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
}

/// The core invariant: the live range set is an exact partition of `[0, total)`.
fn assert_partition(ranges: &[RangeRecord], total: u64, context: &str) {
    let mut sorted: Vec<&RangeRecord> = ranges.iter().collect();
    sorted.sort_by_key(|r| r.start);

    let mut cursor = 0u64;
    for r in &sorted {
        assert!(
            r.end >= r.start,
            "{context}: range {} is inverted ({}..={})",
            r.idx,
            r.start,
            r.end
        );
        assert!(
            r.start >= cursor,
            "{context}: range {} overlaps its predecessor (starts {}, expected >= {cursor})",
            r.idx,
            r.start
        );
        assert!(
            r.start == cursor,
            "{context}: gap before range {} ({}..{})",
            r.idx,
            cursor,
            r.start
        );
        assert!(
            r.end < total,
            "{context}: range {} extends past the content length ({} >= {total})",
            r.idx,
            r.end
        );
        assert!(
            r.bytes_written <= r.size(),
            "{context}: range {} claims {} of {} bytes",
            r.idx,
            r.bytes_written,
            r.size()
        );
        cursor = r.end + 1;
    }
    assert_eq!(
        cursor, total,
        "{context}: ranges do not cover the whole file"
    );
}

/// Sum of every range's progress, which is what the engine reports as
/// "downloaded" and the committer persists.
fn written(ranges: &[RangeRecord]) -> u64 {
    ranges.iter().map(|r| r.bytes_written).sum()
}

#[test]
fn random_lifecycles_preserve_the_partition() {
    for seed in 1..=200u64 {
        let mut rng = Rng::new(seed * 0x9E3779B9);
        let total = 64 * 1024 * 1024 + rng.below(512 * 1024 * 1024);
        let connections = 1 + rng.below(16) as usize;

        let initial = plan(total, connections);
        assert_partition(&initial, total, &format!("seed {seed}: initial plan"));

        let sched = Scheduler::from_ranges(&initial);
        let mut leases = Vec::new();

        for step in 0..300 {
            let context = format!("seed {seed}, step {step}");
            match rng.below(100) {
                // Acquire work (possibly triggering a split).
                0..=39 => {
                    if let Some((lease, split)) = sched.acquire() {
                        if let Some(split) = split {
                            // A split must be contiguous and must never land
                            // inside what the victim has already written.
                            assert_eq!(
                                split.shrunk.end + 1,
                                split.added.start,
                                "{context}: split left a gap"
                            );
                            assert!(
                                split.added.start
                                    >= split.shrunk.start + split.shrunk.bytes_written,
                                "{context}: split point is behind the victim's cursor"
                            );
                        }
                        leases.push(lease);
                    }
                }
                // Make progress on a random lease.
                40..=79 => {
                    if !leases.is_empty() {
                        let i = rng.below(leases.len() as u64) as usize;
                        let lease = &leases[i];
                        let room = lease.end().saturating_sub(lease.start) + 1;
                        let step_bytes = rng.below(room / 4 + 1);
                        let next = (lease.progress() + step_bytes).min(room);
                        lease.publish_progress(next);
                    }
                }
                // Complete a lease.
                80..=89 => {
                    if !leases.is_empty() {
                        let i = rng.below(leases.len() as u64) as usize;
                        let lease = leases.swap_remove(i);
                        sched.complete(lease.idx);
                    }
                }
                // Release a lease back to the queue, keeping its progress.
                90..=96 => {
                    if !leases.is_empty() {
                        let i = rng.below(leases.len() as u64) as usize;
                        let lease = leases.swap_remove(i);
                        let before = lease.progress();
                        sched.release(lease.idx);
                        let snap = sched.snapshot();
                        let found = snap.iter().find(|r| r.idx == lease.idx).unwrap();
                        assert_eq!(
                            found.bytes_written, before,
                            "{context}: release lost progress"
                        );
                    }
                }
                // Simulate a process restart: rebuild the scheduler from the
                // persisted snapshot and carry on.
                _ => {
                    let snapshot = sched.snapshot();
                    assert_partition(&snapshot, total, &format!("{context}: before restart"));
                    let restarted = Scheduler::from_ranges(&snapshot);
                    assert_partition(
                        &restarted.snapshot(),
                        total,
                        &format!("{context}: after restart"),
                    );
                    // Completed ranges survive a restart; in-flight ones keep
                    // their durable prefix.
                    let before_complete = snapshot
                        .iter()
                        .filter(|r| r.state == RangeState::Complete)
                        .count();
                    let after_complete = restarted
                        .snapshot()
                        .iter()
                        .filter(|r| r.state == RangeState::Complete)
                        .count();
                    assert_eq!(
                        before_complete, after_complete,
                        "{context}: restart changed the completed set"
                    );
                    assert_eq!(
                        written(&snapshot),
                        written(&restarted.snapshot()),
                        "{context}: restart changed the written total"
                    );
                }
            }

            assert_partition(&sched.snapshot(), total, &context);
            assert!(
                written(&sched.snapshot()) <= total,
                "{context}: written bytes exceed the file size"
            );
        }
    }
}

#[test]
fn splits_never_hand_out_bytes_a_worker_may_still_write() {
    // Focused on the one race that could silently corrupt a file: the split
    // point must stay clear of the victim's write position by SPLIT_MARGIN.
    for seed in 1..=100u64 {
        let mut rng = Rng::new(seed * 0x2545F491);
        let total = 512 * 1024 * 1024 + rng.below(1 << 30);
        let sched = Scheduler::from_ranges(&plan(total, 1));

        let mut leases = Vec::new();
        let (first, _) = sched.acquire().expect("first lease");
        leases.push(first);

        for _ in 0..60 {
            // Everyone advances a bit.
            for lease in &leases {
                let room = lease.end().saturating_sub(lease.start) + 1;
                let bump = rng.below(4 * 1024 * 1024);
                lease.publish_progress((lease.progress() + bump).min(room));
            }

            let cursors: Vec<(u64, u64, u64)> = sched
                .snapshot()
                .iter()
                .map(|r| (r.idx, r.start + r.bytes_written, r.end))
                .collect();

            if let Some((lease, split)) = sched.acquire() {
                if let Some(split) = split {
                    let victim_cursor = cursors
                        .iter()
                        .find(|(idx, _, _)| *idx == split.shrunk.idx)
                        .map(|(_, cursor, _)| *cursor)
                        .expect("victim was in the snapshot");
                    assert!(
                        split.added.start >= victim_cursor + SPLIT_MARGIN,
                        "seed {seed}: split at {} is within {} bytes of the victim's cursor {victim_cursor}",
                        split.added.start,
                        SPLIT_MARGIN
                    );
                    assert!(
                        split.added.end + 1 - split.added.start >= MIN_SPLIT_TAIL,
                        "seed {seed}: split produced a pointlessly small tail"
                    );
                }
                leases.push(lease);
            }

            // No two leases may ever own overlapping bytes.
            let snapshot = sched.snapshot();
            assert_partition(&snapshot, total, &format!("seed {seed}"));
        }
    }
}

#[test]
fn a_failed_range_never_invalidates_the_others() {
    // PRD Invariant 6, exercised across many interleavings.
    for seed in 1..=100u64 {
        let mut rng = Rng::new(seed * 0x1000193);
        let total = 64 * 1024 * 1024 + rng.below(64 * 1024 * 1024);
        let sched = Scheduler::from_ranges(&plan(total, 4));

        let mut leases = Vec::new();
        while let Some((lease, _)) = sched.acquire() {
            leases.push(lease);
            if leases.len() > 32 {
                break;
            }
        }
        assert!(!leases.is_empty());

        // Complete half, fail one, and check the completed set is untouched.
        let mut completed = Vec::new();
        for lease in leases.iter().take(leases.len() / 2) {
            let room = lease.end() - lease.start + 1;
            lease.publish_progress(room);
            sched.complete(lease.idx);
            completed.push((lease.idx, room));
        }

        let victim = &leases[leases.len() - 1];
        sched.fail(victim.idx);
        assert!(sched.has_failure());
        assert!(!sched.is_finished());

        let snapshot = sched.snapshot();
        for (idx, bytes) in &completed {
            let range = snapshot.iter().find(|r| r.idx == *idx).unwrap();
            assert_eq!(
                range.state,
                RangeState::Complete,
                "seed {seed}: a failure demoted range {idx}"
            );
            assert_eq!(
                range.bytes_written, *bytes,
                "seed {seed}: a failure lost bytes from range {idx}"
            );
        }
        assert_partition(&snapshot, total, &format!("seed {seed}: after failure"));
    }
}
