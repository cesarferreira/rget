//! Resume: remote validation and state reconciliation (PRD §12, §13).
//!
//! Resume is the normal case, not the exceptional one. Two questions have to be
//! answered before a single byte is reused:
//!
//! 1. **Is the remote still the same object?** — [`validate`]
//! 2. **Is the local file still the file we were writing?** — [`check_identity`]
//!    and [`reconcile`]
//!
//! Either answer being "no" means we refuse or re-download, never "hope".

use crate::file::DestFile;
use crate::http::RemoteInfo;
use crate::storage::{DownloadRecord, RangeRecord, RangeState};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Validators {
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub size: Option<u64>,
}

impl Validators {
    pub fn of_record(rec: &DownloadRecord) -> Self {
        Self {
            etag: rec.etag.clone(),
            last_modified: rec.last_modified.clone(),
            size: rec.total_size,
        }
    }

    pub fn of_remote(info: &RemoteInfo) -> Self {
        Self {
            etag: info.etag.clone(),
            last_modified: info.last_modified.clone(),
            size: info.size,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Validation {
    /// A validator positively confirmed the resource is unchanged.
    Unchanged,
    /// Nothing contradicts resuming, but nothing proves it either. The engine
    /// warns; a checksum is the only real protection here.
    Unverifiable(String),
    /// The resource changed. Refuse (PRD Invariant 4).
    Changed {
        reason: String,
        previous: Validators,
        current: Validators,
    },
}

/// Compare what we recorded last time against a fresh probe.
pub fn validate(rec: &DownloadRecord, info: &RemoteInfo) -> Validation {
    let previous = Validators::of_record(rec);
    let current = Validators::of_remote(info);

    let changed = |reason: String| Validation::Changed {
        reason,
        previous: previous.clone(),
        current: current.clone(),
    };

    // Size is the cheapest and bluntest check, and it is decisive.
    if let (Some(before), Some(now)) = (previous.size, current.size) {
        if before != now {
            return changed(format!("size changed from {before} to {now} bytes"));
        }
    }

    let strong_before = previous
        .etag
        .as_deref()
        .filter(|t| !t.trim_start().starts_with("W/"));
    let strong_now = current
        .etag
        .as_deref()
        .filter(|t| !t.trim_start().starts_with("W/"));

    match (strong_before, strong_now) {
        (Some(a), Some(b)) if a == b => return Validation::Unchanged,
        (Some(a), Some(b)) => {
            return changed(format!("ETag changed from {a} to {b}"));
        }
        (Some(_), None) => {
            return Validation::Unverifiable(
                "the server no longer sends a strong ETag, so we cannot confirm the file is \
                 unchanged"
                    .into(),
            );
        }
        _ => {}
    }

    match (
        previous.last_modified.as_deref(),
        current.last_modified.as_deref(),
    ) {
        (Some(a), Some(b)) if a == b => return Validation::Unchanged,
        (Some(a), Some(b)) => {
            return changed(format!("Last-Modified changed from {a} to {b}"));
        }
        _ => {}
    }

    // Weak ETags are equality-of-meaning, not equality-of-bytes; matching ones
    // are reassuring but not proof.
    match (previous.etag.as_deref(), current.etag.as_deref()) {
        (Some(a), Some(b)) if a == b => {
            return Validation::Unverifiable(
                "only a weak ETag is available, which does not guarantee identical bytes".into(),
            );
        }
        (Some(a), Some(b)) => {
            return changed(format!("ETag changed from {a} to {b}"));
        }
        _ => {}
    }

    if previous.size.is_some() && previous.size == current.size {
        return Validation::Unverifiable(
            "the server sends no ETag or Last-Modified; only the size matches".into(),
        );
    }

    Validation::Unverifiable("the server provides no validators at all".into())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Identity {
    /// Same file we were writing into.
    Same,
    /// A different file now occupies the destination path.
    Replaced,
    /// We never recorded an identity (older record, or first run).
    Unrecorded,
}

/// Is the file at the destination the one we recorded progress against?
///
/// Path equality is not enough: the file can be deleted and recreated, or
/// swapped for an unrelated one, between runs.
pub fn check_identity(rec: &DownloadRecord, file: &DestFile) -> Identity {
    let (Some(dev), Some(ino)) = (rec.file_dev, rec.file_ino) else {
        return Identity::Unrecorded;
    };
    match file.identity() {
        Ok(id) if id.dev == dev && id.ino == ino => Identity::Same,
        Ok(_) => Identity::Replaced,
        Err(_) => Identity::Unrecorded,
    }
}

#[derive(Debug, Clone)]
pub struct Reconciled {
    pub ranges: Vec<RangeRecord>,
    /// Bytes we are confident about and will not fetch again.
    pub trusted_bytes: u64,
    /// Bytes the database claimed but we chose to re-download.
    pub discarded_bytes: u64,
    pub notes: Vec<String>,
}

/// Bring persisted ranges into line with the file that is actually on disk.
///
/// The durability protocol (see `docs/CRASH_CONSISTENCY.md`) guarantees the
/// database never claims more than the filesystem holds, so this is a
/// belt-and-braces pass rather than the primary defence. It still earns its
/// keep: it catches a destination truncated by filesystem recovery, a file
/// restored from a smaller backup, or a plan whose total size no longer
/// matches the remote.
pub fn reconcile(ranges: &[RangeRecord], file_len: u64, total: Option<u64>) -> Reconciled {
    let mut out = Vec::with_capacity(ranges.len());
    let mut trusted = 0u64;
    let mut discarded = 0u64;
    let mut notes = Vec::new();

    for r in ranges {
        let mut r = *r;

        // A range beyond the resource's current size is meaningless.
        if let Some(total) = total {
            if r.start >= total {
                discarded += r.bytes_written;
                notes.push(format!(
                    "range {} starts past the end of the file ({} >= {total}); dropping it",
                    r.idx, r.start
                ));
                continue;
            }
            if !r.is_open_ended() && r.end >= total {
                r.end = total - 1;
                if r.bytes_written > r.size() {
                    discarded += r.bytes_written - r.size();
                    r.bytes_written = r.size();
                }
            }
        }

        let claimed_end = r.start + r.bytes_written;
        if claimed_end > file_len {
            // The file is shorter than the database claims: trust the file.
            let keep = file_len.saturating_sub(r.start);
            discarded += r.bytes_written - keep;
            notes.push(format!(
                "range {} claimed {} bytes but the file is only {file_len} bytes; keeping {keep}",
                r.idx, r.bytes_written
            ));
            r.bytes_written = keep;
            r.state = RangeState::Pending;
        }

        if r.state == RangeState::Complete && r.bytes_written < r.size() {
            // Complete but short: contradiction, so re-download it.
            notes.push(format!(
                "range {} was marked complete with only {} of {} bytes; re-downloading",
                r.idx,
                r.bytes_written,
                r.size()
            ));
            discarded += r.bytes_written;
            r.bytes_written = 0;
            r.state = RangeState::Pending;
        }

        if r.state == RangeState::Complete {
            trusted += r.size();
        } else {
            // Anything left `downloading` belonged to a process that died.
            // Its durable prefix is trustworthy; nothing beyond it is.
            r.state = RangeState::Pending;
            trusted += r.bytes_written;
        }
        out.push(r);
    }

    Reconciled {
        ranges: out,
        trusted_bytes: trusted,
        discarded_bytes: discarded,
        notes,
    }
}

/// Does this plan still cover exactly `total` bytes with no gaps? If not the
/// engine must replan rather than patch (PRD Invariant 3).
pub fn plan_is_intact(ranges: &[RangeRecord], total: u64) -> bool {
    if ranges.is_empty() {
        return total == 0;
    }
    let mut sorted: Vec<&RangeRecord> = ranges.iter().collect();
    sorted.sort_by_key(|r| r.start);
    let mut cursor = 0u64;
    for r in sorted {
        if r.start != cursor || r.end < r.start {
            return false;
        }
        cursor = r.end + 1;
    }
    cursor == total
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Status, mint_cookie, now};

    fn rec(etag: Option<&str>, last_modified: Option<&str>, size: Option<u64>) -> DownloadRecord {
        DownloadRecord {
            id: "aa11bb".into(),
            original_url: "https://x.example/f".into(),
            resolved_url: None,
            mirrors: vec![],
            destination: "/tmp/f".into(),
            filename: "f".into(),
            total_size: size,
            etag: etag.map(String::from),
            last_modified: last_modified.map(String::from),
            content_type: None,
            accept_ranges: true,
            expected_checksum: None,
            checksum_algorithm: None,
            file_cookie: mint_cookie(),
            file_dev: Some(1),
            file_ino: Some(2),
            durable_bytes: 0,
            status: Status::Paused,
            error: None,
            created_at: now(),
            updated_at: now(),
            completed_at: None,
        }
    }

    fn info(etag: Option<&str>, last_modified: Option<&str>, size: Option<u64>) -> RemoteInfo {
        RemoteInfo {
            final_url: url::Url::parse("https://x.example/f").unwrap(),
            size,
            accept_ranges: true,
            etag: etag.map(String::from),
            last_modified: last_modified.map(String::from),
            content_type: None,
            content_disposition: None,
            content_encoding: None,
        }
    }

    #[test]
    fn same_strong_etag_is_unchanged() {
        let v = validate(
            &rec(Some("\"abc\""), None, Some(100)),
            &info(Some("\"abc\""), None, Some(100)),
        );
        assert_eq!(v, Validation::Unchanged);
    }

    #[test]
    fn changed_etag_is_refused() {
        let v = validate(
            &rec(Some("\"abc\""), None, Some(100)),
            &info(Some("\"def\""), None, Some(100)),
        );
        match v {
            Validation::Changed {
                reason,
                previous,
                current,
            } => {
                assert!(reason.contains("ETag"), "{reason}");
                assert_eq!(previous.etag.as_deref(), Some("\"abc\""));
                assert_eq!(current.etag.as_deref(), Some("\"def\""));
            }
            other => panic!("expected Changed, got {other:?}"),
        }
    }

    #[test]
    fn size_change_is_decisive_even_with_matching_etag() {
        // A server can serve a stale ETag for changed content; size disagrees,
        // so refuse.
        let v = validate(
            &rec(Some("\"abc\""), None, Some(100)),
            &info(Some("\"abc\""), None, Some(101)),
        );
        assert!(matches!(v, Validation::Changed { .. }));
    }

    #[test]
    fn changed_last_modified_is_refused() {
        let v = validate(
            &rec(None, Some("Mon, 01 Jan 2024 00:00:00 GMT"), Some(100)),
            &info(None, Some("Tue, 02 Jan 2024 00:00:00 GMT"), Some(100)),
        );
        assert!(matches!(v, Validation::Changed { .. }));
    }

    #[test]
    fn weak_etags_are_never_proof() {
        let v = validate(
            &rec(Some("W/\"abc\""), None, Some(100)),
            &info(Some("W/\"abc\""), None, Some(100)),
        );
        assert!(matches!(v, Validation::Unverifiable(_)), "{v:?}");
    }

    #[test]
    fn vanished_etag_is_unverifiable_not_unchanged() {
        let v = validate(
            &rec(Some("\"abc\""), None, Some(100)),
            &info(None, None, Some(100)),
        );
        assert!(matches!(v, Validation::Unverifiable(_)), "{v:?}");
    }

    #[test]
    fn no_validators_at_all_is_unverifiable() {
        let v = validate(&rec(None, None, Some(100)), &info(None, None, Some(100)));
        assert!(matches!(v, Validation::Unverifiable(_)), "{v:?}");
        let v = validate(&rec(None, None, None), &info(None, None, None));
        assert!(matches!(v, Validation::Unverifiable(_)), "{v:?}");
    }

    fn ranges() -> Vec<RangeRecord> {
        vec![
            RangeRecord {
                idx: 0,
                start: 0,
                end: 499,
                state: RangeState::Complete,
                bytes_written: 500,
            },
            RangeRecord {
                idx: 1,
                start: 500,
                end: 999,
                state: RangeState::Downloading,
                bytes_written: 200,
            },
        ]
    }

    #[test]
    fn reconcile_keeps_durable_progress() {
        let r = reconcile(&ranges(), 1000, Some(1000));
        assert_eq!(r.trusted_bytes, 700);
        assert_eq!(r.discarded_bytes, 0);
        assert_eq!(r.ranges[0].state, RangeState::Complete);
        // In-flight ranges come back as pending, keeping their prefix.
        assert_eq!(r.ranges[1].state, RangeState::Pending);
        assert_eq!(r.ranges[1].bytes_written, 200);
    }

    #[test]
    fn reconcile_trusts_a_short_file_over_the_database() {
        // Filesystem recovery truncated the file to 600 bytes.
        let r = reconcile(&ranges(), 600, Some(1000));
        assert_eq!(r.ranges[1].bytes_written, 100);
        assert_eq!(r.discarded_bytes, 100);
        assert!(!r.notes.is_empty());
        assert_eq!(r.trusted_bytes, 600);
    }

    #[test]
    fn reconcile_rejects_complete_but_short_ranges() {
        let mut rs = ranges();
        rs[0].bytes_written = 10; // complete, but only 10 of 500 bytes
        let r = reconcile(&rs, 1000, Some(1000));
        assert_eq!(r.ranges[0].state, RangeState::Pending);
        assert_eq!(r.ranges[0].bytes_written, 0);
        assert_eq!(r.discarded_bytes, 10);
    }

    #[test]
    fn reconcile_clips_ranges_past_a_shrunken_resource() {
        let r = reconcile(&ranges(), 1000, Some(700));
        assert_eq!(r.ranges.len(), 2);
        assert_eq!(r.ranges[1].end, 699);
        // Range 0 is still fully inside the resource.
        assert_eq!(r.ranges[0].state, RangeState::Complete);

        // A range entirely past the new end is dropped, and the surviving
        // range is clipped: 100 bytes clipped off range 0 plus range 1's 200.
        let r = reconcile(&ranges(), 1000, Some(400));
        assert_eq!(r.ranges.len(), 1);
        assert_eq!(r.ranges[0].end, 399);
        assert_eq!(r.discarded_bytes, 300);
        assert_eq!(r.trusted_bytes, 400);
    }

    #[test]
    fn plan_integrity_check() {
        assert!(plan_is_intact(&ranges(), 1000));
        assert!(!plan_is_intact(&ranges(), 1001));
        assert!(!plan_is_intact(&[], 10));
        assert!(plan_is_intact(&[], 0));

        let gapped = vec![
            RangeRecord {
                idx: 0,
                start: 0,
                end: 99,
                state: RangeState::Pending,
                bytes_written: 0,
            },
            RangeRecord {
                idx: 1,
                start: 200,
                end: 999,
                state: RangeState::Pending,
                bytes_written: 0,
            },
        ];
        assert!(!plan_is_intact(&gapped, 1000));
    }

    #[test]
    fn identity_detects_a_swapped_file() {
        let dir = std::env::temp_dir().join(format!("rget-resume-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("f");
        std::fs::write(&path, b"x").unwrap();
        let file = DestFile::open(&path).unwrap();
        let id = file.identity().unwrap();

        let mut r = rec(None, None, Some(1));
        r.file_dev = Some(id.dev);
        r.file_ino = Some(id.ino);
        assert_eq!(check_identity(&r, &file), Identity::Same);

        r.file_ino = Some(id.ino.wrapping_add(1));
        assert_eq!(check_identity(&r, &file), Identity::Replaced);

        r.file_dev = None;
        assert_eq!(check_identity(&r, &file), Identity::Unrecorded);

        std::fs::remove_dir_all(&dir).ok();
    }
}
