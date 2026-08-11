//! Mirrors and source selection (PRD §15).
//!
//! The rule that matters: **never splice bytes from two resources you cannot
//! show are the same file.** Same filename on two hosts proves nothing. So a
//! mirror is admitted only when
//!
//! * its size matches the primary's, **and**
//! * its strong `ETag` matches the primary's, **or** the user gave us a
//!   checksum, which means an end-to-end verification will catch a mismatch.
//!
//! Anything else is reported and skipped rather than silently mixed in.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

use url::Url;

use crate::http::RemoteInfo;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admission {
    /// The URL the user asked for; always usable.
    Primary,
    /// Proven equivalent by strong validators.
    Verified,
    /// Not proven equivalent, but a checksum will catch any mistake.
    ChecksumGuarded,
    Rejected(String),
}

impl Admission {
    pub fn is_usable(&self) -> bool {
        !matches!(self, Admission::Rejected(_))
    }
}

/// Decide whether `candidate` may contribute bytes to the same file as
/// `primary`.
pub fn classify(primary: &RemoteInfo, candidate: &RemoteInfo, has_checksum: bool) -> Admission {
    match (primary.size, candidate.size) {
        (Some(a), Some(b)) if a != b => {
            return Admission::Rejected(format!("size differs ({b} vs {a} bytes)"));
        }
        (Some(_), None) => {
            return Admission::Rejected("mirror did not report a size".into());
        }
        _ => {}
    }

    if !candidate.accept_ranges && primary.accept_ranges {
        return Admission::Rejected("mirror does not support range requests".into());
    }

    if primary.has_strong_etag() && candidate.has_strong_etag() {
        if primary.etag == candidate.etag {
            return Admission::Verified;
        }
        if has_checksum {
            return Admission::ChecksumGuarded;
        }
        return Admission::Rejected(
            "ETag differs from the primary; pass a checksum to allow it anyway".into(),
        );
    }

    if has_checksum {
        Admission::ChecksumGuarded
    } else {
        Admission::Rejected(
            "cannot prove this is the same file (no strong ETag on both sides and no checksum given)"
                .into(),
        )
    }
}

pub struct Source {
    pub url: Url,
    pub admission: Admission,
    /// This source's own `If-Range` validator. Validators are per-resource, not
    /// per-download: sending the primary's ETag to a mirror would make the
    /// mirror answer with a full body, which we would read as "the file
    /// changed". Each source therefore carries the validator it issued.
    pub validator: Option<String>,
    failures: AtomicU32,
}

impl Source {
    pub fn new(url: Url, admission: Admission, validator: Option<String>) -> Self {
        Self {
            url,
            admission,
            validator,
            failures: AtomicU32::new(0),
        }
    }

    pub fn failures(&self) -> u32 {
        self.failures.load(Ordering::Relaxed)
    }
}

/// The set of usable sources for one download, with least-failures selection so
/// a flaky mirror drains itself out of rotation without being banned outright.
pub struct SourceSet {
    sources: Vec<Source>,
    cursor: Mutex<usize>,
}

impl SourceSet {
    /// Keeps only usable sources. The primary is always first.
    pub fn new(sources: Vec<Source>) -> Self {
        let sources: Vec<Source> = sources
            .into_iter()
            .filter(|s| s.admission.is_usable())
            .collect();
        Self {
            sources,
            cursor: Mutex::new(0),
        }
    }

    pub fn single(url: Url, validator: Option<String>) -> Self {
        Self::new(vec![Source::new(url, Admission::Primary, validator)])
    }

    pub fn len(&self) -> usize {
        self.sources.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    pub fn urls(&self) -> Vec<String> {
        self.sources.iter().map(|s| s.url.to_string()).collect()
    }

    /// Pick a source: fewest failures wins, ties broken round-robin so several
    /// workers starting at once spread across mirrors.
    pub fn pick(&self) -> (usize, &Source) {
        let min = self.sources.iter().map(|s| s.failures()).min().unwrap_or(0);
        let candidates: Vec<usize> = self
            .sources
            .iter()
            .enumerate()
            .filter(|(_, s)| s.failures() == min)
            .map(|(i, _)| i)
            .collect();
        let mut cursor = self.cursor.lock().unwrap_or_else(|e| e.into_inner());
        *cursor = cursor.wrapping_add(1);
        let idx = candidates[*cursor % candidates.len()];
        (idx, &self.sources[idx])
    }

    pub fn penalise(&self, idx: usize) {
        if let Some(s) = self.sources.get(idx) {
            s.failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn reward(&self, idx: usize) {
        if let Some(s) = self.sources.get(idx) {
            // Decay rather than reset: a mirror that just succeeded is not
            // proven healthy, it is merely less suspect.
            let _ = s
                .failures
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(1))
                });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(size: Option<u64>, etag: Option<&str>) -> RemoteInfo {
        RemoteInfo {
            final_url: Url::parse("https://a.example/f").unwrap(),
            size,
            accept_ranges: true,
            etag: etag.map(String::from),
            last_modified: None,
            content_type: None,
            content_disposition: None,
            content_encoding: None,
        }
    }

    #[test]
    fn accepts_matching_strong_etags() {
        let a = info(Some(100), Some("\"x\""));
        let b = info(Some(100), Some("\"x\""));
        assert_eq!(classify(&a, &b, false), Admission::Verified);
    }

    #[test]
    fn rejects_size_mismatch_even_with_a_checksum() {
        let a = info(Some(100), Some("\"x\""));
        let b = info(Some(101), Some("\"x\""));
        assert!(matches!(classify(&a, &b, true), Admission::Rejected(_)));
    }

    #[test]
    fn rejects_differing_etags_without_a_checksum() {
        let a = info(Some(100), Some("\"x\""));
        let b = info(Some(100), Some("\"y\""));
        assert!(matches!(classify(&a, &b, false), Admission::Rejected(_)));
        // A checksum makes it safe to try: verification is the backstop.
        assert_eq!(classify(&a, &b, true), Admission::ChecksumGuarded);
    }

    #[test]
    fn rejects_unprovable_equivalence() {
        let a = info(Some(100), None);
        let b = info(Some(100), None);
        assert!(matches!(classify(&a, &b, false), Admission::Rejected(_)));
        assert_eq!(classify(&a, &b, true), Admission::ChecksumGuarded);
    }

    #[test]
    fn rejects_weak_etags_as_proof() {
        let a = info(Some(100), Some("W/\"x\""));
        let b = info(Some(100), Some("W/\"x\""));
        // Weak ETags say "semantically equivalent", not "byte-identical".
        assert!(matches!(classify(&a, &b, false), Admission::Rejected(_)));
    }

    #[test]
    fn rejects_mirror_without_range_support() {
        let a = info(Some(100), Some("\"x\""));
        let mut b = info(Some(100), Some("\"x\""));
        b.accept_ranges = false;
        assert!(matches!(classify(&a, &b, false), Admission::Rejected(_)));
    }

    #[test]
    fn drops_rejected_sources() {
        let set = SourceSet::new(vec![
            Source::new(
                Url::parse("https://a.example/f").unwrap(),
                Admission::Primary,
                None,
            ),
            Source::new(
                Url::parse("https://b.example/f").unwrap(),
                Admission::Rejected("nope".into()),
                None,
            ),
        ]);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn selection_avoids_failing_mirrors() {
        let set = SourceSet::new(vec![
            Source::new(
                Url::parse("https://a.example/f").unwrap(),
                Admission::Primary,
                Some("\"a\"".into()),
            ),
            Source::new(
                Url::parse("https://b.example/f").unwrap(),
                Admission::Verified,
                Some("\"b\"".into()),
            ),
        ]);
        set.penalise(0);
        for _ in 0..6 {
            let (idx, source) = set.pick();
            assert_eq!(
                idx, 1,
                "should avoid the failing mirror, got {}",
                source.url
            );
            // Each source keeps its own validator.
            assert_eq!(source.validator.as_deref(), Some("\"b\""));
        }
        // Once it recovers, rotation resumes.
        set.reward(0);
        let picks: std::collections::HashSet<usize> = (0..8).map(|_| set.pick().0).collect();
        assert_eq!(picks.len(), 2);
    }

    #[test]
    fn single_source_is_always_picked() {
        let set = SourceSet::single(Url::parse("https://a.example/f").unwrap(), None);
        set.penalise(0);
        assert_eq!(set.pick().0, 0);
    }
}
