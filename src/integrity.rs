//! Checksum verification (PRD §16).
//!
//! A mismatch is a failure, never a warning: `rget` exits non-zero and the
//! download is marked failed (PRD Invariant 5).

use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};

use crate::progress::{Event, Reporter};
use crate::shutdown::Cancel;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Algorithm {
    Sha256,
    Sha512,
    Blake3,
}

impl Algorithm {
    pub fn as_str(&self) -> &'static str {
        match self {
            Algorithm::Sha256 => "sha256",
            Algorithm::Sha512 => "sha512",
            Algorithm::Blake3 => "blake3",
        }
    }

    /// Expected hex length, used to reject a truncated or pasted-wrong digest
    /// before spending minutes hashing a large file.
    pub fn hex_len(&self) -> usize {
        match self {
            Algorithm::Sha256 => 64,
            Algorithm::Sha512 => 128,
            Algorithm::Blake3 => 64,
        }
    }

    /// Display name used in the UI.
    pub fn label(&self) -> &'static str {
        match self {
            Algorithm::Sha256 => "SHA-256",
            Algorithm::Sha512 => "SHA-512",
            Algorithm::Blake3 => "BLAKE3",
        }
    }
}

impl std::str::FromStr for Algorithm {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().replace('-', "").as_str() {
            "sha256" => Ok(Algorithm::Sha256),
            "sha512" => Ok(Algorithm::Sha512),
            "blake3" | "b3" => Ok(Algorithm::Blake3),
            other => bail!("unknown checksum algorithm `{other}`"),
        }
    }
}

impl std::fmt::Display for Algorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checksum {
    pub algorithm: Algorithm,
    pub expected: String,
}

impl Checksum {
    /// Accepts a bare digest, or one prefixed `sha256:`/`sha256=`, and
    /// normalises to lowercase hex.
    pub fn parse(algorithm: Algorithm, raw: &str) -> Result<Self> {
        let cleaned = raw
            .trim()
            .rsplit([':', '='])
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        if cleaned.len() != algorithm.hex_len() {
            bail!(
                "{} digest must be {} hex characters, got {}",
                algorithm.label(),
                algorithm.hex_len(),
                cleaned.len()
            );
        }
        if !cleaned.chars().all(|c| c.is_ascii_hexdigit()) {
            bail!("{} digest is not valid hex", algorithm.label());
        }
        Ok(Self {
            algorithm,
            expected: cleaned,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Match { actual: String },
    Mismatch { expected: String, actual: String },
}

impl Outcome {
    pub fn ok(&self) -> bool {
        matches!(self, Outcome::Match { .. })
    }

    pub fn actual(&self) -> &str {
        match self {
            Outcome::Match { actual } | Outcome::Mismatch { actual, .. } => actual,
        }
    }
}

/// Hash a file, reporting progress. Runs on a blocking thread: hashing a large
/// file is CPU-bound and would otherwise stall the runtime.
pub async fn verify(
    path: &Path,
    checksum: Checksum,
    reporter: Reporter,
    cancel: Cancel,
) -> Result<Outcome> {
    let path: PathBuf = path.to_path_buf();
    let total = std::fs::metadata(&path)
        .with_context(|| format!("cannot stat {}", path.display()))?
        .len();

    reporter.emit(Event::VerificationStarted {
        algorithm: checksum.algorithm.as_str().to_string(),
        total_size: total,
    });

    let actual = tokio::task::spawn_blocking({
        let reporter = reporter.clone();
        let algorithm = checksum.algorithm;
        move || hash_file(&path, algorithm, total, &reporter, &cancel)
    })
    .await
    .context("hashing task panicked")??;

    let outcome = if actual == checksum.expected {
        Outcome::Match { actual }
    } else {
        Outcome::Mismatch {
            expected: checksum.expected.clone(),
            actual,
        }
    };

    reporter.emit(Event::VerificationCompleted {
        algorithm: checksum.algorithm.as_str().to_string(),
        ok: outcome.ok(),
        expected: Some(checksum.expected),
        actual: outcome.actual().to_string(),
    });

    Ok(outcome)
}

/// Streaming hash. Buffer is fixed at 1 MiB so memory does not scale with the
/// file (PRD §30).
fn hash_file(
    path: &Path,
    algorithm: Algorithm,
    total: u64,
    reporter: &Reporter,
    cancel: &Cancel,
) -> Result<String> {
    let mut file =
        std::fs::File::open(path).with_context(|| format!("cannot read {}", path.display()))?;
    let mut buf = vec![0u8; 1 << 20];
    let mut hasher = Hasher::new(algorithm);
    let mut read_total = 0u64;
    let mut last_report = std::time::Instant::now();

    loop {
        if cancel.is_cancelled() {
            bail!("verification cancelled");
        }
        let n = file.read(&mut buf).context("read failed while hashing")?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        read_total += n as u64;
        if last_report.elapsed() >= std::time::Duration::from_millis(100) {
            reporter.emit(Event::VerificationProgress {
                bytes: read_total,
                total_size: total,
            });
            last_report = std::time::Instant::now();
        }
    }

    reporter.emit(Event::VerificationProgress {
        bytes: read_total,
        total_size: total,
    });
    Ok(hasher.finalize())
}

enum Hasher {
    Sha256(sha2::Sha256),
    Sha512(sha2::Sha512),
    Blake3(Box<blake3::Hasher>),
}

impl Hasher {
    fn new(algorithm: Algorithm) -> Self {
        use sha2::Digest;
        match algorithm {
            Algorithm::Sha256 => Hasher::Sha256(sha2::Sha256::new()),
            Algorithm::Sha512 => Hasher::Sha512(sha2::Sha512::new()),
            Algorithm::Blake3 => Hasher::Blake3(Box::new(blake3::Hasher::new())),
        }
    }

    fn update(&mut self, data: &[u8]) {
        use sha2::Digest;
        match self {
            Hasher::Sha256(h) => h.update(data),
            Hasher::Sha512(h) => h.update(data),
            Hasher::Blake3(h) => {
                h.update(data);
            }
        }
    }

    fn finalize(self) -> String {
        use sha2::Digest;
        match self {
            Hasher::Sha256(h) => hex(&h.finalize()),
            Hasher::Sha512(h) => hex(&h.finalize()),
            Hasher::Blake3(h) => h.finalize().to_hex().to_string(),
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Hash a byte slice — used by tests and by mirror equivalence checks.
pub fn hash_bytes(algorithm: Algorithm, data: &[u8]) -> String {
    let mut h = Hasher::new(algorithm);
    h.update(data);
    h.finalize()
}

/// Wrap in an `Arc` for sharing with the UI without cloning the digest.
pub type SharedChecksum = Arc<Checksum>;

#[cfg(test)]
mod tests {
    use super::*;

    const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    const ABC_SHA256: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    #[test]
    fn parses_algorithms() {
        assert_eq!("sha256".parse::<Algorithm>().unwrap(), Algorithm::Sha256);
        assert_eq!("SHA-512".parse::<Algorithm>().unwrap(), Algorithm::Sha512);
        assert_eq!("blake3".parse::<Algorithm>().unwrap(), Algorithm::Blake3);
        assert!("md5".parse::<Algorithm>().is_err());
    }

    #[test]
    fn known_digests() {
        assert_eq!(hash_bytes(Algorithm::Sha256, b""), EMPTY_SHA256);
        assert_eq!(hash_bytes(Algorithm::Sha256, b"abc"), ABC_SHA256);
        assert_eq!(
            hash_bytes(Algorithm::Sha512, b"abc"),
            "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a\
             2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
        );
        assert_eq!(
            hash_bytes(Algorithm::Blake3, b"abc"),
            "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"
        );
    }

    #[test]
    fn checksum_parsing_is_forgiving_but_strict() {
        let c = Checksum::parse(
            Algorithm::Sha256,
            &format!("  {}  ", ABC_SHA256.to_uppercase()),
        )
        .unwrap();
        assert_eq!(c.expected, ABC_SHA256);

        let c = Checksum::parse(Algorithm::Sha256, &format!("sha256:{ABC_SHA256}")).unwrap();
        assert_eq!(c.expected, ABC_SHA256);

        // Truncated, over-long and non-hex digests are caught up front.
        assert!(Checksum::parse(Algorithm::Sha256, "abc").is_err());
        assert!(Checksum::parse(Algorithm::Sha256, &"a".repeat(65)).is_err());
        assert!(Checksum::parse(Algorithm::Sha256, &"z".repeat(64)).is_err());
        // Right length, wrong algorithm's length.
        assert!(Checksum::parse(Algorithm::Sha512, ABC_SHA256).is_err());
    }

    #[tokio::test]
    async fn verifies_a_file_and_reports_progress() {
        let dir = std::env::temp_dir().join(format!("rget-integrity-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("data.bin");
        // Larger than the read buffer, so the streaming path is exercised.
        let data: Vec<u8> = (0..(3 << 20)).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &data).unwrap();
        let expected = hash_bytes(Algorithm::Sha256, &data);

        let (reporter, mut rx) = Reporter::new();
        let outcome = verify(
            &path,
            Checksum::parse(Algorithm::Sha256, &expected).unwrap(),
            reporter,
            Cancel::new(),
        )
        .await
        .unwrap();
        assert!(outcome.ok());

        let mut saw_started = false;
        let mut final_progress = 0;
        let mut completed_ok = None;
        while let Ok(ev) = rx.try_recv() {
            match ev {
                Event::VerificationStarted { total_size, .. } => {
                    saw_started = true;
                    assert_eq!(total_size, data.len() as u64);
                }
                Event::VerificationProgress { bytes, .. } => final_progress = bytes,
                Event::VerificationCompleted { ok, .. } => completed_ok = Some(ok),
                _ => {}
            }
        }
        assert!(saw_started);
        assert_eq!(final_progress, data.len() as u64);
        assert_eq!(completed_ok, Some(true));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn mismatch_is_reported_not_swallowed() {
        let dir = std::env::temp_dir().join(format!("rget-integrity-bad-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("data.bin");
        std::fs::write(&path, b"abc").unwrap();

        let outcome = verify(
            &path,
            Checksum::parse(Algorithm::Sha256, EMPTY_SHA256).unwrap(),
            Reporter::silent(),
            Cancel::new(),
        )
        .await
        .unwrap();

        assert!(!outcome.ok());
        match outcome {
            Outcome::Mismatch { expected, actual } => {
                assert_eq!(expected, EMPTY_SHA256);
                assert_eq!(actual, ABC_SHA256);
            }
            Outcome::Match { .. } => panic!("must not report a match"),
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}
