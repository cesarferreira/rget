//! Central SQLite state store (PRD §10, §11).
//!
//! One database for every download, in the platform's data directory — not a
//! sidecar `.part.json` next to each file. That is what makes `rget list`,
//! `rget resume --all` and cross-run recovery possible.
//!
//! Durability rules live in `docs/CRASH_CONSISTENCY.md`. The one that matters
//! here: [`Store::commit_progress`] is the *only* way range progress becomes
//! persistent, it is a single transaction, and the engine calls it only after
//! an `fdatasync` of the destination file.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OptionalExtension, params};

/// Sentinel `end` for a range whose length is unknown (no `Content-Length`).
/// Round-trips through SQLite's i64 columns unlike `u64::MAX`.
pub const OPEN_END: u64 = i64::MAX as u64;

const SCHEMA_VERSION: i64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Pending,
    Downloading,
    Paused,
    Verifying,
    Complete,
    Failed,
}

impl Status {
    pub fn as_str(&self) -> &'static str {
        match self {
            Status::Pending => "pending",
            Status::Downloading => "downloading",
            Status::Paused => "paused",
            Status::Verifying => "verifying",
            Status::Complete => "complete",
            Status::Failed => "failed",
        }
    }

    fn parse(s: &str) -> Status {
        match s {
            "downloading" => Status::Downloading,
            "paused" => Status::Paused,
            "verifying" => Status::Verifying,
            "complete" => Status::Complete,
            "failed" => Status::Failed,
            _ => Status::Pending,
        }
    }

    /// Was this download left mid-flight by a previous process?
    pub fn is_resumable(&self) -> bool {
        matches!(
            self,
            Status::Pending | Status::Downloading | Status::Paused | Status::Failed
        )
    }
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeState {
    Pending,
    Downloading,
    Complete,
    Failed,
}

impl RangeState {
    pub fn as_str(&self) -> &'static str {
        match self {
            RangeState::Pending => "pending",
            RangeState::Downloading => "downloading",
            RangeState::Complete => "complete",
            RangeState::Failed => "failed",
        }
    }

    fn parse(s: &str) -> RangeState {
        match s {
            "downloading" => RangeState::Downloading,
            "complete" => RangeState::Complete,
            "failed" => RangeState::Failed,
            _ => RangeState::Pending,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DownloadRecord {
    pub id: String,
    pub original_url: String,
    pub resolved_url: Option<String>,
    pub mirrors: Vec<String>,
    pub destination: String,
    pub filename: String,
    pub total_size: Option<u64>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_type: Option<String>,
    pub accept_ranges: bool,
    pub expected_checksum: Option<String>,
    pub checksum_algorithm: Option<String>,
    /// Random token minted when the destination file is created. Together with
    /// dev/ino it proves the file on disk is the one we were downloading into.
    pub file_cookie: String,
    pub file_dev: Option<u64>,
    pub file_ino: Option<u64>,
    pub durable_bytes: u64,
    pub status: Status,
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub completed_at: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct RangeRecord {
    pub idx: u64,
    pub start: u64,
    /// Inclusive. [`OPEN_END`] when the total size is unknown.
    pub end: u64,
    pub state: RangeState,
    /// Durable prefix, in bytes, from `start`. Never in-flight bytes.
    pub bytes_written: u64,
}

impl RangeRecord {
    /// Byte length of the range. Named `size` rather than `len` because a range
    /// is never empty, so an `is_empty` counterpart would be meaningless.
    pub fn size(&self) -> u64 {
        self.end.saturating_sub(self.start).saturating_add(1)
    }

    pub fn is_open_ended(&self) -> bool {
        self.end >= OPEN_END
    }

    /// Where a worker picking this range up should ask the server to start.
    pub fn resume_at(&self) -> u64 {
        self.start + self.bytes_written
    }

    pub fn remaining(&self) -> u64 {
        self.size().saturating_sub(self.bytes_written)
    }
}

/// One durable progress update, produced by the committer after its barrier.
#[derive(Debug, Clone, Copy)]
pub struct ProgressUpdate {
    pub idx: u64,
    pub bytes_written: u64,
    pub state: RangeState,
}

pub struct Store {
    conn: Mutex<Connection>,
    path: PathBuf,
}

impl Store {
    /// `$RGET_DB` overrides the location — used by the test suite so tests
    /// never touch a developer's real download list.
    pub fn default_path() -> Result<PathBuf> {
        if let Some(p) = std::env::var_os("RGET_DB") {
            return Ok(PathBuf::from(p));
        }
        let dirs = directories::ProjectDirs::from("", "", "rget")
            .context("cannot determine a data directory for this platform")?;
        Ok(dirs.data_dir().join("downloads.db"))
    }

    pub fn open_default() -> Result<Self> {
        Self::open(&Self::default_path()?)
    }

    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!("cannot create state directory {}", parent.display())
                })?;
            }
        }
        let conn = Connection::open(path)
            .with_context(|| format!("cannot open state database {}", path.display()))?;

        // See docs/CRASH_CONSISTENCY.md for why these exact values.
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;

        let store = Self {
            conn: Mutex::new(conn),
            path: path.to_path_buf(),
        };
        store.migrate()?;
        Ok(store)
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        let store = Self {
            conn: Mutex::new(conn),
            path: PathBuf::from(":memory:"),
        };
        store.migrate()?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn migrate(&self) -> Result<()> {
        let conn = self.lock();
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS downloads (
                id                 TEXT PRIMARY KEY,
                original_url       TEXT NOT NULL,
                resolved_url       TEXT,
                mirrors            TEXT NOT NULL DEFAULT '[]',
                destination        TEXT NOT NULL,
                filename           TEXT NOT NULL,
                total_size         INTEGER,
                etag               TEXT,
                last_modified      TEXT,
                content_type       TEXT,
                accept_ranges      INTEGER NOT NULL DEFAULT 0,
                expected_checksum  TEXT,
                checksum_algorithm TEXT,
                file_cookie        TEXT NOT NULL,
                file_dev           INTEGER,
                file_ino           INTEGER,
                durable_bytes      INTEGER NOT NULL DEFAULT 0,
                status             TEXT NOT NULL,
                error              TEXT,
                created_at         INTEGER NOT NULL,
                updated_at         INTEGER NOT NULL,
                completed_at       INTEGER
            );
            CREATE TABLE IF NOT EXISTS ranges (
                download_id   TEXT NOT NULL REFERENCES downloads(id) ON DELETE CASCADE,
                idx           INTEGER NOT NULL,
                start         INTEGER NOT NULL,
                end           INTEGER NOT NULL,
                state         TEXT NOT NULL,
                bytes_written INTEGER NOT NULL DEFAULT 0,
                updated_at    INTEGER NOT NULL,
                PRIMARY KEY (download_id, idx)
            );
            CREATE INDEX IF NOT EXISTS idx_downloads_dest ON downloads(destination);
            CREATE INDEX IF NOT EXISTS idx_downloads_status ON downloads(status);
            "#,
        )?;
        conn.execute(
            "INSERT INTO meta(key, value) VALUES('schema_version', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value
             WHERE CAST(value AS INTEGER) < CAST(excluded.value AS INTEGER)",
            params![SCHEMA_VERSION.to_string()],
        )?;
        Ok(())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        // A poisoned connection mutex means another thread panicked mid-query.
        // Recovering the guard is correct here: SQLite itself is consistent
        // (transactions are atomic), so the next caller can proceed.
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }

    // -- lookup ------------------------------------------------------------

    pub fn get(&self, id: &str) -> Result<Option<DownloadRecord>> {
        let conn = self.lock();
        let mut stmt = conn.prepare(SELECT_DOWNLOAD)?;
        Ok(stmt.query_row(params![id], row_to_download).optional()?)
    }

    /// Resolve a user-typed short id. Ambiguity is an error, not a coin flip.
    pub fn resolve_id(&self, prefix: &str) -> Result<DownloadRecord> {
        let conn = self.lock();
        let mut stmt = conn.prepare(&format!("{SELECT_DOWNLOAD_ALL} WHERE id LIKE ?1 || '%'"))?;
        let matches: Vec<DownloadRecord> = stmt
            .query_map(params![prefix], row_to_download)?
            .collect::<rusqlite::Result<_>>()?;
        match matches.len() {
            0 => bail!("no download matching id `{prefix}`"),
            1 => Ok(matches.into_iter().next().unwrap()),
            n => {
                let ids: Vec<_> = matches.iter().map(|m| m.id.as_str()).collect();
                bail!("`{prefix}` matches {n} downloads: {}", ids.join(", "))
            }
        }
    }

    /// Find an existing download for this URL landing at this destination.
    /// Matching on both is what makes resume automatic (PRD §5) without
    /// accidentally resuming into a different file.
    pub fn find_for(&self, url: &str, destination: &Path) -> Result<Option<DownloadRecord>> {
        let conn = self.lock();
        let mut stmt = conn.prepare(&format!(
            "{SELECT_DOWNLOAD_ALL} WHERE destination = ?1
               AND (original_url = ?2 OR resolved_url = ?2)
             ORDER BY updated_at DESC LIMIT 1"
        ))?;
        Ok(stmt
            .query_row(params![destination.to_string_lossy(), url], row_to_download)
            .optional()?)
    }

    /// Any download already targeting this destination, regardless of URL —
    /// used to refuse clobbering an unrelated in-flight download.
    pub fn find_by_destination(&self, destination: &Path) -> Result<Option<DownloadRecord>> {
        let conn = self.lock();
        let mut stmt = conn.prepare(&format!(
            "{SELECT_DOWNLOAD_ALL} WHERE destination = ?1 ORDER BY updated_at DESC LIMIT 1"
        ))?;
        Ok(stmt
            .query_row(params![destination.to_string_lossy()], row_to_download)
            .optional()?)
    }

    pub fn list(&self) -> Result<Vec<DownloadRecord>> {
        let conn = self.lock();
        let mut stmt = conn.prepare(&format!("{SELECT_DOWNLOAD_ALL} ORDER BY created_at DESC"))?;
        Ok(stmt
            .query_map([], row_to_download)?
            .collect::<rusqlite::Result<_>>()?)
    }

    pub fn list_resumable(&self) -> Result<Vec<DownloadRecord>> {
        Ok(self
            .list()?
            .into_iter()
            .filter(|d| d.status.is_resumable())
            .collect())
    }

    // -- mutation ----------------------------------------------------------

    pub fn insert(&self, rec: &DownloadRecord) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO downloads (
                id, original_url, resolved_url, mirrors, destination, filename,
                total_size, etag, last_modified, content_type, accept_ranges,
                expected_checksum, checksum_algorithm, file_cookie, file_dev,
                file_ino, durable_bytes, status, error, created_at, updated_at,
                completed_at
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22
             )",
            params![
                rec.id,
                rec.original_url,
                rec.resolved_url,
                serde_json::to_string(&rec.mirrors)?,
                rec.destination,
                rec.filename,
                rec.total_size.map(|v| v as i64),
                rec.etag,
                rec.last_modified,
                rec.content_type,
                rec.accept_ranges as i64,
                rec.expected_checksum,
                rec.checksum_algorithm,
                rec.file_cookie,
                rec.file_dev.map(|v| v as i64),
                rec.file_ino.map(|v| v as i64),
                rec.durable_bytes as i64,
                rec.status.as_str(),
                rec.error,
                rec.created_at,
                rec.updated_at,
                rec.completed_at,
            ],
        )?;
        Ok(())
    }

    /// Refresh the validators and shape we learned from a fresh probe.
    pub fn update_remote_metadata(&self, rec: &DownloadRecord) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "UPDATE downloads SET resolved_url = ?2, total_size = ?3, etag = ?4,
                last_modified = ?5, content_type = ?6, accept_ranges = ?7,
                mirrors = ?8, expected_checksum = ?9, checksum_algorithm = ?10,
                file_dev = ?11, file_ino = ?12, updated_at = ?13
             WHERE id = ?1",
            params![
                rec.id,
                rec.resolved_url,
                rec.total_size.map(|v| v as i64),
                rec.etag,
                rec.last_modified,
                rec.content_type,
                rec.accept_ranges as i64,
                serde_json::to_string(&rec.mirrors)?,
                rec.expected_checksum,
                rec.checksum_algorithm,
                rec.file_dev.map(|v| v as i64),
                rec.file_ino.map(|v| v as i64),
                now(),
            ],
        )?;
        Ok(())
    }

    pub fn set_status(&self, id: &str, status: Status, error: Option<&str>) -> Result<()> {
        let conn = self.lock();
        let completed_at = if status == Status::Complete {
            Some(now())
        } else {
            None
        };
        conn.execute(
            "UPDATE downloads SET status = ?2, error = ?3, updated_at = ?4,
                completed_at = COALESCE(?5, completed_at)
             WHERE id = ?1",
            params![id, status.as_str(), error, now(), completed_at],
        )?;
        Ok(())
    }

    /// Install a fresh range plan, replacing any previous one, atomically.
    pub fn replace_ranges(&self, id: &str, ranges: &[RangeRecord]) -> Result<()> {
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        tx.execute("DELETE FROM ranges WHERE download_id = ?1", params![id])?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO ranges (download_id, idx, start, end, state, bytes_written, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?;
            for r in ranges {
                stmt.execute(params![
                    id,
                    r.idx as i64,
                    r.start as i64,
                    r.end as i64,
                    r.state.as_str(),
                    r.bytes_written as i64,
                    now(),
                ])?;
            }
        }
        let durable: u64 = ranges.iter().map(|r| r.bytes_written).sum();
        tx.execute(
            "UPDATE downloads SET durable_bytes = ?2, updated_at = ?3 WHERE id = ?1",
            params![id, durable as i64, now()],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn load_ranges(&self, id: &str) -> Result<Vec<RangeRecord>> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT idx, start, end, state, bytes_written FROM ranges
             WHERE download_id = ?1 ORDER BY idx",
        )?;
        Ok(stmt
            .query_map(params![id], |row| {
                Ok(RangeRecord {
                    idx: row.get::<_, i64>(0)? as u64,
                    start: row.get::<_, i64>(1)? as u64,
                    end: row.get::<_, i64>(2)? as u64,
                    state: RangeState::parse(&row.get::<_, String>(3)?),
                    bytes_written: row.get::<_, i64>(4)? as u64,
                })
            })?
            .collect::<rusqlite::Result<_>>()?)
    }

    /// The one durable-progress entry point. Called by the committer *after*
    /// `fdatasync` of the destination file, never before.
    ///
    /// One transaction, so a kill at any instruction leaves either the whole
    /// batch or none of it (PRD Invariant 7).
    pub fn commit_progress(&self, id: &str, updates: &[ProgressUpdate]) -> Result<u64> {
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "UPDATE ranges SET bytes_written = ?3, state = ?4, updated_at = ?5
                 WHERE download_id = ?1 AND idx = ?2",
            )?;
            for u in updates {
                stmt.execute(params![
                    id,
                    u.idx as i64,
                    u.bytes_written as i64,
                    u.state.as_str(),
                    now(),
                ])?;
            }
        }
        let durable: i64 = tx.query_row(
            "SELECT COALESCE(SUM(bytes_written), 0) FROM ranges WHERE download_id = ?1",
            params![id],
            |row| row.get(0),
        )?;
        tx.execute(
            "UPDATE downloads SET durable_bytes = ?2, updated_at = ?3 WHERE id = ?1",
            params![id, durable, now()],
        )?;
        tx.commit()?;
        Ok(durable as u64)
    }

    /// Record a split: the victim shrinks and the remainder becomes a new
    /// range, in one transaction so Invariant 3 (no gaps) always holds on disk.
    pub fn apply_split(&self, id: &str, shrunk: RangeRecord, added: RangeRecord) -> Result<()> {
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        tx.execute(
            "UPDATE ranges SET end = ?3, updated_at = ?4 WHERE download_id = ?1 AND idx = ?2",
            params![id, shrunk.idx as i64, shrunk.end as i64, now()],
        )?;
        tx.execute(
            "INSERT INTO ranges (download_id, idx, start, end, state, bytes_written, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(download_id, idx) DO UPDATE SET
                start = excluded.start, end = excluded.end,
                state = excluded.state, bytes_written = excluded.bytes_written,
                updated_at = excluded.updated_at",
            params![
                id,
                added.idx as i64,
                added.start as i64,
                added.end as i64,
                added.state.as_str(),
                added.bytes_written as i64,
                now(),
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Drop all progress for a download but keep its identity (`--restart`).
    pub fn reset(&self, id: &str) -> Result<()> {
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        tx.execute("DELETE FROM ranges WHERE download_id = ?1", params![id])?;
        tx.execute(
            "UPDATE downloads SET durable_bytes = 0, status = ?2, error = NULL,
                completed_at = NULL, updated_at = ?3 WHERE id = ?1",
            params![id, Status::Pending.as_str(), now()],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Forget metadata. Never touches the downloaded file (PRD §20).
    pub fn forget(&self, id: &str) -> Result<()> {
        let conn = self.lock();
        conn.execute("DELETE FROM downloads WHERE id = ?1", params![id])?;
        Ok(())
    }

    /// Mint a short, human-typeable id that is free in this database.
    pub fn mint_id(&self, seed: &str) -> Result<String> {
        for salt in 0..1000u32 {
            let mut hasher = <sha2::Sha256 as sha2::Digest>::new();
            sha2::Digest::update(&mut hasher, seed.as_bytes());
            sha2::Digest::update(&mut hasher, salt.to_le_bytes());
            sha2::Digest::update(&mut hasher, now().to_le_bytes());
            sha2::Digest::update(
                &mut hasher,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.subsec_nanos())
                    .unwrap_or(0)
                    .to_le_bytes(),
            );
            let digest = sha2::Digest::finalize(hasher);
            let id: String = digest[..3].iter().map(|b| format!("{b:02x}")).collect();
            if self.get(&id)?.is_none() {
                return Ok(id);
            }
        }
        bail!("could not allocate a free download id")
    }
}

const SELECT_DOWNLOAD_ALL: &str = "SELECT id, original_url, resolved_url, mirrors, destination,
    filename, total_size, etag, last_modified, content_type, accept_ranges,
    expected_checksum, checksum_algorithm, file_cookie, file_dev, file_ino,
    durable_bytes, status, error, created_at, updated_at, completed_at
    FROM downloads";

const SELECT_DOWNLOAD: &str = "SELECT id, original_url, resolved_url, mirrors, destination,
    filename, total_size, etag, last_modified, content_type, accept_ranges,
    expected_checksum, checksum_algorithm, file_cookie, file_dev, file_ino,
    durable_bytes, status, error, created_at, updated_at, completed_at
    FROM downloads WHERE id = ?1";

fn row_to_download(row: &rusqlite::Row<'_>) -> rusqlite::Result<DownloadRecord> {
    let mirrors: String = row.get(3)?;
    Ok(DownloadRecord {
        id: row.get(0)?,
        original_url: row.get(1)?,
        resolved_url: row.get(2)?,
        mirrors: serde_json::from_str(&mirrors).unwrap_or_default(),
        destination: row.get(4)?,
        filename: row.get(5)?,
        total_size: row.get::<_, Option<i64>>(6)?.map(|v| v as u64),
        etag: row.get(7)?,
        last_modified: row.get(8)?,
        content_type: row.get(9)?,
        accept_ranges: row.get::<_, i64>(10)? != 0,
        expected_checksum: row.get(11)?,
        checksum_algorithm: row.get(12)?,
        file_cookie: row.get(13)?,
        file_dev: row.get::<_, Option<i64>>(14)?.map(|v| v as u64),
        file_ino: row.get::<_, Option<i64>>(15)?.map(|v| v as u64),
        durable_bytes: row.get::<_, i64>(16)? as u64,
        status: Status::parse(&row.get::<_, String>(17)?),
        error: row.get(18)?,
        created_at: row.get(19)?,
        updated_at: row.get(20)?,
        completed_at: row.get(21)?,
    })
}

pub fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// A random 128-bit token, used as the destination file's identity cookie.
pub fn mint_cookie() -> String {
    let mut hasher = <sha2::Sha256 as sha2::Digest>::new();
    sha2::Digest::update(
        &mut hasher,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
            .to_le_bytes(),
    );
    sha2::Digest::update(&mut hasher, std::process::id().to_le_bytes());
    // Stack address varies per run under ASLR; cheap extra entropy.
    let local = 0u8;
    sha2::Digest::update(&mut hasher, (&local as *const u8 as usize).to_le_bytes());
    let digest = sha2::Digest::finalize(hasher);
    digest[..16].iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: &str, dest: &str) -> DownloadRecord {
        DownloadRecord {
            id: id.to_string(),
            original_url: "https://example.com/f.iso".into(),
            resolved_url: Some("https://cdn.example.com/f.iso".into()),
            mirrors: vec!["https://m2.example.com/f.iso".into()],
            destination: dest.into(),
            filename: "f.iso".into(),
            total_size: Some(1000),
            etag: Some("\"abc\"".into()),
            last_modified: None,
            content_type: Some("application/octet-stream".into()),
            accept_ranges: true,
            expected_checksum: None,
            checksum_algorithm: None,
            file_cookie: mint_cookie(),
            file_dev: Some(1),
            file_ino: Some(2),
            durable_bytes: 0,
            status: Status::Downloading,
            error: None,
            created_at: now(),
            updated_at: now(),
            completed_at: None,
        }
    }

    fn plan() -> Vec<RangeRecord> {
        vec![
            RangeRecord {
                idx: 0,
                start: 0,
                end: 499,
                state: RangeState::Pending,
                bytes_written: 0,
            },
            RangeRecord {
                idx: 1,
                start: 500,
                end: 999,
                state: RangeState::Pending,
                bytes_written: 0,
            },
        ]
    }

    #[test]
    fn round_trips_a_download() {
        let s = Store::open_in_memory().unwrap();
        let rec = record("aa11bb", "/tmp/f.iso");
        s.insert(&rec).unwrap();
        let got = s.get("aa11bb").unwrap().unwrap();
        assert_eq!(got.original_url, rec.original_url);
        assert_eq!(got.mirrors, rec.mirrors);
        assert_eq!(got.total_size, Some(1000));
        assert!(got.accept_ranges);
        assert_eq!(got.status, Status::Downloading);
    }

    #[test]
    fn resolves_id_prefixes() {
        let s = Store::open_in_memory().unwrap();
        s.insert(&record("aa11bb", "/tmp/a")).unwrap();
        s.insert(&record("aa22cc", "/tmp/b")).unwrap();
        assert_eq!(s.resolve_id("aa11").unwrap().id, "aa11bb");
        // Ambiguous prefixes must fail loudly rather than pick one.
        let err = s.resolve_id("aa").unwrap_err().to_string();
        assert!(err.contains("matches 2"), "{err}");
        assert!(s.resolve_id("zz").is_err());
    }

    #[test]
    fn finds_by_url_and_destination() {
        let s = Store::open_in_memory().unwrap();
        s.insert(&record("aa11bb", "/tmp/f.iso")).unwrap();
        assert!(
            s.find_for("https://example.com/f.iso", Path::new("/tmp/f.iso"))
                .unwrap()
                .is_some()
        );
        // Resolved URL also matches, so a redirect chain still resumes.
        assert!(
            s.find_for("https://cdn.example.com/f.iso", Path::new("/tmp/f.iso"))
                .unwrap()
                .is_some()
        );
        // Same URL, different destination is a different download.
        assert!(
            s.find_for("https://example.com/f.iso", Path::new("/tmp/other.iso"))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn commit_progress_sums_durable_bytes() {
        let s = Store::open_in_memory().unwrap();
        s.insert(&record("aa11bb", "/tmp/f.iso")).unwrap();
        s.replace_ranges("aa11bb", &plan()).unwrap();

        let durable = s
            .commit_progress(
                "aa11bb",
                &[
                    ProgressUpdate {
                        idx: 0,
                        bytes_written: 500,
                        state: RangeState::Complete,
                    },
                    ProgressUpdate {
                        idx: 1,
                        bytes_written: 100,
                        state: RangeState::Downloading,
                    },
                ],
            )
            .unwrap();
        assert_eq!(durable, 600);
        assert_eq!(s.get("aa11bb").unwrap().unwrap().durable_bytes, 600);

        let ranges = s.load_ranges("aa11bb").unwrap();
        assert_eq!(ranges[0].state, RangeState::Complete);
        assert_eq!(ranges[1].bytes_written, 100);
        assert_eq!(ranges[1].resume_at(), 600);
    }

    #[test]
    fn split_is_atomic_and_leaves_no_gap() {
        let s = Store::open_in_memory().unwrap();
        s.insert(&record("aa11bb", "/tmp/f.iso")).unwrap();
        s.replace_ranges("aa11bb", &plan()).unwrap();

        s.apply_split(
            "aa11bb",
            RangeRecord {
                idx: 1,
                start: 500,
                end: 699,
                state: RangeState::Downloading,
                bytes_written: 100,
            },
            RangeRecord {
                idx: 2,
                start: 700,
                end: 999,
                state: RangeState::Pending,
                bytes_written: 0,
            },
        )
        .unwrap();

        let ranges = s.load_ranges("aa11bb").unwrap();
        assert_eq!(ranges.len(), 3);
        let mut cursor = 0;
        for r in &ranges {
            assert_eq!(r.start, cursor, "gap or overlap before range {}", r.idx);
            cursor = r.end + 1;
        }
        assert_eq!(cursor, 1000);
    }

    #[test]
    fn forget_removes_ranges_but_reset_keeps_the_row() {
        let s = Store::open_in_memory().unwrap();
        s.insert(&record("aa11bb", "/tmp/f.iso")).unwrap();
        s.replace_ranges("aa11bb", &plan()).unwrap();

        s.reset("aa11bb").unwrap();
        assert!(s.load_ranges("aa11bb").unwrap().is_empty());
        assert_eq!(s.get("aa11bb").unwrap().unwrap().status, Status::Pending);

        s.replace_ranges("aa11bb", &plan()).unwrap();
        s.forget("aa11bb").unwrap();
        assert!(s.get("aa11bb").unwrap().is_none());
        // FK cascade cleaned the orphans up.
        assert!(s.load_ranges("aa11bb").unwrap().is_empty());
    }

    #[test]
    fn lists_only_resumable() {
        let s = Store::open_in_memory().unwrap();
        let mut a = record("aa11bb", "/tmp/a");
        a.status = Status::Complete;
        let mut b = record("bb22cc", "/tmp/b");
        b.status = Status::Paused;
        s.insert(&a).unwrap();
        s.insert(&b).unwrap();
        let resumable = s.list_resumable().unwrap();
        assert_eq!(resumable.len(), 1);
        assert_eq!(resumable[0].id, "bb22cc");
        assert_eq!(s.list().unwrap().len(), 2);
    }

    #[test]
    fn ids_and_cookies_are_distinct() {
        let s = Store::open_in_memory().unwrap();
        let a = s.mint_id("https://example.com/x").unwrap();
        s.insert(&record(&a, "/tmp/a")).unwrap();
        let b = s.mint_id("https://example.com/x").unwrap();
        assert_ne!(a, b);
        assert_eq!(a.len(), 6);
        assert_ne!(mint_cookie(), mint_cookie());
        assert_eq!(mint_cookie().len(), 32);
    }

    #[test]
    fn open_end_ranges_round_trip() {
        let s = Store::open_in_memory().unwrap();
        s.insert(&record("aa11bb", "/tmp/f.iso")).unwrap();
        s.replace_ranges(
            "aa11bb",
            &[RangeRecord {
                idx: 0,
                start: 0,
                end: OPEN_END,
                state: RangeState::Pending,
                bytes_written: 0,
            }],
        )
        .unwrap();
        let r = s.load_ranges("aa11bb").unwrap();
        assert!(r[0].is_open_ended());
    }

    #[test]
    fn survives_reopen() {
        let dir = std::env::temp_dir().join(format!("rget-store-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("downloads.db");
        {
            let s = Store::open(&path).unwrap();
            s.insert(&record("aa11bb", "/tmp/f.iso")).unwrap();
            s.replace_ranges("aa11bb", &plan()).unwrap();
            s.commit_progress(
                "aa11bb",
                &[ProgressUpdate {
                    idx: 0,
                    bytes_written: 123,
                    state: RangeState::Downloading,
                }],
            )
            .unwrap();
        }
        {
            let s = Store::open(&path).unwrap();
            assert_eq!(s.get("aa11bb").unwrap().unwrap().durable_bytes, 123);
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}
