//! A deliberately badly-behaved HTTP server for testing (PRD §32).
//!
//! Hand-rolled on a `TcpListener` rather than built on a web framework, because
//! most of what we need to test is a framework's job to prevent: lying about
//! `Content-Length`, malformed `Content-Range`, hanging up mid-body, ignoring
//! `Range`, dribbling bytes out slowly.
//!
//! Every response sets `Connection: close`, which keeps the parser trivial and
//! makes "kill the connection" the natural default rather than a special case.

#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Deterministic pseudo-random content, so every test can assert on a checksum.
pub fn test_body(len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut x: u32 = 0x12345678;
    for _ in 0..len {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        out.push((x & 0xff) as u8);
    }
    out
}

pub fn sha256(data: &[u8]) -> String {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(data);
    let d = h.finalize();
    d.iter().map(|b| format!("{b:02x}")).collect()
}

#[derive(Clone)]
pub struct Config {
    pub body: Vec<u8>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_type: Option<String>,
    pub content_disposition: Option<String>,
    /// Advertise and honour `Range`.
    pub accept_ranges: bool,
    /// Accept the header then serve the whole body anyway, like a broken CDN.
    pub ignore_range: bool,
    /// Send a `Content-Range` that cannot be parsed.
    pub malformed_content_range: bool,
    /// Answer every request with 412, as if a precondition failed.
    pub precondition_fail: bool,
    /// Fail the next N requests with this status (and optional `Retry-After`).
    pub fail_next: Option<Failure>,
    /// Close the connection after writing this many body bytes.
    pub kill_after: Option<usize>,
    /// Add this to the advertised `Content-Length`, without changing the body.
    pub content_length_delta: i64,
    /// Wait this long before writing any body.
    pub delay_before_body: Option<Duration>,
    /// Write at most `bytes` per `interval`, to make a transfer slow enough to
    /// interrupt reliably.
    pub throttle: Option<(usize, Duration)>,
    /// Redirect anything under `/redirect` this many times before serving.
    pub redirect_hops: usize,
    /// Serve a redirect that points at itself.
    pub redirect_loop: bool,
    /// Refuse to serve at all — for mirror-failure tests.
    pub refuse: bool,
}

#[derive(Clone)]
pub struct Failure {
    pub remaining: usize,
    pub status: u16,
    pub retry_after: Option<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            body: test_body(64 * 1024),
            etag: Some("\"v1\"".to_string()),
            last_modified: Some("Mon, 01 Jan 2024 00:00:00 GMT".to_string()),
            content_type: Some("application/octet-stream".to_string()),
            content_disposition: None,
            accept_ranges: true,
            ignore_range: false,
            malformed_content_range: false,
            precondition_fail: false,
            fail_next: None,
            kill_after: None,
            content_length_delta: 0,
            delay_before_body: None,
            throttle: None,
            redirect_hops: 0,
            redirect_loop: false,
            refuse: false,
        }
    }
}

impl Config {
    pub fn with_body(len: usize) -> Self {
        Self {
            body: test_body(len),
            ..Default::default()
        }
    }
}

#[derive(Default)]
pub struct Stats {
    pub requests: usize,
    pub paths: Vec<String>,
    /// Every `Range` header we were sent, parsed.
    pub ranges: Vec<(u64, Option<u64>)>,
    /// Requests that arrived with no `Range` header.
    pub plain_requests: usize,
    pub if_range: Vec<String>,
    pub headers_seen: Vec<HashMap<String, String>>,
    /// Body bytes actually written to clients. The honest measure of whether a
    /// resume re-downloaded work it already had.
    pub bytes_served: usize,
}

pub struct Server {
    pub addr: SocketAddr,
    config: Arc<Mutex<Config>>,
    stats: Arc<Mutex<Stats>>,
}

impl Server {
    pub async fn start(config: Config) -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let config = Arc::new(Mutex::new(config));
        let stats = Arc::new(Mutex::new(Stats::default()));

        tokio::spawn({
            let config = config.clone();
            let stats = stats.clone();
            async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    tokio::spawn(handle(stream, config.clone(), stats.clone()));
                }
            }
        });

        Server {
            addr,
            config,
            stats,
        }
    }

    pub fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }

    pub fn config(&self) -> MutexGuard<'_, Config> {
        self.config.lock().unwrap()
    }

    pub fn stats(&self) -> MutexGuard<'_, Stats> {
        self.stats.lock().unwrap()
    }

    pub fn set(&self, f: impl FnOnce(&mut Config)) {
        f(&mut self.config.lock().unwrap());
    }

    pub fn body(&self) -> Vec<u8> {
        self.config.lock().unwrap().body.clone()
    }

    pub fn request_count(&self) -> usize {
        self.stats.lock().unwrap().requests
    }

    pub fn bytes_served(&self) -> usize {
        self.stats.lock().unwrap().bytes_served
    }

    /// Forget everything recorded so far, so a test can measure one run in
    /// isolation.
    pub fn reset_stats(&self) {
        *self.stats.lock().unwrap() = Stats::default();
    }
}

async fn handle(mut stream: TcpStream, config: Arc<Mutex<Config>>, stats: Arc<Mutex<Stats>>) {
    let Some(request) = read_request(&mut stream).await else {
        return;
    };

    let (method, path, headers) = request;
    let range_header = headers.get("range").cloned();
    {
        let mut s = stats.lock().unwrap();
        s.requests += 1;
        s.paths.push(path.clone());
        s.headers_seen.push(headers.clone());
        match &range_header {
            Some(raw) => {
                if let Some(parsed) = parse_range(raw) {
                    s.ranges.push(parsed);
                }
            }
            None => s.plain_requests += 1,
        }
        if let Some(v) = headers.get("if-range") {
            s.if_range.push(v.clone());
        }
    }

    // Snapshot the config so a test mutating it mid-response cannot tear.
    let cfg = config.lock().unwrap().clone();

    if cfg.refuse {
        let _ = stream.shutdown().await;
        return;
    }

    // Consume one scheduled failure, if any.
    let failure = {
        let mut guard = config.lock().unwrap();
        match &mut guard.fail_next {
            Some(f) if f.remaining > 0 => {
                f.remaining -= 1;
                Some((f.status, f.retry_after))
            }
            _ => None,
        }
    };
    if let Some((status, retry_after)) = failure {
        let mut head = format!(
            "HTTP/1.1 {status} {}\r\nContent-Length: 0\r\nConnection: close\r\n",
            reason(status)
        );
        if let Some(secs) = retry_after {
            head.push_str(&format!("Retry-After: {secs}\r\n"));
        }
        head.push_str("\r\n");
        let _ = stream.write_all(head.as_bytes()).await;
        let _ = stream.shutdown().await;
        return;
    }

    if cfg.redirect_loop {
        let head = format!(
            "HTTP/1.1 302 Found\r\nLocation: {path}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        let _ = stream.write_all(head.as_bytes()).await;
        let _ = stream.shutdown().await;
        return;
    }

    // /redirect/N bounces to /redirect/N-1 and finally to /file.bin.
    if let Some(rest) = path.strip_prefix("/redirect/") {
        let hops: usize = rest.split('/').next().unwrap_or("0").parse().unwrap_or(0);
        let location = if hops > 1 {
            format!("/redirect/{}", hops - 1)
        } else {
            "/final-name.bin".to_string()
        };
        let head = format!(
            "HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        let _ = stream.write_all(head.as_bytes()).await;
        let _ = stream.shutdown().await;
        return;
    }

    if cfg.precondition_fail {
        let head =
            "HTTP/1.1 412 Precondition Failed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        let _ = stream.write_all(head.as_bytes()).await;
        let _ = stream.shutdown().await;
        return;
    }

    let total = cfg.body.len() as u64;

    // RFC 9110: a conditional range whose validator no longer matches must be
    // answered with the whole representation, not a 206.
    let validator_failed = headers
        .get("if-range")
        .is_some_and(|v| cfg.etag.as_deref() != Some(v.as_str()));

    let requested = range_header.as_deref().and_then(parse_range);
    let serve_range = cfg.accept_ranges && !cfg.ignore_range && !validator_failed;

    let (status, start, end) = match (serve_range, requested) {
        (true, Some((start, end))) => {
            if start >= total {
                let head = format!(
                    "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{total}\r\n\
                     Content-Length: 0\r\nConnection: close\r\n\r\n"
                );
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.shutdown().await;
                return;
            }
            let end = end.unwrap_or(total - 1).min(total - 1);
            (206u16, start, end)
        }
        _ => (200u16, 0, total.saturating_sub(1)),
    };

    let slice: &[u8] = if total == 0 {
        &[]
    } else {
        &cfg.body[start as usize..=end as usize]
    };
    let declared = (slice.len() as i64 + cfg.content_length_delta).max(0);

    let mut head = format!("HTTP/1.1 {status} {}\r\n", reason(status));
    head.push_str(&format!("Content-Length: {declared}\r\n"));
    if cfg.accept_ranges {
        head.push_str("Accept-Ranges: bytes\r\n");
    }
    if status == 206 {
        if cfg.malformed_content_range {
            head.push_str("Content-Range: bytes not-a-range\r\n");
        } else {
            head.push_str(&format!("Content-Range: bytes {start}-{end}/{total}\r\n"));
        }
    }
    if let Some(etag) = &cfg.etag {
        head.push_str(&format!("ETag: {etag}\r\n"));
    }
    if let Some(lm) = &cfg.last_modified {
        head.push_str(&format!("Last-Modified: {lm}\r\n"));
    }
    if let Some(ct) = &cfg.content_type {
        head.push_str(&format!("Content-Type: {ct}\r\n"));
    }
    if let Some(cd) = &cfg.content_disposition {
        head.push_str(&format!("Content-Disposition: {cd}\r\n"));
    }
    head.push_str("Connection: close\r\n\r\n");

    if stream.write_all(head.as_bytes()).await.is_err() {
        return;
    }
    if method == "HEAD" {
        let _ = stream.shutdown().await;
        return;
    }

    if let Some(delay) = cfg.delay_before_body {
        tokio::time::sleep(delay).await;
    }

    let limit = cfg.kill_after.unwrap_or(slice.len()).min(slice.len());
    let body = &slice[..limit];

    match cfg.throttle {
        Some((per_tick, interval)) => {
            for chunk in body.chunks(per_tick.max(1)) {
                if stream.write_all(chunk).await.is_err() {
                    return;
                }
                stats.lock().unwrap().bytes_served += chunk.len();
                let _ = stream.flush().await;
                tokio::time::sleep(interval).await;
            }
        }
        None => {
            if stream.write_all(body).await.is_err() {
                return;
            }
            stats.lock().unwrap().bytes_served += body.len();
        }
    }

    if cfg.kill_after.is_some() {
        // Hang up with the promised bytes still outstanding: dropping the
        // stream here is exactly the "connection died mid-range" case.
        return;
    }
    let _ = stream.flush().await;
    let _ = stream.shutdown().await;
}

async fn read_request(stream: &mut TcpStream) -> Option<(String, String, HashMap<String, String>)> {
    let mut buf = Vec::with_capacity(1024);
    let mut byte = [0u8; 1];
    loop {
        match stream.read(&mut byte).await {
            Ok(0) => return None,
            Ok(_) => buf.push(byte[0]),
            Err(_) => return None,
        }
        if buf.ends_with(b"\r\n\r\n") {
            break;
        }
        if buf.len() > 64 * 1024 {
            return None;
        }
    }

    let text = String::from_utf8_lossy(&buf).to_string();
    let mut lines = text.split("\r\n");
    let first = lines.next()?;
    let mut parts = first.split_whitespace();
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();

    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    Some((method, path, headers))
}

/// `bytes=0-1023` / `bytes=1024-` → `(start, end)`.
fn parse_range(raw: &str) -> Option<(u64, Option<u64>)> {
    let spec = raw.trim().strip_prefix("bytes=")?;
    let (start, end) = spec.split_once('-')?;
    let start: u64 = start.trim().parse().ok()?;
    let end = match end.trim() {
        "" => None,
        v => Some(v.parse::<u64>().ok()?),
    };
    Some((start, end))
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        206 => "Partial Content",
        302 => "Found",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        408 => "Request Timeout",
        412 => "Precondition Failed",
        416 => "Range Not Satisfiable",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}

// -- shared test scaffolding ----------------------------------------------

/// A scratch directory plus its own state database, so tests never touch the
/// developer's real download list and never collide with each other.
pub struct Workspace {
    pub dir: std::path::PathBuf,
}

impl Workspace {
    pub fn new(name: &str) -> Workspace {
        static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("rget-it-{name}-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create workspace");
        Workspace { dir }
    }

    pub fn db(&self) -> std::path::PathBuf {
        self.dir.join("state.db")
    }

    pub fn path(&self, name: &str) -> std::path::PathBuf {
        self.dir.join(name)
    }

    pub fn store(&self) -> rget::storage::Store {
        rget::storage::Store::open(&self.db()).expect("open store")
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
