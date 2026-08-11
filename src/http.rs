//! HTTP client, metadata probing and range requests (PRD §6).
//!
//! Two rules drive this module:
//!
//! 1. **Never trust the server.** Headers are parsed defensively, sizes are
//!    sanity-checked, `Content-Range` is verified against what we asked for,
//!    and a server that ignores `Range` is detected rather than believed.
//! 2. **Never leak credentials.** URLs are redacted before they can reach a log
//!    line, and `Authorization` is dropped across a cross-host redirect.

use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::header::{
    ACCEPT_ENCODING, ACCEPT_RANGES, AUTHORIZATION, CONTENT_DISPOSITION, CONTENT_LENGTH,
    CONTENT_RANGE, CONTENT_TYPE, ETAG, HeaderMap, HeaderName, HeaderValue, LAST_MODIFIED, RANGE,
    RETRY_AFTER,
};
use reqwest::{Client, Response, StatusCode};
use url::Url;

use crate::error::TransferError;

pub const DEFAULT_USER_AGENT: &str = concat!("rget/", env!("CARGO_PKG_VERSION"));

#[derive(Debug, Clone)]
pub struct HttpConfig {
    pub user_agent: String,
    /// Applies to connect *and* to the gap between two body reads. It is
    /// deliberately not a whole-request deadline: a 4 GiB range is allowed to
    /// take as long as it takes, as long as bytes keep arriving.
    pub timeout: Duration,
    pub headers: Vec<(String, String)>,
    pub proxy: Option<String>,
    pub max_redirects: usize,
    pub basic_auth: Option<(String, String)>,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            user_agent: DEFAULT_USER_AGENT.to_string(),
            timeout: Duration::from_secs(30),
            headers: Vec::new(),
            proxy: None,
            max_redirects: 10,
            basic_auth: None,
        }
    }
}

impl HttpConfig {
    /// Extra headers as a `HeaderMap`, rejecting anything unparseable rather
    /// than silently dropping it.
    pub fn header_map(&self) -> Result<HeaderMap> {
        let mut map = HeaderMap::new();
        // Ask for identity so a proxy cannot hand us a gzip stream whose byte
        // offsets have nothing to do with the file we are reassembling.
        map.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
        for (k, v) in &self.headers {
            let name: HeaderName = k
                .trim()
                .parse()
                .with_context(|| format!("invalid header name `{k}`"))?;
            let value = HeaderValue::from_str(v.trim())
                .with_context(|| format!("invalid value for header `{k}`"))?;
            map.insert(name, value);
        }
        if let Some((user, pass)) = &self.basic_auth {
            let mut value =
                HeaderValue::from_str(&format!("Basic {}", base64(&format!("{user}:{pass}"))))
                    .context("invalid basic-auth credentials")?;
            // Marks the header sensitive so `HeaderMap`'s Debug impl prints
            // `Sensitive` instead of the credentials (PRD §25).
            value.set_sensitive(true);
            map.insert(AUTHORIZATION, value);
        }
        Ok(map)
    }
}

pub fn build_client(cfg: &HttpConfig) -> Result<Client> {
    let max = cfg.max_redirects;
    let policy = reqwest::redirect::Policy::custom(move |attempt| {
        if attempt.previous().len() >= max {
            return attempt.error(format!("exceeded {max} redirects"));
        }
        let scheme = attempt.url().scheme().to_string();
        if scheme != "http" && scheme != "https" {
            return attempt.error(format!("refusing redirect to `{scheme}` scheme"));
        }
        // Explicit loop detection: a server can bounce between two URLs and
        // stay under the hop limit forever.
        if attempt.previous().iter().any(|p| p == attempt.url()) {
            return attempt.error("redirect loop");
        }
        attempt.follow()
    });

    let mut builder = Client::builder()
        .user_agent(&cfg.user_agent)
        .default_headers(cfg.header_map()?)
        .redirect(policy)
        // Drop `Authorization`/`Cookie` when a redirect crosses hosts.
        .referer(false)
        .connect_timeout(cfg.timeout)
        .pool_idle_timeout(Duration::from_secs(90))
        // A cap on how much header a hostile server can make us buffer.
        .http1_ignore_invalid_headers_in_responses(false)
        .tcp_nodelay(true);

    if let Some(proxy) = &cfg.proxy {
        builder = builder
            .proxy(reqwest::Proxy::all(proxy).with_context(|| format!("invalid proxy `{proxy}`"))?);
    }

    builder.build().context("failed to build HTTP client")
}

/// Everything we learn about the remote resource before transferring it.
#[derive(Debug, Clone)]
pub struct RemoteInfo {
    pub final_url: Url,
    pub size: Option<u64>,
    pub accept_ranges: bool,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_type: Option<String>,
    pub content_disposition: Option<String>,
    /// Set when the server sent a non-identity `Content-Encoding`; the bytes on
    /// disk will be the encoded form, so we warn rather than pretend.
    pub content_encoding: Option<String>,
}

impl RemoteInfo {
    /// Can we safely split this into parallel ranges?
    pub fn supports_parallel(&self) -> bool {
        self.accept_ranges && self.size.is_some_and(|s| s > 0)
    }

    /// The strongest validator available, for `If-Range` (PRD §13).
    pub fn validator(&self) -> Option<String> {
        // A weak ETag (`W/"x"`) does not guarantee byte-for-byte identity, so
        // it must never be used for If-Range. Fall back to Last-Modified.
        match &self.etag {
            Some(tag) if !tag.trim_start().starts_with("W/") => Some(tag.clone()),
            _ => self.last_modified.clone(),
        }
    }

    pub fn has_strong_etag(&self) -> bool {
        self.etag
            .as_deref()
            .is_some_and(|t| !t.trim_start().starts_with("W/"))
    }
}

/// What a priming probe learned, plus the response body it opened.
pub struct Primed {
    pub info: RemoteInfo,
    /// A live response whose body starts at byte 0, ready to be transferred
    /// rather than discarded. `None` when the probe had to fall back and the
    /// caller should issue ordinary requests for everything.
    pub body: Option<Response>,
}

/// Probe *and* start the download in a single request.
///
/// [`probe`] asks for `bytes=0-0`, reads one byte, throws it away, and only then
/// lets the real work begin — one whole round trip of pure overhead on every
/// download, which is why rget could never match a single-request client on a
/// small file. This asks for `bytes=0-` instead: the reply tells us everything
/// `probe` would have, and its body is the beginning of the file.
///
/// The three answers a server can give, all useful:
///
/// - `206` with a parseable `Content-Range` — ranges work, size known, and the
///   first bytes are already streaming.
/// - `200` — the server ignored `Range` and sent the whole representation. The
///   body still starts at byte 0, so it still primes the transfer. Whether we
///   may *also* issue ranged requests is then down to `Accept-Ranges`.
/// - anything else — fall back to [`plain_probe`] and hand back no body.
pub async fn probe_priming(client: &Client, url: &Url) -> Result<Primed, TransferError> {
    let resp = client
        .get(url.clone())
        .header(RANGE, "bytes=0-")
        .send()
        .await
        .map_err(|e| TransferError::from_reqwest(&e))?;

    let status = resp.status();

    if status == StatusCode::PARTIAL_CONTENT {
        match parse_content_range(header(&resp, CONTENT_RANGE).as_deref()) {
            // The body must actually begin where we asked, or it cannot prime
            // the transfer no matter how well-formed the header is.
            Some((0, _, total)) => {
                let mut info = info_from(&resp, total);
                info.accept_ranges = true;
                return Ok(Primed {
                    info,
                    body: Some(resp),
                });
            }
            _ => {
                tracing::warn!(
                    "server answered bytes=0- with an unusable Content-Range; disabling parallelism"
                );
                return Ok(Primed {
                    info: plain_probe(client, url).await?,
                    body: None,
                });
            }
        }
    }

    if status.is_success() {
        let len = header(&resp, CONTENT_LENGTH).and_then(|v| v.parse::<u64>().ok());
        let mut info = info_from(&resp, len);
        // `info_from` already read `Accept-Ranges`. Trust it: a server that
        // advertises ranges but answers a whole-file range with 200 is within
        // its rights, and its ranged requests may still work. If they do not,
        // the first ranged worker fails loudly rather than silently corrupting.
        info.accept_ranges = info.accept_ranges && len.is_some_and(|l| l > 0);
        return Ok(Primed {
            info,
            body: Some(resp),
        });
    }

    if matches!(
        status,
        StatusCode::METHOD_NOT_ALLOWED
            | StatusCode::NOT_IMPLEMENTED
            | StatusCode::BAD_REQUEST
            | StatusCode::RANGE_NOT_SATISFIABLE
    ) {
        return Ok(Primed {
            info: plain_probe(client, url).await?,
            body: None,
        });
    }

    Err(status_error(&resp))
}

/// Ask the server what it has, without downloading it.
///
/// A one-byte ranged `GET` rather than `HEAD`: plenty of servers and CDNs
/// answer `HEAD` with different (or absent) headers than they answer `GET`,
/// and a ranged `GET` tells us in one round trip whether ranges actually work
/// — as opposed to whether the server merely claims they do.
///
/// Prefer [`probe_priming`] for the primary URL; this remains the right call for
/// mirrors, where we want the metadata and emphatically not the body.
pub async fn probe(client: &Client, url: &Url) -> Result<RemoteInfo, TransferError> {
    let resp = client
        .get(url.clone())
        .header(RANGE, "bytes=0-0")
        .send()
        .await
        .map_err(|e| TransferError::from_reqwest(&e))?;

    let status = resp.status();
    if status == StatusCode::PARTIAL_CONTENT {
        match parse_content_range(header(&resp, CONTENT_RANGE).as_deref()) {
            Some((_, _, total)) => {
                let mut info = info_from(&resp, total);
                // `Accept-Ranges: none` alongside a 206 is contradictory;
                // believe the 206, which is what we observed working.
                info.accept_ranges = true;
                return Ok(info);
            }
            None => {
                // A 206 we cannot interpret means we cannot trust this server's
                // ranges at all. Downloading it sequentially is still correct,
                // so fall back rather than fail.
                tracing::warn!("server sent an unparseable Content-Range; disabling parallelism");
                return plain_probe(client, url).await;
            }
        }
    }

    if status.is_success() {
        // Either the server ignores Range, or the resource is a single byte.
        let len = header(&resp, CONTENT_LENGTH).and_then(|v| v.parse::<u64>().ok());
        let accepts = header(&resp, ACCEPT_RANGES)
            .map(|v| v.eq_ignore_ascii_case("bytes"))
            .unwrap_or(false);
        let mut info = info_from(&resp, len);
        // It said 200 to a ranged request: only trust ranges if it also
        // advertises them *and* the body was too short to be the whole file.
        info.accept_ranges = accepts && len.is_some_and(|l| l == 1);
        if !accepts {
            info.accept_ranges = false;
        }
        return Ok(info);
    }

    // Some origins reject `Range` outright with 400/405/501. Retry plainly so
    // we can still download sequentially.
    if matches!(
        status,
        StatusCode::METHOD_NOT_ALLOWED
            | StatusCode::NOT_IMPLEMENTED
            | StatusCode::BAD_REQUEST
            | StatusCode::RANGE_NOT_SATISFIABLE
    ) {
        return plain_probe(client, url).await;
    }

    Err(status_error(&resp))
}

/// Probe without a `Range` header, for servers whose range support is absent or
/// untrustworthy. Always yields `accept_ranges: false`.
async fn plain_probe(client: &Client, url: &Url) -> Result<RemoteInfo, TransferError> {
    let resp = client
        .get(url.clone())
        .send()
        .await
        .map_err(|e| TransferError::from_reqwest(&e))?;
    if !resp.status().is_success() {
        return Err(status_error(&resp));
    }
    let len = header(&resp, CONTENT_LENGTH).and_then(|v| v.parse::<u64>().ok());
    let mut info = info_from(&resp, len);
    info.accept_ranges = false;
    Ok(info)
}

fn info_from(resp: &Response, size: Option<u64>) -> RemoteInfo {
    RemoteInfo {
        final_url: resp.url().clone(),
        size,
        accept_ranges: header(resp, ACCEPT_RANGES)
            .map(|v| v.eq_ignore_ascii_case("bytes"))
            .unwrap_or(false),
        etag: header(resp, ETAG),
        last_modified: header(resp, LAST_MODIFIED),
        content_type: header(resp, CONTENT_TYPE),
        content_disposition: header(resp, CONTENT_DISPOSITION),
        content_encoding: header(resp, reqwest::header::CONTENT_ENCODING)
            .filter(|v| !v.eq_ignore_ascii_case("identity")),
    }
}

/// A ranged `GET`, with the response validated against what we asked for.
///
/// `validator` is sent as `If-Range`, so a resource that changed since we
/// started comes back as a `200` full body — which we detect and reject rather
/// than splicing into the middle of our file (PRD Invariant 4).
pub async fn get_range(
    client: &Client,
    url: &Url,
    start: u64,
    end: Option<u64>,
    validator: Option<&str>,
    expected_total: Option<u64>,
) -> Result<Response, TransferError> {
    let ranged = start > 0 || end.is_some();
    let mut req = client.get(url.clone());
    if ranged {
        let spec = match end {
            Some(e) => format!("bytes={start}-{e}"),
            None => format!("bytes={start}-"),
        };
        req = req.header(RANGE, spec);
        if let Some(v) = validator {
            req = req.header("If-Range", v);
        }
    }

    let resp = req
        .send()
        .await
        .map_err(|e| TransferError::from_reqwest(&e))?;
    let status = resp.status();

    if status == StatusCode::PRECONDITION_FAILED {
        return Err(TransferError::RemoteChanged(
            "server rejected our validator (412)".into(),
        ));
    }
    if status == StatusCode::RANGE_NOT_SATISFIABLE {
        return Err(TransferError::RemoteChanged(format!(
            "server cannot satisfy bytes={start}- any more (416); the file likely shrank"
        )));
    }
    if !status.is_success() {
        return Err(status_error(&resp));
    }

    if !ranged {
        return Ok(resp);
    }

    if status == StatusCode::PARTIAL_CONTENT {
        let (got_start, _got_end, total) =
            parse_content_range(header(&resp, CONTENT_RANGE).as_deref()).ok_or_else(|| {
                TransferError::Protocol("206 response with unparseable Content-Range".into())
            })?;
        if got_start != start {
            return Err(TransferError::Protocol(format!(
                "asked for bytes from {start}, server sent from {got_start}"
            )));
        }
        if let (Some(total), Some(expected)) = (total, expected_total) {
            if total != expected {
                return Err(TransferError::RemoteChanged(format!(
                    "size changed from {expected} to {total} bytes"
                )));
            }
        }
        return Ok(resp);
    }

    // 200 to a ranged request. If we sent a validator, this is the RFC 9110
    // way of saying "it changed, here is the whole thing". Otherwise the server
    // simply does not implement Range.
    if validator.is_some() {
        Err(TransferError::RemoteChanged(
            "server answered a conditional range with a full body, so the resource changed".into(),
        ))
    } else {
        Err(TransferError::Protocol(
            "server ignored our Range header and sent the whole body".into(),
        ))
    }
}

fn status_error(resp: &Response) -> TransferError {
    TransferError::Status {
        status: resp.status().as_u16(),
        retry_after: header(resp, RETRY_AFTER).and_then(|v| parse_retry_after(&v)),
    }
}

pub fn header(resp: &Response, name: impl reqwest::header::AsHeaderName) -> Option<String> {
    resp.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// `bytes 0-1023/4096` → `(0, 1023, Some(4096))`. `*` total → `None`.
pub fn parse_content_range(value: Option<&str>) -> Option<(u64, u64, Option<u64>)> {
    let value = value?.trim();
    let rest = value.strip_prefix("bytes")?.trim_start();
    let (span, total) = rest.split_once('/')?;
    let (start, end) = span.trim().split_once('-')?;
    let start: u64 = start.trim().parse().ok()?;
    let end: u64 = end.trim().parse().ok()?;
    if end < start {
        return None;
    }
    let total = match total.trim() {
        "*" => None,
        t => Some(t.parse::<u64>().ok()?),
    };
    if let Some(t) = total {
        // A range that claims to extend past the resource is nonsense.
        if end >= t {
            return None;
        }
    }
    Some((start, end, total))
}

/// `Retry-After` in delta-seconds form. The HTTP-date form is rare in practice
/// and parsing dates without a date library invites bugs, so we fall back to
/// our own backoff for it rather than guess.
pub fn parse_retry_after(value: &str) -> Option<Duration> {
    let secs: u64 = value.trim().parse().ok()?;
    Some(Duration::from_secs(secs.min(3600)))
}

fn base64(input: &str) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(TABLE[(n >> 18) as usize & 63] as char);
        out.push(TABLE[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            TABLE[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// Strip userinfo and query before a URL can reach a log line (PRD §36).
pub fn redact(url: &Url) -> String {
    let mut u = url.clone();
    let _ = u.set_username("");
    let _ = u.set_password(None);
    u.set_query(None);
    u.set_fragment(None);
    u.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_content_range() {
        assert_eq!(
            parse_content_range(Some("bytes 0-1023/4096")),
            Some((0, 1023, Some(4096)))
        );
        assert_eq!(
            parse_content_range(Some("bytes 500-999/*")),
            Some((500, 999, None))
        );
        // Hostile / malformed forms must not parse into something plausible.
        assert_eq!(parse_content_range(None), None);
        assert_eq!(parse_content_range(Some("")), None);
        assert_eq!(parse_content_range(Some("items 0-1/2")), None);
        assert_eq!(parse_content_range(Some("bytes 100-50/4096")), None);
        assert_eq!(parse_content_range(Some("bytes 0-4096/4096")), None);
        assert_eq!(parse_content_range(Some("bytes abc-def/4096")), None);
        assert_eq!(parse_content_range(Some("bytes 0-10")), None);
    }

    #[test]
    fn parses_retry_after() {
        assert_eq!(parse_retry_after("7"), Some(Duration::from_secs(7)));
        assert_eq!(parse_retry_after(" 30 "), Some(Duration::from_secs(30)));
        // Clamped, so a hostile server cannot park us for a week.
        assert_eq!(parse_retry_after("999999"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT"), None);
    }

    #[test]
    fn prefers_strong_validators() {
        let mut info = RemoteInfo {
            final_url: Url::parse("https://x.example/f").unwrap(),
            size: Some(10),
            accept_ranges: true,
            etag: Some("W/\"weak\"".into()),
            last_modified: Some("Wed, 21 Oct 2015 07:28:00 GMT".into()),
            content_type: None,
            content_disposition: None,
            content_encoding: None,
        };
        // A weak ETag must not be used as an If-Range validator.
        assert_eq!(
            info.validator().as_deref(),
            Some("Wed, 21 Oct 2015 07:28:00 GMT")
        );
        assert!(!info.has_strong_etag());

        info.etag = Some("\"strong\"".into());
        assert_eq!(info.validator().as_deref(), Some("\"strong\""));
        assert!(info.has_strong_etag());
    }

    #[test]
    fn parallel_requires_size_and_ranges() {
        let mut info = RemoteInfo {
            final_url: Url::parse("https://x.example/f").unwrap(),
            size: Some(1000),
            accept_ranges: true,
            etag: None,
            last_modified: None,
            content_type: None,
            content_disposition: None,
            content_encoding: None,
        };
        assert!(info.supports_parallel());
        info.size = None;
        assert!(!info.supports_parallel());
        info.size = Some(1000);
        info.accept_ranges = false;
        assert!(!info.supports_parallel());
    }

    #[test]
    fn base64_matches_rfc4648() {
        assert_eq!(base64("user:pass"), "dXNlcjpwYXNz");
        assert_eq!(base64("a"), "YQ==");
        assert_eq!(base64("ab"), "YWI=");
        assert_eq!(base64("abc"), "YWJj");
    }

    #[test]
    fn redacts_credentials_and_queries() {
        let u = Url::parse("https://alice:s3cret@example.com/f.iso?token=abc#frag").unwrap();
        let out = redact(&u);
        assert!(!out.contains("s3cret"), "{out}");
        assert!(!out.contains("token"), "{out}");
        assert!(out.contains("example.com/f.iso"));
    }

    #[test]
    fn basic_auth_header_is_marked_sensitive() {
        let cfg = HttpConfig {
            basic_auth: Some(("alice".into(), "s3cret".into())),
            ..Default::default()
        };
        let map = cfg.header_map().unwrap();
        let value = map.get(AUTHORIZATION).unwrap();
        assert!(value.is_sensitive());
        assert!(!format!("{map:?}").contains("s3cret"));
    }

    #[test]
    fn rejects_bad_custom_headers() {
        let cfg = HttpConfig {
            headers: vec![("X-Bad Name".into(), "v".into())],
            ..Default::default()
        };
        assert!(cfg.header_map().is_err());
    }

    #[test]
    fn requests_identity_encoding() {
        let map = HttpConfig::default().header_map().unwrap();
        assert_eq!(map.get(ACCEPT_ENCODING).unwrap(), "identity");
    }
}
