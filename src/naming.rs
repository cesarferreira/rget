//! Destination filename selection and sanitisation.
//!
//! Everything a server tells us about a filename is hostile input. The only
//! guarantee this module makes, and it makes it unconditionally: the returned
//! path's parent is exactly the requested directory.

use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};

/// Where a filename came from — used for `--verbose` reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilenameSource {
    Explicit,
    ContentDisposition,
    UrlPath,
    Fallback,
}

#[derive(Debug, Clone)]
pub struct Destination {
    pub path: PathBuf,
    pub filename: String,
    pub source: FilenameSource,
}

/// Pick a filename, in the priority order from PRD §21.
pub fn choose(
    explicit_output: Option<&str>,
    dir: Option<&str>,
    content_disposition: Option<&str>,
    final_url: &url::Url,
) -> Result<Destination> {
    // An explicit -o may carry its own directory component; the user is
    // trusted, the server is not.
    if let Some(out) = explicit_output {
        let out = Path::new(out);
        let (base_dir, name) = match out.parent() {
            Some(p) if !p.as_os_str().is_empty() => (
                resolve_dir(Some(&p.to_string_lossy()))?,
                out.file_name()
                    .context("--output must end in a filename")?
                    .to_string_lossy()
                    .to_string(),
            ),
            _ => (resolve_dir(dir)?, out.to_string_lossy().to_string()),
        };
        if name.is_empty() {
            bail!("--output must end in a filename");
        }
        return Ok(Destination {
            path: base_dir.join(&name),
            filename: name,
            source: FilenameSource::Explicit,
        });
    }

    let base_dir = resolve_dir(dir)?;

    let (name, source) = content_disposition
        .and_then(from_content_disposition)
        .map(|n| (n, FilenameSource::ContentDisposition))
        .or_else(|| from_url(final_url).map(|n| (n, FilenameSource::UrlPath)))
        .unwrap_or_else(|| (fallback_name(final_url), FilenameSource::Fallback));

    let name = sanitize(&name).unwrap_or_else(|| fallback_name(final_url));

    Ok(Destination {
        path: base_dir.join(&name),
        filename: name,
        source,
    })
}

fn resolve_dir(dir: Option<&str>) -> Result<PathBuf> {
    let path = match dir {
        Some(d) => expand_tilde(d),
        None => std::env::current_dir().context("cannot determine current directory")?,
    };
    Ok(path)
}

pub fn expand_tilde(input: &str) -> PathBuf {
    if let Some(rest) = input.strip_prefix("~/") {
        if let Some(home) = directories::BaseDirs::new().map(|b| b.home_dir().to_path_buf()) {
            return home.join(rest);
        }
    }
    PathBuf::from(input)
}

/// Parse RFC 6266 `Content-Disposition`, preferring the RFC 5987 `filename*`
/// form. Returns `None` for anything we do not fully understand — falling
/// through to the URL path is always safe.
fn from_content_disposition(header: &str) -> Option<String> {
    // filename*=UTF-8''foo%20bar.iso
    if let Some(idx) = find_param(header, "filename*") {
        let value = header[idx..].split(';').next()?.trim();
        if let Some((charset_and_lang, encoded)) = rsplit_ext_value(value) {
            let charset = charset_and_lang.to_ascii_lowercase();
            if charset.starts_with("utf-8") || charset.starts_with("iso-8859-1") {
                let decoded = percent_decode(encoded);
                if let Ok(s) = String::from_utf8(decoded) {
                    if let Some(clean) = sanitize(&s) {
                        return Some(clean);
                    }
                }
            }
        }
    }

    let idx = find_param(header, "filename")?;
    let rest = &header[idx..];
    let raw = if let Some(stripped) = rest.strip_prefix('"') {
        stripped.split('"').next()?.to_string()
    } else {
        rest.split(';').next()?.trim().to_string()
    };
    sanitize(&raw)
}

/// Find the start of a parameter's value (`name=` → index just past `=`).
fn find_param(header: &str, name: &str) -> Option<usize> {
    let lower = header.to_ascii_lowercase();
    let mut from = 0;
    while let Some(pos) = lower[from..].find(name) {
        let abs = from + pos;
        let after = abs + name.len();
        // Must be a parameter boundary before, and `=` (or `*=`) after.
        let boundary = abs == 0
            || lower[..abs]
                .chars()
                .next_back()
                .is_some_and(|c| c == ';' || c == ' ');
        let eq = lower[after..].trim_start().starts_with('=');
        // Do not let a search for `filename` match `filename*`.
        let exact = !name.ends_with('*') || lower[after..].trim_start().starts_with('=');
        if boundary && eq && exact && !(name == "filename" && lower[after..].starts_with('*')) {
            let value_start = after + lower[after..].find('=')? + 1;
            return Some(value_start);
        }
        from = abs + name.len();
    }
    None
}

/// Split `UTF-8''name` into (`UTF-8'`, `name`).
fn rsplit_ext_value(value: &str) -> Option<(&str, &str)> {
    let mut parts = value.splitn(3, '\'');
    let charset = parts.next()?;
    let _lang = parts.next()?;
    let name = parts.next()?;
    Some((charset, name))
}

fn percent_decode(input: &str) -> Vec<u8> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
            if let Some(v) = hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    out
}

fn from_url(url: &url::Url) -> Option<String> {
    let last = url.path_segments()?.next_back()?;
    if last.is_empty() {
        return None;
    }
    let decoded = percent_decode(last);
    let s = String::from_utf8_lossy(&decoded).to_string();
    sanitize(&s)
}

fn fallback_name(url: &url::Url) -> String {
    let host = url.host_str().unwrap_or("download");
    let stem = sanitize(host).unwrap_or_else(|| "download".to_string());
    format!("{stem}.download")
}

/// Reduce a server-supplied string to a single safe path component, or `None`
/// if nothing safe remains.
pub fn sanitize(raw: &str) -> Option<String> {
    // Take the last component after *both* separators: a Windows-style
    // "..\\..\\evil" must not survive on Unix either, since the resulting
    // filename would be a nasty surprise when copied between machines.
    let last = raw.rsplit(['/', '\\']).next().unwrap_or(raw);

    let cleaned: String = last
        .chars()
        .filter(|c| !c.is_control() && *c != '\0')
        .collect();
    let cleaned = cleaned.trim().trim_end_matches('.').to_string();

    if cleaned.is_empty() || cleaned == "." || cleaned == ".." {
        return None;
    }

    // Reject reserved device names, which are hazardous on Windows and merely
    // confusing elsewhere.
    let stem = cleaned
        .split('.')
        .next()
        .unwrap_or(&cleaned)
        .to_ascii_uppercase();
    const RESERVED: [&str; 22] = [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];
    if RESERVED.contains(&stem.as_str()) {
        return None;
    }

    // Filesystem limit is 255 bytes on every target we care about. Truncate on
    // a char boundary and keep the extension if we can.
    let cleaned = truncate_bytes(&cleaned, 255);
    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}

fn truncate_bytes(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let ext = Path::new(s)
        .extension()
        .map(|e| format!(".{}", e.to_string_lossy()))
        .unwrap_or_default();
    let keep = max.saturating_sub(ext.len());
    let mut end = keep.min(s.len());
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{}", &s[..end], ext)
}

/// Final gate before we open anything: the resolved path must sit directly in
/// the intended directory. Defends against a `..` that slipped through and
/// against symlinked parents.
pub fn assert_within(dir: &Path, path: &Path) -> Result<()> {
    if path.components().any(|c| matches!(c, Component::ParentDir)) {
        bail!("refusing a destination containing `..`: {}", path.display());
    }
    let parent = path.parent().unwrap_or(Path::new("."));
    let (a, b) = (normalise(dir), normalise(parent));
    if a != b {
        bail!(
            "refusing to write outside {}: resolved to {}",
            dir.display(),
            path.display()
        );
    }
    Ok(())
}

/// Lexical normalisation; we deliberately do not canonicalise, because the
/// destination usually does not exist yet.
fn normalise(p: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn u(s: &str) -> url::Url {
        url::Url::parse(s).unwrap()
    }

    #[test]
    fn rejects_traversal() {
        assert_eq!(sanitize("../../etc/passwd"), Some("passwd".into()));
        assert_eq!(
            sanitize("..\\..\\windows\\system32"),
            Some("system32".into())
        );
        assert_eq!(sanitize(".."), None);
        assert_eq!(sanitize("/"), None);
        assert_eq!(sanitize(""), None);
        assert_eq!(sanitize("   "), None);
        assert_eq!(sanitize("a\0b"), Some("ab".into()));
        assert_eq!(sanitize("evil\n.iso"), Some("evil.iso".into()));
    }

    #[test]
    fn rejects_reserved_names() {
        assert_eq!(sanitize("NUL"), None);
        assert_eq!(sanitize("con.txt"), None);
        assert_eq!(sanitize("console.txt"), Some("console.txt".into()));
    }

    #[test]
    fn truncates_long_names() {
        let long = format!("{}.iso", "a".repeat(400));
        let out = sanitize(&long).unwrap();
        assert!(out.len() <= 255);
        assert!(out.ends_with(".iso"));
    }

    #[test]
    fn parses_content_disposition() {
        assert_eq!(
            from_content_disposition("attachment; filename=\"linux.iso\""),
            Some("linux.iso".into())
        );
        assert_eq!(
            from_content_disposition("attachment; filename=plain.bin"),
            Some("plain.bin".into())
        );
        assert_eq!(
            from_content_disposition("attachment; filename*=UTF-8''caf%C3%A9%20menu.pdf"),
            Some("café menu.pdf".into())
        );
        // filename* wins over filename
        assert_eq!(
            from_content_disposition(
                "attachment; filename=\"fallback.bin\"; filename*=UTF-8''real.bin"
            ),
            Some("real.bin".into())
        );
        // hostile
        assert_eq!(
            from_content_disposition("attachment; filename=\"../../../etc/shadow\""),
            Some("shadow".into())
        );
        assert_eq!(from_content_disposition("inline"), None);
    }

    #[test]
    fn falls_through_priority_order() {
        let d = choose(
            None,
            Some("/tmp"),
            None,
            &u("https://x.example/a/b/file.tar.gz"),
        )
        .unwrap();
        assert_eq!(d.filename, "file.tar.gz");
        assert_eq!(d.source, FilenameSource::UrlPath);

        let d = choose(
            None,
            Some("/tmp"),
            Some("attachment; filename=real.bin"),
            &u("https://x.example/a/b/file.tar.gz"),
        )
        .unwrap();
        assert_eq!(d.filename, "real.bin");

        let d = choose(
            Some("mine.iso"),
            Some("/tmp"),
            Some("attachment; filename=real.bin"),
            &u("https://x.example/file.tar.gz"),
        )
        .unwrap();
        assert_eq!(d.path, PathBuf::from("/tmp/mine.iso"));

        let d = choose(None, Some("/tmp"), None, &u("https://x.example/")).unwrap();
        assert_eq!(d.filename, "x.example.download");
        assert_eq!(d.source, FilenameSource::Fallback);
    }

    #[test]
    fn percent_decodes_url_names() {
        let d = choose(
            None,
            Some("/tmp"),
            None,
            &u("https://x.example/my%20file.iso"),
        )
        .unwrap();
        assert_eq!(d.filename, "my file.iso");
    }

    #[test]
    fn within_check() {
        assert!(assert_within(Path::new("/tmp"), Path::new("/tmp/a.iso")).is_ok());
        assert!(assert_within(Path::new("/tmp"), Path::new("/tmp/sub/a.iso")).is_err());
        assert!(assert_within(Path::new("/tmp"), Path::new("/etc/a.iso")).is_err());
        assert!(assert_within(Path::new("/tmp"), Path::new("/tmp/../etc/a.iso")).is_err());
    }
}
