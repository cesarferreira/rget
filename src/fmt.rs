//! Human-facing formatting and the minimal ANSI styling we need.

use std::time::Duration;

const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];

/// `12.40 GiB`
pub fn bytes(n: u64) -> String {
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else if value >= 100.0 {
        format!("{value:.0} {}", UNITS[unit])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

/// `84.7 MiB/s`
pub fn rate(bytes_per_sec: f64) -> String {
    if !bytes_per_sec.is_finite() || bytes_per_sec <= 0.0 {
        return "--".to_string();
    }
    let mut value = bytes_per_sec;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}/s", UNITS[unit])
}

/// `2m 31s`, `1h 04m`, `812ms`
pub fn duration(d: Duration) -> String {
    let secs = d.as_secs();
    match secs {
        // Sub-second precision matters for retry delays, but a bare "0ms"
        // reads as broken; an elapsed or remaining time of zero is "0s".
        0 if d.subsec_millis() == 0 => "0s".to_string(),
        0 => format!("{}ms", d.subsec_millis()),
        1..=59 => format!("{secs}s"),
        60..=3599 => format!("{}m {:02}s", secs / 60, secs % 60),
        _ => format!("{}h {:02}m", secs / 3600, (secs % 3600) / 60),
    }
}

/// ETA, or `--` when we have no basis for an estimate.
pub fn eta(remaining: u64, bytes_per_sec: f64) -> String {
    if !bytes_per_sec.is_finite() || bytes_per_sec < 1.0 {
        return "--".to_string();
    }
    duration(Duration::from_secs_f64(remaining as f64 / bytes_per_sec))
}

/// `55.0%`
pub fn percent(done: u64, total: Option<u64>) -> String {
    match total {
        Some(t) if t > 0 => format!("{:.1}%", (done as f64 / t as f64) * 100.0),
        _ => "--".to_string(),
    }
}

/// Eighth-width blocks, so the bar creeps forward smoothly instead of jumping a
/// whole cell at a time — the difference between a bar that looks alive and one
/// that looks stuck.
const PARTIALS: [char; 8] = ['▏', '▎', '▍', '▌', '▋', '▊', '▉', '█'];

/// The filled and empty halves of a progress bar, separate so the caller can
/// colour them independently. Together they are always exactly `width` cells.
pub fn bar_parts(done: u64, total: Option<u64>, width: usize) -> (String, String) {
    let frac = match total {
        Some(t) if t > 0 => (done as f64 / t as f64).clamp(0.0, 1.0),
        _ => 0.0,
    };
    let exact = frac * width as f64;
    let whole = (exact.floor() as usize).min(width);

    let mut filled = "█".repeat(whole);
    let mut used = whole;
    let remainder = exact - whole as f64;
    if used < width && remainder > 0.02 {
        let idx = ((remainder * 8.0).round() as usize).clamp(1, 8) - 1;
        filled.push(PARTIALS[idx]);
        used += 1;
    }
    (filled, "░".repeat(width - used))
}

pub fn bar(done: u64, total: Option<u64>, width: usize) -> String {
    let (filled, empty) = bar_parts(done, total, width);
    format!("{filled}{empty}")
}

/// Truncate a URL for display so a signed CDN URL does not wrap the terminal.
/// Also drops the query string, which is where credentials usually hide.
pub fn short_url(raw: &str) -> String {
    let no_query = raw.split(['?', '#']).next().unwrap_or(raw);
    if no_query.len() <= 72 {
        return no_query.to_string();
    }
    let tail = &no_query[no_query.len() - 40..];
    format!("{}…{}", &no_query[..30], tail)
}

// -- styling ---------------------------------------------------------------

/// ANSI styling, globally disabled when output is not a terminal or when
/// `NO_COLOR` is set.
#[derive(Clone, Copy, Debug)]
pub struct Style {
    enabled: bool,
}

impl Style {
    pub fn new(enabled: bool) -> Self {
        Self {
            // `NO_COLOR` is a promise, not a suggestion: https://no-color.org.
            enabled: enabled && std::env::var_os("NO_COLOR").is_none(),
        }
    }

    /// Styling for anything printed to stdout — `list`, `info`, `config`.
    pub fn stdout() -> Self {
        Self::new(std::io::IsTerminal::is_terminal(&std::io::stdout()))
    }

    /// Styling for progress and messages, which go to stderr.
    pub fn stderr() -> Self {
        Self::new(std::io::IsTerminal::is_terminal(&std::io::stderr()))
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn wrap(&self, code: &str, text: &str) -> String {
        if self.enabled {
            format!("\x1b[{code}m{text}\x1b[0m")
        } else {
            text.to_string()
        }
    }

    pub fn bold(&self, t: &str) -> String {
        self.wrap("1", t)
    }
    pub fn dim(&self, t: &str) -> String {
        self.wrap("2", t)
    }
    pub fn green(&self, t: &str) -> String {
        self.wrap("32", t)
    }
    pub fn red(&self, t: &str) -> String {
        self.wrap("31", t)
    }
    pub fn yellow(&self, t: &str) -> String {
        self.wrap("33", t)
    }
    pub fn cyan(&self, t: &str) -> String {
        self.wrap("36", t)
    }
    pub fn blue(&self, t: &str) -> String {
        self.wrap("34", t)
    }
    pub fn magenta(&self, t: &str) -> String {
        self.wrap("35", t)
    }
    pub fn bright_green(&self, t: &str) -> String {
        self.wrap("92", t)
    }
    pub fn bright_cyan(&self, t: &str) -> String {
        self.wrap("96", t)
    }
    pub fn bold_green(&self, t: &str) -> String {
        self.wrap("1;32", t)
    }
    pub fn bold_red(&self, t: &str) -> String {
        self.wrap("1;31", t)
    }
    pub fn bold_cyan(&self, t: &str) -> String {
        self.wrap("1;36", t)
    }

    /// A `·` separator, always dim so it recedes behind the values it divides.
    pub fn sep(&self) -> String {
        self.dim(" · ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_bytes() {
        assert_eq!(bytes(0), "0 B");
        assert_eq!(bytes(512), "512 B");
        assert_eq!(bytes(1536), "1.50 KiB");
        assert_eq!(bytes(13314398618), "12.40 GiB");
        // Three significant figures would be noise past 100.
        assert_eq!(bytes(200 * 1024 * 1024), "200 MiB");
    }

    #[test]
    fn formats_duration() {
        assert_eq!(duration(Duration::ZERO), "0s");
        assert_eq!(duration(Duration::from_millis(812)), "812ms");
        assert_eq!(duration(Duration::from_secs(45)), "45s");
        assert_eq!(duration(Duration::from_secs(151)), "2m 31s");
        assert_eq!(duration(Duration::from_secs(3900)), "1h 05m");
    }

    #[test]
    fn eta_needs_a_basis() {
        assert_eq!(eta(1000, 0.0), "--");
        assert_eq!(eta(1000, f64::NAN), "--");
        assert_eq!(eta(1024, 1024.0), "1s");
    }

    #[test]
    fn bar_is_width_stable() {
        assert_eq!(bar(0, Some(10), 4).chars().count(), 4);
        assert_eq!(bar(10, Some(10), 4).chars().count(), 4);
        assert_eq!(bar(5, None, 4).chars().count(), 4);
    }

    #[test]
    fn short_url_drops_query() {
        assert_eq!(
            short_url("https://x.example/file.iso?token=secret"),
            "https://x.example/file.iso"
        );
    }
}
