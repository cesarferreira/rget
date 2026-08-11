//! Terminal rendering (PRD §19).
//!
//! This is the only module allowed to know what a terminal is. It consumes
//! [`Event`]s and samples [`Stats`]; it never talks to the engine.
//!
//! Human progress goes to **stderr** so it cannot contaminate piped data, and
//! `--json` events go to **stdout** so they can be piped into `jq`. When stderr
//! is not a TTY we emit plain periodic lines with no cursor control at all, so
//! `rget URL 2> log.txt` produces a readable log rather than escape soup.

use std::io::{IsTerminal, Write};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::UnboundedReceiver;

use crate::fmt::{self, Style};
use crate::progress::{Event, NoteLevel, Snapshot, SpeedMeter, Stats};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Live, redrawing block.
    Interactive,
    /// Periodic one-line updates, no escape codes.
    Plain,
    /// One JSON object per line on stdout.
    Json,
    /// Errors and the final result only.
    Quiet,
}

impl Mode {
    /// Pick a mode from the flags and the environment. Explicit flags win;
    /// otherwise interactivity decides (PRD §19).
    pub fn detect(json: bool, quiet: bool, verbose: bool) -> Mode {
        if json {
            return Mode::Json;
        }
        if quiet {
            return Mode::Quiet;
        }
        // Debug logging and a redrawing block fight over the same cursor, so
        // when logs are on we degrade to plain output on purpose.
        let logging = verbose || std::env::var_os("RUST_LOG").is_some();
        if std::io::stderr().is_terminal() && !logging {
            Mode::Interactive
        } else {
            Mode::Plain
        }
    }
}

/// How wide to draw, clamped to something sane for very wide or unknown
/// terminals.
fn terminal_width() -> usize {
    terminal_size::terminal_size()
        .map(|(terminal_size::Width(w), _)| w as usize)
        .unwrap_or(80)
        .clamp(40, 120)
}

struct Ui {
    mode: Mode,
    style: Style,
    stats: Arc<Stats>,
    filename: String,
    total_size: Option<u64>,
    resumed_bytes: u64,
    connections: usize,
    meter: Option<SpeedMeter>,
    /// Lines currently occupied by the live block, so we know how far to move
    /// the cursor back up.
    drawn_lines: usize,
    last_note: Option<String>,
    verifying: Option<(String, u64, u64)>,
    finished: bool,
    verbose: bool,
}

/// Drive the display until the event channel closes.
pub async fn run(mode: Mode, stats: Arc<Stats>, mut rx: UnboundedReceiver<Event>, verbose: bool) {
    let mut ui = Ui {
        mode,
        style: Style::new(mode == Mode::Interactive),
        stats,
        filename: String::new(),
        total_size: None,
        resumed_bytes: 0,
        connections: 0,
        meter: None,
        drawn_lines: 0,
        last_note: None,
        verifying: None,
        finished: false,
        verbose,
    };

    let mut ticker = tokio::time::interval(Duration::from_millis(100));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Plain mode prints a line every few seconds instead of redrawing.
    let mut plain_ticker = tokio::time::interval(Duration::from_secs(5));
    plain_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    if ui.mode == Mode::Interactive {
        let _ = write!(std::io::stderr(), "\x1b[?25l"); // hide cursor
    }

    loop {
        tokio::select! {
            event = rx.recv() => match event {
                Some(event) => ui.handle(event),
                None => break,
            },
            _ = ticker.tick(), if ui.mode == Mode::Interactive => ui.tick(),
            _ = plain_ticker.tick(), if ui.mode == Mode::Plain => ui.tick(),
        }
    }

    if ui.mode == Mode::Interactive {
        let _ = write!(std::io::stderr(), "\x1b[?25h"); // show cursor
        let _ = std::io::stderr().flush();
    }
}

impl Ui {
    fn handle(&mut self, event: Event) {
        if self.mode == Mode::Json {
            self.emit_json(&event);
            return;
        }

        match event {
            Event::DownloadStarted {
                ref filename,
                total_size,
                resumed_bytes,
                connections,
                parallel,
                ref id,
                ..
            } => {
                self.filename = filename.clone();
                self.total_size = total_size;
                self.resumed_bytes = resumed_bytes;
                self.connections = connections;
                self.meter = Some(SpeedMeter::new(Duration::from_secs(3), resumed_bytes));

                if self.mode != Mode::Quiet {
                    if resumed_bytes > 0 {
                        self.line(format!(
                            "{} {}",
                            self.style.bold("Resuming"),
                            self.style.bold(filename)
                        ));
                        self.line(format!(
                            "{} already downloaded",
                            match total_size {
                                Some(t) =>
                                    format!("{} / {}", fmt::bytes(resumed_bytes), fmt::bytes(t)),
                                None => fmt::bytes(resumed_bytes),
                            }
                        ));
                    } else {
                        self.line(self.style.bold(filename));
                    }
                    if self.verbose {
                        self.line(format!(
                            "  id {id}, {} connection(s), {}",
                            if parallel { connections } else { 1 },
                            if parallel {
                                "parallel ranges"
                            } else {
                                "sequential"
                            }
                        ));
                    }
                }
            }
            Event::RetryScheduled {
                attempt,
                delay_ms,
                ref reason,
                ..
            } => {
                let msg = format!(
                    "{}. Retrying in {}... attempt {attempt}",
                    reason,
                    fmt::duration(Duration::from_millis(delay_ms))
                );
                if self.mode == Mode::Interactive {
                    self.last_note = Some(self.style.yellow(&msg));
                } else if self.mode != Mode::Quiet {
                    self.line(msg);
                }
            }
            Event::Note { level, ref message } => {
                let styled = match level {
                    NoteLevel::Info => self.style.dim(&format!("  {message}")),
                    NoteLevel::Warn => self.style.yellow(&format!("  warning: {message}")),
                    NoteLevel::Error => self.style.red(&format!("  error: {message}")),
                };
                if level == NoteLevel::Info && (self.mode == Mode::Quiet || !self.verbose) {
                    return;
                }
                if self.mode == Mode::Quiet && level != NoteLevel::Error {
                    return;
                }
                self.line(styled);
            }
            Event::VerificationStarted {
                ref algorithm,
                total_size,
            } => {
                self.clear_block();
                self.verifying = Some((algorithm.clone(), 0, total_size));
                if self.mode != Mode::Quiet {
                    self.line(format!("Verifying {}...", label_for(algorithm)));
                }
            }
            Event::VerificationProgress { bytes, total_size } => {
                if let Some((algo, _, _)) = &self.verifying {
                    let algo = algo.clone();
                    self.verifying = Some((algo, bytes, total_size));
                }
                if self.mode == Mode::Interactive {
                    self.draw_verification();
                }
            }
            Event::VerificationCompleted {
                ref algorithm,
                ok,
                ref expected,
                ref actual,
            } => {
                self.clear_block();
                self.verifying = None;
                if ok {
                    if self.mode != Mode::Quiet {
                        self.line(format!("{} Checksum verified", self.style.green("✓")));
                    }
                } else {
                    self.line(format!("{} Checksum mismatch", self.style.red("✗")));
                    self.line(format!("Expected:\n{}", expected.as_deref().unwrap_or("?")));
                    self.line(format!("Actual:\n{actual}"));
                    let _ = label_for(algorithm);
                }
            }
            Event::DownloadCompleted {
                downloaded,
                elapsed_ms,
                average_bps,
            } => {
                self.finished = true;
                self.clear_block();
                if self.mode == Mode::Quiet {
                    return;
                }
                self.line(format!(
                    "{} {}",
                    self.style.green("✓"),
                    self.style.bold(&self.filename)
                ));
                self.line(format!(
                    "  {} downloaded in {}",
                    fmt::bytes(downloaded),
                    fmt::duration(Duration::from_millis(elapsed_ms))
                ));
                self.line(format!(
                    "  Average speed: {}",
                    fmt::rate(average_bps as f64)
                ));
            }
            Event::DownloadPaused {
                downloaded,
                total_size,
            } => {
                self.finished = true;
                self.clear_block();
                self.line(self.style.bold("Download paused."));
                self.line(match total_size {
                    Some(total) => format!(
                        "{} / {} already downloaded.",
                        fmt::bytes(downloaded),
                        fmt::bytes(total)
                    ),
                    None => format!("{} already downloaded.", fmt::bytes(downloaded)),
                });
                self.line(self.style.dim("Run the same command to resume."));
            }
            Event::DownloadFailed { ref error } => {
                self.finished = true;
                self.clear_block();
                self.line(format!("{} {error}", self.style.red("✗")));
            }
            // Byte-level and range-level events drive the sampled display
            // rather than printing anything of their own.
            Event::BytesWritten { .. }
            | Event::RangeStarted { .. }
            | Event::RangeCompleted { .. }
            | Event::RangeSplit { .. }
            | Event::Checkpointed { .. } => {}
        }
    }

    fn emit_json(&self, event: &Event) {
        let mut out = std::io::stdout().lock();
        if let Ok(line) = serde_json::to_string(event) {
            let _ = writeln!(out, "{line}");
            let _ = out.flush();
        }
    }

    fn tick(&mut self) {
        // Once verification starts the transfer is over; a further progress
        // line here would print "100% ETA 0s" underneath "Verifying...".
        if self.finished || self.meter.is_none() || self.verifying.is_some() {
            return;
        }
        let downloaded = self.stats.downloaded();
        if let Some(meter) = &mut self.meter {
            meter.record(downloaded);
        }
        match self.mode {
            Mode::Interactive if self.verifying.is_none() => self.draw_block(),
            Mode::Plain => {
                let snap = self.snapshot();
                let _ = writeln!(
                    std::io::stderr(),
                    "{}: {} / {} ({}) at {} ETA {}",
                    snap.filename,
                    fmt::bytes(snap.downloaded),
                    snap.total_size
                        .map(fmt::bytes)
                        .unwrap_or_else(|| "unknown".into()),
                    fmt::percent(snap.downloaded, snap.total_size),
                    fmt::rate(snap.bps),
                    snap.eta_secs
                        .map(|s| fmt::duration(Duration::from_secs(s)))
                        .unwrap_or_else(|| "--".into()),
                );
            }
            _ => {}
        }
    }

    fn snapshot(&self) -> Snapshot {
        let downloaded = self.stats.downloaded();
        let (complete, total_ranges) = self.stats.ranges();
        let (bps, smoothed, elapsed) = match &self.meter {
            Some(m) => (m.rolling_bps(), m.smoothed_bps(), m.elapsed()),
            None => (0.0, 0.0, Duration::ZERO),
        };
        let eta_secs = match (self.total_size, smoothed) {
            (Some(total), s) if s >= 1.0 => {
                Some((total.saturating_sub(downloaded) as f64 / s) as u64)
            }
            _ => None,
        };
        Snapshot {
            filename: self.filename.clone(),
            downloaded,
            total_size: self.total_size,
            bps,
            smoothed_bps: smoothed,
            eta_secs,
            elapsed_ms: elapsed.as_millis() as u64,
            active_connections: self.stats.active_connections(),
            ranges_complete: complete,
            ranges_total: total_ranges,
            retries: self.stats.retries(),
        }
    }

    fn draw_block(&mut self) {
        let width = terminal_width();
        let lines = render_block(&self.snapshot(), self.last_note.as_deref(), width);
        self.paint(&lines);
    }

    fn draw_verification(&mut self) {
        let Some((_, bytes, total)) = self.verifying.clone() else {
            return;
        };
        let width = terminal_width();
        let bar_width = width.saturating_sub(6).min(40);
        let lines = vec![format!(
            "{} {}",
            fmt::bar(bytes, Some(total), bar_width),
            fmt::percent(bytes, Some(total))
        )];
        self.paint(&lines);
    }

    /// Redraw the block in place: move up over what we drew last time, clearing
    /// each line as we go. Never scrolls, so the terminal stays quiet (PRD §4).
    fn paint(&mut self, lines: &[String]) {
        let mut out = std::io::stderr().lock();
        if self.drawn_lines > 0 {
            let _ = write!(out, "\x1b[{}A", self.drawn_lines);
        }
        for line in lines {
            let _ = writeln!(out, "\x1b[2K{line}");
        }
        // If this frame is shorter than the last, wipe the leftovers.
        for _ in lines.len()..self.drawn_lines {
            let _ = writeln!(out, "\x1b[2K");
        }
        if self.drawn_lines > lines.len() {
            let _ = write!(out, "\x1b[{}A", self.drawn_lines - lines.len());
        }
        let _ = out.flush();
        self.drawn_lines = lines.len();
    }

    /// Drop the live block so a permanent message can be printed under it.
    fn clear_block(&mut self) {
        if self.mode != Mode::Interactive || self.drawn_lines == 0 {
            return;
        }
        let mut out = std::io::stderr().lock();
        let _ = write!(out, "\x1b[{}A", self.drawn_lines);
        for _ in 0..self.drawn_lines {
            let _ = writeln!(out, "\x1b[2K");
        }
        let _ = write!(out, "\x1b[{}A", self.drawn_lines);
        let _ = out.flush();
        self.drawn_lines = 0;
    }

    fn line(&mut self, text: impl AsRef<str>) {
        if self.mode == Mode::Json {
            return;
        }
        self.clear_block();
        let _ = writeln!(std::io::stderr(), "{}", text.as_ref());
    }
}

fn label_for(algorithm: &str) -> String {
    match algorithm {
        "sha256" => "SHA-256".into(),
        "sha512" => "SHA-512".into(),
        "blake3" => "BLAKE3".into(),
        other => other.to_uppercase(),
    }
}

/// Build the progress block. Pure, so it can be tested without a terminal.
pub fn render_block(snap: &Snapshot, note: Option<&str>, width: usize) -> Vec<String> {
    let mut lines = Vec::with_capacity(5);

    lines.push(format!(
        "  {} / {}   {}",
        fmt::bytes(snap.downloaded),
        snap.total_size
            .map(fmt::bytes)
            .unwrap_or_else(|| "unknown".to_string()),
        fmt::percent(snap.downloaded, snap.total_size),
    ));

    let eta = snap
        .eta_secs
        .map(|s| fmt::duration(Duration::from_secs(s)))
        .unwrap_or_else(|| "--".to_string());
    lines.push(format!("  {:<22} ETA {}", fmt::rate(snap.bps), eta));

    let mut third = format!(
        "  {:<22} {}/{} chunks",
        format!("{} connections", snap.active_connections),
        snap.ranges_complete,
        snap.ranges_total
    );
    if snap.retries > 0 {
        third.push_str(&format!("   {} retries", snap.retries));
    }
    lines.push(third);

    if let Some(note) = note {
        lines.push(format!("  {note}"));
    }

    let bar_width = width.saturating_sub(8).min(40);
    lines.push(format!(
        "{} {}",
        fmt::bar(snap.downloaded, snap.total_size, bar_width),
        fmt::percent(snap.downloaded, snap.total_size)
    ));

    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap() -> Snapshot {
        Snapshot {
            filename: "linux.iso".into(),
            downloaded: 7_324_070_215,
            total_size: Some(13_314_398_618),
            bps: 88_800_000.0,
            smoothed_bps: 88_000_000.0,
            eta_secs: Some(67),
            elapsed_ms: 151_000,
            active_connections: 8,
            ranges_complete: 14,
            ranges_total: 24,
            retries: 0,
        }
    }

    #[test]
    fn block_has_no_escape_codes_of_its_own() {
        for line in render_block(&snap(), None, 80) {
            assert!(
                !line.contains('\x1b'),
                "styling belongs to the caller: {line}"
            );
        }
    }

    #[test]
    fn block_shows_the_prd_fields() {
        let lines = render_block(&snap(), None, 80);
        let text = lines.join("\n");
        assert!(text.contains("6.82 GiB"), "{text}");
        assert!(text.contains("12.40 GiB"), "{text}");
        assert!(text.contains("55.0%"), "{text}");
        assert!(text.contains("84.7 MiB/s"), "{text}");
        assert!(text.contains("ETA 1m 07s"), "{text}");
        assert!(text.contains("8 connections"), "{text}");
        assert!(text.contains("14/24 chunks"), "{text}");
    }

    #[test]
    fn block_is_stable_width() {
        for width in [40usize, 60, 80, 200] {
            let lines = render_block(&snap(), None, width);
            let bar = lines.last().unwrap();
            assert!(
                bar.chars().count() <= width.clamp(40, 120),
                "bar overflows at width {width}: {bar}"
            );
        }
    }

    #[test]
    fn unknown_total_degrades_gracefully() {
        let mut s = snap();
        s.total_size = None;
        s.eta_secs = None;
        let text = render_block(&s, None, 80).join("\n");
        assert!(text.contains("unknown"), "{text}");
        assert!(text.contains("ETA --"), "{text}");
    }

    #[test]
    fn retries_and_notes_surface() {
        let mut s = snap();
        s.retries = 3;
        let lines = render_block(&s, Some("Connection lost. Retrying in 2s..."), 80);
        let text = lines.join("\n");
        assert!(text.contains("3 retries"), "{text}");
        assert!(text.contains("Retrying in 2s"), "{text}");
    }

    #[test]
    fn mode_detection_respects_flags() {
        assert_eq!(Mode::detect(true, false, false), Mode::Json);
        // --json wins over --quiet: a script asked for machine output.
        assert_eq!(Mode::detect(true, true, false), Mode::Json);
        assert_eq!(Mode::detect(false, true, false), Mode::Quiet);
        // Verbose logging forces plain output so logs and the block do not
        // fight over the cursor.
        assert_eq!(Mode::detect(false, false, true), Mode::Plain);
    }

    #[test]
    fn algorithm_labels() {
        assert_eq!(label_for("sha256"), "SHA-256");
        assert_eq!(label_for("blake3"), "BLAKE3");
        assert_eq!(label_for("weird"), "WEIRD");
    }

    #[test]
    fn style_can_be_disabled() {
        let plain = Style::new(false);
        assert_eq!(plain.green("ok"), "ok");
        let styled = Style::new(true);
        // NO_COLOR may be set in the test environment; either way, no panic and
        // the text survives.
        assert!(styled.green("ok").contains("ok"));
    }
}
