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
                            "{} {} {}",
                            self.style.bright_cyan("⟳"),
                            self.style.dim("Resuming"),
                            self.style.bold(filename)
                        ));
                        self.line(format!(
                            "  {} {}",
                            self.style.bold(&fmt::bytes(resumed_bytes)),
                            self.style.dim(&match total_size {
                                Some(t) => format!("of {} already downloaded", fmt::bytes(t)),
                                None => "already downloaded".to_string(),
                            })
                        ));
                    }
                    // The live block carries the filename on its first line, so
                    // only the non-redrawing modes need it announced here.
                    if resumed_bytes == 0 && self.mode == Mode::Plain {
                        self.line(self.style.bold(filename));
                    }
                    if self.verbose {
                        self.line(format!(
                            "  {} {}",
                            self.style.dim("id"),
                            self.style.dim(&format!(
                                "{id} · {} connection(s) · {}",
                                if parallel { connections } else { 1 },
                                if parallel {
                                    "parallel ranges"
                                } else {
                                    "sequential"
                                }
                            ))
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
                    self.line(format!(
                        "  {} {} {}",
                        self.style.bright_cyan("⋯"),
                        self.style.dim("Verifying"),
                        self.style.bold(&label_for(algorithm))
                    ));
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
                        self.line(format!(
                            "  {} {} {}",
                            self.style.bold_green("✓"),
                            self.style.dim(&label_for(algorithm)),
                            self.style.green("verified")
                        ));
                    }
                } else {
                    // A mismatch is the loudest thing this tool can say, so it
                    // gets the full-width red treatment and both digests.
                    self.line(format!(
                        "  {} {} {}",
                        self.style.bold_red("✗"),
                        self.style.bold(&label_for(algorithm)),
                        self.style.red("MISMATCH")
                    ));
                    self.line(format!(
                        "    {} {}",
                        self.style.dim("expected"),
                        self.style.green(expected.as_deref().unwrap_or("?"))
                    ));
                    self.line(format!(
                        "    {} {}",
                        self.style.dim("actual  "),
                        self.style.red(actual)
                    ));
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
                    "  {} {}",
                    self.style.bold_green("✓"),
                    self.style.bold(&self.filename)
                ));
                self.line(format!(
                    "    {}{}{} {}{}{} {}",
                    self.style.bold(&fmt::bytes(downloaded)),
                    self.style.sep(),
                    self.style.dim("in"),
                    self.style
                        .cyan(&fmt::duration(Duration::from_millis(elapsed_ms))),
                    self.style.sep(),
                    self.style.green(&fmt::rate(average_bps as f64)),
                    self.style.dim("average"),
                ));
            }
            Event::DownloadPaused {
                downloaded,
                total_size,
            } => {
                self.finished = true;
                self.clear_block();
                self.line(format!(
                    "  {} {}",
                    self.style.yellow("⏸"),
                    self.style.bold("Download paused")
                ));
                self.line(format!(
                    "    {} {}",
                    self.style.bold(&fmt::bytes(downloaded)),
                    self.style.dim(&match total_size {
                        Some(total) => format!("of {} downloaded", fmt::bytes(total)),
                        None => "downloaded".to_string(),
                    })
                ));
                self.line(self.style.dim("    Run the same command again to resume."));
            }
            Event::DownloadFailed { ref error } => {
                self.finished = true;
                self.clear_block();
                self.line(format!(
                    "  {} {}",
                    self.style.bold_red("✗"),
                    self.style.red(error)
                ));
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
        let lines = render_block(
            &self.snapshot(),
            self.last_note.as_deref(),
            width,
            &self.style,
        );
        self.paint(&lines);
    }

    fn draw_verification(&mut self) {
        let Some((_, bytes, total)) = self.verifying.clone() else {
            return;
        };
        let width = terminal_width();
        let bar_width = width.saturating_sub(12).clamp(12, 44);
        let (filled, empty) = fmt::bar_parts(bytes, Some(total), bar_width);
        // Cyan rather than green: verification is a different phase, and the
        // colour change is what tells you the download itself is done.
        let lines = vec![format!(
            "  {}{}  {}",
            self.style.bright_cyan(&filled),
            self.style.dim(&empty),
            self.style.bold_cyan(&fmt::percent(bytes, Some(total)))
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

/// Build the progress block.
///
/// Pure — it takes the [`Style`] rather than deciding on one, so tests can
/// render both a plain version (to assert content) and a styled version (to
/// assert colour) without needing a terminal.
pub fn render_block(
    snap: &Snapshot,
    note: Option<&str>,
    width: usize,
    style: &Style,
) -> Vec<String> {
    const INDENT: &str = "  ";
    let mut lines = Vec::with_capacity(6);

    // Name, with a marker that gives the eye somewhere to land.
    let name = truncate_display(&snap.filename, width.saturating_sub(4));
    lines.push(format!(
        "{INDENT}{} {}",
        style.bright_cyan("↓"),
        style.bold(&name)
    ));

    // The bar carries the colour weight: bright where done, receding where not.
    // The percentage is padded to the width of "100.0%" so the block's right
    // edge does not twitch every time the number gains a digit.
    let pct = format!("{:>6}", fmt::percent(snap.downloaded, snap.total_size));
    let bar_width = width
        .saturating_sub(INDENT.len() + 2 + pct.len())
        .clamp(8, 44);
    let (filled, empty) = fmt::bar_parts(snap.downloaded, snap.total_size, bar_width);
    lines.push(format!(
        "{INDENT}{}{}  {}",
        style.bright_green(&filled),
        style.dim(&empty),
        style.bold_green(&pct)
    ));

    // Size, speed and ETA — the three numbers people actually watch.
    let total = snap
        .total_size
        .map(fmt::bytes)
        .unwrap_or_else(|| "unknown".to_string());
    let downloaded = fmt::bytes(snap.downloaded);
    let rate = fmt::rate(snap.bps);
    let eta = snap
        .eta_secs
        .map(|s| fmt::duration(Duration::from_secs(s)))
        .unwrap_or_else(|| "--".to_string());

    let headline = [
        (
            format!("{downloaded} / {total}"),
            format!(
                "{}{}",
                style.bold(&downloaded),
                style.dim(&format!(" / {total}"))
            ),
        ),
        (rate.clone(), style.green(&rate)),
        (
            format!("ETA {eta}"),
            format!("{} {}", style.dim("ETA"), style.cyan(&eta)),
        ),
    ];
    lines.extend(join_wrapped(&headline, INDENT, width, style));

    // Connection detail: diagnostic rather than headline, so it reads dimmer.
    let conns = snap.active_connections.to_string();
    let chunks = format!("{}/{}", snap.ranges_complete, snap.ranges_total);
    let mut detail = vec![
        (
            format!("{conns} connections"),
            format!("{} {}", style.blue(&conns), style.dim("connections")),
        ),
        (
            format!("{chunks} chunks"),
            format!("{} {}", style.magenta(&chunks), style.dim("chunks")),
        ),
    ];
    if snap.retries > 0 {
        let retries = snap.retries.to_string();
        detail.push((
            format!("{retries} retries"),
            format!("{} {}", style.yellow(&retries), style.dim("retries")),
        ));
    }
    lines.extend(join_wrapped(&detail, INDENT, width, style));

    if let Some(note) = note {
        lines.push(format!("{INDENT}{note}"));
    }

    lines
}

/// Join `(plain, styled)` segments with dim separators, starting a new line
/// whenever the plain text would run past `width`.
///
/// The plain half exists purely so we can measure: ANSI escapes have length but
/// occupy no columns, so measuring the styled string would wrap far too early.
fn join_wrapped(
    segments: &[(String, String)],
    indent: &str,
    width: usize,
    style: &Style,
) -> Vec<String> {
    const SEP: &str = " · ";
    let indent_cols = indent.chars().count();

    let mut lines = Vec::new();
    let mut plain = String::new();
    let mut styled = String::new();

    for (segment_plain, segment_styled) in segments {
        let segment_cols = segment_plain.chars().count();
        let would_be = if plain.is_empty() {
            indent_cols + segment_cols
        } else {
            indent_cols + plain.chars().count() + SEP.chars().count() + segment_cols
        };

        if !plain.is_empty() && would_be > width {
            lines.push(format!("{indent}{styled}"));
            plain.clear();
            styled.clear();
        }
        if !plain.is_empty() {
            plain.push_str(SEP);
            styled.push_str(&style.sep());
        }
        plain.push_str(segment_plain);
        styled.push_str(segment_styled);
    }

    if !plain.is_empty() {
        lines.push(format!("{indent}{styled}"));
    }
    lines
}

/// Shorten a name to fit, keeping the extension visible — the tail of a
/// filename is usually the informative part.
fn truncate_display(name: &str, max_cols: usize) -> String {
    let cols = name.chars().count();
    if cols <= max_cols || max_cols < 4 {
        return name.to_string();
    }
    let keep = max_cols - 1;
    let head: String = name.chars().take(keep / 2).collect();
    let tail: String = name
        .chars()
        .skip(cols - (keep - keep / 2))
        .collect::<String>();
    format!("{head}…{tail}")
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

    /// Styling off — what a pipe, a log file or `NO_COLOR` sees.
    fn plain() -> Style {
        Style::new(false)
    }

    /// The bar line, wherever it happens to sit in the block.
    fn bar_line(lines: &[String]) -> String {
        lines
            .iter()
            .find(|l| l.contains('█') || l.contains('░'))
            .expect("the block should contain a progress bar")
            .clone()
    }

    #[test]
    fn plain_style_emits_no_escape_codes() {
        for line in render_block(&snap(), Some("a note"), 80, &plain()) {
            assert!(
                !line.contains('\x1b'),
                "disabled styling must stay disabled: {line}"
            );
        }
    }

    #[test]
    fn styled_block_is_actually_coloured() {
        let style = Style::new(true);
        if !style.is_enabled() {
            return; // NO_COLOR is set in this environment; nothing to assert.
        }
        let lines = render_block(&snap(), None, 80, &style);
        let text = lines.join("\n");
        assert!(text.contains('\x1b'), "expected colour, got: {text:?}");
        // Every sequence we open must be closed, or the colour bleeds into the
        // user's shell prompt after we exit.
        assert_eq!(
            text.matches("\x1b[").count(),
            text.matches("\x1b[0m").count() * 2,
            "unbalanced colour codes: {text:?}"
        );
    }

    #[test]
    fn block_shows_the_prd_fields() {
        let lines = render_block(&snap(), None, 80, &plain());
        let text = lines.join("\n");
        assert!(text.contains("linux.iso"), "{text}");
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
            let lines = render_block(&snap(), None, width, &plain());
            for line in &lines {
                assert!(
                    line.chars().count() <= width,
                    "line overflows at width {width}: {line}"
                );
            }
        }
    }

    #[test]
    fn bar_keeps_its_width_as_progress_changes() {
        // A bar that changes width as it fills makes the whole block jitter.
        let mut widths = std::collections::HashSet::new();
        for done in [
            0u64,
            1,
            5_000,
            7_324_070_215,
            13_314_398_617,
            13_314_398_618,
        ] {
            let mut s = snap();
            s.downloaded = done;
            widths.insert(
                bar_line(&render_block(&s, None, 80, &plain()))
                    .chars()
                    .count(),
            );
        }
        assert_eq!(widths.len(), 1, "bar width jitters: {widths:?}");
    }

    #[test]
    fn unknown_total_degrades_gracefully() {
        let mut s = snap();
        s.total_size = None;
        s.eta_secs = None;
        let text = render_block(&s, None, 80, &plain()).join("\n");
        assert!(text.contains("unknown"), "{text}");
        assert!(text.contains("ETA --"), "{text}");
    }

    #[test]
    fn retries_and_notes_surface() {
        let mut s = snap();
        s.retries = 3;
        let lines = render_block(&s, Some("Connection lost. Retrying in 2s..."), 80, &plain());
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
