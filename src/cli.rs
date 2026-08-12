//! Argument parsing and command dispatch (PRD §37).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use url::Url;

use crate::config;
use crate::engine::{self, DownloadRequest};
use crate::fmt;
use crate::http::{DEFAULT_USER_AGENT, HttpConfig};
use crate::integrity::{Algorithm, Checksum};
use crate::limit;
use crate::progress::Reporter;
use crate::shutdown::Cancel;
use crate::storage::{DownloadRecord, RangeState, Status, Store};
use crate::ui;

/// Conservative by default: enough connections to saturate a fast link, few
/// enough that no reasonable origin treats us as abuse (PRD §7).
pub const DEFAULT_CONNECTIONS: usize = 8;
const MAX_CONNECTIONS: usize = 64;

#[derive(Parser, Debug)]
#[command(
    name = "rget",
    version,
    about = "High-performance resumable download manager",
    long_about = "Downloads a URL as fast and as reliably as possible.\n\n\
                  If a download is interrupted, run the same command again — it \
                  resumes automatically.",
    after_help = "EXAMPLES:\n  \
        rget https://example.com/linux.iso\n  \
        rget URL -o ubuntu.iso --dir ~/Downloads\n  \
        rget URL --connections 16 --sha256 <digest>\n  \
        rget https://mirror1/f.iso https://mirror2/f.iso --sha256 <digest>\n  \
        rget list\n  \
        rget resume --all\n  \
        rget forget --all\n  \
        rget forget --all --files\n  \
        rget config --dir ~/Downloads"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,

    #[command(flatten)]
    pub get: GetArgs,

    /// Only report errors and the final result
    #[arg(long, global = true)]
    pub quiet: bool,

    /// Explain what is happening, including resume decisions
    #[arg(short, long, global = true)]
    pub verbose: bool,

    /// Emit machine-readable progress events on stdout, one JSON object per line
    #[arg(long, global = true)]
    pub json: bool,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// List every download this machine knows about
    List,
    /// Show everything recorded about one download
    Info {
        /// Download id, or any unambiguous prefix
        id: String,
    },
    /// Continue an interrupted download
    Resume {
        /// Download id, or any unambiguous prefix
        id: Option<String>,
        /// Resume every interrupted download, one after another
        #[arg(long)]
        all: bool,
    },
    /// Forget download metadata. Leaves files on disk unless --files is set
    Forget {
        /// Download id, or any unambiguous prefix
        id: Option<String>,
        /// Forget every download this machine knows about
        #[arg(long)]
        all: bool,
        /// Also delete the downloaded file(s) from disk
        #[arg(long)]
        files: bool,
    },
    /// Show or change settings
    Config {
        /// Set the folder downloads go to when no --dir is given
        #[arg(long, value_name = "DIR")]
        dir: Option<String>,
        /// Forget the saved folder, so the next download asks again
        #[arg(long, conflicts_with = "dir")]
        reset: bool,
    },
}

#[derive(Args, Debug, Default)]
pub struct GetArgs {
    /// URL to download. Pass several for mirrors of the same file
    #[arg(value_name = "URL")]
    pub urls: Vec<String>,

    /// Write to this filename instead of the one the server suggests
    #[arg(short, long, value_name = "FILE")]
    pub output: Option<String>,

    /// Directory to download into
    #[arg(long, value_name = "DIR")]
    pub dir: Option<String>,

    /// Parallel connections to use when the server supports ranges
    #[arg(short = 'c', long, value_name = "N")]
    pub connections: Option<usize>,

    /// Verify the finished file against this SHA-256 digest
    #[arg(long, value_name = "HEX")]
    pub sha256: Option<String>,

    /// Verify the finished file against this SHA-512 digest
    #[arg(long, value_name = "HEX")]
    pub sha512: Option<String>,

    /// Verify the finished file against this BLAKE3 digest
    #[arg(long, value_name = "HEX")]
    pub blake3: Option<String>,

    /// Cap total download speed, e.g. 20MiB/s
    #[arg(long, value_name = "RATE")]
    pub limit: Option<String>,

    /// Give up on a stalled connection after this long, e.g. 30s
    #[arg(long, value_name = "DURATION", default_value = "30s")]
    pub timeout: String,

    /// Attempts per range before giving up
    #[arg(long, value_name = "N", default_value_t = 10)]
    pub retries: u32,

    /// Extra request header, repeatable: --header 'Key: value'
    #[arg(long = "header", value_name = "KEY:VALUE")]
    pub headers: Vec<String>,

    /// User-Agent to send
    #[arg(long, value_name = "STRING")]
    pub user_agent: Option<String>,

    /// Proxy URL, e.g. http://localhost:8080 or socks5://localhost:1080
    #[arg(long, value_name = "URL")]
    pub proxy: Option<String>,

    /// HTTP basic auth, as user:password
    #[arg(long, value_name = "USER:PASS")]
    pub user: Option<String>,

    /// Replace an existing file at the destination
    #[arg(long)]
    pub overwrite: bool,

    /// Throw away existing progress and download again from the start
    #[arg(long)]
    pub restart: bool,

    /// Do not reserve the file's full size up front
    #[arg(long)]
    pub no_preallocate: bool,
}

impl GetArgs {
    fn checksum(&self) -> Result<Option<Checksum>> {
        let candidates = [
            (Algorithm::Sha256, self.sha256.as_deref()),
            (Algorithm::Sha512, self.sha512.as_deref()),
            (Algorithm::Blake3, self.blake3.as_deref()),
        ];
        let given: Vec<_> = candidates
            .iter()
            .filter_map(|(algo, value)| value.map(|v| (*algo, v)))
            .collect();
        match given.len() {
            0 => Ok(None),
            1 => {
                let (algo, value) = given[0];
                Ok(Some(Checksum::parse(algo, value)?))
            }
            _ => bail!("pass at most one of --sha256, --sha512, --blake3"),
        }
    }

    fn connections(&self) -> Result<usize> {
        let n = self.connections.unwrap_or(DEFAULT_CONNECTIONS);
        if n == 0 {
            bail!("--connections must be at least 1");
        }
        if n > MAX_CONNECTIONS {
            bail!(
                "--connections {n} is more than {MAX_CONNECTIONS}; that many parallel requests \
                 hurts throughput and looks like an attack to most servers"
            );
        }
        Ok(n)
    }

    fn http_config(&self) -> Result<HttpConfig> {
        let mut headers = Vec::new();
        for raw in &self.headers {
            let (k, v) = raw
                .split_once(':')
                .with_context(|| format!("--header must look like 'Key: value', got `{raw}`"))?;
            if k.trim().is_empty() {
                bail!("--header has an empty name: `{raw}`");
            }
            headers.push((k.to_string(), v.to_string()));
        }

        let basic_auth = match &self.user {
            Some(spec) => {
                let (user, pass) = spec
                    .split_once(':')
                    .with_context(|| "--user must look like user:password".to_string())?;
                Some((user.to_string(), pass.to_string()))
            }
            None => None,
        };

        Ok(HttpConfig {
            user_agent: self
                .user_agent
                .clone()
                .unwrap_or_else(|| DEFAULT_USER_AGENT.to_string()),
            timeout: limit::parse_duration(&self.timeout).map_err(|e| anyhow::anyhow!(e))?,
            headers,
            proxy: self.proxy.clone(),
            max_redirects: 10,
            basic_auth,
        })
    }

    fn parse_urls(&self) -> Result<Vec<Url>> {
        let mut out = Vec::with_capacity(self.urls.len());
        for raw in &self.urls {
            let url = Url::parse(raw).with_context(|| format!("`{raw}` is not a valid URL"))?;
            match url.scheme() {
                "http" | "https" => {}
                other => bail!(
                    "`{other}` URLs are not supported yet (only http and https): {}",
                    crate::http::redact(&url)
                ),
            }
            if url.host_str().is_none() {
                bail!("`{raw}` has no host");
            }
            out.push(url);
        }
        Ok(out)
    }

    fn to_request(&self, urls: Vec<Url>) -> Result<DownloadRequest> {
        Ok(DownloadRequest {
            urls,
            output: self.output.clone(),
            dir: self.dir.clone(),
            connections: self.connections()?,
            checksum: self.checksum()?,
            limit: match &self.limit {
                Some(raw) => Some(limit::parse_rate(raw).map_err(|e| anyhow::anyhow!(e))?),
                None => None,
            },
            http: self.http_config()?,
            retries: self.retries,
            overwrite: self.overwrite,
            restart: self.restart,
            preallocate: !self.no_preallocate,
        })
    }
}

/// Process exit codes. `130` is the shell convention for SIGINT.
pub const EXIT_OK: i32 = 0;
pub const EXIT_FAILURE: i32 = 1;
pub const EXIT_INTERRUPTED: i32 = 130;

pub async fn dispatch(cli: Cli) -> Result<i32> {
    let store = Arc::new(Store::open_default()?);

    match &cli.command {
        Some(Command::List) => {
            cmd_list(&store, cli.json)?;
            Ok(EXIT_OK)
        }
        Some(Command::Info { id }) => {
            cmd_info(&store, id, cli.json)?;
            Ok(EXIT_OK)
        }
        Some(Command::Forget { id, all, files }) => {
            cmd_forget(&store, id.as_deref(), *all, *files)?;
            Ok(EXIT_OK)
        }
        Some(Command::Config { dir, reset }) => {
            cmd_config(&store, dir.as_deref(), *reset, cli.json)?;
            Ok(EXIT_OK)
        }
        Some(Command::Resume { id, all }) => cmd_resume(&store, &cli, id.as_deref(), *all).await,
        None => {
            if cli.get.urls.is_empty() {
                // No URL and no subcommand: show help rather than a bare error.
                use clap::CommandFactory;
                Cli::command().print_help()?;
                println!();
                return Ok(EXIT_FAILURE);
            }
            let urls = cli.get.parse_urls()?;
            let mut request = cli.get.to_request(urls)?;
            // Ask where downloads go, once, before anything touches the network.
            request.dir = Some(resolve_dir(&store, &cli)?);
            run_download(store, request, &cli).await
        }
    }
}

/// Settle the destination folder: `--dir`, else the saved setting, else ask
/// (first run only), else the platform's Downloads folder.
fn resolve_dir(store: &Store, cli: &Cli) -> Result<String> {
    let machine_output = cli.json || cli.quiet;
    let resolved = config::resolve_download_dir(store, cli.get.dir.as_deref(), |default| {
        config::prompt_for_download_dir(default, machine_output)
    })?;

    if cli.verbose {
        eprintln!(
            "  downloading into {} ({})",
            config::tildify(&resolved.path),
            match resolved.source {
                config::DirSource::Flag => "--dir",
                config::DirSource::Saved => "saved setting",
                config::DirSource::Prompted => "just chosen",
                config::DirSource::PlatformDefault => "platform default",
            }
        );
    }
    Ok(resolved.path.to_string_lossy().to_string())
}

/// Run one download with a UI attached and signals wired up.
async fn run_download(store: Arc<Store>, request: DownloadRequest, cli: &Cli) -> Result<i32> {
    let mode = ui::Mode::detect(cli.json, cli.quiet, cli.verbose);
    let (reporter, rx) = Reporter::new();
    let cancel = Cancel::new();

    let ui_task = tokio::spawn(ui::run(mode, reporter.stats.clone(), rx, cli.verbose));
    install_signal_handlers(cancel.clone());
    watch_shutdown_deadline(cancel.clone());

    let result = engine::download(store, request, reporter.clone(), cancel).await;

    // Dropping the reporter closes the event channel, which ends the UI task.
    drop(reporter);
    let _ = ui_task.await;

    match result {
        Ok(report) if report.paused => Ok(EXIT_INTERRUPTED),
        Ok(_) => Ok(EXIT_OK),
        Err(err) => Err(err),
    }
}

async fn cmd_resume(store: &Arc<Store>, cli: &Cli, id: Option<&str>, all: bool) -> Result<i32> {
    let targets: Vec<DownloadRecord> = match (id, all) {
        (Some(_), true) => bail!("pass either an id or --all, not both"),
        (Some(id), false) => vec![store.resolve_id(id)?],
        (None, true) => store.list_resumable()?,
        (None, false) => bail!("which download? pass an id or --all (see `rget list`)"),
    };

    if targets.is_empty() {
        if !cli.quiet {
            println!("Nothing to resume.");
        }
        return Ok(EXIT_OK);
    }

    let mut worst = EXIT_OK;
    for record in targets {
        if record.status == Status::Complete {
            if !cli.quiet {
                println!("{} is already complete.", record.filename);
            }
            continue;
        }
        let request = request_from_record(cli, &record)?;
        match run_download(store.clone(), request, cli).await {
            Ok(EXIT_OK) => {}
            Ok(code) => worst = worst.max(code),
            Err(err) => {
                // One failed download must not abandon the rest of --all.
                eprintln!("{}: {err:#}", record.filename);
                worst = EXIT_FAILURE;
            }
        }
    }
    Ok(worst)
}

/// Rebuild a request from what we persisted, so `rget resume <id>` needs no
/// flags at all.
fn request_from_record(cli: &Cli, record: &DownloadRecord) -> Result<DownloadRequest> {
    let mut urls = vec![
        Url::parse(&record.original_url)
            .with_context(|| format!("stored URL is invalid: {}", record.original_url))?,
    ];
    for mirror in &record.mirrors {
        if let Ok(url) = Url::parse(mirror) {
            urls.push(url);
        }
    }

    let checksum = match (&record.expected_checksum, &record.checksum_algorithm) {
        (Some(digest), Some(algo)) => Some(Checksum::parse(algo.parse::<Algorithm>()?, digest)?),
        _ => None,
    };

    let mut request = cli.get.to_request(urls)?;
    // The destination is already decided; do not re-derive it from headers.
    request.output = Some(record.destination.clone());
    request.dir = None;
    if request.checksum.is_none() {
        request.checksum = checksum;
    }
    Ok(request)
}

fn cmd_list(store: &Store, json: bool) -> Result<()> {
    let downloads = store.list()?;
    if json {
        println!("{}", serde_json::to_string_pretty(&downloads)?);
        return Ok(());
    }
    if downloads.is_empty() {
        println!("No downloads yet.");
        return Ok(());
    }

    let style = fmt::Style::stdout();
    println!("{}", list_header(&style));
    for d in downloads {
        println!("{}", list_row(&d, &style));
    }
    Ok(())
}

// Column widths, shared by the header and the rows so they cannot drift apart.
const ID_W: usize = 8;
const NAME_W: usize = 26;
const BAR_W: usize = 10;
const PCT_W: usize = 5;

fn list_header(style: &fmt::Style) -> String {
    style.dim(&format!(
        "{:<ID_W$} {:<NAME_W$} {:<BAR_W$} {:<PCT_W$}  STATUS",
        "ID", "FILE", "PROGRESS", ""
    ))
}

fn list_row(d: &DownloadRecord, style: &fmt::Style) -> String {
    let pct = match d.total_size {
        Some(total) if total > 0 => {
            format!("{:.0}%", (d.durable_bytes as f64 / total as f64) * 100.0)
        }
        _ => "--".to_string(),
    };
    // A miniature version of the download bar, so a glance down the column
    // tells you how far along everything is.
    let (filled, empty) = fmt::bar_parts(d.durable_bytes, d.total_size, BAR_W);
    // Pad *before* styling: ANSI escapes have length but occupy no columns, so
    // `{:<8}` applied to an already-coloured string silently does nothing.
    format!(
        "{} {} {}{} {:>PCT_W$}  {}",
        style.dim(&format!("{:<ID_W$}", d.id)),
        format_args!("{:<NAME_W$}", truncate(&d.filename, NAME_W)),
        style.bright_green(&filled),
        style.dim(&empty),
        pct,
        colour_status(style, d.status),
    )
}

/// Status colours are the fastest way to read a long list: green is done,
/// yellow is waiting for you, red needs attention.
fn colour_status(style: &fmt::Style, status: Status) -> String {
    let text = status.as_str();
    match status {
        Status::Complete => style.green(text),
        Status::Downloading => style.bright_cyan(text),
        Status::Verifying => style.cyan(text),
        Status::Paused => style.yellow(text),
        Status::Failed => style.red(text),
        Status::Pending => style.dim(text),
    }
}

fn cmd_info(store: &Store, id: &str, json: bool) -> Result<()> {
    let record = store.resolve_id(id)?;
    let ranges = store.load_ranges(&record.id)?;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "download": record,
                "ranges": ranges,
            }))?
        );
        return Ok(());
    }

    let style = fmt::Style::stdout();
    let field = |label: &str, value: &str| {
        println!("  {} {value}", style.dim(&format!("{label:<13}")));
    };

    println!(
        "{}  {}",
        style.dim(&record.id),
        style.bold(&record.filename)
    );
    field("status", &colour_status(&style, record.status));
    field("url", &record.original_url);
    if let Some(resolved) = &record.resolved_url {
        if resolved != &record.original_url {
            field("resolved", &style.dim(resolved));
        }
    }
    for mirror in &record.mirrors {
        field("mirror", &style.dim(mirror));
    }
    field("destination", &record.destination);
    field(
        "size",
        &record
            .total_size
            .map(fmt::bytes)
            .unwrap_or_else(|| "unknown".into()),
    );

    let pct = match record.total_size {
        Some(t) if t > 0 => (record.durable_bytes as f64 / t as f64) * 100.0,
        _ => 0.0,
    };
    let (filled, empty) = fmt::bar_parts(record.durable_bytes, record.total_size, 20);
    field(
        "downloaded",
        &format!(
            "{}{}  {}  {}",
            style.bright_green(&filled),
            style.dim(&empty),
            style.bold(&format!("{pct:.1}%")),
            style.dim(&fmt::bytes(record.durable_bytes)),
        ),
    );

    let complete = ranges
        .iter()
        .filter(|r| r.state == RangeState::Complete)
        .count();
    field(
        "ranges",
        &format!(
            "{} {} {}",
            style.magenta(&format!("{complete}/{}", ranges.len())),
            style.dim("complete ·"),
            style.dim(if record.accept_ranges {
                "server supports resuming"
            } else {
                "server cannot resume"
            }),
        ),
    );
    if let Some(etag) = &record.etag {
        field("etag", etag);
    }
    if let Some(lm) = &record.last_modified {
        field("last-modified", lm);
    }
    if let (Some(algo), Some(digest)) = (&record.checksum_algorithm, &record.expected_checksum) {
        field(algo, &style.dim(digest));
    }
    if let Some(err) = &record.error {
        field("last error", &style.red(err));
    }
    Ok(())
}

fn cmd_config(store: &Store, dir: Option<&str>, reset: bool, json: bool) -> Result<()> {
    if reset {
        store.clear_meta(config::DOWNLOAD_DIR_KEY)?;
        let style = fmt::Style::stdout();
        println!(
            "{} Forgot the saved download folder; the next download will ask again.",
            style.bold_green("✓")
        );
        return Ok(());
    }

    if let Some(dir) = dir {
        let path = config::normalise_dir(dir)?;
        config::save_download_dir(store, &path)?;
        let style = fmt::Style::stdout();
        println!(
            "{} Downloads will be saved to {}",
            style.bold_green("✓"),
            style.bold(&config::tildify(&path))
        );
        return Ok(());
    }

    let saved = config::saved_download_dir(store)?;
    let effective = saved.clone().unwrap_or_else(config::platform_download_dir);

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "download_dir": effective.to_string_lossy(),
                "download_dir_is_saved": saved.is_some(),
                "platform_default": config::platform_download_dir().to_string_lossy(),
                "state_database": store.path().to_string_lossy(),
            }))?
        );
        return Ok(());
    }

    let style = fmt::Style::stdout();
    println!(
        "  {} {}{}",
        style.dim("download folder  "),
        style.bold(&config::tildify(&effective)),
        if saved.is_none() {
            style.dim("  (platform default; not saved yet)")
        } else {
            String::new()
        }
    );
    println!(
        "  {} {}",
        style.dim("state database   "),
        style.dim(&store.path().display().to_string())
    );
    println!();
    println!(
        "{}",
        style.dim("Change it with `rget config --dir <path>`, or `--reset` to be asked again.")
    );
    Ok(())
}

fn cmd_forget(store: &Store, id: Option<&str>, all: bool, files: bool) -> Result<()> {
    let targets: Vec<DownloadRecord> = match (id, all) {
        (Some(_), true) => bail!("pass either an id or --all, not both"),
        (Some(id), false) => vec![store.resolve_id(id)?],
        (None, true) => store.list()?,
        (None, false) => bail!("which download? pass an id or --all (see `rget list`)"),
    };

    let style = fmt::Style::stdout();
    if targets.is_empty() {
        println!("Nothing to forget.");
        return Ok(());
    }

    for record in &targets {
        if files {
            let path = std::path::Path::new(&record.destination);
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(err).with_context(|| format!("cannot delete {}", path.display()));
                }
            }
        }
        store.forget(&record.id)?;
    }

    if targets.len() == 1 {
        let record = &targets[0];
        let note = if files {
            format!("\n  deleted {}", record.destination)
        } else {
            format!("\n  the file at {} was left alone", record.destination)
        };
        println!(
            "{} Forgot {} ({}){}",
            style.bold_green("✓"),
            style.dim(&record.id),
            style.bold(&record.filename),
            style.dim(&note)
        );
    } else {
        let note = if files {
            "metadata and files deleted"
        } else {
            "metadata forgotten; files left alone"
        };
        println!(
            "{} Forgot {} downloads ({})",
            style.bold_green("✓"),
            targets.len(),
            style.dim(note)
        );
    }
    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let keep: String = s.chars().take(max.saturating_sub(1)).collect();
    format!("{keep}…")
}

/// First Ctrl+C (or SIGTERM) pauses; a second one exits immediately (PRD §26).
fn install_signal_handlers(cancel: Cancel) {
    tokio::spawn({
        let cancel = cancel.clone();
        async move {
            let mut hits = 0u32;
            loop {
                if tokio::signal::ctrl_c().await.is_err() {
                    return;
                }
                hits += 1;
                if hits == 1 {
                    eprintln!("\nPausing download...");
                    cancel.cancel();
                } else {
                    eprintln!("Forcing exit; progress up to the last checkpoint is saved.");
                    std::process::exit(EXIT_INTERRUPTED);
                }
            }
        }
    });

    #[cfg(unix)]
    tokio::spawn(async move {
        use tokio::signal::unix::{SignalKind, signal};
        let Ok(mut term) = signal(SignalKind::terminate()) else {
            return;
        };
        if term.recv().await.is_some() {
            eprintln!("\nTerminated; saving progress...");
            cancel.cancel();
        }
    });
}

/// Backstop for PRD §26's "do not wait indefinitely": if the engine has not
/// wound down a while after cancellation, exit anyway. Correctness does not
/// depend on this — the last checkpoint is already durable.
fn watch_shutdown_deadline(cancel: Cancel) {
    tokio::spawn(async move {
        cancel.cancelled().await;
        tokio::time::sleep(Duration::from_secs(15)).await;
        eprintln!("Workers did not stop in time; exiting.");
        std::process::exit(EXIT_INTERRUPTED);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    fn parse(args: &[&str]) -> Cli {
        Cli::parse_from(args)
    }

    #[test]
    fn clap_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn plain_url_is_a_download() {
        let cli = parse(&["rget", "https://example.com/f.iso"]);
        assert!(cli.command.is_none());
        assert_eq!(cli.get.urls, vec!["https://example.com/f.iso"]);
        assert_eq!(cli.get.connections().unwrap(), DEFAULT_CONNECTIONS);
    }

    #[test]
    fn several_urls_are_mirrors() {
        let cli = parse(&["rget", "https://a/f.iso", "https://b/f.iso"]);
        assert_eq!(cli.get.urls.len(), 2);
        let urls = cli.get.parse_urls().unwrap();
        assert_eq!(urls[0].host_str(), Some("a"));
    }

    #[test]
    fn subcommands_win_over_urls() {
        let cli = parse(&["rget", "list"]);
        assert!(matches!(cli.command, Some(Command::List)));
        assert!(cli.get.urls.is_empty());

        let cli = parse(&["rget", "resume", "--all"]);
        match cli.command {
            Some(Command::Resume { id, all }) => {
                assert!(id.is_none());
                assert!(all);
            }
            other => panic!("expected resume, got {other:?}"),
        }

        let cli = parse(&["rget", "forget", "--all", "--files"]);
        match cli.command {
            Some(Command::Forget { id, all, files }) => {
                assert!(id.is_none());
                assert!(all);
                assert!(files);
            }
            other => panic!("expected forget, got {other:?}"),
        }

        let cli = parse(&["rget", "forget", "a82fd1", "--files"]);
        match cli.command {
            Some(Command::Forget { id, all, files }) => {
                assert_eq!(id.as_deref(), Some("a82fd1"));
                assert!(!all);
                assert!(files);
            }
            other => panic!("expected forget, got {other:?}"),
        }
    }

    #[test]
    fn global_flags_work_with_subcommands() {
        let cli = parse(&["rget", "--json", "list"]);
        assert!(cli.json);
        let cli = parse(&["rget", "list", "--json"]);
        assert!(cli.json);
    }

    #[test]
    fn rejects_unsupported_schemes() {
        let cli = parse(&["rget", "ftp://example.com/f.iso"]);
        let err = cli.get.parse_urls().unwrap_err().to_string();
        assert!(err.contains("not supported"), "{err}");

        let cli = parse(&["rget", "not a url"]);
        assert!(cli.get.parse_urls().is_err());
    }

    #[test]
    fn rejects_multiple_checksums() {
        let cli = parse(&[
            "rget",
            "https://a/f",
            "--sha256",
            &"a".repeat(64),
            "--blake3",
            &"b".repeat(64),
        ]);
        assert!(cli.get.checksum().is_err());
    }

    #[test]
    fn accepts_one_checksum() {
        let digest = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        let cli = parse(&["rget", "https://a/f", "--sha256", digest]);
        let checksum = cli.get.checksum().unwrap().unwrap();
        assert_eq!(checksum.algorithm, Algorithm::Sha256);
        assert_eq!(checksum.expected, digest);
    }

    #[test]
    fn validates_connection_counts() {
        let cli = parse(&["rget", "https://a/f", "-c", "0"]);
        assert!(cli.get.connections().is_err());
        let cli = parse(&["rget", "https://a/f", "-c", "1000"]);
        assert!(cli.get.connections().is_err());
        let cli = parse(&["rget", "https://a/f", "-c", "16"]);
        assert_eq!(cli.get.connections().unwrap(), 16);
    }

    #[test]
    fn parses_headers_and_auth() {
        let cli = parse(&[
            "rget",
            "https://a/f",
            "--header",
            "X-Token: abc",
            "--user",
            "alice:s3cret",
        ]);
        let cfg = cli.get.http_config().unwrap();
        assert_eq!(
            cfg.headers,
            vec![("X-Token".to_string(), " abc".to_string())]
        );
        assert_eq!(cfg.basic_auth, Some(("alice".into(), "s3cret".into())));

        let cli = parse(&["rget", "https://a/f", "--header", "nonsense"]);
        assert!(cli.get.http_config().is_err());
        let cli = parse(&["rget", "https://a/f", "--user", "nocolon"]);
        assert!(cli.get.http_config().is_err());
    }

    #[test]
    fn parses_limits_and_timeouts() {
        let cli = parse(&[
            "rget",
            "https://a/f",
            "--limit",
            "20MiB/s",
            "--timeout",
            "45s",
        ]);
        let req = cli.get.to_request(cli.get.parse_urls().unwrap()).unwrap();
        assert_eq!(req.limit, Some(20 * 1024 * 1024));
        assert_eq!(req.http.timeout, Duration::from_secs(45));

        let cli = parse(&["rget", "https://a/f", "--limit", "fast"]);
        assert!(cli.get.to_request(vec![]).is_err());
    }

    #[test]
    fn preallocation_is_on_by_default() {
        let cli = parse(&["rget", "https://a/f"]);
        assert!(cli.get.to_request(vec![]).unwrap().preallocate);
        let cli = parse(&["rget", "https://a/f", "--no-preallocate"]);
        assert!(!cli.get.to_request(vec![]).unwrap().preallocate);
    }

    /// Remove ANSI sequences so we can measure what the terminal actually
    /// shows, rather than how many bytes we wrote.
    fn visible(s: &str) -> String {
        let mut out = String::new();
        let mut chars = s.chars();
        while let Some(c) = chars.next() {
            if c == '\x1b' {
                for c in chars.by_ref() {
                    if c == 'm' {
                        break;
                    }
                }
            } else {
                out.push(c);
            }
        }
        out
    }

    /// Column (not byte) at which `needle` starts. The bar glyphs are three
    /// bytes each, so byte offsets would be meaningless here.
    fn column_of(line: &str, needle: &str) -> Option<usize> {
        let byte = line.find(needle)?;
        Some(line[..byte].chars().count())
    }

    fn listed(id: &str, filename: &str, total: Option<u64>, done: u64) -> DownloadRecord {
        DownloadRecord {
            id: id.into(),
            original_url: "https://x.example/f".into(),
            resolved_url: None,
            mirrors: vec![],
            destination: "/tmp/f".into(),
            filename: filename.into(),
            total_size: total,
            etag: None,
            last_modified: None,
            content_type: None,
            accept_ranges: true,
            expected_checksum: None,
            checksum_algorithm: None,
            file_cookie: "cookie".into(),
            file_dev: None,
            file_ino: None,
            durable_bytes: done,
            status: Status::Paused,
            error: None,
            created_at: 0,
            updated_at: 0,
            completed_at: None,
        }
    }

    /// Padding an already-coloured string is a silent no-op, because escape
    /// sequences have length but occupy no columns. This is the guard against
    /// that whole class of bug.
    #[test]
    fn list_columns_line_up_with_colour_on() {
        let style = fmt::Style::new(true);
        if !style.is_enabled() {
            return; // NO_COLOR set in this environment.
        }
        let header = visible(&list_header(&style));
        let status_col = column_of(&header, "STATUS").expect("header has a STATUS column");

        for record in [
            listed("ab12cd", "short.iso", Some(1000), 500),
            listed(
                "ef34gh",
                "a-considerably-longer-filename.tar.gz",
                Some(1 << 30),
                0,
            ),
            listed("ij56kl", "unknown-size.bin", None, 0),
            listed("mn78op", "done.bin", Some(10), 10),
        ] {
            let row = visible(&list_row(&record, &style));
            let plain_row = visible(&list_row(&record, &fmt::Style::new(false)));
            assert_eq!(
                row, plain_row,
                "styled and unstyled rows must occupy identical columns"
            );
            let status = visible(&colour_status(&style, record.status));
            let at = column_of(&row, &status).expect("row has a status");
            assert_eq!(
                at, status_col,
                "status column misaligned for {}: {row:?} vs header {header:?}",
                record.filename
            );
        }
    }

    #[test]
    fn list_bar_reflects_progress() {
        let style = fmt::Style::new(false);
        assert!(list_row(&listed("a", "f", Some(100), 0), &style).contains("░"));
        let full = list_row(&listed("a", "f", Some(100), 100), &style);
        assert!(full.contains("█"));
        assert!(
            !full.contains("░"),
            "a finished bar should be solid: {full}"
        );
        // Unknown size cannot claim progress it does not know about.
        assert!(list_row(&listed("a", "f", None, 50), &style).contains("--"));
    }

    #[test]
    fn truncates_long_filenames_for_the_table() {
        assert_eq!(truncate("short.iso", 24), "short.iso");
        let long = truncate(&"x".repeat(40), 10);
        assert_eq!(long.chars().count(), 10);
        assert!(long.ends_with('…'));
    }
}
