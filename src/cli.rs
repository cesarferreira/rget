//! Argument parsing and command dispatch (PRD §37).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use url::Url;

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
        rget resume --all"
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
    /// Forget a download's metadata. Never deletes the downloaded file
    Forget {
        /// Download id, or any unambiguous prefix
        id: String,
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
        Some(Command::Forget { id }) => {
            cmd_forget(&store, id)?;
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
            let request = cli.get.to_request(urls)?;
            run_download(store, request, &cli).await
        }
    }
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

    println!("{:<8} {:<24} {:>8}  STATUS", "ID", "FILE", "PROGRESS");
    for d in downloads {
        let progress = match d.total_size {
            Some(total) if total > 0 => {
                format!("{:.0}%", (d.durable_bytes as f64 / total as f64) * 100.0)
            }
            _ => "--".to_string(),
        };
        let name = truncate(&d.filename, 24);
        println!("{:<8} {:<24} {:>8}  {}", d.id, name, progress, d.status);
    }
    Ok(())
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

    println!("{}  {}", record.id, record.filename);
    println!("  status        {}", record.status);
    println!("  url           {}", record.original_url);
    if let Some(resolved) = &record.resolved_url {
        if resolved != &record.original_url {
            println!("  resolved      {resolved}");
        }
    }
    for mirror in &record.mirrors {
        println!("  mirror        {mirror}");
    }
    println!("  destination   {}", record.destination);
    println!(
        "  size          {}",
        record
            .total_size
            .map(fmt::bytes)
            .unwrap_or_else(|| "unknown".into())
    );
    println!(
        "  downloaded    {} ({})",
        fmt::bytes(record.durable_bytes),
        match record.total_size {
            Some(t) if t > 0 => format!("{:.1}%", (record.durable_bytes as f64 / t as f64) * 100.0),
            _ => "--".into(),
        }
    );
    println!(
        "  ranges        {} total, {} complete",
        ranges.len(),
        ranges
            .iter()
            .filter(|r| r.state == RangeState::Complete)
            .count()
    );
    println!(
        "  resumable     {}",
        if record.accept_ranges { "yes" } else { "no" }
    );
    if let Some(etag) = &record.etag {
        println!("  etag          {etag}");
    }
    if let Some(lm) = &record.last_modified {
        println!("  last-modified {lm}");
    }
    if let (Some(algo), Some(digest)) = (&record.checksum_algorithm, &record.expected_checksum) {
        println!("  {algo}        {digest}");
    }
    if let Some(err) = &record.error {
        println!("  last error    {err}");
    }
    Ok(())
}

fn cmd_forget(store: &Store, id: &str) -> Result<()> {
    let record = store.resolve_id(id)?;
    store.forget(&record.id)?;
    println!(
        "Forgot {} ({}). The file at {} was left alone.",
        record.id, record.filename, record.destination
    );
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

    #[test]
    fn truncates_long_filenames_for_the_table() {
        assert_eq!(truncate("short.iso", 24), "short.iso");
        let long = truncate(&"x".repeat(40), 10);
        assert_eq!(long.chars().count(), 10);
        assert!(long.ends_with('…'));
    }
}
