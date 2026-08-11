use anyhow::Result;
use clap::Parser;
use rget_next::cli::{self, Cli};

fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.verbose);

    // A current-thread runtime would serialise the socket reads that are the
    // whole point of this tool.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let code = match runtime.block_on(cli::dispatch(cli)) {
        Ok(code) => code,
        Err(err) => {
            eprintln!("error: {err:#}");
            cli::EXIT_FAILURE
        }
    };

    // Drop the runtime before exiting so in-flight blocking tasks (an fsync, a
    // SQLite commit) finish rather than being cut off mid-write.
    drop(runtime);
    std::process::exit(code);
}

/// `RUST_LOG=debug rget URL` (PRD §29). `--verbose` implies debug for our own
/// crate without turning on every dependency's logging.
fn init_tracing(verbose: bool) {
    use tracing_subscriber::EnvFilter;

    let filter = match std::env::var("RUST_LOG") {
        Ok(value) => EnvFilter::new(value),
        Err(_) if verbose => EnvFilter::new("rget_next=debug"),
        Err(_) => EnvFilter::new("warn"),
    };

    // Logs go to stderr so they never mix into `--json` output on stdout.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(false)
        .try_init();
}
