//! CLI entry point: install tracing, parse the command line (see [`cli`]), and
//! dispatch to the matching runner in [`commands`].

mod cli;
mod commands;

use anyhow::Result;
use tracing_subscriber::EnvFilter;

/// Install the tracing subscriber. Defaults to `info`; override with `RUST_LOG`.
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let matches = cli::cli().get_matches();

    match matches.subcommand() {
        Some(("supervise", sub)) => commands::run_supervise(sub).await,
        Some(("spawn", sub)) => commands::run_spawn(sub).await,
        Some(("list", sub)) => commands::run_list(sub).await,
        Some(("query", sub)) => commands::run_query(sub).await,
        Some(("signal", sub)) => commands::run_signal(sub).await,
        Some(("stop", sub)) => commands::run_stop(sub).await,
        Some(("stdin", sub)) => commands::run_stdin(sub).await,
        Some(("subscribe", sub)) => commands::run_subscribe(sub).await,
        Some(("forget", sub)) => commands::run_forget(sub).await,
        Some(("watch", sub)) => commands::run_watch(sub).await,
        _ => unreachable!("subcommand_required(true) guarantees a match"),
    }
}
