//! Binary-only module: the clap command tree for the `mesh-supervisor` CLI.
//! Runtime behavior lives in [`crate::commands`].

use clap::{Command, arg};
use iroh::{EndpointId, RelayUrl};

/// Parse a memory size with an optional K/M/G suffix (e.g. `256M`) into bytes.
/// Errors are returned as `String` so clap renders them as value-parser errors.
fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let (num, mult) = match s.chars().last() {
        Some('K' | 'k') => (&s[..s.len() - 1], 1024u64),
        Some('M' | 'm') => (&s[..s.len() - 1], 1024 * 1024),
        Some('G' | 'g') => (&s[..s.len() - 1], 1024 * 1024 * 1024),
        _ => (s, 1),
    };

    let n = num
        .trim()
        .parse::<u64>()
        .map_err(|e| format!("invalid size {s:?}: {e}"))?;
    n.checked_mul(mult)
        .ok_or_else(|| format!("size {s:?} overflows u64"))
}

/// `<endpoint>` positional: the supervisor to dial.
fn endpoint_arg() -> clap::Arg {
    arg!(<endpoint> "EndpointId of the supervisor to dial")
        .value_parser(clap::value_parser!(EndpointId))
}

/// `<handle>` positional: the target process handle.
fn handle_arg() -> clap::Arg {
    arg!(<handle> "process handle").value_parser(clap::value_parser!(u64))
}

/// Client-side policy flags (retries, read timeout, telemetry freshness).
fn client_args(cmd: Command) -> Command {
    cmd.arg(
        arg!(--"read-timeout" <secs> "read timeout in seconds")
            .value_parser(clap::value_parser!(u64))
            .env("P2P_CLIENT_READ_TIMEOUT_SECS")
            .required(false),
    )
    .arg(
        arg!(--"max-retries" <n> "max connection retries")
            .value_parser(clap::value_parser!(u32))
            .env("P2P_CLIENT_MAX_RETRIES")
            .required(false),
    )
    .arg(
        arg!(--"retry-base-delay-ms" <ms> "initial retry delay in ms")
            .value_parser(clap::value_parser!(u64))
            .env("P2P_CLIENT_RETRY_BASE_DELAY_MS")
            .required(false),
    )
    .arg(
        arg!(--"retry-max-delay-ms" <ms> "max retry delay in ms")
            .value_parser(clap::value_parser!(u64))
            .env("P2P_CLIENT_RETRY_MAX_DELAY_MS")
            .required(false),
    )
    .arg(
        arg!(--"max-tick-age-ms" <ms> "telemetry freshness window in ms")
            .value_parser(clap::value_parser!(u64))
            .env("P2P_TELEMETRY_MAX_TICK_AGE_MS")
            .required(false),
    )
}

/// Supervisor-side policy flags (timeouts, rate limits, telemetry cadence).
fn supervisor_args(cmd: Command) -> Command {
    cmd.arg(
        arg!(--"request-timeout" <secs> "request read timeout in seconds")
            .value_parser(clap::value_parser!(u64))
            .env("P2P_SUPERVISOR_REQUEST_TIMEOUT_SECS")
            .required(false),
    )
    .arg(
        arg!(--"stop-deadline-secs" <secs> "grace period before SIGKILL on stop/shutdown")
            .value_parser(clap::value_parser!(u64))
            .env("P2P_SUPERVISOR_STOP_DEADLINE_SECS")
            .required(false),
    )
    .arg(
        arg!(--"rate-burst" <n> "rate limiter burst capacity")
            .value_parser(clap::value_parser!(f64))
            .env("P2P_RATE_BURST")
            .required(false),
    )
    .arg(
        arg!(--"rate-refill" <n> "rate limiter tokens per second")
            .value_parser(clap::value_parser!(f64))
            .env("P2P_RATE_REFILL")
            .required(false),
    )
    .arg(
        arg!(--"rate-max-buckets" <n> "max tracked peer buckets")
            .value_parser(clap::value_parser!(usize))
            .env("P2P_RATE_MAX_BUCKETS")
            .required(false),
    )
    .arg(
        arg!(--"rate-eviction-ttl-secs" <secs> "idle peer bucket TTL")
            .value_parser(clap::value_parser!(u64))
            .env("P2P_RATE_EVICTION_TTL_SECS")
            .required(false),
    )
    .arg(
        arg!(--"sample-interval-ms" <ms> "telemetry sample interval in ms")
            .value_parser(clap::value_parser!(u64))
            .env("P2P_TELEMETRY_SAMPLE_INTERVAL_MS")
            .required(false),
    )
    .arg(
        arg!(--"max-tick-age-ms" <ms> "telemetry freshness window in ms")
            .value_parser(clap::value_parser!(u64))
            .env("P2P_TELEMETRY_MAX_TICK_AGE_MS")
            .required(false),
    )
}

/// The full command tree; `supervise` is the server role, everything else is a
/// one-shot client, and `watch` is a telemetry consumer.
pub fn cli() -> Command {
    Command::new("mesh-supervisor")
        .about("distributed process-control + telemetry plane over an iroh P2P mesh")
        .long_about(
            "Each node runs a supervisor that owns an iroh (QUIC) endpoint and the \
             local child processes; the processes never touch the network. Control \
             is acked RPC over a dedicated ALPN; telemetry is lossy gossip. Run \
             `supervise` to host an endpoint and accept control connections; the \
             other subcommands act as a client that dials a supervisor by its \
             EndpointId and issues one control request.",
        )
        .version(env!("CARGO_PKG_VERSION"))
        .subcommand_required(true)
        .arg_required_else_help(true)
        .arg(
            arg!(--relay <url> "home relay URL for WAN reachability")
                .global(true)
                .value_parser(clap::value_parser!(RelayUrl))
                .env("P2P_RELAY")
                .required(false),
        )
        .arg(
            arg!(--"topic-secret" <s> "shared secret for a private telemetry topic")
                .global(true)
                .env("P2P_TOPIC_SECRET")
                .required(false),
        )
        .after_help(
            "EXAMPLES:\n  \
             # Run as a supervisor; prints this node's EndpointId\n  \
             mesh-supervisor supervise\n\n  \
             # Client: spawn a process on a remote supervisor\n  \
             mesh-supervisor spawn <endpoint-id> -- sleep 60\n\n  \
             # Client: list / inspect handles on a supervisor\n  \
             mesh-supervisor list <endpoint-id>\n  \
             mesh-supervisor query <endpoint-id> <handle>",
        )
        .subcommand(supervisor_args(
            Command::new("supervise")
                .about("run as a supervisor: bind an endpoint and accept control connections")
                .arg(
                    arg!(--store <path> "path to the redb store (persists identity + process records)")
                        .default_value("mesh-supervisor.redb"),
                )
                .arg(
                    arg!(--allow <id> ... "EndpointId(s) allowed full control of this supervisor")
                        .value_parser(clap::value_parser!(EndpointId))
                        .required(false),
                )
                .arg(
                    arg!(--"allow-read" <id> ... "EndpointId(s) allowed read-only ops (list/query/subscribe)")
                        .value_parser(clap::value_parser!(EndpointId))
                        .required(false),
                )
                .arg(arg!(--open "accept ALL clients with full control (dangerous; the explicit alternative to an allowlist)"))
                .arg(
                    arg!([peer] ... "EndpointId(s) of other supervisors to bootstrap the telemetry mesh")
                        .value_parser(clap::value_parser!(EndpointId)),
                ),
        ))
        .subcommand(client_args(
            Command::new("spawn")
                .about("client: launch a process on a remote supervisor")
                .arg(endpoint_arg())
                .arg(
                    arg!(--mem <size> "memory cap, e.g. 256M (K/M/G suffixes)")
                        .value_parser(parse_size)
                        .required(false),
                )
                .arg(
                    arg!(--pids <n> "max processes/threads (cgroup pids.max, Linux)")
                        .value_parser(clap::value_parser!(u64))
                        .required(false),
                )
                .arg(
                    arg!(--cpu <secs> "total CPU-time cap in seconds (RLIMIT_CPU)")
                        .value_parser(clap::value_parser!(u64))
                        .required(false),
                )
                .arg(
                    arg!(--restart <policy> "restart policy on exit")
                        .value_parser(["never", "on-failure", "always"])
                        .default_value("never"),
                )
                .arg(
                    arg!(--"max-retries" <n> "consecutive fast restarts before giving up (0 = unlimited)")
                        .value_parser(clap::value_parser!(u32))
                        .default_value("5"),
                )
                .arg(arg!(--isolate "run in fresh namespaces: no network, can't read the store (Linux)"))
                .arg(
                    arg!(<cmd> ... "the command and arguments to run")
                        .trailing_var_arg(true)
                        .allow_hyphen_values(true)
                        .num_args(1..),
                ),
        ))
        .subcommand(client_args(
            Command::new("list")
                .about("client: list the process handles tracked by a supervisor")
                .arg(endpoint_arg()),
        ))
        .subcommand(client_args(
            Command::new("query")
                .about("client: query a process's status")
                .arg(endpoint_arg())
                .arg(handle_arg()),
        ))
        .subcommand(client_args(
            Command::new("signal")
                .about("client: send a signal to a process")
                .arg(endpoint_arg())
                .arg(handle_arg())
                .arg(
                    arg!(<sig> "signal number, e.g. 15 (SIGTERM)")
                        .value_parser(clap::value_parser!(i32)),
                ),
        ))
        .subcommand(client_args(
            Command::new("stop")
                .about("client: stop a process and disable its restart policy")
                .arg(endpoint_arg())
                .arg(handle_arg()),
        ))
        .subcommand(client_args(
            Command::new("stdin")
                .about("client: pipe this command's stdin to a process")
                .arg(endpoint_arg())
                .arg(handle_arg()),
        ))
        .subcommand(client_args(
            Command::new("subscribe")
                .about("client: stream a process's stdout until it exits")
                .arg(endpoint_arg())
                .arg(handle_arg()),
        ))
        .subcommand(client_args(
            Command::new("forget")
                .about("client: drop a finished process's record from a supervisor")
                .arg(endpoint_arg())
                .arg(handle_arg()),
        ))
        .subcommand(client_args(
            Command::new("watch")
                .about("client: join the telemetry topic and print stat ticks live")
                .arg(
                    arg!(<bootstrap> ... "EndpointId(s) of supervisor(s) to bootstrap into the topic")
                        .value_parser(clap::value_parser!(EndpointId))
                        .num_args(1..),
                ),
        ))
}
