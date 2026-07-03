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
        // Named --connect-retries (not --max-retries) so it can't collide with
        // spawn's restart-cap flag of that name; the env var keeps its old name.
        arg!(--"connect-retries" <n> "max connection retries")
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
        .subcommand(
            Command::new("completions")
                .about("local: print a shell completion script to stdout")
                .arg(
                    arg!(<shell> "shell to generate completions for")
                        .value_parser(clap::value_parser!(clap_complete::Shell)),
                ),
        )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::cli;
    use mesh_supervisor::config::{ClientConfig, SupervisorConfig};

    /// Drive the real parser for every subcommand and apply its config
    /// overrides. Catches what only explodes at runtime: duplicate arg ids
    /// (clap's uniqueness assert) and `get_one` on a key the subcommand
    /// doesn't define.
    #[test]
    fn every_subcommand_parses_and_applies_config_overrides() {
        let id = iroh::SecretKey::generate().public().to_string();
        let id = id.as_str();

        let cases: Vec<Vec<&str>> = vec![
            vec!["mesh-supervisor", "supervise", "--open"],
            vec!["mesh-supervisor", "spawn", id, "true"],
            vec!["mesh-supervisor", "list", id],
            vec!["mesh-supervisor", "query", id, "1"],
            vec!["mesh-supervisor", "signal", id, "1", "15"],
            vec!["mesh-supervisor", "stop", id, "1"],
            vec!["mesh-supervisor", "stdin", id, "1"],
            vec!["mesh-supervisor", "subscribe", id, "1"],
            vec!["mesh-supervisor", "forget", id, "1"],
            vec!["mesh-supervisor", "watch", id],
            vec!["mesh-supervisor", "completions", "zsh"],
        ];

        for argv in cases {
            let matches = cli()
                .try_get_matches_from(&argv)
                .unwrap_or_else(|e| panic!("{argv:?} failed to parse: {e}"));
            let (name, sub) = matches.subcommand().expect("subcommand required");
            match name {
                "supervise" => SupervisorConfig::default().with_cli_overrides(sub),
                "completions" => {} // local-only: no config knobs to apply
                _ => ClientConfig::default().with_cli_overrides(sub),
            }
        }
    }

    /// Flags override the struct defaults; untouched knobs keep them.
    #[test]
    fn cli_flags_override_config_defaults() {
        let id = iroh::SecretKey::generate().public().to_string();

        let matches = cli()
            .try_get_matches_from([
                "mesh-supervisor",
                "list",
                &id,
                "--read-timeout",
                "7",
                "--connect-retries",
                "9",
            ])
            .expect("parse list");
        let (_, sub) = matches.subcommand().expect("subcommand");
        let mut cfg = ClientConfig::default();
        cfg.with_cli_overrides(sub);
        assert_eq!(cfg.read_timeout, Duration::from_secs(7));
        assert_eq!(cfg.max_retries, 9);
        assert_eq!(
            cfg.retry_base_delay,
            ClientConfig::default().retry_base_delay,
            "untouched knob must keep its default"
        );

        let matches = cli()
            .try_get_matches_from([
                "mesh-supervisor",
                "supervise",
                "--open",
                "--sample-interval-ms",
                "250",
            ])
            .expect("parse supervise");
        let (_, sub) = matches.subcommand().expect("subcommand");
        let mut cfg = SupervisorConfig::default();
        cfg.with_cli_overrides(sub);
        assert_eq!(cfg.telemetry.sample_interval, Duration::from_millis(250));
    }
}
