use std::collections::HashSet;

use anyhow::Result;
use clap::{ArgMatches, Command, arg};
use iroh::{EndpointAddr, EndpointId, RelayUrl, protocol::Router};
use iroh_gossip::net::Gossip;
use mesh_supervisor::{
    cgroup::Cgroups, client, config::ClientConfig, config::SupervisorConfig, endpoint,
    process::ProcessManager, proto, store::Store, supervisor, telemetry,
};

use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

/// Collect zero or more typed `EndpointId` positional args into a `Vec`.
/// Parsing already happened at the clap layer, so this cannot fail.
fn parse_peers(sub: &ArgMatches, key: &str) -> Vec<EndpointId> {
    sub.get_many::<EndpointId>(key)
        .into_iter()
        .flatten()
        .copied()
        .collect()
}

/// Build the address to dial a supervisor: its id, routed via `relay` when set.
fn dial_addr(id: EndpointId, relay: &Option<RelayUrl>) -> EndpointAddr {
    let addr = EndpointAddr::new(id);
    match relay {
        Some(r) => addr.with_relay_url(r.clone()),
        None => addr,
    }
}

/// Shared setup for every client subcommand: parse config, build an endpoint,
/// send one request, and tear the endpoint down. Returns the supervisor response.
async fn client_call(sub: &ArgMatches, req: proto::Request) -> Result<proto::Response> {
    let mut cfg = ClientConfig::default();
    cfg.with_cli_overrides(sub);
    let target = *sub.get_one::<EndpointId>("endpoint").expect("required arg");
    let relay = sub.get_one::<RelayUrl>("relay").cloned();

    let (endpoint, _book) = endpoint::build_endpoint(vec![], None, relay.clone()).await?;
    let resp = client::request(&endpoint, dial_addr(target, &relay), req, &cfg).await?;
    endpoint.close().await;
    Ok(resp)
}

/// Shared setup for streaming client subcommands (`stdin`, `subscribe`). The
/// caller supplies the async body; endpoint teardown always happens.
async fn client_streaming<F, Fut, T>(sub: &ArgMatches, f: F) -> Result<T>
where
    F: FnOnce(iroh::Endpoint, EndpointAddr, ClientConfig) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut cfg = ClientConfig::default();
    cfg.with_cli_overrides(sub);
    let target = *sub.get_one::<EndpointId>("endpoint").expect("required arg");
    let relay = sub.get_one::<RelayUrl>("relay").cloned();

    let (endpoint, _book) = endpoint::build_endpoint(vec![], None, relay.clone()).await?;
    // Clone the cheap Arc-like endpoint so the streaming future owns its own
    // handle while we retain one to close after it finishes.
    let result = f(endpoint.clone(), dial_addr(target, &relay), cfg).await;
    endpoint.close().await;
    result
}

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

/// Render a process's memory usage in MiB, or `-` when unavailable (no cgroup).
fn mem_str(usage: Option<proto::Usage>) -> String {
    usage.map_or_else(
        || "-".to_string(),
        |u| format!("{}MiB", u.mem_bytes / (1024 * 1024)),
    )
}

/// Install the tracing subscriber. Defaults to `info`; override with `RUST_LOG`.
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
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

fn cli() -> Command {
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
                .arg(
                    arg!(<endpoint> "EndpointId of the supervisor to dial")
                        .value_parser(clap::value_parser!(EndpointId)),
                )
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
                .arg(arg!(<endpoint> "EndpointId of the supervisor to dial").value_parser(clap::value_parser!(EndpointId))),
        ))
        .subcommand(client_args(
            Command::new("query")
                .about("client: query a process's status")
                .arg(arg!(<endpoint> "EndpointId of the supervisor to dial").value_parser(clap::value_parser!(EndpointId)))
                .arg(arg!(<handle> "process handle").value_parser(clap::value_parser!(u64))),
        ))
        .subcommand(client_args(
            Command::new("signal")
                .about("client: send a signal to a process")
                .arg(arg!(<endpoint> "EndpointId of the supervisor to dial").value_parser(clap::value_parser!(EndpointId)))
                .arg(arg!(<handle> "process handle").value_parser(clap::value_parser!(u64)))
                .arg(
                    arg!(<sig> "signal number, e.g. 15 (SIGTERM)")
                        .value_parser(clap::value_parser!(i32)),
                ),
        ))
        .subcommand(client_args(
            Command::new("stop")
                .about("client: stop a process and disable its restart policy")
                .arg(arg!(<endpoint> "EndpointId of the supervisor to dial").value_parser(clap::value_parser!(EndpointId)))
                .arg(arg!(<handle> "process handle").value_parser(clap::value_parser!(u64))),
        ))
        .subcommand(client_args(
            Command::new("stdin")
                .about("client: pipe this command's stdin to a process")
                .arg(arg!(<endpoint> "EndpointId of the supervisor to dial").value_parser(clap::value_parser!(EndpointId)))
                .arg(arg!(<handle> "process handle").value_parser(clap::value_parser!(u64))),
        ))
        .subcommand(client_args(
            Command::new("subscribe")
                .about("client: stream a process's stdout until it exits")
                .arg(arg!(<endpoint> "EndpointId of the supervisor to dial").value_parser(clap::value_parser!(EndpointId)))
                .arg(arg!(<handle> "process handle").value_parser(clap::value_parser!(u64))),
        ))
        .subcommand(client_args(
            Command::new("forget")
                .about("client: drop a finished process's record from a supervisor")
                .arg(arg!(<endpoint> "EndpointId of the supervisor to dial").value_parser(clap::value_parser!(EndpointId)))
                .arg(arg!(<handle> "process handle").value_parser(clap::value_parser!(u64))),
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

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let matches = cli().get_matches();

    match matches.subcommand() {
        Some(("supervise", sub)) => {
            let mut cfg = SupervisorConfig::default();
            cfg.with_cli_overrides(sub);

            let bootstrap = parse_peers(sub, "peer");
            let store_path = sub.get_one::<String>("store").expect("has default");

            // Open the store first: it owns this node's stable identity + records.
            let store = Store::open(store_path)?;
            let secret_key = store.secret_key()?;
            let loaded = store.load()?;
            info!(store = %store_path, reloaded = loaded.records.len(), "store opened");

            // Linux only: per-child cgroup memory caps when a delegated subtree exists.
            let cgroups = Cgroups::detect();
            info!(
                cgroups = cgroups.is_some(),
                "resource isolation (cgroup v2 caps)"
            );

            // Deny-by-default: the operator must choose a posture, never silently open.
            let open = sub.get_flag("open");
            let control: HashSet<EndpointId> = parse_peers(sub, "allow").into_iter().collect();
            let read: HashSet<EndpointId> = parse_peers(sub, "allow-read").into_iter().collect();
            let relay = sub.get_one::<RelayUrl>("relay").cloned();

            if !open && control.is_empty() && read.is_empty() {
                anyhow::bail!(
                    "no authorization configured: pass --allow <id>... / --allow-read <id>..., or --open to accept all clients"
                );
            }
            if open {
                let scope = if relay.is_some() {
                    "anyone on the internet who knows this id"
                } else {
                    "anyone who can reach this supervisor"
                };
                warn!("--open: control is open to {scope}");
            }
            info!(
                control = control.len(),
                read = read.len(),
                open,
                "control authz"
            );
            let authz = supervisor::Authz {
                open,
                control,
                read,
            };

            info!("starting supervisor (binding endpoint, ~7s)...");
            let (endpoint, book) = endpoint::build_endpoint(
                vec![proto::CONTROL_ALPN.to_vec(), iroh_gossip::ALPN.to_vec()],
                Some(secret_key.clone()),
                relay.clone(),
            )
            .await?;

            // On WAN, seed bootstrap peers' relay addrs so gossip can reach them by id.
            if let Some(r) = &relay {
                for id in &bootstrap {
                    book.add_endpoint_info(EndpointAddr::new(*id).with_relay_url(r.clone()));
                }
            }
            let id = endpoint.id();

            let gossip = Gossip::builder().spawn(endpoint.clone());
            let supervisor = supervisor::Supervisor::new(
                ProcessManager::with_store(store, loaded, cgroups, cfg.stop_deadline),
                authz,
                cfg.clone(),
            );
            let router = Router::builder(endpoint)
                .accept(proto::CONTROL_ALPN, supervisor.clone())
                .accept(iroh_gossip::ALPN, gossip.clone())
                .spawn();

            // Telemetry publisher: sample the process table and gossip ticks (lossy).
            let topic = gossip
                .subscribe(
                    telemetry::topic_for(sub.get_one::<String>("topic-secret").map(String::as_str)),
                    bootstrap,
                )
                .await?;
            tokio::spawn(telemetry::publish_loop(
                topic,
                secret_key,
                supervisor.procs(),
                cfg.telemetry.clone(),
            ));

            info!(endpoint_id = %id, "supervisor up (LAN/mDNS, persistent identity)");
            info!("dial with: mesh-supervisor list {id}");
            info!("watch telemetry with: mesh-supervisor watch {id}");
            info!("waiting for control connections (Ctrl-C to quit)");

            tokio::signal::ctrl_c().await?;

            info!("shutting down");
            router.shutdown().await?;

            Ok(())
        }
        Some(("spawn", sub)) => {
            let target = *sub.get_one::<EndpointId>("endpoint").expect("required arg");

            let mut argv = sub
                .get_many::<String>("cmd")
                .expect("required arg")
                .cloned();
            let cmd = argv.next().expect("num_args(1..) guarantees one");
            let args: Vec<String> = argv.collect();

            let limits = proto::Limits {
                memory: sub.get_one::<u64>("mem").copied(),
                pids: sub.get_one::<u64>("pids").copied(),
                cpu: sub.get_one::<u64>("cpu").copied(),
            };
            let policy = match sub.get_one::<String>("restart").map(String::as_str) {
                Some("on-failure") => proto::RestartPolicy::OnFailure,
                Some("always") => proto::RestartPolicy::Always,
                _ => proto::RestartPolicy::Never,
            };
            let spec = proto::Spec {
                cmd: cmd.clone(),
                args,
                env: vec![],
                limits,
                policy,
                max_retries: *sub.get_one::<u32>("max-retries").expect("has default"),
                isolate: sub.get_flag("isolate"),
            };

            info!(%target, %cmd, ?spec.policy, "spawning process on supervisor...");

            let resp = client_call(sub, proto::Request::Spawn(spec)).await?;
            match resp {
                proto::Response::Spawned { id, pid } => info!(handle = id, pid, "spawned"),
                proto::Response::Error(e) => error!("spawn failed: {e:?}"),
                other => warn!("unexpected response: {other:?}"),
            }

            Ok(())
        }
        Some(("list", sub)) => {
            let target = *sub.get_one::<EndpointId>("endpoint").expect("required arg");
            info!(%target, "listing processes on supervisor...");

            match client_call(sub, proto::Request::List).await? {
                proto::Response::List(procs) => {
                    info!(count = procs.len(), "processes");
                    for p in &procs {
                        info!(handle = p.handle, pid = p.pid, state = ?p.state, mem = %mem_str(p.usage), restarts = p.restarts, "process");
                    }
                }
                proto::Response::Error(e) => error!("list failed: {e:?}"),
                other => warn!("unexpected response: {other:?}"),
            }

            Ok(())
        }
        Some(("query", sub)) => {
            let target = *sub.get_one::<EndpointId>("endpoint").expect("required arg");
            let id = *sub.get_one::<u64>("handle").expect("required arg");
            info!(%target, handle = id, "querying process on supervisor...");

            match client_call(sub, proto::Request::Query { id }).await? {
                proto::Response::Status(info) => {
                    info!(handle = info.handle, pid = info.pid, state = ?info.state, mem = %mem_str(info.usage), restarts = info.restarts, "status")
                }
                proto::Response::Error(e) => error!("query failed: {e:?}"),
                other => warn!("unexpected response: {other:?}"),
            }

            Ok(())
        }
        Some(("signal", sub)) => {
            let target = *sub.get_one::<EndpointId>("endpoint").expect("required arg");
            let id = *sub.get_one::<u64>("handle").expect("required arg");
            let sig = *sub.get_one::<i32>("sig").expect("required arg");
            info!(%target, handle = id, sig, "sending signal to process...");

            match client_call(sub, proto::Request::Signal { id, sig }).await? {
                proto::Response::Ack => info!(handle = id, sig, "signal delivered"),
                proto::Response::Error(e) => error!("signal failed: {e:?}"),
                other => warn!("unexpected response: {other:?}"),
            }

            Ok(())
        }
        Some(("stop", sub)) => {
            let target = *sub.get_one::<EndpointId>("endpoint").expect("required arg");
            let id = *sub.get_one::<u64>("handle").expect("required arg");
            info!(%target, handle = id, "stopping process on supervisor...");

            match client_call(sub, proto::Request::Stop { id }).await? {
                proto::Response::Ack => info!(handle = id, "stopped"),
                proto::Response::Error(e) => error!("stop failed: {e:?}"),
                other => warn!("unexpected response: {other:?}"),
            }

            Ok(())
        }
        Some(("stdin", sub)) => {
            let id = *sub.get_one::<u64>("handle").expect("required arg");
            info!(handle = id, "streaming stdin to process...");

            let result = client_streaming(sub, |endpoint, addr, cfg| async move {
                client::stdin_stream(&endpoint, addr, id, &mut tokio::io::stdin(), &cfg).await
            })
            .await;

            match result {
                Ok(()) => info!(handle = id, "stdin delivered"),
                Err(e) => error!("stdin failed: {e:?}"),
            }

            Ok(())
        }
        Some(("subscribe", sub)) => {
            let id = *sub.get_one::<u64>("handle").expect("required arg");
            info!(
                handle = id,
                "subscribing to stdout (streams until process exits)..."
            );

            let result = client_streaming(sub, |endpoint, addr, cfg| async move {
                let mut stdout = tokio::io::stdout();
                client::subscribe(&endpoint, addr, id, &mut stdout, &cfg).await
            })
            .await;

            match &result {
                Ok(()) => info!(handle = id, "stream ended (process exited)"),
                Err(e) => error!("subscribe failed: {e:?}"),
            }

            result
        }
        Some(("forget", sub)) => {
            let target = *sub.get_one::<EndpointId>("endpoint").expect("required arg");
            let id = *sub.get_one::<u64>("handle").expect("required arg");
            info!(%target, handle = id, "forgetting process record on supervisor...");

            match client_call(sub, proto::Request::Forget { id }).await? {
                proto::Response::Ack => info!(handle = id, "forgotten"),
                proto::Response::Error(e) => error!("forget failed: {e:?}"),
                other => warn!("unexpected response: {other:?}"),
            }

            Ok(())
        }
        Some(("watch", sub)) => {
            let mut cfg = ClientConfig::default();
            cfg.with_cli_overrides(sub);

            let bootstrap = parse_peers(sub, "bootstrap");
            let relay = sub.get_one::<RelayUrl>("relay").cloned();

            info!(
                ?bootstrap,
                "joining telemetry topic (binding endpoint, ~7s)..."
            );
            let (endpoint, book) =
                endpoint::build_endpoint(vec![iroh_gossip::ALPN.to_vec()], None, relay.clone())
                    .await?;
            // On WAN, gossip reaches bootstrap peers by id only if we know their relay addr.
            if let Some(r) = &relay {
                for id in &bootstrap {
                    book.add_endpoint_info(EndpointAddr::new(*id).with_relay_url(r.clone()));
                }
            }
            let gossip = Gossip::builder().spawn(endpoint.clone());
            let router = Router::builder(endpoint)
                .accept(iroh_gossip::ALPN, gossip.clone())
                .spawn();

            let topic = gossip
                .subscribe(
                    telemetry::topic_for(sub.get_one::<String>("topic-secret").map(String::as_str)),
                    bootstrap,
                )
                .await?;
            info!("watching telemetry (Ctrl-C to quit)");

            tokio::select! {
                r = telemetry::watch_loop(topic, cfg.telemetry.max_tick_age) => r?,
                _ = tokio::signal::ctrl_c() => info!("shutting down"),
            }

            router.shutdown().await?;

            Ok(())
        }
        _ => unreachable!("subcommand_required(true) guarantees a match"),
    }
}
