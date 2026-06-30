use std::collections::HashSet;

use anyhow::Result;
use clap::{ArgMatches, Command, arg};
use iroh::{EndpointAddr, EndpointId, RelayUrl, protocol::Router};
use iroh_gossip::net::Gossip;
use p2p_telemtry::{
    cgroup::Cgroups, client, endpoint, process::ProcessManager, proto, store::Store, supervisor,
    telemetry,
};
use tokio::io::AsyncReadExt;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

/// Parse zero or more `EndpointId` bootstrap peers from a repeated positional arg.
fn parse_peers(sub: &ArgMatches, key: &str) -> Result<Vec<EndpointId>> {
    sub.get_many::<String>(key)
        .into_iter()
        .flatten()
        .map(|s| s.parse::<EndpointId>().map_err(Into::into))
        .collect()
}

/// Home relay URL from `--relay` or the `P2P_RELAY` env var; `None` = LAN-only.
fn relay_url(sub: &ArgMatches) -> Result<Option<RelayUrl>> {
    let raw = sub
        .get_one::<String>("relay")
        .cloned()
        .or_else(|| std::env::var("P2P_RELAY").ok());
    match raw {
        Some(s) => {
            Ok(Some(s.parse().map_err(|e| {
                anyhow::anyhow!("invalid relay url {s:?}: {e}")
            })?))
        }
        None => Ok(None),
    }
}

/// Shared telemetry-topic secret from `--topic-secret` or `P2P_TOPIC_SECRET`.
fn topic_secret(sub: &ArgMatches) -> Option<String> {
    sub.get_one::<String>("topic-secret")
        .cloned()
        .or_else(|| std::env::var("P2P_TOPIC_SECRET").ok())
}

/// Build the address to dial a supervisor: its id, routed via `relay` when set.
fn dial_addr(id: EndpointId, relay: &Option<RelayUrl>) -> EndpointAddr {
    let addr = EndpointAddr::new(id);
    match relay {
        Some(r) => addr.with_relay_url(r.clone()),
        None => addr,
    }
}

/// Parse a memory size with an optional K/M/G suffix (e.g. `256M`) into bytes.
fn parse_size(s: &str) -> Result<u64> {
    let s = s.trim();
    let (num, mult) = match s.chars().last() {
        Some('K' | 'k') => (&s[..s.len() - 1], 1024),
        Some('M' | 'm') => (&s[..s.len() - 1], 1024 * 1024),
        Some('G' | 'g') => (&s[..s.len() - 1], 1024 * 1024 * 1024),
        _ => (s, 1),
    };

    Ok(num.trim().parse::<u64>()? * mult)
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

fn cli() -> Command {
    Command::new("p2p-telemetry")
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
            arg!(--relay <url> "home relay URL for WAN reachability (env: P2P_RELAY)")
                .global(true)
                .required(false),
        )
        .arg(
            arg!(--"topic-secret" <s> "shared secret for a private telemetry topic (env: P2P_TOPIC_SECRET)")
                .global(true)
                .required(false),
        )
        .after_help(
            "EXAMPLES:\n  \
             # Run as a supervisor; prints this node's EndpointId\n  \
             p2p-telemetry supervise\n\n  \
             # Client: spawn a process on a remote supervisor\n  \
             p2p-telemetry spawn <endpoint-id> -- sleep 60\n\n  \
             # Client: list / inspect handles on a supervisor\n  \
             p2p-telemetry list <endpoint-id>\n  \
             p2p-telemetry query <endpoint-id> <handle>",
        )
        .subcommand(
            Command::new("supervise")
                .about("run as a supervisor: bind an endpoint and accept control connections")
                .arg(
                    arg!(--store <path> "path to the redb store (persists identity + process records)")
                        .default_value("p2p-telemetry.redb"),
                )
                .arg(
                    arg!(--allow <id> ... "EndpointId(s) allowed full control of this supervisor")
                        .required(false),
                )
                .arg(
                    arg!(--"allow-read" <id> ... "EndpointId(s) allowed read-only ops (list/query/subscribe)")
                        .required(false),
                )
                .arg(arg!(--open "accept ALL clients with full control (dangerous; the explicit alternative to an allowlist)"))
                .arg(arg!([peer] ... "EndpointId(s) of other supervisors to bootstrap the telemetry mesh")),
        )
        .subcommand(
            Command::new("spawn")
                .about("client: launch a process on a remote supervisor")
                .arg(arg!(<endpoint> "EndpointId of the supervisor to dial"))
                .arg(arg!(--mem <size> "memory cap, e.g. 256M (K/M/G suffixes)").required(false))
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
                    arg!(<cmd> ... "the command and arguments to run (after `--`)")
                        .trailing_var_arg(true)
                        .num_args(1..),
                ),
        )
        .subcommand(
            Command::new("list")
                .about("client: list the process handles tracked by a supervisor")
                .arg(arg!(<endpoint> "EndpointId of the supervisor to dial")),
        )
        .subcommand(
            Command::new("query")
                .about("client: query a process's status")
                .arg(arg!(<endpoint> "EndpointId of the supervisor to dial"))
                .arg(arg!(<handle> "process handle").value_parser(clap::value_parser!(u64))),
        )
        .subcommand(
            Command::new("signal")
                .about("client: send a signal to a process")
                .arg(arg!(<endpoint> "EndpointId of the supervisor to dial"))
                .arg(arg!(<handle> "process handle").value_parser(clap::value_parser!(u64)))
                .arg(
                    arg!(<sig> "signal number, e.g. 15 (SIGTERM)")
                        .value_parser(clap::value_parser!(i32)),
                ),
        )
        .subcommand(
            Command::new("stop")
                .about("client: stop a process and disable its restart policy")
                .arg(arg!(<endpoint> "EndpointId of the supervisor to dial"))
                .arg(arg!(<handle> "process handle").value_parser(clap::value_parser!(u64))),
        )
        .subcommand(
            Command::new("stdin")
                .about("client: pipe this command's stdin to a process")
                .arg(arg!(<endpoint> "EndpointId of the supervisor to dial"))
                .arg(arg!(<handle> "process handle").value_parser(clap::value_parser!(u64))),
        )
        .subcommand(
            Command::new("subscribe")
                .about("client: stream a process's stdout until it exits")
                .arg(arg!(<endpoint> "EndpointId of the supervisor to dial"))
                .arg(arg!(<handle> "process handle").value_parser(clap::value_parser!(u64))),
        )
        .subcommand(
            Command::new("forget")
                .about("client: drop a finished process's record from a supervisor")
                .arg(arg!(<endpoint> "EndpointId of the supervisor to dial"))
                .arg(arg!(<handle> "process handle").value_parser(clap::value_parser!(u64))),
        )
        .subcommand(
            Command::new("watch")
                .about("client: join the telemetry topic and print stat ticks live")
                .arg(
                    arg!(<bootstrap> ... "EndpointId(s) of supervisor(s) to bootstrap into the topic")
                        .num_args(1..),
                ),
        )
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let matches = cli().get_matches();

    match matches.subcommand() {
        Some(("supervise", sub)) => {
            let bootstrap = parse_peers(sub, "peer")?;
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
            let control: HashSet<EndpointId> = parse_peers(sub, "allow")?.into_iter().collect();
            let read: HashSet<EndpointId> = parse_peers(sub, "allow-read")?.into_iter().collect();
            let relay = relay_url(sub)?;

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
                ProcessManager::with_store(store, loaded, cgroups),
                authz,
            );
            let router = Router::builder(endpoint)
                .accept(proto::CONTROL_ALPN, supervisor.clone())
                .accept(iroh_gossip::ALPN, gossip.clone())
                .spawn();

            // Telemetry publisher: sample the process table and gossip ticks (lossy).
            let topic = gossip
                .subscribe(
                    telemetry::topic_for(topic_secret(sub).as_deref()),
                    bootstrap,
                )
                .await?;
            tokio::spawn(telemetry::publish_loop(
                topic,
                secret_key,
                supervisor.procs(),
            ));

            info!(endpoint_id = %id, "supervisor up (LAN/mDNS, persistent identity)");
            info!("dial with: p2p-telemetry list {id}");
            info!("watch telemetry with: p2p-telemetry watch {id}");
            info!("waiting for control connections (Ctrl-C to quit)");

            tokio::signal::ctrl_c().await?;

            info!("shutting down");
            router.shutdown().await?;

            Ok(())
        }
        Some(("spawn", sub)) => {
            let target: EndpointId = sub
                .get_one::<String>("endpoint")
                .expect("required arg")
                .parse()?;

            let mut argv = sub
                .get_many::<String>("cmd")
                .expect("required arg")
                .cloned();

            let cmd = argv.next().expect("num_args(1..) guarantees one");
            let args: Vec<String> = argv.collect();

            let limits = proto::Limits {
                memory: sub
                    .get_one::<String>("mem")
                    .map(|s| parse_size(s))
                    .transpose()?,
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

            let relay = relay_url(sub)?;
            let (endpoint, _book) = endpoint::build_endpoint(vec![], None, relay.clone()).await?;
            let req = proto::Request::Spawn(spec);
            let resp = client::request(&endpoint, dial_addr(target, &relay), req).await?;
            endpoint.close().await;

            match resp {
                proto::Response::Spawned { id, pid } => info!(handle = id, pid, "spawned"),
                proto::Response::Error(e) => error!("spawn failed: {e:?}"),
                other => warn!("unexpected response: {other:?}"),
            }

            Ok(())
        }
        Some(("list", sub)) => {
            let target: EndpointId = sub
                .get_one::<String>("endpoint")
                .expect("required arg")
                .parse()?;

            info!(%target, "listing processes on supervisor...");
            let relay = relay_url(sub)?;
            let (endpoint, _book) = endpoint::build_endpoint(vec![], None, relay.clone()).await?;
            let resp =
                client::request(&endpoint, dial_addr(target, &relay), proto::Request::List).await?;
            endpoint.close().await;

            match resp {
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
            let target: EndpointId = sub
                .get_one::<String>("endpoint")
                .expect("required arg")
                .parse()?;
            let id = *sub.get_one::<u64>("handle").expect("required arg");

            info!(%target, handle = id, "querying process on supervisor...");
            let relay = relay_url(sub)?;
            let (endpoint, _book) = endpoint::build_endpoint(vec![], None, relay.clone()).await?;
            let resp = client::request(
                &endpoint,
                dial_addr(target, &relay),
                proto::Request::Query { id },
            )
            .await?;
            endpoint.close().await;

            match resp {
                proto::Response::Status(info) => {
                    info!(handle = info.handle, pid = info.pid, state = ?info.state, mem = %mem_str(info.usage), restarts = info.restarts, "status")
                }
                proto::Response::Error(e) => error!("query failed: {e:?}"),
                other => warn!("unexpected response: {other:?}"),
            }

            Ok(())
        }
        Some(("signal", sub)) => {
            let target: EndpointId = sub
                .get_one::<String>("endpoint")
                .expect("required arg")
                .parse()?;
            let id = *sub.get_one::<u64>("handle").expect("required arg");
            let sig = *sub.get_one::<i32>("sig").expect("required arg");

            info!(%target, handle = id, sig, "sending signal to process...");
            let relay = relay_url(sub)?;
            let (endpoint, _book) = endpoint::build_endpoint(vec![], None, relay.clone()).await?;
            let resp = client::request(
                &endpoint,
                dial_addr(target, &relay),
                proto::Request::Signal { id, sig },
            )
            .await?;
            endpoint.close().await;

            match resp {
                proto::Response::Ack => info!(handle = id, sig, "signal delivered"),
                proto::Response::Error(e) => error!("signal failed: {e:?}"),
                other => warn!("unexpected response: {other:?}"),
            }

            Ok(())
        }
        Some(("stop", sub)) => {
            let target: EndpointId = sub
                .get_one::<String>("endpoint")
                .expect("required arg")
                .parse()?;
            let id = *sub.get_one::<u64>("handle").expect("required arg");

            info!(%target, handle = id, "stopping process on supervisor...");
            let relay = relay_url(sub)?;
            let (endpoint, _book) = endpoint::build_endpoint(vec![], None, relay.clone()).await?;
            let resp = client::request(
                &endpoint,
                dial_addr(target, &relay),
                proto::Request::Stop { id },
            )
            .await?;
            endpoint.close().await;

            match resp {
                proto::Response::Ack => info!(handle = id, "stopped"),
                proto::Response::Error(e) => error!("stop failed: {e:?}"),
                other => warn!("unexpected response: {other:?}"),
            }

            Ok(())
        }
        Some(("stdin", sub)) => {
            let target: EndpointId = sub
                .get_one::<String>("endpoint")
                .expect("required arg")
                .parse()?;
            let id = *sub.get_one::<u64>("handle").expect("required arg");

            // Read this command's stdin to EOF and send it as one frame.
            let mut data = Vec::new();
            tokio::io::stdin().read_to_end(&mut data).await?;

            info!(%target, handle = id, bytes = data.len(), "sending stdin to process...");
            let relay = relay_url(sub)?;
            let (endpoint, _book) = endpoint::build_endpoint(vec![], None, relay.clone()).await?;
            let resp = client::request(
                &endpoint,
                dial_addr(target, &relay),
                proto::Request::Stdin { id, data },
            )
            .await?;
            endpoint.close().await;

            match resp {
                proto::Response::Ack => info!(handle = id, "stdin delivered"),
                proto::Response::Error(e) => error!("stdin failed: {e:?}"),
                other => warn!("unexpected response: {other:?}"),
            }

            Ok(())
        }
        Some(("subscribe", sub)) => {
            let target: EndpointId = sub
                .get_one::<String>("endpoint")
                .expect("required arg")
                .parse()?;
            let id = *sub.get_one::<u64>("handle").expect("required arg");

            info!(%target, handle = id, "subscribing to stdout (streams until process exits)...");
            let relay = relay_url(sub)?;
            let (endpoint, _book) = endpoint::build_endpoint(vec![], None, relay.clone()).await?;
            let mut stdout = tokio::io::stdout();
            let result =
                client::subscribe(&endpoint, dial_addr(target, &relay), id, &mut stdout).await;
            endpoint.close().await;

            match &result {
                Ok(()) => info!(handle = id, "stream ended (process exited)"),
                Err(e) => error!("subscribe failed: {e:?}"),
            }

            result
        }
        Some(("forget", sub)) => {
            let target: EndpointId = sub
                .get_one::<String>("endpoint")
                .expect("required arg")
                .parse()?;
            let id = *sub.get_one::<u64>("handle").expect("required arg");

            info!(%target, handle = id, "forgetting process record on supervisor...");
            let relay = relay_url(sub)?;
            let (endpoint, _book) = endpoint::build_endpoint(vec![], None, relay.clone()).await?;
            let resp = client::request(
                &endpoint,
                dial_addr(target, &relay),
                proto::Request::Forget { id },
            )
            .await?;
            endpoint.close().await;

            match resp {
                proto::Response::Ack => info!(handle = id, "forgotten"),
                proto::Response::Error(e) => error!("forget failed: {e:?}"),
                other => warn!("unexpected response: {other:?}"),
            }

            Ok(())
        }
        Some(("watch", sub)) => {
            let bootstrap = parse_peers(sub, "bootstrap")?;
            let relay = relay_url(sub)?;

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
                    telemetry::topic_for(topic_secret(sub).as_deref()),
                    bootstrap,
                )
                .await?;
            info!("watching telemetry (Ctrl-C to quit)");

            tokio::select! {
                r = telemetry::watch_loop(topic) => r?,
                _ = tokio::signal::ctrl_c() => info!("shutting down"),
            }

            router.shutdown().await?;

            Ok(())
        }
        _ => unreachable!("subcommand_required(true) guarantees a match"),
    }
}
