//! Binary-only module: one `run_*` per subcommand, plus the client plumbing
//! (endpoint setup/teardown, dialing, response reporting) they share.

use std::collections::HashSet;

use anyhow::Result;
use clap::ArgMatches;
use iroh::{
    EndpointAddr, EndpointId, RelayUrl, address_lookup::memory::MemoryLookup, protocol::Router,
};
use iroh_gossip::net::Gossip;
use mesh_supervisor::{
    cgroup::Cgroups, client, config::ClientConfig, config::SupervisorConfig, endpoint,
    process::ProcessManager, proto, store::Store, supervisor, telemetry,
};
use tracing::{error, info, warn};

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
    client_streaming(sub, |endpoint, addr, cfg| async move {
        client::request(&endpoint, addr, req, &cfg).await
    })
    .await
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

/// Uniform tail for one-shot subcommands: an `Error` response means the verb
/// failed; anything else is a protocol surprise worth a warning.
fn report_failure(verb: &str, resp: proto::Response) {
    match resp {
        proto::Response::Error(e) => error!("{verb} failed: {e:?}"),
        other => warn!("unexpected response: {other:?}"),
    }
}

/// On WAN, seed peers' relay addrs into the address book so gossip can reach
/// bootstrap peers by id alone.
fn seed_relay_addrs(book: &MemoryLookup, relay: &Option<RelayUrl>, peers: &[EndpointId]) {
    if let Some(r) = relay {
        for id in peers {
            book.add_endpoint_info(EndpointAddr::new(*id).with_relay_url(r.clone()));
        }
    }
}

/// Render a process's memory usage in MiB, or `-` when unavailable (no cgroup).
fn mem_str(usage: Option<proto::Usage>) -> String {
    usage.map_or_else(
        || "-".to_string(),
        |u| format!("{}MiB", u.mem_bytes / (1024 * 1024)),
    )
}

/// Deny-by-default authz posture from `--open` / `--allow` / `--allow-read`.
/// Errors when no posture was chosen; warns on `--open` (`relay` only widens
/// the warning's scope wording).
fn authz_from_matches(sub: &ArgMatches, relay: &Option<RelayUrl>) -> Result<supervisor::Authz> {
    let open = sub.get_flag("open");
    let control: HashSet<EndpointId> = parse_peers(sub, "allow").into_iter().collect();
    let read: HashSet<EndpointId> = parse_peers(sub, "allow-read").into_iter().collect();

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

    Ok(supervisor::Authz {
        open,
        control,
        read,
    })
}

/// Build the spawn `Spec` from the `spawn` subcommand's args.
fn spec_from_matches(sub: &ArgMatches) -> proto::Spec {
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

    proto::Spec {
        cmd,
        args,
        env: vec![],
        limits,
        policy,
        max_retries: *sub.get_one::<u32>("max-retries").expect("has default"),
        isolate: sub.get_flag("isolate"),
    }
}

/// `supervise`: open the store, build the endpoint + gossip + control router,
/// publish telemetry, and run until Ctrl-C.
pub async fn run_supervise(sub: &ArgMatches) -> Result<()> {
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
    let relay = sub.get_one::<RelayUrl>("relay").cloned();
    let authz = authz_from_matches(sub, &relay)?;

    info!("starting supervisor (binding endpoint, ~7s)...");
    let (endpoint, book) = endpoint::build_endpoint(
        vec![proto::CONTROL_ALPN.to_vec(), iroh_gossip::ALPN.to_vec()],
        Some(secret_key.clone()),
        relay.clone(),
    )
    .await?;

    seed_relay_addrs(&book, &relay, &bootstrap);
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

/// `spawn`: launch a process on a remote supervisor.
pub async fn run_spawn(sub: &ArgMatches) -> Result<()> {
    let target = *sub.get_one::<EndpointId>("endpoint").expect("required arg");
    let spec = spec_from_matches(sub);

    info!(%target, cmd = %spec.cmd, ?spec.policy, "spawning process on supervisor...");

    let resp = client_call(sub, proto::Request::Spawn(spec)).await?;
    match resp {
        proto::Response::Spawned { id, pid } => info!(handle = id, pid, "spawned"),
        other => report_failure("spawn", other),
    }

    Ok(())
}

/// `list`: list the process handles tracked by a supervisor.
pub async fn run_list(sub: &ArgMatches) -> Result<()> {
    let target = *sub.get_one::<EndpointId>("endpoint").expect("required arg");
    info!(%target, "listing processes on supervisor...");

    match client_call(sub, proto::Request::List).await? {
        proto::Response::List(procs) => {
            info!(count = procs.len(), "processes");
            for p in &procs {
                info!(handle = p.handle, pid = p.pid, state = ?p.state, mem = %mem_str(p.usage), restarts = p.restarts, "process");
            }
        }
        other => report_failure("list", other),
    }

    Ok(())
}

/// `query`: query one process's status.
pub async fn run_query(sub: &ArgMatches) -> Result<()> {
    let target = *sub.get_one::<EndpointId>("endpoint").expect("required arg");
    let id = *sub.get_one::<u64>("handle").expect("required arg");
    info!(%target, handle = id, "querying process on supervisor...");

    match client_call(sub, proto::Request::Query { id }).await? {
        proto::Response::Status(info) => {
            info!(handle = info.handle, pid = info.pid, state = ?info.state, mem = %mem_str(info.usage), restarts = info.restarts, "status")
        }
        other => report_failure("query", other),
    }

    Ok(())
}

/// `signal`: send a signal to a process.
pub async fn run_signal(sub: &ArgMatches) -> Result<()> {
    let target = *sub.get_one::<EndpointId>("endpoint").expect("required arg");
    let id = *sub.get_one::<u64>("handle").expect("required arg");
    let sig = *sub.get_one::<i32>("sig").expect("required arg");
    info!(%target, handle = id, sig, "sending signal to process...");

    match client_call(sub, proto::Request::Signal { id, sig }).await? {
        proto::Response::Ack => info!(handle = id, sig, "signal delivered"),
        other => report_failure("signal", other),
    }

    Ok(())
}

/// `stop`: stop a process and disarm its restart policy.
pub async fn run_stop(sub: &ArgMatches) -> Result<()> {
    let target = *sub.get_one::<EndpointId>("endpoint").expect("required arg");
    let id = *sub.get_one::<u64>("handle").expect("required arg");
    info!(%target, handle = id, "stopping process on supervisor...");

    match client_call(sub, proto::Request::Stop { id }).await? {
        proto::Response::Ack => info!(handle = id, "stopped"),
        other => report_failure("stop", other),
    }

    Ok(())
}

/// `stdin`: pipe this command's stdin to a process.
pub async fn run_stdin(sub: &ArgMatches) -> Result<()> {
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

/// `subscribe`: stream a process's stdout until it exits.
pub async fn run_subscribe(sub: &ArgMatches) -> Result<()> {
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

/// `forget`: drop a finished process's record from a supervisor.
pub async fn run_forget(sub: &ArgMatches) -> Result<()> {
    let target = *sub.get_one::<EndpointId>("endpoint").expect("required arg");
    let id = *sub.get_one::<u64>("handle").expect("required arg");
    info!(%target, handle = id, "forgetting process record on supervisor...");

    match client_call(sub, proto::Request::Forget { id }).await? {
        proto::Response::Ack => info!(handle = id, "forgotten"),
        other => report_failure("forget", other),
    }

    Ok(())
}

/// `watch`: join the telemetry topic and print stat ticks until Ctrl-C.
pub async fn run_watch(sub: &ArgMatches) -> Result<()> {
    let mut cfg = ClientConfig::default();
    cfg.with_cli_overrides(sub);

    let bootstrap = parse_peers(sub, "bootstrap");
    let relay = sub.get_one::<RelayUrl>("relay").cloned();

    info!(
        ?bootstrap,
        "joining telemetry topic (binding endpoint, ~7s)..."
    );
    let (endpoint, book) =
        endpoint::build_endpoint(vec![iroh_gossip::ALPN.to_vec()], None, relay.clone()).await?;
    seed_relay_addrs(&book, &relay, &bootstrap);
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

/// Local: print a completion script for `shell` to stdout. No endpoint, no network.
pub fn run_completions(sub: &ArgMatches) -> Result<()> {
    let shell = *sub
        .get_one::<clap_complete::Shell>("shell")
        .expect("required arg");
    let mut cmd = crate::cli::cli();
    let name = cmd.get_name().to_string();
    clap_complete::generate(shell, &mut cmd, name, &mut std::io::stdout());

    Ok(())
}
