# mesh-supervisor

A distributed process-control + telemetry plane over an [iroh](https://iroh.computer) (QUIC) P2P mesh.

Each node runs a **supervisor** that owns an iroh endpoint and the local child
processes; the processes never touch the network. Other nodes act as **clients**
that dial a supervisor by its `EndpointId` and issue control requests, or as
**watchers** that subscribe to the telemetry topic.

## Two planes

| Plane | Transport | Semantics |
|---|---|---|
| **Control** | acked RPC over ALPN `/supervisor/control/1` | one request → one response; reliable |
| **Telemetry** | gossip over `iroh-gossip` on a shared topic | fire-and-forget; lossy, never blocks a workload |

## Install

Pre-built binaries for Linux x86_64 and macOS (x86_64 / Apple Silicon) are attached to each [GitHub Release](https://github.com/cbass-d/mesh-supervisor/releases).

```sh
# One-line installer
curl -fsSL https://raw.githubusercontent.com/cbass-d/mesh-supervisor/master/scripts/install.sh | sh
```

Or download the matching `mesh-supervisor-<target>` binary from the latest release, make it executable, and place it on your `PATH`.

## Build & test

```sh
cargo build
cargo test
```

## Quickstart (two terminals, same LAN — mDNS covers same-host)

```sh
# Terminal 1 — run a supervisor; it prints its EndpointId.
# Authz is deny-by-default: pass --open (accept anyone) or --allow <client-id>.
cargo run -- supervise --open

# Terminal 2 — drive it as a client (use the printed <id>)
cargo run -- spawn <id> --mem 256M -- sh -c 'while :; do date; sleep 1; done'
cargo run -- list  <id>          # handles + state, inline
cargo run -- query <id> 1
cargo run -- subscribe <id> 1    # live stdout until the process exits
cargo run -- watch <id>          # live telemetry ticks from the mesh
cargo run -- signal <id> 1 15    # SIGTERM
cargo run -- forget <id> 1       # drop a finished process's record
```

## Across the internet (self-hosted relay, no address publishing)

By default discovery is LAN-only (mDNS). To reach a supervisor across NATs/networks
*without publishing your node to any public directory*, run **your own relay** and point
every node at it — the relay is the rendezvous, and you dial by `(EndpointId, relay)`:

```sh
# Run a relay separately, e.g.:  cargo install iroh-relay && iroh-relay --dev
export P2P_RELAY=https://relay.example.com   # or pass --relay <url> per command

cargo run -- supervise --allow <client-id>   # WAN-reachable; gate control with --allow
cargo run -- list <supervisor-id>            # dialed via the relay
```

Nothing about your node is published; the only shared knowledge is the relay URL (config)
plus the target's `EndpointId`. `--relay` / `P2P_RELAY` applies to every subcommand; omit it
for LAN-only.

**Deny-by-default (everywhere):** a supervisor refuses to start unless you choose a posture —
`--allow <id>...` (full control), `--allow-read <id>...` (read-only: list/query/subscribe), or
`--open` (accept everyone; warns, and warns louder over a relay). Nothing is ever silently open.

## Commands

- `supervise [--store <path>] (--allow <id>... | --allow-read <id>... | --open) [peer...]` —
  run a supervisor. Authz is deny-by-default: `--allow` = full control, `--allow-read` = read-only,
  `--open` = accept everyone. `peer` ids bootstrap the gossip mesh.
- `spawn <id> [--mem <size>] [--pids <n>] [--cpu <secs>] [--restart <policy>] [--max-retries <n>] -- <cmd...>` —
  launch a process. `--mem` e.g. `256M` (RLIMIT_AS + cgroup `memory.max`); `--pids` caps process
  count (cgroup `pids.max`, Linux); `--cpu` caps total CPU-seconds (RLIMIT_CPU). `--restart`
  is `never` (default), `on-failure` (non-zero exit or signal), or `always`; `--max-retries`
  caps consecutive fast restarts before giving up (`0` = unlimited). `--isolate` runs the child in
  fresh namespaces (Linux): no network, and it can't read the node's secret-key store. Children run
  with a scrubbed environment (only PATH + the env you pass).
- `stop <id> <handle>` — stop a process and disarm its restart policy (intentional, unlike a crash):
  SIGTERM, then SIGKILL if it's still up after a grace period.
- `list <id>` / `query <id> <handle>` — inspect tracked processes, their state, and
  (on a cgroup host) live memory.
- `signal <id> <handle> <sig>` — send a signal (e.g. `15` = SIGTERM).
- `stdin <id> <handle>` — pipe this command's stdin to the process.
- `subscribe <id> <handle>` — stream a process's stdout until it exits.
- `watch <bootstrap...>` — join the telemetry topic and print stat ticks live
  (per process: state, memory, and CPU% derived from successive ticks).
- `forget <id> <handle>` — drop a finished process's record.

Global: `--relay <url>` (WAN, env `P2P_RELAY`) and `--topic-secret <s>` (private telemetry
topic, env `P2P_TOPIC_SECRET`) apply to every subcommand.

## Design notes

- **Identity & state persist** (redb store, default `mesh-supervisor.redb`): the node's
  secret key and process records survive a restart, so `EndpointId` and handles are stable.
  A process that was running when the supervisor died reloads as **Stale** (its child is
  gone — children die with the supervisor; no re-adopt).
- **Resource isolation** is portable-first: a `--mem` cap is enforced via `RLIMIT_AS`
  (all Unix, unprivileged) and, on Linux with a delegated cgroup v2 subtree, via an accurate
  per-child `memory.max`. The child joins its cgroup leaf from `pre_exec` (before `exec`),
  so the cap covers exec-time pages and `memory.current` accounts the child accurately.
  Each child runs in its own process group with `PR_SET_PDEATHSIG` so it dies with the
  supervisor even on a crash; cgroups are torn down on exit and swept on boot.
- **Restart policy** (in-memory, per supervisor lifetime): `--restart on-failure|always` relaunches
  a child under the *same handle* (new pid, `restarts` count climbs) with exponential backoff
  (1s→30s), resetting once it stays up ≥10s, and gives up after `--max-retries` consecutive fast
  crashes. `stop` disarms the policy so an intentional stop isn't fought; a reloaded (Stale) process
  is never resurrected. Watch it via the `restarts` field in `list`/`query`/`watch`.
- **Termination escalates**: `stop` SIGTERMs, then after a grace period SIGKILLs a child that
  ignored it — targeting the cgroup (`cgroup.kill`) or process group, never a bare (reusable) pid.
  Shutdown is graceful the same way: SIGTERM all, wait for them to exit (returning as soon as they
  all do, capped at the grace period), then force any survivors down.
- **Namespace isolation** (`--isolate`, Linux, opt-in): the child runs in fresh user/mount/net/uts/ipc
  namespaces, set up unprivileged via a user namespace (single-id self-map to root-in-ns). It gets an
  empty network namespace (no network) and a private mount namespace where the store's directory is
  overmounted with an empty tmpfs, so it can't read the node's secret key. Degrades by failing the
  spawn loudly where unprivileged user namespaces are disabled. Known gaps vs a full container: no PID
  namespace (`std::process::Command` forks rather than `clone(CLONE_NEWPID)`), and non-store files
  outside the hidden directory remain readable (no `pivot_root` rootfs).
- **Telemetry carries live usage**: each tick includes per-process `memory.current` and
  cumulative `cpu_usec` (raw cgroup counters). The watcher diffs successive ticks for a CPU
  rate — robust on a lossy channel, where a rate baked into a dropped tick would be lost.
  On hosts without a cgroup, usage is simply absent (no `/proc` fallback).
- **Authz** is deny-by-default by cryptographically-verified `EndpointId`: `--allow` (full control),
  `--allow-read` (read-only), or explicit `--open`. A read-only id can `list`/`query`/`subscribe`
  but not `spawn`/`signal`/`stdin`/`forget`.
- **Hardening:** the store (holding the secret key) is `0600`; spawned children get a scrubbed
  environment (`env_clear` + minimal PATH) so supervisor secrets don't leak; `--pids`/`--cpu`
  bound runaway/fork-bomb children. authz is deny-by-default with full/read-only
  roles. The telemetry topic can be made private with `--topic-secret` (topic = `blake3(secret)`,
  so peers without it can't join/read/inject); without it, telemetry is public. The control plane
  is rate-limited per peer (token bucket, ~10 req/s, burst 20), and error replies are sanitized —
  OS/parser detail is logged server-side, never reflected to clients. Spawned children can opt into
  network + filesystem isolation with `--isolate` (fresh namespaces); without it they still run as
  the supervisor's user. Remaining for hostile multi-tenant use: PID-namespace isolation and a full
  `pivot_root` filesystem jail (today `--isolate` hides only the store directory).
- Wire format is binary **postcard** on both planes (control frames and signed
  telemetry ticks) and for the on-disk store records — compact, deterministic
  (clean to sign over), one `serde` swap from the original JSON.
