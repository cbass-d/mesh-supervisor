# mesh-supervisor

[![CI](https://github.com/cbass-d/mesh-supervisor/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/cbass-d/mesh-supervisor/actions/workflows/ci.yml)

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

Pre-built binaries for Linux x86_64 are attached to each [GitHub Release](https://github.com/cbass-d/mesh-supervisor/releases).

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
- `spawn <id> [--mem <size>] [--pids <n>] [--cpu <secs>] [--restart <policy>] [--max-retries <n>] [--isolate] -- <cmd...>` —
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
- `completions <shell>` — print a completion script for bash/zsh/fish/etc. to stdout
  (local; no network). E.g. `mesh-supervisor completions zsh > ~/.zfunc/_mesh-supervisor`.

Global: `--relay <url>` (WAN, env `P2P_RELAY`) and `--topic-secret <s>` (private telemetry
topic, env `P2P_TOPIC_SECRET`) apply to every subcommand.

## Design notes

- **Identity & state persist** (redb store): the node's secret key and process records
  survive a restart, so `EndpointId` and handles are stable. A process that was running
  reloads as **Stale** — children die with the supervisor and are never re-adopted.
- **Resource caps are portable-first**: rlimits everywhere (`RLIMIT_AS`, `RLIMIT_CPU`),
  accurate cgroup v2 caps (`memory.max`, `pids.max`) on Linux with a delegated subtree.
  Each child gets its own process group, joins its cgroup leaf before `exec`, and carries
  `PR_SET_PDEATHSIG` so it dies with the supervisor even on a crash.
- **Restart policy**: `--restart on-failure|always` relaunches under the same handle with
  exponential backoff (1s→30s, reset after 10s stable), giving up after `--max-retries`
  consecutive fast crashes. `stop` disarms the policy; a Stale process is never resurrected.
- **Termination escalates**: SIGTERM, then after a grace period SIGKILL — targeting the
  cgroup (`cgroup.kill`) or process group, never a bare (reusable) pid. Supervisor shutdown
  drains all children the same way.
- **`--isolate`** (Linux, opt-in) puts the child in fresh user/mount/net/uts/ipc namespaces,
  set up unprivileged: no network, and the node's secret-key store is hidden under a tmpfs.
  Known gaps vs a full container: no PID namespace, no `pivot_root` rootfs.
- **Telemetry ticks carry raw counters** (`memory.current` gauge, cumulative `cpu_usec`);
  the watcher diffs successive ticks for a CPU rate, so a dropped tick loses nothing.
  Without a cgroup, usage is simply absent (no `/proc` fallback).
- **Security posture**: authz is deny-by-default by cryptographically verified `EndpointId`
  (`--allow` full control, `--allow-read` read-only, `--open` explicit). The store holding
  the secret key is `0600`; children get a scrubbed environment (`env_clear` + minimal PATH);
  the control plane is rate-limited per peer and error replies are sanitized. `--topic-secret`
  makes the telemetry topic private (topic = `blake3(secret)`); without it telemetry is public.
- **Wire format is postcard** on both planes and in the on-disk store — compact and
  deterministic (clean to sign over).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
