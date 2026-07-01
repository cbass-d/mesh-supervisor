//! Lossy telemetry plane: supervisors gossip per-process stat ticks on a shared
//! topic; watchers join the topic and print them. Never blocks a workload.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use iroh::{EndpointId, SecretKey, Signature};
use iroh_gossip::{
    api::{Event, GossipTopic},
    proto::TopicId,
};
use n0_future::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::process::ProcessManager;
use crate::proto::{Handle, ProcInfo};

/// Well-known label hashed into the default public topic when no secret is set.
const DEFAULT_TOPIC_LABEL: &str = "mesh-supervisor/telemetry/1";

/// The topic to use: `blake3(secret)` when a shared secret is set (private — peers
/// without the secret can't compute it, so they can't join/read/inject), else
/// `blake3` of a well-known public label (any supervisor/watcher meets there with
/// zero config).
pub fn topic_for(secret: Option<&str>) -> TopicId {
    let label = secret.unwrap_or(DEFAULT_TOPIC_LABEL);

    TopicId::from_bytes(*blake3::hash(label.as_bytes()).as_bytes())
}

/// Publish cadence. One snapshot per interval; ticks are lossy under load.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// Reject ticks whose timestamp is more than this far from now (replay/staleness).
/// Assumes publisher and watcher clocks are roughly in sync (e.g. NTP).
const MAX_TICK_AGE_MS: u64 = 10_000;

/// A telemetry tick's authenticated content: one supervisor's process table at a
/// moment in time. `from`, `ts`, and `seq` are bound into the signed payload, so
/// none can be forged (see [`sign_tick`] / [`open_tick`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tick {
    pub from: EndpointId,
    pub ts: u64,
    /// Per-publisher monotonic sequence number. Resets on supervisor restart.
    pub seq: u64,
    pub stats: Vec<ProcInfo>,
}

/// Milliseconds since the Unix epoch (0 if the clock is before it).
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Whether a tick stamped `ts` is acceptably close to `now` (both ms): within the
/// window in either direction, tolerating modest clock skew.
fn fresh(ts: u64, now: u64) -> bool {
    now.abs_diff(ts) <= MAX_TICK_AGE_MS
}

/// Decide whether to accept a decoded tick. Rejects stale timestamps and any
/// sequence number that is not strictly greater than the last accepted tick from
/// the same publisher. On acceptance, updates `latest_seq`.
fn accept_tick(tick: &Tick, now: u64, latest_seq: &mut HashMap<EndpointId, u64>) -> bool {
    if !fresh(tick.ts, now) {
        return false;
    }
    if latest_seq
        .get(&tick.from)
        .is_some_and(|&prev| tick.seq <= prev)
    {
        return false;
    }
    latest_seq.insert(tick.from, tick.seq);
    true
}

/// Wire form: the serialized `Tick` body plus a signature over those exact bytes.
#[derive(Serialize, Deserialize)]
struct SignedTick {
    body: Vec<u8>,
    sig: Signature,
}

/// Serialize and sign a tick (authored by `secret`'s id) for broadcast.
/// `seq` is the per-publisher monotonic sequence number; callers must increment
/// it for every tick and never reuse a value.
pub fn sign_tick(secret: &SecretKey, seq: u64, stats: Vec<ProcInfo>) -> Result<Vec<u8>> {
    let body = postcard::to_allocvec(&Tick {
        from: secret.public(),
        ts: now_millis(),
        seq,
        stats,
    })?;
    let sig = secret.sign(&body);

    Ok(postcard::to_allocvec(&SignedTick { body, sig })?)
}

/// Parse and verify a tick from the wire; errors if the signature doesn't match
/// the `from` id claimed inside it (so a topic member can't impersonate another).
pub fn open_tick(content: &[u8]) -> Result<Tick> {
    let signed: SignedTick = postcard::from_bytes(content)?;
    let tick: Tick = postcard::from_bytes(&signed.body)?;
    tick.from.verify(&signed.body, &signed.sig)?;

    Ok(tick)
}

/// Sample `procs` on an interval and broadcast each non-empty tick, signed by
/// `secret`. Runs until the topic closes; errors are logged, never propagated.
pub async fn publish_loop(mut topic: GossipTopic, secret: SecretKey, procs: ProcessManager) {
    let mut interval = tokio::time::interval(SAMPLE_INTERVAL);
    let mut seq = 0u64;

    loop {
        interval.tick().await;
        let stats = procs.snapshot();
        if stats.is_empty() {
            continue; // nothing to report yet
        }

        seq += 1;
        match sign_tick(&secret, seq, stats) {
            Ok(bytes) => {
                if let Err(e) = topic.broadcast(bytes.into()).await {
                    warn!("telemetry broadcast failed: {e}");
                }
            }
            Err(e) => warn!("telemetry encode failed: {e}"),
        }
    }
}

/// Print telemetry ticks from the topic until the stream ends. Drops ticks that
/// fail signature verification, fall outside the freshness window, or replay an
/// already-seen sequence number from the same publisher.
pub async fn watch_loop(mut topic: GossipTopic) -> Result<()> {
    let mut latest_seq: HashMap<EndpointId, u64> = HashMap::new();
    // Last (cpu_usec, ts) per (publisher, handle) for computing a CPU rate.
    let mut prev: HashMap<(EndpointId, Handle), (u64, u64)> = HashMap::new();

    while let Some(event) = topic.next().await {
        match event {
            Ok(Event::Received(msg)) => match open_tick(&msg.content) {
                Ok(tick) => {
                    if !accept_tick(&tick, now_millis(), &mut latest_seq) {
                        warn!(from = %tick.from, ts = tick.ts, seq = tick.seq, "dropping stale/replayed tick");
                        continue;
                    }

                    for s in &tick.stats {
                        // Diff cumulative cpu_usec against our last sample for a rate.
                        let cpu_pct = s.usage.and_then(|u| {
                            let (pc, pt) =
                                prev.insert((tick.from, s.handle), (u.cpu_usec, tick.ts))?;
                            let dt_ms = tick.ts.checked_sub(pt).filter(|&d| d > 0)?;

                            Some(u.cpu_usec.saturating_sub(pc) as f64 / (dt_ms as f64 * 10.0))
                        });
                        let mem = s.usage.map_or_else(
                            || "-".to_string(),
                            |u| format!("{}MiB", u.mem_bytes / (1024 * 1024)),
                        );
                        let cpu = cpu_pct.map_or_else(|| "-".to_string(), |p| format!("{p:.1}%"));

                        info!(
                            from = %tick.from,
                            handle = s.handle,
                            pid = s.pid,
                            state = ?s.state,
                            mem = %mem,
                            cpu = %cpu,
                            "tick",
                        );
                    }
                }
                Err(e) => warn!("dropping unverified/malformed tick: {e}"),
            },
            Ok(Event::NeighborUp(id)) => info!(%id, "neighbor up"),
            Ok(Event::NeighborDown(id)) => info!(%id, "neighbor down"),
            Ok(Event::Lagged) => warn!("gossip lagged: some ticks dropped"),
            Err(e) => {
                warn!("gossip stream error: {e}");
                break;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_secret_changes_and_is_stable() {
        assert_eq!(topic_for(Some("hunter2")), topic_for(Some("hunter2")));
        assert_ne!(topic_for(Some("hunter2")), topic_for(Some("other")));
        assert_ne!(topic_for(Some("hunter2")), topic_for(None));
    }

    #[test]
    fn fresh_window_rejects_old_and_far_future() {
        let now = 1_000_000;
        assert!(fresh(now, now));
        assert!(fresh(now - MAX_TICK_AGE_MS, now)); // within window (past)
        assert!(fresh(now + MAX_TICK_AGE_MS, now)); // within window (skew ahead)
        assert!(!fresh(now - MAX_TICK_AGE_MS - 1, now)); // too old → replay
        assert!(!fresh(now + MAX_TICK_AGE_MS + 1, now)); // too far ahead
    }

    #[test]
    fn signed_tick_roundtrips_and_rejects_forgery() {
        use crate::proto::ProcState;

        let sk = SecretKey::generate();
        let stats = vec![ProcInfo {
            handle: 1,
            pid: 42,
            state: ProcState::Running,
            usage: None,
            restarts: 0,
        }];

        let tick = open_tick(&sign_tick(&sk, 1, stats).unwrap()).unwrap();
        assert_eq!(tick.from, sk.public());
        assert_eq!(tick.seq, 1);
        assert_eq!(tick.stats.len(), 1);

        // Forge: claim a victim's id but sign with a different key → rejected.
        let victim = SecretKey::generate().public();
        let body = postcard::to_allocvec(&Tick {
            from: victim,
            ts: 0,
            seq: 1,
            stats: vec![],
        })
        .unwrap();
        let forged = postcard::to_allocvec(&SignedTick {
            sig: SecretKey::generate().sign(&body),
            body,
        })
        .unwrap();
        assert!(open_tick(&forged).is_err(), "forged tick must be rejected");
    }

    #[test]
    fn accept_tick_rejects_replay_and_reordering() {
        let id = SecretKey::generate().public();
        let now = 1_000_000;
        let mut latest = HashMap::new();

        let tick = |seq: u64| Tick {
            from: id,
            ts: now,
            seq,
            stats: vec![],
        };

        assert!(accept_tick(&tick(1), now, &mut latest));
        // Same or lower sequence number is a replay.
        assert!(!accept_tick(&tick(1), now, &mut latest), "duplicate seq");
        assert!(!accept_tick(&tick(0), now, &mut latest), "regressed seq");
        // Higher sequence number is accepted.
        assert!(accept_tick(&tick(2), now, &mut latest));
        // Stale timestamp is still rejected even with a fresh sequence number.
        assert!(!accept_tick(
            &tick(3),
            now + MAX_TICK_AGE_MS + 1,
            &mut latest
        ));
    }
}
