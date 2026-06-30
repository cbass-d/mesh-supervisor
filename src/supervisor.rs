//! Control-plane `ProtocolHandler`: one request → one response per stream.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use iroh::{
    EndpointId,
    endpoint::{Connection, SendStream},
    protocol::{AcceptError, ProtocolHandler},
};
use parking_lot::Mutex;
use tokio::sync::broadcast::error::RecvError;
use tracing::{debug, warn};

use crate::process::ProcessManager;
use crate::proto::{ControlError, Request, Response, read_msg, write_msg};

/// Per-peer token bucket: sustained `REFILL`/s with a burst of `BURST`.
const BURST: f64 = 20.0;
const REFILL: f64 = 10.0;

/// Per-peer request rate limiter (token bucket keyed by `EndpointId`). Each request
/// is its own connection, so limiting must be per-peer, not per-connection.
#[derive(Debug, Default)]
struct RateLimiter {
    buckets: Mutex<HashMap<EndpointId, (f64, Instant)>>,
}

impl RateLimiter {
    /// Consume one token for `peer`; `false` if it's currently rate-limited.
    fn allow(&self, peer: EndpointId) -> bool {
        let now = Instant::now();
        let mut buckets = self.buckets.lock();
        let (tokens, last) = buckets.entry(peer).or_insert((BURST, now));

        *tokens = (*tokens + now.duration_since(*last).as_secs_f64() * REFILL).min(BURST);
        *last = now;
        if *tokens >= 1.0 {
            *tokens -= 1.0;

            return true;
        }

        false
    }
}

/// Authorization policy. `open` admits everyone (full control); otherwise a client
/// id must be in `control` (full) or `read` (read-only ops). Default = deny-all.
#[derive(Debug, Default)]
pub struct Authz {
    pub open: bool,
    pub control: HashSet<EndpointId>,
    pub read: HashSet<EndpointId>,
}

impl Authz {
    /// Admit every client with full control (the explicit `--open` posture).
    pub fn open() -> Self {
        Self {
            open: true,
            ..Default::default()
        }
    }
}

/// Supervisor state: the process table shared across all control connections.
#[derive(Debug, Default, Clone)]
pub struct Supervisor {
    procs: ProcessManager,
    authz: Arc<Authz>,
    limiter: Arc<RateLimiter>,
}

impl ProtocolHandler for Supervisor {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let remote = conn.remote_id();
        let (mut send, mut recv) = conn.accept_bi().await?;

        // Rate-limit, parse, and authorize centrally: one chokepoint per request.
        let outcome = if !self.limiter.allow(remote) {
            warn!(%remote, "connection rate limited");
            Err(ControlError::RateLimited)
        } else {
            match read_msg::<Request>(&mut recv).await {
                // Don't reflect the parser's detail back to the client; log it instead.
                Err(e) => {
                    debug!(%remote, "malformed request: {e}");
                    Err(ControlError::BadRequest("malformed request".into()))
                }
                Ok(req) => self.authorize(remote, &req).map(|()| req),
            }
        };

        match outcome {
            // Rejected (bad frame or denied): one-shot error, same framing as any reply.
            Err(e) => write_msg(&mut send, &Response::Error(e))
                .await
                .map_err(|e| AcceptError::from_boxed(e.into()))?,
            // Subscribe is long-lived: an Ack, then raw stdout until exit.
            Ok(Request::Subscribe { id }) => {
                self.subscribe(remote, id, &mut send)
                    .await
                    .map_err(|e| AcceptError::from_boxed(e.into()))?;
            }
            // Everything else is one request, one response.
            Ok(req) => {
                let resp = self.dispatch(remote, req).await;
                write_msg(&mut send, &resp)
                    .await
                    .map_err(|e| AcceptError::from_boxed(e.into()))?;
            }
        }

        // Keep the connection alive until the client closes, so the reply is
        // delivered before the connection is dropped.
        conn.closed().await;

        Ok(())
    }

    /// Called by `Router::shutdown`: kill every tracked child.
    async fn shutdown(&self) {
        self.procs.kill_all().await;
    }
}

impl Supervisor {
    /// Build a supervisor around a process table and an authorization policy.
    pub fn new(procs: ProcessManager, authz: Authz) -> Self {
        Self {
            procs,
            authz: Arc::new(authz),
            limiter: Arc::default(),
        }
    }

    /// Shared process table, for the telemetry publisher to sample.
    pub fn procs(&self) -> ProcessManager {
        self.procs.clone()
    }

    /// Handle one already-authorized request. `Subscribe` is handled in `accept()`.
    async fn dispatch(&self, remote: EndpointId, req: Request) -> Response {
        debug!(%remote, ?req, "control request");

        match req {
            Request::Spawn(spec) => match self.procs.spawn(spec) {
                Ok((id, pid)) => Response::Spawned { id, pid },
                Err(e) => {
                    warn!(%remote, "spawn failed: {e}");
                    Response::Error(ControlError::SpawnFailed("could not start process".into()))
                }
            },
            Request::Query { id } => match self.procs.query(id) {
                Ok(info) => Response::Status(info),
                Err(e) => Response::Error(e),
            },
            Request::Signal { id, sig } => match self.procs.signal(id, sig) {
                Ok(()) => Response::Ack,
                Err(e) => Response::Error(e),
            },
            Request::Stop { id } => match self.procs.stop(id) {
                Ok(()) => Response::Ack,
                Err(e) => Response::Error(e),
            },
            Request::Stdin { id, data } => match self.procs.write_stdin(id, &data).await {
                Ok(()) => Response::Ack,
                Err(e) => Response::Error(e),
            },
            Request::Forget { id } => match self.procs.forget(id) {
                Ok(()) => Response::Ack,
                Err(e) => Response::Error(e),
            },
            Request::List => Response::List(self.procs.snapshot()),
            Request::Subscribe { .. } => unreachable!("Subscribe is handled in accept()"),
        }
    }

    /// Authorize a request: open admits all; `control` ids do anything; `read` ids
    /// do read-only ops only; everyone else is denied.
    fn authorize(&self, remote: EndpointId, req: &Request) -> Result<(), ControlError> {
        let a = &self.authz;
        if a.open || a.control.contains(&remote) {
            return Ok(());
        }

        if req.is_read_only() && a.read.contains(&remote) {
            return Ok(());
        }

        Err(ControlError::Denied)
    }
}

impl Supervisor {
    /// Stream an already-authorized process's stdout: an `Ack`, then raw chunks until exit.
    async fn subscribe(
        &self,
        remote: EndpointId,
        id: u64,
        send: &mut SendStream,
    ) -> anyhow::Result<()> {
        let mut rx = match self.procs.subscribe(id) {
            Ok(rx) => rx,
            Err(e) => {
                write_msg(send, &Response::Error(e)).await?;

                return Ok(());
            }
        };

        write_msg(send, &Response::Ack).await?;
        debug!(%remote, id, "stdout subscription started");

        loop {
            match rx.recv().await {
                Ok(chunk) => send.write_all(&chunk).await?,
                Err(RecvError::Closed) => break, // stdout EOF: process exited
                Err(RecvError::Lagged(n)) => warn!(id, dropped = n, "subscriber lagged"),
            }
        }

        send.finish()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::SecretKey;

    #[test]
    fn rate_limiter_allows_burst_then_blocks() {
        let limiter = RateLimiter::default();
        let peer = SecretKey::generate().public();

        // A full burst is admitted back-to-back (refill over microseconds is ~0)...
        for _ in 0..BURST as u32 {
            assert!(limiter.allow(peer));
        }
        // ...then the bucket is empty.
        assert!(!limiter.allow(peer), "burst exhausted should be limited");

        // A different peer has its own bucket, unaffected.
        assert!(limiter.allow(SecretKey::generate().public()));
    }
}
