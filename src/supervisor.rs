//! Control-plane `ProtocolHandler`: one request → one response per stream.

use std::collections::HashSet;
use std::sync::Arc;

use iroh::{
    EndpointId,
    endpoint::{Connection, SendStream},
    protocol::{AcceptError, ProtocolHandler},
};
use tokio::sync::broadcast::error::RecvError;
use tracing::{debug, warn};

use crate::config::SupervisorConfig;
use crate::process::ProcessManager;
use crate::proto::{ControlError, Request, Response, read_msg, write_msg};
use crate::ratelimit::RateLimiter;

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
#[derive(Debug, Clone)]
pub struct Supervisor {
    procs: ProcessManager,
    authz: Arc<Authz>,
    limiter: Arc<RateLimiter>,
    cfg: SupervisorConfig,
}

/// Box an `anyhow` error into the iroh accept-error type.
fn accept_err(e: anyhow::Error) -> AcceptError {
    AcceptError::from_boxed(e.into())
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
            match tokio::time::timeout(self.cfg.request_timeout, read_msg::<Request>(&mut recv))
                .await
            {
                Ok(Ok(req)) => self.authorize(remote, &req).map(|()| req),
                // Don't reflect the parser's detail back to the client; log it instead.
                Ok(Err(e)) => {
                    debug!(%remote, "malformed request: {e}");
                    Err(ControlError::BadRequest("malformed request".into()))
                }
                Err(_) => {
                    warn!(%remote, "timed out waiting for request");
                    Err(ControlError::Timeout)
                }
            }
        };

        match outcome {
            // Rejected (bad frame or denied): one-shot error, same framing as any reply.
            Err(e) => write_msg(&mut send, &Response::Error(e))
                .await
                .map_err(accept_err)?,
            // Subscribe is long-lived: an Ack, then raw stdout until exit.
            Ok(Request::Subscribe { id }) => {
                self.subscribe(remote, id, &mut send)
                    .await
                    .map_err(accept_err)?;
            }
            // StdinStream is long-lived in the other direction: copy raw bytes from
            // the QUIC recv stream into the child's stdin, then Ack.
            Ok(Request::StdinStream { id }) => {
                let resp = match self.procs.pipe_stdin(id, &mut recv).await {
                    Ok(()) => Response::Ack,
                    Err(e) => Response::Error(e),
                };
                write_msg(&mut send, &resp).await.map_err(accept_err)?;
            }
            // Everything else is one request, one response.
            Ok(req) => {
                let resp = self.dispatch(remote, req).await;
                write_msg(&mut send, &resp).await.map_err(accept_err)?;
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
    /// Build a supervisor around a process table, an authorization policy, and
    /// operational configuration.
    pub fn new(procs: ProcessManager, authz: Authz, cfg: SupervisorConfig) -> Self {
        Self {
            procs,
            authz: Arc::new(authz),
            limiter: Arc::new(RateLimiter::new(cfg.rate_limiter.clone())),
            cfg,
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
            Request::Forget { id } => match self.procs.forget(id) {
                Ok(()) => Response::Ack,
                Err(e) => Response::Error(e),
            },
            Request::List => Response::List(self.procs.snapshot()),
            Request::Subscribe { .. } => unreachable!("Subscribe is handled in accept()"),
            Request::StdinStream { .. } => unreachable!("StdinStream is handled in accept()"),
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
