//! Shared wire protocol: control ALPN, request/response types, JSON framing.

use anyhow::Result;
use iroh::endpoint::{RecvStream, SendStream};
use serde::{Serialize, de::DeserializeOwned};

/// Control-plane ALPN. Trailing `/1` is the version — mismatches fail at handshake.
pub const CONTROL_ALPN: &[u8] = b"/supervisor/control/1";

/// Max bytes for a single framed message (M2: one request/response per stream).
pub const MAX_FRAME: usize = 64 * 1024;

/// Ephemeral, supervisor-local id. Mesh-wide id = (EndpointId, Handle).
pub type Handle = u64;

/// Per-process resource caps. `None` = unlimited.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    /// Max memory in bytes: `RLIMIT_AS` (portable) + cgroup `memory.max` (Linux).
    pub memory: Option<u64>,
    /// Max processes/threads in the job: cgroup `pids.max` (Linux only).
    pub pids: Option<u64>,
    /// Max total CPU time in seconds: `RLIMIT_CPU` (portable).
    pub cpu: Option<u64>,
}

/// When the supervisor relaunches a child after it exits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, serde::Deserialize)]
pub enum RestartPolicy {
    /// Never relaunch (today's behavior).
    #[default]
    Never,
    /// Relaunch only on a non-zero exit or death by signal.
    OnFailure,
    /// Relaunch on any exit, including a clean one.
    Always,
}

/// Everything needed to launch (and relaunch) a child: the immutable spawn spec.
#[derive(Debug, Clone, PartialEq, Default, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    pub cmd: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub limits: Limits,
    pub policy: RestartPolicy,
    /// Consecutive fast restarts allowed before giving up; `0` = unlimited.
    pub max_retries: u32,
    /// Run the child in fresh namespaces (user/mount/net/uts/ipc), Linux only.
    pub isolate: bool,
}

/// A control request from a client.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Request {
    Spawn(Spec),
    Signal {
        id: Handle,
        sig: i32,
    },
    /// Stop a process and disarm its restart policy (intentional, vs a crash).
    Stop {
        id: Handle,
    },
    Stdin {
        id: Handle,
        data: Vec<u8>,
    },
    Query {
        id: Handle,
    },
    Subscribe {
        id: Handle,
    },
    Forget {
        id: Handle,
    },
    List,
}

impl Request {
    /// Read-only requests (no process mutation); the rest require control rights.
    pub fn is_read_only(&self) -> bool {
        matches!(
            self,
            Request::List | Request::Query { .. } | Request::Subscribe { .. }
        )
    }
}

/// Lifecycle state of a tracked process.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, serde::Deserialize)]
pub enum ProcState {
    Running,
    Exited(Option<i32>),
    /// Reloaded from the store after a supervisor restart; the child is gone.
    Stale,
}

/// Live resource usage from a child's cgroup leaf: raw kernel counters, not rates.
/// `cpu_usec` is cumulative (diff successive samples for a rate); `mem_bytes` is
/// instantaneous (`memory.current`, includes page cache).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Usage {
    pub mem_bytes: u64,
    pub cpu_usec: u64,
}

/// A process snapshot: identity, state, and (when running under a cgroup) usage.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcInfo {
    pub handle: Handle,
    pub pid: u32,
    pub state: ProcState,
    /// `None` when not running or no cgroup is available on this host.
    pub usage: Option<Usage>,
    /// Times the supervisor has relaunched this handle.
    pub restarts: u32,
}

/// A control response from the supervisor.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Response {
    Spawned { id: Handle, pid: u32 },
    Ack,
    Status(ProcInfo),
    List(Vec<ProcInfo>),
    Error(ControlError),
}

/// A control-plane error, carried inside `Response::Error`.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ControlError {
    NotFound(Handle),
    Denied,
    /// Too many requests from this peer; back off.
    RateLimited,
    SpawnFailed(String),
    BadRequest(String),
    /// The supervisor closed the connection because the client was too slow to
    /// send a request or complete a handshake.
    Timeout,
}

/// Write one length-prefixed (u32 BE) postcard message. The single framing for
/// every request and response
pub async fn write_msg<T: Serialize>(send: &mut SendStream, msg: &T) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    let bytes = postcard::to_allocvec(msg)?;
    if bytes.len() > MAX_FRAME {
        anyhow::bail!("frame too large: {}", bytes.len());
    }
    send.write_u32(bytes.len() as u32).await?;
    send.write_all(&bytes).await?;

    Ok(())
}

/// Read one length-prefixed (u32 BE) postcard message written by [`write_msg`].
pub async fn read_msg<T: DeserializeOwned>(recv: &mut RecvStream) -> Result<T> {
    use tokio::io::AsyncReadExt;

    let len = recv.read_u32().await? as usize;
    if len > MAX_FRAME {
        anyhow::bail!("frame too large: {len}");
    }
    let mut bytes = vec![0u8; len];
    recv.read_exact(&mut bytes).await?;

    Ok(postcard::from_bytes(&bytes)?)
}
