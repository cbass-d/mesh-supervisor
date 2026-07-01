//! Spawns and tracks child processes. M4: spawn, reap, query, signal, stdin.

use std::{
    collections::HashMap,
    ffi::{CStr, CString},
    os::unix::ffi::OsStringExt,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, ChildStdin, Command},
    sync::{Mutex as AsyncMutex, broadcast},
};
use tracing::{info, warn};

use crate::cgroup::Cgroups;
use crate::proto::{ControlError, Handle, ProcInfo, ProcState, RestartPolicy, Spec};
use crate::store::{Loaded, Store};

/// Broadcast capacity (chunks) and per-read chunk size for stdout fan-out.
const STDOUT_CAP: usize = 1024;
const CHUNK: usize = 8 * 1024;
/// Minimal PATH for spawned children after `env_clear` (overridable by client env).
const DEFAULT_PATH: &str = "/usr/bin:/bin";

/// Restart backoff: `base * 2^(attempt-1)`, capped; reset once a child stays up
/// at least `STABLE_RESET` (so an occasional crash isn't treated as a tight loop).
const RESTART_BASE: Duration = Duration::from_secs(1);
const RESTART_MAX: Duration = Duration::from_secs(30);
const STABLE_RESET: Duration = Duration::from_secs(10);

/// Grace period after a `stop`/shutdown SIGTERM before escalating to SIGKILL.
const STOP_DEADLINE: Duration = Duration::from_secs(5);

/// Write the calling process's pid into an open `cgroup.procs`, joining that leaf.
/// Runs in the child after fork, before exec, so it must stay async-signal-safe
/// (see `CommandExt::pre_exec` Safety docs): `write!` of an integer to a raw file
/// neither allocates nor locks, unlike `to_string`/`println!`.
fn join_cgroup(file: &std::fs::File) -> std::io::Result<()> {
    use std::io::Write;

    let pid = nix::unistd::getpid().as_raw();
    let mut f: &std::fs::File = file;

    write!(f, "{pid}")
}

/// The directory to hide from an isolated child: the (canonical) parent of the
/// store file, so the child can't read the node's secret key. `None` if no store.
fn store_hide_dir(store: Option<&Store>) -> Option<CString> {
    let abs = std::fs::canonicalize(store?.path()).ok()?;

    CString::new(abs.parent()?.to_owned().into_os_string().into_vec()).ok()
}

/// Enter fresh namespaces (user/mount/net/uts/ipc) and hide `store_dir` from the
/// child. Runs in the child after fork, before exec, so it must stay
/// async-signal-safe: raw syscalls, c-string literals, stack-formatted ids, no alloc.
#[cfg(target_os = "linux")]
fn enter_namespaces(store_dir: Option<&CStr>) -> std::io::Result<()> {
    use nix::libc;

    fn err() -> std::io::Error {
        std::io::Error::last_os_error()
    }

    /// Fixed-capacity sink so `write!` can format into the stack (no alloc); the
    /// whole buffer is then written in one syscall — the kernel requires uid_map/
    /// gid_map to be set with a single write(2), so we can't `write!` to the file.
    struct StackBuf {
        buf: [u8; 32],
        len: usize,
    }

    impl std::fmt::Write for StackBuf {
        fn write_str(&mut self, s: &str) -> std::fmt::Result {
            let end = self.len + s.len();
            let dst = self.buf.get_mut(self.len..end).ok_or(std::fmt::Error)?;
            dst.copy_from_slice(s.as_bytes());
            self.len = end;

            Ok(())
        }
    }

    /// Write `"0 <id> 1\n"` (map ns-root → our real id) in one write(2).
    fn write_map(path: &CStr, id: u32) -> std::io::Result<()> {
        use std::fmt::Write as _;

        let mut map = StackBuf {
            buf: [0; 32],
            len: 0,
        };
        let _ = writeln!(map, "0 {id} 1"); // infallible: always fits in 32 bytes

        write_file(path, &map.buf[..map.len])
    }

    fn write_file(path: &CStr, bytes: &[u8]) -> std::io::Result<()> {
        let fd = unsafe { libc::open(path.as_ptr(), libc::O_WRONLY) };
        if fd < 0 {
            return Err(err());
        }

        let n = unsafe { libc::write(fd, bytes.as_ptr().cast(), bytes.len()) };
        unsafe { libc::close(fd) };
        if n < 0 {
            return Err(err());
        }

        Ok(())
    }

    fn mnt(
        src: &CStr,
        target: &CStr,
        fstype: *const libc::c_char,
        flags: libc::c_ulong,
    ) -> std::io::Result<()> {
        let r = unsafe {
            libc::mount(
                src.as_ptr(),
                target.as_ptr(),
                fstype,
                flags,
                std::ptr::null(),
            )
        };
        if r != 0 {
            return Err(err());
        }

        Ok(())
    }

    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };

    let flags = libc::CLONE_NEWUSER
        | libc::CLONE_NEWNS
        | libc::CLONE_NEWNET
        | libc::CLONE_NEWUTS
        | libc::CLONE_NEWIPC;
    if unsafe { libc::unshare(flags) } != 0 {
        return Err(err());
    }

    // Single-id self-map: ns root → our real uid/gid (no privilege gained on host).
    write_file(c"/proc/self/setgroups", b"deny")?;
    write_map(c"/proc/self/uid_map", uid)?;
    write_map(c"/proc/self/gid_map", gid)?;

    // Contain mount propagation, then hide the store directory from the child.
    mnt(
        c"none",
        c"/",
        std::ptr::null(),
        libc::MS_REC | libc::MS_PRIVATE,
    )?;

    if let Some(dir) = store_dir {
        mnt(c"tmpfs", dir, c"tmpfs".as_ptr(), 0)?;
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn enter_namespaces(_store_dir: Option<&CStr>) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "namespace isolation is Linux-only",
    ))
}

/// On reload a `Running` child is actually dead (it died with the old supervisor).
fn reloaded(state: ProcState) -> ProcState {
    match state {
        ProcState::Running => ProcState::Stale,
        other => other,
    }
}

/// Whether an exit with `code` (`None` = killed by signal) warrants a relaunch.
fn should_restart(policy: RestartPolicy, code: Option<i32>) -> bool {
    match policy {
        RestartPolicy::Never => false,
        RestartPolicy::Always => true,
        RestartPolicy::OnFailure => code != Some(0),
    }
}

/// A freshly launched child plus the handles the table needs to track it.
struct Launched {
    child: Child,
    pid: u32,
    stdin: Arc<AsyncMutex<Option<ChildStdin>>>,
    stdout: broadcast::Receiver<Bytes>,
}

/// Durable description of a spawned process: the spawn spec is its identity,
/// `pid`/`status` are mutable state. Persisted via [`Store`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub cmd: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub pid: u32,
    pub status: ProcState,
}

#[derive(Debug)]
struct ProcEntry {
    pid: u32,
    status: ProcState,
    /// Per-handle lock so concurrent `Stdin` writes serialize without blocking the table.
    stdin: Arc<AsyncMutex<Option<ChildStdin>>>,
    /// Subscription factory: `resubscribe()` for each new stdout subscriber.
    /// The pump task holds the only Sender, so the channel closes at stdout EOF.
    stdout: broadcast::Receiver<Bytes>,
    /// Times this handle has been relaunched.
    restarts: u32,
    /// Restart enabled; cleared by `stop` (intentional) so a crash still restarts.
    armed: bool,
}

impl ProcEntry {
    /// A reloaded tombstone: no live child, so stdin is closed and stdout is empty.
    /// A persisted `Running` becomes `Stale` (the child died with the old supervisor).
    /// Restart is in-memory only, so a reloaded entry is never relaunched (`Never`).
    fn stale(rec: &Record) -> Self {
        let (_, stdout) = broadcast::channel(1); // sender dropped → closed stream
        ProcEntry {
            pid: rec.pid,
            status: reloaded(rec.status),
            stdin: Arc::new(AsyncMutex::new(None)),
            stdout,
            restarts: 0,
            armed: false,
        }
    }

    /// Bare snapshot of this entry — every field but live cgroup usage, which
    /// [`ProcessManager::fill_usage`] fills off the table lock.
    fn info(&self, handle: Handle) -> ProcInfo {
        ProcInfo {
            handle,
            pid: self.pid,
            state: self.status,
            usage: None,
            restarts: self.restarts,
        }
    }
}

/// Cheap-to-clone handle to the shared process table.
#[derive(Debug, Default, Clone)]
pub struct ProcessManager(Arc<Inner>);

#[derive(Debug, Default)]
struct Inner {
    procs: Mutex<HashMap<Handle, ProcEntry>>,
    next: AtomicU64,
    /// `None` = in-memory only (tests); `Some` = records survive restart.
    store: Option<Store>,
    /// `Some` = per-child cgroup v2 memory caps (Linux); `None` = rlimits only.
    cgroups: Option<Cgroups>,
    /// Set on shutdown so supervisor tasks stop relaunching during teardown.
    shutdown: AtomicBool,
}

impl ProcessManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build from a store: reload tombstoned records and continue the handle counter.
    pub fn with_store(store: Store, loaded: Loaded, cgroups: Option<Cgroups>) -> Self {
        let map = loaded
            .records
            .iter()
            .map(|(id, rec)| (*id, ProcEntry::stale(rec)))
            .collect();

        Self(Arc::new(Inner {
            procs: Mutex::new(map),
            next: AtomicU64::new(loaded.next_handle),
            store: Some(store),
            cgroups,
            shutdown: AtomicBool::new(false),
        }))
    }

    /// Launch a process per `spec`, track it, and supervise it (relaunch per policy).
    pub fn spawn(&self, spec: Spec) -> std::io::Result<(Handle, u32)> {
        let id = self.0.next.fetch_add(1, Ordering::Relaxed) + 1; // handles start at 1
        let launched = self.launch_child(id, &spec)?;
        let pid = launched.pid;

        self.0.procs.lock().insert(
            id,
            ProcEntry {
                pid,
                status: ProcState::Running,
                stdin: launched.stdin,
                stdout: launched.stdout,
                restarts: 0,
                armed: true,
            },
        );
        self.persist(id, &spec, pid, ProcState::Running);

        // Supervise: wait for exit, relaunch per policy until stopped or capped.
        tokio::spawn(self.clone().supervise(id, spec, launched.child));

        Ok((id, pid))
    }

    /// OS-level launch for `id` per `spec`: create the cgroup leaf, spawn the
    /// sandboxed child (caps + cgroup join in pre_exec), and start the stdout pump.
    /// Does not touch the table — callers wire the result into a [`ProcEntry`].
    fn launch_child(&self, id: Handle, spec: &Spec) -> std::io::Result<Launched> {
        // Linux: a per-child cgroup leaf the child joins itself (in pre_exec, below).
        let cg_procs = self
            .0
            .cgroups
            .as_ref()
            .and_then(|cg| cg.create_leaf(id, &spec.limits));

        let mut command = Command::new(&spec.cmd);
        command
            // Scrub the supervisor's env (may hold secrets); keep only a sane PATH
            // so bare command names resolve, plus whatever the client passes.
            .env_clear()
            .env("PATH", DEFAULT_PATH)
            .args(&spec.args)
            .envs(spec.env.iter().map(|(k, v)| (k, v)))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .process_group(0); // own group: signals reach the child and its descendants

        // Per-child sandbox, applied in the child after fork, before exec.
        let (memory, cpu) = (spec.limits.memory, spec.limits.cpu);
        let isolate = spec.isolate;
        let store_dir = isolate
            .then(|| store_hide_dir(self.0.store.as_ref()))
            .flatten();
        unsafe {
            command.pre_exec(move || {
                use nix::sys::resource::{Resource, setrlimit};

                // die if the supervisor dies, even on SIGKILL. Tied to the
                // spawning worker thread, which lives for the runtime's lifetime.
                #[cfg(target_os = "linux")]
                nix::sys::prctl::set_pdeathsig(nix::sys::signal::Signal::SIGKILL)
                    .map_err(|e| std::io::Error::from_raw_os_error(e as i32))?;

                // Portable address-space cap (blunt; cgroup memory.max is accurate).
                if let Some(bytes) = memory {
                    setrlimit(Resource::RLIMIT_AS, bytes, bytes)
                        .map_err(|e| std::io::Error::from_raw_os_error(e as i32))?;
                }
                // Portable total-CPU-time cap: SIGKILL after this many CPU seconds.
                if let Some(secs) = cpu {
                    setrlimit(Resource::RLIMIT_CPU, secs, secs)
                        .map_err(|e| std::io::Error::from_raw_os_error(e as i32))?;
                }
                // Join our cgroup leaf before exec, so memory.max covers exec-time
                // pages and they're charged to the leaf (accurate memory.current).
                if let Some(f) = &cg_procs {
                    join_cgroup(f)?;
                }

                // Last, drop into fresh namespaces (so the cgroup join above still
                // runs with host privileges).
                if isolate {
                    enter_namespaces(store_dir.as_deref())?;
                }

                Ok(())
            });
        }

        let mut child = match command.spawn() {
            Ok(c) => c,
            Err(e) => {
                if let Some(cg) = &self.0.cgroups {
                    cg.remove(id); // spawn failed after the leaf was created
                }

                return Err(e);
            }
        };

        let pid = child.id().expect("child has a pid before it is awaited");
        let stdin = Arc::new(AsyncMutex::new(child.stdin.take()));

        // Pump task: read stdout in chunks into a broadcast channel for subscribers.
        // It owns the only Sender, so the channel closes when stdout hits EOF.
        let (tx, stdout) = broadcast::channel::<Bytes>(STDOUT_CAP);
        if let Some(mut out) = child.stdout.take() {
            tokio::spawn(async move {
                let mut buf = BytesMut::with_capacity(CHUNK);
                // read_buf fills the spare capacity; split().freeze() hands the chunk
                // off as an owned Bytes with no extra copy. With no subscribers we
                // still drain the pipe (so the child never blocks) but reuse the
                // buffer instead of allocating for an audience that isn't there.
                while let Ok(n) = out.read_buf(&mut buf).await {
                    if n == 0 {
                        break; // EOF
                    }

                    if tx.receiver_count() == 0 {
                        buf.clear();
                    } else {
                        let _ = tx.send(buf.split().freeze());
                        buf.reserve(CHUNK);
                    }
                }
            });
        }

        Ok(Launched {
            child,
            pid,
            stdin,
            stdout,
        })
    }

    /// Persist a spawn/relaunch record (best-effort: a store error never fails it).
    fn persist(&self, id: Handle, spec: &Spec, pid: u32, status: ProcState) {
        if let Some(store) = &self.0.store {
            let rec = Record {
                cmd: spec.cmd.clone(),
                args: spec.args.clone(),
                env: spec.env.clone(),
                pid,
                status,
            };

            if let Err(e) = store.put(id, &rec) {
                warn!(handle = id, "failed to persist record: {e:#}");
            }
        }
    }

    /// Own the child, record each exit, and relaunch per `spec.policy` with backoff
    /// until the process is stopped, the retry cap is hit, or the supervisor shuts down.
    async fn supervise(self, id: Handle, spec: Spec, first: Child) {
        let mut child = first;
        let mut attempt: u32 = 0; // consecutive fast restarts, for backoff + cap

        loop {
            let started = Instant::now();
            let code = child.wait().await.ok().and_then(|s| s.code());

            if let Some(e) = self.0.procs.lock().get_mut(&id) {
                e.status = ProcState::Exited(code);
            }
            if let Some(store) = &self.0.store
                && let Err(e) = store.set_status(id, ProcState::Exited(code))
            {
                warn!(handle = id, "failed to persist exit: {e:#}");
            }
            if let Some(cg) = &self.0.cgroups {
                cg.remove(id);
            }

            let armed = self.0.procs.lock().get(&id).is_some_and(|e| e.armed);
            if self.0.shutdown.load(Ordering::Relaxed)
                || !armed
                || !should_restart(spec.policy, code)
            {
                break;
            }

            // Reset the counter if the child stayed up long enough to count as healthy.
            attempt = if started.elapsed() >= STABLE_RESET {
                1
            } else {
                attempt + 1
            };
            if spec.max_retries != 0 && attempt > spec.max_retries {
                warn!(
                    handle = id,
                    restarts = attempt - 1,
                    "restart cap hit; giving up"
                );

                break;
            }

            // Exponential backoff: base * 2^(attempt-1), capped (shift capped too).
            let delay = (RESTART_BASE * (1 << (attempt - 1).min(5))).min(RESTART_MAX);
            info!(
                handle = id,
                attempt,
                delay_ms = delay.as_millis() as u64,
                "restarting"
            );
            tokio::time::sleep(delay).await;

            if self.0.shutdown.load(Ordering::Relaxed) {
                break;
            }

            match self.launch_child(id, &spec) {
                Ok(l) => {
                    child = l.child;
                    if let Some(e) = self.0.procs.lock().get_mut(&id) {
                        e.pid = l.pid;
                        e.status = ProcState::Running;
                        e.stdin = l.stdin;
                        e.stdout = l.stdout;
                        e.restarts += 1;
                    }

                    self.persist(id, &spec, l.pid, ProcState::Running);
                }
                Err(e) => {
                    warn!(handle = id, "relaunch failed: {e}; giving up");

                    break;
                }
            }
        }
    }

    /// Handles of all tracked processes, ascending.
    pub fn list(&self) -> Vec<Handle> {
        let mut handles: Vec<Handle> = self.0.procs.lock().keys().copied().collect();
        handles.sort_unstable();

        handles
    }

    /// Fill live cgroup usage into a bare [`proc_info`]: filesystem I/O, so call it
    /// after releasing the table lock. Stays `None` when not running or no cgroup.
    fn fill_usage(&self, info: &mut ProcInfo) {
        if matches!(info.state, ProcState::Running)
            && let Some(cg) = &self.0.cgroups
        {
            info.usage = cg.usage(info.handle);
        }
    }

    /// Snapshot of every tracked process, ascending by handle.
    pub fn snapshot(&self) -> Vec<ProcInfo> {
        // Copy the cheap fields under the lock; fill cgroup usage (I/O) after
        // releasing it, so the table lock is never held across the filesystem.
        let mut out: Vec<ProcInfo> = {
            let map = self.0.procs.lock();
            map.iter().map(|(&h, p)| p.info(h)).collect()
        };
        for info in &mut out {
            self.fill_usage(info);
        }
        out.sort_by_key(|i| i.handle);

        out
    }

    /// Snapshot for one handle.
    pub fn query(&self, id: Handle) -> Result<ProcInfo, ControlError> {
        let mut info = {
            let map = self.0.procs.lock();
            map.get(&id).ok_or(ControlError::NotFound(id))?.info(id)
        };
        self.fill_usage(&mut info);

        Ok(info)
    }

    /// Stop a process and disarm its restart policy (so this intentional stop, unlike
    /// a crash, isn't relaunched). SIGTERMs it now and, if it's still up after
    /// `STOP_DEADLINE`, escalates to SIGKILL. A no-op if it isn't running.
    pub fn stop(&self, id: Handle) -> Result<(), ControlError> {
        let pid = {
            let mut map = self.0.procs.lock();
            let entry = map.get_mut(&id).ok_or(ControlError::NotFound(id))?;
            entry.armed = false;

            matches!(entry.status, ProcState::Running).then_some(entry.pid)
        };

        if let Some(pid) = pid {
            tokio::spawn(self.clone().terminate(id, pid));
        }

        Ok(())
    }

    /// SIGTERM a child, then SIGKILL it if it's still running after `STOP_DEADLINE`.
    /// The escalation targets the cgroup (or process group) rather than the bare pid,
    /// so it can't land on a reused pid.
    async fn terminate(self, id: Handle, pid: u32) {
        let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
        tokio::time::sleep(STOP_DEADLINE).await;

        let alive = self
            .0
            .procs
            .lock()
            .get(&id)
            .is_some_and(|e| matches!(e.status, ProcState::Running));

        if !alive {
            return; // exited within the grace period
        }

        warn!(handle = id, pid, "SIGTERM ignored; escalating to SIGKILL");

        if let Some(cg) = &self.0.cgroups {
            cg.kill(id);
        } else {
            let _ = kill(Pid::from_raw(-(pid as i32)), Signal::SIGKILL); // process group
        }
    }

    /// Send signal `sig` to a tracked process.
    pub fn signal(&self, id: Handle, sig: i32) -> Result<(), ControlError> {
        let pid = {
            let map = self.0.procs.lock();
            let entry = map.get(&id).ok_or(ControlError::NotFound(id))?;
            // Never signal a non-running entry: its pid may have been reused.
            if !matches!(entry.status, ProcState::Running) {
                return Err(ControlError::BadRequest("process is not running".into()));
            }
            entry.pid
        };

        let signal = Signal::try_from(sig)
            .map_err(|_| ControlError::BadRequest(format!("invalid signal {sig}")))?;

        kill(Pid::from_raw(pid as i32), signal).map_err(|e| {
            warn!(handle = id, "signal delivery failed: {e}");

            ControlError::BadRequest("signal delivery failed".into())
        })
    }

    /// Pipe a raw byte stream into a process's stdin until EOF or the child closes
    /// its read end. The per-process stdin lock is held for the duration so that only
    /// one writer is active at a time.
    pub async fn pipe_stdin<R>(&self, id: Handle, recv: &mut R) -> Result<(), ControlError>
    where
        R: AsyncRead + Unpin,
    {
        // Hold the table lock only to clone the per-handle lock, then copy off-lock.
        let stdin = {
            let map = self.0.procs.lock();
            map.get(&id)
                .ok_or(ControlError::NotFound(id))?
                .stdin
                .clone()
        };

        let mut guard = stdin.lock().await;
        let Some(s) = guard.as_mut() else {
            return Err(ControlError::BadRequest("stdin unavailable".into()));
        };

        match tokio::io::copy(recv, s).await {
            Ok(_) => {
                // EOF on the QUIC stream: close the child's stdin so it sees EOF too.
                *guard = None;
                Ok(())
            }
            Err(e) => {
                *guard = None; // broken pipe (child closed its read end); mark closed
                warn!(handle = id, "stdin pipe closed: {e}");

                Err(ControlError::BadRequest("stdin pipe closed".into()))
            }
        }
    }

    /// A fresh receiver for a process's stdout stream.
    pub fn subscribe(&self, id: Handle) -> Result<broadcast::Receiver<Bytes>, ControlError> {
        let map = self.0.procs.lock();
        Ok(map
            .get(&id)
            .ok_or(ControlError::NotFound(id))?
            .stdout
            .resubscribe())
    }

    /// Drop a tracked process's record (table + store). Refused while it runs.
    pub fn forget(&self, id: Handle) -> Result<(), ControlError> {
        {
            let mut map = self.0.procs.lock();
            let entry = map.get(&id).ok_or(ControlError::NotFound(id))?;
            if matches!(entry.status, ProcState::Running) {
                return Err(ControlError::BadRequest(
                    "cannot forget a running process".into(),
                ));
            }
            map.remove(&id);
        }

        if let Some(store) = &self.0.store
            && let Err(e) = store.remove(id)
        {
            warn!(handle = id, "failed to forget from store: {e:#}");
        }

        Ok(())
    }

    /// Graceful supervisor shutdown: stop relaunching, SIGTERM every child, wait for
    /// them to exit (returning early once all do, capped at `STOP_DEADLINE`), then
    /// force any survivors down — `cgroup.kill` where available, else SIGKILL each
    /// process group.
    pub async fn kill_all(&self) {
        self.0.shutdown.store(true, Ordering::Relaxed); // stop supervisors relaunching

        let pids: Vec<i32> = self.0.procs.lock().values().map(|e| e.pid as i32).collect();
        for pid in &pids {
            let _ = kill(Pid::from_raw(*pid), Signal::SIGTERM);
        }

        // Give children a grace period to exit, but return as soon as they all have.
        let deadline = Instant::now() + STOP_DEADLINE;
        while Instant::now() < deadline && self.any_running() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Force down whatever ignored SIGTERM (harmless if already gone).
        if let Some(cg) = &self.0.cgroups {
            cg.shutdown();
        } else {
            for pid in &pids {
                let _ = kill(Pid::from_raw(-pid), Signal::SIGKILL); // process group
            }
        }
    }

    /// Whether any tracked process is still running (for the shutdown grace wait).
    fn any_running(&self) -> bool {
        self.0
            .procs
            .lock()
            .values()
            .any(|e| matches!(e.status, ProcState::Running))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    async fn wait_until_exited(pm: &ProcessManager, id: Handle) -> bool {
        for _ in 0..50 {
            if !matches!(pm.query(id).unwrap().state, ProcState::Running) {
                return true;
            }

            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        false
    }

    /// A minimal spec (no limits, no restart) for the common test case.
    fn spec(cmd: &str, args: &[&str]) -> Spec {
        Spec {
            cmd: cmd.into(),
            args: args.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn spawn_tracks_handle() {
        let pm = ProcessManager::new();
        let (id, pid) = pm.spawn(spec("sleep", &["30"])).unwrap();

        assert!(pid > 0);
        assert_eq!(pm.list(), vec![id]);
        assert!(matches!(pm.query(id).unwrap().state, ProcState::Running));

        pm.kill_all().await;
    }

    #[tokio::test]
    async fn signal_then_query_reports_exit() {
        let pm = ProcessManager::new();
        let (id, _) = pm.spawn(spec("sleep", &["30"])).unwrap();

        pm.signal(id, 15).unwrap(); // SIGTERM

        assert!(
            wait_until_exited(&pm, id).await,
            "did not exit after SIGTERM"
        );
        assert!(!matches!(pm.query(id).unwrap().state, ProcState::Running));
    }

    #[tokio::test]
    async fn stdin_pipe_succeeds() {
        use std::io::Cursor;

        let pm = ProcessManager::new();
        let (id, _) = pm.spawn(spec("cat", &[])).unwrap();

        let data = b"hello\n";
        let mut input = Cursor::new(&data[..]);
        pm.pipe_stdin(id, &mut input).await.unwrap();

        // cat exits once its stdin reaches EOF.
        assert!(wait_until_exited(&pm, id).await);
    }

    #[tokio::test]
    async fn unknown_handle_is_not_found() {
        let pm = ProcessManager::new();

        assert!(matches!(pm.query(999), Err(ControlError::NotFound(999))));
        assert!(matches!(
            pm.signal(999, 15),
            Err(ControlError::NotFound(999))
        ));
    }

    #[tokio::test]
    async fn forget_drops_an_exited_process() {
        let pm = ProcessManager::new();
        let (id, _) = pm.spawn(spec("sleep", &["30"])).unwrap();

        assert!(
            pm.forget(id).is_err(),
            "a running process cannot be forgotten"
        );

        pm.signal(id, 15).unwrap();
        assert!(
            wait_until_exited(&pm, id).await,
            "did not exit after SIGTERM"
        );

        pm.forget(id).expect("forget exited");
        assert!(pm.list().is_empty());
    }

    #[tokio::test]
    async fn child_env_is_scrubbed() {
        let pm = ProcessManager::new();
        // `sleep` first so we subscribe before any output (broadcast has no replay).
        let (id, _) = pm
            .spawn(Spec {
                cmd: "sh".into(),
                args: vec!["-c".into(), "sleep 0.2; env".into()],
                env: vec![("FOO".into(), "bar".into())],
                ..Default::default()
            })
            .unwrap();
        let mut rx = pm.subscribe(id).unwrap();

        let mut out = Vec::new();
        while let Ok(chunk) = rx.recv().await {
            out.extend_from_slice(&chunk);
        }
        let out = String::from_utf8_lossy(&out);

        // env_clear took effect: PATH is exactly our default (not the inherited,
        // longer host PATH), and only the client-provided var is added.
        assert!(
            out.contains("PATH=/usr/bin:/bin"),
            "env not scrubbed: {out}"
        );
        assert!(out.contains("FOO=bar"), "provided env missing: {out}");
    }
}
