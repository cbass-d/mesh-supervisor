//! The per-child sandbox applied between fork and exec.
//!
//! Everything reachable from [`Sandbox::apply`] runs in the child after `fork`
//! and before `exec` (`CommandExt::pre_exec`), so it **must stay
//! async-signal-safe**: raw syscalls, C-string literals, stack-formatted ids —
//! no allocation, no locks (see the `pre_exec` Safety docs).

use std::ffi::{CStr, CString};
use std::fs::File;
use std::os::unix::ffi::OsStringExt;

use crate::proto::Limits;
use crate::store::Store;

/// Per-child sandbox: pdeathsig, rlimits, cgroup join, optional namespaces.
/// Built in the parent (allocation is fine in [`Sandbox::new`]); applied in
/// the child ([`Sandbox::apply`], async-signal-safe code only).
#[derive(Debug)]
pub(crate) struct Sandbox {
    memory: Option<u64>,
    cpu: Option<u64>,
    cgroup_procs: Option<File>,
    isolate: bool,
    store_dir: Option<CString>,
}

impl Sandbox {
    /// Capture everything [`Sandbox::apply`] needs while still in the parent.
    pub(crate) fn new(
        limits: &Limits,
        isolate: bool,
        cgroup_procs: Option<File>,
        store: Option<&Store>,
    ) -> Self {
        Self {
            memory: limits.memory,
            cpu: limits.cpu,
            cgroup_procs,
            isolate,
            store_dir: isolate.then(|| store_hide_dir(store)).flatten(),
        }
    }

    /// Runs in the child after fork, before exec (`pre_exec`). Order matters:
    /// pdeathsig, rlimits, cgroup join (still with host privileges), and
    /// namespaces last.
    pub(crate) fn apply(&self) -> std::io::Result<()> {
        use nix::sys::resource::{Resource, setrlimit};

        // die if the supervisor dies, even on SIGKILL. Tied to the
        // spawning worker thread, which lives for the runtime's lifetime.
        #[cfg(target_os = "linux")]
        nix::sys::prctl::set_pdeathsig(nix::sys::signal::Signal::SIGKILL)
            .map_err(|e| std::io::Error::from_raw_os_error(e as i32))?;

        // Portable address-space cap (blunt; cgroup memory.max is accurate).
        if let Some(bytes) = self.memory {
            setrlimit(Resource::RLIMIT_AS, bytes, bytes)
                .map_err(|e| std::io::Error::from_raw_os_error(e as i32))?;
        }
        // Portable total-CPU-time cap: SIGKILL after this many CPU seconds.
        if let Some(secs) = self.cpu {
            setrlimit(Resource::RLIMIT_CPU, secs, secs)
                .map_err(|e| std::io::Error::from_raw_os_error(e as i32))?;
        }
        // Join our cgroup leaf before exec, so memory.max covers exec-time
        // pages and they're charged to the leaf (accurate memory.current).
        if let Some(f) = &self.cgroup_procs {
            join_cgroup(f)?;
        }

        // Last, drop into fresh namespaces (so the cgroup join above still
        // runs with host privileges).
        if self.isolate {
            enter_namespaces(self.store_dir.as_deref())?;
        }

        Ok(())
    }
}

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
