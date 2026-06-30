//! cgroup v2 memory caps for child processes (Linux, unprivileged via systemd
//! delegation). Best-effort: `detect` returns `None` where unsupported and the
//! caller falls back to rlimits.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tracing::{debug, warn};

use crate::proto::{Handle, Limits, Usage};

const MOUNT: &str = "/sys/fs/cgroup";

/// Fixed name so a restart can find and sweep a previous run's leftovers.
const GROUP: &str = "p2p-telemetry";

/// Parent cgroup under which each child gets a leaf with `memory.max`.
#[derive(Debug)]
pub struct Cgroups {
    parent: PathBuf,
}

impl Cgroups {
    /// Find a writable v2 parent with the memory controller, create our group
    /// (sweeping any leftovers first), and enable `memory` for the leaves.
    pub fn detect() -> Option<Self> {
        let host = parent_with_memory()?;
        let parent = host.join(GROUP);

        remove_group(&parent); // sweep a prior run's leftovers
        fs::create_dir(&parent).ok()?;
        if let Err(e) = fs::write(parent.join("cgroup.subtree_control"), "+memory +pids") {
            warn!("cgroup controller delegation failed: {e}");
            let _ = fs::remove_dir(&parent);

            return None;
        }
        debug!(parent = %parent.display(), "cgroup memory/pids caps enabled");

        Some(Self { parent })
    }

    /// Create a leaf for `handle`, apply its caps, and return its `cgroup.procs`
    /// opened for writing. The child joins itself from `pre_exec` (before `exec`),
    /// so exec-time pages are capped and charged to the leaf, not the parent.
    pub fn create_leaf(&self, handle: Handle, limits: &Limits) -> Option<fs::File> {
        let leaf = self.parent.join(handle.to_string());
        if let Err(e) = fs::create_dir(&leaf) {
            warn!(handle, "cgroup create failed: {e}");

            return None;
        }
        if let Some(bytes) = limits.memory
            && let Err(e) = fs::write(leaf.join("memory.max"), bytes.to_string())
        {
            warn!(handle, "cgroup memory.max failed: {e}");
        }
        if let Some(n) = limits.pids
            && let Err(e) = fs::write(leaf.join("pids.max"), n.to_string())
        {
            warn!(handle, "cgroup pids.max failed: {e}");
        }

        match fs::OpenOptions::new()
            .write(true)
            .open(leaf.join("cgroup.procs"))
        {
            Ok(f) => Some(f),
            Err(e) => {
                warn!(handle, "cgroup.procs open failed: {e}");

                None
            }
        }
    }

    /// Read live usage from a leaf: `memory.current` + `cpu.stat`'s cumulative
    /// `usage_usec`. `None` if the leaf or either counter is gone (e.g. just reaped).
    pub fn usage(&self, handle: Handle) -> Option<Usage> {
        let leaf = self.parent.join(handle.to_string());
        let mem_bytes = fs::read_to_string(leaf.join("memory.current"))
            .ok()?
            .trim()
            .parse()
            .ok()?;
        let cpu_usec = fs::read_to_string(leaf.join("cpu.stat"))
            .ok()?
            .lines()
            .find_map(|l| l.strip_prefix("usage_usec ")?.trim().parse().ok())?;

        Some(Usage {
            mem_bytes,
            cpu_usec,
        })
    }

    /// SIGKILL every process in a leaf via `cgroup.kill`, without removing it (the
    /// reaper still does the rmdir). Atomic and reuse-safe. Best-effort.
    pub fn kill(&self, handle: Handle) {
        let leaf = self.parent.join(handle.to_string());
        let _ = fs::write(leaf.join("cgroup.kill"), "1");
    }

    /// Kill any survivors in the leaf for `handle` and remove it (called on reap).
    pub fn remove(&self, handle: Handle) {
        let leaf = self.parent.join(handle.to_string());
        let _ = fs::write(leaf.join("cgroup.kill"), "1");
        rmdir_retry(&leaf);
    }

    /// Kill all children and remove the whole group (graceful shutdown).
    pub fn shutdown(&self) {
        remove_group(&self.parent);
    }
}

/// Nearest ancestor of our own cgroup whose `subtree_control` already grants the
/// memory controller — the place we can create capped child cgroups.
fn parent_with_memory() -> Option<PathBuf> {
    let self_cg = fs::read_to_string("/proc/self/cgroup").ok()?;
    let rel = self_cg.lines().next()?.split_once("::")?.1.trim();
    let dir = PathBuf::from(MOUNT).join(rel.strip_prefix('/').unwrap_or(rel));

    let mut cur = dir.as_path();
    while let Some(parent) = cur.parent() {
        if !parent.starts_with(MOUNT) {
            break;
        }

        let sc = fs::read_to_string(parent.join("cgroup.subtree_control")).unwrap_or_default();
        if sc.split_whitespace().any(|c| c == "memory") {
            return Some(parent.to_path_buf());
        }
        if parent == Path::new(MOUNT) {
            break;
        }
        cur = parent;
    }

    None
}

/// Kill + remove every leaf, then the parent. cgroup dirs can't be `remove_dir_all`'d
/// (control files aren't unlinkable), so leaves are removed one by one.
fn remove_group(parent: &Path) {
    if let Ok(entries) = fs::read_dir(parent) {
        for leaf in entries.flatten().map(|e| e.path()).filter(|p| p.is_dir()) {
            let _ = fs::write(leaf.join("cgroup.kill"), "1");
            rmdir_retry(&leaf);
        }
    }
    rmdir_retry(parent);
}

/// Remove a cgroup dir, retrying briefly while the kernel reaps killed processes.
fn rmdir_retry(dir: &Path) {
    for _ in 0..50 {
        if fs::remove_dir(dir).is_ok() || !dir.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    warn!(dir = %dir.display(), "cgroup leftover could not be removed");
}
