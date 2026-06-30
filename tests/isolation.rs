//! M8: per-child cgroup v2 memory cap + teardown. Skipped where cgroup delegation
//! is unavailable (non-Linux, no systemd user subtree), so it stays portable.

use std::path::{Path, PathBuf};

use p2p_telemtry::{
    cgroup::Cgroups,
    process::ProcessManager,
    proto::{Limits, Spec},
    store::Store,
};

fn temp_store_path() -> PathBuf {
    use std::time::{SystemTime, UNIX_EPOCH};

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    std::env::temp_dir().join(format!("p2p-iso-{}-{nanos}.redb", std::process::id()))
}

#[tokio::test]
async fn child_is_capped_and_torn_down() {
    let Some(cgroups) = Cgroups::detect() else {
        eprintln!("cgroup v2 delegation unavailable — skipping isolation test");

        return;
    };

    let path = temp_store_path();
    let _ = std::fs::remove_file(&path);
    let store = Store::open(&path).expect("store");
    let loaded = store.load().expect("load");
    let pm = ProcessManager::with_store(store, loaded, Some(cgroups));

    let cap = 64 * 1024 * 1024;
    let (handle, pid) = pm
        .spawn(Spec {
            cmd: "sleep".into(),
            args: vec!["30".into()],
            limits: Limits {
                memory: Some(cap),
                pids: Some(32),
                ..Default::default()
            },
            ..Default::default()
        })
        .expect("failed to spawn process");

    // The child landed in its own leaf cgroup (black-box via /proc/<pid>/cgroup).
    let cg = std::fs::read_to_string(format!("/proc/{pid}/cgroup"))
        .expect("falied to read child cgroup");
    let rel = cg
        .lines()
        .next()
        .unwrap()
        .split_once("::")
        .unwrap()
        .1
        .trim();

    assert!(
        rel.ends_with(&format!("/p2p-telemetry/{handle}")),
        "child not in its leaf cgroup: {rel}"
    );

    let leaf = format!("/sys/fs/cgroup{rel}");
    let mem_max = std::fs::read_to_string(format!("{leaf}/memory.max")).expect("memory.max");
    let pids_max = std::fs::read_to_string(format!("{leaf}/pids.max")).expect("pids.max");

    assert_eq!(mem_max.trim(), cap.to_string());
    assert_eq!(pids_max.trim(), "32");

    // Live usage is read back from the leaf: a running child charges some memory.
    let info = pm.query(handle).expect("query");
    let usage = info.usage.expect("running child has cgroup usage");
    assert!(usage.mem_bytes > 0, "memory.current should be non-zero");

    // Shutdown kills the child and removes the cgroup — no leftovers.
    pm.kill_all().await;
    assert!(
        !Path::new(&leaf).exists(),
        "cgroup leaf leaked after shutdown"
    );

    let _ = std::fs::remove_file(&path);
}
