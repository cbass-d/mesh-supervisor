//! `--isolate`: a spawned child runs in fresh user/mount/net/uts/ipc namespaces.
//! Skipped where unprivileged user namespaces are unavailable (some hardened
//! kernels), so it stays portable. Uses `ProcessManager` directly (no endpoint).

use std::path::PathBuf;

use p2p_telemtry::{process::ProcessManager, proto::Spec, store::Store};

/// Spawn `sh -c <script>` isolated and return its full stdout, or `None` if the
/// kernel rejects the namespace setup (→ skip).
async fn isolated_output(pm: &ProcessManager, script: &str) -> Option<String> {
    let spec = Spec {
        cmd: "sh".into(),
        args: vec!["-c".into(), script.into()],
        isolate: true,
        ..Default::default()
    };
    let (id, _) = match pm.spawn(spec) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("unprivileged namespaces unavailable ({e}) — skipping");

            return None;
        }
    };

    // Subscribe before the script's `sleep` elapses, then drain to EOF (exit).
    let mut rx = pm.subscribe(id).unwrap();
    let mut out = Vec::new();
    while let Ok(chunk) = rx.recv().await {
        out.extend_from_slice(&chunk);
    }

    Some(String::from_utf8_lossy(&out).into_owned())
}

#[tokio::test]
async fn isolated_child_has_fresh_user_and_net_namespaces() {
    let pm = ProcessManager::new();
    let script = "sleep 0.2; echo MAP; cat /proc/self/uid_map; echo NET; cat /proc/net/dev";
    let Some(out) = isolated_output(&pm, script).await else {
        return;
    };

    // user namespace: a single-id self-map (`0 <uid> 1`) is installed.
    let map = out
        .lines()
        .skip_while(|l| l.trim() != "MAP")
        .nth(1)
        .expect("uid_map line");
    let fields: Vec<&str> = map.split_whitespace().collect();
    assert_eq!(fields.len(), 3, "uid_map malformed: {map:?}");
    assert_eq!(fields[0], "0", "ns root maps from 0");
    assert_eq!(fields[2], "1", "single id mapped");

    // network namespace: /proc/net/dev (per-netns) lists only loopback.
    let ifaces: Vec<&str> = out.lines().filter(|l| l.contains(':')).collect();
    assert_eq!(ifaces.len(), 1, "fresh netns should have only lo:\n{out}");
    assert!(ifaces[0].contains("lo"), "the one interface is loopback");
}

#[tokio::test]
async fn isolated_child_cannot_read_the_store() {
    // A store in its own directory, so hiding that dir only hides the store.
    let dir = std::env::temp_dir().join(format!("p2p-ns-{}", std::process::id()));
    let _ = std::fs::create_dir(&dir);
    let store_path: PathBuf = dir.join("node.redb");
    let _ = std::fs::remove_file(&store_path);

    let store = Store::open(&store_path).expect("store");
    let loaded = store.load().expect("load");
    let pm = ProcessManager::with_store(store, loaded, None);

    let script = format!(
        "sleep 0.2; cat {} 2>/dev/null; echo DONE:$?",
        store_path.display()
    );
    let Some(out) = isolated_output(&pm, &script).await else {
        let _ = std::fs::remove_dir_all(&dir);

        return;
    };

    // The store's directory is overmounted with an empty tmpfs, so `cat` fails.
    assert!(
        out.contains("DONE:1"),
        "isolated child should not be able to read the store:\n{out}"
    );
    // And the supervisor's own view is untouched — the file still exists.
    assert!(store_path.exists(), "host store must be unaffected");

    let _ = std::fs::remove_dir_all(&dir);
}
