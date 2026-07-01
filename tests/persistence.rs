//! M7: process records and node identity survive a supervisor restart.

use std::path::PathBuf;

use mesh_supervisor::{
    process::{ProcessManager, Record},
    proto::{ProcState, Spec},
    store::Store,
};

/// A unique scratch path so concurrent test runs don't share a store file.
fn temp_store_path() -> PathBuf {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("p2p-store-{}-{nanos}.redb", std::process::id()))
}

#[tokio::test]
async fn process_state_survives_restart() {
    let path = temp_store_path();
    let _ = std::fs::remove_file(&path);

    // The record is left "running" after a supservisor stops
    {
        let store = Store::open(&path).expect("store open failed");
        store
            .put(
                1,
                &Record {
                    cmd: "sleep".into(),
                    args: vec!["30".into()],
                    env: vec![],
                    pid: 4242,
                    status: ProcState::Running,
                },
            )
            .expect("put");
    }

    // Restart: reload the table.
    let store = Store::open(&path).expect("store reopen failed");
    let loaded = store.load().expect("store load failed");
    assert_eq!(loaded.next_handle, 1);

    let pm = ProcessManager::with_store(store, loaded, None);

    // The child is gone, so the record reloads as a stale tombstone (pid preserved).
    assert_eq!(pm.list(), vec![1]);
    let info = pm.query(1).expect("query failed");
    assert_eq!(
        info.state,
        ProcState::Stale,
        "reloaded record must be stale"
    );
    assert_eq!(info.pid, 4242);

    // Signalling a stale entry is refused (its pid may have been reused).
    assert!(pm.signal(1, 15).is_err());

    // A fresh spawn continues the counter
    let (new_handle, _pid) = pm
        .spawn(Spec {
            cmd: "true".into(),
            ..Default::default()
        })
        .expect("spawn");
    assert!(new_handle > 1, "new handle must not reuse a reloaded one");

    let _ = std::fs::remove_file(&path);
}

#[test]
fn identity_survives_restart() {
    let path = temp_store_path();
    let _ = std::fs::remove_file(&path);

    let first = Store::open(&path)
        .expect("store open failed")
        .secret_key()
        .expect("key")
        .public();
    let second = Store::open(&path)
        .expect("store reopen failed")
        .secret_key()
        .expect("key")
        .public();
    assert_eq!(first, second, "identity must survive restart");

    let _ = std::fs::remove_file(&path);
}
