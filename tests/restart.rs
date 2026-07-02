//! Restart policy: relaunch on failure up to a cap, leave clean exits alone, and
//! let `stop` disarm an otherwise-Always policy. Uses `ProcessManager` directly
//! (no endpoint), so these run without the ~7s bind latency.

use std::time::Duration;

use mesh_supervisor::{
    process::ProcessManager,
    proto::{ProcState, RestartPolicy, Spec},
};

fn spec(cmd: &str, policy: RestartPolicy, max_retries: u32) -> Spec {
    Spec {
        cmd: cmd.into(),
        policy,
        max_retries,
        ..Default::default()
    }
}

#[tokio::test]
async fn on_failure_restarts_until_cap() {
    let pm = ProcessManager::new();

    // `false` exits 1 immediately, so OnFailure keeps relaunching it.
    let (id, _) = pm
        .spawn(spec("false", RestartPolicy::OnFailure, 2))
        .expect("spawn");

    // Backoff is 1s then 2s, so the two allowed restarts land within a few seconds.
    let mut restarts = 0;
    for _ in 0..40 {
        restarts = pm.query(id).unwrap().restarts;
        if restarts >= 2 {
            break;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(restarts, 2, "should restart up to the cap");

    // Past the cap it gives up: stays Exited and the count doesn't climb further.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let info = pm.query(id).unwrap();
    assert_eq!(info.restarts, 2, "must not exceed the cap");
    assert!(
        matches!(info.state, ProcState::Exited(_)),
        "gave up terminal"
    );

    pm.kill_all().await;
}

#[tokio::test]
async fn clean_exit_is_not_restarted() {
    let pm = ProcessManager::new();

    // `true` exits 0, so OnFailure must leave it alone.
    let (id, _) = pm
        .spawn(spec("true", RestartPolicy::OnFailure, 0))
        .expect("spawn");

    // Wait past the base backoff: a clean exit under OnFailure must not relaunch.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let info = pm.query(id).unwrap();
    assert_eq!(info.restarts, 0, "clean exit under OnFailure is terminal");
    assert!(matches!(info.state, ProcState::Exited(Some(0))));
}

#[tokio::test]
async fn stop_disarms_restart() {
    let pm = ProcessManager::new();
    let (id, _) = pm
        .spawn(Spec {
            args: vec!["30".into()],
            ..spec("sleep", RestartPolicy::Always, 0)
        })
        .expect("spawn");

    assert!(matches!(pm.query(id).unwrap().state, ProcState::Running));
    pm.stop(id).expect("calling stop on process manager failed");

    // Despite Always, an intentional stop must not come back (wait past the backoff).
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let info = pm.query(id).unwrap();
    assert_eq!(info.restarts, 0, "a stopped process must not restart");
    assert!(matches!(info.state, ProcState::Exited(_)));
}

#[tokio::test]
async fn stop_during_backoff_does_not_relaunch() {
    let pm = ProcessManager::new();

    // `false` exits 1 immediately; under Always it enters the ~1s restart backoff.
    let (id, _) = pm
        .spawn(spec("false", RestartPolicy::Always, 0))
        .expect("spawn");

    // Catch it in the backoff window (Exited, awaiting relaunch) before the first
    // ~1s backoff elapses; `false` exits in milliseconds so this lands immediately.
    let mut in_backoff = false;
    for _ in 0..40 {
        if matches!(pm.query(id).unwrap().state, ProcState::Exited(_)) {
            in_backoff = true;
            break;
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(in_backoff, "child never reached the backoff window");

    // A stop() landing in that window must be honored: the child must not come
    // back after the backoff (previously supervise re-checked only `shutdown`).
    pm.stop(id).expect("stop");
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let info = pm.query(id).unwrap();
    assert_eq!(
        info.restarts, 0,
        "a stop() during the restart-backoff window must prevent relaunch"
    );
    assert!(matches!(info.state, ProcState::Exited(_)));

    pm.kill_all().await;
}

#[tokio::test]
async fn stop_escalates_to_sigkill() {
    let pm = ProcessManager::new();

    // A child that ignores SIGTERM (shell trap) — only SIGKILL can stop it. `sh` is
    // needed here precisely for the trap; no coreutil ignores SIGTERM.
    let (id, _) = pm
        .spawn(Spec {
            cmd: "sh".into(),
            args: vec!["-c".into(), "trap '' TERM; sleep 60".into()],
            ..Default::default()
        })
        .expect("spawn");

    // Let the trap install, then stop: SIGTERM is ignored, so it must SIGKILL after
    // the deadline (~5s).
    tokio::time::sleep(Duration::from_millis(300)).await;
    pm.stop(id).expect("stop");

    let mut exited = false;
    for _ in 0..80 {
        if !matches!(pm.query(id).unwrap().state, ProcState::Running) {
            exited = true;
            break;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        exited,
        "stop must SIGKILL a SIGTERM-ignoring child after the deadline"
    );
}
