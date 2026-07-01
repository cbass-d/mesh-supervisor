//! End-to-end control surface over loopback: spawn, list, stdin, query, signal.

use std::time::Duration;

use iroh::{
    Endpoint, SecretKey, address_lookup::memory::MemoryLookup, endpoint::presets, protocol::Router,
};
use mesh_supervisor::{
    client,
    process::ProcessManager,
    proto::{CONTROL_ALPN, ControlError, ProcInfo, ProcState, Request, Response, Spec},
    supervisor::{Authz, Supervisor},
    telemetry,
};
use n0_future::StreamExt;

/// Bind a LAN-less endpoint (no mDNS)
async fn test_endpoint(alpns: Vec<Vec<u8>>) -> Endpoint {
    Endpoint::builder(presets::Minimal)
        .alpns(alpns)
        .bind()
        .await
        .expect("bind failed")
}

/// Like `test_endpoint`, but with a static address book so peers resolve each
/// other by `EndpointId` without mDNS (gossip bootstrap takes ids, not addrs).
async fn test_endpoint_static(alpns: Vec<Vec<u8>>, book: MemoryLookup) -> Endpoint {
    Endpoint::builder(presets::Minimal)
        .alpns(alpns)
        .address_lookup(book)
        .bind()
        .await
        .expect("bind failed")
}

#[tokio::test]
async fn spawn_then_list_over_loopback() {
    let server = test_endpoint(vec![CONTROL_ALPN.to_vec()]).await;
    let router = Router::builder(server.clone())
        .accept(
            CONTROL_ALPN,
            Supervisor::new(ProcessManager::new(), Authz::open()),
        )
        .spawn();

    // Minimal preset: direct (LAN) addresses are ready right after bind.
    let addr = server.addr();
    let cl = test_endpoint(vec![]).await;

    // Empty to start.
    let resp = client::request(&cl, addr.clone(), Request::List)
        .await
        .expect("list failed");
    assert_eq!(resp, Response::List(vec![]));

    // Spawn one child.
    let req = Request::Spawn(Spec {
        cmd: "sleep".into(),
        args: vec!["30".into()],
        ..Default::default()
    });
    let id = match client::request(&cl, addr.clone(), req)
        .await
        .expect("spawn failed")
    {
        Response::Spawned { id, pid } => {
            assert!(pid > 0);
            id
        }
        other => panic!("expected Spawned, got {other:?}"),
    };

    // Now the supervisor tracks it, with state.
    let resp = client::request(&cl, addr.clone(), Request::List)
        .await
        .expect("list failed");
    match resp {
        Response::List(procs) => {
            assert_eq!(procs.len(), 1);
            assert_eq!(procs[0].handle, id);
            assert_eq!(procs[0].state, ProcState::Running);
        }
        other => panic!("expected List, got {other:?}"),
    }

    // Query: running.
    let resp = client::request(&cl, addr.clone(), Request::Query { id })
        .await
        .expect("query failed");
    assert!(matches!(
        resp,
        Response::Status(ProcInfo {
            state: ProcState::Running,
            ..
        })
    ));

    // Stdin: write a frame (sleep ignores it, but the pipe accepts the bytes).
    let req = Request::Stdin {
        id,
        data: b"hi\n".to_vec(),
    };
    assert_eq!(
        client::request(&cl, addr.clone(), req)
            .await
            .expect("stdin"),
        Response::Ack
    );

    // Signal SIGTERM, then poll until the reaper records the exit.
    let req = Request::Signal { id, sig: 15 };
    assert_eq!(
        client::request(&cl, addr.clone(), req)
            .await
            .expect("signal"),
        Response::Ack
    );

    let mut exited = false;
    for _ in 0..50 {
        let resp = client::request(&cl, addr.clone(), Request::Query { id })
            .await
            .expect("query failed");
        if matches!(
            resp,
            Response::Status(ProcInfo {
                state: ProcState::Exited(_),
                ..
            })
        ) {
            exited = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(exited, "process did not exit after SIGTERM");

    cl.close().await;
    router.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn control_denied_when_not_on_allowlist() {
    // Allowlist holds some other id, so our client is rejected.
    let stranger = SecretKey::generate().public();
    let authz = Authz {
        control: [stranger].into_iter().collect(),
        ..Default::default()
    };

    let server = test_endpoint(vec![CONTROL_ALPN.to_vec()]).await;
    let router = Router::builder(server.clone())
        .accept(CONTROL_ALPN, Supervisor::new(ProcessManager::new(), authz))
        .spawn();
    let addr = server.addr();
    let cl = test_endpoint(vec![]).await;

    let resp = client::request(&cl, addr, Request::List)
        .await
        .expect("request");
    assert!(matches!(resp, Response::Error(ControlError::Denied)));

    cl.close().await;
    router.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn read_only_client_can_inspect_not_control() {
    let cl = test_endpoint(vec![]).await;
    // The client is granted read-only rights only.
    let authz = Authz {
        read: [cl.id()].into_iter().collect(),
        ..Default::default()
    };

    let server = test_endpoint(vec![CONTROL_ALPN.to_vec()]).await;
    let router = Router::builder(server.clone())
        .accept(CONTROL_ALPN, Supervisor::new(ProcessManager::new(), authz))
        .spawn();
    let addr = server.addr();

    // A read-only op is allowed.
    let resp = client::request(&cl, addr.clone(), Request::List)
        .await
        .expect("list");
    assert!(matches!(resp, Response::List(_)));

    // A control op is denied.
    let spawn = Request::Spawn(Spec {
        cmd: "true".into(),
        ..Default::default()
    });
    let resp = client::request(&cl, addr, spawn).await.expect("spawn req");
    assert!(matches!(resp, Response::Error(ControlError::Denied)));

    cl.close().await;
    router.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn telemetry_tick_reaches_watcher_over_gossip() {
    use iroh_gossip::net::Gossip;

    // Two static address books, cross-registered after bind so the watcher can
    // resolve the supervisor's EndpointId (no mDNS in tests).
    let sup_book = MemoryLookup::new();
    let watch_book = MemoryLookup::new();

    let server = test_endpoint_static(vec![iroh_gossip::ALPN.to_vec()], sup_book.clone()).await;
    let watcher = test_endpoint_static(vec![iroh_gossip::ALPN.to_vec()], watch_book.clone()).await;
    sup_book.add_endpoint_info(watcher.addr());
    watch_book.add_endpoint_info(server.addr());

    let server_id = server.id();

    // Supervisor side: gossip + one running process to report on.
    let sup_gossip = Gossip::builder().spawn(server.clone());
    let supervisor = Supervisor::new(ProcessManager::new(), Authz::open());
    let _sup_router = Router::builder(server.clone())
        .accept(iroh_gossip::ALPN, sup_gossip.clone())
        .spawn();
    let (id, _pid) = supervisor
        .procs()
        .spawn(Spec {
            cmd: "sleep".into(),
            args: vec!["30".into()],
            ..Default::default()
        })
        .expect("spawn");

    let sup_topic = sup_gossip
        .subscribe(telemetry::topic_for(None), vec![])
        .await
        .expect("supervisor subscribe");
    tokio::spawn(telemetry::publish_loop(
        sup_topic,
        server.secret_key().clone(),
        supervisor.procs(),
    ));

    // Watcher side: join the topic, bootstrapping off the supervisor.
    let watch_gossip = Gossip::builder().spawn(watcher.clone());
    let _watch_router = Router::builder(watcher.clone())
        .accept(iroh_gossip::ALPN, watch_gossip.clone())
        .spawn();
    let mut topic = watch_gossip
        .subscribe(telemetry::topic_for(None), vec![server_id])
        .await
        .expect("watcher subscribe");

    // Wait for the first tick (publisher samples once/sec); fail fast if none.
    let tick = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match topic.next().await {
                Some(Ok(iroh_gossip::api::Event::Received(msg))) => {
                    break telemetry::open_tick(&msg.content).expect("verify tick");
                }
                Some(Ok(_)) => continue, // NeighborUp/Down, Lagged
                Some(Err(e)) => panic!("gossip stream error: {e}"),
                None => panic!("gossip stream ended before any tick"),
            }
        }
    })
    .await
    .expect("no telemetry tick within timeout");

    assert_eq!(tick.from, server_id);
    assert!(tick.seq > 0, "tick must carry a positive sequence number");
    assert_eq!(tick.stats.len(), 1);
    assert_eq!(tick.stats[0].handle, id);
    assert_eq!(tick.stats[0].state, ProcState::Running);

    supervisor.procs().kill_all().await;
    server.close().await;
    watcher.close().await;
}

#[tokio::test]
async fn subscribe_streams_stdout_to_two_clients() {
    let server = test_endpoint(vec![CONTROL_ALPN.to_vec()]).await;
    let router = Router::builder(server.clone())
        .accept(
            CONTROL_ALPN,
            Supervisor::new(ProcessManager::new(), Authz::open()),
        )
        .spawn();
    let addr = server.addr();
    let cl = test_endpoint(vec![]).await;

    // Sleep first so both subscribers attach before any output is produced.
    let req = Request::Spawn(Spec {
        cmd: "sh".into(),
        args: vec!["-c".into(), "sleep 0.4; printf 'hello\\nworld\\n'".into()],
        ..Default::default()
    });
    let id = match client::request(&cl, addr.clone(), req)
        .await
        .expect("spawn")
    {
        Response::Spawned { id, .. } => id,
        other => panic!("expected Spawned, got {other:?}"),
    };

    // Two concurrent subscribers; both should receive the full stdout.
    let mut buf_a = Vec::new();
    let mut buf_b = Vec::new();
    let (ra, rb) = tokio::join!(
        client::subscribe(&cl, addr.clone(), id, &mut buf_a),
        client::subscribe(&cl, addr.clone(), id, &mut buf_b),
    );
    ra.expect("subscribe a");
    rb.expect("subscribe b");

    assert_eq!(buf_a, b"hello\nworld\n");
    assert_eq!(buf_b, b"hello\nworld\n");

    cl.close().await;
    router.shutdown().await.expect("shutdown");
}
