//! Client side: dial a supervisor, send one request, return its response.

use std::future::Future;

use anyhow::{Context, Result, bail};
use iroh::{
    Endpoint, EndpointAddr,
    endpoint::{Connection, RecvStream, SendStream},
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::config::ClientConfig;
use crate::proto::{CONTROL_ALPN, Request, Response, read_msg, write_msg};

/// Retry `f` with exponential backoff until it succeeds or `max_retries` is
/// exhausted. Only transient transport setup errors should be returned as `Err`;
/// application-level failures should be returned as `Ok` and handled by callers.
async fn with_retry<F, Fut, T>(cfg: &ClientConfig, mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    // Always make at least one attempt: `max_retries` of 0 is accepted CLI/env
    // input, and `1..=0` would be empty and fall through to `unreachable!()`.
    let attempts = cfg.max_retries.max(1);
    let mut delay = cfg.retry_base_delay;
    for attempt in 1..=attempts {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if attempt == attempts {
                    return Err(e);
                }

                tracing::debug!(attempt, error = %e, "client transport setup failed, retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(cfg.retry_max_delay);
            }
        }
    }

    unreachable!()
}

/// Dial `target` on the control ALPN and open a bi-directional stream,
/// retrying transport setup with exponential backoff.
async fn connect_bi(
    endpoint: &Endpoint,
    target: &EndpointAddr,
    cfg: &ClientConfig,
) -> Result<(Connection, SendStream, RecvStream)> {
    with_retry(cfg, || async {
        let conn = endpoint.connect(target.clone(), CONTROL_ALPN).await?;
        let (send, recv) = conn.open_bi().await?;
        Ok((conn, send, recv))
    })
    .await
}

/// Read one framed `Response` within the configured timeout; `what` names the
/// awaited reply in the timeout error.
async fn read_response(recv: &mut RecvStream, cfg: &ClientConfig, what: &str) -> Result<Response> {
    tokio::time::timeout(cfg.read_timeout, read_msg::<Response>(recv))
        .await
        .with_context(|| format!("timed out waiting for {what}"))?
}

/// Dial `target`, send `req`, and return the supervisor's response.
///
/// `target` accepts a bare `EndpointId` (resolved via mDNS) or a full `EndpointAddr`.
/// Connection establishment is retried with exponential backoff; once a stream is
/// open the request is sent exactly once.
pub async fn request(
    endpoint: &Endpoint,
    target: impl Into<EndpointAddr>,
    req: Request,
    cfg: &ClientConfig,
) -> Result<Response> {
    let target = target.into();
    let (conn, mut send, mut recv) = connect_bi(endpoint, &target, cfg).await?;

    write_msg(&mut send, &req).await?;
    let resp = read_response(&mut recv, cfg, "response").await?;

    conn.close(0u32.into(), b"done");

    Ok(resp)
}

/// Subscribe to a process's stdout: read the `Ack`, then copy the live stream to `out`
/// until the supervisor closes it (the process exited).
///
/// Connection establishment is retried with exponential backoff; once the
/// subscription handshake begins it is not retried (application-level rejections
/// such as `NotFound` or `Denied` are final).
pub async fn subscribe(
    endpoint: &Endpoint,
    target: impl Into<EndpointAddr>,
    id: u64,
    out: &mut (impl AsyncWrite + Unpin),
    cfg: &ClientConfig,
) -> Result<()> {
    let target = target.into();
    let (conn, mut send, mut recv) = connect_bi(endpoint, &target, cfg).await?;

    write_msg(&mut send, &Request::Subscribe { id }).await?;
    match read_response(&mut recv, cfg, "subscribe ack").await? {
        Response::Ack => {}
        Response::Error(e) => bail!("subscribe rejected: {e:?}"),
        other => bail!("unexpected response: {other:?}"),
    }

    tokio::io::copy(&mut recv, out).await?;
    out.flush().await?;
    conn.close(0u32.into(), b"done");

    Ok(())
}

/// Pipe `input` into a process's stdin over a raw QUIC byte stream.
///
/// Connection establishment is retried with exponential backoff. The stream is
/// closed (EOF) when `input` reaches EOF. The supervisor's final `Ack` or `Error`
/// response is read before returning.
pub async fn stdin_stream(
    endpoint: &Endpoint,
    target: impl Into<EndpointAddr>,
    id: u64,
    input: &mut (impl AsyncRead + Unpin),
    cfg: &ClientConfig,
) -> Result<()> {
    let target = target.into();
    let (conn, mut send, mut recv) = connect_bi(endpoint, &target, cfg).await?;

    write_msg(&mut send, &Request::StdinStream { id }).await?;
    tokio::io::copy(input, &mut send).await?;
    send.finish()?;

    match read_response(&mut recv, cfg, "stdin response").await? {
        Response::Ack => {}
        Response::Error(e) => bail!("stdin stream rejected: {e:?}"),
        other => bail!("unexpected response: {other:?}"),
    }

    conn.close(0u32.into(), b"done");

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    fn test_client_config() -> ClientConfig {
        ClientConfig {
            read_timeout: Duration::from_secs(30),
            max_retries: 5,
            retry_base_delay: Duration::from_millis(1),
            retry_max_delay: Duration::from_millis(10),
            telemetry: Default::default(),
        }
    }

    #[tokio::test]
    async fn retry_succeeds_after_failures() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();
        let cfg = test_client_config();
        let result = with_retry(&cfg, move || {
            let attempts = attempts_clone.clone();
            async move {
                let n = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if n < 3 {
                    Err(anyhow::anyhow!("transient"))
                } else {
                    Ok(42)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_gives_up_after_max_attempts() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();
        let cfg = test_client_config();
        let result = with_retry(&cfg, move || {
            let attempts = attempts_clone.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(anyhow::anyhow!("always fails"))
            }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), cfg.max_retries as usize);
    }

    #[tokio::test]
    async fn retry_with_zero_max_retries_makes_one_attempt() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();
        let mut cfg = test_client_config();
        cfg.max_retries = 0; // accepted CLI/env value; must not reach unreachable!()
        let result = with_retry(&cfg, move || {
            let attempts = attempts_clone.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(anyhow::anyhow!("always fails"))
            }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "max_retries=0 must make exactly one attempt, not panic"
        );
    }
}
