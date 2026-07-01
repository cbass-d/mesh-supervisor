//! Client side: dial a supervisor, send one request, return its response.

use std::future::Future;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use iroh::{Endpoint, EndpointAddr};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::proto::{CONTROL_ALPN, Request, Response, read_msg, write_msg};

/// Maximum connection attempts for a single client operation.
const MAX_RETRIES: u32 = 5;
/// Initial delay between retries.
const BASE_DELAY: Duration = Duration::from_millis(100);
/// Maximum delay between retries.
const MAX_DELAY: Duration = Duration::from_secs(2);
/// How long to wait for a response after a stream is open.
const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Retry `f` with exponential backoff until it succeeds or `max_retries` is
/// exhausted. Only transient transport setup errors should be returned as `Err`;
/// application-level failures should be returned as `Ok` and handled by callers.
async fn with_retry<F, Fut, T>(
    max_retries: u32,
    base_delay: Duration,
    max_delay: Duration,
    mut f: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut delay = base_delay;
    for attempt in 1..=max_retries {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if attempt == max_retries {
                    return Err(e);
                }

                tracing::debug!(attempt, error = %e, "client transport setup failed, retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(max_delay);
            }
        }
    }

    unreachable!()
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
) -> Result<Response> {
    let target = target.into();
    let (conn, mut send, mut recv) = with_retry(MAX_RETRIES, BASE_DELAY, MAX_DELAY, || async {
        let conn = endpoint.connect(target.clone(), CONTROL_ALPN).await?;
        let (send, recv) = conn.open_bi().await?;
        Ok((conn, send, recv))
    })
    .await?;

    write_msg(&mut send, &req).await?;
    let resp = tokio::time::timeout(READ_TIMEOUT, read_msg::<Response>(&mut recv))
        .await
        .context("timed out waiting for response")??;

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
) -> Result<()> {
    let target = target.into();
    let (conn, mut send, mut recv) = with_retry(MAX_RETRIES, BASE_DELAY, MAX_DELAY, || async {
        let conn = endpoint.connect(target.clone(), CONTROL_ALPN).await?;
        let (send, recv) = conn.open_bi().await?;
        Ok((conn, send, recv))
    })
    .await?;

    write_msg(&mut send, &Request::Subscribe { id }).await?;
    match tokio::time::timeout(READ_TIMEOUT, read_msg::<Response>(&mut recv))
        .await
        .context("timed out waiting for subscribe ack")??
    {
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
) -> Result<()> {
    let target = target.into();
    let (conn, mut send, mut recv) = with_retry(MAX_RETRIES, BASE_DELAY, MAX_DELAY, || async {
        let conn = endpoint.connect(target.clone(), CONTROL_ALPN).await?;
        let (send, recv) = conn.open_bi().await?;
        Ok((conn, send, recv))
    })
    .await?;

    write_msg(&mut send, &Request::StdinStream { id }).await?;
    tokio::io::copy(input, &mut send).await?;
    send.finish()?;

    match tokio::time::timeout(READ_TIMEOUT, read_msg::<Response>(&mut recv))
        .await
        .context("timed out waiting for stdin response")??
    {
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

    use super::*;

    #[tokio::test]
    async fn retry_succeeds_after_failures() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();
        let result = with_retry(
            5,
            Duration::from_millis(1),
            Duration::from_millis(10),
            move || {
                let attempts = attempts_clone.clone();
                async move {
                    let n = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    if n < 3 {
                        Err(anyhow::anyhow!("transient"))
                    } else {
                        Ok(42)
                    }
                }
            },
        )
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_gives_up_after_max_attempts() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();
        let result = with_retry(
            3,
            Duration::from_millis(1),
            Duration::from_millis(10),
            move || {
                let attempts = attempts_clone.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err::<(), _>(anyhow::anyhow!("always fails"))
                }
            },
        )
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }
}
