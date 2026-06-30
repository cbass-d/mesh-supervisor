//! Client side: dial a supervisor, send one request, return its response.

use anyhow::{Result, bail};
use iroh::{Endpoint, EndpointAddr};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::proto::{CONTROL_ALPN, Request, Response, read_msg, write_msg};

/// Dial `target`, send `req`, and return the supervisor's response.
///
/// `target` accepts a bare `EndpointId` (resolved via mDNS) or a full `EndpointAddr`.
pub async fn request(
    endpoint: &Endpoint,
    target: impl Into<EndpointAddr>,
    req: Request,
) -> Result<Response> {
    let conn = endpoint.connect(target, CONTROL_ALPN).await?;
    let (mut send, mut recv) = conn.open_bi().await?;

    write_msg(&mut send, &req).await?;
    let resp = read_msg::<Response>(&mut recv).await?;

    conn.close(0u32.into(), b"done");

    Ok(resp)
}

/// Subscribe to a process's stdout: read the `Ack`, then copy the live stream to `out`
/// until the supervisor closes it (the process exited).
pub async fn subscribe(
    endpoint: &Endpoint,
    target: impl Into<EndpointAddr>,
    id: u64,
    out: &mut (impl AsyncWrite + Unpin),
) -> Result<()> {
    let conn = endpoint.connect(target, CONTROL_ALPN).await?;
    let (mut send, mut recv) = conn.open_bi().await?;

    write_msg(&mut send, &Request::Subscribe { id }).await?;
    match read_msg::<Response>(&mut recv).await? {
        Response::Ack => {}
        Response::Error(e) => bail!("subscribe rejected: {e:?}"),
        other => bail!("unexpected response: {other:?}"),
    }

    tokio::io::copy(&mut recv, out).await?;
    out.flush().await?;
    conn.close(0u32.into(), b"done");

    Ok(())
}
