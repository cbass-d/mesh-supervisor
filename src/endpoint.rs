//! Builds the iroh endpoint shared by both roles.

use anyhow::Result;
use iroh::{
    Endpoint, RelayMode, RelayUrl, SecretKey, address_lookup::memory::MemoryLookup,
    endpoint::presets,
};
use iroh_mdns_address_lookup::MdnsAddressLookup;

/// Bind a node endpoint.
///
/// `alpns`: control/gossip ALPNs for a supervisor, empty for a dial-only client.
/// `secret_key`: `Some` for a stable persisted identity, `None` for ephemeral.
/// `relay`: a self-hosted relay for WAN reachability (no address publishing);
/// `None` = LAN-only via mDNS.
///
/// Returns a `MemoryLookup` the caller may seed with `(id → relay)` addrs so
/// gossip can reach bootstrap peers by id without a discovery service.
pub async fn build_endpoint(
    alpns: Vec<Vec<u8>>,
    secret_key: Option<SecretKey>,
    relay: Option<RelayUrl>,
) -> Result<(Endpoint, MemoryLookup)> {
    let book = MemoryLookup::new();
    let mut builder = Endpoint::builder(presets::Minimal)
        .alpns(alpns)
        .address_lookup(MdnsAddressLookup::builder())
        .address_lookup(book.clone());

    if let Some(key) = secret_key {
        builder = builder.secret_key(key);
    }
    if let Some(url) = relay {
        builder = builder.relay_mode(RelayMode::custom([url]));
    }

    Ok((builder.bind().await?, book))
}
