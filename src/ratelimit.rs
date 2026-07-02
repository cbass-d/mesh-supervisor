//! Per-peer request rate limiting: a token bucket keyed by `EndpointId`. Each
//! control request is its own connection, so limiting must be per-peer, not
//! per-connection.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use iroh::EndpointId;
use parking_lot::Mutex;

use crate::config::RateLimiterConfig;

/// One token-bucket entry.
#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last_used: Instant,
}

/// Per-peer request rate limiter.
#[derive(Debug)]
pub(crate) struct RateLimiter {
    cfg: RateLimiterConfig,
    buckets: Mutex<HashMap<EndpointId, Bucket>>,
}

impl RateLimiter {
    pub(crate) fn new(cfg: RateLimiterConfig) -> Self {
        Self {
            cfg,
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// Consume one token for `peer`; `false` if it's currently rate-limited.
    pub(crate) fn allow(&self, peer: EndpointId) -> bool {
        let now = Instant::now();
        let cfg = &self.cfg;

        // Fast path: update the bucket and decide under a short critical section.
        // We return whether the map is over capacity so cleanup can run afterwards
        // without holding the lock during the O(max_buckets) eviction scan.
        let (allowed, over_cap) = {
            let mut buckets = self.buckets.lock();
            let bucket = buckets.entry(peer).or_insert_with(|| Bucket {
                tokens: cfg.burst,
                last_used: now,
            });
            bucket.tokens = (bucket.tokens
                + now.duration_since(bucket.last_used).as_secs_f64() * cfg.refill)
                .min(cfg.burst);

            bucket.last_used = now;
            let allowed = if bucket.tokens >= 1.0 {
                bucket.tokens -= 1.0;
                true
            } else {
                false
            };

            (allowed, buckets.len() > cfg.max_buckets)
        };

        // Slow path: eviction is best-effort cleanup, so it runs under a separate
        // lock acquisition and cannot delay other rate-limit decisions.
        if over_cap {
            let mut buckets = self.buckets.lock();
            if buckets.len() > cfg.max_buckets {
                Self::evict_lru(&mut buckets);
            }
            Self::evict_stale(&mut buckets, now, cfg.eviction_ttl);
        }

        allowed
    }

    /// Remove the least-recently-used bucket. `O(max_buckets)`; the cap keeps it small.
    fn evict_lru(buckets: &mut HashMap<EndpointId, Bucket>) {
        let oldest = buckets
            .iter()
            .min_by_key(|(_, b)| b.last_used)
            .map(|(id, _)| *id);

        if let Some(id) = oldest {
            buckets.remove(&id);
        }
    }

    /// Remove buckets that haven't been used since `now - eviction_ttl`.
    fn evict_stale(
        buckets: &mut HashMap<EndpointId, Bucket>,
        now: Instant,
        eviction_ttl: Duration,
    ) {
        let cutoff = now - eviction_ttl;
        buckets.retain(|_, b| b.last_used >= cutoff);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::SecretKey;

    fn test_rate_cfg() -> RateLimiterConfig {
        RateLimiterConfig::default()
    }

    #[test]
    fn rate_limiter_allows_burst_then_blocks() {
        let cfg = test_rate_cfg();
        let limiter = RateLimiter::new(cfg.clone());
        let peer = SecretKey::generate().public();

        // A full burst is admitted back-to-back (refill over microseconds is ~0)...
        for _ in 0..cfg.burst as u32 {
            assert!(limiter.allow(peer));
        }
        // ...then the bucket is empty.
        assert!(!limiter.allow(peer), "burst exhausted should be limited");

        // A different peer has its own bucket, unaffected.
        assert!(limiter.allow(SecretKey::generate().public()));
    }

    #[test]
    fn rate_limiter_evicts_lru_when_full() {
        let cfg = test_rate_cfg();
        let limiter = RateLimiter::new(cfg.clone());
        let peers: Vec<_> = (0..cfg.max_buckets + 1)
            .map(|_| SecretKey::generate().public())
            .collect();

        // Touch each peer once in order, sleeping a tiny bit so last_used differs.
        for peer in &peers {
            assert!(limiter.allow(*peer));
            std::thread::sleep(Duration::from_millis(1));
        }

        assert_eq!(
            limiter.buckets.lock().len(),
            cfg.max_buckets,
            "bucket count should be capped"
        );

        // The first peer (oldest last_used) should have been evicted.
        assert!(
            !limiter.buckets.lock().contains_key(&peers[0]),
            "oldest peer should be evicted"
        );

        // Re-allowing the evicted peer creates a new bucket; count stays capped.
        assert!(limiter.allow(peers[0]));
        assert_eq!(limiter.buckets.lock().len(), cfg.max_buckets);
    }

    #[test]
    fn evict_stale_removes_idle_peers() {
        let cfg = test_rate_cfg();
        let now = Instant::now();
        let old = SecretKey::generate().public();
        let recent = SecretKey::generate().public();
        let mut buckets = HashMap::new();
        buckets.insert(
            old,
            Bucket {
                tokens: 0.0,
                last_used: now - cfg.eviction_ttl - Duration::from_secs(1),
            },
        );
        buckets.insert(
            recent,
            Bucket {
                tokens: 0.0,
                last_used: now,
            },
        );

        RateLimiter::evict_stale(&mut buckets, now, cfg.eviction_ttl);

        assert!(!buckets.contains_key(&old));
        assert!(buckets.contains_key(&recent));
    }
}
