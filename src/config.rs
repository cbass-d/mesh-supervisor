//! Operational policy knobs: timeouts, rate limits, retry behavior, and telemetry
//! cadence. Defaults mirror the hard-coded constants used before this module existed.
//!
//! CLI flags and environment variables are owned by `clap` (with the `env` feature);
//! this module only provides the default values and the `with_cli_overrides` hooks
//! that read parsed `ArgMatches`.

use std::time::Duration;

use clap::ArgMatches;

/// Client-side connection/read behavior. Used by every subcommand that dials a
/// supervisor or joins the telemetry topic as a watcher.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// How long to wait for a response after a stream is open.
    pub read_timeout: Duration,
    /// Maximum connection attempts for a single operation.
    pub max_retries: u32,
    /// Initial delay between retries.
    pub retry_base_delay: Duration,
    /// Maximum delay between retries.
    pub retry_max_delay: Duration,
    /// Telemetry settings (used by `watch` for the freshness window).
    pub telemetry: TelemetryConfig,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            read_timeout: Duration::from_secs(30),
            max_retries: 5,
            retry_base_delay: Duration::from_millis(100),
            retry_max_delay: Duration::from_secs(2),
            telemetry: TelemetryConfig::default(),
        }
    }
}

impl ClientConfig {
    /// Apply subcommand-scoped CLI flags from `matches`.
    pub fn with_cli_overrides(&mut self, matches: &ArgMatches) {
        if let Some(secs) = matches.get_one::<u64>("read-timeout") {
            self.read_timeout = Duration::from_secs(*secs);
        }
        if let Some(n) = matches.get_one::<u32>("connect-retries") {
            self.max_retries = *n;
        }
        if let Some(ms) = matches.get_one::<u64>("retry-base-delay-ms") {
            self.retry_base_delay = Duration::from_millis(*ms);
        }
        if let Some(ms) = matches.get_one::<u64>("retry-max-delay-ms") {
            self.retry_max_delay = Duration::from_millis(*ms);
        }
        self.telemetry.with_cli_overrides(matches);
    }
}

/// Supervisor-side request handling and process shutdown.
#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    /// How long the supervisor waits for a client request after opening a stream.
    pub request_timeout: Duration,
    /// Grace period after SIGTERM before escalating to SIGKILL on stop/shutdown.
    pub stop_deadline: Duration,
    /// Per-peer request rate limiting.
    pub rate_limiter: RateLimiterConfig,
    /// Telemetry publishing and freshness settings.
    pub telemetry: TelemetryConfig,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(30),
            stop_deadline: Duration::from_secs(5),
            rate_limiter: RateLimiterConfig::default(),
            telemetry: TelemetryConfig::default(),
        }
    }
}

impl SupervisorConfig {
    /// Apply subcommand-scoped CLI flags from the `supervise` `ArgMatches`.
    pub fn with_cli_overrides(&mut self, matches: &ArgMatches) {
        if let Some(secs) = matches.get_one::<u64>("request-timeout") {
            self.request_timeout = Duration::from_secs(*secs);
        }
        if let Some(secs) = matches.get_one::<u64>("stop-deadline-secs") {
            self.stop_deadline = Duration::from_secs(*secs);
        }
        self.rate_limiter.with_cli_overrides(matches);
        self.telemetry.with_cli_overrides(matches);
    }
}

/// Token-bucket rate limiter parameters.
#[derive(Debug, Clone)]
pub struct RateLimiterConfig {
    /// Burst capacity.
    pub burst: f64,
    /// Tokens refilled per second.
    pub refill: f64,
    /// Absolute upper bound on tracked peer buckets.
    pub max_buckets: usize,
    /// Idle time after which a bucket can be reclaimed.
    pub eviction_ttl: Duration,
}

impl Default for RateLimiterConfig {
    fn default() -> Self {
        Self {
            burst: 20.0,
            refill: 10.0,
            max_buckets: 2048,
            eviction_ttl: Duration::from_secs(90),
        }
    }
}

impl RateLimiterConfig {
    pub fn with_cli_overrides(&mut self, matches: &ArgMatches) {
        if let Some(n) = matches.get_one::<f64>("rate-burst") {
            self.burst = *n;
        }
        if let Some(n) = matches.get_one::<f64>("rate-refill") {
            self.refill = *n;
        }
        if let Some(n) = matches.get_one::<usize>("rate-max-buckets") {
            self.max_buckets = *n;
        }
        if let Some(secs) = matches.get_one::<u64>("rate-eviction-ttl-secs") {
            self.eviction_ttl = Duration::from_secs(*secs);
        }
    }
}

/// Telemetry publishing cadence and freshness window.
#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    /// Publish cadence for telemetry ticks.
    pub sample_interval: Duration,
    /// Reject ticks whose timestamp is farther than this from now.
    pub max_tick_age: Duration,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            sample_interval: Duration::from_secs(1),
            max_tick_age: Duration::from_millis(10_000),
        }
    }
}

impl TelemetryConfig {
    pub fn with_cli_overrides(&mut self, matches: &ArgMatches) {
        if let Some(ms) = matches.get_one::<u64>("sample-interval-ms") {
            self.sample_interval = Duration::from_millis(*ms);
        }
        if let Some(ms) = matches.get_one::<u64>("max-tick-age-ms") {
            self.max_tick_age = Duration::from_millis(*ms);
        }
    }
}
