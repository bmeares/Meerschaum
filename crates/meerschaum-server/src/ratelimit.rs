//! Per-route-group rate limits (`api:permissions:rate_limits`, new in v5, §7.1): a token bucket
//! per `(route group, client)` that refills `requests` tokens every `seconds`.
//!
//! ```yaml
//! api:
//!   permissions:
//!     rate_limits:
//!       login: {requests: 10, seconds: 60}
//!       '*': {requests: 1000, seconds: 1}
//! ```
//!
//! A limited request gets `429 {"detail": "Rate limit exceeded."}` with `Retry-After`.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::config::{RateLimit, ServerConfig};

#[derive(Debug, Clone, Copy)]
struct Bucket {
    tokens: f64,
    last: Instant,
}

/// The rate limiter shared by all requests.
#[derive(Debug, Default)]
pub struct RateLimiter {
    buckets: Mutex<HashMap<(String, String), Bucket>>,
}

impl RateLimiter {
    /// A limiter for a configuration.
    pub fn new(_cfg: &ServerConfig) -> RateLimiter {
        RateLimiter::default()
    }

    /// Take one token for `client` in `group`; `Err(retry_after)` when the bucket is empty.
    pub fn check(&self, group: &str, client: &str, limit: RateLimit) -> Result<(), Duration> {
        let now = Instant::now();
        let capacity = f64::from(limit.requests);
        let rate = capacity / limit.per.as_secs_f64().max(f64::EPSILON);
        let mut buckets = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        if buckets.len() > 100_000 {
            // Drop full buckets so idle clients do not accumulate.
            buckets.retain(|_, b| {
                b.tokens + now.duration_since(b.last).as_secs_f64() * rate < capacity
            });
        }
        let b = buckets
            .entry((group.to_string(), client.to_string()))
            .or_insert(Bucket {
                tokens: capacity,
                last: now,
            });
        let elapsed = now.duration_since(b.last).as_secs_f64();
        b.tokens = (b.tokens + elapsed * rate).min(capacity);
        b.last = now;
        if b.tokens >= 1.0 {
            b.tokens -= 1.0;
            Ok(())
        } else {
            Err(Duration::from_secs_f64((1.0 - b.tokens) / rate))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_per_client() {
        let l = RateLimiter::default();
        let limit = RateLimit {
            requests: 2,
            per: Duration::from_secs(60),
        };
        assert!(l.check("login", "a", limit).is_ok());
        assert!(l.check("login", "a", limit).is_ok());
        assert!(l.check("login", "a", limit).is_err());
        assert!(l.check("login", "b", limit).is_ok());
        assert!(l.check("pipes", "a", limit).is_ok());
    }
}
