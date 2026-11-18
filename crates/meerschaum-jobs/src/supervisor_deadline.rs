//! Host-selected immutable compute lifetime for credential-free discovery only.
//!
//! The daemon obtains this typed value from its trusted launch resolver. Neither a child
//! environment variable, persisted job property, callback refresh nor output selects it.

use serde::{Deserialize, Serialize};
use std::io;
use std::time::{Duration, Instant};

/// Absolute Main-owned deadline. A supervisor pins it once to monotonic time before fork.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComputeDeadline {
    /// Unix seconds captured by the trusted Main-created metadata operation.
    pub expires_at: i64,
}

impl ComputeDeadline {
    /// Reject an expired or unbounded launch instead of starting code without a lifetime.
    pub fn pin(self) -> io::Result<Instant> {
        self.pin_at(chrono::Utc::now().timestamp(), Instant::now())
    }

    fn pin_at(self, utc: i64, monotonic: Instant) -> io::Result<Instant> {
        let remaining = self
            .expires_at
            .checked_sub(utc)
            .filter(|remaining| *remaining > 0 && *remaining <= 60)
            .ok_or_else(|| {
                io::Error::other("The metadata compute deadline is expired or unsupported.")
            })?;
        monotonic
            .checked_add(Duration::from_secs(remaining as u64))
            .ok_or_else(|| io::Error::other("The metadata compute deadline is unsupported."))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn immutable_deadline_is_pinned_once_and_never_renewed_or_selected_by_extra_fields() {
        let began = Instant::now();
        let deadline = ComputeDeadline { expires_at: 160 };
        let pinned = deadline.pin_at(100, began).unwrap();
        assert_eq!(pinned.duration_since(began), Duration::from_secs(60));
        // Wall-clock rollback has no effect on the already-pinned supervisor deadline.
        assert!(deadline.pin_at(99, began).is_err());
        assert_eq!(pinned.duration_since(began), Duration::from_secs(60));
        assert!(deadline.pin_at(160, began).is_err());
        assert!(
            ComputeDeadline {
                expires_at: i64::MAX
            }
            .pin_at(-1, began)
            .is_err()
        );
        assert!(
            serde_json::from_str::<ComputeDeadline>(r#"{"expires_at":160,"renew":true}"#).is_err()
        );
    }
}
