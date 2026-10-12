//! `SuccessTuple` (the universal `(bool, str)` return type) and `SyncResult` (§5.7).

use std::fmt;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// `SuccessTuple`: `(success, message)`. Serializes as a two-element JSON array, the way a
/// Python tuple is serialized.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(from = "(bool, String)", into = "(bool, String)")]
pub struct SuccessTuple {
    /// Whether the operation succeeded.
    pub success: bool,
    /// A human-readable message (`"Success"` by convention).
    pub message: String,
}

impl SuccessTuple {
    /// `(True, message)`.
    pub fn ok(message: impl Into<String>) -> SuccessTuple {
        SuccessTuple {
            success: true,
            message: message.into(),
        }
    }

    /// `(True, "Success")`.
    pub fn success() -> SuccessTuple {
        SuccessTuple::ok("Success")
    }

    /// `(False, message)`.
    pub fn fail(message: impl Into<String>) -> SuccessTuple {
        SuccessTuple {
            success: false,
            message: message.into(),
        }
    }

    /// The tuple form.
    pub fn as_tuple(&self) -> (bool, &str) {
        (self.success, &self.message)
    }

    /// Whether the message would be suppressed after an action: `print_tuple` skips messages
    /// whose `.strip()` is in `STATIC_CONFIG['system']['success']['ignore']`.
    pub fn is_ignored_message(&self) -> bool {
        crate::static_config::static_config()
            .system
            .success
            .ignore
            .iter()
            .any(|m| m.as_deref() == Some(crate::pyfmt::py_strip(&self.message)))
    }
}

impl From<(bool, String)> for SuccessTuple {
    fn from((success, message): (bool, String)) -> Self {
        SuccessTuple { success, message }
    }
}

impl From<(bool, &str)> for SuccessTuple {
    fn from((success, message): (bool, &str)) -> Self {
        SuccessTuple {
            success,
            message: message.to_string(),
        }
    }
}

impl From<SuccessTuple> for (bool, String) {
    fn from(t: SuccessTuple) -> Self {
        (t.success, t.message)
    }
}

impl<E: fmt::Display> From<Result<String, E>> for SuccessTuple {
    fn from(r: Result<String, E>) -> Self {
        match r {
            Ok(m) => SuccessTuple::ok(m),
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }
}

impl fmt::Display for SuccessTuple {
    /// `str((True, 'Success'))`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "({}, {})",
            if self.success { "True" } else { "False" },
            crate::pyfmt::str_repr(&self.message)
        )
    }
}

/// A sync bound on a pipe's datetime axis: a timestamp or an integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Bound {
    /// A timezone-aware timestamp.
    Datetime(DateTime<Utc>),
    /// An integer axis value.
    Int(i64),
}

/// A half-open interval `[begin, end)` of changed data; `None` is unbounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChangedInterval {
    /// Inclusive lower bound.
    pub begin: Option<Bound>,
    /// Exclusive upper bound.
    pub end: Option<Bound>,
}

/// The structured result of a sync (§5.7). It converts to a [`SuccessTuple`] and serializes
/// its tuple-compatible fields first.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncResult {
    /// Whether the sync succeeded.
    pub success: bool,
    /// The message.
    pub message: String,
    /// Rows inserted.
    pub inserted: u64,
    /// Rows updated.
    pub updated: u64,
    /// Rows upserted.
    pub upserted: u64,
    /// Rows deleted.
    pub deleted: u64,
    /// The intervals of the datetime axis that changed.
    pub changed_intervals: Vec<ChangedInterval>,
    /// Wall-clock duration of the sync.
    pub duration: Option<Duration>,
    /// The sync's lower bound.
    pub begin: Option<Bound>,
    /// The sync's upper bound.
    pub end: Option<Bound>,
}

impl SyncResult {
    /// A result with no row counts.
    pub fn new(success: bool, message: impl Into<String>) -> SyncResult {
        SyncResult {
            success,
            message: message.into(),
            inserted: 0,
            updated: 0,
            upserted: 0,
            deleted: 0,
            changed_intervals: Vec::new(),
            duration: None,
            begin: None,
            end: None,
        }
    }

    /// The `(success, message)` view.
    pub fn as_success_tuple(&self) -> SuccessTuple {
        SuccessTuple {
            success: self.success,
            message: self.message.clone(),
        }
    }

    /// Total rows written (inserted + updated + upserted).
    pub fn rows_written(&self) -> u64 {
        self.inserted + self.updated + self.upserted
    }
}

impl From<SyncResult> for SuccessTuple {
    fn from(r: SyncResult) -> Self {
        r.as_success_tuple()
    }
}

impl From<SuccessTuple> for SyncResult {
    fn from(t: SuccessTuple) -> Self {
        SyncResult::new(t.success, t.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tuple_json() {
        let t = SuccessTuple::success();
        assert_eq!(serde_json::to_string(&t).unwrap(), r#"[true,"Success"]"#);
        let back: SuccessTuple = serde_json::from_str(r#"[false,"nope"]"#).unwrap();
        assert_eq!(back, SuccessTuple::fail("nope"));
        assert_eq!(t.to_string(), "(True, 'Success')");
        assert!(t.is_ignored_message());
        assert!(SuccessTuple::ok(" Success\n").is_ignored_message());
        assert!(!SuccessTuple::ok("Inserted 1").is_ignored_message());
        let r = SyncResult::new(true, "Inserted 1");
        assert_eq!(SuccessTuple::from(r).message, "Inserted 1");
    }
}
