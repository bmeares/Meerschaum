//! Instance-wide policies every instance connector applies the same way:
//!
//! - **Protected connectors** (`protected: true`): [`protected_refusal`] is the check each write
//!   path makes before it mutates anything. Meerschaum's own bookkeeping pipes (connector keys
//!   `mrsm`: users, plugins, tokens, saved Pipelines, run history, sync status) are not user
//!   data and are never refused, so logins, run records and status records keep working on a
//!   protected instance.
//! - **Hidden internal pipes**: the `mrsm` pipes (saved Pipelines' registry pipes
//!   `Pipe('mrsm', 'pipeline', <name>)` and every other internal pipe) are left out of
//!   `fetch_pipes_keys` results unless the query names `mrsm` in its connector keys
//!   ([`hides_internal_pipes`], [`hide_internal_pipes`]), so listings and bulk actions (`show
//!   pipes`, `sync pipes`, `clear pipes`, `GET /pipes/keys`, the console) never touch them,
//!   while the code that reads the registry by its exact keys still finds it.

use meerschaum_core::{Pipe, SuccessTuple};

use crate::traits::{Connector, PipeKeysEntry, PipesKeys, PipesKeysQuery};

/// The connector keys of Meerschaum's internal pipes.
pub const INTERNAL_CONNECTOR_KEYS: &str = "mrsm";

/// Whether `pipe` is one of Meerschaum's internal pipes (connector keys `mrsm`).
pub fn is_internal_pipe(pipe: &Pipe) -> bool {
    pipe.connector_keys() == INTERNAL_CONNECTOR_KEYS
}

/// The refusal a write to `pipe` through `conn` gets (`Some`) when `conn` is protected, writes
/// are not allowed ([`meerschaum_core::protected::allowed`]) and `pipe` is not an internal pipe.
/// `op` is the verb phrase of the refusal (`"sync"`, `"clear"`, ...); the pipe is named after it.
pub fn protected_refusal<C: Connector + ?Sized>(
    conn: &C,
    pipe: &Pipe,
    op: &str,
) -> Option<SuccessTuple> {
    if is_internal_pipe(pipe) || !conn.is_protected() {
        return None;
    }
    meerschaum_core::protected::check(true, &format!("{op} {pipe}"), &conn.connector_keys())
        .err()
        .map(SuccessTuple::fail)
}

/// [`protected_refusal`] for a write that does not concern one pipe (raw SQL execution):
/// `Err(message)` when `conn` is protected and writes are not allowed.
pub fn check_protected<C: Connector + ?Sized>(conn: &C, op: &str) -> Result<(), String> {
    meerschaum_core::protected::check(conn.is_protected(), op, &conn.connector_keys())
}

/// Whether a `fetch_pipes_keys` query leaves out the internal `mrsm` pipes: unless one of its
/// connector keys is exactly `mrsm` (negations like `_mrsm` do not count).
pub fn hides_internal_pipes(query: &PipesKeysQuery) -> bool {
    !query
        .connector_keys
        .iter()
        .any(|k| k == INTERNAL_CONNECTOR_KEYS)
}

/// `keys` without the internal `mrsm` pipes when [`hides_internal_pipes`] says so.
pub fn hide_internal_pipes(query: &PipesKeysQuery, keys: PipesKeys) -> PipesKeys {
    if !hides_internal_pipes(query) {
        return keys;
    }
    let visible = |e: &PipeKeysEntry| e.connector_keys != INTERNAL_CONNECTOR_KEYS;
    match keys {
        PipesKeys::List(entries) => PipesKeys::List(entries.into_iter().filter(visible).collect()),
        PipesKeys::Dict(entries) => {
            PipesKeys::Dict(entries.into_iter().filter(|(_, e)| visible(e)).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::{ConnectorResult, PipeId};
    use indexmap::IndexMap;

    struct Conn(bool);

    #[async_trait::async_trait]
    impl Connector for Conn {
        fn connector_type(&self) -> &str {
            "sql"
        }
        fn label(&self) -> &str {
            "prod"
        }
        fn is_protected(&self) -> bool {
            self.0
        }
        async fn test_connection(&self) -> ConnectorResult<bool> {
            Ok(true)
        }
    }

    fn entry(ck: &str) -> PipeKeysEntry {
        PipeKeysEntry {
            connector_keys: ck.into(),
            metric_key: "m".into(),
            location_key: None,
            extra: None,
        }
    }

    #[test]
    fn internal_pipes_are_hidden_unless_named() {
        let mut dict = IndexMap::new();
        dict.insert(PipeId::Int(1), entry("sql:main"));
        dict.insert(PipeId::Int(2), entry("mrsm"));
        let keys = PipesKeys::Dict(dict);
        let all = PipesKeysQuery::default();
        assert_eq!(hide_internal_pipes(&all, keys.clone()).len(), 1);
        let negated = PipesKeysQuery {
            connector_keys: vec!["_sql:other".into()],
            ..Default::default()
        };
        assert_eq!(hide_internal_pipes(&negated, keys.clone()).len(), 1);
        let explicit = PipesKeysQuery {
            connector_keys: vec!["sql:main".into(), "mrsm".into()],
            ..Default::default()
        };
        assert_eq!(hide_internal_pipes(&explicit, keys.clone()).len(), 2);
        let list = PipesKeys::List(vec![entry("mrsm"), entry("plugin:x")]);
        assert_eq!(hide_internal_pipes(&all, list).len(), 1);
    }

    #[test]
    fn protected_refuses_user_pipes_only() {
        let pipe = Pipe::builder("plugin:noaa", "weather").build().unwrap();
        let internal = Pipe::builder("mrsm", "pipeline")
            .location(Some("daily"))
            .build()
            .unwrap();
        assert!(protected_refusal(&Conn(false), &pipe, "sync").is_none());
        assert!(protected_refusal(&Conn(true), &internal, "sync").is_none());
        if !meerschaum_core::protected::allowed() {
            let refused = protected_refusal(&Conn(true), &pipe, "sync").unwrap();
            assert!(!refused.success);
            assert!(
                refused
                    .message
                    .starts_with("Refusing to sync Pipe('plugin:noaa', 'weather') on protected instance 'sql:prod'."),
                "{}",
                refused.message
            );
            assert!(check_protected(&Conn(true), "execute SQL").is_err());
        }
        let _scope = meerschaum_core::protected::allow(true);
        assert!(protected_refusal(&Conn(true), &pipe, "sync").is_none());
        assert!(check_protected(&Conn(true), "execute SQL").is_ok());
    }
}
