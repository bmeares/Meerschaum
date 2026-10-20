//! The dirty-range ledger, edge cursors and the definition hash (§5.4).
//!
//! - A parent writes a [`LedgerEntry`] (a sequence number and the interval its sync changed,
//!   bucketed onto its epoch-aligned chunk grid) **before** committing data.
//! - Each edge keeps an [`EdgeCursor`]: the last parent sequence number the child consumed and
//!   the parent watermark at that moment. A child advances it only **after** its sync succeeds.
//! - During planning the unconsumed entries are combined into intervals ([`dirty_intervals`]),
//!   mapped onto the child's axis and grid ([`map_to_child`]) and drive `backfill`.
//! - Entries every child has consumed are pruned ([`compaction_floor`]).

use chrono::{DateTime, Utc};
use meerschaum_core::dtypes::PrecisionUnit;
use meerschaum_core::success::Bound;
use meerschaum_core::{Map, Value};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::axis::{Axis, AxisKind, Interval, Rounding, convert, occupied_cells, union};
use crate::graph::EdgeOptions;
use crate::node::PipeRef;

/// One row of the `mrsm:engine_ledger` pipe: datetime `created`, ids `pipe_ref` and `seq`,
/// columns `begin` and `end`. `begin` / `end` are `None` for pipes without a datetime column
/// (the entry carries only its sequence number).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LedgerEntry {
    /// The parent that changed.
    pub pipe_ref: PipeRef,
    /// The per-pipe sequence number assigned by the instance (monotonically increasing).
    pub seq: i64,
    /// Inclusive lower bound of the changed interval (grid-aligned).
    pub begin: Option<Bound>,
    /// Exclusive upper bound of the changed interval (grid-aligned).
    pub end: Option<Bound>,
    /// When the entry was written (the Unix epoch when omitted in a fixture).
    #[serde(default = "crate::state::epoch")]
    pub created: DateTime<Utc>,
}

/// One row of the `mrsm:engine_cursors` pipe (primary key `(parent_ref, child_ref)`, upsert).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeCursor {
    /// The parent.
    pub parent_ref: PipeRef,
    /// The child.
    pub child_ref: PipeRef,
    /// The last parent sequence number the child consumed.
    pub seq: i64,
    /// The parent's watermark when the cursor last moved.
    pub parent_watermark_at_cursor: Option<Bound>,
}

/// Canonical JSON of a value: object keys sorted (by code point), no whitespace. Two parameter
/// trees that differ only in key order produce the same text.
pub fn canonical_json(value: &Value) -> String {
    let mut out = String::new();
    write_canonical(value, &mut out);
    out
}

fn write_canonical(value: &Value, out: &mut String) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Int(i) => out.push_str(&i.to_string()),
        Value::Float(f) => {
            out.push_str(&serde_json::to_string(f).unwrap_or_else(|_| "null".to_string()))
        }
        Value::Str(s) => out.push_str(&serde_json::to_string(s).expect("strings serialize")),
        Value::List(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        Value::Map(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            out.push('{');
            for (i, k) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(k).expect("strings serialize"));
                out.push(':');
                write_canonical(&m[k], out);
            }
            out.push('}');
        }
    }
}

/// The scheme prefix of [`definition_hash`]. A stored hash with a scheme this build does not
/// know was computed by a different scheme and is treated as unknown rather than as a change.
pub const DEFINITION_HASH_SCHEME: &str = "v2:";

/// The scheme of rc1, which hashed `dtypes`, `fetch` and `sql` only. A stored `v1:` hash is still
/// compared (against [`legacy_definition_hash`]) so a definition changed before the upgrade is
/// noticed, and is replaced by a `v2:` hash after the next successful sync.
pub const LEGACY_DEFINITION_HASH_SCHEME: &str = "v1:";

/// The parameters a definition change is detected in (`columns` since 5.0.0rc2, reduced to the
/// roles in [`DEFINITION_COLUMN_ROLES`]).
const DEFINITION_KEYS: [&str; 4] = ["columns", "dtypes", "fetch", "sql"];

/// The `columns` roles that are part of the definition: a changed or renamed `datetime`, `id` or
/// `primary` column changes what the table is keyed and ordered by. Other roles (`value`, custom
/// index roles) and a column set to `null` / `''` do not: adding an index must not make a
/// rebuild that clears the table and re-fetches its whole history.
const DEFINITION_COLUMN_ROLES: [&str; 3] = ["datetime", "id", "primary"];

/// `columns` as the definition hash sees it: each of [`DEFINITION_COLUMN_ROLES`] with its column
/// name, `null` when the role is unset (a null, empty or non-string value is unset).
fn definition_columns(columns: Option<&Value>) -> Value {
    let mut m = Map::new();
    for role in DEFINITION_COLUMN_ROLES {
        let column = match columns {
            Some(Value::Map(cols)) => match cols.get(role) {
                Some(Value::Str(name)) if !name.is_empty() => Value::Str(name.clone()),
                _ => Value::Null,
            },
            _ => Value::Null,
        };
        m.insert(role.to_string(), column);
    }
    Value::Map(m)
}

/// The keys of the rc1 (`v1:`) scheme.
const LEGACY_DEFINITION_KEYS: [&str; 3] = ["dtypes", "fetch", "sql"];

fn hash_keys(parameters: &Map, keys: &[&str], scheme: &str) -> String {
    let mut m = Map::new();
    for key in keys {
        let value = if scheme == DEFINITION_HASH_SCHEME && *key == "columns" {
            definition_columns(parameters.get("columns"))
        } else {
            parameters.get(*key).cloned().unwrap_or(Value::Null)
        };
        m.insert((*key).to_string(), value);
    }
    let digest = Sha256::digest(canonical_json(&Value::Map(m)).as_bytes());
    let mut hex = String::with_capacity(scheme.len() + 64);
    hex.push_str(scheme);
    for byte in digest {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}

/// The definition hash of a pipe's **registered** parameters: SHA-256 over the canonical JSON
/// of `{"columns": {"datetime", "id", "primary"}, "dtypes": ..., "fetch": ..., "sql": ...}`
/// (missing keys are `null`), prefixed with the scheme (`v2:`). Templates and `MRSM{}` symlinks are hashed as written,
/// unresolved.
pub fn definition_hash(parameters: &Map) -> String {
    hash_keys(parameters, &DEFINITION_KEYS, DEFINITION_HASH_SCHEME)
}

/// The rc1 (`v1:`) hash, over `dtypes`, `fetch` and `sql` only.
pub fn legacy_definition_hash(parameters: &Map) -> String {
    hash_keys(
        parameters,
        &LEGACY_DEFINITION_KEYS,
        LEGACY_DEFINITION_HASH_SCHEME,
    )
}

/// Whether a stored hash differs from the current parameters under the stored hash's scheme.
/// No stored hash, or a scheme this build does not know, is a baseline and not a change; a
/// stored rc1 `v1:` hash is compared with the rc1 scheme, so upgrading never rebuilds a pipe
/// whose definition did not change, and a `columns` edit is only noticed once a `v2:` hash is
/// stored.
pub fn definition_changed(stored: Option<&str>, parameters: &Map) -> bool {
    match stored {
        Some(s) if s.starts_with(DEFINITION_HASH_SCHEME) => s != definition_hash(parameters),
        Some(s) if s.starts_with(LEGACY_DEFINITION_HASH_SCHEME) => {
            s != legacy_definition_hash(parameters)
        }
        _ => false,
    }
}

/// An entry's interval on its pipe's axis. Missing bounds, or bounds of the wrong kind, are
/// unbounded: an entry without an interval dirties everything.
pub fn entry_interval(entry: &LedgerEntry, axis: &Axis) -> Interval {
    if axis.kind == AxisKind::None {
        return Interval::full();
    }
    Interval::new(
        entry.begin.as_ref().and_then(|b| axis.pos(b)),
        entry.end.as_ref().and_then(|e| axis.pos(e)),
    )
}

/// The union of the intervals of unconsumed entries, on the parent's axis.
pub fn dirty_intervals<'a>(
    entries: impl IntoIterator<Item = &'a LedgerEntry>,
    parent_axis: &Axis,
) -> Vec<Interval> {
    union(entries.into_iter().map(|e| entry_interval(e, parent_axis)))
}

/// The intervals a sync's changed rows dirty: the epoch-aligned grid cells that hold at least
/// one changed datetime value, merged into contiguous runs (one ledger entry per run).
pub fn ledger_intervals(positions: impl IntoIterator<Item = i64>, axis: &Axis) -> Vec<Interval> {
    match axis.kind {
        AxisKind::None => vec![Interval::full()],
        _ => occupied_cells(positions, axis.chunk),
    }
}

/// Map a position from a parent's axis onto a child's axis. `axis_unit` is the edge's
/// override of the parent's integer unit.
pub fn map_position(
    pos: i64,
    parent: &Axis,
    child: &Axis,
    axis_unit: Option<PrecisionUnit>,
    rounding: Rounding,
) -> Option<i64> {
    match (parent.kind, child.kind) {
        (AxisKind::None, _) | (_, AxisKind::None) => None,
        (AxisKind::Datetime, AxisKind::Datetime) => Some(pos),
        (AxisKind::Int, AxisKind::Int) => {
            // Integer → integer converts only when both units are stated; otherwise the axes
            // are assumed to share a unit.
            match (axis_unit.or(parent.explicit_unit), child.explicit_unit) {
                (Some(from), Some(to)) => Some(convert(pos, from, to, rounding)),
                _ => Some(pos),
            }
        }
        _ => {
            let from = parent.unit(axis_unit)?;
            let to = child.unit(None)?;
            Some(convert(pos, from, to, rounding))
        }
    }
}

/// Map a parent's watermark onto the child's axis (floor).
pub fn map_watermark(
    watermark: &Bound,
    parent: &Axis,
    child: &Axis,
    options: &EdgeOptions,
) -> Option<i64> {
    let pos = parent.pos(watermark)?;
    map_position(pos, parent, child, options.axis_unit, Rounding::Floor)
}

/// Map dirty parent intervals onto a child (§5.4): convert units across the edge, widen by the
/// edge's dependency window, then widen to the child's epoch-aligned chunk grid. With no
/// datetime axis on either side, any dirty range dirties the child's whole range.
///
/// The edge options describe the child's query: `lookback = N` means the child's row at `t`
/// reads parent rows in `[t − N, t]` (a trailing window), so a parent change in `[b, e)` dirties
/// the child in `[b, e + N)`; `lookahead = N` means it reads `[t, t + N]`, which dirties
/// `[b − N, e)`.
pub fn map_to_child(
    intervals: &[Interval],
    parent: &Axis,
    child: &Axis,
    options: &EdgeOptions,
) -> Vec<Interval> {
    if intervals.is_empty() {
        return Vec::new();
    }
    if parent.kind == AxisKind::None || child.kind == AxisKind::None {
        return vec![Interval::full()];
    }
    let lookback = options.lookback.map_or(0, |x| x.to_units(child));
    let lookahead = options.lookahead.map_or(0, |x| x.to_units(child));
    union(intervals.iter().map(|iv| {
        let begin = iv
            .begin
            .and_then(|b| map_position(b, parent, child, options.axis_unit, Rounding::Floor));
        let end = iv
            .end
            .and_then(|e| map_position(e, parent, child, options.axis_unit, Rounding::Ceil));
        Interval::new(begin, end)
            .extend(lookahead, lookback)
            .widen_to_grid(child.chunk)
    }))
}

/// The highest sequence number of a parent that may be pruned: the minimum cursor over its
/// children that have a cursor. Children without a cursor plan from watermarks on their first
/// run and never read old entries, so they do not hold entries back. A parent without any
/// cursor keeps everything (`None`); a parent with no children at all may prune everything,
/// which the caller expresses by passing `i64::MAX`.
pub fn compaction_floor(cursor_seqs: impl IntoIterator<Item = Option<i64>>) -> Option<i64> {
    cursor_seqs.into_iter().flatten().min()
}

/// The sequence numbers of `entries` at or below `floor`.
pub fn prunable(entries: &[LedgerEntry], floor: Option<i64>) -> Vec<i64> {
    match floor {
        None => Vec::new(),
        Some(f) => entries
            .iter()
            .filter(|e| e.seq <= f)
            .map(|e| e.seq)
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Extent;
    use chrono::{TimeDelta, TimeZone};

    fn dt(d: u32, h: u32) -> Bound {
        Bound::Datetime(Utc.with_ymd_and_hms(2026, 10, d, h, 0, 0).unwrap())
    }

    fn us(d: u32, h: u32) -> i64 {
        Utc.with_ymd_and_hms(2026, 10, d, h, 0, 0)
            .unwrap()
            .timestamp_micros()
    }

    #[test]
    fn hash_ignores_key_order_and_other_keys() {
        let a = Value::from_json_str(
            r#"{"sql": "SELECT 1", "fetch": {"a": 1, "b": 2}, "tags": ["x"]}"#,
        )
        .unwrap();
        let b = Value::from_json_str(r#"{"fetch": {"b": 2, "a": 1}, "sql": "SELECT 1"}"#).unwrap();
        let c = Value::from_json_str(r#"{"fetch": {"b": 2, "a": 1}, "sql": "SELECT 2"}"#).unwrap();
        let ha = definition_hash(a.as_map().unwrap());
        assert!(ha.starts_with("v2:") && ha.len() == 67);
        assert_eq!(ha, definition_hash(b.as_map().unwrap()));
        assert_ne!(ha, definition_hash(c.as_map().unwrap()));
        let a = a.as_map().unwrap();
        assert!(!definition_changed(None, a));
        assert!(!definition_changed(Some("v0:abc"), a));
        assert!(definition_changed(Some("v2:abc"), a));
        assert!(!definition_changed(Some(&ha), a));
        assert_eq!(
            canonical_json(&Value::from_json_str(r#"{"b": [1, 2.5, null], "a": "é"}"#).unwrap()),
            r#"{"a":"é","b":[1,2.5,null]}"#
        );
    }

    #[test]
    fn columns_are_part_of_the_definition() {
        let json = |s: &str| Value::from_json_str(s).unwrap();
        let before = json(r#"{"columns": {"datetime": "ts", "id": "sensor"}, "sql": "SELECT 1"}"#);
        let after = json(r#"{"columns": {"datetime": "time", "id": "sensor"}, "sql": "SELECT 1"}"#);
        let reordered =
            json(r#"{"sql": "SELECT 1", "columns": {"id": "sensor", "datetime": "ts"}}"#);
        let (before, after, reordered) = (
            before.as_map().unwrap(),
            after.as_map().unwrap(),
            reordered.as_map().unwrap(),
        );
        let stored = definition_hash(before);
        assert!(definition_changed(Some(&stored), after));
        assert!(!definition_changed(Some(&stored), reordered));
        assert_ne!(stored, definition_hash(after));
    }

    #[test]
    fn only_the_datetime_id_and_primary_columns_are_part_of_the_definition() {
        let json = |s: &str| Value::from_json_str(s).unwrap();
        let base = json(r#"{"columns": {"datetime": "ts"}, "sql": "SELECT 1"}"#);
        let stored = definition_hash(base.as_map().unwrap());
        // Another role, a custom index role and an unset (null / empty) column are not a change.
        for same in [
            r#"{"columns": {"datetime": "ts", "value": "v"}, "sql": "SELECT 1"}"#,
            r#"{"columns": {"datetime": "ts", "station": "s"}, "sql": "SELECT 1"}"#,
            r#"{"columns": {"datetime": "ts", "id": null, "primary": ""}, "sql": "SELECT 1"}"#,
            r#"{"columns": {"datetime": "ts"}, "sql": "SELECT 1", "tags": ["x"]}"#,
        ] {
            let params = json(same);
            assert!(
                !definition_changed(Some(&stored), params.as_map().unwrap()),
                "{same}"
            );
        }
        // A new, renamed or removed datetime / id / primary is.
        for different in [
            r#"{"columns": {"datetime": "ts", "id": "sensor"}, "sql": "SELECT 1"}"#,
            r#"{"columns": {"datetime": "ts", "primary": "pk"}, "sql": "SELECT 1"}"#,
            r#"{"columns": {"datetime": "time"}, "sql": "SELECT 1"}"#,
            r#"{"sql": "SELECT 1"}"#,
        ] {
            let params = json(different);
            assert!(
                definition_changed(Some(&stored), params.as_map().unwrap()),
                "{different}"
            );
        }
    }

    #[test]
    fn rc1_hashes_are_compared_with_the_rc1_scheme() {
        let json = |s: &str| Value::from_json_str(s).unwrap();
        let rc1 = json(r#"{"columns": {"datetime": "ts"}, "sql": "SELECT 1"}"#);
        let rc1 = rc1.as_map().unwrap();
        let stored = legacy_definition_hash(rc1);
        assert!(stored.starts_with("v1:"));
        // Upgrading does not rebuild a pipe whose rc1 definition is unchanged ...
        assert!(!definition_changed(Some(&stored), rc1));
        // ... a `columns` edit made before the first rc2 sync is not noticed (baseline) ...
        let renamed = json(r#"{"columns": {"datetime": "time"}, "sql": "SELECT 1"}"#);
        assert!(!definition_changed(
            Some(&stored),
            renamed.as_map().unwrap()
        ));
        // ... and a `sql` edit still is.
        let edited = json(r#"{"columns": {"datetime": "ts"}, "sql": "SELECT 2"}"#);
        assert!(definition_changed(Some(&stored), edited.as_map().unwrap()));
    }

    #[test]
    fn maps_daily_ranges_with_lookback() {
        let hourly = Axis::datetime("ts", TimeDelta::hours(1), TimeDelta::zero());
        let daily = Axis::datetime("day", TimeDelta::days(1), TimeDelta::zero());
        let entry = LedgerEntry {
            pipe_ref: PipeRef::from_string("x"),
            seq: 1,
            begin: Some(dt(5, 3)),
            end: Some(dt(5, 4)),
            created: Utc::now(),
        };
        let dirty = dirty_intervals([&entry], &hourly);
        let mapped = map_to_child(&dirty, &hourly, &daily, &EdgeOptions::default());
        assert_eq!(mapped, vec![Interval::new(Some(us(5, 0)), Some(us(6, 0)))]);
        let opts = EdgeOptions {
            lookback: Some(Extent::Micros(
                TimeDelta::days(2).num_microseconds().unwrap(),
            )),
            ..Default::default()
        };
        // A two-day trailing window: the change on the 5th dirties the 5th through the 7th.
        let mapped = map_to_child(&dirty, &hourly, &daily, &opts);
        assert_eq!(mapped, vec![Interval::new(Some(us(5, 0)), Some(us(8, 0)))]);
        let opts = EdgeOptions {
            lookahead: Some(Extent::Micros(
                TimeDelta::days(2).num_microseconds().unwrap(),
            )),
            ..Default::default()
        };
        let mapped = map_to_child(&dirty, &hourly, &daily, &opts);
        assert_eq!(mapped, vec![Interval::new(Some(us(3, 0)), Some(us(6, 0)))]);
    }

    #[test]
    fn maps_epoch_millis_onto_datetime() {
        let mut ms = Axis::int("ts_ms", Some(PrecisionUnit::Millisecond), 86_400_000, 0);
        ms.derived_unit = Some(PrecisionUnit::Millisecond);
        let daily = Axis::datetime("ts", TimeDelta::days(1), TimeDelta::zero());
        let begin_ms = us(5, 0) / 1000;
        let iv = Interval::new(Some(begin_ms), Some(begin_ms + 86_400_000));
        assert_eq!(
            map_to_child(&[iv], &ms, &daily, &EdgeOptions::default()),
            vec![Interval::new(Some(us(5, 0)), Some(us(6, 0)))]
        );
        // Without a stated unit, the edge's `axis: {unit: s}` decides.
        let mut unitless = Axis::int("ts_s", None, 86_400, 0);
        unitless.derived_unit = Some(PrecisionUnit::Microsecond);
        let opts = EdgeOptions {
            axis_unit: Some(PrecisionUnit::Second),
            ..Default::default()
        };
        let begin_s = us(5, 0) / 1_000_000;
        let iv = Interval::new(Some(begin_s), Some(begin_s + 3600));
        assert_eq!(
            map_to_child(&[iv], &unitless, &daily, &opts),
            vec![Interval::new(Some(us(5, 0)), Some(us(6, 0)))]
        );
    }

    #[test]
    fn no_axis_dirties_everything() {
        let none = Axis::none();
        let daily = Axis::datetime("ts", TimeDelta::days(1), TimeDelta::zero());
        let iv = Interval::new(Some(0), Some(10));
        assert_eq!(
            map_to_child(&[iv], &none, &daily, &EdgeOptions::default()),
            vec![Interval::full()]
        );
        assert!(map_to_child(&[], &none, &daily, &EdgeOptions::default()).is_empty());
        assert_eq!(ledger_intervals([1, 2], &none), vec![Interval::full()]);
    }

    #[test]
    fn compaction() {
        assert_eq!(compaction_floor([Some(5), None, Some(3)]), Some(3));
        assert_eq!(compaction_floor([None, None]), None);
        let e = |seq| LedgerEntry {
            pipe_ref: PipeRef::from_string("x"),
            seq,
            begin: None,
            end: None,
            created: Utc::now(),
        };
        assert_eq!(prunable(&[e(1), e(2), e(4)], Some(3)), vec![1, 2]);
        assert!(prunable(&[e(1)], None).is_empty());
    }
}
