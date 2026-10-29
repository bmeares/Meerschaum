//! Datetime and integer axes, half-open intervals and the epoch-aligned chunk grid (§5.4).
//!
//! Every position on an axis is an `i64`: microseconds since the Unix epoch (UTC) on a datetime
//! axis, the raw value on an integer axis. Intervals are half-open `[begin, end)` with `None`
//! meaning unbounded (−∞ for `begin`, +∞ for `end`). Converting between axes goes through
//! nanoseconds with exact integer arithmetic.

use chrono::{DateTime, TimeDelta, Utc};
use meerschaum_core::dtypes::PrecisionUnit;
use meerschaum_core::success::Bound;
use serde::{Deserialize, Serialize};

/// What a pipe's `datetime` column measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AxisKind {
    /// No `datetime` column: no watermark, ledger entries carry only a sequence number.
    None,
    /// A timestamp column (positions are microseconds since the epoch).
    Datetime,
    /// An integer column (an epoch in some unit, or an arbitrary sequence).
    Int,
}

/// A node's axis: its kind, units, chunk grid and backtrack window, all in axis units.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Axis {
    /// The axis kind.
    pub kind: AxisKind,
    /// The `datetime` column name.
    pub column: Option<String>,
    /// For an integer axis: the unit from an explicit `precision` parameter.
    #[serde(default, with = "opt_unit")]
    pub explicit_unit: Option<PrecisionUnit>,
    /// For an integer axis: the unit core derives when `precision` is not set (used only
    /// to convert across an integer/datetime edge, with a warning).
    #[serde(default, with = "opt_unit")]
    pub derived_unit: Option<PrecisionUnit>,
    /// The chunk (partition) width in axis units (`get_chunk_interval`). Always ≥ 1.
    pub chunk: i64,
    /// The backtrack window in axis units (`get_backtrack_interval`).
    pub backtrack: i64,
}

impl Axis {
    /// An axis without a `datetime` column.
    pub fn none() -> Axis {
        Axis {
            kind: AxisKind::None,
            column: None,
            explicit_unit: None,
            derived_unit: None,
            chunk: 1,
            backtrack: 0,
        }
    }

    /// A datetime axis with the given chunk and backtrack widths.
    pub fn datetime(column: &str, chunk: TimeDelta, backtrack: TimeDelta) -> Axis {
        Axis {
            kind: AxisKind::Datetime,
            column: Some(column.to_string()),
            explicit_unit: None,
            derived_unit: None,
            chunk: chunk.num_microseconds().unwrap_or(i64::MAX).max(1),
            backtrack: backtrack.num_microseconds().unwrap_or(0),
        }
    }

    /// An integer axis with the given unit (if known), chunk and backtrack widths.
    pub fn int(column: &str, unit: Option<PrecisionUnit>, chunk: i64, backtrack: i64) -> Axis {
        Axis {
            kind: AxisKind::Int,
            column: Some(column.to_string()),
            explicit_unit: unit,
            derived_unit: unit,
            chunk: chunk.max(1),
            backtrack,
        }
    }

    /// The unit used for conversions: microseconds on a datetime axis; on an integer axis the
    /// `override_unit`, else the explicit unit, else the derived unit.
    pub fn unit(&self, override_unit: Option<PrecisionUnit>) -> Option<PrecisionUnit> {
        match self.kind {
            AxisKind::None => None,
            AxisKind::Datetime => Some(PrecisionUnit::Microsecond),
            AxisKind::Int => override_unit.or(self.explicit_unit).or(self.derived_unit),
        }
    }

    /// The position of a bound on this axis, if the bound's kind matches the axis.
    pub fn pos(&self, bound: &Bound) -> Option<i64> {
        match (self.kind, bound) {
            (AxisKind::Datetime, Bound::Datetime(dt)) => Some(dt.timestamp_micros()),
            (AxisKind::Int, Bound::Int(i)) => Some(*i),
            _ => None,
        }
    }

    /// The bound at a position on this axis (`None` for an axis without a datetime column).
    pub fn bound(&self, pos: i64) -> Option<Bound> {
        match self.kind {
            AxisKind::None => None,
            AxisKind::Datetime => DateTime::<Utc>::from_timestamp_micros(pos).map(Bound::Datetime),
            AxisKind::Int => Some(Bound::Int(pos)),
        }
    }

    /// A duration in this axis's units, following core's `_scale_delta_to_int_axis`: microseconds
    /// on a datetime axis; on an integer axis seconds × the explicit precision's scalar, or
    /// whole minutes when no `precision` is configured (legacy).
    pub fn delta_units(&self, delta: TimeDelta) -> i64 {
        match self.kind {
            AxisKind::None => 0,
            AxisKind::Datetime => delta.num_microseconds().unwrap_or(i64::MAX),
            AxisKind::Int => match self.explicit_unit {
                Some(u) => convert(
                    delta.num_microseconds().unwrap_or(i64::MAX),
                    PrecisionUnit::Microsecond,
                    u,
                    Rounding::Floor,
                ),
                None => delta.num_minutes(),
            },
        }
    }
}

/// Serde for `Option<PrecisionUnit>` as the unit's full name (`"millisecond"`); aliases such as
/// `"ms"` are accepted when reading.
pub mod opt_unit {
    use meerschaum_core::dtypes::PrecisionUnit;
    use serde::{Deserialize, Deserializer, Serializer};

    /// Serialize.
    pub fn serialize<S: Serializer>(u: &Option<PrecisionUnit>, s: S) -> Result<S::Ok, S::Error> {
        match u {
            Some(u) => s.serialize_str(u.name()),
            None => s.serialize_none(),
        }
    }

    /// Deserialize.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<PrecisionUnit>, D::Error> {
        match Option::<String>::deserialize(d)? {
            None => Ok(None),
            Some(name) => PrecisionUnit::parse(&name).map(Some).ok_or_else(|| {
                serde::de::Error::custom(format!("invalid precision unit '{name}'"))
            }),
        }
    }
}

/// Rounding direction for unit conversions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rounding {
    /// Toward −∞ (used for interval begins).
    Floor,
    /// Toward +∞ (used for interval ends).
    Ceil,
}

/// Nanoseconds per unit.
pub fn unit_nanos(unit: PrecisionUnit) -> i128 {
    match unit {
        PrecisionUnit::Nanosecond => 1,
        PrecisionUnit::Microsecond => 1_000,
        PrecisionUnit::Millisecond => 1_000_000,
        PrecisionUnit::Second => 1_000_000_000,
        PrecisionUnit::Minute => 60_000_000_000,
        PrecisionUnit::Hour => 3_600_000_000_000,
        PrecisionUnit::Day => 86_400_000_000_000,
    }
}

fn floor_div(a: i128, b: i128) -> i128 {
    a.div_euclid(b)
}

fn ceil_div(a: i128, b: i128) -> i128 {
    -((-a).div_euclid(b))
}

fn saturate(v: i128) -> i64 {
    v.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

/// Convert a position between units with exact integer arithmetic.
pub fn convert(value: i64, from: PrecisionUnit, to: PrecisionUnit, rounding: Rounding) -> i64 {
    if from == to {
        return value;
    }
    let nanos = value as i128 * unit_nanos(from);
    let per = unit_nanos(to);
    saturate(match rounding {
        Rounding::Floor => floor_div(nanos, per),
        Rounding::Ceil => ceil_div(nanos, per),
    })
}

/// The largest grid edge `≤ pos` on an epoch-aligned grid of width `width`.
pub fn grid_floor(pos: i64, width: i64) -> i64 {
    let w = width.max(1) as i128;
    saturate(floor_div(pos as i128, w) * w)
}

/// The smallest grid edge `≥ pos` on an epoch-aligned grid of width `width`.
pub fn grid_ceil(pos: i64, width: i64) -> i64 {
    let w = width.max(1) as i128;
    saturate(ceil_div(pos as i128, w) * w)
}

/// A half-open interval `[begin, end)` on an axis; `None` is unbounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Interval {
    /// Inclusive lower bound (`None` = −∞).
    pub begin: Option<i64>,
    /// Exclusive upper bound (`None` = +∞).
    pub end: Option<i64>,
}

fn begin_key(b: Option<i64>) -> i128 {
    b.map_or(i128::MIN, i128::from)
}

fn end_key(e: Option<i64>) -> i128 {
    e.map_or(i128::MAX, i128::from)
}

impl Interval {
    /// `[begin, end)`.
    pub fn new(begin: Option<i64>, end: Option<i64>) -> Interval {
        Interval { begin, end }
    }

    /// `(−∞, +∞)`.
    pub fn full() -> Interval {
        Interval {
            begin: None,
            end: None,
        }
    }

    /// Whether the interval contains no positions.
    pub fn is_empty(&self) -> bool {
        begin_key(self.begin) >= end_key(self.end)
    }

    /// Whether the interval is unbounded on either side.
    pub fn is_unbounded(&self) -> bool {
        self.begin.is_none() || self.end.is_none()
    }

    /// Whether `pos` lies in the interval.
    pub fn contains(&self, pos: i64) -> bool {
        begin_key(self.begin) <= pos as i128 && (pos as i128) < end_key(self.end)
    }

    /// The intersection (possibly empty).
    pub fn intersect(&self, other: &Interval) -> Interval {
        let begin = if begin_key(self.begin) >= begin_key(other.begin) {
            self.begin
        } else {
            other.begin
        };
        let end = if end_key(self.end) <= end_key(other.end) {
            self.end
        } else {
            other.end
        };
        Interval { begin, end }
    }

    /// Widen to the enclosing cells of an epoch-aligned grid: `[floor(begin), ceil(end))`.
    pub fn widen_to_grid(&self, width: i64) -> Interval {
        Interval {
            begin: self.begin.map(|b| grid_floor(b, width)),
            end: self.end.map(|e| grid_ceil(e, width)),
        }
    }

    /// The grid cells covering a bounded interval, in order. Returns `None` when the interval is
    /// unbounded or would need more than `max_cells` cells.
    pub fn grid_cells(&self, width: i64, max_cells: usize) -> Option<Vec<Interval>> {
        let (Some(b), Some(e)) = (self.begin, self.end) else {
            return None;
        };
        if b >= e {
            return Some(Vec::new());
        }
        let width = width.max(1);
        let start = grid_floor(b, width) as i128;
        let stop = grid_ceil(e, width) as i128;
        let n = (stop - start) / width as i128;
        if n > max_cells as i128 {
            return None;
        }
        Some(
            (0..n)
                .map(|i| {
                    let lo = start + i * width as i128;
                    Interval::new(Some(saturate(lo)), Some(saturate(lo + width as i128)))
                })
                .collect(),
        )
    }

    /// Shift the begin down by `lookback` and the end up by `lookahead` (saturating).
    pub fn extend(&self, lookback: i64, lookahead: i64) -> Interval {
        Interval {
            begin: self.begin.map(|b| b.saturating_sub(lookback)),
            end: self.end.map(|e| e.saturating_add(lookahead)),
        }
    }
}

/// The union of intervals as a sorted list of disjoint, non-adjacent, non-empty intervals.
/// Overlapping and touching intervals (`[a, b) ∪ [b, c)`) merge.
pub fn union(intervals: impl IntoIterator<Item = Interval>) -> Vec<Interval> {
    let mut items: Vec<Interval> = intervals.into_iter().filter(|i| !i.is_empty()).collect();
    items.sort_by_key(|i| (begin_key(i.begin), end_key(i.end)));
    let mut out: Vec<Interval> = Vec::with_capacity(items.len());
    for iv in items {
        match out.last_mut() {
            Some(cur) if begin_key(iv.begin) <= end_key(cur.end) => {
                if end_key(iv.end) > end_key(cur.end) {
                    cur.end = iv.end;
                }
            }
            _ => out.push(iv),
        }
    }
    out
}

/// The epoch-aligned grid cells occupied by a set of positions, merged into contiguous runs.
pub fn occupied_cells(positions: impl IntoIterator<Item = i64>, width: i64) -> Vec<Interval> {
    union(positions.into_iter().map(|p| {
        let lo = grid_floor(p, width);
        Interval::new(Some(lo), Some(lo.saturating_add(width.max(1))))
    }))
}

/// One chunk from [`get_chunk_bounds`]: `(begin, end)` with `None` unbounded.
pub type ChunkBound = (Option<i64>, Option<i64>);

/// Port of `Pipe.get_chunk_bounds` (`meerschaum/core/Pipe/_data.py:784`) over axis positions.
///
/// `oldest` / `newest` stand in for `get_sync_time(newest=False/True)`, which Python queries
/// when `begin` / `end` is `None`. `is_datetime` selects the `+1 minute` (datetime) or `+1`
/// (integer) widening of an implicit end.
///
/// TODO(core-merge): move to `meerschaum-core` next to `get_chunk_interval` once core has it.
#[allow(clippy::too_many_arguments)] // Mirrors the Python signature plus the two sync times.
pub fn get_chunk_bounds(
    begin: Option<i64>,
    end: Option<i64>,
    bounded: bool,
    chunk_interval: i64,
    align: bool,
    is_datetime: bool,
    oldest: Option<i64>,
    newest: Option<i64>,
) -> Result<Vec<ChunkBound>, String> {
    let chunk = chunk_interval.max(1);
    let include_less_than_begin = !bounded && begin.is_none();
    let include_greater_than_end = !bounded && end.is_none();
    let begin = begin.or(oldest);
    let mut consolidate_end_chunk = false;
    let end = match end {
        Some(e) => Some(e),
        None => newest.map(|n| {
            consolidate_end_chunk = true;
            n.saturating_add(if is_datetime { 60_000_000 } else { 1 })
        }),
    };
    let (begin, end) = match (begin, end) {
        (None, None) => return Ok(vec![(None, None)]),
        (Some(b), Some(e)) => (b, e),
        // Python raises a `TypeError` comparing `None` with a bound here.
        _ => return Err("cannot compute chunk bounds with only one bound".into()),
    };
    // PARITY: `if begin and end:` — an integer bound of 0 is falsy in Python, so the
    // `begin >= end` early return is skipped for it (`_data.py:856`).
    let truthy = |v: i64| is_datetime || v != 0;
    if truthy(begin) && truthy(end) && begin >= end {
        return Ok(if bounded {
            vec![(Some(begin), Some(begin))]
        } else {
            vec![(Some(begin), None)]
        });
    }
    let mut cursor = if align {
        grid_floor(begin, chunk)
    } else {
        begin
    };
    let mut bounds: Vec<(i64, i64)> = Vec::new();
    while cursor < end {
        let next = cursor.saturating_add(chunk);
        bounds.push((cursor, next));
        cursor = next;
        if bounds.len() >= 1_000_000 {
            return Err(format!(
                "Too many chunks of size '{chunk}' between '{begin}' and '{end}'."
            ));
        }
    }
    if bounds.len() > 1 && consolidate_end_chunk {
        let last = bounds.pop().expect("len > 1");
        let second_last = bounds.pop().expect("len > 1");
        bounds.push((second_last.0, last.1));
    }
    if bounds.is_empty() && end >= begin {
        bounds.push((begin, end));
    }
    let Some(last) = bounds.last_mut() else {
        // Python raises an `IndexError` on `chunk_bounds[-1]`.
        return Err("no chunk bounds (end precedes begin)".into());
    };
    if last.1 > end {
        last.1 = end;
    }
    if last.0 == last.1 {
        bounds.pop();
    }
    if align
        && let Some(first) = bounds.first_mut()
        && first.0 < begin
    {
        first.0 = begin;
    }
    let mut out: Vec<ChunkBound> = bounds
        .into_iter()
        .map(|(b, e)| (Some(b), Some(e)))
        .collect();
    if include_less_than_begin {
        out.insert(0, (None, Some(begin)));
    }
    if include_greater_than_end {
        out.push((Some(end), None));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const DAY_US: i64 = 86_400_000_000;

    #[test]
    fn conversions() {
        assert_eq!(
            convert(
                1_500,
                PrecisionUnit::Millisecond,
                PrecisionUnit::Second,
                Rounding::Floor
            ),
            1
        );
        assert_eq!(
            convert(
                1_500,
                PrecisionUnit::Millisecond,
                PrecisionUnit::Second,
                Rounding::Ceil
            ),
            2
        );
        assert_eq!(
            convert(
                -1_500,
                PrecisionUnit::Millisecond,
                PrecisionUnit::Second,
                Rounding::Floor
            ),
            -2
        );
        assert_eq!(
            convert(
                3,
                PrecisionUnit::Second,
                PrecisionUnit::Microsecond,
                Rounding::Floor
            ),
            3_000_000
        );
        assert_eq!(grid_floor(-1, 10), -10);
        assert_eq!(grid_ceil(1, 10), 10);
        assert_eq!(grid_ceil(10, 10), 10);
    }

    #[test]
    fn union_merges_adjacent() {
        let u = union([
            Interval::new(Some(5), Some(10)),
            Interval::new(Some(0), Some(5)),
            Interval::new(Some(20), Some(30)),
            Interval::new(Some(25), Some(26)),
            Interval::new(Some(40), Some(40)),
        ]);
        assert_eq!(
            u,
            vec![
                Interval::new(Some(0), Some(10)),
                Interval::new(Some(20), Some(30))
            ]
        );
        let u = union([Interval::new(None, Some(3)), Interval::new(Some(2), None)]);
        assert_eq!(u, vec![Interval::full()]);
    }

    #[test]
    fn chunk_bounds_aligned() {
        // Python: get_chunk_bounds(begin=day 0 + 12h, end=day 3, bounded=True, align=True)
        let b = get_chunk_bounds(
            Some(DAY_US / 2),
            Some(3 * DAY_US),
            true,
            DAY_US,
            true,
            true,
            None,
            None,
        )
        .unwrap();
        assert_eq!(
            b,
            vec![
                (Some(DAY_US / 2), Some(DAY_US)),
                (Some(DAY_US), Some(2 * DAY_US)),
                (Some(2 * DAY_US), Some(3 * DAY_US)),
            ]
        );
        // Unaligned and unbounded: leading and trailing open chunks.
        let b = get_chunk_bounds(Some(5), Some(25), false, 10, false, false, None, None).unwrap();
        assert_eq!(b, vec![(Some(5), Some(15)), (Some(15), Some(25))]);
        let b = get_chunk_bounds(None, None, false, 10, true, false, Some(3), Some(21)).unwrap();
        assert_eq!(
            b,
            vec![
                (None, Some(3)),
                (Some(3), Some(10)),
                (Some(10), Some(22)),
                (Some(22), None)
            ]
        );
        assert_eq!(
            get_chunk_bounds(Some(10), Some(5), true, 3, true, false, None, None).unwrap(),
            vec![(Some(10), Some(10))]
        );
        assert_eq!(
            get_chunk_bounds(None, None, false, 3, true, false, None, None).unwrap(),
            vec![(None, None)]
        );
    }

    fn interval_strategy() -> impl Strategy<Value = Interval> {
        (
            proptest::option::weighted(0.9, -1_000i64..1_000),
            proptest::option::weighted(0.9, -1_000i64..1_000),
        )
            .prop_map(|(b, e)| Interval::new(b, e))
    }

    proptest! {
        #[test]
        fn union_preserves_membership(
            ivs in proptest::collection::vec(interval_strategy(), 0..12),
            probes in proptest::collection::vec(-1_100i64..1_100, 0..64),
        ) {
            let u = union(ivs.clone());
            for p in probes {
                let before = ivs.iter().any(|i| i.contains(p));
                let after = u.iter().any(|i| i.contains(p));
                prop_assert_eq!(before, after);
            }
        }

        #[test]
        fn union_is_sorted_disjoint_and_non_adjacent(
            ivs in proptest::collection::vec(interval_strategy(), 0..12),
        ) {
            let u = union(ivs);
            for i in &u {
                prop_assert!(!i.is_empty());
            }
            for w in u.windows(2) {
                // Strictly separated: the next begin is beyond the previous end.
                prop_assert!(begin_key(w[1].begin) > end_key(w[0].end));
            }
            // Idempotent.
            prop_assert_eq!(union(u.clone()), u);
        }

        #[test]
        fn grid_cells_cover_and_align(
            b in -10_000i64..10_000,
            len in 1i64..5_000,
            width in 1i64..700,
        ) {
            let iv = Interval::new(Some(b), Some(b + len));
            let cells = iv.grid_cells(width, 100_000).unwrap();
            let widened = iv.widen_to_grid(width);
            prop_assert_eq!(union(cells.clone()), vec![widened]);
            for c in &cells {
                let (cb, ce) = (c.begin.unwrap(), c.end.unwrap());
                prop_assert_eq!(cb.rem_euclid(width), 0);
                prop_assert_eq!(ce - cb, width);
            }
            prop_assert!(widened.begin.unwrap() <= b);
            prop_assert!(widened.end.unwrap() >= b + len);
            prop_assert!(b - widened.begin.unwrap() < width);
            prop_assert!(widened.end.unwrap() - (b + len) < width);
        }

        #[test]
        fn occupied_cells_contain_every_position(
            positions in proptest::collection::vec(-5_000i64..5_000, 1..40),
            width in 1i64..300,
        ) {
            let cells = occupied_cells(positions.clone(), width);
            for p in &positions {
                prop_assert!(cells.iter().any(|c| c.contains(*p)));
            }
            for c in &cells {
                prop_assert_eq!(c.begin.unwrap().rem_euclid(width), 0);
                prop_assert_eq!(c.end.unwrap().rem_euclid(width), 0);
                // Every cell in a run holds at least one position.
                for cell in c.grid_cells(width, 100_000).unwrap() {
                    prop_assert!(positions.iter().any(|p| cell.contains(*p)));
                }
            }
        }

        #[test]
        fn aligned_chunks_tile_the_range(
            b in -10_000i64..10_000,
            len in 1i64..5_000,
            width in 1i64..700,
        ) {
            let chunks = get_chunk_bounds(
                Some(b), Some(b + len), true, width, true, false, None, None,
            ).unwrap();
            prop_assert_eq!(chunks.first().unwrap().0, Some(b));
            prop_assert_eq!(chunks.last().unwrap().1, Some(b + len));
            for w in chunks.windows(2) {
                prop_assert_eq!(w[0].1, w[1].0);
                prop_assert_eq!(w[0].1.unwrap().rem_euclid(width), 0);
            }
        }
    }
}
