//! Main's effective native verify grid. Typed endpoints use default VerifyArgs:
//! no user-selected deduplication, rowcount-only mode, interval, loop or batch flags.
//! Main resolves the exact effective native calls once; worker consumes them without
//! re-reading watermarks or choosing a new bound_time. No write/completion proof.

use chrono::Duration;
use meerschaum_connector::DateBound;
use meerschaum_connector_sql::instance::data::{get_chunk_bounds, subtract_interval};
use meerschaum_core::{Pipe, Value, pipe::ChunkInterval, success::Bound};
use meerschaum_server::executor::pipe_operation::{WindowCoverage, WindowRange};

const MAX_CALLS: usize = 1024;

/// Collected only by Main's fresh original-source+captured-graph metadata adapter.
/// Ok(None) is absence; a read failure must never be converted to None by the issuer.
pub struct Observations {
    pub local_oldest: Option<DateBound>,
    pub local_newest: Option<DateBound>,
    pub remote_oldest: Option<DateBound>,
    pub remote_newest: Option<DateBound>,
    pub aware: bool,
}

pub struct NativeVerifyPlan {
    pub calls: Vec<WindowRange>,
    pub coverage: WindowCoverage,
    pub fallback_sync: bool,
}
fn unavailable() -> String {
    "Main could not issue this verification's exact effective native calls. It cannot substitute an unbounded worker plan.".into()
}

fn bump(bound: DateBound) -> Result<DateBound, String> {
    match bound {
        DateBound::Int(value) => value
            .checked_add(1)
            .map(DateBound::Int)
            .ok_or_else(unavailable),
        DateBound::Datetime(mut value) => {
            value.utc = value
                .utc
                .checked_add_signed(Duration::minutes(1))
                .ok_or_else(unavailable)?;
            Ok(DateBound::Datetime(value))
        }
    }
}
fn smaller(left: &DateBound, right: &DateBound) -> Result<bool, String> {
    match (left, right) {
        (DateBound::Int(left), DateBound::Int(right)) => Ok(left < right),
        (DateBound::Datetime(left), DateBound::Datetime(right)) => Ok(left.utc < right.utc),
        _ => Err(unavailable()),
    }
}
fn bound(value: DateBound) -> Bound {
    match value {
        DateBound::Int(value) => Bound::Int(value),
        DateBound::Datetime(value) => Bound::Datetime(value.utc.and_utc()),
    }
}
fn position(value: &DateBound) -> i128 {
    match value {
        DateBound::Int(value) => i128::from(*value),
        DateBound::Datetime(value) => i128::from(value.utc.and_utc().timestamp_micros()),
    }
}

/// Shared with ordinary native verification; Main and workers use one bound-interval rule.
pub fn bound_interval(pipe: &Pipe) -> Option<ChunkInterval> {
    let verify = pipe.parameters().get("verify")?.as_map()?;
    let units = ["minutes", "hours", "days", "weeks", "years", "seconds"];
    let (key, value) = verify.iter().find(|(key, _)| {
        units
            .iter()
            .any(|unit| key.as_str() == format!("bound_{unit}"))
    })?;
    if value.is_null() {
        return None;
    }
    let integer = pipe.column("datetime").as_deref().is_none_or(|column| {
        pipe.parameters()
            .get("dtypes")
            .and_then(Value::as_map)
            .and_then(|dtypes| dtypes.get(column))
            .map(meerschaum_core::pyfmt::py_str)
            .unwrap_or_else(|| "datetime".into())
            .to_lowercase()
            .contains("int")
    });
    let number = match value {
        Value::Int(value) => *value as f64,
        Value::Float(value) => *value,
        value => meerschaum_core::pyfmt::py_str(value)
            .trim()
            .parse()
            .unwrap_or(0.0),
    };
    if integer {
        return Some(ChunkInterval::Int(number as i64));
    }
    let seconds = match key.trim_start_matches("bound_") {
        "minutes" => 60.0,
        "hours" => 3600.0,
        "days" => 86400.0,
        "weeks" => 604800.0,
        "years" => 365.0 * 86400.0,
        _ => 1.0,
    };
    Some(ChunkInterval::Delta(Duration::microseconds(
        (number * seconds * 1e6) as i64,
    )))
}

fn bound_time(pipe: &Pipe, observed: &Observations) -> Result<Option<DateBound>, String> {
    let Some(interval) = bound_interval(pipe) else {
        return Ok(None);
    };
    let (Some(oldest), Some(newest)) = (&observed.local_oldest, &observed.local_newest) else {
        return Ok(None);
    };
    let candidate = subtract_interval(newest.clone(), interval);
    let extreme = match (oldest, newest) {
        (DateBound::Datetime(oldest), DateBound::Datetime(newest)) => {
            newest.utc - oldest.utc
                >= Duration::days(meerschaum_core::static_config().pipes.max_bound_time_days)
        }
        (DateBound::Int(_), DateBound::Int(_)) => false,
        _ => return Err(unavailable()),
    };
    Ok((smaller(oldest, &candidate)? || extreme).then_some(candidate))
}

/// default_begin is the freshly Main-resolved native determine_begin(Default)
/// for the ONLY full None/None sync call. Explicit finite-end calls have an actual
/// unbounded lower predicate, matching native determine_begin's end shortcut.
/// Caller must collect this only if the pure plan requires it; no eager remote
/// source probe/unsupported query is added to an otherwise local verification.
pub fn plan(
    pipe: &Pipe,
    requested_begin: Option<DateBound>,
    requested_end: Option<DateBound>,
    interval: ChunkInterval,
    observed: &Observations,
    default_begin: Option<DateBound>,
) -> Result<NativeVerifyPlan, String> {
    let coerce = |value: DateBound| value.coerce_tz(observed.aware);
    let effective = bound_time(pipe, observed)?;
    let bounded = effective.is_some(); // typed endpoint default bounded=None
    let mut begin = requested_begin.map(coerce);
    let mut end = requested_end.map(coerce);
    if bounded && begin.is_none() {
        begin = effective
            .or_else(|| observed.local_oldest.clone())
            .or_else(|| observed.remote_oldest.clone());
    }
    if bounded && end.is_none() {
        end = observed
            .local_newest
            .clone()
            .or_else(|| observed.remote_newest.clone())
            .map(bump)
            .transpose()?;
    }
    let fallback_sync = bounded && begin.is_none() && end.is_none();
    let mut calls = if fallback_sync {
        vec![(begin.clone(), end.clone())]
    } else {
        // Guard the REAL native helper before allocating a million-element Vec.
        // This phase supports <=MAX_CALLS; later compact/paged grid instructions
        // must preserve the native edge/consolidation rules for larger plans.
        let low = begin.as_ref().or(observed.local_oldest.as_ref());
        let high = match end.as_ref() {
            Some(end) => Some(end.clone()),
            None => observed.local_newest.clone().map(bump).transpose()?,
        };
        if let (Some(low), Some(high)) = (low, high.as_ref()) {
            let width = match interval {
                ChunkInterval::Int(width) => i128::from(width.max(1)),
                ChunkInterval::Delta(width) => {
                    i128::from(width.num_microseconds().unwrap_or(i64::MAX).max(1))
                }
            };
            let distance = position(high).saturating_sub(position(low));
            if distance > 0 && distance / width > MAX_CALLS as i128 {
                return Err("This verification needs paged Main grid instructions; no oversized or unbounded fallback was issued.".into());
            }
        }
        get_chunk_bounds(
            begin,
            end,
            bounded,
            interval,
            true,
            observed.local_oldest.clone(),
            observed.local_newest.clone(),
        )?
    };
    if calls.is_empty() {
        return Ok(NativeVerifyPlan {
            calls: Vec::new(),
            coverage: WindowCoverage::AxisRanges,
            fallback_sync: false,
        });
    }
    if calls.len() > MAX_CALLS {
        return Err(unavailable());
    }
    let mut no_predicate = false;
    for (begin, end) in &mut calls {
        if begin.is_none() && end.is_none() {
            *begin = default_begin.clone();
            no_predicate |= begin.is_none();
        }
        if let (Some(begin), Some(end)) = (begin, end)
            && !smaller(begin, end)?
        {
            return Err("This native verification issued an empty/reversed call; Main must emit an explicit no-op instruction.".into());
        }
    }
    let coverage = if no_predicate && calls.len() == 1 {
        WindowCoverage::WholeRelation
    } else {
        WindowCoverage::AxisRanges
    };
    Ok(NativeVerifyPlan {
        calls: calls
            .into_iter()
            .map(|(begin, end)| WindowRange {
                begin: begin.map(bound),
                end: end.map(bound),
            })
            .collect(),
        coverage,
        fallback_sync,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn pipe(verify: serde_json::Value) -> Pipe {
        Pipe::builder("sql:source", "verify-native")
            .instance("sql:main")
            .parameters(
                meerschaum_core::Value::from_json_str(
                    &serde_json::json!({
                        "columns":{"datetime":"tick"},"dtypes":{"tick":"int64"},"verify":verify,
                    })
                    .to_string(),
                )
                .unwrap()
                .to_map(),
            )
            .build()
            .unwrap()
    }
    fn observations(oldest: Option<i64>, newest: Option<i64>) -> Observations {
        Observations {
            local_oldest: oldest.map(DateBound::Int),
            local_newest: newest.map(DateBound::Int),
            remote_oldest: None,
            remote_newest: None,
            aware: true,
        }
    }
    fn call(begin: Option<i64>, end: Option<i64>) -> WindowRange {
        WindowRange {
            begin: begin.map(Bound::Int),
            end: end.map(Bound::Int),
        }
    }
    #[test]
    fn actual_unbounded_native_verify_edges_do_not_mint_a_null_axis_predicate() {
        let plan = plan(
            &pipe(serde_json::json!({})),
            None,
            None,
            ChunkInterval::Int(1),
            &observations(Some(10), Some(12)),
            None,
        )
        .unwrap();
        assert_eq!(
            plan.calls,
            vec![
                call(None, Some(10)),
                call(Some(10), Some(11)),
                call(Some(11), Some(13)),
                call(Some(13), None)
            ]
        );
        assert_eq!(plan.coverage, WindowCoverage::AxisRanges);
        assert!(!plan.fallback_sync);
    }
    #[test]
    fn registered_bound_days_on_an_integer_axis_is_native_units_and_caps_real_calls() {
        let pipe = pipe(serde_json::json!({"bound_days":4}));
        let plan = plan(
            &pipe,
            None,
            None,
            ChunkInterval::Int(2),
            &observations(Some(0), Some(10)),
            None,
        )
        .unwrap();
        assert_eq!(
            plan.calls,
            vec![
                call(Some(6), Some(8)),
                call(Some(8), Some(10)),
                call(Some(10), Some(11))
            ]
        );
        assert_eq!(plan.coverage, WindowCoverage::AxisRanges);
    }
    #[test]
    fn absent_local_bounds_use_only_the_main_resolved_default_not_remote_proposals() {
        let pipe = pipe(serde_json::json!({}));
        let mut observed = observations(None, None);
        observed.remote_oldest = Some(DateBound::Int(-1000));
        observed.remote_newest = Some(DateBound::Int(1000));
        let whole = plan(&pipe, None, None, ChunkInterval::Int(1), &observed, None).unwrap();
        assert_eq!(whole.calls, vec![call(None, None)]);
        assert_eq!(whole.coverage, WindowCoverage::WholeRelation);
        let current = plan(
            &pipe,
            None,
            None,
            ChunkInterval::Int(1),
            &observed,
            Some(DateBound::Int(25)),
        )
        .unwrap();
        assert_eq!(current.calls, vec![call(Some(25), None)]);
        assert_eq!(current.coverage, WindowCoverage::AxisRanges);
    }
    #[test]
    fn overflow_and_oversized_grids_refuse_and_empty_native_grid_is_an_explicit_noop() {
        let pipe = pipe(serde_json::json!({}));
        assert!(
            plan(
                &pipe,
                None,
                None,
                ChunkInterval::Int(1),
                &observations(Some(1), Some(i64::MAX)),
                None
            )
            .is_err()
        );
        assert!(
            plan(
                &pipe,
                Some(DateBound::Int(1)),
                Some(DateBound::Int(100_000)),
                ChunkInterval::Int(1),
                &observations(None, None),
                None
            )
            .is_err()
        );
        assert!(
            plan(
                &pipe,
                Some(DateBound::Int(0)),
                Some(DateBound::Int(0)),
                ChunkInterval::Int(1),
                &observations(None, None),
                None
            )
            .unwrap()
            .calls
            .is_empty()
        );
        // PARITY: the native helper treats equal nonzero unbounded verification
        // bounds as a tail call, while integer zero follows its empty-grid path.
        // Main must retain that exact native result rather than invent a no-op.
        let equal_nonzero = plan(
            &pipe,
            Some(DateBound::Int(10)),
            Some(DateBound::Int(10)),
            ChunkInterval::Int(1),
            &observations(None, None),
            None,
        )
        .unwrap();
        assert_eq!(equal_nonzero.calls, vec![call(Some(10), None)]);
        assert_eq!(equal_nonzero.coverage, WindowCoverage::AxisRanges);
    }
}
