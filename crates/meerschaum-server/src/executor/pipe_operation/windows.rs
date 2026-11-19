//! Main-created node-window evidence. These private canonical values are never
//! selected by a route body, job property, worker result or environment variable.

use meerschaum_connector::{DataQuery, DateBound};
use meerschaum_core::success::Bound;
use meerschaum_core::{Pipe, Value};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::ApiError;

const MAX_RANGES: usize = 1024;

fn denied() -> ApiError {
    ApiError::new(403, "The request exceeds its Main-admitted node window.")
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WindowAxis {
    WholeRelation,
    Integer {
        column: String,
        explicit_unit: Option<String>,
    },
    Datetime {
        column: String,
        dtype: String,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AxisGuard {
    column: Option<String>,
    dtype: Option<String>,
    precision: Option<Value>,
    autotime: bool,
}

impl AxisGuard {
    fn capture(pipe: &Pipe) -> Self {
        let column = pipe.column("datetime");
        Self {
            dtype: column
                .as_deref()
                .and_then(|column| pipe.dtype(column))
                .map(|dtype| axis_dtype(&dtype)),
            column,
            precision: pipe.raw_parameters().get("precision").cloned(),
            autotime: pipe.autotime(),
        }
    }
    fn matches(&self, pipe: &Pipe) -> bool {
        let next = Self::capture(pipe);
        self.column == next.column
            && self.precision == next.precision
            && self.autotime == next.autotime
            && self.dtype == next.dtype
    }
}

// The public dtype equality intentionally treats timezone/unit variants as equivalent.
// A window cannot: changing its physical axis interpretation invalidates the old permit.
fn axis_dtype(dtype: &str) -> String {
    let dtype = meerschaum_core::dtypes::resolve_alias(dtype);
    meerschaum_core::dtypes::pd_dtype(dtype)
        .unwrap_or(dtype)
        .chars()
        .filter(|character| !character.is_whitespace())
        .flat_map(char::to_lowercase)
        .collect()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WindowRange {
    pub begin: Option<Bound>,
    pub end: Option<Bound>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WindowCoverage {
    WholeRelation,
    #[default]
    AxisRanges,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum State {
    NotIssued {
        reason: String,
    },
    Issued {
        axis: WindowAxis,
        #[serde(default)]
        coverage: WindowCoverage,
        source: Vec<WindowRange>,
        read: Vec<WindowRange>,
        write: Vec<WindowRange>,
        replacement: Option<Vec<WindowRange>>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeWindow {
    id: Uuid,
    reference: String,
    registration: Value,
    guard: AxisGuard,
    #[serde(default)]
    accepted_rows: super::AcceptedRows,
    state: State,
}

/// Constructed only from the actual freshly admitted Main catalog record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallbackBinding {
    pub grant: Uuid,
    pub launch: Uuid,
    pub context_revision: u64,
}

impl WindowAxis {
    fn bound(&self, value: &DateBound) -> Result<Bound, ApiError> {
        use meerschaum_kernel::value::Value as KValue;
        match (self, value) {
            (Self::Integer { .. }, DateBound::Int(value)) => Ok(Bound::Int(*value)),
            (
                Self::Integer {
                    explicit_unit: Some(unit),
                    ..
                },
                DateBound::Datetime(value),
            ) => {
                let unit =
                    meerschaum_core::dtypes::PrecisionUnit::parse(unit).ok_or_else(denied)?;
                meerschaum_kernel::cast::datetime_to_int(
                    &KValue::Datetime(value.clone()),
                    unit.name(),
                )
                .map(Bound::Int)
                .map_err(|_| denied())
            }
            (Self::Datetime { dtype, .. }, DateBound::Datetime(value)) => {
                let dtype = if dtype == "datetime" {
                    meerschaum_core::dtypes::pd_dtype("datetime").ok_or_else(denied)?
                } else {
                    dtype.as_str()
                };
                let value = meerschaum_kernel::cast::coerce_datetime(
                    value,
                    !dtype.to_lowercase().contains("utc"),
                );
                Ok(Bound::Datetime(value.utc.and_utc()))
            }
            _ => Err(denied()),
        }
    }
    fn position(&self, value: &Bound) -> Result<i128, ApiError> {
        match (self, value) {
            (Self::Integer { .. }, Bound::Int(value)) => Ok(i128::from(*value)),
            (Self::Datetime { .. }, Bound::Datetime(value)) => Ok(i128::from(value.timestamp())
                * 1_000_000_000
                + i128::from(value.timestamp_subsec_nanos())),
            _ => Err(denied()),
        }
    }
}

impl WindowRange {
    fn positions(&self, axis: &WindowAxis) -> Result<(Option<i128>, Option<i128>), ApiError> {
        let begin = self
            .begin
            .as_ref()
            .map(|value| axis.position(value))
            .transpose()?;
        let end = self
            .end
            .as_ref()
            .map(|value| axis.position(value))
            .transpose()?;
        if begin.zip(end).is_some_and(|(begin, end)| begin >= end) {
            return Err(denied());
        }
        Ok((begin, end))
    }
}

fn normalize(
    axis: &WindowAxis,
    mut ranges: Vec<WindowRange>,
) -> Result<Vec<WindowRange>, ApiError> {
    if ranges.is_empty() || ranges.len() > MAX_RANGES {
        return Err(denied());
    }
    for range in &ranges {
        range.positions(axis)?;
    }
    ranges.sort_by_key(|range| {
        range
            .begin
            .as_ref()
            .map(|value| axis.position(value).expect("validated bound"))
            .map_or((0, 0), |value| (1, value))
    });
    let mut merged: Vec<WindowRange> = Vec::new();
    for range in ranges {
        if let Some(last) = merged.last_mut() {
            let (_, last_end) = last.positions(axis)?;
            let (begin, end) = range.positions(axis)?;
            if last_end.is_none()
                || begin.is_none_or(|begin| last_end.is_some_and(|end| begin <= end))
            {
                if last_end.is_some() && (end.is_none() || end > last_end) {
                    last.end = range.end;
                }
                continue;
            }
        }
        merged.push(range);
    }
    Ok(merged)
}

fn permits(axis: &WindowAxis, ranges: &[WindowRange], query: &DataQuery) -> Result<(), ApiError> {
    let proposed = WindowRange {
        begin: query
            .begin
            .as_ref()
            .map(|value| axis.bound(value))
            .transpose()?,
        end: query
            .end
            .as_ref()
            .map(|value| axis.bound(value))
            .transpose()?,
    };
    let (begin, end) = proposed.positions(axis)?;
    for range in ranges {
        let (allowed_begin, allowed_end) = range.positions(axis)?;
        if allowed_begin.is_none_or(|allowed| begin.is_some_and(|begin| begin >= allowed))
            && allowed_end.is_none_or(|allowed| end.is_some_and(|end| end <= allowed))
        {
            return Ok(());
        }
    }
    Err(denied())
}

impl NodeWindow {
    /// Main's native predicate collector supplies this; public request proposals
    /// and registered-pipe metadata selectors are not row authority.
    pub fn with_accepted_rows(mut self, rows: super::AcceptedRows) -> Result<Self, ApiError> {
        rows.native()?;
        self.accepted_rows = rows;
        Ok(self)
    }
    pub(super) fn is_issued(&self) -> bool {
        matches!(self.state, State::Issued { .. }) && self.accepted_rows.issued()
    }
    pub fn not_issued(pipe: &Pipe, registration: Value, reason: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            reference: pipe.pipe_ref(),
            registration,
            guard: AxisGuard::capture(pipe),
            accepted_rows: super::AcceptedRows::Unissued,
            state: State::NotIssued {
                reason: reason.into().chars().take(256).collect(),
            },
        }
    }
    /// Only the Main native planner calls this; ranges have resolved Default/backtrack and
    /// original axis conversions. No callback-proposed bounds ever enter this constructor.
    pub fn from_main_plan(
        pipe: &Pipe,
        registration: Value,
        axis: WindowAxis,
        ranges: Vec<WindowRange>,
        coverage: WindowCoverage,
        replacement: bool,
    ) -> Result<Self, ApiError> {
        if registration.is_null() || pipe.pipe_ref().len() > 2048 {
            return Err(denied());
        }
        if coverage == WindowCoverage::WholeRelation
            && ranges
                != vec![WindowRange {
                    begin: None,
                    end: None,
                }]
            || matches!(axis, WindowAxis::WholeRelation)
                && coverage != WindowCoverage::WholeRelation
        {
            return Err(denied());
        }
        match &axis {
            WindowAxis::WholeRelation if pipe.column("datetime").is_none() => {}
            WindowAxis::Integer { column, .. }
                if pipe.column("datetime").as_deref() == Some(column)
                    && pipe.dtype(column).is_some_and(|dtype| {
                        meerschaum_core::dtypes::are_dtypes_equal(&dtype, "int")
                    }) => {}
            WindowAxis::Datetime { column, dtype }
                if pipe.column("datetime").as_deref() == Some(column)
                    && pipe
                        .dtype(column)
                        .is_some_and(|actual| axis_dtype(&actual) == axis_dtype(dtype))
                    && meerschaum_core::dtypes::are_dtypes_equal(dtype, "datetime") => {}
            _ => return Err(denied()),
        }
        let ranges = match axis {
            WindowAxis::WholeRelation => {
                if ranges
                    != vec![WindowRange {
                        begin: None,
                        end: None,
                    }]
                    || pipe.column("datetime").is_some()
                {
                    return Err(denied());
                }
                ranges
            }
            _ => normalize(&axis, ranges)?,
        };
        Ok(Self {
            id: Uuid::new_v4(),
            reference: pipe.pipe_ref(),
            registration,
            guard: AxisGuard::capture(pipe),
            // This constructor is used by Main's typed MCP roots, whose accepted
            // contract carries no row filter. Generic captures must supply theirs.
            accepted_rows: super::AcceptedRows::All,
            state: State::Issued {
                axis,
                coverage,
                source: ranges.clone(),
                read: ranges.clone(),
                write: ranges.clone(),
                replacement: replacement.then_some(ranges),
            },
        })
    }
    pub fn validate(&self, pipe: &Pipe, registration: &Value) -> Result<(), ApiError> {
        if self.id.is_nil()
            || self.reference != pipe.pipe_ref()
            || &self.registration != registration
            || !self.guard.matches(pipe)
            || !self.accepted_rows.issued()
        {
            return Err(denied());
        }
        match &self.state {
            State::Issued { .. } => Ok(()),
            State::NotIssued { .. } => Err(ApiError::unavailable(
                "Main has not issued a runnable window for this node.",
            )),
        }
    }
    pub fn source(&self, query: &DataQuery) -> Result<(), ApiError> {
        self.query(query, 0)
    }
    pub fn data_window(
        &self,
        binding: &CallbackBinding,
        issuer: &CallbackBinding,
    ) -> Result<meerschaum_connector::mutation::DataWindow, ApiError> {
        use meerschaum_connector::mutation::{DataAxis, DataCoverage, DataSpan, DataWindow};
        let State::Issued {
            axis,
            coverage,
            write,
            replacement,
            ..
        } = &self.state
        else {
            return Err(ApiError::unavailable(
                "Main has not issued this node's native write window.",
            ));
        };
        let axis = match axis {
            WindowAxis::WholeRelation => DataAxis::WholeRelation,
            WindowAxis::Integer { column, .. } => DataAxis::Integer {
                column: column.clone(),
            },
            WindowAxis::Datetime { column, .. } => DataAxis::Datetime {
                column: column.clone(),
            },
        };
        let spans = |ranges: &[WindowRange]| {
            ranges
                .iter()
                .map(|range| DataSpan {
                    begin: range.begin,
                    end: range.end,
                })
                .collect()
        };
        DataWindow::new(
            self.reference.clone(),
            self.seal(binding, issuer)?,
            axis,
            match coverage {
                WindowCoverage::WholeRelation => DataCoverage::WholeRelation,
                WindowCoverage::AxisRanges => DataCoverage::AxisRanges,
            },
            spans(write),
            replacement.as_ref().map(|ranges| spans(ranges)),
        )
        .and_then(|window| {
            window.with_row_predicate(
                self.accepted_rows
                    .native()
                    .map_err(|error| error.detail_text())?,
            )
        })
        .map_err(|reason| ApiError::new(403, reason))
    }
    pub fn read(&self, query: &DataQuery) -> Result<(), ApiError> {
        self.query(query, 1)
    }
    pub fn replacement(&self, query: &DataQuery) -> Result<(), ApiError> {
        self.query(query, 2)
    }
    fn query(&self, query: &DataQuery, mode: u8) -> Result<(), ApiError> {
        self.accepted_rows.request(query.params.as_ref())?;
        let State::Issued {
            axis,
            coverage,
            source,
            read,
            replacement,
            ..
        } = &self.state
        else {
            return Err(denied());
        };
        if *coverage == WindowCoverage::AxisRanges && query.begin.is_none() && query.end.is_none() {
            return Err(denied());
        }
        if matches!(axis, WindowAxis::WholeRelation) {
            return if query.begin.is_none()
                && query.end.is_none()
                && (mode != 2 || replacement.is_some())
            {
                Ok(())
            } else {
                Err(denied())
            };
        }
        let ranges = match mode {
            0 => source,
            1 => read,
            2 => replacement.as_ref().ok_or_else(denied)?,
            _ => return Err(denied()),
        };
        permits(axis, ranges, query)
    }
    /// Stable through a trusted non-axis inference CAS; it excludes mutable non-axis dtype
    /// metadata. Callback retirement/context revision still invalidates every old lease.
    pub fn seal(
        &self,
        binding: &CallbackBinding,
        issuer: &CallbackBinding,
    ) -> Result<String, ApiError> {
        if binding.grant.is_nil() || binding.launch.is_nil() || binding.context_revision == 0 {
            return Err(denied());
        }
        if issuer.grant.is_nil() || issuer.launch.is_nil() || issuer.context_revision == 0 {
            return Err(denied());
        }
        let bytes =
            serde_json::to_vec(&(binding, issuer, self)).map_err(|_| ApiError::internal())?;
        Ok(super::super::plugins::sha256(&bytes))
    }
    /// Called ONLY inside existing native-inference expected-old CAS. Ordinary peer changes
    /// never reach this path. Keep identity/ranges for non-axis changes; otherwise fail closed.
    pub fn inferred(&mut self, pipe: &Pipe, registration: &Value) -> Result<(), String> {
        if self.reference != pipe.pipe_ref() || &self.registration != registration {
            return Err("Native inference changed its registered node identity.".into());
        }
        if !self.guard.matches(pipe) || !self.accepted_rows.unchanged(pipe) {
            self.state=State::NotIssued {reason:"Native inference changed the node axis; Main must replan within the original request.".into()};
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerschaum_core::Map;

    #[test]
    fn legacy_missing_row_capture_is_not_all_and_filter_meaning_is_part_of_the_seal() {
        use std::collections::BTreeMap;
        let mut pipe = pipe();
        let mut dtypes = pipe.raw_parameters().get("dtypes").unwrap().to_map();
        dtypes.insert("kind".into(), "int64".into());
        let mut params = pipe.raw_parameters().clone();
        params.insert("dtypes".into(), Value::Map(dtypes));
        pipe.set_parameters(params);
        let base = NodeWindow::from_main_plan(
            &pipe,
            Value::Int(7),
            WindowAxis::Integer {
                column: "id".into(),
                explicit_unit: None,
            },
            vec![WindowRange {
                begin: Some(Bound::Int(5)),
                end: Some(Bound::Int(10)),
            }],
            WindowCoverage::AxisRanges,
            false,
        )
        .unwrap();
        let mut legacy = serde_json::to_value(&base).unwrap();
        legacy.as_object_mut().unwrap().remove("accepted_rows");
        let legacy: NodeWindow = serde_json::from_value(legacy).unwrap();
        assert!(legacy.validate(&pipe, &Value::Int(7)).is_err());
        let binding = CallbackBinding {
            grant: Uuid::new_v4(),
            launch: Uuid::new_v4(),
            context_revision: 1,
        };
        let filter = Map::from_iter([("kind".into(), Value::Int(2))]);
        let mut captured = base
            .clone()
            .with_accepted_rows(super::super::AcceptedRows::Filter {
                params: filter.clone(),
                columns: BTreeMap::from([(
                    "kind".into(),
                    super::super::RowColumn {
                        dtype: Some("int64".into()),
                        native_type: Some("INTEGER".into()),
                        literal_null_marker: false,
                    },
                )]),
            })
            .unwrap();
        assert_ne!(
            base.seal(&binding, &binding).unwrap(),
            captured.seal(&binding, &binding).unwrap()
        );
        assert!(captured.source(&query(Some(5), Some(10))).is_err());
        let mut filtered = query(Some(5), Some(10));
        filtered.params = Some(filter);
        captured.source(&filtered).unwrap();
        let mut dtypes = pipe.raw_parameters().get("dtypes").unwrap().to_map();
        dtypes.insert("kind".into(), "string".into());
        let mut params = pipe.raw_parameters().clone();
        params.insert("dtypes".into(), Value::Map(dtypes));
        pipe.set_parameters(params);
        captured.inferred(&pipe, &Value::Int(7)).unwrap();
        assert!(
            captured.validate(&pipe, &Value::Int(7)).is_err(),
            "Filtered nonaxis dtype changes preserved an old predicate"
        );
    }

    fn pipe() -> Pipe {
        Pipe::builder("sql:source", "accepted").instance("sql:main")
            .parameters(Value::from_json_str(r#"{"columns":{"datetime":"id"},"dtypes":{"id":"int64"},"access":{"owner":"alice","visibility":"private"}}"#).unwrap().to_map())
            .build().unwrap()
    }
    fn query(begin: Option<i64>, end: Option<i64>) -> DataQuery {
        DataQuery {
            begin: begin.map(DateBound::Int),
            end: end.map(DateBound::Int),
            ..Default::default()
        }
    }
    fn issued(pipe: &Pipe) -> NodeWindow {
        NodeWindow::from_main_plan(
            pipe,
            Value::Int(7),
            WindowAxis::Integer {
                column: "id".into(),
                explicit_unit: None,
            },
            vec![
                WindowRange {
                    begin: Some(Bound::Int(10)),
                    end: Some(Bound::Int(20)),
                },
                WindowRange {
                    begin: Some(Bound::Int(30)),
                    end: Some(Bound::Int(40)),
                },
            ],
            WindowCoverage::AxisRanges,
            true,
        )
        .unwrap()
    }
    #[test]
    fn source_and_read_ranges_cannot_omit_bounds_or_bridge_a_gap() {
        let pipe = pipe();
        let window = issued(&pipe);
        for requested in [query(Some(10), Some(20)), query(Some(32), Some(37))] {
            window.source(&requested).unwrap();
            window.read(&requested).unwrap();
            window.replacement(&requested).unwrap();
        }
        for requested in [
            query(None, Some(20)),
            query(Some(10), None),
            query(Some(12), Some(35)),
            query(Some(9), Some(20)),
            query(Some(10), Some(21)),
            query(Some(20), Some(20)),
        ] {
            assert!(window.source(&requested).is_err());
            assert!(window.read(&requested).is_err());
            assert!(window.replacement(&requested).is_err());
        }
        let pending =
            NodeWindow::not_issued(&pipe, Value::Int(7), "Waiting for proven parent commits.");
        assert!(pending.source(&query(None, None)).is_err());
        assert!(pending.validate(&pipe, &Value::Int(7)).is_err());
    }
    #[test]
    fn trusted_non_axis_inference_preserves_page_seal_but_axis_or_launch_changes_do_not() {
        let mut pipe = pipe();
        let mut window = issued(&pipe);
        let binding = CallbackBinding {
            grant: Uuid::new_v4(),
            launch: Uuid::new_v4(),
            context_revision: 1,
        };
        let before = window.seal(&binding, &binding).unwrap();
        let mut parameters = pipe.raw_parameters().clone();
        parameters
            .get_mut("dtypes")
            .and_then(Value::as_map_mut)
            .unwrap()
            .insert("payload".into(), Value::from("bytes"));
        pipe.set_parameters(parameters);
        window.inferred(&pipe, &Value::Int(7)).unwrap();
        window.validate(&pipe, &Value::Int(7)).unwrap();
        assert_eq!(before, window.seal(&binding, &binding).unwrap());
        assert_ne!(
            before,
            window
                .seal(
                    &CallbackBinding {
                        context_revision: 2,
                        ..binding.clone()
                    },
                    &binding
                )
                .unwrap()
        );
        assert_ne!(
            before,
            window
                .seal(
                    &CallbackBinding {
                        grant: Uuid::new_v4(),
                        ..binding.clone()
                    },
                    &binding
                )
                .unwrap()
        );
        assert!(window.validate(&pipe, &Value::Int(8)).is_err());
        let mut parameters = pipe.raw_parameters().clone();
        parameters.insert(
            "columns".into(),
            Value::Map(Map::from_iter([("datetime".into(), Value::from("other"))])),
        );
        pipe.set_parameters(parameters);
        window.inferred(&pipe, &Value::Int(7)).unwrap();
        assert!(window.validate(&pipe, &Value::Int(7)).is_err());
        assert!(window.source(&query(Some(10), Some(20))).is_err());
    }
    #[test]
    fn wrong_axis_and_unproven_whole_relation_are_refused_at_issuance() {
        let pipe = pipe();
        let ranges = vec![WindowRange {
            begin: None,
            end: None,
        }];
        assert!(
            NodeWindow::from_main_plan(
                &pipe,
                Value::Int(7),
                WindowAxis::WholeRelation,
                ranges.clone(),
                WindowCoverage::WholeRelation,
                true
            )
            .is_err()
        );
        assert!(
            NodeWindow::from_main_plan(
                &pipe,
                Value::Int(7),
                WindowAxis::Integer {
                    column: "foreign".into(),
                    explicit_unit: None
                },
                ranges,
                WindowCoverage::AxisRanges,
                true
            )
            .is_err()
        );
        let window = issued(&pipe);
        let dated = DataQuery {
            begin: DateBound::parse("2026-01-01T00:00:00Z"),
            end: DateBound::parse("2026-01-02T00:00:00Z"),
            ..Default::default()
        };
        assert!(window.source(&dated).is_err());
    }
    #[test]
    fn native_whole_relation_is_explicit_and_not_inferred_from_merged_range_bounds() {
        let pipe = pipe();
        let scalar = NodeWindow::from_main_plan(
            &pipe,
            Value::Int(7),
            WindowAxis::Integer {
                column: "id".into(),
                explicit_unit: None,
            },
            vec![
                WindowRange {
                    begin: None,
                    end: Some(Bound::Int(20)),
                },
                WindowRange {
                    begin: Some(Bound::Int(20)),
                    end: None,
                },
            ],
            WindowCoverage::AxisRanges,
            false,
        )
        .unwrap();
        assert!(scalar.source(&query(None, None)).is_err());
        scalar.source(&query(Some(0), Some(40))).unwrap();
        let whole = NodeWindow::from_main_plan(
            &pipe,
            Value::Int(7),
            WindowAxis::Integer {
                column: "id".into(),
                explicit_unit: None,
            },
            vec![WindowRange {
                begin: None,
                end: None,
            }],
            WindowCoverage::WholeRelation,
            false,
        )
        .unwrap();
        whole.source(&query(None, None)).unwrap();
    }
}
