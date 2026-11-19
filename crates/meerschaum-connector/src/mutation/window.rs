//! Process-local Main-issued row constraints. No serde, environment,
//! request flags or worker-supplied descriptor can select this capability.

use super::super::{DateBound, SyncOptions};
use meerschaum_core::Map;
use meerschaum_core::{Pipe, success::Bound};
use meerschaum_kernel::{Frame, Value};
use std::collections::BTreeMap;

const MAX_SPANS: usize = 1024;
const MAX_ROWS: usize = 1_000_000;

/// Main's captured interpretation of an accepted row predicate column. It is not
/// inferred from a child frame or a transport-provided dtype.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowColumn {
    pub dtype: Option<String>,
    pub native_type: Option<String>,
    pub literal_null_marker: bool,
}
impl RowColumn {
    pub fn supports_integer_equality(&self) -> bool {
        let dtype = self
            .dtype
            .as_deref()
            .unwrap_or_default()
            .to_ascii_lowercase();
        let native = self
            .native_type
            .as_deref()
            .unwrap_or_default()
            .to_ascii_uppercase();
        matches!(dtype.as_str(), "int" | "int8" | "int16" | "int32" | "int64")
            && matches!(
                native.as_str(),
                "INTEGER" | "INT" | "BIGINT" | "SMALLINT" | "TINYINT"
            )
            && !self.literal_null_marker
    }
}
/// Process-local accepted row selection. Serialized legacy defaults live in the
/// Main NodeWindow model; missing evidence cannot mean an unbounded predicate.
#[derive(Clone, Debug, PartialEq)]
pub enum RowPredicate {
    Unissued,
    All,
    Filter {
        params: Map,
        columns: BTreeMap<String, RowColumn>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataAxis {
    WholeRelation,
    Integer { column: String },
    Datetime { column: String },
}
/// Issued by Main from actual native predicates, never inferred from peer omissions or
/// a normalized union of complementary predicates (which still exclude NULL axes).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataCoverage {
    WholeRelation,
    AxisRanges,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataSpan {
    pub begin: Option<Bound>,
    pub end: Option<Bound>,
}

/// A process-local mapping of two actual registered objects from a trusted native
/// registry. This type has no wire, environment or job-property decoder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeTarget {
    logical: String,
    physical: String,
    registration: String,
}
impl NativeTarget {
    /// The host must first prove the captured native connector and both original and
    /// configured-service authority. Matching arbitrary URLs or strings is insufficient.
    pub fn from_registered(logical: &Pipe, physical: &Pipe) -> Result<Self, String> {
        let registration = logical.registration_id().ok_or_else(refused)?;
        if physical.registration_id().as_ref() != Some(&registration)
            || logical.connector_keys() != physical.connector_keys()
            || logical.metric_key() != physical.metric_key()
            || logical.location_key() != physical.location_key()
            || logical.raw_parameters() != physical.raw_parameters()
            || !logical.instance_keys().starts_with("api:")
            || !physical.instance_keys().starts_with("sql:")
        {
            return Err(refused());
        }
        Ok(Self {
            logical: logical.pipe_ref(),
            physical: physical.pipe_ref(),
            registration,
        })
    }
    pub fn logical_reference(&self) -> &str {
        &self.logical
    }
    pub fn physical_reference(&self) -> &str {
        &self.physical
    }
    pub fn registration(&self) -> &str {
        &self.registration
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct DataWindow {
    reference: String,
    proof: String,
    axis: DataAxis,
    coverage: DataCoverage,
    rows: Vec<DataSpan>,
    positions: Vec<(Option<i128>, Option<i128>)>,
    replacement: Option<Vec<DataSpan>>,
    native_target: Option<NativeTarget>,
    rows_predicate: RowPredicate,
}
fn refused() -> String {
    "This batch exceeds its Main-issued node window; no rows from this batch were admitted.".into()
}
fn position(axis: &DataAxis, bound: &Bound) -> Result<i128, String> {
    match (axis, bound) {
        (DataAxis::Integer { .. }, Bound::Int(value)) => Ok(i128::from(*value)),
        (DataAxis::Datetime { .. }, Bound::Datetime(value)) => Ok(i128::from(value.timestamp())
            * 1_000_000_000
            + i128::from(value.timestamp_subsec_nanos())),
        _ => Err(refused()),
    }
}
fn range(axis: &DataAxis, span: &DataSpan) -> Result<(Option<i128>, Option<i128>), String> {
    let begin = span
        .begin
        .as_ref()
        .map(|bound| position(axis, bound))
        .transpose()?;
    let end = span
        .end
        .as_ref()
        .map(|bound| position(axis, bound))
        .transpose()?;
    if begin.zip(end).is_some_and(|(begin, end)| begin >= end) {
        return Err(refused());
    }
    Ok((begin, end))
}
fn concrete(bound: &DateBound) -> Bound {
    match bound {
        DateBound::Int(value) => Bound::Int(*value),
        DateBound::Datetime(value) => Bound::Datetime(value.utc.and_utc()),
    }
}
impl DataWindow {
    /// Only trusted Main adapter code constructs this from its canonical issued window.
    pub fn new(
        reference: String,
        proof: String,
        axis: DataAxis,
        coverage: DataCoverage,
        rows: Vec<DataSpan>,
        replacement: Option<Vec<DataSpan>>,
    ) -> Result<Self, String> {
        if reference.len() > 2048
            || reference.split('|').count() != 4
            || proof.len() != 64
            || !proof.bytes().all(|byte| byte.is_ascii_hexdigit())
            || rows.is_empty()
            || rows.len() > MAX_SPANS
        {
            return Err(refused());
        }
        if coverage == DataCoverage::WholeRelation
            && rows
                != vec![DataSpan {
                    begin: None,
                    end: None,
                }]
            || axis == DataAxis::WholeRelation && coverage != DataCoverage::WholeRelation
        {
            return Err(refused());
        }
        for spans in [Some(&rows), replacement.as_ref()].into_iter().flatten() {
            if spans.is_empty() || spans.len() > MAX_SPANS {
                return Err(refused());
            }
            for span in spans {
                if axis == DataAxis::WholeRelation {
                    if span.begin.is_some() || span.end.is_some() {
                        return Err(refused());
                    }
                } else {
                    range(&axis, span)?;
                }
            }
        }
        let positions = if axis == DataAxis::WholeRelation {
            Vec::new()
        } else {
            let positions = rows
                .iter()
                .map(|span| range(&axis, span))
                .collect::<Result<Vec<_>, _>>()?;
            if positions
                .windows(2)
                .any(|pair| match (pair[0].1, pair[1].0) {
                    (Some(end), Some(begin)) => end > begin,
                    _ => true,
                })
            {
                return Err(refused());
            }
            positions
        };
        Ok(Self {
            reference,
            proof,
            axis,
            coverage,
            rows,
            positions,
            replacement,
            native_target: None,
            rows_predicate: RowPredicate::All,
        })
    }
    pub fn reference(&self) -> &str {
        &self.reference
    }
    /// The trusted Main issuer supplies its exact accepted predicate/interpretation.
    pub fn with_row_predicate(mut self, predicate: RowPredicate) -> Result<Self, String> {
        if let RowPredicate::Filter { params, columns } = &predicate
            && (params.is_empty()
                || params.len() > 256
                || params.len() != columns.len()
                || params.keys().any(|column| {
                    column.is_empty()
                        || column.len() > 2048
                        || column.chars().any(char::is_control)
                        || !columns.contains_key(column)
                })
                || columns.values().any(|column| {
                    column.native_type.as_ref().is_none_or(String::is_empty)
                        || [column.dtype.as_deref(), column.native_type.as_deref()]
                            .into_iter()
                            .flatten()
                            .any(|value| value.len() > 256 || value.chars().any(char::is_control))
                })
                || meerschaum_core::Value::Map(params.clone())
                    .to_json_string()
                    .len()
                    > 64 * 1024)
        {
            return Err(refused());
        }
        self.rows_predicate = predicate;
        Ok(self)
    }
    pub fn row_predicate(&self) -> &RowPredicate {
        &self.rows_predicate
    }
    /// Bind this logical window to an actual host-proven native registered target.
    pub fn for_native_target(&self, target: &NativeTarget) -> Result<Self, String> {
        if self.reference != target.logical || self.native_target.is_some() {
            return Err(refused());
        }
        let mut window = self.clone();
        window.native_target = Some(target.clone());
        Ok(window)
    }
    /// Preserve logical source/window identity while comparing fresh canonical issuance.
    pub fn logical_window(&self) -> Self {
        let mut window = self.clone();
        window.native_target = None;
        window
    }
    pub fn native_reference(&self) -> &str {
        self.native_target
            .as_ref()
            .map_or(&self.reference, |target| &target.physical)
    }
    pub fn native_target(&self) -> Option<&NativeTarget> {
        self.native_target.as_ref()
    }
    pub fn proof(&self) -> &str {
        &self.proof
    }
    pub fn axis(&self) -> &DataAxis {
        &self.axis
    }
    pub fn covers_relation(&self) -> bool {
        self.coverage == DataCoverage::WholeRelation
    }
    pub fn spans(&self) -> &[DataSpan] {
        &self.rows
    }
    pub fn replacement_spans(&self) -> Option<&[DataSpan]> {
        self.replacement.as_deref()
    }
    pub fn column(&self) -> Option<&str> {
        match &self.axis {
            DataAxis::WholeRelation => None,
            DataAxis::Integer { column } | DataAxis::Datetime { column } => Some(column),
        }
    }
    pub fn contains_bound(&self, bound: &Bound) -> Result<bool, String> {
        if self.covers_relation() {
            return Ok(true);
        }
        let value = position(&self.axis, bound)?;
        let next = self
            .positions
            .partition_point(|(begin, _)| begin.is_none_or(|begin| begin <= value));
        Ok(next
            .checked_sub(1)
            .and_then(|index| self.positions.get(index))
            .is_some_and(|(_, end)| end.is_none_or(|end| value < end)))
    }
    /// Only typed native values reach this method, after the canonical dtype cast and
    /// host-selected autotime assignment. Missing/null/unknown generated axes refuse.
    pub fn validate_frame(
        &self,
        pipe: &Pipe,
        frame: &Frame,
        options: &SyncOptions,
    ) -> Result<(), String> {
        if pipe.pipe_ref() != self.native_reference() || frame.height() > MAX_ROWS {
            return Err(refused());
        }
        if matches!(self.rows_predicate, RowPredicate::Unissued) {
            return Err("Main has not issued this node's accepted row-selection predicate.".into());
        }
        if let RowPredicate::Filter { params, columns } = &self.rows_predicate {
            for (name, expected) in params {
                let column = columns.get(name).ok_or_else(refused)?;
                let meerschaum_core::Value::Int(expected) = expected else {
                    return Err(refused());
                };
                if !column.supports_integer_equality() || pipe.dtype(name) != column.dtype {
                    return Err(refused());
                }
                if frame.height() > 0 {
                    let values = frame.column(name).ok_or_else(refused)?;
                    if (0..frame.height()).any(|row| {
                        !matches!(values.data.value_ref(row).as_ref(), Value::Int(actual) if actual == expected)
                    }) {
                        return Err(refused());
                    }
                }
            }
        }
        self.validate_replacement(options)?;
        if self.axis == DataAxis::WholeRelation {
            return Ok(());
        }
        let column = self.column().ok_or_else(refused)?;
        if pipe.column("datetime").as_deref() != Some(column) {
            return Err(refused());
        }
        if frame.height() == 0 {
            return Ok(());
        }
        let primary_axis = pipe.column("primary").as_deref() == Some(column);
        // With an actual Main no-predicate plan, a nullable non-generated axis is not a
        // range restriction. Native preflight still rejects DEFAULT/generated layouts.
        if self.covers_relation() && !primary_axis {
            return Ok(());
        }
        let values=frame.column(column).ok_or_else(||
            "The native axis is absent or database-generated. This window requires controlled native allocation before writing; retry with explicit in-range axis values.".to_string())?;
        for row in 0..frame.height() {
            let bound=match (&self.axis,values.data.value_ref(row).as_ref()){
                (DataAxis::Integer{..},Value::Int(value))=>Bound::Int(*value),
                (DataAxis::Datetime{..},Value::Datetime(value))=>Bound::Datetime(value.utc.and_utc()),
                _=>return Err("The normalized axis is null or incompatible. No part of this mixed batch is admitted; database-generated values need a controlled allocation adapter.".into()),
            };
            if !self.contains_bound(&bound)? {
                return Err(refused());
            }
        }
        Ok(())
    }
    /// Exact DELETE bounds must come from the same native options the actual builder
    /// consumes. A frame's extents alone never authorize a larger replace deletion.
    pub fn validate_replacement(&self, options: &SyncOptions) -> Result<(), String> {
        if !options.replace {
            return Ok(());
        }
        if let RowPredicate::Filter { params, .. } = &self.rows_predicate
            && options.params.as_ref() != Some(params)
        {
            return Err(refused());
        }
        let allowed = self
            .replacement
            .as_ref()
            .ok_or_else(|| "Main did not admit replacement for this node.".to_string())?;
        if self.axis == DataAxis::WholeRelation {
            return if options.begin.is_none() && options.end.is_none() {
                Ok(())
            } else {
                Err(refused())
            };
        }
        let proposed = DataSpan {
            begin: options.begin.as_ref().map(concrete),
            end: options.end.as_ref().map(concrete),
        };
        if !self.covers_relation() && proposed.begin.is_none() && proposed.end.is_none() {
            return Err(refused());
        }
        let (begin, end) = range(&self.axis, &proposed)?;
        for span in allowed {
            let (low, high) = range(&self.axis, span)?;
            if low.is_none_or(|low| begin.is_some_and(|begin| begin >= low))
                && high.is_none_or(|high| end.is_some_and(|end| end <= high))
            {
                return Ok(());
            }
        }
        Err(refused())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerschaum_core::Value as CoreValue;
    use meerschaum_kernel::Column;
    fn pipe() -> Pipe {
        Pipe::builder("sql:source","window").instance("sql:main")
        .parameters(CoreValue::from_json_str(r#"{"columns":{"datetime":"id","primary":"id"},"dtypes":{"id":"int64"},"autoincrement":true}"#).unwrap().to_map()).build().unwrap()
    }
    fn window(pipe: &Pipe) -> DataWindow {
        DataWindow::new(
            pipe.pipe_ref(),
            "a".repeat(64),
            DataAxis::Integer {
                column: "id".into(),
            },
            DataCoverage::AxisRanges,
            vec![DataSpan {
                begin: Some(Bound::Int(10)),
                end: Some(Bound::Int(20)),
            }],
            Some(vec![DataSpan {
                begin: Some(Bound::Int(10)),
                end: Some(Bound::Int(20)),
            }]),
        )
        .unwrap()
    }
    #[test]
    fn normalized_mixed_outlier_is_rejected_as_a_whole_and_generated_axes_never_assumed() {
        let pipe = pipe();
        let window = window(&pipe);
        let options = SyncOptions::default();
        let valid = Frame::new(vec![Column::object(
            "id",
            vec![Value::Int(10), Value::Int(19)],
        )])
        .unwrap();
        window.validate_frame(&pipe, &valid, &options).unwrap();
        for values in [
            vec![Value::Int(12), Value::Int(20)],
            vec![Value::Int(12), Value::Null],
            vec![Value::Int(12), Value::Str("13".into())],
        ] {
            let mixed = Frame::new(vec![Column::object("id", values)]).unwrap();
            assert!(window.validate_frame(&pipe, &mixed, &options).is_err());
        }
        let generated = Frame::new(vec![Column::object("payload", vec![Value::Int(1)])]).unwrap();
        assert!(window.validate_frame(&pipe, &generated, &options).is_err());
        let replace = SyncOptions {
            replace: true,
            begin: Some(DateBound::Int(10)),
            end: Some(DateBound::Int(20)),
            ..options.clone()
        };
        window.validate_frame(&pipe, &valid, &replace).unwrap();
        for (begin, end) in [
            (None, Some(DateBound::Int(20))),
            (Some(DateBound::Int(9)), Some(DateBound::Int(20))),
            (Some(DateBound::Int(10)), Some(DateBound::Int(21))),
        ] {
            let too_wide = SyncOptions {
                begin,
                end,
                ..replace.clone()
            };
            assert!(window.validate_frame(&pipe, &valid, &too_wide).is_err());
        }
    }
    #[test]
    fn main_no_predicate_allows_nullable_axis_but_range_union_never_mints_that_authority() {
        let mut pipe = pipe();
        pipe.set_parameters(
            CoreValue::from_json_str(r#"{"columns":{"datetime":"id"},"dtypes":{"id":"int64"}}"#)
                .unwrap()
                .to_map(),
        );
        let rows = vec![DataSpan {
            begin: None,
            end: None,
        }];
        let whole = DataWindow::new(
            pipe.pipe_ref(),
            "a".repeat(64),
            DataAxis::Integer {
                column: "id".into(),
            },
            DataCoverage::WholeRelation,
            rows.clone(),
            Some(rows.clone()),
        )
        .unwrap();
        let scalar_union = DataWindow::new(
            pipe.pipe_ref(),
            "b".repeat(64),
            DataAxis::Integer {
                column: "id".into(),
            },
            DataCoverage::AxisRanges,
            rows.clone(),
            Some(rows),
        )
        .unwrap();
        let nullable = Frame::new(vec![Column::object(
            "id",
            vec![Value::Null, Value::Int(12)],
        )])
        .unwrap();
        whole
            .validate_frame(&pipe, &nullable, &SyncOptions::default())
            .unwrap();
        assert!(
            scalar_union
                .validate_frame(&pipe, &nullable, &SyncOptions::default())
                .is_err()
        );
        let deletion = SyncOptions {
            replace: true,
            ..Default::default()
        };
        whole.validate_replacement(&deletion).unwrap();
        assert!(scalar_union.validate_replacement(&deletion).is_err());
        let primary = super::tests::pipe();
        let whole_primary = DataWindow::new(
            primary.pipe_ref(),
            "c".repeat(64),
            DataAxis::Integer {
                column: "id".into(),
            },
            DataCoverage::WholeRelation,
            vec![DataSpan {
                begin: None,
                end: None,
            }],
            None,
        )
        .unwrap();
        assert!(
            whole_primary
                .validate_frame(&primary, &nullable, &SyncOptions::default())
                .is_err(),
            "An unknown DB-generated primary axis was adopted from no-predicate authority."
        );
    }
}
