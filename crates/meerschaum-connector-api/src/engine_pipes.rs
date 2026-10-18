//! The Syncing Engine's state pipes over the API (§5.8, R2): the wire shared by the
//! `APIConnector` (which routes `mrsm:engine_*` pipes to `/engine/pipes/{metric}/*`) and the
//! server (which keeps them on its own instance).
//!
//! Rows travel as `orient='records'` JSON ([`crate::wire::to_json`]); both ends rebuild frames
//! with the state pipe's static dtypes ([`engine_frame`]), so no dtype inference query runs.

use indexmap::IndexMap;
use meerschaum_connector::pipe_ops::{Doc, engine_state_parameters};
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::enforce::{PipeDtypesContext, PipeInput, pipe_enforce_dtypes};
use serde::{Deserialize, Serialize};

use crate::wire::{ToJsonOptions, to_json};

/// Whether `metric_key` names one of the engine's state pipes (`engine_state`, `engine_ledger`,
/// `engine_cursors`, `engine_runs`, `engine_nodes`, `engine_events`).
pub fn is_engine_metric(metric_key: &str) -> bool {
    engine_state_parameters(metric_key).is_some()
}

/// Whether `pipe` is an engine state pipe (`mrsm:engine_*`).
pub fn is_engine_pipe(pipe: &Pipe) -> bool {
    pipe.connector_keys() == "mrsm" && is_engine_metric(pipe.metric_key())
}

/// Body of `POST /engine/pipes/{metric}/read` and `/clear`: the `get_pipe_data` /
/// `clear_pipe` query of a state pipe.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EnginePipeQuery {
    /// `params` (a JSON object; `None` = every row).
    #[serde(default)]
    pub params: Option<serde_json::Value>,
    /// `select_columns` (read only).
    #[serde(default)]
    pub select_columns: Option<Vec<String>>,
    /// The lower datetime bound (ISO), inclusive (read only).
    #[serde(default)]
    pub begin: Option<String>,
    /// The upper datetime bound (ISO), exclusive.
    #[serde(default)]
    pub end: Option<String>,
}

impl EnginePipeQuery {
    /// The `params` as a parameters map.
    pub fn params_map(&self) -> Option<Map> {
        self.params
            .as_ref()
            .and_then(|j| Value::from_json_str(&j.to_string()).ok())
            .map(|v| v.to_map())
    }

    /// The query of a `get_pipe_data` / `clear_pipe` call on a state pipe.
    pub fn from_data_query(query: &meerschaum_connector::DataQuery) -> EnginePipeQuery {
        EnginePipeQuery {
            params: Self::json_params(&query.params),
            select_columns: query.select_columns.clone(),
            begin: query
                .begin
                .as_ref()
                .map(meerschaum_connector::DateBound::isoformat),
            end: query
                .end
                .as_ref()
                .map(meerschaum_connector::DateBound::isoformat),
        }
    }

    /// `params` from a parameters map.
    pub fn json_params(params: &Option<Map>) -> Option<serde_json::Value> {
        params
            .as_ref()
            .and_then(|m| serde_json::to_value(Value::Map(m.clone())).ok())
    }
}

/// The static enforcement context of a state pipe (its configured dtypes; no instance query).
pub fn engine_dtypes_context(metric_key: &str) -> Option<PipeDtypesContext> {
    let params = engine_state_parameters(metric_key)?;
    let dtypes: IndexMap<String, String> = match params.get("dtypes") {
        Some(Value::Map(m)) => m
            .iter()
            .map(|(k, v)| (k.clone(), meerschaum_core::pyfmt::py_str(v)))
            .collect(),
        _ => IndexMap::new(),
    };
    Some(PipeDtypesContext {
        explicit_dtypes: dtypes.clone(),
        dtypes,
        enforce: true,
        tz_aware: true,
        mixed_numerics: true,
    })
}

/// Rows of a state pipe as a frame with its static dtypes. Columns the pipe does not declare
/// are dropped, so a client can never add columns to a state table.
pub fn engine_frame(metric_key: &str, docs: Vec<Doc>) -> Result<Frame, String> {
    let ctx = engine_dtypes_context(metric_key)
        .ok_or_else(|| format!("'{metric_key}' is not an engine state pipe."))?;
    let docs = docs
        .into_iter()
        .map(|d| {
            d.into_iter()
                .filter(|(k, _)| ctx.dtypes.contains_key(k))
                .collect()
        })
        .collect();
    pipe_enforce_dtypes(PipeInput::Docs(docs), &ctx).map_err(|e| e.to_string())
}

/// A frame as `orient='records'` JSON (ISO datetimes, JSON columns as JSON).
pub fn records_json(df: &Frame) -> String {
    to_json(df, None, &ToJsonOptions::default())
}

/// `orient='records'` JSON rows as documents.
pub fn docs_from_records(items: &[serde_json::Value]) -> Vec<Doc> {
    items
        .iter()
        .filter_map(serde_json::Value::as_object)
        .map(|o| {
            o.iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        meerschaum_kernel::frame::json_to_value(v.clone()),
                    )
                })
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerschaum_kernel::value::{DateTimeValue, Value as KValue};

    #[test]
    fn rows_round_trip_through_records() {
        let created = chrono::NaiveDate::from_ymd_opt(2026, 1, 2)
            .unwrap()
            .and_hms_opt(3, 4, 5)
            .unwrap();
        let doc: Doc = [
            ("created", KValue::Datetime(DateTimeValue::utc(created))),
            ("pipe_ref", KValue::Str("api:x|a|b|None".into())),
            ("seq", KValue::Int(7)),
            (
                "begin",
                KValue::Json(serde_json::json!({"datetime": "2026-01-01T00:00:00+00:00"})),
            ),
            ("end", KValue::Null),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        let df = engine_frame("engine_ledger", vec![doc]).unwrap();
        let json: serde_json::Value = serde_json::from_str(&records_json(&df)).unwrap();
        let back =
            engine_frame("engine_ledger", docs_from_records(json.as_array().unwrap())).unwrap();
        assert_eq!(
            meerschaum_connector::pipe_ops::frame_to_docs(&back),
            meerschaum_connector::pipe_ops::frame_to_docs(&df)
        );
        assert!(is_engine_metric("engine_runs"));
        assert!(!is_engine_metric("users"));
    }

    #[test]
    fn undeclared_columns_are_dropped() {
        let doc: Doc = [
            ("pipe_ref", KValue::Str("p".into())),
            ("definition_hash", KValue::Str("h".into())),
            ("evil", KValue::Str("x".into())),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        let df = engine_frame("engine_state", vec![doc]).unwrap();
        assert!(!df.names().iter().any(|n| n == "evil"));
        assert!(df.names().iter().any(|n| n == "definition_hash"));
    }

    #[test]
    fn query_params_round_trip() {
        let mut params = Map::new();
        params.insert("pipe_ref".into(), Value::Str("p".into()));
        let q = EnginePipeQuery {
            params: EnginePipeQuery::json_params(&Some(params.clone())),
            ..Default::default()
        };
        let j = serde_json::to_string(&q).unwrap();
        let back: EnginePipeQuery = serde_json::from_str(&j).unwrap();
        assert_eq!(back.params_map(), Some(params));
    }
}
