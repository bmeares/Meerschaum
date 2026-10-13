//! Fetch results as they cross the boundary.
//!
//! v4's fetch contract accepts a DataFrame, a GeoDataFrame, a list of dicts, a dict of lists, a
//! JSON (or simple-lines) string, or a generator of any of these. DataFrames arrive as Arrow
//! ([`FetchChunk::Frame`]); the other shapes arrive as typed values so the kernel applies v4's
//! own `parse_df_datetimes` / `enforce_dtypes` rules through [`PipeInput`].

use indexmap::IndexMap;
use meerschaum_kernel::Frame;
use meerschaum_kernel::enforce::PipeInput;
use meerschaum_kernel::value::Value as KValue;
use rmpv::Value as Wire;

use crate::error::{WorkerError, WorkerResult};
use crate::protocol::{ipc_to_frame, wire_to_columns, wire_to_docs};

/// A document: column name to cell value.
pub type Doc = IndexMap<String, KValue>;

/// One chunk of a fetch result (or the data of a forwarded `pipe.sync()` call).
#[derive(Debug, Clone)]
pub enum FetchChunk {
    /// A DataFrame / GeoDataFrame / Polars frame (Arrow).
    Frame(Frame),
    /// A list of dicts.
    Docs(Vec<Doc>),
    /// A dict of lists.
    Columns(IndexMap<String, Vec<KValue>>),
    /// A JSON or simple-lines string.
    Text(String),
}

impl FetchChunk {
    /// Decode a chunk from its wire `kind`, `value` and data frames.
    pub(crate) fn from_wire(
        kind: &str,
        value: &Wire,
        frames: &mut Vec<Vec<u8>>,
    ) -> WorkerResult<Option<FetchChunk>> {
        Ok(Some(match kind {
            "arrow" => {
                if frames.is_empty() {
                    return Err(WorkerError::Protocol(
                        "an arrow chunk arrived without a data frame".into(),
                    ));
                }
                FetchChunk::Frame(ipc_to_frame(&frames.remove(0))?)
            }
            "docs" => FetchChunk::Docs(wire_to_docs(value)?),
            "columns" => FetchChunk::Columns(wire_to_columns(value)?),
            "text" => FetchChunk::Text(value.as_str().unwrap_or_default().to_string()),
            "none" | "infer" => return Ok(None),
            other => {
                return Err(WorkerError::Protocol(format!(
                    "unknown chunk kind '{other}'"
                )));
            }
        }))
    }

    /// The number of rows (lines for text).
    pub fn len(&self) -> usize {
        match self {
            FetchChunk::Frame(f) => f.height(),
            FetchChunk::Docs(d) => d.len(),
            FetchChunk::Columns(c) => c.values().map(Vec::len).max().unwrap_or(0),
            FetchChunk::Text(t) => t.lines().filter(|l| !l.trim().is_empty()).count(),
        }
    }

    /// Whether the chunk has no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The chunk as the input of `Pipe.enforce_dtypes()` (`pipe_enforce_dtypes`).
    ///
    /// Text follows `Pipe.enforce_dtypes`: a string starting with `{` or `[` is JSON
    /// (`pd.read_json`), anything else simple lines (`parse_simple_lines`, one `string_to_dict`
    /// per line).
    pub fn into_pipe_input(self) -> WorkerResult<PipeInput> {
        Ok(match self {
            FetchChunk::Frame(f) => PipeInput::Frame(f),
            FetchChunk::Docs(d) => PipeInput::Docs(d),
            FetchChunk::Columns(c) => PipeInput::Columns(c),
            FetchChunk::Text(t) => text_to_input(&t)?,
        })
    }

    /// The chunk as a frame without pipe context (no datetime parsing beyond the kernel's
    /// `convert_dtypes`). Prefer [`FetchChunk::into_pipe_input`] when syncing.
    pub fn into_frame(self) -> WorkerResult<Frame> {
        let input = self.into_pipe_input()?;
        let built = match input {
            PipeInput::Frame(f) => Ok(f),
            PipeInput::Docs(d) => meerschaum_kernel::docs::frame_from_docs(&d),
            PipeInput::Columns(c) => meerschaum_kernel::docs::frame_from_columns(&c),
        };
        built.map_err(|e| WorkerError::Protocol(format!("cannot build a frame: {e}")))
    }
}

/// Parse a text payload the way `Pipe.enforce_dtypes` does for strings.
pub fn text_to_input(text: &str) -> WorkerResult<PipeInput> {
    let trimmed = text.trim();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        // PARITY: `pd.read_json` also infers dates for columns with date-like names
        // (`convert_dates`); the kernel's `parse_df_datetimes` applies its ISO-pattern rule
        // instead (core/Pipe/_dtypes.py:72).
        let json: serde_json::Value = serde_json::from_str(trimmed)
            .map_err(|e| WorkerError::Protocol(format!("invalid JSON text: {e}")))?;
        return json_to_input(json);
    }
    let mut docs = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let map = meerschaum_core::pyfmt::string_to_dict(line).map_err(|e| {
            WorkerError::Protocol(format!("Cannot parse simple lines into a dataframe: {e}"))
        })?;
        docs.push(
            map.iter()
                .map(|(k, v)| (k.clone(), KValue::from(v)))
                .collect::<Doc>(),
        );
    }
    Ok(PipeInput::Docs(docs))
}

fn json_cell(v: serde_json::Value) -> KValue {
    meerschaum_kernel::frame::json_to_value(v)
}

fn json_to_input(json: serde_json::Value) -> WorkerResult<PipeInput> {
    match json {
        serde_json::Value::Array(items) => {
            let docs = items
                .into_iter()
                .map(|item| match item {
                    serde_json::Value::Object(m) => {
                        Ok(m.into_iter().map(|(k, v)| (k, json_cell(v))).collect())
                    }
                    other => Err(WorkerError::Protocol(format!(
                        "each JSON record must be an object, got {other}"
                    ))),
                })
                .collect::<WorkerResult<Vec<Doc>>>()?;
            Ok(PipeInput::Docs(docs))
        }
        serde_json::Value::Object(m) => {
            let mut cols: IndexMap<String, Vec<KValue>> = IndexMap::new();
            for (k, v) in m {
                let values = match v {
                    serde_json::Value::Array(items) => items.into_iter().map(json_cell).collect(),
                    // `{"col": {"0": v0, "1": v1}}` (pandas' default `orient='columns'`).
                    serde_json::Value::Object(inner) => {
                        inner.into_iter().map(|(_, v)| json_cell(v)).collect()
                    }
                    scalar => vec![json_cell(scalar)],
                };
                cols.insert(k, values);
            }
            Ok(PipeInput::Columns(cols))
        }
        other => Err(WorkerError::Protocol(format!(
            "cannot build a frame from JSON {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_shapes() {
        match text_to_input(r#"[{"a": 1}, {"a": 2}]"#).unwrap() {
            PipeInput::Docs(d) => assert_eq!(d.len(), 2),
            _ => panic!("expected docs"),
        }
        match text_to_input(r#"{"a": [1, 2, 3]}"#).unwrap() {
            PipeInput::Columns(c) => assert_eq!(c["a"].len(), 3),
            _ => panic!("expected columns"),
        }
        match text_to_input("a:1,b:2\na:3,b:4\n").unwrap() {
            PipeInput::Docs(d) => {
                assert_eq!(d.len(), 2);
                assert_eq!(d[1]["a"], KValue::Int(3));
            }
            _ => panic!("expected docs"),
        }
    }
}
