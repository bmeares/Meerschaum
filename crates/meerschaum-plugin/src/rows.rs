//! JSON rows in and out of [`Frame`]s, so a plugin can build typed Arrow data without depending
//! on `meerschaum-kernel` itself: `Frame::from_rows(rows, dtypes)` and `frame.to_rows()`
//! ([`FrameExt`], in the [`crate::prelude`]).
use crate::{Frame, Result, error, sdk};
use indexmap::IndexMap;
use meerschaum_kernel::enforce::{PipeDtypesContext, PipeInput, pipe_enforce_dtypes};
use serde_json::{Value, json};

/// Row conversions for [`Frame`]. `use meerschaum_plugin::prelude::*;` (or `FrameExt`) brings
/// them into scope.
pub trait FrameExt: Sized {
    /// A frame from JSON object rows (`json!({...})` values or `serde_json::Map`s).
    ///
    /// `dtypes` gives Meerschaum dtypes by column: `datetime` (ISO-8601 strings, UTC),
    /// `date`, `int` (and `int8`..`int64`), `float`, `string` (`str`), `bool`, `json`,
    /// `numeric` (`numeric[precision,scale]`), `uuid`, `bytes`, `geometry[...]`, or a pandas
    /// dtype (`Int32`, `float32`, ...). A declared column no row has is added as nulls, so the
    /// frame always has the declared schema (also with zero rows). Undeclared columns are
    /// inferred the way a Python plugin's documents are (`pd.DataFrame(docs).convert_dtypes()`):
    /// integers, floats, booleans and strings are typed, ISO timestamps stay strings.
    ///
    /// ```
    /// use meerschaum_plugin::prelude::*;
    /// let frame = Frame::from_rows(
    ///     vec![json!({"ts": "2024-01-01T00:00:00Z", "station": "a", "temp": 21.5})],
    ///     &[("ts", "datetime"), ("temp", "float")],
    /// )?;
    /// assert_eq!(frame.height(), 1);
    /// assert!(frame.dtypes()["ts"].starts_with("datetime64"));
    /// # Ok::<(), meerschaum_plugin::Error>(())
    /// ```
    fn from_rows<R: Into<Value>>(
        rows: impl IntoIterator<Item = R>,
        dtypes: &[(&str, &str)],
    ) -> Result<Self>;

    /// The rows as JSON objects: integers, floats, booleans and JSON stay themselves;
    /// datetimes become ISO-8601 strings, nulls `null`, other values text.
    fn to_rows(&self) -> Vec<Value>;
}

impl FrameExt for Frame {
    fn from_rows<R: Into<Value>>(
        rows: impl IntoIterator<Item = R>,
        dtypes: &[(&str, &str)],
    ) -> Result<Frame> {
        let rows: Vec<Value> = rows.into_iter().map(Into::into).collect();
        let docs = sdk::rows_to_docs(rows)?;
        let declared: IndexMap<String, String> = dtypes
            .iter()
            .map(|(column, dtype)| (column.to_string(), dtype.to_string()))
            .collect();
        let frame = if declared.is_empty() {
            sdk::docs_to_frame(&docs)?
        } else {
            // The pipe sync path: parse the declared datetimes, then enforce every declared dtype.
            pipe_enforce_dtypes(
                PipeInput::Docs(docs),
                &PipeDtypesContext {
                    dtypes: declared.clone(),
                    explicit_dtypes: declared.clone(),
                    enforce: true,
                    tz_aware: true,
                    mixed_numerics: false,
                },
            )
            .map_err(error)?
        };
        Ok(meerschaum_kernel::diff::add_missing_cols(&frame, &declared))
    }

    fn to_rows(&self) -> Vec<Value> {
        frame_rows(self)
    }
}

/// A frame's rows as JSON objects (the rows ABI's encoding).
pub(crate) fn frame_rows(frame: &Frame) -> Vec<Value> {
    (0..frame.height())
        .map(|i| {
            Value::Object(
                frame
                    .columns()
                    .iter()
                    .zip(frame.row(i))
                    .map(|(column, value)| {
                        (
                            column.name.clone(),
                            match &value {
                                meerschaum_kernel::Value::Bool(v) => json!(v),
                                meerschaum_kernel::Value::Int(v) => json!(v),
                                meerschaum_kernel::Value::Float(v) => json!(v),
                                meerschaum_kernel::Value::Json(v) => v.clone(),
                                // Text as it is: `json_serialize_value` reads 4.1's null-like
                                // strings ('', 'None', 'nan', ...) as `null`.
                                meerschaum_kernel::Value::Str(v) => json!(v),
                                _ => meerschaum_kernel::serialize::json_serialize_value(&value),
                            },
                        )
                    })
                    .collect(),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerschaum_kernel::Value as K;

    #[test]
    fn declared_dtypes_cover_the_meerschaum_vocabulary() {
        let frame = Frame::from_rows(
            vec![
                json!({"ts": "2024-01-01T00:00:00Z", "day": "2024-01-02", "id": 1, "f": 1.5,
                    "s": "a", "b": true, "j": {"a": 1}, "n": "1.25",
                    "u": "6b9d8a2e-6c43-4e1c-8a2d-1f1f1f1f1f1f", "by": "aGk=", "i32": 5, "extra": 7}),
                json!({"ts": "2024-01-01 01:00:00", "day": "2024-01-03", "id": 2, "f": 2,
                    "s": "b", "b": false, "j": [1, 2], "n": 3,
                    "u": "6b9d8a2e-6c43-4e1c-8a2d-1f1f1f1f1f10", "by": "aGk=", "i32": 6}),
            ],
            &[
                ("ts", "datetime"),
                ("day", "date"),
                ("id", "int"),
                ("f", "float"),
                ("s", "string"),
                ("b", "bool"),
                ("j", "json"),
                ("n", "numeric"),
                ("u", "uuid"),
                ("by", "bytes"),
                ("i32", "int32"),
                ("missing", "datetime"),
            ],
        )
        .unwrap();
        let dtypes = frame.dtypes();
        for (column, dtype) in [
            ("ts", "datetime64[us, UTC]"),
            ("day", "date32[day][pyarrow]"),
            ("id", "int64"),
            ("f", "float64"),
            ("s", "string"),
            ("b", "bool"),
            ("by", "binary[pyarrow]"),
            ("i32", "int32"),
            ("missing", "datetime64[us, UTC]"),
            // Undeclared columns are inferred like a Python plugin's documents.
            ("extra", "int64"),
        ] {
            assert_eq!(dtypes[column], dtype, "{column}");
        }
        let row = frame.row(1);
        let cell = |name: &str| row[frame.names().iter().position(|n| n == name).unwrap()].clone();
        assert!(matches!(cell("j"), K::Json(_)));
        assert!(matches!(cell("n"), K::Decimal(_)));
        assert!(matches!(cell("u"), K::Uuid(_)));
        assert_eq!(cell("by"), K::Bytes(b"hi".to_vec()));
        assert_eq!(cell("f"), K::Float(2.0));
        let rows = frame.to_rows();
        assert_eq!(rows[1]["ts"], "2024-01-01T01:00:00+00:00");
        assert_eq!(rows[0]["j"], json!({"a": 1}));
        assert_eq!(rows[0]["missing"], Value::Null);
    }

    #[test]
    fn zero_rows_keep_the_declared_schema_and_undeclared_rows_are_inferred() {
        let empty =
            Frame::from_rows(Vec::<Value>::new(), &[("ts", "datetime"), ("v", "float")]).unwrap();
        assert_eq!(empty.height(), 0);
        assert_eq!(empty.dtypes()["ts"], "datetime64[us, UTC]");
        assert_eq!(empty.dtypes()["v"], "float64");
        let inferred = Frame::from_rows(
            [serde_json::Map::from_iter([("id".to_string(), json!(1))])],
            &[],
        )
        .unwrap();
        assert_eq!(inferred.dtypes()["id"], "int64");
        assert!(Frame::from_rows([json!(1)], &[]).is_err());
    }

    /// Text that 4.1 would read as null (`''`, `'None'`, `'nan'`, ...) stays text in rows, as
    /// it does in the frame and in a Python plugin's DataFrame; only real nulls are `null`.
    #[test]
    fn null_like_text_stays_text_in_rows() {
        let texts = ["", "None", "nan", "NA", "<NA>", "NaT", "NaTz", "null"];
        let rows = texts
            .iter()
            .map(|text| json!({"declared": text, "inferred": text}))
            .chain([json!({"declared": null, "inferred": null})]);
        let frame = Frame::from_rows(rows, &[("declared", "string")]).unwrap();
        let rows = frame.to_rows();
        for (row, text) in rows.iter().zip(texts) {
            assert_eq!(row["declared"], json!(text), "{row}");
            assert_eq!(row["inferred"], json!(text), "{row}");
        }
        assert_eq!(rows[texts.len()]["declared"], Value::Null);
        assert_eq!(rows[texts.len()]["inferred"], Value::Null);
    }
}
