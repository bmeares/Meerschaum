//! The dtype of a `filter_unseen_df` result column, as pandas decides it.
//!
//! v4 returns the rows of `merge(new, old, how='left', on=None)` (`meerschaum/utils/dataframe.py`),
//! so every output column has the dtype pandas gives the *merge* of the two frames' columns, after
//! `normalize_nulls` has called `infer_objects()` on both:
//!
//! 1. [`infer_objects`] decides what an `object` column becomes: all-`int` columns turn into
//!    `int64` (or `float64` when `None` / `NaN` is present), `float`s into `float64`, `bool`s into
//!    `bool` (only without nulls), strings into `str`, datetimes of one timezone into
//!    `datetime64[us(, tz)]`. Everything else (bytes, decimals, UUIDs, dates, JSON, geometries,
//!    all-null columns, mixed columns, and anything holding `pd.NA` next to numbers or booleans)
//!    stays `object`.
//! 2. [`merge_kind`] decides the merge: equal dtypes keep their dtype; numeric dtypes keep the
//!    left (new) frame's; datetime and string dtypes of the same class keep the left's; every
//!    other pair becomes `object` (pandas raises for some of them, e.g. `int64` against `str`;
//!    the kernel does not raise and uses `object` there).
//!
//! The pandas backends (NumPy, nullable, Arrow-backed) are erased: the table in
//! `crates/meerschaum-py/PARITY.md` (rows 17 and 18) lists the pairs whose result depends on the
//! backend (`str` against `string`, `int64[pyarrow]` against `float64`) and are not modeled.

use polars::prelude::DataType;

use crate::frame::ColumnData;
use crate::value::Value;

/// The class of a column dtype once the pandas backend is erased.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    Int,
    Float,
    Bool,
    Str,
    Bytes,
    Date,
    Datetime,
    /// Any other native dtype (decimals, categoricals, durations, times).
    Other,
    /// pandas `object`.
    Object,
}

/// A column's dtype class and its dtype string (`int64`, `datetime64[us, UTC]`, `object`, ...).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Kind {
    pub class: Class,
    pub dtype: String,
    /// NumPy-backed: the dtype came from `infer_objects()` (which never infers Arrow-backed
    /// dtypes), not from a cast to an Arrow dtype or from a native column.
    pub numpy: bool,
}

impl Kind {
    fn new(class: Class, dtype: &str) -> Self {
        Kind {
            class,
            dtype: dtype.to_string(),
            numpy: false,
        }
    }

    /// `object`.
    pub fn object() -> Self {
        Kind::new(Class::Object, "object")
    }

    /// Whether the dtype is pandas `object`.
    pub fn is_object(&self) -> bool {
        self.class == Class::Object
    }
}

/// The kind of a native Polars dtype.
pub fn native_kind(dtype: &DataType) -> Kind {
    let class = match dtype {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Class::Int,
        DataType::Float32 | DataType::Float64 => Class::Float,
        DataType::Boolean => Class::Bool,
        DataType::String => Class::Str,
        DataType::Binary | DataType::BinaryOffset => Class::Bytes,
        DataType::Date => Class::Date,
        DataType::Datetime(..) => Class::Datetime,
        _ => Class::Other,
    };
    Kind {
        class,
        dtype: crate::dtypes::pandas_dtype_of(dtype),
        numpy: false,
    }
}

/// What `pd.Series(values, dtype=object).infer_objects()` gives (`object` when nothing infers).
pub fn infer_objects(values: &[Value]) -> Kind {
    let mut kind = infer_objects_inner(values);
    kind.numpy = !kind.is_object();
    kind
}

fn infer_objects_inner(values: &[Value]) -> Kind {
    let (mut ints, mut floats, mut bools, mut strs, mut dts) = (0usize, 0, 0, 0, 0);
    let (mut nones, mut nans, mut nas, mut nats) = (0usize, 0, 0, 0);
    let mut other = false;
    let mut aware: Option<Option<String>> = None;
    let mut mixed_tz = false;
    for v in values {
        match v {
            Value::Null => nones += 1,
            Value::NA => nas += 1,
            Value::NaT => nats += 1,
            Value::Float(f) if f.is_nan() => nans += 1,
            Value::Float(_) => floats += 1,
            Value::Int(_) => ints += 1,
            Value::Bool(_) => bools += 1,
            Value::Str(_) => strs += 1,
            Value::Datetime(d) => {
                dts += 1;
                let tz = d.tz.as_ref().map(|t| t.name());
                match &aware {
                    None => aware = Some(tz),
                    Some(first) if *first != tz => mixed_tz = true,
                    Some(_) => {}
                }
            }
            _ => other = true,
        }
    }
    let typed = ints + floats + bools + strs + dts;
    if other || typed + nans == 0 {
        return Kind::object();
    }
    let kinds = [ints > 0, floats > 0, bools > 0, strs > 0, dts > 0];
    let (has_float, has_bool, has_str, has_dt) = (kinds[1], kinds[2], kinds[3], kinds[4]);
    // NaN alone, or beside numbers, is a float; beside strings or datetimes it is a null.
    let numeric = !has_bool && !has_str && !has_dt;
    if numeric {
        // `pd.NA` and `NaT` next to numbers keep the column `object`.
        if nas > 0 || nats > 0 {
            return Kind::object();
        }
        return if has_float || nans > 0 || nones > 0 {
            Kind::new(Class::Float, "float64")
        } else {
            Kind::new(Class::Int, "int64")
        };
    }
    let only = |n: usize| n > 0 && typed == n;
    if has_bool && only(bools) {
        // A boolean column with any null stays `object`.
        return if nones + nans + nas + nats > 0 {
            Kind::object()
        } else {
            Kind::new(Class::Bool, "bool")
        };
    }
    if has_str && only(strs) {
        // `NaT` next to strings is not a string null.
        return if nats > 0 {
            Kind::object()
        } else {
            Kind::new(Class::Str, "string")
        };
    }
    if has_dt && only(dts) && !mixed_tz && nas == 0 {
        let tz = aware.flatten();
        return match tz {
            Some(tz) => Kind::new(Class::Datetime, &format!("datetime64[us, {tz}]")),
            None => Kind::new(Class::Datetime, "datetime64[us]"),
        };
    }
    Kind::object()
}

/// The kind of a column as `merge` sees it (after `normalize_nulls`).
pub fn column_kind(data: &ColumnData) -> Kind {
    match data {
        ColumnData::Native(s) => native_kind(s.dtype()),
        ColumnData::Object(values) => infer_objects(values),
    }
}

/// The dtype of `merge(new, old, how='left')`'s key column (see the module documentation).
pub fn merge_kind(new: &Kind, old: &Kind) -> Kind {
    if new.class == old.class && new.dtype == old.dtype && new.numpy == old.numpy {
        return new.clone();
    }
    match (new.class, old.class) {
        (Class::Int | Class::Float, Class::Int | Class::Float) => new.clone(),
        // PARITY: an Arrow-backed boolean / string column does not merge with the NumPy-backed
        // one `infer_objects()` makes (`bool[pyarrow]` against `bool` is `object`).
        (Class::Bool, Class::Bool) | (Class::Str, Class::Str) if new.numpy != old.numpy => {
            Kind::object()
        }
        (Class::Datetime, Class::Datetime)
        | (Class::Str, Class::Str)
        | (Class::Bool, Class::Bool) => new.clone(),
        _ => Kind::object(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dtype_of(values: &[Value]) -> String {
        infer_objects(values).dtype
    }

    #[test]
    fn infers_like_pandas() {
        let nan = Value::Float(f64::NAN);
        assert_eq!(dtype_of(&[Value::Int(1), Value::Int(2)]), "int64");
        assert_eq!(dtype_of(&[Value::Int(1), Value::Null]), "float64");
        assert_eq!(dtype_of(&[Value::Int(1), nan.clone()]), "float64");
        assert_eq!(dtype_of(&[Value::Int(1), Value::NA]), "object");
        assert_eq!(dtype_of(&[Value::Int(1), Value::Float(2.5)]), "float64");
        assert_eq!(dtype_of(&[Value::Float(1.5), Value::NA]), "object");
        assert_eq!(dtype_of(&[nan.clone(), Value::Null]), "float64");
        assert_eq!(dtype_of(&[Value::Null, Value::Null]), "object");
        assert_eq!(dtype_of(&[Value::Bool(true), Value::Bool(false)]), "bool");
        assert_eq!(dtype_of(&[Value::Bool(true), Value::Null]), "object");
        assert_eq!(dtype_of(&[Value::Bool(true), Value::Int(1)]), "object");
        assert_eq!(dtype_of(&[Value::Bool(true), nan.clone()]), "object");
        assert_eq!(dtype_of(&[Value::Str("a".into()), Value::NA]), "string");
        assert_eq!(dtype_of(&[Value::Str("a".into()), Value::Int(1)]), "object");
        assert_eq!(dtype_of(&[Value::Bytes(vec![1])]), "object");
        assert_eq!(dtype_of(&[]), "object");
    }

    #[test]
    fn merges_like_pandas() {
        let int = Kind::new(Class::Int, "int64");
        let float = Kind::new(Class::Float, "float64");
        let string = Kind::new(Class::Str, "string");
        assert_eq!(merge_kind(&int, &float), int);
        assert_eq!(merge_kind(&float, &int), float);
        assert_eq!(merge_kind(&string, &string), string);
        assert!(merge_kind(&int, &string).is_object());
        assert!(
            merge_kind(&Kind::new(Class::Bytes, "binary[pyarrow]"), &Kind::object()).is_object()
        );
        assert!(merge_kind(&Kind::object(), &Kind::object()).is_object());
    }
}
