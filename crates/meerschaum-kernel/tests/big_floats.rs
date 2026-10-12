//! Regression: integral floats outside the `int64` range (`1e20`, `-1e20`, `2**63`, `2**64`) must
//! never saturate to `i64::MAX` / `i64::MIN`. The expected values are v4's (`MRSM_CORE=python`):
//!
//! - `pd.DataFrame(docs).convert_dtypes()` keeps them `Float64` (an integral float column becomes
//!   `Int64` only when `arr.astype(int) == arr`, i.e. inside `[-2**63, 2**63)`);
//! - `Series.astype('int64[pyarrow]')` raises ("Float value ... was truncated converting to
//!   int64"), so `enforce_dtypes(..., {'a': 'int'})` leaves the column as it was and
//!   `filter_unseen_df` against an `int64` frame falls back to the mixed int/float → numeric rule;
//! - `float`, `numeric` and untyped columns keep the values.

use indexmap::IndexMap;
use meerschaum_kernel::diff::{FilterUnseenOptions, filter_unseen_df};
use meerschaum_kernel::docs::convert_dtypes;
use meerschaum_kernel::enforce::{EnforceOptions, enforce_dtypes_with_path, pandas_astype};
use meerschaum_kernel::{Column, ColumnData, Decimal, Frame, Value};
use polars::prelude::{DataType, NamedFrom, Series};

const BIG: [f64; 4] = [
    1e20,
    -1e20,
    9_223_372_036_854_775_808.0,
    18_446_744_073_709_551_616.0,
];

fn float_frame() -> Frame {
    Frame::new(vec![Column::native(Series::new("a".into(), &BIG))]).unwrap()
}

fn object_frame() -> Frame {
    Frame::new(vec![Column::object(
        "a",
        BIG.iter().map(|f| Value::Float(*f)).collect(),
    )])
    .unwrap()
}

fn floats_of(data: &ColumnData) -> Vec<f64> {
    data.values()
        .into_iter()
        .map(|v| match v {
            Value::Float(f) => f,
            other => panic!("expected a float, got {other:?}"),
        })
        .collect()
}

fn dtypes(typ: &str) -> IndexMap<String, String> {
    IndexMap::from([("a".to_string(), typ.to_string())])
}

#[test]
fn convert_dtypes_keeps_out_of_range_integral_floats() {
    let values: Vec<Value> = BIG.iter().map(|f| Value::Float(*f)).collect();
    let ColumnData::Native(s) = convert_dtypes("a", &values) else {
        panic!("expected a native column");
    };
    assert_eq!(s.dtype(), &DataType::Float64);
    assert_eq!(floats_of(&ColumnData::Native(s)), BIG.to_vec());

    // -2**63 is exactly representable as int64: pandas makes it `Int64`.
    let values = vec![
        Value::Float(-9_223_372_036_854_775_808.0),
        Value::Float(1.0),
    ];
    let ColumnData::Native(s) = convert_dtypes("a", &values) else {
        panic!("expected a native column");
    };
    assert_eq!(s.dtype(), &DataType::Int64);
    assert_eq!(
        ColumnData::Native(s).values(),
        vec![Value::Int(i64::MIN), Value::Int(1)]
    );
}

#[test]
fn astype_int_rejects_out_of_range_floats() {
    for f in BIG {
        let native = ColumnData::Native(Series::new("a".into(), &[f]));
        assert!(
            pandas_astype("a", &native, "int64[pyarrow]").is_err(),
            "{f}"
        );
        let object = ColumnData::Object(vec![Value::Float(f)]);
        assert!(pandas_astype("a", &object, "int64").is_err(), "{f}");
        let decimal = ColumnData::Object(vec![Value::Decimal(
            Decimal::parse(&format!("{f:.0}")).unwrap(),
        )]);
        assert!(pandas_astype("a", &decimal, "Int64").is_err(), "{f}");
    }
    let edge = ColumnData::Native(Series::new("a".into(), &[-9_223_372_036_854_775_808.0f64]));
    assert_eq!(
        pandas_astype("a", &edge, "int64").unwrap().values(),
        vec![Value::Int(i64::MIN)]
    );
}

#[test]
fn enforce_keeps_out_of_range_floats() {
    for typ in ["float", "numeric", "int"] {
        for frame in [float_frame(), object_frame()] {
            let (out, _) =
                enforce_dtypes_with_path(&frame, &dtypes(typ), &EnforceOptions::default()).unwrap();
            let data = out.data("a").unwrap();
            if typ == "numeric" {
                let expected: Vec<Value> = [
                    "1E+20",
                    "-1E+20",
                    "9.223372036854776E+18",
                    "1.8446744073709552E+19",
                ]
                .iter()
                .map(|s| Value::Decimal(Decimal::parse(s).unwrap()))
                .collect();
                assert_eq!(data.values(), expected, "{typ}");
                continue;
            }
            assert_eq!(floats_of(data), BIG.to_vec(), "{typ}");
        }
    }
    // Untyped: unchanged.
    let (out, _) =
        enforce_dtypes_with_path(&float_frame(), &IndexMap::new(), &EnforceOptions::default())
            .unwrap();
    assert_eq!(floats_of(out.data("a").unwrap()), BIG.to_vec());
}

#[test]
fn filter_unseen_does_not_saturate() {
    let new = float_frame();
    // Against a float frame: the values pass through.
    let old = Frame::new(vec![Column::native(Series::new("a".into(), &[1.5f64]))]).unwrap();
    let delta = filter_unseen_df(Some(&old), &new, &FilterUnseenOptions::default());
    assert_eq!(floats_of(delta.data("a").unwrap()), BIG.to_vec());

    // Against an int frame: the cast to int64 fails, the mixed rule makes decimals.
    let old = Frame::new(vec![Column::native(Series::new("a".into(), &[1i64]))]).unwrap();
    let delta = filter_unseen_df(Some(&old), &new, &FilterUnseenOptions::default());
    let expected: Vec<Value> = [
        "100000000000000000000",
        "-100000000000000000000",
        "9223372036854776000",
        "18446744073709552000",
    ]
    .iter()
    .map(|s| Value::Decimal(Decimal::parse(s).unwrap()))
    .collect();
    assert_eq!(delta.data("a").unwrap().values(), expected);
}
