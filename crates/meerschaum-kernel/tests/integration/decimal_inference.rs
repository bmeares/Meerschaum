//! The Polars decimal type of an object column of `Decimal`s, as Polars infers it from Python
//! `Decimal`s in v4 (`enforce_dtypes(df, {'a': 'numeric'}, as_polars=True)` on `meerschaum==4.1.4`,
//! polars 1.44.1; recorded 2026-10-18): the most integer digits (none below 1) + the largest scale.

use meerschaum_kernel::frame::object_values_to_series;
use meerschaum_kernel::{Decimal, Value};
use polars::prelude::DataType;

fn inferred(values: &[Option<&str>]) -> DataType {
    let values: Vec<Value> = values
        .iter()
        .map(|v| match v {
            Some(text) => Value::Decimal(Decimal::parse(text).unwrap()),
            None => Value::Null,
        })
        .collect();
    object_values_to_series("a", &values).dtype().clone()
}

#[test]
fn decimal_precision_and_scale_follow_polars_inference() {
    let cases: &[(&[Option<&str>], usize, usize)] = &[
        (&[Some("0.000012345")], 9, 9),
        (&[Some("0.5")], 1, 1),
        (&[Some("0.05"), Some("12.3")], 4, 2),
        (&[Some("0")], 1, 0),
        (&[Some("100")], 3, 0),
        (&[Some("1E+2")], 3, 0),
        (&[Some("-0.001"), Some("5")], 4, 3),
        (&[Some("123.456"), Some("0.1")], 6, 3),
        (&[Some("0.10")], 2, 2),
        (&[Some("99999.99999")], 10, 5),
        (&[Some("0.000012345"), None], 9, 9),
    ];
    for (values, precision, scale) in cases {
        assert_eq!(
            inferred(values),
            DataType::Decimal(*precision, *scale),
            "{values:?}"
        );
    }
}
