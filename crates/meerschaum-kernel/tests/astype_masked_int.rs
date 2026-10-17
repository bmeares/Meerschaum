//! `Series.astype('Int64')` (pandas' masked integer) refuses fractional floats and decimals, as
//! pandas 2.3 raises `TypeError: cannot safely cast non-equivalent object to int64`, while the
//! Arrow-backed `int64[pyarrow]` truncates an `object` column (PARITY in `pandas_astype`).
//! Oracle: released v4.1.4's pandas (`Series([Decimal('2.2')], dtype=object).astype('Int64')`).

use meerschaum_kernel::decimal::Decimal;
use meerschaum_kernel::enforce::pandas_astype;
use meerschaum_kernel::frame::ColumnData;
use meerschaum_kernel::value::Value;

fn object(values: Vec<Value>) -> ColumnData {
    ColumnData::Object(values)
}

#[test]
fn masked_integers_refuse_fractions() {
    for fractional in [
        Value::Decimal(Decimal::parse("2.2").unwrap()),
        Value::Float(2.2),
    ] {
        assert!(pandas_astype("c", &object(vec![fractional.clone()]), "Int64").is_err());
        // The Arrow-backed dtype truncates (PARITY).
        assert!(pandas_astype("c", &object(vec![fractional]), "int64[pyarrow]").is_ok());
    }
}

#[test]
fn masked_integers_take_whole_numbers() {
    for whole in [
        Value::Decimal(Decimal::parse("2").unwrap()),
        Value::Float(2.0),
        Value::Int(2),
    ] {
        let out = pandas_astype("c", &object(vec![whole]), "Int64").unwrap();
        assert_eq!(out.values(), vec![Value::Int(2)]);
    }
}
