//! rc15 Q18: `filter_unseen_df` compares a literal column's null-like strings as text.

use meerschaum_kernel::Frame;
use meerschaum_kernel::diff::{FilterUnseenOptions, filter_unseen_df};
use meerschaum_kernel::frame::Column;
use meerschaum_kernel::value::Value;

fn frame(codes: &[Option<&str>]) -> Frame {
    Frame::new(vec![
        Column::object(
            "code",
            codes
                .iter()
                .map(|c| c.map_or(Value::Null, |s| Value::Str(s.into())))
                .collect(),
        ),
        Column::object("n", (0..codes.len() as i64).map(Value::Int).collect()),
    ])
    .unwrap()
}

#[test]
fn literal_columns_compare_null_like_strings_as_text() {
    let new = frame(&[Some("None"), Some("NA"), Some("x")]);
    let nulls = frame(&[None, None, Some("x")]);
    let literal = FilterUnseenOptions {
        literal_null_markers: vec!["code".to_string()],
        ..Default::default()
    };
    // v4's rule: 'None' / 'NA' are nulls, so nothing changed.
    let delta = filter_unseen_df(Some(&nulls), &new, &FilterUnseenOptions::default());
    assert_eq!(delta.height(), 0);
    // A literal column: the text replaced the nulls, and the delta keeps the text.
    let delta = filter_unseen_df(Some(&nulls), &new, &literal);
    assert_eq!(delta.height(), 2);
    let codes = delta.data("code").unwrap().values();
    assert_eq!(codes[0], Value::Str("None".into()));
    assert_eq!(codes[1], Value::Str("NA".into()));
    // The same text on both sides is unchanged.
    let delta = filter_unseen_df(Some(&new), &new, &literal);
    assert_eq!(delta.height(), 0);
}
