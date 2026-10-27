//! Null-like strings (rc15 Q18): in a declared text column and a text index column the strings
//! `'None'`, `'NA'`, `'nan'`, `'NaT'`, `'NaTz'`, `''` and `'<NA>'` are values, written as text and
//! matched as text by `params`; only real nulls are `NULL`. Other columns keep v4's rule.
mod common;
use common::*;
use meerschaum_connector::DataQuery;
use meerschaum_kernel::value::Value as KValue;

const MARKERS: [&str; 7] = ["None", "NA", "nan", "NaT", "NaTz", "<NA>", "x"];

flavor_tests!(null_like_strings_stay_text, |c: Ctx| async move {
    let pipe = c.pipe(
        "q18",
        "markers",
        None,
        r#"{"columns": {"id": "code"}, "dtypes": {"code": "string", "label": "string"}}"#,
    );
    let mut docs: Vec<Doc> = MARKERS
        .iter()
        .enumerate()
        .map(|(n, m)| doc(vec![("code", s(m)), ("label", s(m)), ("n", i(n as i64))]))
        .collect();
    docs.push(doc(vec![
        ("code", KValue::Null),
        ("label", KValue::Null),
        ("n", i(MARKERS.len() as i64)),
    ]));
    ok(c.sync(&pipe, docs.clone()).await);
    let rows = c.docs(&pipe).await;
    assert_eq!(rows.len(), MARKERS.len() + 1, "{}", c.flavor);
    let text = |col: &str| -> Vec<String> {
        let mut v: Vec<String> = rows
            .iter()
            .filter_map(|d| match &d[col] {
                KValue::Str(s) => Some(s.clone()),
                _ => None,
            })
            .collect();
        v.sort();
        v
    };
    let mut expected: Vec<String> = MARKERS.iter().map(|m| m.to_string()).collect();
    expected.sort();
    // Oracle stores '' as NULL (it has no empty strings); nothing else is lost.
    assert_eq!(text("code"), expected, "{} code: {rows:?}", c.flavor);
    assert_eq!(text("label"), expected, "{} label: {rows:?}", c.flavor);

    for m in MARKERS {
        let q = q_params(&format!(r#"{{"code": "{m}"}}"#));
        assert_eq!(
            c.rowcount(&pipe, q.clone()).await,
            Some(1),
            "{} params code={m}",
            c.flavor
        );
        let df = c.get_data(&pipe, q).await.expect("rows");
        assert_eq!(df.height(), 1, "{} code={m}", c.flavor);
    }
    assert_eq!(
        c.rowcount(&pipe, q_params(r#"{"code": null}"#)).await,
        Some(1)
    );
    assert_eq!(
        c.rowcount(&pipe, q_params(r#"{"code": "_None"}"#)).await,
        Some(MARKERS.len() as i64)
    );
    assert_eq!(
        c.rowcount(&pipe, q_params(r#"{"label": "NA"}"#)).await,
        Some(1)
    );

    // A second sync of the same rows inserts nothing (the backtrack matches 'None' as text).
    let res = ok(c.sync(&pipe, docs).await);
    assert_eq!(
        c.rowcount(&pipe, DataQuery::default()).await,
        Some(MARKERS.len() as i64 + 1),
        "{}: {}",
        c.flavor,
        res.message
    );
    c.delete(&pipe).await;
});

// The empty string in a text index column and a declared text column: text on every flavor but
// Oracle, which stores `''` as `NULL`; there `''` is a null (as in v4), so re-syncs match the
// stored rows instead of inserting or updating them again, and `params={'code': ''}` finds them.
flavor_tests!(empty_strings_resync_idempotently, |c: Ctx| async move {
    let pipe = c.pipe(
        "q18",
        "empty",
        None,
        r#"{"columns": {"datetime": "ts", "id": "code"}, "dtypes": {"label": "string"}}"#,
    );
    let docs: Vec<Doc> = ["", "a", "None"]
        .iter()
        .enumerate()
        .map(|(n, code)| {
            doc(vec![
                ("ts", ndt(2024, 1, 1, 0, 0)),
                ("code", s(code)),
                ("label", s(if n == 0 { "" } else { "x" })),
                ("v", i(n as i64)),
            ])
        })
        .collect();
    ok(c.sync(&pipe, docs.clone()).await);
    for attempt in 1..=2 {
        let res = ok(c.sync(&pipe, docs.clone()).await);
        assert_eq!(
            res.message, "Inserted 0, updated 0 rows.",
            "{} re-sync {attempt}",
            c.flavor
        );
    }
    assert_eq!(c.rowcount(&pipe, DataQuery::default()).await, Some(3));
    // `params={'code': ''}` finds the row (`IS NULL` on Oracle, `= ''` elsewhere).
    assert_eq!(
        c.rowcount(&pipe, q_params(r#"{"code": ""}"#)).await,
        Some(1),
        "{}",
        c.flavor
    );
    let rows = c.docs(&pipe).await;
    let empty = rows
        .iter()
        .find(|d| as_int(&d["v"]) == Some(0))
        .expect("row 0");
    if c.is_oracle() {
        assert!(empty["code"].is_na() && empty["label"].is_na(), "{empty:?}");
    } else {
        assert_eq!(empty["code"], s(""), "{}", c.flavor);
        assert_eq!(empty["label"], s(""), "{}", c.flavor);
    }
    c.delete(&pipe).await;
});

// An undeclared, non-index column keeps v4's rule: PostgreSQL's `COPY` writes 'None' as NULL.
flavor_tests!(undeclared_columns_keep_the_v4_rule, |c: Ctx| async move {
    if !c.is_pg() {
        return;
    }
    let pipe = c.pipe("q18", "undeclared", None, r#"{"columns": {"id": "id"}}"#);
    ok(c.sync(
        &pipe,
        vec![
            doc(vec![("id", i(1)), ("other", s("None"))]),
            doc(vec![("id", i(2)), ("other", s("x"))]),
        ],
    )
    .await);
    let rows = c.docs(&pipe).await;
    let nulls = rows.iter().filter(|d| d["other"].is_na()).count();
    assert_eq!(nulls, 1, "{rows:?}");
    c.delete(&pipe).await;
});
