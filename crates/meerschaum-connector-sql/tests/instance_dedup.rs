//! `tests/test_deduplicate.py`.
mod common;
mod fork_helpers;
use common::*;
use indexmap::IndexMap;
use meerschaum_connector::DataQuery;
use meerschaum_connector_sql::dtypes::get_db_type_from_pd_type;
use meerschaum_connector_sql::queries::table::{
    CreateTableOptions, QueryOrDtypes, get_create_table_queries,
};
use meerschaum_kernel::docs::{frame_from_docs, parse_df_datetimes};

flavor_tests!(test_deduplicate_default, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "deduplicate",
        Some("default_method"),
        r#"{"columns": {"datetime": "datetime", "id": "id"}}"#,
    );
    let raw = [
        ("2023-01-01 00:00:00", 1),
        ("2023-01-01 00:00:00", 1),
        ("2023-01-02 00:00:00", 2),
        ("2023-01-02 00:00:00", 2),
        ("2023-01-02 00:00:00", 2),
        ("2023-01-03 00:00:00", 3),
        ("2023-01-03 00:00:00", 4),
    ];
    let docs: Vec<Doc> = raw
        .iter()
        .map(|(d, n)| doc(vec![("datetime", s(d)), ("id", i(*n))]))
        .collect();
    let num_distinct = 4;
    let df = parse_df_datetimes(&frame_from_docs(&docs).unwrap(), &[], false, false);

    // `conn.to_sql(df, pipe.target)`: pandas creates a plain table first.
    let sp = c.conn.sql_pipe(&pipe).unwrap();
    let schema = c.conn.get_pipe_schema(&sp);
    let flavor = c.conn.flavor();
    let dtypes: IndexMap<String, String> = df.dtypes();
    let create = get_create_table_queries(
        &QueryOrDtypes::Dtypes(dtypes.clone()),
        &sp.target,
        flavor,
        &CreateTableOptions {
            schema: schema.clone(),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        c.conn.exec_queries_all(&create, true, true).await,
        "{create:?}"
    );
    let db_types: IndexMap<String, String> = dtypes
        .iter()
        .map(|(k, v)| (k.clone(), get_db_type_from_pd_type(v, Some(flavor))))
        .collect();
    let (ok_, msg) = c
        .conn
        .to_sql(&df, &sp.target, schema.as_deref(), &db_types, None)
        .await;
    assert!(ok_, "{msg}");
    assert_eq!(
        c.rowcount(&pipe, DataQuery::default()).await,
        Some(raw.len() as i64)
    );

    let res = ok(c.conn.deduplicate_pipe(&pipe, &DataQuery::default()).await);
    eprintln!("{}", res.message);
    assert_eq!(
        c.rowcount(&pipe, DataQuery::default()).await,
        Some(num_distinct)
    );
    c.delete(&pipe).await;
});

// test_deduplicate_without_instance_method: skipped — the chunked `Pipe.deduplicate`
// fallback (`_use_instance_method=False`) is engine-level, not an instance method.
