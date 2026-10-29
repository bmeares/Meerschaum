//! `tests/test_index_queries.py` (the mocked-state builders) and live index creation / dropping
//! for upsert pipes on each flavor (`test_create_drop_indices`-style).
mod common;
use common::*;
use indexmap::IndexMap;
use meerschaum_connector_sql::Flavor;
use meerschaum_connector_sql::pipe::SqlPipe;
use meerschaum_connector_sql::queries::ConnectorCtx;
use meerschaum_connector_sql::queries::index::{
    DropIndexState, IndexState, get_create_index_queries, get_drop_index_queries,
};
use meerschaum_connector_sql::queries::table::IndexDoc;
use meerschaum_core::Pipe;

fn upsert_pipe() -> SqlPipe {
    let pipe = Pipe::builder("test", "upsert")
        .parameters(params(
            r#"{"upsert": true, "null_indices": false, "target": "upsert_rows",
                "columns": {"datetime": "dt", "id": "id"},
                "dtypes": {"dt": "datetime", "id": "string"}}"#,
        ))
        .build()
        .unwrap();
    SqlPipe::new(pipe, Flavor::Postgresql).unwrap()
}

fn cols_types() -> IndexMap<String, String> {
    [("dt", "TIMESTAMP WITH TIME ZONE"), ("id", "TEXT")]
        .into_iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect()
}

fn ctx_pg() -> ConnectorCtx {
    ConnectorCtx::with_schema(Flavor::Postgresql, Some("public"))
}

#[test]
fn test_postgresql_upsert_creates_one_unique_constraint() {
    let pipe = upsert_pipe();
    let queries = get_create_index_queries(
        &ctx_pg(),
        &pipe,
        &IndexState {
            existing_cols_types: cols_types(),
            ..Default::default()
        },
    )
    .unwrap();
    eprintln!("{queries:#?}");
    let unique: Vec<&String> = queries
        .iter()
        .filter(|(k, _)| k.contains("unique") || k.contains("IX_upsert_rows_dt_id"))
        .flat_map(|(_, v)| v)
        .collect();
    assert_eq!(unique.len(), 1, "{queries:#?}");
    assert!(
        unique[0].contains("ADD CONSTRAINT \"UQ_upsert_rows_dt_id\" UNIQUE"),
        "{}",
        unique[0]
    );
    assert!(!unique[0].contains("CREATE UNIQUE INDEX"));
}

#[test]
fn test_postgresql_upsert_recognizes_and_drops_constraint_index() {
    let pipe = upsert_pipe();
    let doc = || IndexDoc {
        name: Some("UQ_upsert_rows_dt_id".into()),
        type_: Some("UNIQUE".into()),
        clustered: None,
    };
    let existing: IndexMap<String, Vec<IndexDoc>> = [
        ("dt".to_string(), vec![doc()]),
        ("id".to_string(), vec![doc()]),
    ]
    .into_iter()
    .collect();
    let create = get_create_index_queries(
        &ctx_pg(),
        &pipe,
        &IndexState {
            existing_cols_types: cols_types(),
            existing_cols_indices: existing.clone(),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        !create
            .values()
            .flatten()
            .any(|q| q.contains("UQ_upsert_rows_dt_id")),
        "{create:#?}"
    );
    let drop = get_drop_index_queries(
        &ctx_pg(),
        &pipe,
        &DropIndexState {
            exists: true,
            cols_indices: existing,
            is_hypertable: false,
            temp_table_exists: false,
        },
    );
    assert_eq!(
        drop["unique"],
        vec![
            "ALTER TABLE \"public\".\"upsert_rows\"\nDROP CONSTRAINT \"UQ_upsert_rows_dt_id\""
                .to_string()
        ]
    );
}

// Live: the upsert pipe's unique index (or constraint) is created on the first sync and
// recognized / dropped by `drop_indices`, then recreated by `create_indices`.
flavor_tests!(test_create_drop_indices_upsert, |c: Ctx| async move {
    if c.is_duckdb() {
        // PARITY: v4 has no DuckDB columns-indices query, so indices are never reported.
        return;
    }
    let pipe = c.pipe(
        "test",
        "upsert_ix",
        None,
        r#"{"upsert": true, "null_indices": false, "columns": {"datetime": "dt", "id": "id"}}"#,
    );
    ok(c.sync(
        &pipe,
        vec![doc(vec![
            ("dt", ndt(2024, 1, 1, 0, 0)),
            ("id", s("a")),
            ("v", i(1)),
        ])],
    )
    .await);
    let names = c.conn.get_pipe_index_names(&pipe).unwrap();
    let unique = names["unique"].clone();
    let indexed = |ix: &IndexMap<String, Vec<IndexDoc>>| -> Vec<String> {
        ix.values()
            .flatten()
            .filter_map(|d| d.name.clone())
            .map(|n| n.to_lowercase())
            .collect()
    };
    let ix = c.conn.get_pipe_columns_indices(&pipe).await.unwrap();
    let have = indexed(&ix);
    let expect = if c.is_pg() {
        unique.replace("IX_", "UQ_").to_lowercase()
    } else {
        unique.to_lowercase()
    };
    if c.flavor == "mysql" {
        // PARITY: MySQL 5.7 rejects a unique key on the TEXT `id` column ("BLOB/TEXT column
        // used in key specification without a key length"), as v4 does; without the key, the
        // upsert inserts a second row.
        assert!(!have.contains(&expect), "{have:?}");
        ok(c.sync(
            &pipe,
            vec![doc(vec![
                ("dt", ndt(2024, 1, 1, 0, 0)),
                ("id", s("a")),
                ("v", i(2)),
            ])],
        )
        .await);
        assert_eq!(c.docs(&pipe).await.len(), 2);
        c.delete(&pipe).await;
        return;
    }
    if c.is_mssql() {
        // PARITY: MSSQL rejects the unique constraint on the `NVARCHAR(MAX)` `id` column
        // ("is of a type that is invalid for use as a key column", as v4 does), but the
        // `MERGE` upsert still updates the row in place.
        assert!(!have.contains(&expect), "{have:?}");
        ok(c.sync(
            &pipe,
            vec![doc(vec![
                ("dt", ndt(2024, 1, 1, 0, 0)),
                ("id", s("a")),
                ("v", i(2)),
            ])],
        )
        .await);
        let docs = c.docs(&pipe).await;
        assert_eq!(docs.len(), 1);
        assert_eq!(as_int(&docs[0]["v"]), Some(2));
        c.delete(&pipe).await;
        return;
    }
    assert!(have.contains(&expect), "{expect} not in {have:?}");

    // Upserting the same key keeps one row.
    ok(c.sync(
        &pipe,
        vec![doc(vec![
            ("dt", ndt(2024, 1, 1, 0, 0)),
            ("id", s("a")),
            ("v", i(2)),
        ])],
    )
    .await);
    let docs = c.docs(&pipe).await;
    assert_eq!(docs.len(), 1);
    assert_eq!(as_int(&docs[0]["v"]), Some(2));

    ok(c.conn.drop_pipe_indices(&pipe, None).await);
    let have = indexed(&c.conn.get_pipe_columns_indices(&pipe).await.unwrap());
    assert!(!have.contains(&expect), "{expect} still in {have:?}");
    ok(c.conn.create_pipe_indices(&pipe, None).await);
    let have = indexed(&c.conn.get_pipe_columns_indices(&pipe).await.unwrap());
    assert!(have.contains(&expect), "{expect} not recreated: {have:?}");
    c.delete(&pipe).await;
});
