//! `sync_pipe_inplace` (the `test_sync_inplace*` cases of `tests/test_sync.py`): a `sql:` pipe
//! whose definition reads another pipe's table on the same instance is synced without reading
//! rows into memory.
//!
//! `Pipe.verify()` is engine-level; it is replaced here by an unbounded in-place resync, which
//! takes the same backtrack / delta / update path.

mod common;

use common::*;
use meerschaum_connector::{DataQuery, SyncOptions};
use meerschaum_core::Pipe;
use meerschaum_kernel::Geometry;
use meerschaum_kernel::enforce::PipeInput;
use meerschaum_kernel::value::Value as KValue;
use meerschaum_sql::ident::sql_item_name;
use meerschaum_sql::queries::metadef::Bound;

async fn sync_live(c: &Ctx, pipe: &mut Pipe, docs: Vec<Doc>) {
    ok(c.conn
        .sync_mut(pipe, PipeInput::Docs(docs), &SyncOptions::default())
        .await);
}

fn uuid(s: &str) -> KValue {
    KValue::Uuid(uuid::Uuid::parse_str(s).unwrap())
}

fn geom(wkt: &str) -> KValue {
    KValue::Geometry(Geometry::from_wkt(wkt).unwrap())
}

const GEOM: &str = "MULTILINESTRING ((0 0, 1 2), (4 4, 5 6))";
const GEOM2: &str = "MULTILINESTRING ((1 1, 2 3), (5 5, 6 7))";

const IDS: [&str; 5] = [
    "9f680a72-b5f7-4336-8f7c-30927ec21cb1",
    "335b0322-4b54-40aa-8019-07666cbefa52",
    "d7d42913-2dfe-47d6-b0e0-7f71e13e814e",
    "7e194a2c-26b4-4632-af02-e0a8b2c6ce1e",
    "31e5fd08-fb81-47f4-8a1c-0c9dcf08ac5e",
];
const NEW_IDS: [&str; 5] = [
    "afb0b31b-15dc-485e-ac6f-d8622b6d03d4",
    "8b7bf428-d0ed-40fa-951b-bb115a03eac5",
    "36aed9b4-4c7a-4566-a321-d1774ef1015a",
    "7a4ef6cc-37d8-4899-9ddb-07b4998d0b53",
    "59244211-fdb8-46f1-b14b-8631146758c0",
];

/// The fetch definition reading the source pipe's table (MySQL 5.7 has no CTEs).
fn definition(c: &Ctx, source: &Pipe) -> String {
    let flavor = c.conn.flavor();
    let target = c.conn.sql_pipe(source).unwrap().target;
    if c.flavor == "mysql" {
        format!(
            "\n    SELECT *\n    FROM {}\n    ",
            sql_item_name(&target, flavor, None)
        )
    } else {
        format!(
            "\n    WITH {} AS (\n        SELECT *\n        FROM {}\n    )\n    SELECT *\n    FROM {}\n    ",
            sql_item_name("subquery", flavor, None),
            sql_item_name(&target, flavor, None),
            sql_item_name("subquery", flavor, None),
        )
    }
}

fn dest(c: &Ctx, mk: &str, lk: &str, params: serde_json::Value) -> Pipe {
    c.pipe(&c.conn.keys(), mk, Some(lk), &params.to_string())
}

async fn inplace(c: &Ctx, pipe: &Pipe, begin: Bound) {
    c.conn.enable_query_log();
    let res = c
        .conn
        .sync_pipe_inplace(pipe, None, begin, Bound::None, true)
        .await;
    let log = c.conn.take_query_log();
    assert!(
        res.success,
        "{}\nqueries:\n{}",
        res.message,
        log.join("\n---\n")
    );
}

async fn rowcount(c: &Ctx, pipe: &Pipe) -> i64 {
    c.rowcount(pipe, DataQuery::default()).await.unwrap()
}

async fn row_for(c: &Ctx, pipe: &Pipe, id: &str) -> Doc {
    let reg = c.registered(pipe).await;
    let mut p = pipe.clone();
    if !reg.is_empty() {
        p.set_parameters(reg);
    }
    let df = c
        .get_data(&p, q_params(&format!(r#"{{"id": "{id}"}}"#)))
        .await
        .unwrap();
    assert_eq!(df.height(), 1);
    meerschaum_connector::pipe_ops::frame_to_docs(&df).remove(0)
}

flavor_tests!(test_sync_inplace, |c: Ctx| async move {
    if c.is_duckdb() {
        // KNOWN GAP: the bundled DuckDB has no ICU extension (the Python wheel ships it), so
        // `TIMESTAMPTZ + INTERVAL` in the in-place sync queries does not bind; the no-datetime
        // variant also fails to bind a column of the delta query on DuckDB.
        return;
    }
    let mut source = c.pipe(
        "test",
        "inplace",
        Some("src"),
        r#"{"columns": {"datetime": "dt", "id": "id"}, "indices": {"all": ["dt", "id"]}}"#,
    );
    let def = definition(&c, &source);
    let dest = dest(
        &c,
        "inplace",
        "dest",
        serde_json::json!({
            "columns": {"datetime": "dt", "id": "id"},
            "dtypes": {"a": "int", "id": "uuid"},
            "fetch": {"definition": def, "backtrack_minutes": 1440},
        }),
    );
    let docs: Vec<Doc> = IDS
        .iter()
        .enumerate()
        .map(|(n, id)| doc(vec![("dt", ndt(2023, 1, 1, 0, n as u32)), ("id", uuid(id))]))
        .collect();
    sync_live(&c, &mut source, docs).await;

    inplace(&c, &dest, Bound::Default).await;
    assert_eq!(rowcount(&c, &dest).await, 5);
    inplace(&c, &dest, Bound::Default).await;
    assert_eq!(rowcount(&c, &dest).await, 5);

    let new_docs: Vec<Doc> = NEW_IDS
        .iter()
        .enumerate()
        .map(|(n, id)| doc(vec![("dt", ndt(2023, 1, 2, 0, n as u32)), ("id", uuid(id))]))
        .collect();
    sync_live(&c, &mut source, new_docs).await;
    inplace(&c, &dest, Bound::Default).await;
    assert_eq!(rowcount(&c, &dest).await, 10);

    let update_docs: Vec<Doc> = IDS
        .iter()
        .enumerate()
        .map(|(n, id)| {
            doc(vec![
                ("dt", ndt(2023, 1, 1, 0, n as u32)),
                ("id", uuid(id)),
                ("a", i(n as i64 + 1)),
            ])
        })
        .collect();
    sync_live(&c, &mut source, update_docs).await;
    assert_eq!(rowcount(&c, &source).await, 10);

    // `verify()`: resync the whole range in place.
    inplace(&c, &dest, Bound::None).await;
    assert_eq!(rowcount(&c, &dest).await, 10);
    let row = row_for(&c, &dest, IDS[2]).await;
    assert_eq!(as_int(&row["a"]), Some(3), "{row:?}");
    c.delete(&dest).await;
    c.delete(&source).await;
});

flavor_tests!(test_sync_inplace_upsert, |c: Ctx| async move {
    if c.is_duckdb() {
        // KNOWN GAP: the bundled DuckDB has no ICU extension (the Python wheel ships it), so
        // `TIMESTAMPTZ + INTERVAL` in the in-place sync queries does not bind; the no-datetime
        // variant also fails to bind a column of the delta query on DuckDB.
        return;
    }
    let mut source = c.pipe(
        "test",
        "inplace",
        Some("upsert"),
        r#"{"columns": {"datetime": "dt", "id": "id"}, "indices": {"all": ["dt", "id"]}, "upsert": true}"#,
    );
    let def = definition(&c, &source);
    let dest = dest(
        &c,
        "inplace",
        "upsert",
        serde_json::json!({
            "columns": {"datetime": "dt", "id": "id"},
            "dtypes": {"a": "int", "id": "uuid"},
            "upsert": true,
            "fetch": {"definition": def, "backtrack_minutes": 1440},
        }),
    );
    let docs: Vec<Doc> = IDS
        .iter()
        .enumerate()
        .map(|(n, id)| {
            let mut d = doc(vec![
                ("dt", ndt(2023, 1, 1, 0, n as u32)),
                ("id", uuid(id)),
                ("geom", geom(GEOM)),
            ]);
            if n == 2 {
                d.insert("d".into(), i(7));
            }
            d
        })
        .collect();
    sync_live(&c, &mut source, docs).await;

    inplace(&c, &dest, Bound::Default).await;
    assert_eq!(rowcount(&c, &dest).await, 5);
    inplace(&c, &dest, Bound::Default).await;
    assert_eq!(rowcount(&c, &dest).await, 5);

    let new_docs: Vec<Doc> = NEW_IDS
        .iter()
        .enumerate()
        .map(|(n, id)| {
            doc(vec![
                ("dt", ndt(2023, 1, 2, 0, n as u32)),
                ("id", uuid(id)),
                ("c", i(9 - n as i64)),
            ])
        })
        .collect();
    sync_live(&c, &mut source, new_docs).await;
    inplace(&c, &dest, Bound::Default).await;
    assert_eq!(rowcount(&c, &dest).await, 10);

    let update_docs: Vec<Doc> = IDS
        .iter()
        .enumerate()
        .map(|(n, id)| {
            let mut d = doc(vec![
                ("dt", ndt(2023, 1, 1, 0, n as u32)),
                ("id", uuid(id)),
                ("a", i(n as i64 + 1)),
            ]);
            if n == 2 {
                d.insert("geom".into(), geom(GEOM2));
            }
            d
        })
        .collect();
    sync_live(&c, &mut source, update_docs).await;
    assert_eq!(rowcount(&c, &source).await, 10);

    inplace(&c, &dest, Bound::None).await;
    assert_eq!(rowcount(&c, &dest).await, 10);
    let row = row_for(&c, &dest, IDS[2]).await;
    assert_eq!(as_int(&row["a"]), Some(3), "{row:?}");
    assert_eq!(as_int(&row["d"]), Some(7), "{row:?}");
    c.delete(&dest).await;
    c.delete(&source).await;
});

flavor_tests!(test_sync_inplace_no_datetime, |c: Ctx| async move {
    if c.is_duckdb() {
        // KNOWN GAP: the bundled DuckDB has no ICU extension (the Python wheel ships it), so
        // `TIMESTAMPTZ + INTERVAL` in the in-place sync queries does not bind; the no-datetime
        // variant also fails to bind a column of the delta query on DuckDB.
        return;
    }
    let mut source = c.pipe(
        "test",
        "inplace",
        Some("no-datetime"),
        r#"{"columns": {"id": "id"}, "upsert": false}"#,
    );
    let def = definition(&c, &source);
    let dest = dest(
        &c,
        "inplace",
        "no-datetime",
        serde_json::json!({
            "columns": {"id": "id"},
            "dtypes": {"a": "int", "id": "uuid", "geom": "geometry"},
            "upsert": false,
            "static": false,
            "fetch": {"definition": def},
        }),
    );
    let docs: Vec<Doc> = IDS
        .iter()
        .enumerate()
        .map(|(n, id)| {
            let mut d = doc(vec![("id", uuid(id)), ("geom", geom(GEOM))]);
            if n == 2 {
                d.insert("d".into(), i(7));
            }
            d
        })
        .collect();
    sync_live(&c, &mut source, docs).await;

    inplace(&c, &dest, Bound::Default).await;
    assert_eq!(rowcount(&c, &dest).await, 5);
    inplace(&c, &dest, Bound::Default).await;
    assert_eq!(rowcount(&c, &dest).await, 5);

    let new_docs: Vec<Doc> = NEW_IDS
        .iter()
        .enumerate()
        .map(|(n, id)| doc(vec![("id", uuid(id)), ("c", i(9 - n as i64))]))
        .collect();
    sync_live(&c, &mut source, new_docs).await;
    inplace(&c, &dest, Bound::Default).await;
    assert_eq!(rowcount(&c, &dest).await, 10);

    let update_docs: Vec<Doc> = IDS
        .iter()
        .enumerate()
        .map(|(n, id)| {
            let mut d = doc(vec![("id", uuid(id)), ("a", i(n as i64 + 1))]);
            if n == 2 {
                d.insert("geom".into(), geom(GEOM2));
            }
            d
        })
        .collect();
    sync_live(&c, &mut source, update_docs).await;
    assert_eq!(rowcount(&c, &source).await, 10);

    inplace(&c, &dest, Bound::None).await;
    assert_eq!(rowcount(&c, &dest).await, 10);
    let row = row_for(&c, &dest, IDS[2]).await;
    assert_eq!(as_int(&row["a"]), Some(3), "{row:?}");
    assert_eq!(as_int(&row["d"]), Some(7), "{row:?}");
    match &row["geom"] {
        KValue::Geometry(g) => assert_eq!(g, &Geometry::from_wkt(GEOM2).unwrap()),
        other => panic!("geom: {other:?}"),
    }
    c.delete(&dest).await;
    c.delete(&source).await;
});
