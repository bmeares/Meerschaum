//! Wave 6 (task B): cross-pipe `{{ Pipe() }}` templates resolved against the instance, and the
//! raw-versus-resolved parameters rule (stored parameters keep their templates; execution uses
//! the resolved ones). Table prefix `w6b_`.

mod common;
use common::*;
use meerschaum_connector::{InstanceConnector, InstanceProvider};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_connector_sql::instance::fetch::FetchArgs;
use meerschaum_core::Pipe;
use std::collections::HashMap;
use std::sync::Arc;

fn short() -> String {
    format!("w6b_{}", &uuid::Uuid::new_v4().simple().to_string()[..8])
}

fn pipe_on(conn: &SqlConnector, ck: &str, mk: &str, parameters: &str) -> Pipe {
    Pipe::builder(ck, mk)
        .instance(&conn.keys())
        .parameters(params(parameters))
        .build()
        .unwrap()
}

async fn seed_parent(c: &Ctx, ck: &str) -> Pipe {
    let parent = pipe_on(
        &c.conn,
        ck,
        "par",
        r#"{"columns": {"datetime": "dt", "id": "id"}}"#,
    );
    assert!(c.conn.register_pipe(&parent).await.success);
    ok(c.sync(
        &parent,
        vec![
            doc(vec![
                ("dt", ndt(2024, 1, 1, 0, 0)),
                ("id", i(1)),
                ("val", i(10)),
            ]),
            doc(vec![
                ("dt", ndt(2024, 1, 2, 0, 0)),
                ("id", i(2)),
                ("val", i(20)),
            ]),
        ],
    )
    .await);
    parent
}

// Register a pipe whose `sql` uses `{{ Pipe(...) }}` and whose `target` uses `{{ self.metric }}`;
// after an in-place sync the stored parameters still hold the templates and the rows land in the
// resolved table.
flavor_tests!(raw_parameters_persist_and_resolve, |c: Ctx| async move {
    let ck = short();
    let parent = seed_parent(&c, &ck).await;
    let child_ck = c.conn.keys();
    let sql = format!(
        "SELECT * FROM {{{{ Pipe('{ck}', 'par', instance='{}') }}}}",
        c.conn.keys()
    );
    let parameters = format!(
        r#"{{"columns": {{"datetime": "dt", "id": "id"}}, "target": "{ck}_{{{{ self.metric }}}}", "sql": "{sql}"}}"#,
        sql = sql.replace('\'', "\\'").replace('"', "\\\"")
    );
    // JSON cannot contain `\'`: undo the single-quote escape.
    let parameters = parameters.replace("\\'", "'");
    let child = pipe_on(&c.conn, &child_ck, &format!("{ck}child"), &parameters);
    assert!(child.raw_parameters()["target"].py_str().contains("{{"));
    assert!(c.conn.register_pipe(&child).await.success);
    let res = c
        .conn
        .sync_pipe_inplace(
            &child,
            None,
            meerschaum_connector_sql::queries::metadef::Bound::Default,
            meerschaum_connector_sql::queries::metadef::Bound::None,
            true,
        )
        .await;
    assert!(res.success, "{}", res.message);

    // Stored parameters keep the templates.
    let stored = c.registered(&child).await;
    assert!(
        stored["target"].py_str().contains("{{ self.metric }}"),
        "{stored:?}"
    );
    assert!(stored["sql"].py_str().contains("{{ Pipe("), "{stored:?}");

    // Data landed in the resolved table.
    let resolved_table = format!("{ck}_{ck}child");
    let rows = c
        .conn
        .read(&format!(
            "SELECT COUNT(*) AS n FROM {}",
            meerschaum_connector_sql::ident::sql_item_name(&resolved_table, c.conn.flavor(), None)
        ))
        .await
        .unwrap();
    let n = meerschaum_connector::pipe_ops::frame_to_docs(&rows)[0]["n"].clone();
    assert_eq!(as_int(&n), Some(2));

    // Reading through the instance resolves the target too.
    let docs = c.docs(&child).await;
    assert_eq!(docs.len(), 2);
    assert_eq!(as_int(&docs[1]["val"]), Some(20));

    // `edit_pipe` of the pipe the engine handed out still persists raw parameters.
    let prepared = c.conn.prepare_pipe(&child).await;
    assert!(c.conn.edit_pipe(&prepared, false).await.success);
    let stored = c.registered(&child).await;
    assert!(stored["sql"].py_str().contains("{{ Pipe("), "{stored:?}");

    c.delete(&child).await;
    let _ = c
        .conn
        .driver()
        .exec(&format!(
            "DROP TABLE {}",
            meerschaum_connector_sql::ident::sql_item_name(&resolved_table, c.conn.flavor(), None)
        ))
        .await;
    c.delete(&parent).await;
});

// The bare template is the quoted table name; `.columns[...]`, `.target` and `.dtypes` read the
// referenced pipe's REGISTERED parameters; `fetch` runs the resolved definition.
flavor_tests!(template_attribute_chains_and_fetch, |c: Ctx| async move {
    let ck = short();
    let parent = seed_parent(&c, &ck).await;
    let inst = c.conn.keys();
    let p = format!("Pipe('{ck}', 'par', instance='{inst}')");
    // The child is constructed with NO columns of its own: `.columns['datetime']` must come from
    // the referenced pipe's registered parameters.
    let sql = format!(
        "SELECT {{{{ {p}.columns['datetime'] }}}} AS dt, id, val FROM {{{{ {p} }}}} WHERE id = 2"
    );
    let child = pipe_on(
        &c.conn,
        &inst,
        &format!("{ck}fetch"),
        &serde_json::json!({"sql": sql}).to_string(),
    );
    let df = c
        .conn
        .fetch(
            &child,
            &FetchArgs {
                begin: meerschaum_connector_sql::queries::metadef::Bound::None,
                check_existing: false,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(df.height(), 1);
    let doc0 = &meerschaum_connector::pipe_ops::frame_to_docs(&df)[0];
    assert_eq!(as_int(&doc0["val"]), Some(20));

    // Resolved pipe: the templates are gone from `parameters()` but not from `raw_parameters()`.
    let prepared = c.conn.prepare_pipe(&child).await;
    let resolved_sql = prepared.parameters()["sql"].py_str();
    assert!(!resolved_sql.contains("{{"), "{resolved_sql}");
    assert!(resolved_sql.contains("SELECT dt AS dt"), "{resolved_sql}");
    assert!(prepared.raw_parameters()["sql"].py_str().contains("{{"));

    // `.target`.
    let t = pipe_on(
        &c.conn,
        &inst,
        &format!("{ck}tgt"),
        &serde_json::json!({"note": format!("{{{{ {p}.target }}}}")}).to_string(),
    );
    let t = c.conn.prepare_pipe(&t).await;
    assert_eq!(
        t.parameters()["note"],
        KValueCore::from(format!("{ck}_par"))
    );
    c.delete(&parent).await;
});

// Task K: `sync_pipe_mut` / `sync_pipe_mut_cached` resolve cross-pipe templates through the
// instance on every call (the façade hands them the RAW parameters), write to the resolved table
// and leave the caller's raw parameters holding the templates.
flavor_tests!(sync_pipe_mut_resolves_templates, |c: Ctx| async move {
    use meerschaum_connector::SyncOptions;
    let ck = short();
    let parent = seed_parent(&c, &ck).await;
    let inst = c.conn.keys();
    let p = format!("Pipe('{ck}', 'par', instance='{inst}')");
    let mut child = pipe_on(
        &c.conn,
        &inst,
        &format!("{ck}mut"),
        &serde_json::json!({
            "columns": {"datetime": "dt", "id": "id"},
            "target": format!("{{{{ {p}.target }}}}_mut"),
        })
        .to_string(),
    );
    assert!(child.raw_parameters()["target"].py_str().contains("{{"));
    let frame = || {
        meerschaum_kernel::Frame::from_object_columns(vec![
            ("dt".into(), vec![ndt(2024, 1, 1, 0, 0)]),
            ("id".into(), vec![i(7)]),
            ("val".into(), vec![i(70)]),
        ])
        .unwrap()
    };
    let opts = SyncOptions {
        check_existing: false,
        ..Default::default()
    };
    let res = c.conn.sync_pipe_mut(&mut child, frame(), &opts).await;
    assert!(res.success, "{}", res.message);
    // Raw parameters keep the template; the rows are in the resolved table.
    assert!(child.raw_parameters()["target"].py_str().contains("{{"));
    let resolved_table = format!("{ck}_par_mut");
    let rows = c
        .conn
        .read(&format!(
            "SELECT COUNT(*) AS n FROM {}",
            meerschaum_connector_sql::ident::sql_item_name(&resolved_table, c.conn.flavor(), None)
        ))
        .await
        .unwrap();
    let n = meerschaum_connector::pipe_ops::frame_to_docs(&rows)[0]["n"].clone();
    assert_eq!(as_int(&n), Some(1));
    // Same for the cached variant.
    let res = c
        .conn
        .sync_pipe_mut_cached(&mut child, frame(), &opts, None)
        .await;
    assert!(res.success, "{}", res.message);
    assert!(child.raw_parameters()["target"].py_str().contains("{{"));
    let _ = c
        .conn
        .driver()
        .exec(&format!(
            "DROP TABLE {}",
            meerschaum_connector_sql::ident::sql_item_name(&resolved_table, c.conn.flavor(), None)
        ))
        .await;
    c.delete(&parent).await;
});

use meerschaum_core::Value as KValueCore;

struct Static(HashMap<String, Arc<dyn InstanceConnector>>);

impl InstanceProvider for Static {
    fn instance(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        self.0.get(keys).cloned()
    }
}

// A reference naming another instance resolves against THAT instance's registered state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_instance_reference_resolves_on_that_instance() {
    let (pa, pb) = (temp_sqlite_path("w6bxa"), temp_sqlite_path("w6bxb"));
    let a = SqlConnector::from_uri("w6b_a", &format!("sqlite:///{}", pa.display()))
        .await
        .unwrap();
    let b = Arc::new(
        SqlConnector::from_uri("w6b_b", &format!("sqlite:///{}", pb.display()))
            .await
            .unwrap(),
    );
    let ck = short();
    let far = |inst: &SqlConnector, col: &str| {
        pipe_on(
            inst,
            &ck,
            "far",
            &serde_json::json!({"columns": {"datetime": col}}).to_string(),
        )
    };
    assert!(b.register_pipe(&far(&b, "far_dt")).await.success);
    // The same keys on instance A are registered with different columns: they must not be used.
    assert!(a.register_pipe(&far(&a, "decoy_dt")).await.success);

    let sql = format!(
        "SELECT {{{{ Pipe('{ck}', 'far', instance='sql:w6b_b').columns['datetime'] }}}} AS x, \
         {{{{ Pipe('{ck}', 'far').columns['datetime'] }}}} AS y"
    );
    let child = pipe_on(
        &a,
        "sql:w6b_a",
        &format!("{ck}x"),
        &serde_json::json!({"sql": sql}).to_string(),
    );
    // The provider supplies the foreign instance.
    let mut instances: HashMap<String, Arc<dyn InstanceConnector>> = HashMap::new();
    instances.insert("sql:w6b_b".into(), b.clone());
    a.set_instance_provider(Arc::new(Static(instances)));
    let prepared = a.prepare_pipe(&child).await;
    assert_eq!(
        prepared.parameters()["sql"].py_str(),
        "SELECT far_dt AS x, decoy_dt AS y"
    );
    // Raw parameters still hold the templates.
    assert!(
        prepared.raw_parameters()["sql"]
            .py_str()
            .contains("{{ Pipe(")
    );
    let _ = (std::fs::remove_file(pa), std::fs::remove_file(pb));
}

fn internal_pipe(c: &Ctx, name: &str) -> Pipe {
    let table = format!(
        "w6b_{}_{name}",
        &uuid::Uuid::new_v4().simple().to_string()[..6]
    );
    Pipe::builder("mrsm", &table)
        .instance(&c.conn.keys())
        .kwarg("temporary", true)
        .kwarg("static", true)
        .kwarg("autoincrement", false)
        .kwarg("upsert", true)
        .kwarg("columns", params(r#"{"primary":"key"}"#))
        .kwarg("dtypes", params(r#"{"key":"string","value":"int"}"#))
        .kwarg("target", table.clone())
        .build()
        .unwrap()
}

fn kv(key: &str, value: i64) -> Doc {
    doc(vec![("key", s(key)), ("value", i(value))])
}

// MySQL / MariaDB / MSSQL / Oracle index internal string keys as 255-character columns: a longer key
// is refused up front with a clear message; other flavors keep storing it.
flavor_tests!(internal_key_length_bound, |c: Ctx| async move {
    let pipe = internal_pipe(&c, "len");
    let fits = "k".repeat(255);
    ok(c.sync(&pipe, vec![kv(&fits, 1)]).await);
    let too_long = "k".repeat(256);
    let res = c.sync(&pipe, vec![kv(&too_long, 2)]).await;
    if c.is_mysql() || c.is_mssql() || c.is_oracle() {
        assert!(!res.success, "{}", res.message);
        assert!(
            res.message.contains("exceeds 255 characters"),
            "{}",
            res.message
        );
        assert_eq!(c.docs(&pipe).await.len(), 1);
    } else {
        assert!(res.success, "{}", res.message);
    }
    let _ = c.conn.drop_pipe(&pipe).await;
});

// DuckDB has no staged upsert template: a batch repeating a key keeps the last row; a null key is
// rejected explicitly.
#[cfg(feature = "duckdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_internal_batch_with_repeated_keys() {
    let Some(c) = ctx("duckdb", "w6bdup").await else {
        return;
    };
    let pipe = internal_pipe(&c, "dup");
    ok(c.sync(&pipe, vec![kv("a", 1), kv("b", 2), kv("a", 3)])
        .await);
    let docs = c.docs(&pipe).await;
    assert_eq!(docs.len(), 2, "{docs:?}");
    let a = docs.iter().find(|d| d["key"] == s("a")).unwrap();
    assert_eq!(as_int(&a["value"]), Some(3));
    // A later batch upserts over the stored rows.
    ok(c.sync(&pipe, vec![kv("b", 5), kv("b", 6)]).await);
    let docs = c.docs(&pipe).await;
    let b = docs.iter().find(|d| d["key"] == s("b")).unwrap();
    assert_eq!(as_int(&b["value"]), Some(6));
    let res = c
        .sync(
            &pipe,
            vec![doc(vec![
                ("key", meerschaum_kernel::value::Value::Null),
                ("value", i(1)),
            ])],
        )
        .await;
    assert!(!res.success, "{}", res.message);
    let _ = c.conn.drop_pipe(&pipe).await;
}
