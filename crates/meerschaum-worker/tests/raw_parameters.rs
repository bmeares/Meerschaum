//! Raw-vs-resolved parameters: a forwarded `pipe.sync()` registers the pipe with its template
//! text intact, and edits keep it too (Python-free; sqlite in a temporary directory).

use meerschaum_core::{Map, Pipe, Value as CValue};
use meerschaum_kernel::value::Value as KValue;
use meerschaum_sql::SqlConnector;
use meerschaum_worker::FetchChunk;
use meerschaum_worker::callback::sync_chunk_native;

const SQL: &str = "SELECT * FROM {{ Pipe('sql:rs', 'other') }}";

fn pipe() -> Pipe {
    let mut params = Map::new();
    params.insert("target".into(), CValue::Str("{{ self.metric }}_tbl".into()));
    params.insert("sql".into(), CValue::Str(SQL.into()));
    let mut cols = Map::new();
    cols.insert("primary".into(), CValue::Str("id".into()));
    params.insert("columns".into(), CValue::Map(cols));
    Pipe::builder("plugin:wk_raw", "weather")
        .instance("sql:rs")
        .parameters(params)
        .build()
        .unwrap()
}

fn assert_raw(attrs: &Map) {
    let Some(CValue::Map(p)) = attrs.get("parameters") else {
        panic!("no parameters: {attrs:?}");
    };
    assert_eq!(p["target"], CValue::Str("{{ self.metric }}_tbl".into()));
    assert_eq!(p["sql"], CValue::Str(SQL.into()));
}

#[tokio::test]
async fn forwarded_sync_and_edit_keep_templates() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite:///{}", dir.path().join("rs.db").display());
    let instance = SqlConnector::from_uri("rs", &uri).await.unwrap();
    let p = pipe();
    assert_eq!(p.parameters()["target"], CValue::Str("weather_tbl".into()));

    let docs = vec![
        [("id".to_string(), KValue::Int(1))]
            .into_iter()
            .collect::<indexmap::IndexMap<_, _>>(),
    ];
    let res = sync_chunk_native(&instance, &p, FetchChunk::Docs(docs), &Map::new()).await;
    assert!(res.success, "{}", res.message);
    assert_raw(&instance.get_pipe_attributes(&p).await.unwrap());

    let mut edited = p.clone();
    let mut patch = Map::new();
    patch.insert("tags".into(), CValue::List(vec![CValue::Str("t".into())]));
    edited.update_parameters(patch);
    assert!(instance.edit_pipe(&edited, true).await.success);
    let attrs = instance.get_pipe_attributes(&p).await.unwrap();
    assert_raw(&attrs);
}
