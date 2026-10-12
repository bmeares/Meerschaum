//! Golden tests for the instance-method query text: replays `fixtures/instance_queries.json`
//! (generated from the v4 Python implementation by `fixtures/gen_instance_queries.py`, with every
//! database read mocked) through the Rust builders and compares byte for byte.

use std::collections::BTreeMap;

use indexmap::IndexMap;
use meerschaum_connector::{DataQuery, DateBound};
use meerschaum_core::{Pipe, Value};
use meerschaum_kernel::value::DateTimeValue;
use meerschaum_sql::flavor::Flavor;
use meerschaum_sql::instance::data::{DataQueryOptions, PipeState, pipe_dtypes};
use meerschaum_sql::instance::sync::AlterColumnsInput;
use meerschaum_sql::instance::tables::instance_table_ddl;
use meerschaum_sql::instance::{InstanceSettings, SqlConnector};
use meerschaum_sql::uri::{ConnectionSpec, ConnectorAttributes};
use serde_json::Value as J;

fn fixture() -> J {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/instance_queries.json"
    );
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn conn(flavor: Flavor) -> SqlConnector {
    let attrs = if matches!(flavor, Flavor::Sqlite) {
        ConnectorAttributes {
            label: Some("gen_sqlite".into()),
            flavor: Some(flavor),
            database: Some("/tmp/_mrsm_rs_gen_instance_queries.db".into()),
            ..Default::default()
        }
    } else {
        ConnectorAttributes {
            label: Some(format!("gen_{}", flavor.as_str())),
            flavor: Some(flavor),
            host: Some("localhost".into()),
            username: Some("u".into()),
            password: Some("p".into()),
            database: Some("d".into()),
            ..Default::default()
        }
    };
    let spec = ConnectionSpec::from_attributes(&attrs).unwrap();
    SqlConnector::connect_lazy("gen", spec, InstanceSettings::defaults(flavor)).unwrap()
}

fn bound(v: &J) -> Option<DateBound> {
    match v {
        J::Null => None,
        J::Number(n) => Some(DateBound::Int(n.as_i64().unwrap())),
        J::Object(o) => {
            let s = o["$dt"].as_str().unwrap();
            Some(match chrono::DateTime::parse_from_rfc3339(s) {
                Ok(dt) => DateBound::Datetime(DateTimeValue::utc(dt.naive_utc())),
                Err(_) => DateBound::Datetime(DateTimeValue::naive(
                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").unwrap(),
                )),
            })
        }
        other => panic!("unexpected bound {other}"),
    }
}

fn core_map(v: &J) -> meerschaum_core::Map {
    Value::from_json_str(&v.to_string()).unwrap().to_map()
}

fn str_map(v: &J) -> IndexMap<String, String> {
    v.as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string()))
        .collect()
}

fn data_query(q: &J) -> (DataQuery, DataQueryOptions) {
    let strings = |k: &str| {
        q.get(k).map(|v| {
            v.as_array()
                .unwrap()
                .iter()
                .map(|s| s.as_str().unwrap().to_string())
                .collect()
        })
    };
    let mut opts = DataQueryOptions::default();
    let order = match q.get("order") {
        None => None,
        Some(J::Null) => {
            opts.no_order = true;
            None
        }
        Some(v) => Some(v.as_str().unwrap().to_string()),
    };
    (
        DataQuery {
            select_columns: strings("select_columns"),
            omit_columns: strings("omit_columns"),
            begin: q.get("begin").and_then(bound),
            end: q.get("end").and_then(bound),
            params: q.get("params").filter(|p| !p.is_null()).map(core_map),
            limit: q.get("limit").and_then(J::as_i64),
            order,
            ..Default::default()
        },
        opts,
    )
}

fn strings(v: &J) -> Vec<String> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn instance_queries_match_python() {
    let fx = fixture();
    let mut conns: BTreeMap<String, SqlConnector> = BTreeMap::new();
    let mut failures: Vec<String> = Vec::new();
    let mut checked = 0usize;
    for case in fx["cases"].as_array().unwrap() {
        let flavor_name = case["flavor"].as_str().unwrap();
        let flavor: Flavor = flavor_name.parse().unwrap();
        let conn = conns
            .entry(flavor_name.to_string())
            .or_insert_with(|| conn(flavor));
        let fname = case["fn"].as_str().unwrap();
        if fname == "instance_tables" {
            let schema = case["schema"].as_str();
            for (table, stmts) in case["result"].as_object().unwrap() {
                checked += 1;
                let got = instance_table_ddl(table, flavor, schema);
                if got != strings(stmts) {
                    failures.push(format!(
                        "instance_tables {flavor_name} {table}:\n  python: {stmts}\n  rust:   {got:?}"
                    ));
                }
            }
            continue;
        }
        let shape = case["shape"].as_str().unwrap();
        let pipe = Pipe::builder("gen", shape)
            .parameters(core_map(&case["parameters"]))
            .build()
            .unwrap();
        let sp = conn.sql_pipe(&pipe).unwrap();
        let cols_types = str_map(&case["cols_types"]);
        let state = PipeState {
            exists: true,
            dtypes: pipe_dtypes(&sp, &cols_types, true),
            explicit_dtypes: pipe_dtypes(&sp, &cols_types, false),
            cols_types: cols_types.clone(),
        };
        let q = &case["query"];
        let expected = &case["result"];
        let got: J = match fname {
            "get_pipe_data_query" => {
                let (dq, opts) = data_query(q);
                match conn.build_data_query(&sp, &state, &dq, &opts).await {
                    Ok(s) => J::String(s),
                    Err(e) => serde_json::json!({ "error": e.to_string() }),
                }
            }
            "get_sync_time_query" => {
                let params = q.get("params").filter(|p| !p.is_null()).map(core_map);
                conn.get_sync_time_query(
                    &sp,
                    &state,
                    params.as_ref(),
                    q["newest"].as_bool().unwrap(),
                )
                .map(J::String)
                .unwrap_or(J::Null)
            }
            "get_pipe_rowcount_query" => {
                let (dq, _) = data_query(q);
                conn.get_pipe_rowcount_query(&sp, &state, &dq)
                    .unwrap()
                    .map(J::String)
                    .unwrap_or(J::Null)
            }
            "clear_pipe_query" => {
                let (dq, _) = data_query(q);
                J::String(conn.get_clear_pipe_query(&sp, &state, &dq).unwrap())
            }
            "get_add_columns_queries" => {
                let df = str_map(&q["df"]);
                serde_json::to_value(conn.add_columns_queries_for(&sp, &df, &cols_types)).unwrap()
            }
            "get_alter_columns_queries" => {
                let df = str_map(&q["df"]);
                let input = AlterColumnsInput {
                    numeric_cols: df
                        .iter()
                        .filter(|(_, t)| t.starts_with("numeric"))
                        .map(|(c, _)| c.clone())
                        .collect(),
                    df_cols_types: df,
                    db_cols_types: cols_types.clone(),
                    pipe_dtypes: state.dtypes.clone(),
                    explicit_dtypes: state.explicit_dtypes.clone(),
                    new_special_cols: IndexMap::new(),
                    session_id: "abc".into(),
                };
                serde_json::to_value(conn.alter_columns_queries_for(&sp, &input).0).unwrap()
            }
            other => panic!("unknown fn {other}"),
        };
        checked += 1;
        if &got != expected {
            failures.push(format!(
                "{fname} {flavor_name} {shape} {q}:\n  python: {expected}\n  rust:   {got}"
            ));
        }
    }
    let n = failures.len();
    let shown: Vec<&String> = failures.iter().take(25).collect();
    assert!(
        failures.is_empty(),
        "{n} of {checked} cases differ (first 25):\n{}",
        shown
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    );
    eprintln!("{checked} instance query cases match.");
}
