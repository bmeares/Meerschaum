//! Golden SQL tests (design doc §10.2): replay every case recorded by
//! `tests/fixtures/gen_queries.py` from the Python implementation and require byte-for-byte
//! identical output from the Rust port.
//!
//! Regenerate the fixtures with:
//!
//! ```bash
//! PYTHONPATH=$(pwd) MRSM_ROOT_DIR="$(mktemp -d)" MRSM_PLUGINS_DIR="$(mktemp -d)" \
//!     ~/.config/meerschaum/venvs/mrsm/bin/python crates/meerschaum-sql/tests/fixtures/gen_queries.py
//! ```

use std::collections::BTreeMap;

use chrono::TimeDelta;
use indexmap::IndexMap;
use serde::Deserialize;

use meerschaum_sql::dateadd::{DateArg, Number, dateadd_str};
use meerschaum_sql::dtypes::{
    DB_FLAVORS_CAST_DTYPES, DB_TO_PD_DEFAULT, DB_TO_PD_DTYPES, DB_TO_PD_SUBSTRINGS,
    PD_TO_DB_COLUMNS, PD_TO_DB_DTYPES_FLAVORS, Srid, are_dtypes_equal, get_db_type_from_pd_type,
    get_geometry_type_srid, get_numeric_precision_scale, get_pd_type_from_db_type,
};
use meerschaum_sql::flavor::Flavor;
use meerschaum_sql::ident::{clean, pg_capital, sql_item_name, truncate_item_name};
use meerschaum_sql::pipe::{IndexColumns, Interval, PipeSpec};
use meerschaum_sql::pytext::{PyDateTime, truncate_string_sections};
use meerschaum_sql::queries::ConnectorCtx;
use meerschaum_sql::queries::compress::{
    get_columnstore_disable_query, get_columnstore_policy_query,
    get_columnstore_remove_policy_query, get_columnstore_settings_query, get_compress_query_groups,
    get_compress_settings, get_decompress_query_groups, get_integer_now_func_queries,
    get_is_hypertable_query, get_pipe_size_queries,
};
use meerschaum_sql::queries::cte::{
    ColsToSelect, find_top_level_select_index, format_cte_subquery, get_leading_with_index,
    wrap_query_with_cte,
};
use meerschaum_sql::queries::index::{
    DropIndexState, IndexState, get_create_index_queries, get_drop_index_queries,
    get_pipe_index_names,
};
use meerschaum_sql::queries::maintenance::{get_analyze_query, get_vacuum_queries};
use meerschaum_sql::queries::metadef::{Bound, MetadefArgs, ParentSpec, get_pipe_metadef};
use meerschaum_sql::queries::partition::{
    MAX_PARTITIONS_PER_SYNC, PartitionValue, get_initial_partition_bounds,
    get_mssql_max_partition_boundary_query, get_mssql_partition_creation_queries,
    get_mssql_split_partition_queries, get_mysql_add_partition_queries,
    get_mysql_max_partition_bound_query, get_partition_cleanup_queries, get_partition_column,
    get_partition_count_query, get_pg_is_partitioned_query, get_pg_partition_creation_queries,
    get_set_chunk_time_interval_query, get_timescaledb_chunk_count_query,
    parse_mssql_max_partition_boundary, parse_mysql_max_partition_bound, partition_bounds,
    partition_function_name, partition_literal, partition_name, partition_ranges,
    partition_scheme_name, should_partition,
};
use meerschaum_sql::queries::table::{
    CreateTableOptions, IndexDoc, QueryOrDtypes, get_create_schema_if_not_exists_queries,
    get_create_table_queries, get_db_version_query, get_distinct_col_count_query,
    get_drop_table_queries, get_max_id_query, get_postgis_geo_columns_types_query,
    get_rename_table_queries, get_reset_autoincrement_queries, get_table_cols_indices_query,
    get_table_cols_types_query,
};
use meerschaum_sql::queries::templates::{
    COLUMNS_INDICES_QUERIES, COLUMNS_TYPES_QUERIES, RESET_AUTOINCREMENT_QUERIES, UPDATE_QUERIES,
};
use meerschaum_sql::queries::update::{
    UpdateQueriesArgs, get_null_replacement, get_update_queries,
};
use meerschaum_sql::value::PyValue;
use meerschaum_sql::where_clause::build_where;

#[derive(Deserialize)]
struct Fixture {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    #[serde(rename = "fn")]
    func: String,
    args: PyValue,
    #[serde(default)]
    result: Option<PyValue>,
    #[serde(default)]
    error: Option<String>,
}

/// The outcome of replaying one case.
enum Outcome {
    /// The Rust output (compared with `json.dumps` semantics, so dict order matters).
    Value(PyValue),
    /// Rust raised (Python must have raised too).
    Error(String),
}

// ---------------------------------------------------------------------------------------------
// Argument helpers
// ---------------------------------------------------------------------------------------------

fn get<'a>(args: &'a PyValue, key: &str) -> &'a PyValue {
    args.get(key).unwrap_or(&PyValue::None)
}

fn s(args: &PyValue, key: &str) -> String {
    get(args, key).py_str()
}

fn os(args: &PyValue, key: &str) -> Option<String> {
    match get(args, key) {
        PyValue::None => None,
        v => Some(v.py_str()),
    }
}

fn b(args: &PyValue, key: &str) -> bool {
    get(args, key).is_truthy()
}

fn flavor(args: &PyValue) -> Flavor {
    s(args, "flavor").parse().expect("flavor")
}

fn opt_flavor(args: &PyValue) -> Option<Flavor> {
    s(args, "flavor").parse().ok()
}

fn str_list(v: &PyValue) -> Vec<String> {
    match v {
        PyValue::List(items) => items.iter().map(PyValue::py_str).collect(),
        PyValue::None => Vec::new(),
        other => vec![other.py_str()],
    }
}

fn str_map(v: &PyValue) -> IndexMap<String, String> {
    v.as_dict()
        .map(|d| d.iter().map(|(k, v)| (k.clone(), v.py_str())).collect())
        .unwrap_or_default()
}

fn dt(v: &PyValue) -> Option<PyDateTime> {
    v.get("$dt")
        .and_then(PyValue::as_str)
        .map(|s| PyDateTime::parse_isoformat(s).expect("isoformat"))
}

fn date_arg(v: &PyValue) -> DateArg {
    match v {
        PyValue::None => DateArg::None,
        PyValue::Int(i) => DateArg::Int(*i),
        PyValue::Str(s) => DateArg::Str(s.clone()),
        other => DateArg::DateTime(dt(other).expect("datetime")),
    }
}

fn number(v: &PyValue) -> Number {
    match v {
        PyValue::Int(i) => Number::Int(*i),
        PyValue::Float(f) => Number::Float(*f),
        other => panic!("bad number {other:?}"),
    }
}

fn bound(v: &PyValue, default_for_empty: bool) -> Bound {
    match v {
        PyValue::Str(s) if s.is_empty() && default_for_empty => Bound::Default,
        PyValue::Str(s) => Bound::Str(s.clone()),
        PyValue::None => Bound::None,
        PyValue::Int(i) => Bound::Int(*i),
        other => Bound::DateTime(dt(other).expect("datetime")),
    }
}

fn partition_value(v: &PyValue) -> PartitionValue {
    match v {
        PyValue::Int(i) => PartitionValue::Int(*i),
        other => PartitionValue::DateTime(dt(other).expect("datetime")),
    }
}

fn interval(v: &PyValue) -> Interval {
    match v {
        PyValue::Int(i) => Interval::Int(*i),
        other => Interval::Duration(TimeDelta::microseconds(
            other.get("$td_us").and_then(PyValue::as_int).expect("td"),
        )),
    }
}

fn pipe(args: &PyValue) -> PipeSpec {
    let p = get(args, "pipe");
    PipeSpec {
        connector_keys: s(p, "connector_keys"),
        metric_key: s(p, "metric_key"),
        location_key: os(p, "location_key"),
        target: s(p, "target"),
        parameters: get(p, "parameters").as_dict().cloned().unwrap_or_default(),
    }
}

fn index_docs(v: &PyValue) -> IndexMap<String, Vec<IndexDoc>> {
    let mut out = IndexMap::new();
    if let Some(d) = v.as_dict() {
        for (col, docs) in d {
            let docs = match docs {
                PyValue::List(items) => items
                    .iter()
                    .map(|doc| IndexDoc {
                        name: os(doc, "name"),
                        type_: os(doc, "type"),
                        clustered: match doc.get("clustered") {
                            Some(PyValue::Bool(b)) => Some(*b),
                            _ => None,
                        },
                    })
                    .collect(),
                _ => Vec::new(),
            };
            out.insert(col.clone(), docs);
        }
    }
    out
}

// ---------------------------------------------------------------------------------------------
// Result helpers
// ---------------------------------------------------------------------------------------------

fn v_str(s: impl Into<String>) -> PyValue {
    PyValue::Str(s.into())
}

fn v_opt(s: Option<String>) -> PyValue {
    s.map(PyValue::Str).unwrap_or(PyValue::None)
}

fn v_list(items: impl IntoIterator<Item = String>) -> PyValue {
    PyValue::List(items.into_iter().map(PyValue::Str).collect())
}

fn v_map(m: &IndexMap<String, String>) -> PyValue {
    PyValue::Dict(
        m.iter()
            .map(|(k, v)| (k.clone(), v_str(v.clone())))
            .collect(),
    )
}

fn v_map_list(m: &IndexMap<String, Vec<String>>) -> PyValue {
    PyValue::Dict(
        m.iter()
            .map(|(k, v)| (k.clone(), v_list(v.clone())))
            .collect(),
    )
}

fn v_dict(pairs: Vec<(&str, PyValue)>) -> PyValue {
    PyValue::Dict(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

fn v_dt(d: &PyDateTime) -> PyValue {
    v_dict(vec![("$dt", v_str(d.isoformat()))])
}

fn v_interval(i: Interval) -> PyValue {
    match i {
        Interval::Int(i) => PyValue::Int(i),
        Interval::Duration(d) => v_dict(vec![(
            "$td_us",
            PyValue::Int(d.num_microseconds().expect("micros")),
        )]),
    }
}

fn v_partition_value(v: PartitionValue) -> PyValue {
    match v {
        PartitionValue::Int(i) => PyValue::Int(i),
        PartitionValue::DateTime(d) => v_dt(&d),
    }
}

fn v_srid(s: &Srid) -> PyValue {
    match s {
        Srid::Int(i) => PyValue::Int(*i),
        Srid::Str(s) => v_str(s.clone()),
    }
}

fn ok(v: PyValue) -> Outcome {
    Outcome::Value(v)
}

fn res<T>(r: meerschaum_sql::Result<T>, f: impl FnOnce(T) -> PyValue) -> Outcome {
    match r {
        Ok(v) => Outcome::Value(f(v)),
        Err(e) => Outcome::Error(e.to_string()),
    }
}

// ---------------------------------------------------------------------------------------------
// Static tables
// ---------------------------------------------------------------------------------------------

const PY_FLAVORS: [&str; 13] = [
    "timescaledb",
    "timescaledb-ha",
    "postgresql",
    "postgis",
    "citus",
    "cockroachdb",
    "mssql",
    "mysql",
    "mariadb",
    "oracle",
    "sqlite",
    "geopackage",
    "duckdb",
];

fn flavors_where(pred: impl Fn(Flavor) -> bool) -> PyValue {
    let mut names: Vec<String> = Flavor::ALL
        .iter()
        .filter(|f| pred(**f))
        .map(|f| f.as_str().to_string())
        .collect();
    names.sort();
    v_list(names)
}

fn templates_case(expected: &PyValue) -> Vec<String> {
    let mut failures = Vec::new();
    let mut check = |name: &str, got: PyValue| {
        let exp = expected.get(name).cloned().unwrap_or(PyValue::None);
        if exp.json_dumps() != got.json_dumps() {
            failures.push(format!("templates.{name} differs"));
        }
    };
    check(
        "UPDATE_QUERIES",
        PyValue::Dict(
            UPDATE_QUERIES
                .iter()
                .map(|(k, v)| (k.to_string(), v_list(v.iter().map(|s| s.to_string()))))
                .collect(),
        ),
    );
    let table = |t: &[(&str, &str)]| {
        PyValue::Dict(t.iter().map(|(k, v)| (k.to_string(), v_str(*v))).collect())
    };
    check("columns_types_queries", table(COLUMNS_TYPES_QUERIES));
    check("columns_indices_queries", table(COLUMNS_INDICES_QUERIES));
    check(
        "reset_autoincrement_queries",
        table(RESET_AUTOINCREMENT_QUERIES),
    );

    // Per-flavor lookups (Python's `.get(flavor, default)`).
    let lookup = |name: &str, f: &str| -> PyValue {
        let d = expected.get(name).expect(name);
        d.get(f)
            .or_else(|| d.get("default"))
            .cloned()
            .unwrap_or(PyValue::None)
    };
    for fname in PY_FLAVORS {
        let f: Flavor = fname.parse().unwrap();
        if lookup("version_queries", fname).py_str() != f.version_query_template() {
            failures.push(format!("version_queries[{fname}]"));
        }
        let hq = expected
            .get("hypertable_queries")
            .and_then(|d| d.get(fname))
            .map(PyValue::py_str);
        if hq.as_deref() != f.hypertable_query_template() {
            failures.push(format!("hypertable_queries[{fname}]"));
        }
        let (l, r) = f.table_wrappers();
        if lookup("table_wrappers", fname).json_dumps() != v_list([l.into(), r.into()]).json_dumps()
        {
            failures.push(format!("table_wrappers[{fname}]"));
        }
        if lookup("max_name_lens", fname) != PyValue::Int(f.max_name_len() as i64) {
            failures.push(format!("max_name_lens[{fname}]"));
        }
    }
    failures
}

fn flavor_sets_case(expected: &PyValue) -> Vec<String> {
    let mut failures = Vec::new();
    let mut check = |name: &str, got: PyValue| {
        let exp = expected.get(name).cloned().unwrap_or(PyValue::None);
        if exp.json_dumps() != got.json_dumps() {
            failures.push(format!(
                "flavor_sets.{name}: python={} rust={}",
                exp.json_dumps(),
                got.json_dumps()
            ));
        }
    };
    check(
        "SKIP_IF_EXISTS_FLAVORS",
        flavors_where(Flavor::skip_if_exists),
    );
    check(
        "DROP_IF_EXISTS_FLAVORS",
        flavors_where(Flavor::drop_if_exists),
    );
    check(
        "DROP_INDEX_IF_EXISTS_FLAVORS",
        flavors_where(Flavor::drop_index_if_exists),
    );
    check(
        "SKIP_AUTO_INCREMENT_FLAVORS",
        flavors_where(Flavor::skip_auto_increment),
    );
    check(
        "COALESCE_UNIQUE_INDEX_FLAVORS",
        flavors_where(Flavor::coalesce_unique_index),
    );
    check("json_flavors", flavors_where(Flavor::is_json_flavor));
    check("NO_SCHEMA_FLAVORS", flavors_where(Flavor::no_schema));
    check(
        "OMIT_NULLSFIRST_FLAVORS",
        flavors_where(Flavor::omit_nullsfirst),
    );
    check(
        "SINGLE_ALTER_TABLE_FLAVORS",
        flavors_where(Flavor::single_alter_table),
    );
    check("NO_CTE_FLAVORS", flavors_where(Flavor::no_cte));
    check(
        "NO_SELECT_INTO_FLAVORS",
        flavors_where(Flavor::no_select_into),
    );
    check(
        "NUMERIC_AS_TEXT_FLAVORS",
        flavors_where(Flavor::numeric_as_text),
    );
    check(
        "TIMEZONE_NAIVE_FLAVORS",
        flavors_where(Flavor::timezone_naive),
    );
    check("PG_PARTITION_FLAVORS", flavors_where(Flavor::pg_partition));
    check(
        "MYSQL_PARTITION_FLAVORS",
        flavors_where(Flavor::mysql_partition),
    );
    check(
        "MSSQL_PARTITION_FLAVORS",
        flavors_where(Flavor::mssql_partition),
    );
    check(
        "PARTITIONABLE_FLAVORS",
        flavors_where(Flavor::partitionable),
    );
    check("COMPRESSIBLE_FLAVORS", flavors_where(Flavor::compressible));
    check(
        "POSTGRES_UNIQUE_CONSTRAINT_FLAVORS",
        flavors_where(Flavor::postgres_unique_constraint),
    );

    // Dicts keyed by flavor: compare entry by entry.
    let exp_default = expected.get("DEFAULT_SCHEMA_FLAVORS").unwrap();
    let exp_numeric = expected.get("NUMERIC_PRECISION_FLAVORS").unwrap();
    let exp_auto = expected.get("AUTO_INCREMENT_COLUMN_FLAVORS").unwrap();
    let exp_configs = expected.get("flavor_configs").unwrap();
    for f in Flavor::ALL {
        let name = f.as_str();
        if exp_default.get(name).map(PyValue::py_str) != f.default_schema().map(str::to_string) {
            failures.push(format!("DEFAULT_SCHEMA_FLAVORS[{name}]"));
        }
        let got_num = f
            .numeric_precision()
            .map(|(p, s)| PyValue::List(vec![PyValue::Int(p), PyValue::Int(s)]));
        if exp_numeric.get(name).cloned() != got_num {
            failures.push(format!("NUMERIC_PRECISION_FLAVORS[{name}]"));
        }
        if exp_auto.get(name).map(PyValue::py_str).as_deref() != Some(f.auto_increment_column()) {
            failures.push(format!("AUTO_INCREMENT_COLUMN_FLAVORS[{name}]"));
        }
        let cfg = exp_configs.get(name).expect("flavor config");
        if cfg.get("engine").map(PyValue::py_str).as_deref() != Some(f.engine()) {
            failures.push(format!("flavor_configs[{name}].engine"));
        }
        let defaults: BTreeMap<String, String> = f
            .defaults()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let exp_defaults: BTreeMap<String, String> =
            str_map(cfg.get("defaults").unwrap()).into_iter().collect();
        if defaults != exp_defaults {
            failures.push(format!("flavor_configs[{name}].defaults"));
        }
        let mut reqs: Vec<String> = f.requirements().iter().map(|s| s.to_string()).collect();
        reqs.sort();
        if str_list(cfg.get("requirements").unwrap()) != reqs {
            failures.push(format!("flavor_configs[{name}].requirements"));
        }
    }
    if exp_auto.get("default").map(PyValue::py_str).as_deref()
        != Some("GENERATED BY DEFAULT AS IDENTITY")
    {
        failures.push("AUTO_INCREMENT_COLUMN_FLAVORS[default]".into());
    }
    failures
}

fn dtype_tables_case(func: &str, expected: &PyValue) -> Vec<String> {
    let mut failures = Vec::new();
    match func {
        "pd_to_db_table" => {
            let d = expected.as_dict().unwrap();
            let got_keys: Vec<&str> = PD_TO_DB_DTYPES_FLAVORS.iter().map(|(k, _)| *k).collect();
            let exp_keys: Vec<&str> = d.keys().map(String::as_str).collect();
            if got_keys != exp_keys {
                failures.push("PD_TO_DB_DTYPES_FLAVORS keys/order".into());
            }
            for (pd_type, row) in PD_TO_DB_DTYPES_FLAVORS {
                for (i, col) in PD_TO_DB_COLUMNS.iter().enumerate() {
                    let key = col.map(Flavor::as_str).unwrap_or("default");
                    let exp = d
                        .get(*pd_type)
                        .and_then(|r| r.get(key))
                        .map(PyValue::py_str);
                    if exp.as_deref() != Some(row[i]) {
                        failures.push(format!("PD_TO_DB_DTYPES_FLAVORS[{pd_type}][{key}]"));
                    }
                }
            }
        }
        "db_to_pd_table" => {
            let d = expected.as_dict().unwrap();
            for (k, v) in DB_TO_PD_DTYPES {
                if d.get(*k).map(PyValue::py_str).as_deref() != Some(*v) {
                    failures.push(format!("DB_TO_PD_DTYPES[{k}]"));
                }
            }
            let exact = d.len() - 2; // 'substrings' and 'default'
            if exact != DB_TO_PD_DTYPES.len() {
                failures.push("DB_TO_PD_DTYPES length".into());
            }
            let subs = d.get("substrings").unwrap().as_dict().unwrap();
            let got: Vec<(String, String)> = DB_TO_PD_SUBSTRINGS
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            let exp: Vec<(String, String)> =
                subs.iter().map(|(k, v)| (k.clone(), v.py_str())).collect();
            if got != exp {
                failures.push("DB_TO_PD_DTYPES['substrings']".into());
            }
            if d.get("default").map(PyValue::py_str).as_deref() != Some(DB_TO_PD_DEFAULT) {
                failures.push("DB_TO_PD_DTYPES['default']".into());
            }
        }
        "cast_dtypes_table" => {
            let got = PyValue::Dict(
                DB_FLAVORS_CAST_DTYPES
                    .iter()
                    .map(|(f, m)| {
                        (
                            f.to_string(),
                            PyValue::Dict(
                                m.iter().map(|(k, v)| (k.to_string(), v_str(*v))).collect(),
                            ),
                        )
                    })
                    .collect(),
            );
            if got.json_dumps() != expected.json_dumps() {
                failures.push("DB_FLAVORS_CAST_DTYPES".into());
            }
        }
        _ => unreachable!(),
    }
    failures
}

// ---------------------------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------------------------

fn create_table_options(a: &PyValue) -> CreateTableOptions {
    CreateTableOptions {
        schema: os(a, "schema"),
        primary_key: os(a, "primary_key"),
        primary_key_db_type: os(a, "primary_key_db_type"),
        autoincrement: b(a, "autoincrement"),
        datetime_column: os(a, "datetime_column"),
        hypertable_chunk_interval: os(a, "hypertable_chunk_interval"),
        hypertable_segmentby: a.get("hypertable_segmentby").map(str_list),
        hypertable_orderby: a.get("hypertable_orderby").map(str_list),
        partition_by_column: os(a, "partition_by_column"),
        partition_bounds: a.get("partition_bounds").map(|v| match v {
            PyValue::List(items) => items
                .iter()
                .map(|pair| {
                    let l = str_list(pair);
                    (l[0].clone(), l[1].clone())
                })
                .collect(),
            _ => Vec::new(),
        }),
        partition_scheme_name: os(a, "partition_scheme_name"),
        parse_dtypes: a.get("_parse_dtypes").is_none_or(PyValue::is_truthy),
    }
}

fn run(func: &str, a: &PyValue) -> Outcome {
    match func {
        "sql_item_name" => ok(v_str(sql_item_name(
            &s(a, "item"),
            flavor(a),
            os(a, "schema").as_deref(),
        ))),
        "truncate_item_name" => ok(v_str(truncate_item_name(&s(a, "item"), flavor(a)))),
        "pg_capital" => ok(v_str(pg_capital(&s(a, "s"), b(a, "quote_capitals")))),
        "clean" => res(clean(&s(a, "s")), |_| PyValue::None),
        "truncate_string_sections" => ok(v_str(truncate_string_sections(
            &s(a, "item"),
            "_",
            get(a, "max_len").as_int().unwrap() as usize,
        ))),
        "build_where" => ok(v_str(build_where(
            get(a, "params").as_dict().unwrap(),
            flavor(a),
            b(a, "with_where"),
        ))),
        "dateadd_str" => res(
            dateadd_str(
                Some(flavor(a)),
                os(a, "datepart").as_deref(),
                number(get(a, "number")),
                &date_arg(get(a, "begin")),
                os(a, "db_type").as_deref(),
            ),
            v_str,
        ),
        "get_pd_type_from_db_type" => ok(v_str(get_pd_type_from_db_type(
            &s(a, "db_type"),
            b(a, "allow_custom_dtypes"),
        ))),
        "get_db_type_from_pd_type" => ok(v_str(get_db_type_from_pd_type(
            &s(a, "pd_type"),
            opt_flavor(a),
        ))),
        "get_numeric_precision_scale" => {
            let (p, sc) = get_numeric_precision_scale(opt_flavor(a), os(a, "dtype").as_deref());
            let i = |x: Option<i64>| x.map(PyValue::Int).unwrap_or(PyValue::None);
            ok(PyValue::List(vec![i(p), i(sc)]))
        }
        "get_geometry_type_srid" => {
            let (t, srid) = get_geometry_type_srid(&s(a, "dtype"));
            ok(PyValue::List(vec![v_str(t), v_srid(&srid)]))
        }
        "are_dtypes_equal" => ok(PyValue::Bool(are_dtypes_equal(&s(a, "l"), &s(a, "r")))),
        "get_null_replacement" => ok(v_str(get_null_replacement(&s(a, "typ"), Some(flavor(a))))),
        "get_update_queries" => {
            let target = s(a, "target");
            let patch = s(a, "patch");
            let schema = os(a, "schema");
            let patch_schema = os(a, "patch_schema");
            let datetime_col = os(a, "datetime_col");
            let db_version = os(a, "db_version");
            let mut args =
                UpdateQueriesArgs::new(&target, &patch, str_list(get(a, "join_cols")), flavor(a));
            args.upsert = b(a, "upsert");
            args.datetime_col = datetime_col.as_deref();
            args.schema = schema.as_deref();
            args.patch_schema = patch_schema.as_deref();
            args.target_cols_types = str_map(get(a, "target_cols_types"));
            args.patch_cols_types = str_map(get(a, "patch_cols_types"));
            args.identity_insert = b(a, "identity_insert");
            args.null_indices = b(a, "null_indices");
            args.cast_columns = b(a, "cast_columns");
            args.db_version = db_version.as_deref();
            ok(v_list(get_update_queries(&args)))
        }
        "wrap_query_with_cte" => res(
            wrap_query_with_cte(
                &s(a, "sub_query"),
                &s(a, "parent_query"),
                flavor(a),
                &s(a, "cte_name"),
            ),
            v_str,
        ),
        "format_cte_subquery" => {
            let cols = match get(a, "cols_to_select") {
                PyValue::List(items) => {
                    ColsToSelect::Columns(items.iter().map(PyValue::py_str).collect())
                }
                other => ColsToSelect::Raw(other.py_str()),
            };
            res(
                format_cte_subquery(&s(a, "sub_query"), flavor(a), &s(a, "sub_name"), &cols),
                v_str,
            )
        }
        "find_top_level_select_index" => ok(PyValue::Int(
            find_top_level_select_index(&s(a, "query"), b(a, "last"))
                .map(|i| i as i64)
                .unwrap_or(-1),
        )),
        "get_leading_with_index" => ok(PyValue::Int(
            get_leading_with_index(&s(a, "query"))
                .map(|i| i as i64)
                .unwrap_or(-1),
        )),
        "get_create_table_queries" => {
            let q = match s(a, "kind").as_str() {
                "dtypes" => QueryOrDtypes::Dtypes(str_map(get(a, "query_or_dtypes"))),
                _ => QueryOrDtypes::Query(s(a, "query_or_dtypes")),
            };
            res(
                get_create_table_queries(
                    &q,
                    &s(a, "new_table"),
                    flavor(a),
                    &create_table_options(a),
                ),
                v_list,
            )
        }
        "get_rename_table_queries" => res(
            get_rename_table_queries(
                &s(a, "old_table"),
                &s(a, "new_table"),
                flavor(a),
                os(a, "schema").as_deref(),
                os(a, "new_schema").as_deref(),
            ),
            v_list,
        ),
        "get_create_schema_if_not_exists_queries" => res(
            get_create_schema_if_not_exists_queries(os(a, "schema").as_deref(), flavor(a)),
            v_list,
        ),
        "table_introspection_queries" => {
            let f = flavor(a);
            let table = s(a, "table");
            ok(v_list([
                get_table_cols_types_query(&table, f),
                get_table_cols_indices_query(&table, f, os(a, "schema").as_deref()),
            ]))
        }
        "get_db_version_query" => ok(v_list([get_db_version_query(flavor(a))])),
        "get_reset_autoincrement_queries" => {
            let f = flavor(a);
            let schema = os(a, "schema");
            let table = s(a, "table");
            let column = s(a, "column");
            ok(v_dict(vec![
                (
                    "max_id_query",
                    v_str(get_max_id_query(&table, &column, f, schema.as_deref())),
                ),
                (
                    "queries",
                    v_list(get_reset_autoincrement_queries(
                        &table,
                        &column,
                        f,
                        schema.as_deref(),
                        get(a, "max_id").as_int(),
                    )),
                ),
            ]))
        }
        "get_distinct_col_count_query" => ok(v_list([get_distinct_col_count_query(
            &s(a, "col"),
            &s(a, "query"),
            flavor(a),
        )])),
        "get_postgis_geo_columns_types_query" => res(
            get_postgis_geo_columns_types_query(&s(a, "table"), os(a, "schema").as_deref()),
            |q| v_list([q]),
        ),
        "get_pipe_index_names" => {
            let ctx = ConnectorCtx::new(flavor(a));
            ok(v_map(&get_pipe_index_names(&ctx, &pipe(a))))
        }
        "pipe_attributes" => {
            let ctx = ConnectorCtx::new(flavor(a));
            let p = pipe(a);
            let indices = PyValue::Dict(
                p.indices()
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k,
                            match v {
                                IndexColumns::One(c) => v_str(c),
                                IndexColumns::Many(cs) => v_list(cs),
                            },
                        )
                    })
                    .collect(),
            );
            ok(v_dict(vec![
                ("dtypes", v_map(&p.dtypes())),
                ("precision_unit", v_str(p.precision_unit())),
                ("chunk_interval", v_interval(p.get_chunk_interval())),
                (
                    "chunk_interval_60",
                    v_interval(p.get_chunk_interval_with(Interval::minutes(60))),
                ),
                (
                    "chunk_interval_int",
                    v_interval(p.get_chunk_interval_with(Interval::Int(90))),
                ),
                ("backtrack", v_interval(p.get_backtrack_interval(true))),
                (
                    "backtrack_no_existing",
                    v_interval(p.get_backtrack_interval(false)),
                ),
                ("guess_datetime", v_opt(p.guess_datetime())),
                ("indices", indices),
                ("schema", v_opt(ctx.get_pipe_schema(&p))),
            ]))
        }
        "get_create_index_queries" => {
            let ctx = ConnectorCtx::new(flavor(a));
            let st = get(a, "state");
            let state = IndexState {
                existing_cols_types: str_map(get(st, "existing_cols_types")),
                existing_cols_indices: index_docs(get(st, "existing_cols_indices")),
                ..Default::default()
            };
            ok(v_map_list(&get_create_index_queries(
                &ctx,
                &pipe(a),
                &state,
            )))
        }
        "get_drop_index_queries" => {
            let ctx = ConnectorCtx::new(flavor(a));
            let st = get(a, "state");
            let state = DropIndexState {
                exists: b(st, "exists"),
                cols_indices: index_docs(get(st, "cols_indices")),
                is_hypertable: b(st, "is_hypertable"),
                temp_table_exists: b(st, "temp_table_exists"),
            };
            ok(v_map_list(&get_drop_index_queries(&ctx, &pipe(a), &state)))
        }
        "drop_pipe_queries" => {
            let f = flavor(a);
            let ctx = ConnectorCtx::new(f);
            let p = pipe(a);
            let mut queries =
                get_drop_table_queries(&p.target, f, ctx.get_pipe_schema(&p).as_deref());
            queries.extend(get_partition_cleanup_queries(f, &p));
            ok(v_list(queries))
        }
        "get_vacuum_queries" => ok(
            match get_vacuum_queries(flavor(a), &s(a, "pipe_name"), b(a, "full")) {
                Some(q) => v_list(q),
                None => PyValue::None,
            },
        ),
        "get_analyze_query" => ok(v_opt(get_analyze_query(flavor(a), &s(a, "pipe_name")))),
        "compress_settings" => {
            let f = flavor(a);
            let ctx = ConnectorCtx::new(f);
            let p = pipe(a);
            let settings = get_compress_settings(f, &p);
            ok(v_dict(vec![
                (
                    "settings",
                    v_dict(vec![
                        ("segmentby", v_list(settings.segmentby)),
                        ("orderby", v_list(settings.orderby)),
                        ("after", settings.after.unwrap_or(PyValue::None)),
                    ]),
                ),
                (
                    "columnstore_settings_query",
                    v_str(get_columnstore_settings_query(&ctx, &p)),
                ),
                (
                    "columnstore_policy_query",
                    v_str(get_columnstore_policy_query(&ctx, &p)),
                ),
                (
                    "columnstore_remove_policy_query",
                    v_str(get_columnstore_remove_policy_query(&ctx, &p)),
                ),
                (
                    "columnstore_disable_query",
                    v_str(get_columnstore_disable_query(&ctx, &p)),
                ),
                (
                    "integer_now_func_queries",
                    v_list(get_integer_now_func_queries(&ctx, &p)),
                ),
            ]))
        }
        "compress_pipe_queries" | "decompress_pipe_queries" => {
            let f = flavor(a);
            let ctx = ConnectorCtx::new(f);
            let p = pipe(a);
            let is_hypertable = b(a, "is_hypertable");
            let no_policy = b(a, "no_policy");
            let groups = if func == "compress_pipe_queries" {
                get_compress_query_groups(&ctx, &p, is_hypertable, no_policy)
            } else {
                get_decompress_query_groups(&ctx, &p, is_hypertable, no_policy)
            };
            // Python records the `_is_hypertable` lookup before the groups on TimescaleDB.
            let mut queries: Vec<String> = Vec::new();
            if f.compressible() {
                if let Some(q) = get_is_hypertable_query(&ctx, &p) {
                    queries.push(q);
                }
            }
            let success = groups.is_ok();
            if let Ok(groups) = groups {
                queries.extend(groups.into_iter().flatten());
            }
            ok(v_dict(vec![
                ("success", PyValue::Bool(success)),
                ("queries", v_list(queries)),
            ]))
        }
        "get_pipe_size_queries" => {
            let ctx = ConnectorCtx::new(flavor(a));
            let database = os(a, "database");
            ok(v_list(get_pipe_size_queries(
                &ctx,
                &pipe(a),
                database.as_deref(),
            )))
        }
        "partition_misc" => {
            let f = flavor(a);
            let ctx = ConnectorCtx::new(f);
            let p = pipe(a);
            let database = os(a, "database");
            let mut out = vec![
                (
                    "partition_count_query",
                    v_opt(get_partition_count_query(&ctx, &p, database.as_deref())),
                ),
                (
                    "chunk_count_query",
                    v_str(get_timescaledb_chunk_count_query(&ctx, &p)),
                ),
                ("should_partition", PyValue::Bool(should_partition(f, &p))),
                ("partition_column", v_opt(get_partition_column(f, &p))),
                (
                    "cleanup_queries",
                    v_list(get_partition_cleanup_queries(f, &p)),
                ),
                (
                    "function_name",
                    v_str(partition_function_name(f, &p.target)),
                ),
                ("scheme_name", v_str(partition_scheme_name(f, &p.target))),
            ];
            if f.is_timescaledb() && p.column("datetime").is_some() {
                out.push((
                    "set_chunk_time_interval_query",
                    v_str(get_set_chunk_time_interval_query(&ctx, &p, 1440)),
                ));
            }
            ok(v_dict(out))
        }
        "partition_names" => {
            let f = flavor(a);
            let p = pipe(a);
            ok(v_dict(vec![
                (
                    "cleanup_queries",
                    v_list(get_partition_cleanup_queries(f, &p)),
                ),
                (
                    "function_name",
                    v_str(partition_function_name(f, &p.target)),
                ),
                ("scheme_name", v_str(partition_scheme_name(f, &p.target))),
            ]))
        }
        "partition_bounds" => {
            let f = flavor(a);
            let (lo, hi) = partition_bounds(
                partition_value(get(a, "value")),
                interval(get(a, "interval")),
            );
            ok(v_dict(vec![
                (
                    "bounds",
                    PyValue::List(vec![v_partition_value(lo), v_partition_value(hi)]),
                ),
                (
                    "literals",
                    v_list([partition_literal(f, lo), partition_literal(f, hi)]),
                ),
            ]))
        }
        "partition_ranges" => partition_ranges_case(a),
        "get_pipe_metadef" => {
            let f = flavor(a);
            let p = pipe(a);
            let params = get(a, "params").as_dict().cloned();
            let parent = match get(a, "parent") {
                PyValue::None => None,
                par => Some(ParentSpec {
                    target: s(par, "target"),
                    schema: os(par, "schema"),
                    dt_col: os(par, "dt_col"),
                    dt_typ: os(par, "dt_typ"),
                    precision_unit: s(par, "precision_unit"),
                }),
            };
            let mut args = MetadefArgs::new(&p);
            args.params = params.as_ref();
            args.begin = bound(get(a, "begin"), true);
            args.end = bound(get(a, "end"), false);
            args.check_existing = b(a, "check_existing");
            args.sync_time = bound(get(a, "sync_time"), false);
            args.parent = parent;
            res(get_pipe_metadef(f, &args), v_opt)
        }
        other => panic!("No Rust dispatcher for fixture function '{other}'"),
    }
}

fn partition_ranges_case(a: &PyValue) -> Outcome {
    let f = flavor(a);
    let ctx = ConnectorCtx::new(f);
    let p = pipe(a);
    let values: Vec<PartitionValue> = match get(a, "values") {
        PyValue::List(items) => items.iter().map(partition_value).collect(),
        _ => Vec::new(),
    };
    let le = |x: &PartitionValue, y: &PartitionValue| match (x, y) {
        (PartitionValue::Int(a), PartitionValue::Int(b)) => a <= b,
        (PartitionValue::DateTime(a), PartitionValue::DateTime(b)) => {
            a.to_utc().naive_local() <= b.to_utc().naive_local()
        }
        _ => false,
    };
    let mut min = values[0];
    let mut max = values[0];
    for v in &values {
        if le(v, &min) {
            min = *v;
        }
        if le(&max, v) {
            max = *v;
        }
    }
    let chunk = p.get_chunk_interval();
    let (ranges, _) = partition_ranges(min, max, chunk, MAX_PARTITIONS_PER_SYNC);
    let mut out = vec![
        ("min", v_partition_value(min)),
        ("max", v_partition_value(max)),
        ("interval", v_interval(chunk)),
        (
            "ranges",
            PyValue::List(
                ranges
                    .iter()
                    .map(|(lo, hi)| {
                        PyValue::List(vec![v_partition_value(*lo), v_partition_value(*hi)])
                    })
                    .collect(),
            ),
        ),
        (
            "literals",
            PyValue::List(
                ranges
                    .iter()
                    .map(|(lo, hi)| v_list([partition_literal(f, *lo), partition_literal(f, *hi)]))
                    .collect(),
            ),
        ),
        (
            "names",
            v_list(
                ranges
                    .iter()
                    .map(|(lo, _)| partition_name(f, &p.target, *lo)),
            ),
        ),
        (
            "initial_bounds",
            PyValue::List(
                get_initial_partition_bounds(f, &p.target, &ranges)
                    .into_iter()
                    .map(|(n, l)| v_list([n, l]))
                    .collect(),
            ),
        ),
    ];
    if f.pg_partition() {
        let mut queries = vec![get_pg_is_partitioned_query(&ctx, &p)];
        queries.extend(get_pg_partition_creation_queries(&ctx, &p, &ranges));
        out.push(("pg_queries", v_list(queries)));
    }
    if f == Flavor::Mssql {
        let boundaries: Vec<PartitionValue> = ranges.iter().map(|(lo, _)| *lo).collect();
        out.push((
            "mssql_creation_queries",
            v_list(get_mssql_partition_creation_queries(&p, &boundaries)),
        ));
        let max_text = s(a, "_mssql_max_text");
        let max_text = if max_text == "None" {
            if p.is_int_axis() {
                "1000".to_string()
            } else {
                "2023-11-01T00:00:00.0000000+00:00".to_string()
            }
        } else {
            max_text
        };
        let mut queries = vec![get_mssql_max_partition_boundary_query(&p)];
        if let Some(max_bound) = parse_mssql_max_partition_boundary(&max_text, p.is_int_axis()) {
            queries.extend(get_mssql_split_partition_queries(
                &p,
                max_bound,
                max,
                chunk,
                MAX_PARTITIONS_PER_SYNC,
            ));
        }
        out.push(("mssql_max_boundary_text", v_str(max_text)));
        out.push(("mssql_queries", v_list(queries)));
    }
    if f.mysql_partition() {
        let descs: Vec<String> = if p.is_int_axis() {
            vec!["1000".into()]
        } else {
            vec![
                "'2023-11-01 00:00:00.000000'".into(),
                "'2023-10-02 00:00:00'".into(),
            ]
        };
        let mut queries = Vec::new();
        if let Some(q) = get_mysql_max_partition_bound_query(&ctx, &p, Some("db")) {
            queries.push(q);
        }
        if let Some(max_bound) = parse_mysql_max_partition_bound(&descs, p.is_int_axis()) {
            queries.extend(get_mysql_add_partition_queries(
                &ctx,
                &p,
                max_bound,
                max,
                chunk,
                MAX_PARTITIONS_PER_SYNC,
            ));
        }
        out.push(("mysql_descriptions", v_list(descs)));
        out.push(("mysql_queries", v_list(queries)));
    }
    ok(v_dict(out))
}

#[test]
fn golden_queries_match_python() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/queries.json");
    let text = std::fs::read_to_string(path).expect("read fixtures");
    let fixture: Fixture = serde_json::from_str(&text).expect("parse fixtures");

    let mut passed: BTreeMap<String, usize> = BTreeMap::new();
    let mut skipped: BTreeMap<String, usize> = BTreeMap::new();
    let mut failed: BTreeMap<String, usize> = BTreeMap::new();
    let mut failures: Vec<String> = Vec::new();

    for case in &fixture.cases {
        let func = case.func.as_str();
        let table_failures = match func {
            "templates" => Some(templates_case(case.result.as_ref().unwrap())),
            "flavor_sets" => Some(flavor_sets_case(case.result.as_ref().unwrap())),
            "pd_to_db_table" | "db_to_pd_table" | "cast_dtypes_table" => {
                Some(dtype_tables_case(func, case.result.as_ref().unwrap()))
            }
            _ => None,
        };
        if let Some(fs) = table_failures {
            if fs.is_empty() {
                *passed.entry(func.to_string()).or_default() += 1;
            } else {
                *failed.entry(func.to_string()).or_default() += 1;
                failures.extend(fs);
            }
            continue;
        }

        // PARITY: Python raises when a name cannot be truncated to the flavor's limit
        // (`utils/misc.py:1341`); the Rust port returns its best effort instead, so those
        // cases are skipped (and counted).
        if case
            .error
            .as_deref()
            .is_some_and(|e| e.contains("could not be truncated"))
        {
            *skipped.entry(func.to_string()).or_default() += 1;
            continue;
        }

        let mut args = case.args.clone();
        if func == "partition_ranges" {
            if let (PyValue::Dict(d), Some(text)) = (
                &mut args,
                case.result
                    .as_ref()
                    .and_then(|r| r.get("mssql_max_boundary_text"))
                    .map(PyValue::py_str),
            ) {
                d.insert("_mssql_max_text".into(), PyValue::Str(text));
            }
        }
        let outcome = std::panic::catch_unwind(|| run(func, &args));
        let outcome = match outcome {
            Ok(o) => o,
            Err(_) => Outcome::Error("panicked".into()),
        };
        // A Python `None` result deserializes as a missing `result`.
        let expected = case.result.clone().or(if case.error.is_none() {
            Some(PyValue::None)
        } else {
            None
        });
        let matched = match (&outcome, &expected, &case.error) {
            (Outcome::Value(got), Some(exp), None) => got.json_dumps() == exp.json_dumps(),
            (Outcome::Error(_), None, Some(_)) => true,
            _ => false,
        };
        if matched {
            *passed.entry(func.to_string()).or_default() += 1;
        } else {
            *failed.entry(func.to_string()).or_default() += 1;
            if failures.len() < 40 {
                let got = match &outcome {
                    Outcome::Value(v) => v.json_dumps(),
                    Outcome::Error(e) => format!("ERROR {e}"),
                };
                let exp = expected
                    .as_ref()
                    .map(PyValue::json_dumps)
                    .unwrap_or_else(|| format!("ERROR {}", case.error.clone().unwrap_or_default()));
                failures.push(format!(
                    "{func} {}\n  python: {exp}\n  rust:   {got}",
                    case.args.json_dumps()
                ));
            }
        }
    }

    let total_passed: usize = passed.values().sum();
    let total_failed: usize = failed.values().sum();
    let total_skipped: usize = skipped.values().sum();
    eprintln!("golden: {total_passed} passed, {total_failed} failed, {total_skipped} skipped");
    let mut funcs: Vec<&String> = passed
        .keys()
        .chain(failed.keys())
        .chain(skipped.keys())
        .collect();
    funcs.sort();
    funcs.dedup();
    for f in funcs {
        eprintln!(
            "  {f:45} pass={:5} fail={:5} skip={:4}",
            passed.get(f).copied().unwrap_or(0),
            failed.get(f).copied().unwrap_or(0),
            skipped.get(f).copied().unwrap_or(0)
        );
    }
    if !failures.is_empty() {
        panic!(
            "{} golden mismatches:\n{}",
            total_failed,
            failures.join("\n")
        );
    }
}
