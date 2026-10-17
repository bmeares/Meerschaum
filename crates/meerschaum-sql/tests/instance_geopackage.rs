//! The `geopackage` flavor: GeoPackage metadata, binary geometry encoding and R-tree spatial
//! indices (`instance/geopackage.rs`), plus a parity check against v4: the pipes of
//! `tests/fixtures/geopackage/scenario.json` are synced by v4 (`gpkg_parity.py`, run as a
//! subprocess with `PYTHONPATH` set to this repository) into one GeoPackage and by Rust into
//! another; both files are then read with GDAL (geopandas / pyogrio) and with each
//! implementation's `get_data`, and the geometries, CRS and column dtypes must match.
//!
//! The v4 half needs the Python reference environment (`MRSM_TEST_PYTHON`, default
//! `~/.config/meerschaum/venvs/mrsm/bin/python`, with geopandas and pyogrio) and skips without
//! it. `MRSM_TEST_KEEP=1` keeps the files.

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;

use common::*;
use meerschaum_connector::{DataQuery, SyncOptions};
use meerschaum_core::{Pipe, Value};
use meerschaum_kernel::Geometry;
use meerschaum_kernel::enforce::PipeInput;
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};
use meerschaum_sql::SqlConnector;
use meerschaum_sql::driver::SqlValue;
use serde_json::{Value as J, json};

fn geom(wkt: &str) -> KValue {
    KValue::Geometry(Geometry::from_wkt(wkt).unwrap())
}

async fn scalar(conn: &SqlConnector, sql: &str) -> Option<SqlValue> {
    conn.driver().value(sql).await.unwrap()
}

async fn count(conn: &SqlConnector, sql: &str) -> i64 {
    scalar(conn, sql).await.and_then(|v| v.as_i64()).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn geopackage_metadata_and_rtree() {
    let path = temp_sqlite_path("gpkg_meta").with_extension("gpkg");
    let conn = SqlConnector::from_uri("rs_gpkg", &format!("geopackage:///{}", path.display()))
        .await
        .unwrap();
    let pipe = Pipe::builder("geo", "parcels")
        .instance(&conn.keys())
        .parameters(params(
            r#"{"columns": {"datetime": "dt", "id": "id"},
                "dtypes": {"geometry": "geometry[MultiPolygon, 2263]"},
                "indices": {"geometry": "geometry"}}"#,
        ))
        .build()
        .unwrap();
    let docs = vec![
        doc(vec![
            ("dt", ndt(2024, 1, 1, 0, 0)),
            ("id", i(1)),
            (
                "geometry",
                geom("MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)))"),
            ),
        ]),
        doc(vec![
            ("dt", ndt(2024, 1, 2, 0, 0)),
            ("id", i(2)),
            (
                "geometry",
                geom("MULTIPOLYGON (((10 10, 12 10, 12 13, 10 13, 10 10)))"),
            ),
        ]),
        doc(vec![("dt", ndt(2024, 1, 3, 0, 0)), ("id", i(3))]),
    ];
    let res = conn
        .sync(&pipe, PipeInput::Docs(docs), &SyncOptions::default())
        .await;
    assert!(res.success, "{}", res.message);

    // GeoPackage header and metadata.
    assert_eq!(
        count(&conn, "PRAGMA application_id").await,
        meerschaum_sql::instance::geopackage::GPKG_APPLICATION_ID
    );
    assert_eq!(
        scalar(
            &conn,
            "SELECT column_name || '|' || geometry_type_name || '|' || srs_id FROM gpkg_geometry_columns WHERE table_name = 'geo_parcels'"
        )
        .await,
        Some(SqlValue::Text("geometry|MULTIPOLYGON|2263".into()))
    );
    assert_eq!(
        count(
            &conn,
            "SELECT COUNT(*) FROM gpkg_contents WHERE table_name = 'geo_parcels' AND data_type = 'features' AND srs_id = 2263"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &conn,
            "SELECT COUNT(*) FROM gpkg_spatial_ref_sys WHERE srs_id = 2263 AND organization = 'EPSG'"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &conn,
            "SELECT COUNT(*) FROM gpkg_ogr_contents WHERE table_name = 'geo_parcels' AND feature_count IS NULL"
        )
        .await,
        1
    );
    // GeoPackage binary geometry (`GP` magic, srs_id -1 as v4 writes it).
    assert_eq!(
        scalar(
            &conn,
            "SELECT hex(substr(geometry, 1, 8)) FROM geo_parcels WHERE id = 1"
        )
        .await,
        Some(SqlValue::Text("47500001FFFFFFFF".into()))
    );

    // The R-tree holds one box per non-null geometry, maintained by the triggers.
    let rtree = "rtree_geo_parcels_geometry";
    assert_eq!(
        count(&conn, &format!("SELECT COUNT(*) FROM {rtree}")).await,
        2
    );
    assert_eq!(
        count(
            &conn,
            "SELECT COUNT(*) FROM gpkg_extensions WHERE table_name = 'geo_parcels' AND extension_name = 'gpkg_rtree_index'"
        )
        .await,
        1
    );
    let maxy = |id: i64| {
        let conn = &conn;
        async move {
            match scalar(
                conn,
                &format!(
                    "SELECT r.maxy FROM {rtree} r JOIN geo_parcels t ON t.rowid = r.id WHERE t.id = {id}"
                ),
            )
            .await
            {
                Some(SqlValue::Float(f)) => Some(f),
                _ => None,
            }
        }
    };
    assert_eq!(maxy(2).await, Some(13.0));

    // An update moves the box; a new geometry for a null row adds one.
    let res = conn
        .sync(
            &pipe,
            PipeInput::Docs(vec![
                doc(vec![
                    ("dt", ndt(2024, 1, 2, 0, 0)),
                    ("id", i(2)),
                    (
                        "geometry",
                        geom("MULTIPOLYGON (((10 10, 12 10, 12 20, 10 20, 10 10)))"),
                    ),
                ]),
                doc(vec![
                    ("dt", ndt(2024, 1, 3, 0, 0)),
                    ("id", i(3)),
                    (
                        "geometry",
                        geom("MULTIPOLYGON (((5 5, 6 5, 6 6, 5 6, 5 5)))"),
                    ),
                ]),
            ]),
            &SyncOptions::default(),
        )
        .await;
    assert!(res.success, "{}", res.message);
    assert_eq!(maxy(2).await, Some(20.0));
    assert_eq!(maxy(3).await, Some(6.0));
    assert_eq!(
        count(&conn, &format!("SELECT COUNT(*) FROM {rtree}")).await,
        3
    );

    // Spatial query through the index.
    assert_eq!(
        count(
            &conn,
            &format!("SELECT COUNT(*) FROM {rtree} WHERE minx <= 5.5 AND maxx >= 5.5 AND miny <= 5.5 AND maxy >= 5.5")
        )
        .await,
        1
    );

    // Data round-trips.
    let df = conn
        .get_pipe_data(&pipe, &DataQuery::default())
        .await
        .unwrap()
        .unwrap();
    let geoms = df.data("geometry").unwrap().values();
    match &geoms[0] {
        KValue::Geometry(g) => assert_eq!(g.to_wkt(), "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)))"),
        other => panic!("not a geometry: {other:?}"),
    }

    // Deleting rows clears their boxes.
    let res = conn
        .clear_pipe(
            &pipe,
            &DataQuery {
                params: Some(params(r#"{"id": 1}"#)),
                ..Default::default()
            },
        )
        .await;
    assert!(res.success, "{}", res.message);
    assert_eq!(
        count(&conn, &format!("SELECT COUNT(*) FROM {rtree}")).await,
        2
    );

    // Dropping and recreating the index; vacuum rebuilds it.
    let res = conn.drop_pipe_indices(&pipe, None).await;
    assert!(res.success, "{}", res.message);
    assert_eq!(
        count(
            &conn,
            &format!("SELECT COUNT(*) FROM sqlite_master WHERE name = '{rtree}'")
        )
        .await,
        0
    );
    let res = conn.create_pipe_indices(&pipe, None).await;
    assert!(res.success, "{}", res.message);
    assert_eq!(
        count(&conn, &format!("SELECT COUNT(*) FROM {rtree}")).await,
        2
    );
    let res = conn.vacuum_pipe(&pipe, false).await;
    assert!(res.success, "{}", res.message);
    assert_eq!(
        count(&conn, &format!("SELECT COUNT(*) FROM {rtree}")).await,
        2
    );

    // Dropping the pipe removes its metadata.
    let res = conn.drop_pipe(&pipe).await;
    assert!(res.success, "{}", res.message);
    assert_eq!(
        count(
            &conn,
            "SELECT COUNT(*) FROM gpkg_contents WHERE table_name = 'geo_parcels'"
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &conn,
            &format!("SELECT COUNT(*) FROM sqlite_master WHERE name = '{rtree}'")
        )
        .await,
        0
    );

    // A table without geometry is a plain table.
    let plain = Pipe::builder("geo", "plain")
        .instance(&conn.keys())
        .parameters(params(r#"{"columns": {"id": "id"}}"#))
        .build()
        .unwrap();
    let res = conn
        .sync(
            &plain,
            PipeInput::Docs(vec![doc(vec![("id", i(1)), ("v", fl(1.5))])]),
            &SyncOptions::default(),
        )
        .await;
    assert!(res.success, "{}", res.message);
    assert_eq!(
        count(
            &conn,
            "SELECT COUNT(*) FROM gpkg_contents WHERE table_name = 'geo_plain'"
        )
        .await,
        0
    );
    conn.driver().close().await;
    let _ = std::fs::remove_file(&path);
}

// ------------------------------------------------------------------------------------------
// Parity with v4.
// ------------------------------------------------------------------------------------------

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/geopackage")
}

fn python() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("MRSM_TEST_PYTHON") {
        return Some(PathBuf::from(p));
    }
    let home = std::env::var("HOME").ok()?;
    let p = Path::new(&home).join(".config/meerschaum/venvs/mrsm/bin/python");
    p.exists().then_some(p)
}

fn run_py(exe: &Path, work: &Path, args: &[&str]) -> J {
    let root = work.join("mrsm_root");
    let plugins = work.join("mrsm_plugins");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&plugins).unwrap();
    let mut cmd = Command::new(exe);
    cmd.arg(fixtures().join("gpkg_parity.py"))
        .args(args)
        .current_dir(repo_root())
        .env("PYTHONPATH", repo_root())
        .env("MRSM_ROOT_DIR", &root)
        .env("MRSM_PLUGINS_DIR", &plugins);
    // The geometry stack (shapely, geopandas) lives in the reference interpreter's own venv.
    if let Some(venvs) = exe.parent().and_then(Path::parent).and_then(Path::parent) {
        if venvs.ends_with("venvs") {
            cmd.env("MRSM_VENVS_DIR", venvs);
        }
    }
    let out = cmd.output().expect("python runs");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let last = stdout
        .lines()
        .rev()
        .find(|l| l.trim_start().starts_with(['[', '{']));
    match last {
        Some(l) => serde_json::from_str(l).expect("JSON output"),
        None => panic!(
            "python {args:?} failed:\nstdout:\n{stdout}\nstderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        ),
    }
}

fn scenario() -> J {
    serde_json::from_str(&std::fs::read_to_string(fixtures().join("scenario.json")).unwrap())
        .unwrap()
}

fn decode(v: &J) -> KValue {
    match v {
        J::Null => KValue::Null,
        J::Bool(b) => KValue::Bool(*b),
        J::Number(n) => match n.as_i64() {
            Some(i) => KValue::Int(i),
            None => KValue::Float(n.as_f64().unwrap()),
        },
        J::String(s) => KValue::Str(s.clone()),
        J::Object(o) if o.contains_key("$dt") => {
            let dt = chrono::DateTime::parse_from_rfc3339(o["$dt"].as_str().unwrap()).unwrap();
            KValue::Datetime(DateTimeValue::utc(dt.naive_utc()))
        }
        J::Object(o) if o.contains_key("$wkt") => geom(o["$wkt"].as_str().unwrap()),
        other => KValue::Json(other.clone()),
    }
}

fn spec_pipe(conn: &SqlConnector, spec: &J) -> Pipe {
    Pipe::builder(
        spec["connector_keys"].as_str().unwrap(),
        spec["metric_key"].as_str().unwrap(),
    )
    .location(spec["location_key"].as_str())
    .instance(&conn.keys())
    .parameters(
        Value::from_json_str(&spec["parameters"].to_string())
            .unwrap()
            .to_map(),
    )
    .build()
    .unwrap()
}

/// Rust's `get_data` of every scenario pipe: geometries as WKT, other cells as `str()`.
async fn rust_read(db: &Path) -> J {
    let conn = SqlConnector::from_uri("gpkg_parity_rs", &format!("geopackage:///{}", db.display()))
        .await
        .unwrap();
    let mut out = serde_json::Map::new();
    for spec in scenario()["pipes"].as_array().unwrap() {
        let pipe = spec_pipe(&conn, spec);
        let sp = conn.sql_pipe(&pipe).unwrap();
        let df = conn
            .get_pipe_data(&pipe, &DataQuery::default())
            .await
            .unwrap()
            .unwrap();
        let rows: Vec<J> = meerschaum_connector::pipe_ops::frame_to_docs(&df)
            .iter()
            .map(|d| {
                J::Object(
                    d.iter()
                        .map(|(k, v)| {
                            let cell = match v {
                                v if v.is_na() => J::Null,
                                KValue::Geometry(g) => J::String(g.to_wkt()),
                                other => J::String(other.py_str()),
                            };
                            (k.clone(), cell)
                        })
                        .collect(),
                )
            })
            .collect();
        out.insert(sp.target.clone(), json!({ "rows": rows }));
    }
    conn.driver().close().await;
    J::Object(out)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn geopackage_v4_parity() {
    let Some(exe) = python() else {
        eprintln!("Skipping: no Python reference environment (set MRSM_TEST_PYTHON).");
        return;
    };
    let work =
        std::env::temp_dir().join(format!("_mrsm_rs_gpkg_{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(&work).unwrap();
    let sc = fixtures().join("scenario.json");
    let sc = sc.to_str().unwrap();
    let py_db = work.join("py.gpkg");
    let rs_db = work.join("rs.gpkg");
    let s = |p: &Path| p.to_str().unwrap().to_string();

    // v4 syncs into one file, Rust into the other.
    let py_res = run_py(&exe, &work, &["sync", &s(&py_db), sc]);
    let conn = SqlConnector::from_uri(
        "gpkg_parity_rs",
        &format!("geopackage:///{}", rs_db.display()),
    )
    .await
    .unwrap();
    for (i, spec) in scenario()["pipes"].as_array().unwrap().iter().enumerate() {
        let pipe = spec_pipe(&conn, spec);
        let docs = spec["docs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| {
                d.as_object()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| (k.clone(), decode(v)))
                    .collect()
            })
            .collect();
        let res = conn
            .sync(&pipe, PipeInput::Docs(docs), &SyncOptions::default())
            .await;
        assert!(res.success, "rust sync of pipe {i}: {}", res.message);
        assert_eq!(
            py_res[i][0],
            J::Bool(true),
            "v4 sync of pipe {i}: {}",
            py_res[i]
        );
        assert_eq!(
            py_res[i][1].as_str().unwrap(),
            res.message,
            "message of pipe {i}"
        );
    }
    conn.driver().close().await;

    let mut failures = Vec::new();
    let mut check = |label: &str, a: &J, b: &J| {
        if a != b {
            failures.push(format!("{label}:\n    v4:   {a}\n    rust: {b}"));
        }
    };
    // GDAL reads both files the same (layers, CRS, column dtypes, geometries, values).
    let py_ogr = run_py(&exe, &work, &["ogr", &s(&py_db), sc]);
    let rs_ogr = run_py(&exe, &work, &["ogr", &s(&rs_db), sc]);
    check("GDAL", &py_ogr, &rs_ogr);
    // v4 reads both files the same.
    let py_read_py = run_py(&exe, &work, &["read", &s(&py_db), sc]);
    let py_read_rs = run_py(&exe, &work, &["read", &s(&rs_db), sc]);
    check("v4 get_data", &py_read_py, &py_read_rs);
    // Rust reads both files the same, with v4's geometries.
    let rs_read_py = rust_read(&py_db).await;
    let rs_read_rs = rust_read(&rs_db).await;
    check("rust get_data", &rs_read_py, &rs_read_rs);
    for (table, v) in py_read_py.as_object().unwrap() {
        let geoms = |rows: &J| -> Vec<J> {
            rows["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| {
                    J::Array(
                        r.as_object()
                            .unwrap()
                            .iter()
                            .filter(|(k, _)| {
                                ["geometry", "geom", "g", "shape"].contains(&k.as_str())
                            })
                            .map(|(_, v)| v.clone())
                            .collect(),
                    )
                })
                .collect()
        };
        check(
            &format!("{table} geometries (v4 vs rust get_data)"),
            &J::Array(geoms(v)),
            &J::Array(geoms(&rs_read_rs[table])),
        );
    }

    let report = failures.join("\n");
    if failures.is_empty() && std::env::var("MRSM_TEST_KEEP").is_err() {
        let _ = std::fs::remove_dir_all(&work);
    }
    eprintln!("geopackage parity artifacts: {}", work.display());
    assert!(
        failures.is_empty(),
        "{} difference(s):\n{report}",
        failures.len()
    );
}
