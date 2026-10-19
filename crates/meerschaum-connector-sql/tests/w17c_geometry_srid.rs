//! The SRID / spatial reference a geometry column stores, per flavor, as v4.1.4 stores it.
//!
//! Reference (v4.1.4 against the same containers, `pipe.sync()` of a frame, then of a frame
//! updating one row and adding another):
//! - PostGIS flavors (`postgis`, `timescaledb-ha`): `geometry[Point, 4326]` creates
//!   `geometry(Point,4326)` and every row stores SRID 4326 (PostGIS assigns the column SRID to the
//!   SRID-less WKB); a bare `geometry` column keeps SRID 0 on inserted *and* updated rows, even
//!   when the frame carries a CRS; a frame CRS with no configured dtype infers
//!   `geometry[MultiPolygon, 6570]`. A `geography[Point, 4326]` column becomes TEXT on the second
//!   sync (a v4 quirk kept as is); its updated and new rows then hold SRID-less WKB.
//! - `timescaledb` / `postgresql` (no PostGIS types), `sqlite`, `mysql`, `mariadb`, `duckdb`:
//!   a TEXT column of plain hex WKB without the EWKB SRID flag; `mssql`: WKT text.
//! - `geopackage`: the GeoPackage blob's header SRS id is -1; `gpkg_geometry_columns.srs_id`
//!   is the frame's CRS (else the dtype's SRID, else the undefined 99999).
//!
//! The test databases come from `MRSM_TEST_*_URI` (skipped when unset); the TimescaleDB-HA
//! image (PostGIS included) is `MRSM_TEST_TIMESCALE_HA_URI`.

mod common;
use common::*;

use meerschaum_connector::SyncOptions;
use meerschaum_core::dtypes::Srid;
use meerschaum_kernel::docs::frame_from_docs;
use meerschaum_kernel::enforce::PipeInput;
use meerschaum_kernel::value::Value as KValue;
use meerschaum_kernel::{Frame, Geometry};

macro_rules! geo_flavor_tests {
    ($name:ident, [$($flavor:ident => $label:literal),* $(,)?], $body:expr) => {
        mod $name {
            #[allow(unused_imports)]
            use super::*;
            $(
                #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
                async fn $flavor() {
                    let Some(ctx) = $crate::common::ctx($label, stringify!($name)).await else {
                        return;
                    };
                    let f = $body;
                    f(ctx).await;
                }
            )*
        }
    };
}

/// A frame of `(dt, id, geom)` rows; `crs` is the geometry column's CRS (a GeoDataFrame's).
fn geo_frame(rows: &[(u32, i64, &str)], crs: Option<i64>) -> Frame {
    let docs: Vec<Doc> = rows
        .iter()
        .map(|(h, id, wkt)| {
            doc(vec![
                ("dt", udt(2024, 1, 1, *h, 0)),
                ("id", i(*id)),
                ("geom", KValue::Geometry(Geometry::from_wkt(wkt).unwrap())),
            ])
        })
        .collect();
    let mut df = frame_from_docs(&docs).unwrap();
    if let Some(c) = df.column_mut("geom") {
        c.crs = crs.map(Srid::Code);
    }
    df
}

fn table_of(c: &Ctx, pipe: &meerschaum_core::Pipe) -> String {
    let sp = c.conn.sql_pipe(pipe).unwrap();
    c.conn.pipe_table_name(&sp)
}

/// Rows `(id, value)` of a two-column query, ordered by the query.
async fn pairs(c: &Ctx, sql: &str) -> Vec<(i64, String)> {
    let df = c
        .conn
        .read(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let names = df.names();
    let ids = df.data(&names[0]).unwrap().values();
    let vals = df.data(&names[1]).unwrap().values();
    ids.iter()
        .zip(vals.iter())
        .map(|(id, v)| (as_int(id).unwrap(), v.py_str()))
        .collect()
}

/// Sync the first frame (ids 1, 2), then an update of id 1 plus a new id 3, on a live `Pipe`
/// (Python's `pipe` object keeps the special dtypes a sync detects for its next sync).
async fn sync_insert_update(
    c: &Ctx,
    pipe: &meerschaum_core::Pipe,
    crs: Option<i64>,
    wkts: [&str; 4],
) {
    let mut live = pipe.clone();
    for (rows, expected) in [
        ([(1, 1, wkts[0]), (2, 2, wkts[1])], "Inserted 2, updated 0"),
        ([(1, 1, wkts[2]), (3, 3, wkts[3])], "Inserted 1, updated 1"),
    ] {
        let res = ok(c
            .conn
            .sync_mut(
                &mut live,
                PipeInput::Frame(geo_frame(&rows, crs)),
                &SyncOptions::default(),
            )
            .await);
        assert!(res.message.contains(expected), "{}", res.message);
    }
}

const POINTS: [&str; 4] = ["POINT (1 2)", "POINT (3 4)", "POINT (11 2)", "POINT (5 6)"];

async fn postgis_srids(c: &Ctx, pipe: &meerschaum_core::Pipe) -> (String, Vec<(i64, String)>) {
    let table = table_of(c, pipe);
    let typ = pairs(
        c,
        &format!(
            "SELECT 0 AS id, format_type(atttypid, atttypmod) AS typ FROM pg_attribute \
             WHERE attrelid = '{}'::regclass AND attname = 'geom'",
            table.replace('\'', "''")
        ),
    )
    .await
    .pop()
    .map(|(_, t)| t)
    .unwrap_or_default();
    let srids = pairs(
        c,
        &format!("SELECT id, ST_SRID(geom) FROM {table} ORDER BY id"),
    )
    .await;
    (typ, srids)
}

fn all_srids(srids: &[(i64, String)], expected: &str) -> bool {
    srids.len() == 3 && srids.iter().all(|(_, s)| s == expected)
}

geo_flavor_tests!(
    postgis_family,
    [postgis => "postgis", timescaledb_ha => "timescaledb-ha"],
    |c: Ctx| async move {
        // Typed column: PostGIS assigns the typmod SRID to every row.
        let typed = c.pipe(
            "geo",
            "typed",
            None,
            r#"{"columns": {"datetime": "dt", "id": "id"}, "dtypes": {"geom": "geometry[Point, 4326]"}}"#,
        );
        sync_insert_update(&c, &typed, None, POINTS).await;
        let (typ, srids) = postgis_srids(&c, &typed).await;
        assert_eq!(typ, "geometry(Point,4326)");
        assert!(all_srids(&srids, "4326"), "{srids:?}");
        c.delete(&typed).await;

        // Bare column with a GeoDataFrame CRS: SRID 0 on inserted and updated rows.
        // PARITY: v4's update frame comes out of a pandas merge without its CRS
        // (meerschaum/utils/dataframe.py, `filter_unseen_df`).
        let bare = c.pipe(
            "geo",
            "bare",
            None,
            r#"{"columns": {"datetime": "dt", "id": "id"}, "dtypes": {"geom": "geometry"}}"#,
        );
        sync_insert_update(&c, &bare, Some(4326), POINTS).await;
        let (typ, srids) = postgis_srids(&c, &bare).await;
        assert_eq!(typ, "geometry");
        assert!(all_srids(&srids, "0"), "{srids:?}");
        c.delete(&bare).await;

        // No dtype: the frame's CRS is inferred and persisted.
        let inferred = c.pipe("geo", "inferred", None, r#"{"columns": {"datetime": "dt", "id": "id"}}"#);
        sync_insert_update(
            &c,
            &inferred,
            Some(6570),
            [
                "MULTIPOLYGON (((1 0, 2 0, 2 1, 1 0)))",
                "MULTIPOLYGON (((5 0, 6 0, 6 1, 5 0)))",
                "MULTIPOLYGON (((11 0, 12 0, 12 1, 11 0)))",
                "MULTIPOLYGON (((7 0, 8 0, 8 1, 7 0)))",
            ],
        )
        .await;
        let (typ, srids) = postgis_srids(&c, &inferred).await;
        assert_eq!(typ, "geometry(MultiPolygon,6570)");
        assert!(all_srids(&srids, "6570"), "{srids:?}");
        let reg = c.registered(&inferred).await;
        assert_eq!(
            reg["dtypes"].get("geom").map(|v| v.py_str()).as_deref(),
            Some("geometry[MultiPolygon, 6570]")
        );
        c.delete(&inferred).await;

        // PARITY: v4 alters a `geography[Point, 4326]` column to TEXT on the pipe's second sync
        // (the frame's detected `geometry[Point, 4326]` differs from the configured geography
        // type, `_pipes.py`, `get_alter_columns_queries`), then updates the row from an SRID-0
        // staging column with `CAST(p."geom" AS TEXT)` and appends plain WKB text. The row
        // left untouched keeps the geography's EWKB text (SRID 4326).
        let geog = c.pipe(
            "geo",
            "geog",
            None,
            r#"{"columns": {"datetime": "dt", "id": "id"}, "dtypes": {"geom": "geography[Point, 4326]"}}"#,
        );
        sync_insert_update(&c, &geog, None, POINTS).await;
        let (typ, srids) = postgis_srids(&c, &geog).await;
        assert_eq!(typ, "text");
        assert_eq!(
            srids,
            vec![
                (1, "0".to_string()),
                (2, "4326".to_string()),
                (3, "0".to_string())
            ]
        );
        c.delete(&geog).await;
    }
);

/// Plain WKB (little-endian point type 1, no SRID flag `0x20000000`) as uppercase hex.
fn is_plain_wkb_hex(s: &str) -> bool {
    s.starts_with("0101000000") && s.len() == 42
}

geo_flavor_tests!(
    text_flavors,
    [
        sqlite => "sqlite",
        timescaledb => "timescaledb",
        postgresql => "postgresql",
        mysql => "mysql",
        mariadb => "mariadb",
        mssql => "mssql",
        duckdb => "duckdb",
    ],
    |c: Ctx| async move {
        for (mk, dtype, crs) in [
            ("typed", Some("geometry[Point, 4326]"), None),
            ("bare", Some("geometry"), Some(4326)),
            ("inferred", None, Some(4326)),
        ] {
            let parameters = match dtype {
                Some(t) => format!(
                    r#"{{"columns": {{"datetime": "dt", "id": "id"}}, "dtypes": {{"geom": "{t}"}}}}"#
                ),
                None => r#"{"columns": {"datetime": "dt", "id": "id"}}"#.to_string(),
            };
            let pipe = c.pipe("geo", mk, None, &parameters);
            sync_insert_update(&c, &pipe, crs, POINTS).await;
            let table = table_of(&c, &pipe);
            let vals = pairs(&c, &format!("SELECT id, geom FROM {table} ORDER BY id")).await;
            assert_eq!(vals.len(), 3, "{mk}: {vals:?}");
            for (id, v) in &vals {
                if c.is_mssql() {
                    // `geometry_format = 'wkt'` for MSSQL (`_sql.py`, `to_sql`).
                    assert!(v.starts_with("POINT ("), "{mk} {id}: {v}");
                } else {
                    assert!(is_plain_wkb_hex(v), "{mk} {id}: {v}");
                }
            }
            // `pipe.get_data()` with the registered dtypes (a Python `Pipe` reads them).
            let mut registered = pipe.clone();
            registered.set_parameters(c.registered(&pipe).await);
            let df = c.get_data(&registered, Default::default()).await.unwrap();
            for v in df.data("geom").unwrap().values() {
                match v {
                    KValue::Geometry(g) => assert!(g.srid.unwrap_or(0) == 0, "{mk}: {g:?}"),
                    other => panic!("{mk}: not a geometry: {other:?}"),
                }
            }
            c.delete(&pipe).await;
        }
    }
);

geo_flavor_tests!(
    geopackage_srs,
    [geopackage => "geopackage"],
    |c: Ctx| async move {
        for (mk, dtype, crs, srs_id) in [
            ("typed", Some("geometry[Point, 6570]"), None, "6570"),
            // PARITY: v4 registers the GeoDataFrame's CRS through GDAL
            // (`df.head(0).to_file(..., driver='GPKG')` in `_init_geopackage_pipe`).
            ("bare_crs", Some("geometry"), Some(4326), "4326"),
            ("bare", Some("geometry"), None, "99999"),
            ("inferred", None, Some(6570), "6570"),
        ] {
            let parameters = match dtype {
                Some(t) => format!(
                    r#"{{"columns": {{"datetime": "dt", "id": "id"}}, "dtypes": {{"geom": "{t}"}}}}"#
                ),
                None => r#"{"columns": {"datetime": "dt", "id": "id"}}"#.to_string(),
            };
            let pipe = c.pipe("geo", mk, None, &parameters);
            sync_insert_update(&c, &pipe, crs, POINTS).await;
            let table = table_of(&c, &pipe);
            let registered = pairs(
                &c,
                &format!(
                    "SELECT 0 AS id, srs_id FROM gpkg_geometry_columns WHERE table_name = '{}'",
                    table.trim_matches('"')
                ),
            )
            .await;
            assert_eq!(registered, vec![(0, srs_id.to_string())], "{mk}");
            // PARITY: `serialize_geometry(geom, 'gpkg_wkb')` is called without an SRID, so
            // every blob's header carries `srs_id` -1 (`srid or -1`).
            let headers = pairs(
                &c,
                &format!("SELECT id, hex(substr(geom, 1, 8)) FROM {table} ORDER BY id"),
            )
            .await;
            assert_eq!(headers.len(), 3);
            assert!(
                headers.iter().all(|(_, h)| h == "47500001FFFFFFFF"),
                "{mk}: {headers:?}"
            );
            c.delete(&pipe).await;
        }
    }
);
