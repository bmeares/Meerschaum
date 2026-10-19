//! The `geopackage` flavor's metadata (`_init_geopackage_pipe`, `_pipes.py:4372`): a SQLite
//! database whose geometry columns are GeoPackage binary blobs, registered in the `gpkg_*`
//! tables so GDAL (and pyogrio / geopandas / QGIS) open the pipe tables as feature layers, plus
//! R-tree spatial indices for `indices: {<key>: <geometry column>}` (§6.2).
//!
//! v4 writes the metadata through GDAL (`GeoDataFrame.to_file`): on a plain SQLite file it builds
//! a temporary GeoPackage and copies its `gpkg_*` tables over. Without GDAL, this module writes
//! the rows GDAL writes for an empty frame, and creates the `gpkg_*` tables with GDAL's DDL when
//! they are missing. Existing (GDAL- or v4-created) metadata tables are reused as they are.
//!
//! Deviations from v4, all because v4's results are not usable:
//! - v4 records `feature_count = 0` in `gpkg_ogr_contents` (the count of the empty frame it
//!   wrote) and never updates it, so GDAL reports zero features for every v4 pipe table. The row
//!   is written with a `NULL` count here, so GDAL counts the rows.
//! - v4 cannot create a table without geometry (`'DataFrame' object has no attribute 'to_file'`)
//!   or with several geometry columns (GDAL's one-geometry-column limit); here such tables are
//!   created, registering no geometry column or only the first one.
//! - v4 leaves the metadata rows of dropped tables behind; they are removed here.
//! - Spatial indices are new in v5 (v4 created a B-tree index on the blob column).

use indexmap::IndexMap;
use meerschaum_core::SuccessTuple;
use meerschaum_core::dtypes::{Srid, are_dtypes_equal, get_geometry_type_srid};
use meerschaum_kernel::Frame;

use crate::driver::{SqlParam, SqlValue};
use crate::flavor::Flavor;
use crate::pipe::SqlPipe;

use super::SqlConnector;

/// `PRAGMA application_id` of a GeoPackage (`GPKG`).
pub const GPKG_APPLICATION_ID: i64 = 0x4750_4B47;
/// `PRAGMA user_version` GDAL writes (GeoPackage 1.4).
pub const GPKG_USER_VERSION: i64 = 10400;

/// GDAL's `srs_id` for a layer without a CRS.
pub const UNDEFINED_SRS_ID: i64 = 99999;

const WGS84_WKT: &str = r#"GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AXIS["Latitude",NORTH],AXIS["Longitude",EAST],AUTHORITY["EPSG","4326"]]"#;
/// `ESRI:102003` (USA Contiguous Albers Equal Area Conic) as GDAL writes it. GDAL resolves EPSG
/// codes from `organization_coordsys_id` alone, but not ESRI codes, so the definitions of the
/// ESRI codes in use are carried here.
const ESRI_102003_WKT: &str = r#"PROJCS["USA_Contiguous_Albers_Equal_Area_Conic",GEOGCS["NAD83",DATUM["North_American_Datum_1983",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6269"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4269"]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["latitude_of_center",37.5],PARAMETER["longitude_of_center",-96],PARAMETER["standard_parallel_1",29.5],PARAMETER["standard_parallel_2",45.5],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH],AUTHORITY["ESRI","102003"]]"#;
const UNDEFINED_SRS_WKT: &str = r#"LOCAL_CS["Undefined SRS",LOCAL_DATUM["unknown",32767],UNIT["unknown",0],AXIS["Easting",EAST],AXIS["Northing",NORTH]]"#;

/// GDAL's DDL for the required GeoPackage tables (GDAL 3.11, GeoPackage 1.4).
const GPKG_TABLES_DDL: &[&str] = &[
    "CREATE TABLE gpkg_spatial_ref_sys (srs_name TEXT NOT NULL,srs_id INTEGER NOT NULL PRIMARY KEY,organization TEXT NOT NULL,organization_coordsys_id INTEGER NOT NULL,definition  TEXT NOT NULL,description TEXT)",
    "CREATE TABLE gpkg_contents (table_name TEXT NOT NULL PRIMARY KEY,data_type TEXT NOT NULL,identifier TEXT UNIQUE,description TEXT DEFAULT '',last_change DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),min_x DOUBLE, min_y DOUBLE,max_x DOUBLE, max_y DOUBLE,srs_id INTEGER,CONSTRAINT fk_gc_r_srs_id FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id))",
    "CREATE TABLE gpkg_ogr_contents(table_name TEXT NOT NULL PRIMARY KEY,feature_count INTEGER DEFAULT NULL)",
    "CREATE TABLE gpkg_geometry_columns (table_name TEXT NOT NULL,column_name TEXT NOT NULL,geometry_type_name TEXT NOT NULL,srs_id INTEGER NOT NULL,z TINYINT NOT NULL,m TINYINT NOT NULL,CONSTRAINT pk_geom_cols PRIMARY KEY (table_name, column_name),CONSTRAINT uk_gc_table_name UNIQUE (table_name),CONSTRAINT fk_gc_tn FOREIGN KEY (table_name) REFERENCES gpkg_contents(table_name),CONSTRAINT fk_gc_srs FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys (srs_id))",
    "CREATE TABLE gpkg_tile_matrix_set (table_name TEXT NOT NULL PRIMARY KEY,srs_id INTEGER NOT NULL,min_x DOUBLE NOT NULL,min_y DOUBLE NOT NULL,max_x DOUBLE NOT NULL,max_y DOUBLE NOT NULL,CONSTRAINT fk_gtms_table_name FOREIGN KEY (table_name) REFERENCES gpkg_contents(table_name),CONSTRAINT fk_gtms_srs FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys (srs_id))",
    "CREATE TABLE gpkg_tile_matrix (table_name TEXT NOT NULL,zoom_level INTEGER NOT NULL,matrix_width INTEGER NOT NULL,matrix_height INTEGER NOT NULL,tile_width INTEGER NOT NULL,tile_height INTEGER NOT NULL,pixel_x_size DOUBLE NOT NULL,pixel_y_size DOUBLE NOT NULL,CONSTRAINT pk_ttm PRIMARY KEY (table_name, zoom_level),CONSTRAINT fk_tmm_table_name FOREIGN KEY (table_name) REFERENCES gpkg_contents(table_name))",
];

/// `gpkg_extensions` (GDAL's DDL), created with the first spatial index.
const GPKG_EXTENSIONS_DDL: &str = "CREATE TABLE IF NOT EXISTS gpkg_extensions (table_name TEXT,column_name TEXT,extension_name TEXT NOT NULL,definition TEXT NOT NULL,scope TEXT NOT NULL,CONSTRAINT ge_tce UNIQUE (table_name, column_name, extension_name))";

/// The `geometry_columns` view v4 adds when it copies the metadata into a plain SQLite file
/// (`_pipes.py:4480`).
const GEOMETRY_COLUMNS_VIEW: &str = "CREATE VIEW geometry_columns AS\nSELECT\n    table_name AS f_table_name,\n    column_name AS f_geometry_column,\n    geometry_type_name AS \"type\",\n    CASE\n        WHEN z = 0 THEN 'XY'\n        WHEN z = 1 THEN 'XYZ'\n        WHEN z = 2 THEN 'XYZ'\n    END AS coord_dimension,\n    srs_id AS srid,\n    0 AS spatial_index_enabled\nFROM gpkg_geometry_columns";

/// A `gpkg_spatial_ref_sys` row.
#[derive(Debug, Clone, PartialEq)]
pub struct SrsRow {
    pub srs_name: String,
    pub srs_id: i64,
    pub organization: String,
    pub organization_coordsys_id: i64,
    pub definition: String,
    pub description: Option<String>,
}

/// The rows GDAL inserts into a new `gpkg_spatial_ref_sys`.
pub fn default_srs_rows() -> Vec<SrsRow> {
    vec![
        SrsRow {
            srs_name: "Undefined Cartesian SRS".into(),
            srs_id: -1,
            organization: "NONE".into(),
            organization_coordsys_id: -1,
            definition: "undefined".into(),
            description: Some("undefined Cartesian coordinate reference system".into()),
        },
        SrsRow {
            srs_name: "Undefined geographic SRS".into(),
            srs_id: 0,
            organization: "NONE".into(),
            organization_coordsys_id: 0,
            definition: "undefined".into(),
            description: Some("undefined geographic coordinate reference system".into()),
        },
        SrsRow {
            srs_name: "WGS 84 geodetic".into(),
            srs_id: 4326,
            organization: "EPSG".into(),
            organization_coordsys_id: 4326,
            definition: WGS84_WKT.into(),
            description: Some(
                "longitude/latitude coordinates in decimal degrees on the WGS 84 spheroid".into(),
            ),
        },
    ]
}

/// The spatial reference row for a geometry dtype's SRID.
///
/// GDAL writes the CRS name and its WKT from the PROJ database. Without PROJ, other codes get the
/// authority string as the name and an `undefined` definition: GDAL resolves EPSG codes from
/// `organization` + `organization_coordsys_id` when reading, but not ESRI codes other than the
/// ones with a definition here (reported as having no CRS).
pub fn srs_row_for(srid: &Srid) -> SrsRow {
    let (org, code) = match srid {
        Srid::Code(c) => ("EPSG".to_string(), *c),
        Srid::Authority(a) => match a.rsplit_once(':') {
            Some((org, code)) => (
                org.trim().to_uppercase(),
                code.trim().parse::<i64>().unwrap_or(0),
            ),
            None => ("EPSG".to_string(), a.trim().parse::<i64>().unwrap_or(0)),
        },
    };
    if code == 0 {
        // `GeoDataFrame.to_file` without a CRS: GDAL's "Undefined SRS".
        return SrsRow {
            srs_name: "Undefined SRS".into(),
            srs_id: UNDEFINED_SRS_ID,
            organization: "GDAL".into(),
            organization_coordsys_id: UNDEFINED_SRS_ID,
            definition: UNDEFINED_SRS_WKT.into(),
            description: Some("Custom undefined coordinate reference system".into()),
        };
    }
    if org == "EPSG" && code == 4326 {
        return default_srs_rows().remove(2);
    }
    if org == "ESRI" && code == 102003 {
        return SrsRow {
            srs_name: "USA_Contiguous_Albers_Equal_Area_Conic".into(),
            srs_id: code,
            organization: org,
            organization_coordsys_id: code,
            definition: ESRI_102003_WKT.into(),
            description: None,
        };
    }
    SrsRow {
        srs_name: format!("{org}:{code}"),
        srs_id: code,
        organization: org,
        organization_coordsys_id: code,
        definition: "undefined".into(),
        description: None,
    }
}

/// The geometry columns of a table's dtypes, in column order.
pub fn geometry_columns(dtypes: &IndexMap<String, String>) -> Vec<(String, String)> {
    dtypes
        .iter()
        .filter(|(_, t)| {
            let t = t.to_lowercase();
            t.starts_with("geography") || are_dtypes_equal(&t, "geometry")
        })
        .map(|(c, t)| (c.clone(), t.clone()))
        .collect()
}

/// `gpkg_geometry_columns.geometry_type_name` as v4 leaves it.
///
/// PARITY: v4 only overwrites GDAL's `GEOMETRY` (written for an empty frame) for columns whose
/// *name* contains `geometry` or `geography` (`_pipes.py:4413` tests `col`, not the dtype).
pub fn geometry_type_name(col: &str, gtype: &str) -> String {
    let lower = col.to_lowercase();
    if lower.contains("geometry") || lower.contains("geography") {
        gtype.to_uppercase()
    } else {
        "GEOMETRY".to_string()
    }
}

/// The R-tree table name for a table's geometry column (GDAL's `rtree_<table>_<column>`).
pub fn rtree_name(table: &str, col: &str) -> String {
    format!("rtree_{table}_{col}")
}

fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// The queries creating (or rebuilding) a GDAL-compatible R-tree spatial index.
///
/// The triggers are GDAL's (`rtree_<t>_<c>_insert`, `_update2` .. `_update7`, `_delete`) with
/// the table's `rowid` as the feature id, since pipe tables have no `fid` column. Rebuilding is
/// idempotent: the triggers are replaced and the index is repopulated from the table.
pub fn get_rtree_create_queries(table: &str, col: &str) -> Vec<String> {
    let rt = rtree_name(table, col);
    let (t, c, r) = (quote(table), quote(col), quote(&rt));
    let id = "rowid";
    let bbox = |which: &str| {
        format!(
            "ST_MinX({which}.{c}), ST_MaxX({which}.{c}),ST_MinY({which}.{c}), ST_MaxY({which}.{c})"
        )
    };
    let trigger = |suffix: &str| quote(&format!("{rt}_{suffix}"));
    let mut queries: Vec<String> = [
        "insert", "update2", "update4", "update5", "update6", "update7", "delete",
    ]
    .iter()
    .map(|s| format!("DROP TRIGGER IF EXISTS {}", trigger(s)))
    .collect();
    queries.extend([
        format!("CREATE VIRTUAL TABLE IF NOT EXISTS {r} USING rtree(id, minx, maxx, miny, maxy)"),
        format!("DELETE FROM {r}"),
        format!(
            "INSERT OR REPLACE INTO {r} SELECT {t}.{id}, ST_MinX({c}), ST_MaxX({c}), ST_MinY({c}), ST_MaxY({c}) FROM {t} WHERE {c} NOT NULL AND NOT ST_IsEmpty({c})"
        ),
        format!(
            "CREATE TRIGGER {} AFTER INSERT ON {t} WHEN (new.{c} NOT NULL AND NOT ST_IsEmpty(NEW.{c})) BEGIN INSERT OR REPLACE INTO {r} VALUES (NEW.{id},{}); END",
            trigger("insert"),
            bbox("NEW")
        ),
        format!(
            "CREATE TRIGGER {} AFTER UPDATE OF {c} ON {t} WHEN OLD.{id} = NEW.{id} AND (NEW.{c} NOTNULL AND NOT ST_IsEmpty(NEW.{c})) AND (OLD.{c} NOTNULL AND NOT ST_IsEmpty(OLD.{c})) BEGIN UPDATE {r} SET minx = ST_MinX(NEW.{c}), maxx = ST_MaxX(NEW.{c}),miny = ST_MinY(NEW.{c}), maxy = ST_MaxY(NEW.{c}) WHERE id = NEW.{id};END",
            trigger("update6")
        ),
        format!(
            "CREATE TRIGGER {} AFTER UPDATE OF {c} ON {t} WHEN OLD.{id} = NEW.{id} AND (NEW.{c} NOTNULL AND NOT ST_IsEmpty(NEW.{c})) AND (OLD.{c} ISNULL OR ST_IsEmpty(OLD.{c})) BEGIN INSERT INTO {r} VALUES (NEW.{id},{}); END",
            trigger("update7"),
            bbox("NEW")
        ),
        format!(
            "CREATE TRIGGER {} AFTER UPDATE OF {c} ON {t} WHEN OLD.{id} = NEW.{id} AND (NEW.{c} ISNULL OR ST_IsEmpty(NEW.{c})) BEGIN DELETE FROM {r} WHERE id = OLD.{id}; END",
            trigger("update2")
        ),
        format!(
            "CREATE TRIGGER {} AFTER UPDATE ON {t} WHEN OLD.{id} != NEW.{id} AND (NEW.{c} NOTNULL AND NOT ST_IsEmpty(NEW.{c})) BEGIN DELETE FROM {r} WHERE id = OLD.{id}; INSERT OR REPLACE INTO {r} VALUES (NEW.{id},{}); END",
            trigger("update5"),
            bbox("NEW")
        ),
        format!(
            "CREATE TRIGGER {} AFTER UPDATE ON {t} WHEN OLD.{id} != NEW.{id} AND (NEW.{c} ISNULL OR ST_IsEmpty(NEW.{c})) BEGIN DELETE FROM {r} WHERE id IN (OLD.{id}, NEW.{id}); END",
            trigger("update4")
        ),
        format!(
            "CREATE TRIGGER {} AFTER DELETE ON {t} WHEN old.{c} NOT NULL BEGIN DELETE FROM {r} WHERE id = OLD.{id}; END",
            trigger("delete")
        ),
        GPKG_EXTENSIONS_DDL.to_string(),
        format!(
            "INSERT INTO gpkg_extensions (table_name, column_name, extension_name, definition, scope) SELECT {tl}, {cl}, 'gpkg_rtree_index', 'http://www.geopackage.org/spec120/#extension_rtree', 'write-only' WHERE NOT EXISTS (SELECT 1 FROM gpkg_extensions WHERE table_name = {tl} AND column_name = {cl} AND extension_name = 'gpkg_rtree_index')",
            tl = literal(table),
            cl = literal(col)
        ),
    ]);
    queries
}

/// The queries dropping a table's R-tree spatial index (its triggers, virtual table and
/// `gpkg_extensions` row).
pub fn get_rtree_drop_queries(table: &str, col: &str, extensions_exist: bool) -> Vec<String> {
    let rt = rtree_name(table, col);
    let mut queries: Vec<String> = [
        "insert", "update2", "update4", "update5", "update6", "update7", "delete",
    ]
    .iter()
    .map(|s| format!("DROP TRIGGER IF EXISTS {}", quote(&format!("{rt}_{s}"))))
    .collect();
    queries.push(format!("DROP TABLE IF EXISTS {}", quote(&rt)));
    if extensions_exist {
        queries.push(format!(
            "DELETE FROM gpkg_extensions WHERE table_name = {} AND column_name = {} AND extension_name = 'gpkg_rtree_index'",
            literal(table),
            literal(col)
        ));
    }
    queries
}

impl SqlConnector {
    fn is_geopackage(&self) -> bool {
        self.flavor == Flavor::Geopackage
    }

    async fn sqlite_object_exists(&self, name: &str) -> bool {
        self.value_params(
            "SELECT 1 FROM sqlite_master WHERE name = ?",
            &[SqlParam::Text(name.to_string())],
        )
        .await
        .ok()
        .flatten()
        .is_some()
    }

    /// Create GDAL's `gpkg_*` tables (and the default spatial reference rows) when missing.
    pub async fn ensure_geopackage_tables(&self) -> bool {
        if self.sqlite_object_exists("gpkg_spatial_ref_sys").await {
            return true;
        }
        let mut queries: Vec<String> = Vec::new();
        for ddl in GPKG_TABLES_DDL {
            let name = ddl
                .trim_start_matches("CREATE TABLE ")
                .split([' ', '('])
                .next()
                .unwrap_or_default();
            if !self.sqlite_object_exists(name).await {
                queries.push(ddl.to_string());
            }
        }
        queries.push(format!("PRAGMA application_id = {GPKG_APPLICATION_ID}"));
        queries.push(format!("PRAGMA user_version = {GPKG_USER_VERSION}"));
        if !self.exec_queries_all(&queries, true, true).await {
            return false;
        }
        for row in default_srs_rows() {
            if !self.insert_srs_row(&row).await {
                return false;
            }
        }
        true
    }

    async fn insert_srs_row(&self, row: &SrsRow) -> bool {
        let exists = self
            .value_params(
                "SELECT 1 FROM gpkg_spatial_ref_sys WHERE srs_id = ?",
                &[SqlParam::Int(row.srs_id)],
            )
            .await
            .ok()
            .flatten()
            .is_some();
        if exists {
            return true;
        }
        self.exec_params(
            "INSERT INTO gpkg_spatial_ref_sys (srs_name, srs_id, organization, organization_coordsys_id, definition, description) VALUES (?, ?, ?, ?, ?, ?)",
            &[
                SqlParam::Text(row.srs_name.clone()),
                SqlParam::Int(row.srs_id),
                SqlParam::Text(row.organization.clone()),
                SqlParam::Int(row.organization_coordsys_id),
                SqlParam::Text(row.definition.clone()),
                SqlParam::opt_text(row.description.as_deref()),
            ],
        )
        .await
        .is_some()
    }

    /// `_init_geopackage_pipe(df, pipe)`: register a new pipe table's geometry column in the
    /// GeoPackage metadata. `dtypes` are the table's columns and dtypes in column order; `df` is
    /// the frame the table is created from.
    ///
    /// The spatial reference is the CRS of `df`'s geometry column, else the dtype's SRID.
    // PARITY: v4 writes the empty frame with `df.head(0).to_file(..., driver='GPKG')`
    // (meerschaum/connectors/sql/_pipes.py, `_init_geopackage_pipe`), so GDAL registers the
    // GeoDataFrame's CRS: a bare `geometry` dtype synced from a GeoDataFrame in EPSG:4326 gets
    // `srs_id` 4326, not the undefined 99999.
    pub async fn init_geopackage_pipe(
        &self,
        sp: &SqlPipe,
        dtypes: &IndexMap<String, String>,
        df: &Frame,
    ) -> SuccessTuple {
        if !self.is_geopackage() {
            return SuccessTuple::ok("Success");
        }
        let table = sp.target.clone();
        let geom_cols = geometry_columns(dtypes);
        let Some((col, typ)) = geom_cols.first().cloned() else {
            return SuccessTuple::ok("Success");
        };
        if !self.ensure_geopackage_tables().await {
            return SuccessTuple::fail(format!("Failed to init table '{table}'."));
        }
        let (gtype, dtype_srid) = get_geometry_type_srid(&typ, "geometry", Srid::Code(0))
            .unwrap_or_else(|_| ("geometry".to_string(), Srid::Code(0)));
        let srid = df
            .column(&col)
            .and_then(|c| c.crs.clone())
            .filter(Srid::truthy)
            .unwrap_or(dtype_srid);
        let srs = srs_row_for(&srid);
        if !self.insert_srs_row(&srs).await {
            return SuccessTuple::fail(format!("Failed to init table '{table}'."));
        }
        let t = SqlParam::Text(table.clone());
        let registered = |sql: &'static str| {
            let t = t.clone();
            async move { self.value_params(sql, &[t]).await.ok().flatten().is_some() }
        };
        let mut queries: Vec<(String, Vec<SqlParam>)> = Vec::new();
        if !registered("SELECT 1 FROM gpkg_contents WHERE table_name = ?").await {
            queries.push((
                "INSERT INTO gpkg_contents (table_name, data_type, identifier, description, last_change, srs_id) VALUES (?, 'features', ?, '', strftime('%Y-%m-%dT%H:%M:%fZ','now'), ?)".into(),
                vec![t.clone(), t.clone(), SqlParam::Int(srs.srs_id)],
            ));
        }
        if !registered("SELECT 1 FROM gpkg_geometry_columns WHERE table_name = ?").await {
            queries.push((
                "INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m) VALUES (?, ?, ?, ?, 0, 0)".into(),
                vec![
                    t.clone(),
                    SqlParam::Text(col.clone()),
                    SqlParam::Text(geometry_type_name(&col, &gtype)),
                    SqlParam::Int(srs.srs_id),
                ],
            ));
        }
        if self.sqlite_object_exists("gpkg_ogr_contents").await {
            if registered("SELECT 1 FROM gpkg_ogr_contents WHERE table_name = ?").await {
                queries.push((
                    "UPDATE gpkg_ogr_contents SET feature_count = NULL WHERE table_name = ?".into(),
                    vec![t.clone()],
                ));
            } else {
                queries.push((
                    "INSERT INTO gpkg_ogr_contents (table_name, feature_count) VALUES (?, NULL)"
                        .into(),
                    vec![t.clone()],
                ));
            }
        }
        if !self.sqlite_object_exists("geometry_columns").await {
            queries.push((GEOMETRY_COLUMNS_VIEW.to_string(), Vec::new()));
        }
        if queries.is_empty() {
            return SuccessTuple::ok("Success");
        }
        match self.driver().exec_many_with(&queries).await {
            Ok(r) if r.iter().all(Option::is_some) => SuccessTuple::ok("Success"),
            _ => SuccessTuple::fail(format!("Failed to init table '{table}'.")),
        }
    }

    /// The geometry column registered for a table in `gpkg_geometry_columns`.
    pub async fn geopackage_geometry_column(&self, table: &str) -> Option<String> {
        if !self.is_geopackage() || !self.sqlite_object_exists("gpkg_geometry_columns").await {
            return None;
        }
        match self
            .value_params(
                "SELECT column_name FROM gpkg_geometry_columns WHERE table_name = ?",
                &[SqlParam::Text(table.to_string())],
            )
            .await
            .ok()
            .flatten()?
        {
            SqlValue::Text(s) => Some(s),
            _ => None,
        }
    }

    /// The index keys of a pipe that are spatial indices on GeoPackage: an index over exactly
    /// the table's registered geometry column.
    pub async fn geopackage_spatial_index_keys(&self, sp: &SqlPipe) -> Vec<(String, String)> {
        let Some(geom_col) = self.geopackage_geometry_column(&sp.target).await else {
            return Vec::new();
        };
        sp.index_columns()
            .into_iter()
            .filter(|(_, cols)| cols.as_vec() == vec![geom_col.clone()])
            .map(|(k, _)| (k, geom_col.clone()))
            .collect()
    }

    /// Remove a dropped table's GeoPackage metadata rows and spatial indices.
    pub async fn drop_geopackage_metadata(&self, table: &str) -> bool {
        if !self.is_geopackage() || !self.sqlite_object_exists("gpkg_contents").await {
            return true;
        }
        let mut queries: Vec<String> = Vec::new();
        if let Some(col) = self.geopackage_geometry_column(table).await {
            queries.extend(get_rtree_drop_queries(
                table,
                &col,
                self.sqlite_object_exists("gpkg_extensions").await,
            ));
        }
        let lit = literal(table);
        for meta in [
            "gpkg_geometry_columns",
            "gpkg_ogr_contents",
            "gpkg_contents",
        ] {
            if self.sqlite_object_exists(meta).await {
                queries.push(format!("DELETE FROM {meta} WHERE table_name = {lit}"));
            }
        }
        self.exec_queries_all(&queries, false, true).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn srs_rows() {
        assert_eq!(srs_row_for(&Srid::Code(4326)).srs_name, "WGS 84 geodetic");
        assert_eq!(srs_row_for(&Srid::Code(0)).srs_id, UNDEFINED_SRS_ID);
        let esri = srs_row_for(&Srid::Authority("ESRI:102003".into()));
        assert_eq!((esri.organization.as_str(), esri.srs_id), ("ESRI", 102003));
        let epsg = srs_row_for(&Srid::Authority("EPSG:2263".into()));
        assert_eq!((epsg.organization.as_str(), epsg.srs_id), ("EPSG", 2263));
    }

    #[test]
    fn type_names() {
        assert_eq!(
            geometry_type_name("geometry", "MultiPolygon"),
            "MULTIPOLYGON"
        );
        assert_eq!(geometry_type_name("geom", "Point"), "GEOMETRY");
    }

    #[test]
    fn rtree_queries() {
        let q = get_rtree_create_queries("t", "geom");
        assert!(
            q.iter()
                .any(|s| s.contains("USING rtree(id, minx, maxx, miny, maxy)"))
        );
        assert!(
            q.iter()
                .any(|s| s.starts_with("CREATE TRIGGER \"rtree_t_geom_insert\""))
        );
        let geo = geometry_columns(&IndexMap::from([
            ("a".to_string(), "int".to_string()),
            ("g".to_string(), "geometry[Point, 4326]".to_string()),
            ("h".to_string(), "geography".to_string()),
        ]));
        assert_eq!(geo.len(), 2);
    }
}
