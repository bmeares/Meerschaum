//! The GeoPackage SQL functions the R-tree spatial index triggers call (`ST_MinX`, `ST_MaxX`,
//! `ST_MinY`, `ST_MaxY`, `ST_IsEmpty`; GeoPackage 1.2+ §F.3), registered on every `geopackage`
//! connection. GDAL registers the same functions on its own connections, so the triggers this
//! connector creates are the ones GDAL creates (see `instance/geopackage.rs`).
//!
//! A plain SQLite client without these functions (e.g. Python's `sqlite3`) cannot insert into a
//! spatially indexed table, as with any GDAL-written GeoPackage.
#![allow(unsafe_code)]

use std::ffi::c_int;
use std::panic::{AssertUnwindSafe, catch_unwind};

use libsqlite3_sys as ffi;
use meerschaum_kernel::geometry::{Geometry, Shape, gpkg_wkb_to_wkb};
use sqlx::sqlite::SqliteConnection;

/// A geometry's bounding box.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Envelope {
    pub min_x: f64,
    pub max_x: f64,
    pub min_y: f64,
    pub max_y: f64,
}

/// The empty-geometry flag of the GeoPackage binary header.
const EMPTY_FLAG: u8 = 0x10;

fn extend(env: &mut Option<Envelope>, c: &[f64]) {
    let (Some(&x), Some(&y)) = (c.first(), c.get(1)) else {
        return;
    };
    if x.is_nan() || y.is_nan() {
        return;
    }
    match env {
        Some(e) => {
            e.min_x = e.min_x.min(x);
            e.max_x = e.max_x.max(x);
            e.min_y = e.min_y.min(y);
            e.max_y = e.max_y.max(y);
        }
        None => {
            *env = Some(Envelope {
                min_x: x,
                max_x: x,
                min_y: y,
                max_y: y,
            })
        }
    }
}

fn shape_envelope(g: &Geometry, env: &mut Option<Envelope>) {
    match &g.shape {
        Shape::Point(c) => c.iter().for_each(|c| extend(env, c)),
        Shape::LineString(cs) => cs.iter().for_each(|c| extend(env, c)),
        Shape::Polygon(rings) => rings.iter().flatten().for_each(|c| extend(env, c)),
        Shape::MultiPoint(ps) => ps.iter().flatten().for_each(|c| extend(env, c)),
        Shape::MultiLineString(ls) => ls.iter().flatten().for_each(|c| extend(env, c)),
        Shape::MultiPolygon(ps) => ps.iter().flatten().flatten().for_each(|c| extend(env, c)),
        Shape::GeometryCollection(gs) => gs.iter().for_each(|g| shape_envelope(g, env)),
    }
}

/// Parse a GeoPackage geometry blob (or plain WKB).
fn parse(blob: &[u8]) -> Option<Geometry> {
    let wkb = if blob.starts_with(b"GP") {
        gpkg_wkb_to_wkb(blob).ok()?.0
    } else {
        blob.to_vec()
    };
    Geometry::from_wkb(&wkb).ok()
}

/// `ST_IsEmpty(geom)`: the header's empty flag, or a geometry without coordinates.
pub fn gpkg_is_empty(blob: &[u8]) -> bool {
    if blob.starts_with(b"GP") && blob.len() >= 4 && blob[3] & EMPTY_FLAG != 0 {
        return true;
    }
    gpkg_envelope(blob).is_none()
}

/// The bounding box of a GeoPackage geometry blob (`None` when empty or unreadable).
pub fn gpkg_envelope(blob: &[u8]) -> Option<Envelope> {
    let g = parse(blob)?;
    let mut env = None;
    shape_envelope(&g, &mut env);
    env
}

/// The blob argument of a scalar function call.
///
/// # Safety
/// `arg` must be a valid `sqlite3_value` for the duration of the call.
unsafe fn blob_arg<'a>(arg: *mut ffi::sqlite3_value) -> Option<&'a [u8]> {
    // SAFETY: `arg` is a protected value owned by SQLite for the duration of the callback; the
    // pointer from `sqlite3_value_blob` stays valid until the value is converted or freed, which
    // does not happen before the callback returns.
    unsafe {
        if ffi::sqlite3_value_type(arg) != ffi::SQLITE_BLOB {
            return None;
        }
        let len = ffi::sqlite3_value_bytes(arg);
        let ptr = ffi::sqlite3_value_blob(arg);
        if ptr.is_null() || len <= 0 {
            return Some(&[]);
        }
        Some(std::slice::from_raw_parts(ptr.cast::<u8>(), len as usize))
    }
}

/// Shared body of the envelope functions.
///
/// # Safety
/// Called by SQLite with a valid context and `argc` valid arguments.
unsafe fn envelope_fn(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
    pick: fn(&Envelope) -> f64,
) {
    // SAFETY: SQLite passes `argc` valid argument pointers and a valid context.
    unsafe {
        if argc != 1 {
            ffi::sqlite3_result_null(ctx);
            return;
        }
        let blob = blob_arg(*argv);
        let env = blob.and_then(|b| catch_unwind(AssertUnwindSafe(|| gpkg_envelope(b))).ok()?);
        match env {
            Some(e) => ffi::sqlite3_result_double(ctx, pick(&e)),
            None => ffi::sqlite3_result_null(ctx),
        }
    }
}

unsafe extern "C" fn st_minx(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: forwarded from SQLite's callback arguments.
    unsafe { envelope_fn(ctx, argc, argv, |e| e.min_x) }
}

unsafe extern "C" fn st_maxx(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: forwarded from SQLite's callback arguments.
    unsafe { envelope_fn(ctx, argc, argv, |e| e.max_x) }
}

unsafe extern "C" fn st_miny(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: forwarded from SQLite's callback arguments.
    unsafe { envelope_fn(ctx, argc, argv, |e| e.min_y) }
}

unsafe extern "C" fn st_maxy(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: forwarded from SQLite's callback arguments.
    unsafe { envelope_fn(ctx, argc, argv, |e| e.max_y) }
}

unsafe extern "C" fn st_isempty(
    ctx: *mut ffi::sqlite3_context,
    argc: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: SQLite passes `argc` valid argument pointers and a valid context.
    unsafe {
        if argc != 1 {
            ffi::sqlite3_result_null(ctx);
            return;
        }
        match blob_arg(*argv) {
            Some(b) => {
                let empty = catch_unwind(AssertUnwindSafe(|| gpkg_is_empty(b))).unwrap_or(true);
                ffi::sqlite3_result_int(ctx, empty as c_int);
            }
            None => ffi::sqlite3_result_null(ctx),
        }
    }
}

type ScalarFn =
    unsafe extern "C" fn(*mut ffi::sqlite3_context, c_int, *mut *mut ffi::sqlite3_value);

const FUNCTIONS: &[(&[u8], ScalarFn)] = &[
    (b"ST_MinX\0", st_minx),
    (b"ST_MaxX\0", st_maxx),
    (b"ST_MinY\0", st_miny),
    (b"ST_MaxY\0", st_maxy),
    (b"ST_IsEmpty\0", st_isempty),
];

/// Register the functions on a connection (the pool's `after_connect` hook).
pub(crate) async fn register(conn: &mut SqliteConnection) -> Result<(), sqlx::Error> {
    let mut handle = conn.lock_handle().await?;
    let db = handle.as_raw_handle().as_ptr();
    for (name, func) in FUNCTIONS {
        // SAFETY: `db` is the open connection held locked by `handle` for this call; `name` is
        // a NUL-terminated static string; the callbacks are `extern "C"` functions with
        // SQLite's scalar-function signature that take no user data.
        let rc = unsafe {
            ffi::sqlite3_create_function_v2(
                db,
                name.as_ptr().cast(),
                1,
                ffi::SQLITE_UTF8 | ffi::SQLITE_DETERMINISTIC,
                std::ptr::null_mut(),
                Some(*func),
                None,
                None,
                None,
            )
        };
        if rc != ffi::SQLITE_OK {
            return Err(sqlx::Error::Protocol(format!(
                "Could not register the GeoPackage function {} (code {rc}).",
                String::from_utf8_lossy(&name[..name.len() - 1])
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelopes() {
        let g = Geometry::from_wkt("LINESTRING (0 1, 4 -2)").unwrap();
        let blob = g.to_gpkg_wkb(None);
        assert_eq!(
            gpkg_envelope(&blob),
            Some(Envelope {
                min_x: 0.0,
                max_x: 4.0,
                min_y: -2.0,
                max_y: 1.0
            })
        );
        assert!(!gpkg_is_empty(&blob));
        let empty = Geometry::from_wkt("POINT EMPTY").unwrap().to_gpkg_wkb(None);
        assert!(gpkg_is_empty(&empty));
    }
}
