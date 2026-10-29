//! The geometry CRS through `filter_unseen_df` / `filter_existing` (v4.1.4 behavior).
//!
//! v4 builds the delta with a pandas merge, so the delta of two GeoDataFrames is a plain
//! DataFrame without a CRS; without an `old` frame the new frame comes back unchanged. The SQL
//! instance types its update table from the delta, so a CRS that survives here changes the SRID
//! PostGIS stores on updated rows.

use indexmap::IndexMap;
use meerschaum_core::dtypes::Srid;
use meerschaum_kernel::detect::get_geometry_cols_types;
use meerschaum_kernel::diff::{FilterUnseenOptions, filter_unseen_df};
use meerschaum_kernel::{Column, ColumnData, Frame, Geometry, Value};

fn frame(ids: &[i64], points: &[(f64, f64)], crs: Option<i64>) -> Frame {
    let ids = Column::new(
        "id",
        ColumnData::Object(ids.iter().map(|i| Value::Int(*i)).collect()),
    );
    let mut geom = Column::new(
        "geom",
        ColumnData::Object(
            points
                .iter()
                .map(|(x, y)| {
                    Value::Geometry(Geometry::from_wkt(&format!("POINT ({x} {y})")).unwrap())
                })
                .collect(),
        ),
    );
    geom.crs = crs.map(Srid::Code);
    Frame::new(vec![ids, geom]).unwrap()
}

fn crs(df: &Frame) -> Option<Srid> {
    df.column("geom").and_then(|c| c.crs.clone())
}

#[test]
fn delta_against_an_old_frame_has_no_crs() {
    let old = frame(&[1], &[(0.0, 0.0)], Some(4326));
    let new = frame(&[1, 2], &[(0.0, 0.0), (1.0, 1.0)], Some(4326));
    let delta = filter_unseen_df(Some(&old), &new, &FilterUnseenOptions::default());
    assert_eq!(delta.height(), 1);
    assert_eq!(crs(&delta), None);
    // `get_geometry_cols(update_df, with_types_srids=True)` -> ('Point', 0).
    let types: IndexMap<String, String> = get_geometry_cols_types(&delta);
    assert_eq!(
        types.get("geom").map(String::as_str),
        Some("geometry[Point]")
    );

    // An empty old frame still goes through the merge.
    let empty = frame(&[], &[], Some(4326));
    let delta = filter_unseen_df(Some(&empty), &new, &FilterUnseenOptions::default());
    assert_eq!(delta.height(), 2);
    assert_eq!(crs(&delta), None);
}

#[test]
fn without_an_old_frame_the_crs_is_kept() {
    let new = frame(&[1, 2], &[(0.0, 0.0), (1.0, 1.0)], Some(4326));
    let delta = filter_unseen_df(None, &new, &FilterUnseenOptions::default());
    assert_eq!(crs(&delta), Some(Srid::Code(4326)));
    assert_eq!(
        get_geometry_cols_types(&delta)
            .get("geom")
            .map(String::as_str),
        Some("geometry[Point, 4326]")
    );
}
