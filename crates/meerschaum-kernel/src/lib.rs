//! **Internal crate** of [Meerschaum](https://meerschaum.io): its API may change in any minor
//! release. Depend on the [`meerschaum`](https://docs.rs/meerschaum) crate instead.
//!
//! `meerschaum-kernel`: the Arrow/Polars data kernel of Meerschaum v5 (design §4.1, §10.1).
//!
//! Ports v4's dataframe semantics to Rust with observable parity:
//!
//! - [`dtypes`], [`value`], [`decimal`], [`cast`]: pandas / Polars dtype strings (the Meerschaum
//!   dtype vocabulary itself comes from `meerschaum_core::dtypes`) and Python-like scalar casts
//!   (`meerschaum/utils/dtypes/__init__.py`). [`Value`] is the *cell* scalar of object columns
//!   (with `pd.NA`, `NaT`, decimals, geometries); it converts to and from
//!   `meerschaum_core::Value`, the parameters value.
//! - [`frame`]: [`Frame`], the in-memory table type of every crate. Arrow batches convert into it
//!   ([`Frame::from_record_batches`]), keeping the `meerschaum:dtype` / `meerschaum:db_type`
//!   field tags on each column ([`frame::ColumnTags`]).
//! - [`detect`]: dtype detection on frames (`get_json_cols`, `get_numeric_cols`, ...).
//! - [`enforce`]: `enforce_dtypes` and the `Pipe.enforce_dtypes` call path.
//! - [`diff`]: `filter_unseen_df` and the DataFrame part of `Pipe.filter_existing`;
//!   [`merge_dtype`] decides the dtypes of its result the way pandas' `merge` does.
//! - [`serialize`], [`geometry`]: JSON / UUID / numeric / bytes / date / datetime / geometry
//!   (WKB with the CRS carried as GeoArrow metadata, D10).
//! - [`query`]: `query_df` with the `params` grammar.
//!
//! Quirks preserved on purpose are marked `// PARITY:` with a pointer to the v4 source.
#![deny(unsafe_code)]

pub mod cast;
pub mod decimal;
pub mod detect;
pub mod diff;
pub mod docs;
pub mod dtypes;
pub mod enforce;
pub mod frame;
pub mod geometry;
pub mod merge_dtype;
pub mod query;
pub mod serialize;
pub mod value;

pub use decimal::Decimal;
pub use frame::{Column, ColumnData, Frame};
pub use geometry::Geometry;
pub use value::Value;
