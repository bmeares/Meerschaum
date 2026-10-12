//! `meerschaum-kernel`: the Arrow/Polars data kernel of Meerschaum v5 (design §4.1, §10.1).
//!
//! Ports v4's dataframe semantics to Rust with observable parity:
//!
//! - [`dtypes`], [`value`], [`decimal`], [`cast`]: Meerschaum / pandas / Polars dtype strings and
//!   Python-like scalar casts (`meerschaum/utils/dtypes/__init__.py`).
//! - [`detect`]: dtype detection on frames (`get_json_cols`, `get_numeric_cols`, ...).
//! - [`enforce`]: `enforce_dtypes` and the `Pipe.enforce_dtypes` call path.
//! - [`diff`]: `filter_unseen_df` and the DataFrame part of `Pipe.filter_existing`.
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
pub mod query;
pub mod serialize;
pub mod value;

pub use decimal::Decimal;
pub use frame::{Column, ColumnData, Frame};
pub use geometry::Geometry;
pub use value::Value;
