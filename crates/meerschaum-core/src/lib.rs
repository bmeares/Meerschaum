//! `meerschaum-core`: see `docs/design/v5.0.0.md` §4.1 for this crate's responsibilities.
//!
//! This crate owns the pure-data core of Meerschaum:
//!
//! - [`value`]: the ordered, Python-compatible [`Value`] used for pipe parameters, config and
//!   `params` filters (the one parameters value type of the workspace).
//! - [`pyfmt`]: Python-compatible formatting and parsing (`json.dumps`, `repr`, `float` repr,
//!   `ast.literal_eval`, `str.strip` / `splitlines`, `textwrap.dedent` / `indent`,
//!   `str.format`, `datetime` strings).
//! - [`yaml`]: a PyYAML-compatible (YAML 1.1) loader and a block emitter.
//! - [`static_config`]: `static.toml` (D15), the port of `STATIC_CONFIG`.
//! - [`config`]: the config registry (`get_config`, `MRSM{}` symlinks, environment overrides, paths).
//! - [`pipe`]: pipe keys, parameters and the pure-data pipe attributes.
//! - [`dtypes`]: the Meerschaum dtype vocabulary, `geometry[...]` parsing, precision units, the
//!   database → pandas type table, and the Arrow field-tag keys shared by every crate.
//! - [`template`]: `{{ Pipe() }}` templating as a parser, an AST and a resolver trait.
//! - [`success`]: `SuccessTuple` and `SyncResult`.
//! - [`users`]: `User` and `Token` types and passlib-compatible password hashing.
//!
//! Behavior follows the Python implementation (v4.1.4). Deliberately preserved Python quirks
//! are marked in the source with `// PARITY:` comments.

#![deny(unsafe_code)]
#![warn(missing_docs)]

pub mod config;
pub mod dtypes;
pub mod pipe;
pub mod pyfmt;
pub mod static_config;
pub mod success;
pub mod template;
pub mod users;
pub mod value;
pub mod version;
pub mod yaml;

pub use config::Config;
pub use pipe::Pipe;
pub use static_config::{StaticConfig, static_config};
pub use success::{SuccessTuple, SyncResult};
pub use value::{Map, Value};
