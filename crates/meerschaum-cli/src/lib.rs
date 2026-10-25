//! **Internal library** of the `mrsm` binary: its API may change in any minor release. Depend on the
//! [`meerschaum`](https://docs.rs/meerschaum) crate for the Rust API.
//!
//! `meerschaum-cli`: the `mrsm` binary's argument parser and dispatch (Meerschaum v5).
//!
//! See `docs/design/v5.0.0.md` §4.3. The argument surface is ported from v4's
//! `meerschaum/_internal/arguments/_parser.py` and must parse every command line exactly as v4
//! does (§11.1 "Jobs and CLI", §14 "Argument parity"):
//!
//! - [`spec`]: the declarative argument table (every flag, alias, type, `nargs`, help text).
//! - [`command`]: the clap [`Command`](clap::Command) built from the table (introspection).
//! - [`argparse`]: the `argparse.parse_known_args` engine that reproduces v4's parsing.
//! - [`parse`]: `parse_arguments`, `parse_line`, `parse_synonyms`.
//! - [`sysargs`]: `+` chaining, `:` pipeline flags, executor keys, sysargs round trip.
//! - [`types`], [`dateutil`], [`pyjson`], [`pyliteral`], [`shlex`]: the `type=` callables and
//!   the Python behaviors they depend on.
//! - [`schema`]: the JSON schema (`mrsm --print-arg-schema`).
//! - [`actions`], [`dispatch`], [`env`](mod@env): action resolution and the entry point.
//! - [`native`]: the instance and job actions (`show`/`sync`/`verify`/... pipes, users, jobs).
#![deny(unsafe_code)]

pub mod action_docs;
pub mod actions;
pub mod argparse;
pub mod builtin;
pub mod command;
pub mod completions;
pub mod compose_action;
pub mod dateutil;
pub mod dispatch;
pub mod env;
pub mod extras;
pub mod format;
pub mod help;
pub mod jobs_backend;
pub mod native;
pub mod native_plugins;
pub mod parse;
pub mod pyjson;
pub mod pyliteral;
pub mod run;
pub mod schema;
pub mod shell;
pub mod shlex;
pub mod spec;
pub mod sysargs;
pub mod types;
pub mod value;

pub use parse::{parse_arguments, parse_line};
pub use run::{run as run_cli, run_main, set_entry_child_marker};

/// The version `show version` and `--version` print.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub use value::{ArgsMap, PyException, PyValue};

/// Python action and manifest integration.
pub mod python;
/// Rust HTTP and MCP server startup.
pub mod server;
