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
//! - [`actions`], [`dispatch`], [`env`]: action resolution and the entry point.
#![deny(unsafe_code)]

pub mod actions;
pub mod argparse;
pub mod command;
pub mod dateutil;
pub mod dispatch;
pub mod env;
pub mod help;
pub mod parse;
pub mod pyjson;
pub mod pyliteral;
pub mod schema;
pub mod shlex;
pub mod spec;
pub mod sysargs;
pub mod types;
pub mod value;

pub use parse::{parse_arguments, parse_line};
pub use value::{ArgsMap, PyException, PyValue};
