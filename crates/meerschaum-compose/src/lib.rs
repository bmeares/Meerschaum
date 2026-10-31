//! **Internal crate** of [Meerschaum](https://meerschaum.io): its API may change in any minor
//! release. Depend on the [`meerschaum`](https://docs.rs/meerschaum) crate instead.
//!
//! `meerschaum-compose`: Meerschaum Compose (§6.3 of `docs/design/v5.0.0.md`), a port of
//! `meerschaum/compose/` and `meerschaum/actions/compose.py`.
//!
//! - [`model`]: a typed, lenient serde model of `mrsm-compose.yaml`.
//! - [`envyaml`] and [`dotenv`]: the `${VAR}` / `$VAR` substitution (envyaml) and `.env` loading
//!   (python-dotenv) the Python implementation uses, with the same precedence.
//! - [`project`]: locating the compose file, resolving the project (connectors with
//!   `'{type:label}'` host references, `MRSM{}` symlinks, `{MRSM_ROOT_DIR}` / `{__file__}`,
//!   root and plugins directories) and the per-project environment.
//! - [`pipes`], [`jobs`], [`plugins`]: the defined pipes, job commands and ownership, plugin
//!   installation and the obsolete `compose` plugin detection.
//! - [`command`]: the exact commands compose runs (`run_mrsm_command`), through a runner.
//! - [`ops`]: the instance connector, job supervisor and prompt as traits, with in-memory fakes.
//! - [`subactions`]: `up`, `down`, `run`, `explain`, `init`, `ps`, `logs` and the passthrough
//!   (`sync pipes`, `start/stop/delete jobs` scoped to the project, `start api`, the shell).
//! - [`tree`]: `include:` and `parent:` (v5): compose trees, their overrides and their order.
//! - [`lint`]: the `explain` lint. [`render`]: the `rich` table layout `explain` prints.
//!
//! Python quirks that are reproduced on purpose are marked `// PARITY:` in the source.

#![deny(unsafe_code)]
#![warn(missing_docs)]

pub mod command;
pub mod dotenv;
pub mod env;
pub mod envyaml;
pub mod error;
pub mod jobs;
pub mod lint;
pub mod model;
pub mod ops;
pub mod pipelines;
pub mod pipes;
pub mod plugins;
pub mod project;
pub mod refs;
pub mod render;
pub mod shlex;
pub mod subactions;
pub mod tree;

pub use command::{CommandRunner, MrsmCommand, RecordingRunner};
pub use error::{ComposeError, Message, Messages};
pub use ops::{
    InMemoryInstance, InMemoryJobs, InstanceOps, JobInfo, JobOps, PipeFilters, Prompter,
};
pub use project::{Host, Project, Session};
pub use subactions::{ComposeArgs, ComposeOutput, Services, compose};
