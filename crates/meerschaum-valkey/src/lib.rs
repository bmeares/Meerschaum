//! `meerschaum-valkey`: the Valkey instance connector (`docs/design/v5.0.0.md` §4.1), a port of
//! `meerschaum/connectors/valkey` (v4.1.4) over [`fred`].
//!
//! - [`ValkeyConnector`]: attributes and URI, `get` / `set`, `push_df` / `push_docs`,
//!   `read` / `read_docs`, every instance method (pipes, users, plugins; tokens through the
//!   generic tokens pipe), `fetch`, and the §5.8 lease pair (`SET NX PX`).
//! - [`keys`]: the key layout (`mrsm_pipe:*`, `{target}:indices:*`, `mrsm_user:*`, ...).
//! - [`backend`]: the commands used, implemented by [`FredBackend`] (live) and
//!   [`MemoryBackend`] (an in-memory model for tests and offline replays).
//!
//! Python quirks kept on purpose are marked `// PARITY:`.
#![deny(unsafe_code)]

pub mod backend;
pub mod connector;
pub mod dbtypes;
pub mod fred_backend;
pub mod instance;
pub mod keys;
pub mod memory;
pub mod pipes;
pub mod pyjson;
pub mod users;

pub use backend::{BackendError, ScoreBound, ValkeyBackend};
pub use connector::ValkeyConnector;
pub use fred_backend::FredBackend;
pub use memory::MemoryBackend;
