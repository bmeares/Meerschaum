//! **Internal crate** of [Meerschaum](https://meerschaum.io): its API may change in any minor
//! release. Depend on the [`meerschaum`](https://docs.rs/meerschaum) crate instead.
//!
//! `meerschaum-worker`: how Rust runs Python (design §4.2, D2; §4.3 manifests; §4.4 adapter;
//! §5.6 v4-pattern streams).
//!
//! - [`protocol`]: the versioned wire protocol (MessagePack control messages, Arrow IPC data,
//!   length-prefixed frames over a Unix socket / Windows named pipe) and value conversions.
//! - [`worker`]: one `python -m meerschaum.worker` process, its handshake and its reader task.
//! - [`pool`]: the pool sized by `system.engine.python_workers`, with lazy spawning, per-call
//!   timeouts, crash isolation and restart backoff.
//! - [`connector`]: [`PythonConnector`], which implements the `Connector` and
//!   `InstanceConnector` traits by forwarding to plugin code.
//! - [`callback`]: the callback channel (`pipe.sync(df)` from plugin code runs natively).
//! - [`stream`]: supervised long-lived streams (v4 `sync()` pattern and `subscribe()`).
//! - [`plugins`]: plugin actions, sync hooks, `setup()`.
//! - [`manifest`]: the plugin manifest schema, source fingerprints and the cache.
//! - [`sync`]: `Pipe.sync()` for a Python source into a native instance.
//!
//! The Python side lives in `meerschaum/worker/`. See `README.md` for the protocol spec.
#![deny(unsafe_code)]
#![warn(missing_docs)]

pub mod callback;
pub mod chunk;
pub mod connector;
pub mod error;
pub mod manifest;
pub mod plugins;
pub mod pool;
pub mod protocol;
pub mod stream;
pub mod sync;
pub mod transport;
pub mod worker;

pub use callback::{CallbackHandler, CallbackRequest, InstanceSyncHandler};
pub use chunk::FetchChunk;
pub use connector::{ConnectorInfo, FetchOptions, FetchOutcome, PythonConnector};
pub use error::{WorkerError, WorkerResult};
pub use manifest::{Manifest, ensure_manifest, manifest_path};
pub use pool::{PoolConfig, WorkerPool};
pub use protocol::PROTOCOL_VERSION;
pub use stream::{PythonStream, StreamEvent, StreamMode, StreamOptions};
pub use worker::{CallOptions, Worker, WorkerOptions};
