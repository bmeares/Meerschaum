//! Isolated teams' processes on the API server's host: the scopes the server records for them,
//! the sandbox a team process applies to itself at start, and team execution's probe and shells.
//!
//! - [`scopes`]: the server's record of each isolated team's scope (`team_scopes/<key>/`).
//! - [`sandbox`]: process-start enforcement of a team's filesystem sandbox.
//! - [`execution`]: team execution's processes (`meerschaum_server::team_execution`).

pub mod execution;
pub mod sandbox;
pub mod scopes;
