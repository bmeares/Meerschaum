//! **Internal crate** of [Meerschaum](https://meerschaum.io): its API may change in any minor
//! release. Depend on the [`meerschaum`](https://docs.rs/meerschaum) crate instead.
//!
//! `meerschaum-jobs`: job supervision for Meerschaum v5 (`docs/design/v5.0.0.md` §6.4).
//!
//! Jobs keep v4's on-disk layout under `$MRSM_ROOT_DIR`, so jobs created by Python 4.1 are read by
//! Rust and vice versa:
//!
//! ```text
//! jobs/<name>/properties.json    label, target, result, process.{launched,launch_id,began,paused,ended},
//!                                env, restart, owner, team (see [`ownership`]), ...
//! jobs/<name>/process.pid        the PID of the process doing the work (flock'ed while it runs)
//! jobs/<name>/input.stdin        FIFO (0600) feeding the job's stdin; input.stdin.block while waiting
//! jobs/<name>/.stop.json         {"stop_time": ..., "action": "quit" | "kill" | "pause"}
//! jobs/<name>/prompt_kwargs.json, .externally-managed, pickle.pkl (Python only)
//! jobs/<name>/laps.json          lap heartbeats of a looping job (v5, see [`health`])
//! logs/<name>.log.<n>            rotating log subfiles; logs/.<name>.log.offset is the read cursor
//! ```
//!
//! Modules:
//!
//! - [`daemon`]: `Daemon` (properties, status, signals, `run` / `kill` / `quit` / `pause` / `resume`);
//! - [`supervisor`]: the detached process which runs a job (double fork, stdio into the log);
//! - [`job`]: `Job` and `monitor_logs`;
//! - [`executor`] / [`systemd`] / [`remote`]: the executor interface (`local`, `systemd` over a
//!   replaceable [`runner::ProcessRunner`], `api:<label>` over an injected [`remote::RemoteJobs`]
//!   client); [`systemd_job`] is what runs inside a unit;
//! - [`rotating`], [`stdin`], [`properties`]: the file formats;
//! - [`schedule`]: the `-s` grammar; [`laps`]: the lap loop; [`scheduler`]: per-root schedule
//!   and debounce state; [`health`]: the lap heartbeats of a job and the health computed from them;
//! - [`startup`]: the per-root startup admission gate (jitter, load and memory checks, file-lock
//!   slots) every job passes before its worker starts;
//! - [`ownership`]: who a job belongs to (`owner`, `team`: see
//!   [`meerschaum_core::job_access`]) and the update of a team's jobs when it is renamed or
//!   deleted.
//!
//! Behavior follows the Python implementation (v4.1.4); preserved quirks are marked `// PARITY:`.

#![warn(missing_docs)]

pub mod config;
pub mod daemon;
pub mod detached;
pub mod executor;
pub mod health;
pub mod job;
pub mod laps;
pub mod names;
pub mod ownership;
pub mod process;
pub mod properties;
pub mod pytime;
pub mod remote;
pub mod rotating;
pub mod runner;
pub mod schedule;
pub mod scheduler;
pub mod shlex;
pub mod startup;
pub mod stdin;
pub mod supervisor;
pub mod systemd;
pub mod systemd_job;

pub use config::{JobPaths, JobsConfig, Seconds};
pub use daemon::{Daemon, JobsContext, Status};
pub use executor::{JobExecutor, LocalExecutor, get_jobs};
pub use health::{HealthState, Heartbeat, JobHealth};
pub use job::{Job, JobOptions, MonitorControl, MonitorOptions};
pub use laps::{LapPacer, LapPause, LoopControl, LoopRunner, SleepFrom};
pub use remote::{RemoteJobs, RemoteJobsResolver};
pub use rotating::RotatingFile;
pub use runner::{FakeSystemctl, ProcessRunner, SystemRunner};
pub use schedule::{JitterSpec, Trigger, parse_schedule, parse_schedule_jittered};
pub use scheduler::{RootScheduler, ScheduleState};
pub use startup::{StartKind, StartupConfig, StartupPolicy};
pub use supervisor::Launcher;
