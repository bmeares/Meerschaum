//! The job actions over `meerschaum-jobs`: `start jobs` (`actions/start.py`), `stop jobs`
//! (`actions/stop.py`), `pause jobs` (`actions/pause.py`), `restart jobs`
//! (`actions/restart.py`), `attach jobs|logs` (`actions/attach.py`), `delete jobs`
//! (`actions/delete.py`), `show jobs` / `show logs` (`actions/show.py`), `pprint_jobs`
//! (`utils/formatting/_jobs.py`), the `--schedule` loop (`utils/schedule.py`) and the process-wide
//! stop flag (`utils/threading.py` `stop_requested`).

use std::sync::Arc;

use indexmap::IndexMap;
use meerschaum_core::SuccessTuple;
use meerschaum_jobs::config::{JobPaths, JobsConfig};
use meerschaum_jobs::executor::get_filtered_jobs_for;
use meerschaum_jobs::{Job, JobOptions, JobsContext, Launcher, Status};

use crate::dispatch::Runtime;
use crate::value::ArgsMap;

use super::{Session, float, jobs_laps, prompt, question_text, session, sync};
use super::{string, warn};

mod edit;
mod lifecycle;
mod listing;
mod logs;
mod schedule;
mod stop_flag;

pub use edit::edit_jobs;
pub use lifecycle::{
    compose_env, delete_jobs, emit_job_warnings, move_job_to_systemd, moves_local_jobs_to_systemd,
    pause_jobs, restart_jobs, start_jobs, start_kind_of, stop_jobs,
};
pub use listing::{health_line, pprint_jobs, show_jobs};
pub use logs::{attach_jobs, show_logs};
pub use schedule::{run_schedule, run_schedule_jittered};
pub use stop_flag::{
    disown_signals, install_stop_handler, is_embedded, own_signals, release_stop_handler,
    reset_stop_request, set_embedded, signals_owned, sleep_unless_stopped, stop_handler_installed,
    stop_requested_flag, with_call_stop,
};

/// A lightweight local-only [`JobsContext`] for shell completions (never loads native plugins
/// or remote resolvers, so interactive shell startup does not block on a background manifest
/// refresh).
pub fn local_jobs_context(rt: &mut Runtime<'_>) -> JobsContext {
    let config = rt.config();
    JobsContext::new(
        JobPaths::from_config(config),
        JobsConfig::from_config(config),
        Launcher::from_env(),
    )
}

/// The jobs context for the runtime's config registry.
///
/// Besides the paths and configuration it installs the resolver for `api:<label>` executors
/// ([`super::remote_jobs::CliRemoteResolver`], over the session's runtime): the jobs crate owns
/// the client trait and the CLI injects the `APIConnector` implementation.
pub fn jobs_context(rt: &mut Runtime<'_>) -> JobsContext {
    let session = super::session(rt);
    let config = rt.config();
    // `<type>:<label>` executors of native plugins (none: the context resolves no other types).
    let natives = session.native_plugins(config).ok();
    let resolver = super::remote_jobs::CliRemoteResolver::new(
        session.clone(),
        super::remote_jobs::CliRemoteResolver::section(config),
    );
    let context = JobsContext::new(
        JobPaths::from_config(config),
        JobsConfig::from_config(config),
        Launcher::from_env(),
    )
    .with_remote(Arc::new(resolver));
    match natives.filter(|n| !n.executor_types().is_empty()) {
        // The executors' host calls (`host::plugin_config()`, ...) use the session's connectors.
        Some(natives) => context.with_executors(Arc::new(
            crate::native_executors::NativeExecutors::new(natives, config)
                .with_host(session, config),
        )),
        None => context,
    }
}

/// A failed operation on a job of a native plugin's executor shows the executor's reason
/// (``"`pause_job` is not supported by the 'procs:main' executor."``); the built-in executors
/// keep v4's summary-only output.
fn warn_custom_executor_failure(rt: &mut Runtime<'_>, job: &Job, result: &SuccessTuple) {
    let kind = job.executor_keys.split(':').next().unwrap_or_default();
    if !result.success && !matches!(kind, "local" | "systemd" | "api" | "None" | "") {
        warn(rt, &result.message);
    }
}

fn executor_keys(args: &ArgsMap) -> Option<String> {
    string(args, "executor_keys")
}

/// `get_filtered_jobs(executor_keys, action, warn=..., include_hidden=...)`: the named jobs of
/// the executor (`None`: the context's, combining `local` and `systemd` on a systemd host).
///
/// A failed listing (an unreachable or unauthorized API, a broken `systemctl`) is an `Err` with
/// the failed `SuccessTuple` for the action to return: it is never read as "no jobs".
fn filtered_jobs(
    rt: &mut Runtime<'_>,
    ctx: &JobsContext,
    exec_keys: Option<&str>,
    filter: &[String],
    warn_missing: bool,
    include_hidden: bool,
) -> Result<IndexMap<String, Job>, SuccessTuple> {
    filtered_jobs_with(
        rt,
        ctx,
        exec_keys,
        filter,
        warn_missing,
        include_hidden,
        true,
    )
}

fn filtered_jobs_with(
    rt: &mut Runtime<'_>,
    ctx: &JobsContext,
    exec_keys: Option<&str>,
    filter: &[String],
    warn_missing: bool,
    include_hidden: bool,
    combine_local_and_systemd: bool,
) -> Result<IndexMap<String, Job>, SuccessTuple> {
    let filtered = get_filtered_jobs_for(
        ctx,
        exec_keys,
        filter,
        include_hidden,
        combine_local_and_systemd,
    )
    .map_err(|e| {
        let keys = meerschaum_jobs::executor::normalize_executor_keys(exec_keys)
            .unwrap_or_else(|| meerschaum_jobs::job::get_executor_keys_from_context(ctx));
        SuccessTuple::fail(format!("Could not get the jobs of executor '{keys}': {e}"))
    })?;
    for w in &filtered.listing_warnings {
        warn(rt, w);
    }
    if warn_missing {
        for w in &filtered.missing {
            warn(rt, w);
        }
    }
    Ok(filtered.jobs)
}

fn with_status(jobs: &mut IndexMap<String, Job>, status: Status) -> Vec<String> {
    jobs.iter_mut()
        .filter_map(|(n, j)| (j.status() == status).then(|| n.clone()))
        .collect()
}

/// Jobs of the same executors as `jobs`, freshly opened by name (a `Job` is not `Clone`): for
/// printing a subset and for re-reading statuses after an action.
fn subset(
    ctx: &JobsContext,
    jobs: &IndexMap<String, Job>,
    names: &[String],
) -> IndexMap<String, Job> {
    names
        .iter()
        .filter_map(|n| {
            let keys = jobs.get(n).map(|j| j.executor_keys.clone())?;
            Job::new(
                n,
                None,
                JobOptions {
                    executor_keys: Some(keys),
                    ..Default::default()
                },
                ctx,
            )
            .ok()
            .map(|j| (n.clone(), j))
        })
        .collect()
}

#[cfg(test)]
mod warning_tests {
    use super::*;
    use meerschaum_core::value::Value;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Clone, Default)]
    struct Sink(Rc<RefCell<Vec<u8>>>);

    impl std::io::Write for Sink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.borrow_mut().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn the_warnings_of_a_started_job_are_printed_once_through_the_formatter() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = JobsContext::for_root(dir.path());
        let mut job = Job::new("w", None, Default::default(), &ctx).unwrap();
        job.warnings
            .push("No Python interpreter is pinned in the unit of job 'w'.".into());
        let (mut out, err) = (Vec::new(), Sink::default());
        {
            let mut rt = Runtime::new(
                crate::types::DateContext::system(),
                80,
                &mut out,
                |_: &str| None,
            );
            rt.err = Box::new(err.clone());
            emit_job_warnings(&mut rt, &mut job);
            emit_job_warnings(&mut rt, &mut job);
        }
        let text = String::from_utf8(err.0.borrow().clone()).unwrap();
        assert_eq!(
            text.matches("No Python interpreter is pinned").count(),
            1,
            "{text}"
        );
        assert!(text.contains("🔔"), "{text}");
        assert!(job.warnings.is_empty());
    }

    /// `restart jobs` (and `compose up`) move a non-default root's `local` job to systemd: the
    /// unit gets the job's command and settings, the local directory becomes the hidden sister.
    #[test]
    fn a_local_job_of_another_root_moves_to_systemd() {
        use meerschaum_jobs::executor::JobExecutor;
        let dir = tempfile::tempdir().unwrap();
        let fake = Arc::new(meerschaum_jobs::FakeSystemctl::new());
        let mut ctx = JobsContext::for_root(dir.path().join("project/root"))
            .with_process_runner(fake.clone())
            .with_launcher(meerschaum_jobs::Launcher {
                supervisor: vec!["unused".into()],
                program: vec!["/usr/bin/mrsm".into()],
            })
            .with_process_env(vec![("PATH".into(), "/usr/bin".into())]);
        ctx.paths.systemd_user_dir = dir.path().join("home/.config/systemd/user");
        // As an installed copy on a systemd host would choose it.
        ctx.config.executor = Some("systemd".into());
        assert!(moves_local_jobs_to_systemd(&ctx));
        let mut env = IndexMap::new();
        env.insert("MRSM__COMPOSE_PROJECT".to_string(), "weather".to_string());
        let mut local = Job::new(
            "weather sync",
            Some(vec!["sync".into(), "pipes".into(), "--loop".into()]),
            JobOptions {
                executor_keys: Some("local".into()),
                env: Some(env),
                ..Default::default()
            },
            &ctx,
        )
        .unwrap();
        assert!(local.daemon().setup(true).success);
        let (mut out, err) = (Vec::new(), Sink::default());
        let moved = {
            let mut rt = Runtime::new(
                crate::types::DateContext::system(),
                80,
                &mut out,
                |_: &str| None,
            );
            rt.err = Box::new(err.clone());
            move_job_to_systemd(&mut rt, &ctx, &mut local)
        };
        assert!(moved.success, "{}", moved.message);
        let warned = String::from_utf8(err.0.borrow().clone()).unwrap();
        assert!(
            warned.contains("Moved the local job 'weather sync' to systemd"),
            "{warned}"
        );
        let ex = meerschaum_jobs::systemd::SystemdExecutor::new(ctx.clone());
        assert_eq!(ex.get_job_names(), vec!["weather sync".to_string()]);
        // Named after the Compose project, with the same command and environment.
        assert_eq!(
            ex.get_service_name("weather sync"),
            "mrsm-weather.weather-sync.service"
        );
        assert_eq!(
            ex.get_job_sysargs("weather sync"),
            vec!["sync", "pipes", "--loop"]
        );
        let props = ex.get_job_properties("weather sync");
        assert_eq!(
            props
                .get("env")
                .and_then(|e| e.get("MRSM__COMPOSE_PROJECT"))
                .and_then(Value::as_str),
            Some("weather")
        );
        assert!(
            fake.rendered_calls()
                .iter()
                .any(|c| c.starts_with("start mrsm-weather."))
        );
        // The default root's local jobs were someone's choice: they stay.
        let mut default_ctx = ctx.clone();
        default_ctx.paths.root = dir.path().join("home/.config/meerschaum");
        assert!(!moves_local_jobs_to_systemd(&default_ctx));
    }
}
