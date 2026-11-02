//! `get_job_summaries` and the `systemd` snapshot behind it: every job's state from one batched
//! `systemctl show`, equal to what each job's getters read one query at a time. Nothing here
//! runs `systemctl`: the context's runner is [`FakeSystemctl`].

use std::sync::Arc;

use meerschaum_jobs::config::Seconds;
use meerschaum_jobs::executor::get_jobs_for;
use meerschaum_jobs::runner::FakeService;
use meerschaum_jobs::summary::{JobSummary, listing_context};
use meerschaum_jobs::systemd::{SystemdExecutor, parse_unit_states, snapshot_context};
use meerschaum_jobs::{
    FakeSystemctl, Job, JobExecutor, JobOptions, JobsContext, Launcher, get_job_summaries,
};

struct Fixture {
    _dir: tempfile::TempDir,
    fake: Arc<FakeSystemctl>,
    ctx: JobsContext,
}

impl Fixture {
    fn new() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().canonicalize().unwrap();
        let fake = Arc::new(FakeSystemctl::new());
        let mut ctx = JobsContext::for_root(tmp.join("root"));
        ctx.paths.systemd_user_dir = tmp.join("userhome/.config/systemd/user");
        ctx.config.timeout_seconds = Seconds::Float(0.3);
        ctx.config.check_timeout_interval_seconds = Seconds::Float(0.05);
        ctx.config.executor = Some("systemd".into());
        let ctx = ctx
            .with_launcher(Launcher {
                supervisor: vec!["unused".into()],
                program: vec!["/usr/bin/python3".into()],
            })
            .with_process_runner(fake.clone())
            .with_process_env(vec![
                ("PATH".to_string(), "/usr/bin:/bin".to_string()),
                (
                    "MRSM_ROOT_DIR".to_string(),
                    tmp.join("root").display().to_string(),
                ),
            ]);
        Fixture {
            _dir: dir,
            fake,
            ctx,
        }
    }

    fn executor(&self) -> SystemdExecutor {
        SystemdExecutor::new(self.ctx.clone())
    }

    fn create(&self, name: &str, sysargs: &[&str]) -> String {
        let sysargs: Vec<String> = sysargs.iter().map(|s| s.to_string()).collect();
        let created = self.executor().create_job(name, &sysargs, None);
        assert!(created.success, "{}", created.message);
        self.executor().get_service_name(name)
    }

    /// The `systemctl` commands which read a unit's state.
    fn state_queries(&self) -> Vec<String> {
        self.fake
            .rendered_calls()
            .into_iter()
            .filter(|c| c.starts_with("is-active") || c.starts_with("show"))
            .collect()
    }
}

fn service(state: &str, pid: i32, stopped: bool) -> FakeService {
    FakeService {
        active_state: state.into(),
        main_pid: pid,
        stopped,
        enabled: true,
        active_enter: if pid != 0 {
            "Tue 2000-01-04 10:00:00 UTC".into()
        } else {
            String::new()
        },
        inactive_enter: if state == "inactive" {
            "Tue 2000-01-04 11:30:00 UTC".into()
        } else {
            String::new()
        },
        ..FakeService::default()
    }
}

/// Jobs in every state: running, paused, stopped (with a result), starting, a unit whose main
/// process is gone, and a stopped local job.
fn mixed(f: &Fixture) {
    let units = [
        ("run one", service("active", 4_100_001, false)),
        ("paused one", service("active", 4_100_002, true)),
        ("stopped one", service("inactive", 0, false)),
        ("starting one", service("activating", 0, false)),
        ("gone one", {
            let mut s = service("active", 4_100_003, false);
            s.process_gone = true;
            s
        }),
    ];
    for (name, state) in units {
        let unit = f.create(name, &["sync", "pipes", "-c", "sql:x", "--loop"]);
        f.fake.set_service(&unit, state);
    }
    std::fs::write(
        f.executor().get_result_path("stopped one"),
        r#"[false, "Failed to sync."]"#,
    )
    .unwrap();
    let mut local = Job::new(
        "local one",
        Some(vec!["show".into(), "version".into()]),
        JobOptions {
            executor_keys: Some("local".into()),
            ..Default::default()
        },
        &f.ctx,
    )
    .unwrap();
    assert!(local.daemon().setup(true).success);
    f.fake.clear_calls();
}

#[test]
fn summaries_read_every_systemd_job_with_one_systemctl_call() {
    let f = Fixture::new();
    mixed(&f);
    let summaries = get_job_summaries(&f.ctx, false).unwrap();
    assert_eq!(
        f.state_queries().len(),
        1,
        "one batched `show`: {:?}",
        f.fake.rendered_calls()
    );
    assert!(f.state_queries()[0].starts_with("show --property=Id,ActiveState,MainPID"));

    let status = |name: &str| summaries[name].status.as_str();
    assert_eq!(status("run one"), "running");
    assert_eq!(status("paused one"), "paused");
    assert_eq!(status("stopped one"), "stopped");
    assert_eq!(status("starting one"), "running");
    assert_eq!(status("gone one"), "stopped");
    assert_eq!(status("local one"), "stopped");
    assert_eq!(summaries["run one"].pid, Some(4_100_001));
    assert_eq!(
        summaries["stopped one"]
            .ended
            .as_ref()
            .map(|d| d.isoformat()),
        Some("2000-01-04T11:30:00".to_string())
    );
    assert_eq!(
        summaries["run one"].began.as_ref().map(|d| d.isoformat()),
        Some("2000-01-04T10:00:00".to_string())
    );
    assert!(!summaries["stopped one"].result.success);
    assert_eq!(summaries["stopped one"].result.message, "Failed to sync.");
    assert!(summaries["run one"].restart);
    assert_eq!(summaries["local one"].executor_keys, "local");
    assert_eq!(
        summaries["run one"].sysargs,
        vec!["sync", "pipes", "-c", "sql:x", "--loop"]
    );
    let json = summaries["run one"].to_json();
    assert_eq!(json["status"], "running");
    assert_eq!(json["health"]["state"], "unknown");
}

#[test]
fn summaries_equal_what_each_jobs_getters_read() {
    let f = Fixture::new();
    mixed(&f);
    let summaries = get_job_summaries(&f.ctx, false).unwrap();
    let live: Vec<JobSummary> = get_jobs_for(&f.ctx, Some("local"), false, true)
        .unwrap()
        .jobs
        .into_values()
        .map(|mut job| JobSummary::read(&mut job))
        .collect();
    assert_eq!(live.len(), 6);
    for summary in live {
        assert_eq!(summaries[&summary.name], summary, "{}", summary.name);
    }
    // The getters one at a time ask `systemctl` again and again.
    assert!(f.state_queries().len() > 20, "{:?}", f.state_queries());
}

#[test]
fn the_snapshot_answers_only_state_queries_of_known_units() {
    let f = Fixture::new();
    mixed(&f);
    let ctx = snapshot_context(&f.ctx);
    f.fake.clear_calls();
    let ex = SystemdExecutor::new(ctx.clone());
    assert_eq!(ex.get_job_status("run one"), "running");
    assert_eq!(ex.get_job_pid("paused one"), Some(4_100_002));
    assert!(f.fake.rendered_calls().is_empty());
    // Anything else goes to `systemctl`.
    assert!(ex.start_job("stopped one").success);
    assert_eq!(f.fake.rendered_calls().len(), 1);
    assert_eq!(ex.get_job_status("not a job"), "stopped");
    assert_eq!(f.fake.rendered_calls().len(), 2);
}

#[test]
fn a_root_without_systemd_jobs_asks_systemctl_nothing() {
    let f = Fixture::new();
    let summaries = get_job_summaries(&f.ctx, false).unwrap();
    assert!(summaries.is_empty());
    assert!(f.fake.rendered_calls().is_empty());
    // A local context lists its local jobs only, with no snapshot.
    let mut ctx = f.ctx.clone();
    ctx.config.executor = Some("local".into());
    mixed(&f);
    let local = listing_context(&ctx, Some("local"), true);
    let listing = get_jobs_for(&local, Some("local"), false, true).unwrap();
    assert_eq!(listing.jobs.len(), 1);
    assert!(f.fake.rendered_calls().is_empty());
}

#[test]
fn a_listing_reads_systemd_commands_from_the_unit_files() {
    let f = Fixture::new();
    mixed(&f);
    let listing = get_jobs_for(&f.ctx, Some("local"), false, true).unwrap();
    let mut sysargs = Vec::new();
    for (_, mut job) in listing.jobs {
        sysargs.push(job.sysargs());
    }
    assert_eq!(sysargs.len(), 6);
    assert!(
        f.fake.rendered_calls().is_empty(),
        "{:?}",
        f.fake.rendered_calls()
    );
}

#[test]
fn batched_show_output_is_read_by_unit() {
    let requested = vec!["mrsm-a.service".to_string(), "mrsm-b.service".to_string()];
    let text = "Id=mrsm-a.service\nActiveState=active\nSubState=running\n\
        ActiveEnterTimestamp=Mon 2026-11-02 12:51:57 UTC\nInactiveEnterTimestamp=\nMainPID=42\n\n\
        Id=mrsm-b.service\nActiveState=inactive\nMainPID=0\nActiveEnterTimestamp=\n\
        InactiveEnterTimestamp=Mon 2026-11-02 12:52:08 UTC\n";
    let states = parse_unit_states(text, &requested);
    assert_eq!(states["mrsm-a.service"].active_state, "active");
    assert_eq!(states["mrsm-a.service"].main_pid, "42");
    assert_eq!(
        states["mrsm-b.service"].inactive_enter,
        "Mon 2026-11-02 12:52:08 UTC"
    );
    // Blocks without an `Id` are matched by position, when every unit has one.
    let states = parse_unit_states("ActiveState=active\n\nActiveState=failed\n", &requested);
    assert_eq!(states["mrsm-b.service"].active_state, "failed");
    assert!(parse_unit_states("ActiveState=active\n", &requested).is_empty());
}
