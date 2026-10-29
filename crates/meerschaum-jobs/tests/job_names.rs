//! A job name is one ordinary path component: `..`, `a/b` and friends never reach a recursive
//! removal (`DELETE /jobs/%2E%2E` once resolved to the whole root directory).
#![cfg(unix)]

use meerschaum_jobs::daemon::Daemon;
use meerschaum_jobs::executor::JobExecutor;
use meerschaum_jobs::job::{Job, JobOptions};
use meerschaum_jobs::systemd::SystemdExecutor;
use meerschaum_jobs::{FakeSystemctl, JobsContext};

const BAD: &[&str] = &[
    "..", ".", "", "a/b", "../x", "x/..", "..\\x", "a\0b", "/etc",
];

fn context() -> (tempfile::TempDir, JobsContext) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    std::fs::create_dir_all(root.join("jobs")).unwrap();
    std::fs::write(root.join("sentinel"), "keep").unwrap();
    let mut ctx = JobsContext::for_root(&root);
    ctx.paths.systemd_user_dir = dir.path().join("home/.config/systemd/user");
    (dir, ctx)
}

#[test]
fn path_like_names_are_rejected() {
    let (_dir, ctx) = context();
    for name in BAD {
        let err = Job::new(name, None, JobOptions::default(), &ctx).unwrap_err();
        assert!(
            err.to_string().starts_with("Invalid name:"),
            "{name:?}: {err}"
        );
    }
    // Ordinary names (spaces, dashes, dots inside) still work.
    for name in ["my job", "a-b", "v1.2", "..x", "x.."] {
        Job::new(name, None, JobOptions::default(), &ctx).unwrap();
    }
}

#[test]
fn cleanup_refuses_to_leave_the_jobs_directory() {
    let (_dir, ctx) = context();
    for name in BAD {
        let mut daemon = Daemon::new(name, vec![], None, Default::default(), &ctx);
        let result = daemon.cleanup(false);
        assert!(!result.success, "{name:?}: {result:?}");
    }
    assert!(ctx.paths.root.join("sentinel").exists());
    assert!(ctx.paths.jobs_dir.exists());
    // A real job directory is still removed.
    let mut daemon = Daemon::new("real", vec![], None, Default::default(), &ctx);
    std::fs::create_dir_all(ctx.paths.jobs_dir.join("real")).unwrap();
    assert!(daemon.cleanup(false).success);
    assert!(!ctx.paths.jobs_dir.join("real").exists());
}

#[test]
fn the_systemd_executor_refuses_them_too() {
    let (_dir, ctx) = context();
    let fake = std::sync::Arc::new(FakeSystemctl::default());
    let ctx = ctx.with_process_runner(fake.clone());
    let sysd = ctx.paths.systemd_jobs_dir.clone();
    std::fs::create_dir_all(&sysd).unwrap();
    std::fs::write(sysd.parent().unwrap().join("sentinel"), "keep").unwrap();
    let ex = SystemdExecutor::new(ctx);
    for name in BAD {
        assert!(!ex.delete_job(name).success, "{name:?}");
        assert!(
            !ex.create_job(name, &["show".into(), "version".into()], None)
                .success,
            "{name:?}"
        );
    }
    assert!(sysd.parent().unwrap().join("sentinel").exists());
    assert!(
        fake.rendered_calls().is_empty(),
        "{:?}",
        fake.rendered_calls()
    );
}

/// A supervisor of the same name under *another* root is not this root's job: it must neither
/// make a new job "already running" nor be killed by `stop`.
#[test]
fn a_same_named_supervisor_of_another_root_is_not_found() {
    let (dir, ctx) = context();
    let other_jobs = dir.path().join("other-root/jobs");
    std::fs::create_dir_all(&other_jobs).unwrap();
    let mut child = std::process::Command::new("sh")
        .args([
            "-c",
            "sleep 60; true",
            "supervisor",
            "daemon_id='dupe'",
            "--jobs-dir",
        ])
        .arg(&other_jobs)
        .spawn()
        .unwrap();
    // The command line is visible once the shell has exec'ed (a moment).
    // A job is only searched for once it has recorded that it began (and not that it ended).
    let began = |ctx: &JobsContext| {
        std::fs::create_dir_all(ctx.paths.jobs_dir.join("dupe")).unwrap();
        let mut d = Daemon::new("dupe", vec![], None, Default::default(), ctx);
        assert!(d.capture_process_timestamp("began", true).success);
    };
    let seen = |ctx: &JobsContext| Daemon::new("dupe", vec![], None, Default::default(), ctx).pid();
    let mut other_ctx = JobsContext::for_root(dir.path().join("other-root"));
    other_ctx.paths.jobs_dir = other_jobs;
    began(&other_ctx);
    began(&ctx);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while seen(&other_ctx).is_none() {
        assert!(
            std::time::Instant::now() < deadline,
            "the process was not found"
        );
        // (a negative result is remembered; the process may not have been visible yet)
        meerschaum_jobs::detached::invalidate("dupe", &other_ctx.paths.jobs_dir);
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    // The marker is on the supervisor's command line; the job is its worker (the `sleep`).
    let worker = meerschaum_jobs::process::children(child.id() as i32).unwrap()[0];
    assert_eq!(seen(&other_ctx), Some(worker));
    assert_eq!(seen(&ctx), None);
    let daemon = Daemon::new("dupe", vec![], None, Default::default(), &ctx);
    assert_eq!(daemon.kill_detached_processes(None), 0);
    assert!(
        child.try_wait().unwrap().is_none(),
        "the other root's process was killed"
    );
    let _ = child.kill();
    let _ = child.wait();
}
