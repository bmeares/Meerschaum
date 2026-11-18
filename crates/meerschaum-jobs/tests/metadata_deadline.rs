//! Real native lifetime proof independent of API sweeps. Signed Metadata context/source/kernel
//! admission is covered by the CLI executor fixture; this target exercises the detached native
//! supervisor after its initiating asynchronous operation is canceled while its owner lives.
#![cfg(target_os = "linux")]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use meerschaum_jobs::Seconds;
use meerschaum_jobs::process::{self, ProcState};
use meerschaum_jobs::supervisor::{
    ComputeDeadline, JobGeneration, LifecycleOwner, ResultProtocol, SupervisorOptions,
};
use uuid::Uuid;

type Birth = (i32, chrono::NaiveDateTime);
fn capture(pid: i32) -> Birth {
    (
        pid,
        process::process_start_time(pid).expect("The owned process birth is unavailable."),
    )
}
fn alive(identity: &Birth) -> bool {
    process::process_start_time(identity.0) == Some(identity.1)
        && !matches!(
            process::proc_state(identity.0),
            ProcState::Gone | ProcState::Zombie
        )
}

struct Owned {
    supervisor: Child,
    worker: Option<Birth>,
    generation: JobGeneration,
    properties: PathBuf,
}
impl Drop for Owned {
    fn drop(&mut self) {
        if let Some(identity) = &self.worker
            && alive(identity)
        {
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(identity.0),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
        let deadline = Instant::now() + Duration::from_secs(1);
        while self.supervisor.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = self.supervisor.kill();
        let _ = self.supervisor.wait();
    }
}
struct Bystander(Child);
impl Drop for Bystander {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn launch(root: &Path, expires_at: i64) -> Owned {
    let id = Uuid::new_v4();
    let name = format!("mrsm-{}", id.simple());
    let directory = root
        .join("execution-contexts")
        .join(id.simple().to_string())
        .join(format!("generation-{}", Uuid::new_v4().simple()));
    std::fs::create_dir_all(&directory).unwrap();
    let jobs_dir = root.join("jobs");
    let job = jobs_dir.join(&name);
    let logs = root.join("logs");
    std::fs::create_dir_all(&job).unwrap();
    std::fs::create_dir_all(&logs).unwrap();
    let properties = job.join("properties.json");
    std::fs::write(
        &properties,
        r#"{"process":{"launch_id":"deadline-fixture"}}"#,
    )
    .unwrap();
    let generation = JobGeneration {
        context: id.simple().to_string(),
        revision: 1,
        owner: "metadata_operator".into(),
        owner_id: Some("registered-metadata-operator".into()),
        identity: "a".repeat(64),
        directory: directory.clone(),
    };
    generation.pending().unwrap();
    let options = SupervisorOptions {
        compute_deadline: Some(ComputeDeadline { expires_at }),
        daemon_id: name.clone(),
        jobs_dir,
        log_path: logs.join(format!("{name}.log")),
        error_log_path: root.join("daemon_errors.log"),
        log_offset_path: logs.join(format!(".{name}.log.offset")),
        keep_output: true,
        write_timestamps: false,
        timestamp_format: "%H:%M".into(),
        num_files_to_keep: 2,
        max_file_size: 100_000,
        refresh_files_seconds: Seconds::Int(1),
        capture_stdin: false,
        cwd: directory.clone(),
        command: vec![
            "/bin/sh".into(),
            "-c".into(),
            "trap '' TERM INT; trap 'printf resumed > resumed.marker' CONT; printf '%s' \"$$\" > worker.pid; while :; do :; done".into(),
        ],
        startup: None,
        launch_id: Some("deadline-fixture".into()),
        lifecycle_owner: Some(LifecycleOwner::current().unwrap()),
        job_generation: Some(generation.clone()),
        result_protocol: ResultProtocol::Metadata,
    };
    use std::os::unix::process::CommandExt;
    let supervisor = Command::new(env!("CARGO_BIN_EXE_mrsm-job-supervisor"))
        .args(options.to_args())
        .current_dir(&directory)
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()
        .unwrap();
    Owned {
        supervisor,
        worker: None,
        generation,
        properties,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canceled_operation_cannot_extend_running_or_paused_native_metadata_compute() {
    let root = tempfile::tempdir().unwrap();
    let owner = LifecycleOwner::current().unwrap();
    let mut bystander = Bystander(
        Command::new("/bin/sleep")
            .arg("60")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let bystander_identity = capture(bystander.0.id() as i32);
    let expires_at = chrono::Utc::now().timestamp() + 8;
    let (send, receive) = tokio::sync::oneshot::channel();
    let path = root.path().to_path_buf();
    let driver = tokio::spawn(async move {
        let children = vec![launch(&path, expires_at), launch(&path, expires_at)];
        let _ = send.send(children);
        std::future::pending::<()>().await;
    });
    let mut children = receive.await.unwrap();
    let ready = Instant::now() + Duration::from_secs(3);
    for child in &mut children {
        let marker = child.generation.directory.join("worker.pid");
        let pid = loop {
            if let Ok(text) = std::fs::read_to_string(&marker)
                && let Ok(pid) = text.parse::<i32>()
            {
                break pid;
            }
            assert!(
                Instant::now() < ready,
                "The owned CPU-only worker did not start."
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        child.worker = Some(capture(pid));
    }
    let paused = children[1].worker.as_ref().unwrap().0;
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(paused),
        nix::sys::signal::Signal::SIGSTOP,
    )
    .unwrap();
    let until = Instant::now() + Duration::from_secs(1);
    while process::proc_state(paused) != ProcState::Stopped {
        assert!(Instant::now() < until, "The owned worker never paused.");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    driver.abort();
    assert!(driver.await.unwrap_err().is_cancelled());
    // No API maintenance thread participates. The owner lives; only the pinned native deadline
    // can stop this deliberately non-I/O loop after the initiating operation disappears.
    assert!(owner.alive());
    let until = Instant::now() + Duration::from_secs(12);
    loop {
        let ended = children.iter_mut().all(|child| {
            child.supervisor.try_wait().unwrap().is_some() && !alive(child.worker.as_ref().unwrap())
        });
        if ended {
            break;
        }
        assert!(
            Instant::now() < until,
            "Native Metadata compute outlived its deadline."
        );
        assert!(owner.alive() && alive(&bystander_identity));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(owner.alive());
    assert!(
        !children[1]
            .generation
            .directory
            .join("resumed.marker")
            .exists(),
        "Deadline retirement resumed a paused worker before killing it."
    );
    assert!(bystander.0.try_wait().unwrap().is_none());
    assert!(alive(&bystander_identity));
    for child in &children {
        assert!(
            child.generation.inactive().unwrap(),
            "The native owned-tree cleanup proof is missing."
        );
        let properties: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&child.properties).unwrap()).unwrap();
        assert_eq!(properties["result"][0], false, "{properties}");
        assert!(
            properties["result"][1]
                .as_str()
                .unwrap()
                .contains("immutable compute deadline"),
            "{properties}"
        );
        assert!(properties["process"]["ended"].is_string());
    }
}
