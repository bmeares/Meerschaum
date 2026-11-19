//! Actual native result-file completion, independent of mutable job properties.
//! Kernel/source/SDK admission remains covered by the CLI Metadata fixture; this target
//! checks only the supervisor's receipt origin and exact relaunch invalidation.
#![cfg(target_os = "linux")]

use meerschaum_jobs::Seconds;
use meerschaum_jobs::supervisor::{
    ComputeDeadline, JobGeneration, LifecycleOwner, ResultProtocol, SupervisorOptions,
};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use uuid::Uuid;

struct Owned(Child);
impl Drop for Owned {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn completed(root: &Path, command: &str) -> (JobGeneration, i64, PathBuf) {
    let id = Uuid::new_v4();
    let name = format!("mrsm-{}", id.simple());
    let directory = root
        .join("execution-contexts")
        .join(id.simple().to_string())
        .join(format!("generation-{}", Uuid::new_v4().simple()));
    let job = root.join("jobs").join(&name);
    let logs = root.join("logs");
    for path in [&directory, &job, &logs] {
        std::fs::create_dir_all(path).unwrap();
    }
    let properties = job.join("properties.json");
    let launch = Uuid::new_v4().simple().to_string();
    std::fs::write(
        &properties,
        serde_json::json!({"process":{"launch_id":launch}}).to_string(),
    )
    .unwrap();
    let generation = JobGeneration {
        context: id.simple().to_string(),
        revision: 1,
        owner: "metadata_receipt_operator".into(),
        owner_id: Some("receipt-operator-id".into()),
        identity: "a".repeat(64),
        directory: directory.clone(),
    };
    generation.pending().unwrap();
    let deadline = chrono::Utc::now().timestamp() + 30;
    let options = SupervisorOptions {
        compute_deadline: Some(ComputeDeadline {
            expires_at: deadline,
        }),
        daemon_id: name.clone(),
        jobs_dir: root.join("jobs"),
        log_path: logs.join(format!("{name}.log")),
        error_log_path: root.join("daemon-errors.log"),
        log_offset_path: logs.join(format!(".{name}.log.offset")),
        keep_output: true,
        write_timestamps: false,
        timestamp_format: "%H:%M".into(),
        num_files_to_keep: 2,
        max_file_size: 100_000,
        refresh_files_seconds: Seconds::Int(1),
        capture_stdin: false,
        cwd: directory,
        command: vec!["/bin/sh".into(), "-c".into(), command.into()],
        startup: None,
        launch_id: Some(launch),
        lifecycle_owner: Some(LifecycleOwner::current().unwrap()),
        job_generation: Some(generation.clone()),
        result_protocol: ResultProtocol::Metadata,
    };
    use std::os::unix::process::CommandExt;
    let mut child = Owned(
        Command::new(env!("CARGO_BIN_EXE_mrsm-job-supervisor"))
            .args(options.to_args())
            .current_dir(root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .process_group(0)
            .spawn()
            .unwrap(),
    );
    let until = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success(), "The owned supervisor failed: {status}");
            break;
        }
        assert!(
            Instant::now() < until,
            "The owned short Metadata command did not finish."
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(generation.inactive().unwrap());
    (generation, deadline, properties)
}

#[test]
fn actual_native_receipt_keeps_result_digest_when_properties_change_and_pending_invalidates_it() {
    let root = tempfile::tempdir().unwrap();
    let (generation, deadline, properties) = completed(
        root.path(),
        "printf '%s' '[true,\"actual-native-result\"]' > \"$MRSM_JOB_RESULT_PATH\"",
    );
    let receipt = generation.metadata_result(deadline).unwrap();
    assert_eq!(receipt.generation(), &generation);
    assert_eq!(receipt.deadline(), deadline);
    assert_eq!(receipt.message_bytes(), b"actual-native-result".len());
    use sha2::{Digest, Sha256};
    let expected = Sha256::digest(b"actual-native-result")
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(receipt.message_sha256(), expected);
    assert!(!receipt.launch_id().is_empty());
    assert!(receipt.completed_at() <= deadline);
    let digest = receipt.receipt_sha256().to_string();
    // The ordinary result is intentionally mutable and never becomes the native proof.
    std::fs::write(
        &properties,
        r#"{"result":[true,"forged-properties-result"]}"#,
    )
    .unwrap();
    assert_eq!(
        generation
            .metadata_result(deadline)
            .unwrap()
            .receipt_sha256(),
        digest
    );
    assert!(generation.metadata_result(deadline - 1).is_err());
    generation.pending().unwrap();
    assert!(generation.metadata_result(deadline).is_err());
}

#[test]
fn missing_result_and_nonzero_exit_cannot_create_successful_metadata_provenance() {
    let root = tempfile::tempdir().unwrap();
    let (missing, deadline, properties) = completed(root.path(), "exit 0");
    assert!(missing.metadata_result(deadline).is_err());
    let properties: serde_json::Value =
        serde_json::from_slice(&std::fs::read(properties).unwrap()).unwrap();
    assert_eq!(
        properties["result"][0], false,
        "Exit status fallback must not become Metadata provenance."
    );
    let (nonzero, deadline, _) = completed(
        root.path(),
        "printf '%s' '[true,\"looks-successful\"]' > \"$MRSM_JOB_RESULT_PATH\"; exit 3",
    );
    assert!(nonzero.metadata_result(deadline).is_err());
}
