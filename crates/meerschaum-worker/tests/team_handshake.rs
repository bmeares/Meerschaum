//! An isolated team's worker must prove, in its handshake, that it loaded the team's
//! configuration before it receives any call (5.1). A worker from an older `meerschaum` package
//! (no capability), one that loaded another team's scope or one whose configuration defines
//! connectors outside the team's set is stopped, the call fails with the cause and no team code
//! runs. The worker here is a stand-in written for the test: it speaks the protocol with a tiny
//! MessagePack encoder of its own, so the result never depends on which `meerschaum` package is
//! installed.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use meerschaum_worker::protocol::{Msg, PROTOCOL_VERSION};
use meerschaum_worker::worker::TeamExpectation;
use meerschaum_worker::{CallOptions, PoolConfig, Worker, WorkerError, WorkerOptions, WorkerPool};

/// A stand-in worker: says `hello` as `TPR_FAKE_MODE` asks, and records any call it receives
/// (team code would run there) in `TPR_FAKE_RAN`.
const FAKE_WORKER: &str = r#"
import os, socket, struct, sys

def pack(v):
    if v is None:
        return b'\xc0'
    if isinstance(v, bool):
        return b'\xc3' if v else b'\xc2'
    if isinstance(v, int):
        return b'\xcf' + struct.pack('>Q', v)
    if isinstance(v, str):
        b = v.encode()
        return b'\xdb' + struct.pack('>I', len(b)) + b
    if isinstance(v, list):
        return b'\xdd' + struct.pack('>I', len(v)) + b''.join(pack(i) for i in v)
    if isinstance(v, dict):
        return b'\xdf' + struct.pack('>I', len(v)) + b''.join(pack(k) + pack(x) for k, x in v.items())
    raise TypeError(v)

def send(f, msg):
    msg['frames'] = 0
    payload = pack(msg)
    f.write(struct.pack('>IB', len(payload), 1) + payload)
    f.flush()

address = sys.argv[sys.argv.index('--socket') + 1]
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.connect(address)
f = s.makefile('rwb')
mode = os.environ['TPR_FAKE_MODE']
hello = {
    't': 'hello', 'protocol': int(os.environ['TPR_PROTOCOL']), 'pid': os.getpid(),
    'python': '3', 'meerschaum': '5.0.2' if mode == 'old' else '5.1.0', 'msgpack': 'fake',
}
if mode != 'old':
    hello['capabilities'] = ['team_scope']
    connectors = ['sql:wetlands']
    team = 'wetlands'
    fingerprint = os.environ.get('MRSM_TEAM_FINGERPRINT', '')
    if mode == 'wrong_team':
        team = 'drylands'
    if mode == 'wrong_fingerprint':
        fingerprint = 'another'
    if mode == 'outside':
        connectors = ['sql:main', 'sql:wetlands']
    if mode != 'no_report':
        hello['team_scope'] = {
            'team': team, 'fingerprint': fingerprint, 'config_file': '',
            'connectors': connectors,
        }
send(f, hello)
header = f.read(5)
if header:
    open(os.environ['TPR_FAKE_RAN'], 'w').write('a call reached the worker')
"#;

struct Fake {
    _dir: tempfile::TempDir,
    dir: PathBuf,
}

impl Fake {
    fn new() -> Option<Fake> {
        if cfg!(not(unix)) {
            return None;
        }
        let ok = Command::new("python3")
            .args(["-c", "import socket"])
            .output()
            .is_ok_and(|o| o.status.success());
        if !ok {
            eprintln!("Skipping: no `python3`.");
            return None;
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        std::fs::write(path.join("tpr_fake_worker.py"), FAKE_WORKER).unwrap();
        Some(Fake {
            _dir: dir,
            dir: path,
        })
    }

    fn ran(&self) -> PathBuf {
        self.dir.join("ran")
    }

    fn options(&self, mode: &str, team: Option<TeamExpectation>) -> WorkerOptions {
        WorkerOptions {
            python: PathBuf::from("python3"),
            module: "tpr_fake_worker".into(),
            env_clear: true,
            env: vec![
                ("PATH".into(), std::env::var("PATH").unwrap_or_default()),
                ("PYTHONPATH".into(), self.dir.display().to_string()),
                ("TPR_FAKE_MODE".into(), mode.into()),
                ("TPR_PROTOCOL".into(), PROTOCOL_VERSION.to_string()),
                ("TPR_FAKE_RAN".into(), self.ran().display().to_string()),
                ("MRSM_TEAM_FINGERPRINT".into(), "fp-1".into()),
            ],
            startup_timeout: Duration::from_secs(30),
            quiet: true,
            team,
            ..WorkerOptions::default()
        }
    }
}

fn wetlands() -> TeamExpectation {
    TeamExpectation {
        team: "wetlands".into(),
        fingerprint: "fp-1".into(),
        connectors: ["sql:wetlands".to_string()].into_iter().collect(),
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn assert_nothing_ran(ran: &Path) {
    std::thread::sleep(Duration::from_millis(200));
    assert!(!ran.exists(), "team code ran in a refused worker");
}

#[test]
fn a_worker_that_cannot_prove_the_teams_configuration_is_stopped() {
    let Some(fake) = Fake::new() else { return };
    let rt = runtime();
    for (mode, cause) in [
        ("old", "predates team-scoped workers"),
        ("no_report", "did not report the team's configuration"),
        ("wrong_team", "loaded the scope of team 'drylands'"),
        (
            "wrong_fingerprint",
            "another version of the team's configuration",
        ),
        ("outside", "connectors the team was not given (sql:main)"),
    ] {
        let opts = fake.options(mode, Some(wetlands()));
        match rt.block_on(Worker::spawn(&opts)) {
            Err(WorkerError::Refused(message)) => {
                assert!(message.contains(cause), "{mode}: {message}");
                assert!(message.contains("'wetlands'"), "{mode}: {message}");
                if mode == "old" {
                    assert!(message.contains("5.0.2"), "{message}");
                    assert!(message.contains("MRSM_PYTHON"), "{message}");
                }
            }
            Err(other) => panic!("{mode}: unexpected error {other}"),
            Ok(_) => panic!("{mode}: the worker was accepted"),
        }
        assert_nothing_ran(&fake.ran());

        // Through a pool: the call fails with the same cause and never reaches a worker.
        let pool = WorkerPool::new(PoolConfig {
            size: 1,
            worker: opts,
            ..PoolConfig::default()
        });
        let result =
            rt.block_on(pool.call("ping", Msg::new(), Vec::new(), CallOptions::default(), None));
        match result {
            Err(WorkerError::Refused(message)) => {
                assert!(message.contains(cause), "{mode}: {message}")
            }
            other => panic!("{mode}: {:?}", other.map(|_| ())),
        }
        assert_nothing_ran(&fake.ran());
        rt.block_on(pool.shutdown());
    }
}

#[test]
fn a_worker_that_proves_the_teams_configuration_serves_and_the_shared_pool_is_unchanged() {
    let Some(fake) = Fake::new() else { return };
    let rt = runtime();
    // The proof matches: accepted.
    let worker = rt
        .block_on(Worker::spawn(&fake.options("ok", Some(wetlands()))))
        .expect("accepted");
    assert_eq!(worker.hello().team_scope.as_ref().unwrap().team, "wetlands");
    drop(worker);
    // Without an expectation (the shared pool), an older package is tolerated as before.
    let worker = rt
        .block_on(Worker::spawn(&fake.options("old", None)))
        .expect("the shared pool accepts an older package");
    assert_eq!(worker.hello().meerschaum, "5.0.2");
    assert!(worker.hello().capabilities.is_empty());
}
