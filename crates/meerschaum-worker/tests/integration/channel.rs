//! 5.1: the channel between the Rust host and a Python worker trusts only the worker. Another
//! process of the same operating-system user (an isolated team's sandboxed code, on a kernel
//! whose Landlock does not restrict connecting to pathname Unix sockets) that reaches the channel
//! first must neither be taken for the worker (it would receive the calls meant for it: plugin
//! connectors' attributes, other teams' data) nor keep the worker out. The stand-in worker here
//! waits until the "intruder" has tried, so the race is decided in the intruder's favor whenever
//! the channel can be reached at all.

use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use meerschaum_worker::protocol::PROTOCOL_VERSION;
use meerschaum_worker::{Worker, WorkerOptions};

/// A minimal MessagePack writer and the protocol's framing, shared by the worker and the
/// intruder.
const SPEAK: &str = r#"
import os, socket, struct, sys, time

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

def hello(f, pid):
    msg = {'t': 'hello', 'protocol': int(os.environ['TPR_PROTOCOL']), 'pid': pid,
           'python': '3', 'meerschaum': '5.1.0', 'msgpack': 'fake', 'frames': 0}
    payload = pack(msg)
    f.write(struct.pack('>IB', len(payload), 1) + payload)
    f.flush()
"#;

/// The worker: waits for the intruder's verdict, then connects (or opens its inherited end) and
/// says `hello` with its own PID.
const WORKER: &str = r#"
from tpr_speak import *
deadline = time.time() + 20
while not os.path.exists(os.environ['TPR_GO']) and time.time() < deadline:
    time.sleep(0.02)
address = sys.argv[sys.argv.index('--socket') + 1]
if address.startswith('fd:'):
    s = socket.socket(fileno=int(address[3:]))
else:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(address)
f = s.makefile('rwb')
hello(f, os.getpid())
f.read(1)
"#;

/// The intruder: finds the worker's process (`TPR_MARK` in its command line), reads the address
/// it was given and, if that is a path, connects and says `hello` as PID 4242. Prints what it did.
const INTRUDER: &str = r#"
from tpr_speak import *
mark = os.environ['TPR_MARK']
deadline = time.time() + 20
address = None
while address is None and time.time() < deadline:
    for pid in os.listdir('/proc'):
        if not pid.isdigit():
            continue
        try:
            argv = open(f'/proc/{pid}/cmdline', 'rb').read().split(b'\0')
        except OSError:
            continue
        if b'--socket' in argv and any(mark.encode() in a for a in argv):
            address = argv[argv.index(b'--socket') + 1].decode()
            break
    time.sleep(0.02)
verdict = 'no worker found'
if address is not None and address.startswith('fd:'):
    verdict = 'nothing to connect to: ' + address
elif address is not None:
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(address)
        f = s.makefile('rwb')
        hello(f, 4242)
        verdict = 'connected'
    except OSError as e:
        verdict = 'refused: ' + str(e)
open(os.environ['TPR_GO'], 'w').write(verdict)
print(verdict, flush=True)
time.sleep(3)
"#;

#[test]
fn another_process_cannot_take_the_workers_place() {
    let ok = Command::new("python3")
        .args(["-c", "import socket"])
        .output()
        .is_ok_and(|o| o.status.success());
    if !ok {
        eprintln!("Skipping: no `python3`.");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("tpr_speak.py"), SPEAK).unwrap();
    let mark = format!("tpr_chan_worker_{}", std::process::id());
    std::fs::write(dir.path().join(format!("{mark}.py")), WORKER).unwrap();
    std::fs::write(dir.path().join("tpr_intruder.py"), INTRUDER).unwrap();
    let go = dir.path().join("go");
    let env = |cmd: &mut Command| {
        cmd.env("PYTHONPATH", dir.path())
            .env("TPR_PROTOCOL", PROTOCOL_VERSION.to_string())
            .env("TPR_GO", &go)
            .env("TPR_MARK", &mark);
    };
    let mut intruder = Command::new("python3");
    intruder.arg("-m").arg("tpr_intruder");
    env(&mut intruder);
    let intruder = intruder
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let opts = WorkerOptions {
        python: PathBuf::from("python3"),
        module: mark.clone(),
        env_clear: true,
        env: vec![
            ("PATH".into(), std::env::var("PATH").unwrap_or_default()),
            ("PYTHONPATH".into(), dir.path().display().to_string()),
            ("TPR_PROTOCOL".into(), PROTOCOL_VERSION.to_string()),
            ("TPR_GO".into(), go.display().to_string()),
        ],
        startup_timeout: Duration::from_secs(30),
        quiet: true,
        ..WorkerOptions::default()
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let started = Instant::now();
    let worker = runtime.block_on(Worker::spawn(&opts));
    let out = intruder.wait_with_output().unwrap();
    let verdict = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let worker = worker.unwrap_or_else(|e| panic!("{e} (intruder: {verdict})"));
    // The host's peer is the process it started, whatever the intruder did.
    assert_ne!(
        worker.pid(),
        4242,
        "the intruder was taken for the worker: {verdict}"
    );
    assert!(
        verdict.starts_with("nothing to connect to"),
        "the intruder could reach the channel: {verdict}"
    );
    assert!(started.elapsed() < Duration::from_secs(25));
    runtime.block_on(worker.shutdown(Duration::from_secs(5)));
}
