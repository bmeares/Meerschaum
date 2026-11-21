//! Real inherited-descriptor control: only the selected standard streams cross native exec.

use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::os::unix::net::UnixStream;
use std::process::{Command, Stdio};

const FILE_SECRET: &[u8] = b"synthetic-parent-file-not-for-a-worker";
const STREAM_SECRET: &[u8] = b"synthetic-parent-connected-stream-not-for-a-worker";
const STDIN_MESSAGE: &str = "the selected standard input survives";
const PROBE: &str = "MRSM_TEST_NATIVE_EXEC_FD_PROBE";

fn inheritable(fd: i32) {
    // SAFETY: fcntl operates only on this fixture's live owned descriptor.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    assert!(flags >= 0);
    assert_eq!(
        unsafe { libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) },
        0
    );
}

#[test]
fn descriptor_probe_child() {
    if std::env::var_os(PROBE).is_none() {
        return;
    }
    let file: i32 = std::env::var("MRSM_TEST_PARENT_FILE_FD")
        .unwrap()
        .parse()
        .unwrap();
    let socket: i32 = std::env::var("MRSM_TEST_PARENT_STREAM_FD")
        .unwrap()
        .parse()
        .unwrap();
    let mut file_bytes = [0u8; 128];
    let mut stream_bytes = [0u8; 128];
    // Numeric descriptors may be absent or reused by the child harness. Raw pread/recv return
    // EBADF/ESPIPE/ENOTSOCK safely; PEEK + DONTWAIT cannot consume or block another channel.
    let file_count =
        unsafe { libc::pread(file, file_bytes.as_mut_ptr().cast(), file_bytes.len(), 0) };
    let stream_count = unsafe {
        libc::recv(
            socket,
            stream_bytes.as_mut_ptr().cast(),
            stream_bytes.len(),
            libc::MSG_DONTWAIT | libc::MSG_PEEK,
        )
    };
    let mut stdin = String::new();
    std::io::stdin().read_to_string(&mut stdin).unwrap();
    println!(
        "FD_PROBE:{}",
        serde_json::json!({
            "file": file_count == FILE_SECRET.len() as isize && &file_bytes[..FILE_SECRET.len()] == FILE_SECRET,
            "stream": stream_count == STREAM_SECRET.len() as isize && &stream_bytes[..STREAM_SECRET.len()] == STREAM_SECRET,
            "stdin": stdin == STDIN_MESSAGE,
        })
    );
    eprintln!("SELECTED_STDERR_SURVIVED");
}

fn probe(root: &std::path::Path, file: i32, stream: i32, sealed: bool) -> serde_json::Value {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "exec_descriptors::descriptor_probe_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env_clear()
        .env(PROBE, "1")
        .env("MRSM_ROOT_DIR", root)
        .env("MRSM_TEST_PARENT_FILE_FD", file.to_string())
        .env("MRSM_TEST_PARENT_STREAM_FD", stream.to_string())
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if sealed {
        meerschaum_jobs::supervisor::seal_native_command(&mut command).unwrap();
    }
    let mut child = command.spawn().unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(STDIN_MESSAGE.as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("SELECTED_STDERR_SURVIVED"));
    let stdout = String::from_utf8(output.stdout).unwrap();
    stdout
        .lines()
        .find_map(|line| {
            line.split_once("FD_PROBE:")
                .map(|(_, json)| serde_json::from_str(json).unwrap())
        })
        .unwrap_or_else(|| panic!("The native exec probe produced no result: {stdout}"))
}

#[test]
fn sealed_native_exec_discards_inheritable_main_file_and_connected_stream_but_preserves_stdio_and_parent()
 {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("main-only-file");
    std::fs::write(&path, FILE_SECRET).unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let (mut socket, mut peer) = UnixStream::pair().unwrap();
    inheritable(file.as_raw_fd());
    inheritable(socket.as_raw_fd());
    peer.write_all(STREAM_SECRET).unwrap();
    assert_eq!(
        probe(root.path(), file.as_raw_fd(), socket.as_raw_fd(), false),
        serde_json::json!({"file":true,"stream":true,"stdin":true}),
        "The unsealed control did not expose the intended inherited channels."
    );
    assert_eq!(
        probe(root.path(), file.as_raw_fd(), socket.as_raw_fd(), true),
        serde_json::json!({"file":false,"stream":false,"stdin":true})
    );
    let mut bytes = vec![0; FILE_SECRET.len()];
    assert_eq!(file.read_at(&mut bytes, 0).unwrap(), bytes.len());
    assert_eq!(bytes, FILE_SECRET);
    let mut bytes = vec![0; STREAM_SECRET.len()];
    socket.read_exact(&mut bytes).unwrap();
    assert_eq!(bytes, STREAM_SECRET);
    // The operation runs only in the forked child; the parent's original flags stay unchanged.
    assert_eq!(
        unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
        0
    );
    assert_eq!(
        unsafe { libc::fcntl(socket.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
        0
    );
}
