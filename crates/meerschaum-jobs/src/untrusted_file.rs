//! Bounded protocol reads from writable job state. Never follow links or open a FIFO.

use std::io::{self, Read};
use std::path::Path;

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "Invalid job protocol file.")
}

fn read(file: std::fs::File, limit: usize) -> io::Result<Vec<u8>> {
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > limit as u64 {
        return Err(invalid());
    }
    let mut bytes = Vec::with_capacity((metadata.len() as usize).min(limit));
    file.take((limit as u64).checked_add(1).ok_or_else(invalid)?)
        .read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(invalid());
    }
    Ok(bytes)
}

pub(crate) fn read_regular(
    path: &Path,
    limit: usize,
    consume: bool,
) -> io::Result<Option<Vec<u8>>> {
    let result = read_at(path, limit, consume);
    match result {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        other => other.map(Some),
    }
}

#[cfg(unix)]
fn read_at(path: &Path, limit: usize, consume: bool) -> io::Result<Vec<u8>> {
    use rustix::fs::{AtFlags, Mode, OFlags, open, openat, unlinkat};
    use std::path::Component;
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let parent = absolute.parent().ok_or_else(invalid)?;
    let name = absolute.file_name().ok_or_else(invalid)?;
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let mut directory = open("/", flags, Mode::empty())?;
    // Hold every selected directory while resolving its next component. A child cannot
    // redirect an unconfined reader through either the leaf or an intermediate symlink.
    for component in parent.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(component) => {
                directory = openat(&directory, component, flags, Mode::empty())?
            }
            _ => return Err(invalid()),
        }
    }
    let result = (|| {
        let leaf = openat(
            &directory,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC | OFlags::NOCTTY,
            Mode::empty(),
        )?;
        read(std::fs::File::from(leaf), limit)
    })();
    if consume {
        // Unlink in the held directory, even if a child replaces its pathname concurrently.
        let _ = unlinkat(&directory, name, AtFlags::empty());
    }
    result
}

#[cfg(not(unix))]
fn read_at(path: &Path, limit: usize, consume: bool) -> io::Result<Vec<u8>> {
    // Mandatory untrusted Api execution is Linux-only. Preserve bounded legacy file reads
    // on other platforms, refusing any observed link in the selected path.
    let mut prefix = std::path::PathBuf::new();
    for component in path.components() {
        prefix.push(component);
        if std::fs::symlink_metadata(&prefix)?.file_type().is_symlink() {
            return Err(invalid());
        }
    }
    if !std::fs::symlink_metadata(path)?.is_file() {
        return Err(invalid());
    }
    let result = read(std::fs::File::open(path)?, limit);
    if consume {
        let _ = std::fs::remove_file(path);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_protocol_reader_consumes_regular_files_and_refuses_large_or_linked_data() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("result.json");
        assert!(read_regular(&file, 32, true).unwrap().is_none());
        std::fs::write(&file, b"[true,\"owned\"]").unwrap();
        assert_eq!(
            read_regular(&file, 32, true).unwrap().unwrap(),
            b"[true,\"owned\"]"
        );
        assert!(!file.exists());
        std::fs::write(&file, [b'x'; 33]).unwrap();
        assert!(read_regular(&file, 32, true).is_err());
        assert!(!file.exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let outside = root.path().join("outside.json");
            std::fs::write(&outside, b"[true,\"outside\"]").unwrap();
            symlink(&outside, &file).unwrap();
            assert!(read_regular(&file, 32, true).is_err());
            assert!(outside.exists());
            let directory = root.path().join("state");
            std::fs::create_dir(&directory).unwrap();
            std::fs::write(directory.join("result.json"), b"[true,\"outside\"]").unwrap();
            let linked = root.path().join("linked");
            symlink(&directory, &linked).unwrap();
            assert!(read_regular(&linked.join("result.json"), 32, true).is_err());
            assert!(directory.join("result.json").exists());
        }
    }

    #[cfg(unix)]
    #[test]
    fn protocol_fifo_is_rejected_without_waiting_for_a_writer() {
        const CHILD: &str = "MRSM_TEST_PROTOCOL_FIFO";
        let test = "untrusted_file::tests::protocol_fifo_is_rejected_without_waiting_for_a_writer";
        if let Some(path) = std::env::var_os(CHILD) {
            assert!(read_regular(Path::new(&path), 32, false).is_err());
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let fifo = root.path().join("result.json");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&fifo)
                .env_remove("TMUX")
                .env_remove("TMUX_PANE")
                .status()
                .unwrap()
                .success()
        );
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test, "--test-threads=1"])
            .env(CHILD, fifo)
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("Protocol reader blocked on owned FIFO.");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}
