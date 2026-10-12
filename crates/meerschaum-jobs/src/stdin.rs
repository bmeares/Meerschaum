//! Port of `meerschaum.utils.daemon.StdinFile`: a FIFO (`jobs/<id>/input.stdin`, mode 0600) through
//! which `attach` and `write_stdin` feed a daemon's input, plus the `input.stdin.block` marker a
//! reader creates while it waits.

use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// The daemon-side and client-side ends of a job's stdin FIFO.
#[derive(Debug)]
pub struct StdinFile {
    /// `input.stdin`.
    pub file_path: PathBuf,
    /// `input.stdin.block`.
    pub blocking_file_path: PathBuf,
    refresh: Duration,
    reader: Option<File>,
    writer: Option<File>,
}

impl StdinFile {
    /// `StdinFile(file_path, lock_file_path)`; the block file defaults to `<file_path>.block`.
    pub fn new(file_path: impl Into<PathBuf>, lock_file_path: Option<PathBuf>) -> StdinFile {
        let file_path = file_path.into();
        let blocking_file_path = lock_file_path.unwrap_or_else(|| {
            let mut name = file_path
                .file_name()
                .map(|n| n.to_os_string())
                .unwrap_or_default();
            name.push(".block");
            file_path.with_file_name(name)
        });
        StdinFile {
            file_path,
            blocking_file_path,
            refresh: Duration::from_millis(100),
            reader: None,
            writer: None,
        }
    }

    /// Set the polling interval (`system.cli.refresh_seconds`).
    pub fn with_refresh(mut self, refresh: Duration) -> StdinFile {
        self.refresh = refresh;
        self
    }

    /// Create the FIFO (mode 0600) if it does not exist.
    pub fn ensure_fifo(&self) -> std::io::Result<()> {
        ensure_fifo(&self.file_path)
    }

    /// `write(data)`: send input to the daemon.
    ///
    /// Python's `open(path, 'wb')` blocks until a reader opens the FIFO; this returns an error
    /// instead when no daemon holds the read end.
    pub fn write(&mut self, data: &[u8]) -> std::io::Result<()> {
        if self.writer.is_none() {
            if let Some(parent) = self.file_path.parent() {
                fs::create_dir_all(parent)?;
            }
            self.ensure_fifo()?;
            self.writer = Some(open_fifo_writer(&self.file_path)?);
        }
        if let Some(w) = self.writer.as_mut() {
            match w.write_all(data).and_then(|_| w.flush()) {
                Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {}
                other => other?,
            }
        }
        Ok(())
    }

    fn reader(&mut self) -> std::io::Result<&mut File> {
        if self.reader.is_none() {
            if let Some(parent) = self.file_path.parent() {
                fs::create_dir_all(parent)?;
            }
            self.ensure_fifo()?;
            self.reader = Some(open_fifo_reader(&self.file_path)?);
        }
        Ok(self.reader.as_mut().expect("reader"))
    }

    /// `read(size)`: poll the FIFO, touching the block file while waiting.
    pub fn read(&mut self, size: usize) -> std::io::Result<Vec<u8>> {
        let size = size.max(1);
        loop {
            let mut buf = vec![0u8; size];
            let n = match self.reader()?.read(&mut buf) {
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => 0,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => 0,
                Err(e) => return Err(e),
            };
            if n > 0 {
                let _ = fs::remove_file(&self.blocking_file_path);
                buf.truncate(n);
                return Ok(buf);
            }
            if !self.blocking_file_path.exists() {
                let _ = File::create(&self.blocking_file_path);
            }
            std::thread::sleep(self.refresh);
        }
    }

    /// `readline()`: read one byte at a time up to (not including) `\n`.
    pub fn readline(&mut self) -> std::io::Result<String> {
        let mut line = Vec::new();
        loop {
            let data = self.read(1)?;
            if data.is_empty() || data == b"\n" {
                break;
            }
            line.extend_from_slice(&data);
        }
        Ok(String::from_utf8_lossy(&line).to_string())
    }

    /// `close()`: drop both ends and remove the block file.
    pub fn close(&mut self) {
        self.reader = None;
        self.writer = None;
        let _ = fs::remove_file(&self.blocking_file_path);
    }

    /// `is_open()`.
    pub fn is_open(&self) -> bool {
        self.reader.is_some()
    }
}

/// `os.mkfifo(path, mode=0o600)` unless something already exists at `path`.
pub fn ensure_fifo(path: &Path) -> std::io::Result<()> {
    if path.exists() {
        return Ok(());
    }
    #[cfg(unix)]
    {
        use nix::sys::stat::Mode;
        match nix::unistd::mkfifo(path, Mode::from_bits_truncate(0o600)) {
            Ok(()) | Err(nix::errno::Errno::EEXIST) => Ok(()),
            Err(e) => Err(std::io::Error::from(e)),
        }
    }
    #[cfg(not(unix))]
    {
        File::create(path).map(|_| ())
    }
}

#[cfg(unix)]
fn open_fifo_reader(path: &Path) -> std::io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(path)
}

#[cfg(unix)]
fn open_fifo_writer(path: &Path) -> std::io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    let file = fs::OpenOptions::new()
        .write(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(path)?;
    // Back to blocking writes once a reader is known to exist.
    let flags =
        nix::fcntl::fcntl(&file, nix::fcntl::FcntlArg::F_GETFL).map_err(std::io::Error::from)?;
    let mut flags = nix::fcntl::OFlag::from_bits_truncate(flags);
    flags.remove(nix::fcntl::OFlag::O_NONBLOCK);
    nix::fcntl::fcntl(&file, nix::fcntl::FcntlArg::F_SETFL(flags)).map_err(std::io::Error::from)?;
    Ok(file)
}

#[cfg(not(unix))]
fn open_fifo_reader(path: &Path) -> std::io::Result<File> {
    File::open(path)
}

#[cfg(not(unix))]
fn open_fifo_writer(path: &Path) -> std::io::Result<File> {
    fs::OpenOptions::new().append(true).open(path)
}

/// Open the FIFO read-write for a daemon's standard input: the read end never sees EOF while
/// the daemon holds its own write end, so reads block until `attach` writes something.
#[cfg(unix)]
pub fn open_fifo_for_daemon(path: &Path) -> std::io::Result<File> {
    ensure_fifo(path)?;
    fs::OpenOptions::new().read(true).write(true).open(path)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn fifo_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("job").join("input.stdin");
        let mut reader = StdinFile::new(&path, None).with_refresh(Duration::from_millis(10));
        // No reader yet: writing fails instead of blocking.
        assert!(StdinFile::new(&path, None).write(b"x").is_err());
        let _ = reader.reader().unwrap();
        let mut writer = StdinFile::new(&path, None);
        writer.write(b"hello\nworld").unwrap();
        assert_eq!(reader.readline().unwrap(), "hello");
        assert!(!reader.blocking_file_path.exists());
        use std::os::unix::fs::FileTypeExt;
        assert!(fs::metadata(&path).unwrap().file_type().is_fifo());
        reader.close();
    }
}
