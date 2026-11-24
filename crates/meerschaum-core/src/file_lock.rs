//! Files used for inter-process locks.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

/// Open or create a lock file without truncating it. Windows requires a readable handle
/// for LockFileEx when the handle's write access is limited to appending.
pub fn open(path: impl AsRef<Path>) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .read(cfg!(windows))
        .append(true)
        .open(path)
}
