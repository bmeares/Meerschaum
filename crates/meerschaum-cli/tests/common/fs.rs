//! Shared filesystem, log-tail, plugin archive, and generation-dir helpers for integration tests.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

/// Read up to the last `max_bytes` of a regular file without following symlinks or blocking on FIFOs.
#[cfg(unix)]
pub fn file_tail(path: &Path, max_bytes: usize) -> String {
    use rustix::fs::{Mode, OFlags, open};
    use std::io::{Read, Seek, SeekFrom};

    let flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
    let Ok(fd) = open(path, flags, Mode::empty()) else {
        return String::new();
    };
    let mut file = std::fs::File::from(fd);
    let Ok(metadata) = file.metadata() else {
        return String::new();
    };
    if !metadata.is_file()
        || file
            .seek(SeekFrom::Start(
                metadata.len().saturating_sub(max_bytes as u64),
            ))
            .is_err()
    {
        return String::new();
    }
    let mut bytes = Vec::new();
    if file.take(max_bytes as u64).read_to_end(&mut bytes).is_err() {
        return String::new();
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Write a single-file `.tar.gz` plugin archive under `dir/{plugin}.tar.gz` with mode `0o400`.
pub fn write_plugin_archive(dir: &Path, plugin: &str, filename: &str, source: &str) -> PathBuf {
    use flate2::Compression;
    use flate2::write::GzEncoder;

    std::fs::create_dir_all(dir).unwrap();
    let path = dir.join(format!("{plugin}.tar.gz"));
    let file = std::fs::File::create(&path).unwrap();
    let mut archive = tar::Builder::new(GzEncoder::new(file, Compression::fast()));
    let mut header = tar::Header::new_gnu();
    header.set_size(source.len() as u64);
    header.set_mode(0o400);
    header.set_cksum();
    archive
        .append_data(&mut header, filename, source.as_bytes())
        .unwrap();
    archive.into_inner().unwrap().finish().unwrap();
    path
}

/// Return the single `generation-*` subdirectory inside `dir`.
pub fn only_generation(dir: &Path) -> PathBuf {
    let mut generations = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("generation-")
        })
        .collect::<Vec<_>>();
    generations.sort();
    assert_eq!(generations.len(), 1, "{generations:?}");
    generations.into_iter().next().unwrap()
}
