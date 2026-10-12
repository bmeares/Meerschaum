//! Port of `meerschaum.utils.daemon.RotatingFile` and of the transform applied by
//! `FileDescriptorInterceptor`.
//!
//! A rotating log is a base path `logs/<name>.log` whose output lives in subfiles
//! `<name>.log.0`, `<name>.log.1`, ...; when the latest subfile reaches `max_file_size` bytes a new
//! one is started and all but the newest `num_files_to_keep` are deleted.
//!
//! Writes are unbuffered. Python's `RotatingFile.write()` reopens the latest subfile on every call
//! and leaves the previous handle to be flushed when it is garbage-collected, so on disk Python
//! can lag by one write; the eventual contents are the same.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use regex::Regex;

use crate::pytime;

/// Errors raised by [`RotatingFile::new`] (Python raises `ValueError`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RotatingFileError {
    /// `num_files_to_keep < 1`.
    #[error("At least 1 file must be kept.")]
    TooFewFiles,
    /// `max_file_size < 100`.
    #[error("Subfiles must contain at least 100 bytes.")]
    TooSmall,
}

/// The settings of a rotating log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotatingFileOptions {
    /// How many subfiles to keep.
    pub num_files_to_keep: i64,
    /// The subfile size threshold in bytes.
    pub max_file_size: i64,
    /// Prepend `strftime(timestamp_format) + ' | '` to writes made through [`RotatingFile::write`].
    pub write_timestamps: bool,
    /// The timestamp format.
    pub timestamp_format: String,
}

impl Default for RotatingFileOptions {
    fn default() -> Self {
        RotatingFileOptions {
            num_files_to_keep: 5,
            max_file_size: 100_000,
            write_timestamps: false,
            timestamp_format: "%Y-%m-%d %H:%M".to_string(),
        }
    }
}

#[derive(Debug)]
struct Current {
    index: i64,
    file: File,
    /// The handle's position (`_current_file_obj.tell()`).
    pos: u64,
    /// Whether the handle is in `subfile_objects` (opened by `increment_subfiles`).
    in_dict: bool,
}

/// A file-like object which writes into rotating subfiles.
#[derive(Debug)]
pub struct RotatingFile {
    file_path: PathBuf,
    options: RotatingFileOptions,
    subfile_regex: Regex,
    /// `_cursor`: `(subfile_index, position)`.
    cursor: (i64, u64),
    current: Option<Current>,
    /// The keys of `subfile_objects`.
    handles: Vec<i64>,
    prefix_override: Option<String>,
}

impl RotatingFile {
    /// `RotatingFile(file_path, num_files_to_keep, max_file_size, write_timestamps, timestamp_format)`.
    pub fn new(
        file_path: impl Into<PathBuf>,
        options: RotatingFileOptions,
    ) -> Result<RotatingFile, RotatingFileError> {
        if options.num_files_to_keep < 1 {
            return Err(RotatingFileError::TooFewFiles);
        }
        if options.max_file_size < 100 {
            return Err(RotatingFileError::TooSmall);
        }
        Ok(RotatingFile {
            file_path: file_path.into(),
            options,
            subfile_regex: Regex::new(r"^(.*)\.log(?:\.\d+)?").expect("valid regex"),
            cursor: (0, 0),
            current: None,
            handles: Vec::new(),
            prefix_override: None,
        })
    }

    /// Use a fixed timestamp prefix instead of the current time (for tests and fixtures).
    pub fn with_fixed_prefix(mut self, prefix: impl Into<String>) -> RotatingFile {
        self.prefix_override = Some(prefix.into());
        self
    }

    /// The base path (`logs/<name>.log`).
    pub fn file_path(&self) -> &Path {
        &self.file_path
    }

    /// The settings.
    pub fn options(&self) -> &RotatingFileOptions {
        &self.options
    }

    /// `_cursor`.
    pub fn cursor(&self) -> (i64, u64) {
        self.cursor
    }

    /// Set `_cursor` (e.g. from the persisted log offset).
    pub fn set_cursor(&mut self, cursor: (i64, u64)) {
        self.cursor = cursor;
    }

    fn base_name(&self) -> String {
        self.file_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default()
    }

    fn parent(&self) -> PathBuf {
        self.file_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
    }

    /// `get_index_from_subfile_name`: `-1` when the name does not parse.
    pub fn get_index_from_subfile_name(&self, subfile_name: &str) -> i64 {
        // PARITY: `int(subfile_name.replace(self.file_path.name + '.', ''))`.
        let stripped = subfile_name.replace(&format!("{}.", self.base_name()), "");
        meerschaum_core::pyfmt::parse_py_int(&stripped).unwrap_or(-1)
    }

    /// `get_subfile_name_from_index`.
    pub fn get_subfile_name_from_index(&self, index: i64) -> String {
        format!("{}.{}", self.base_name(), index)
    }

    /// `get_subfile_path_from_index`.
    pub fn get_subfile_path_from_index(&self, index: i64) -> PathBuf {
        self.parent().join(self.get_subfile_name_from_index(index))
    }

    /// `get_existing_subfile_paths`: matching files sorted by index.
    pub fn get_existing_subfile_paths(&self) -> Vec<PathBuf> {
        let parent = self.parent();
        let Ok(entries) = fs::read_dir(&parent) else {
            return Vec::new();
        };
        let base = self.base_name();
        let mut names: Vec<(String, i64)> = entries
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.starts_with(&base) && self.subfile_regex.is_match(n))
            .map(|n| {
                let ix = self.get_index_from_subfile_name(&n);
                (n, ix)
            })
            .collect();
        names.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        names.into_iter().map(|(n, _)| parent.join(n)).collect()
    }

    /// `get_existing_subfile_indices`.
    pub fn get_existing_subfile_indices(&self) -> Vec<i64> {
        self.get_existing_subfile_paths()
            .iter()
            .map(|p| self.index_of(p))
            .collect()
    }

    fn index_of(&self, path: &Path) -> i64 {
        self.get_index_from_subfile_name(
            &path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default(),
        )
    }

    /// `get_latest_subfile_index`: the last existing index, or 0 when there are none.
    pub fn get_latest_subfile_index(&self) -> i64 {
        self.get_existing_subfile_paths()
            .last()
            .map(|p| self.index_of(p))
            .unwrap_or(0)
    }

    /// `get_latest_subfile_path`.
    pub fn get_latest_subfile_path(&self) -> PathBuf {
        self.get_subfile_path_from_index(self.get_latest_subfile_index())
    }

    /// `is_subfile_too_large(index, potential_new_len)`.
    pub fn is_subfile_too_large(&self, index: i64, potential_new_len: u64) -> bool {
        let path = self.get_subfile_path_from_index(index);
        match fs::metadata(&path) {
            Ok(m) => m.len() + potential_new_len >= self.options.max_file_size as u64,
            Err(_) => false,
        }
    }

    fn open_append(path: &Path) -> std::io::Result<(File, u64)> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        let len = file.metadata()?.len();
        Ok((file, len))
    }

    /// `refresh_files(potential_new_len)`: (re)open the latest subfile and rotate if needed.
    pub fn refresh_files(&mut self, potential_new_len: u64) -> std::io::Result<()> {
        let latest = self.get_latest_subfile_index();
        let latest_path = self.get_subfile_path_from_index(latest);
        let is_first_run_with_logs = latest > -1 && self.current.is_none();
        // PARITY: `lost_latest_handle` is always true while a handle is open: it parses the
        // handle's *full path* as a subfile name, which yields index -1. So the latest subfile
        // is reopened on every refresh.
        let lost_latest_handle = self.current.is_some();
        if is_first_run_with_logs || lost_latest_handle {
            let (file, pos) = Self::open_append(&latest_path)?;
            self.current = Some(Current {
                index: latest,
                file,
                pos,
                in_dict: false,
            });
        }
        let create_new_file = latest == -1
            || self.current.is_none()
            || self.is_subfile_too_large(latest, potential_new_len);
        if create_new_file {
            self.increment_subfiles(1)?;
        }
        Ok(())
    }

    /// `increment_subfiles(increment_by)`: start a new subfile and delete old ones.
    pub fn increment_subfiles(&mut self, increment_by: i64) -> std::io::Result<()> {
        let latest = self.get_latest_subfile_index();
        let new_index = latest + increment_by;
        let path = self.get_subfile_path_from_index(new_index);
        let (file, pos) = Self::open_append(&path)?;
        self.current = Some(Current {
            index: new_index,
            file,
            pos,
            in_dict: true,
        });
        self.handles.retain(|i| *i == new_index);
        if !self.handles.contains(&new_index) {
            self.handles.push(new_index);
        }
        self.delete(true);
        Ok(())
    }

    /// `delete(unused_only)`: delete old subfiles (all of them unless `unused_only`).
    pub fn delete(&mut self, unused_only: bool) {
        let existing = self.get_existing_subfile_paths();
        let keep = self.options.num_files_to_keep as usize;
        if unused_only && existing.len() <= keep {
            return;
        }
        if !unused_only {
            self.close();
        }
        let end = if unused_only {
            existing.len().saturating_sub(keep)
        } else {
            existing.len()
        };
        for path in &existing[..end] {
            let _ = fs::remove_file(path);
        }
    }

    /// `close()`: drop every open handle.
    pub fn close(&mut self) {
        self.current = None;
        self.handles.clear();
    }

    fn prefix(&self) -> String {
        match &self.prefix_override {
            Some(p) => p.clone(),
            None => pytime::timestamp_prefix(pytime::utcnow(), &self.options.timestamp_format),
        }
    }

    /// The current timestamp prefix (`get_timestamp_prefix_str`).
    pub fn get_timestamp_prefix_str(&self) -> String {
        self.prefix()
    }

    /// `write(data)`: the file-object API (`print()` into a `RotatingFile`). With timestamps,
    /// each call becomes `prefix + data + '\n'`.
    pub fn write(&mut self, data: &str) -> std::io::Result<()> {
        let (prefix, suffix) = if self.options.write_timestamps {
            (self.prefix(), "\n")
        } else {
            (String::new(), "")
        };
        // PARITY: `len(prefix_str + data + suffix_str)` counts characters, not bytes.
        let potential = (prefix.chars().count() + data.chars().count() + suffix.len()) as u64;
        self.refresh_files(potential)?;
        let text = format!("{prefix}{data}{suffix}");
        self.write_current(text.as_bytes())?;
        self.delete(true);
        Ok(())
    }

    /// Append raw bytes to the latest subfile (the file-descriptor path the daemon's output takes).
    /// Rotation happens before the write when the latest subfile has reached `max_file_size`.
    pub fn write_raw(&mut self, data: &[u8]) -> std::io::Result<()> {
        if self.current.is_none() {
            self.refresh_files(0)?;
        } else if self.is_subfile_too_large(self.get_latest_subfile_index(), 0) {
            self.increment_subfiles(1)?;
        }
        self.write_current(data)
    }

    fn write_current(&mut self, data: &[u8]) -> std::io::Result<()> {
        let Some(current) = self.current.as_mut() else {
            return Ok(());
        };
        current.file.write_all(data)?;
        current.file.flush()?;
        current.pos = current
            .file
            .metadata()
            .map(|m| m.len())
            .unwrap_or(current.pos);
        Ok(())
    }

    fn paths_from_cursor(&self) -> Vec<(i64, PathBuf)> {
        self.get_existing_subfile_paths()
            .iter()
            .map(|p| self.index_of(p))
            .filter(|ix| *ix >= self.cursor.0)
            .map(|ix| (ix, self.get_subfile_path_from_index(ix)))
            .collect()
    }

    /// Read a subfile from `seek_ix` the way Python's text mode does: strict UTF-8 with up to
    /// five one-byte seek-backs on decode errors, then universal-newline translation.
    fn read_text_from(path: &Path, seek_ix: u64) -> (Option<String>, u64) {
        let Ok(bytes) = fs::read(path) else {
            return (None, 0);
        };
        let len = bytes.len() as u64;
        for i in 0..5u64 {
            let start = seek_ix.saturating_sub(i).min(len) as usize;
            if let Ok(s) = std::str::from_utf8(&bytes[start..]) {
                return (Some(universal_newlines(s)), len);
            }
        }
        (None, len)
    }

    fn after_read(&mut self, read_indices: &[i64]) {
        if let Some(current) = self.current.as_mut() {
            if current.in_dict && read_indices.contains(&current.index) {
                current.pos = current
                    .file
                    .metadata()
                    .map(|m| m.len())
                    .unwrap_or(current.pos);
            }
        }
    }

    /// `read()`: the text of the subfiles from the cursor on.
    pub fn read(&mut self) -> String {
        let paths = self.paths_from_cursor();
        let mut buffer = String::new();
        let mut refresh_cursor = true;
        let last_ix = paths.last().map(|(ix, _)| *ix);
        let mut read_indices = Vec::new();
        for (ix, path) in &paths {
            let seek_ix = if *ix == self.cursor.0 {
                self.cursor.1
            } else {
                0
            };
            let (text, len) = Self::read_text_from(path, seek_ix);
            if let Some(text) = text {
                buffer.push_str(&text);
            }
            read_indices.push(*ix);
            // PARITY: "Handle the case when no files have yet been opened."
            if self.handles.is_empty() && Some(*ix) == last_ix {
                self.cursor = (*ix, len);
                refresh_cursor = false;
            }
        }
        self.after_read(&read_indices);
        if refresh_cursor {
            self.refresh_cursor();
        }
        buffer
    }

    /// `readlines()`: the lines from the cursor on (a line split across subfiles is joined).
    pub fn readlines(&mut self) -> Vec<String> {
        let paths = self.paths_from_cursor();
        let mut lines: Vec<String> = Vec::new();
        let mut refresh_cursor = true;
        let last_ix = paths.last().map(|(ix, _)| *ix);
        let mut read_indices = Vec::new();
        for (ix, path) in &paths {
            let seek_ix = if *ix == self.cursor.0 {
                self.cursor.1
            } else {
                0
            };
            let (text, len) = Self::read_text_from(path, seek_ix);
            let subfile_lines: Vec<String> =
                text.map(|t| split_lines_keepends(&t)).unwrap_or_default();
            read_indices.push(*ix);
            if self.handles.is_empty() && Some(*ix) == last_ix {
                self.cursor = (*ix, len);
                refresh_cursor = false;
            }
            let mut new_lines = subfile_lines.as_slice();
            if let (Some(last), Some(first)) = (lines.last_mut(), subfile_lines.first()) {
                if !last.ends_with('\n') {
                    last.push_str(first);
                    new_lines = &subfile_lines[1..];
                }
            }
            lines.extend(new_lines.iter().cloned());
        }
        self.after_read(&read_indices);
        if refresh_cursor {
            self.refresh_cursor();
        }
        lines
    }

    /// `refresh_cursor()`: `(latest index, current handle position)`.
    pub fn refresh_cursor(&mut self) {
        let existing = self.get_existing_subfile_paths();
        let ix = existing.last().map(|p| self.index_of(p)).unwrap_or(0);
        let pos = self.current.as_ref().map(|c| c.pos).unwrap_or(0);
        self.cursor = (ix, pos);
    }

    /// `seek(position)`: `0` rewinds to the oldest subfile; anything else seeks the latest one.
    pub fn seek(&mut self, position: u64) {
        let indices = self.get_existing_subfile_indices();
        let min_ix = indices.first().copied().unwrap_or(0);
        let max_ix = indices.last().copied().unwrap_or(0);
        if position == 0 {
            self.cursor = (min_ix, 0);
            return;
        }
        self.cursor = (max_ix, position);
        if let Some(current) = self.current.as_mut() {
            current.pos = position;
        }
    }

    /// `touch()`: touch the latest subfile.
    pub fn touch(&self) -> std::io::Result<()> {
        let path = self.get_latest_subfile_path();
        OpenOptions::new().create(true).append(true).open(path)?;
        Ok(())
    }
}

/// Python's universal newlines on read: `\r\n` and `\r` become `\n`.
pub fn universal_newlines(s: &str) -> String {
    if !s.contains('\r') {
        return s.to_string();
    }
    s.replace("\r\n", "\n").replace('\r', "\n")
}

/// `str.splitlines(keepends=True)` restricted to `\n` (text already translated).
pub fn split_lines_keepends(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut start = 0;
    for (i, c) in s.char_indices() {
        if c == '\n' {
            out.push(s[start..=i].to_string());
            start = i + 1;
        }
    }
    if start < s.len() {
        out.push(s[start..].to_string());
    }
    out
}

/// The transform `FileDescriptorInterceptor` applies to every chunk read from a daemon's stdout
/// or stderr.
#[derive(Debug, Clone)]
pub struct Interceptor {
    is_first_read: bool,
}

impl Default for Interceptor {
    fn default() -> Self {
        Interceptor {
            is_first_read: true,
        }
    }
}

impl Interceptor {
    /// A fresh interceptor (its first chunk gains a leading newline).
    pub fn new() -> Interceptor {
        Interceptor::default()
    }

    /// Reinstall the interceptor (what Python's log-refresh timer does every
    /// `refresh_files_seconds`): the next chunk gains a leading newline again.
    pub fn reset(&mut self) {
        self.is_first_read = true;
    }

    /// Transform one chunk: prepend `\n` to the first chunk, then follow every newline with the
    /// timestamp prefix.
    pub fn transform(&mut self, data: &[u8], prefix: &str) -> Vec<u8> {
        let mut data_vec;
        let data = if self.is_first_read {
            self.is_first_read = false;
            data_vec = Vec::with_capacity(data.len() + 1);
            data_vec.push(b'\n');
            data_vec.extend_from_slice(data);
            &data_vec[..]
        } else {
            data
        };
        // PARITY: `last_char_is_newline = data[-1] == b'\n'` compares an int to bytes and is
        // always False, so a trailing newline is followed by a (dangling) prefix too.
        let mut out = Vec::with_capacity(data.len() + prefix.len() * 4);
        for &b in data {
            out.push(b);
            if b == b'\n' {
                out.extend_from_slice(prefix.as_bytes());
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interceptor_quirks() {
        let mut i = Interceptor::new();
        let p = "TS | ";
        assert_eq!(i.transform(b"hello\n", p), b"\nTS | hello\nTS | ".to_vec());
        assert_eq!(i.transform(b"a\nb", p), b"a\nTS | b".to_vec());
        i.reset();
        assert_eq!(i.transform(b"x", p), b"\nTS | x".to_vec());
    }

    #[test]
    fn index_parsing() {
        let rf = RotatingFile::new("/tmp/x/job.log", RotatingFileOptions::default()).unwrap();
        assert_eq!(rf.get_index_from_subfile_name("job.log.3"), 3);
        assert_eq!(rf.get_index_from_subfile_name("job.log"), -1);
        assert_eq!(rf.get_index_from_subfile_name("job.log.x"), -1);
        assert!(
            RotatingFile::new(
                "/tmp/x",
                RotatingFileOptions {
                    max_file_size: 10,
                    ..Default::default()
                }
            )
            .is_err()
        );
    }
}
