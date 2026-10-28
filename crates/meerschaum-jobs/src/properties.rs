//! `properties.json` and `.stop.json`: reading, writing and
//! the cascading merge `Daemon.properties` performs on every access.

use std::fs;
use std::io::Write;
use std::path::Path;

use meerschaum_core::pyfmt::{JsonStyle, json_dumps};
use meerschaum_core::success::SuccessTuple;
use meerschaum_core::value::{Map, Value, apply_patch_maps};

/// `json.dumps(value)` with the default separators and `ensure_ascii=True` (`json.dump`).
pub fn dumps(value: &Value) -> String {
    json_dumps(value, JsonStyle::Default, true)
}

/// `Daemon.read_properties()`: `None` when the file is missing, `{}` when it cannot be parsed.
pub fn read_properties(path: &Path) -> Option<Map> {
    if !path.exists() {
        return None;
    }
    let text = fs::read_to_string(path).unwrap_or_default();
    match Value::from_json_str(&text) {
        Ok(Value::Map(m)) => Some(m),
        // PARITY: `properties or {}` (a falsy JSON document reads as an empty dict).
        _ => Some(Map::new()),
    }
}

/// Port of `generate_password(length)`: random ASCII letters and digits.
pub fn random_alnum(length: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut bytes = vec![0u8; length];
    let _ = getrandom::fill(&mut bytes);
    bytes
        .iter()
        .map(|b| ALPHABET[(*b as usize) % ALPHABET.len()] as char)
        .collect()
}

/// `Daemon.write_properties()`: publish a complete JSON document with a same-directory rename.
/// Unlike v4's backup-then-write, readers never see an existing file missing or partially written.
/// Failed writes leave the previous file intact and remove only their own staging file.
pub fn write_properties(path: &Path, props: &Map) -> SuccessTuple {
    write_properties_with(path, props, true, |from, to| fs::rename(from, to))
}

/// [`write_properties`] into a job directory that must still exist: a directory that was deleted
/// (the job was deleted while its supervisor ran) is never created again, and the write fails.
pub fn write_existing_properties(path: &Path, props: &Map) -> SuccessTuple {
    write_properties_with(path, props, false, |from, to| fs::rename(from, to))
}

/// The publication operation is injectable so tests can pause or fail the atomic replacement.
fn write_properties_with(
    path: &Path,
    props: &Map,
    create_dir: bool,
    publish: impl FnOnce(&Path, &Path) -> std::io::Result<()>,
) -> SuccessTuple {
    let Some(dir) = path.parent() else {
        return SuccessTuple::fail("Invalid properties path.");
    };
    if !create_dir && !dir.is_dir() {
        return SuccessTuple::fail("The job's directory does not exist.");
    }
    (|| -> std::io::Result<()> {
        if create_dir {
            fs::create_dir_all(dir)?;
        }
        let staged = dir.join(format!(
            ".properties-{}-{}.tmp",
            std::process::id(),
            random_alnum(16)
        ));
        let permissions = match fs::metadata(path) {
            Ok(metadata) => Some(metadata.permissions()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        // Exclusive creation ensures a failed writer never truncates or removes another
        // writer's staging file. Nothing below recreates a directory deleted during the write.
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        if let Some(permissions) = &permissions {
            use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
            options.mode(permissions.mode() & 0o7777);
        }
        let mut file = options.open(&staged)?;
        let result = (|| {
            if let Some(permissions) = permissions {
                // Restore any bits cleared by umask before writing content. New files retain
                // OpenOptions' ordinary defaults, as fs::write used before atomic publication.
                file.set_permissions(permissions)?;
            }
            file.write_all(dumps(&Value::Map(props.clone())).as_bytes())?;
            drop(file);
            publish(&staged, path)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&staged);
        }
        result
    })()
    .map(|_| SuccessTuple::success())
    .unwrap_or_else(|e| SuccessTuple::fail(e.to_string()))
}

/// The properties whose value on disk always wins over an in-memory copy: who a job belongs to
/// (see [`meerschaum_core::job_access`]). Only the API server, the console and a team rename or
/// deletion write them, so a supervisor (or any `Daemon`) holding older properties never puts a
/// renamed or deleted team back.
pub const FILE_OWNED_KEYS: [&str; 2] = [
    meerschaum_core::job_access::OWNER_KEY,
    meerschaum_core::job_access::TEAM_KEY,
];

/// The getter `Daemon.properties`: merge the file's properties into the in-memory ones.
///
/// - an empty in-memory dict takes the file's properties;
/// - an in-memory `result` of `None` yields to a non-`None` result on disk;
/// - then the file's properties are patched with the in-memory ones (in-memory wins), except
///   for [`FILE_OWNED_KEYS`], whose value in the file wins (v5).
pub fn merge_properties(memory: &Map, file: Option<&Map>) -> Map {
    let file_props = file.cloned().unwrap_or_default();
    let mut mem = if memory.is_empty() {
        file_props.clone()
    } else {
        memory.clone()
    };
    let mem_result_none = mem.get("result").is_none_or(Value::is_null);
    let file_result_some = file_props.get("result").is_some_and(|v| !v.is_null());
    if mem_result_none && file_result_some {
        mem.shift_remove("result");
    }
    let mut merged = apply_patch_maps(&file_props, &mem);
    for key in FILE_OWNED_KEYS {
        if let Some(value) = file_props.get(key) {
            merged.insert(key.to_string(), value.clone());
        }
    }
    merged
}

/// `.stop.json`: when and how a job was manually stopped.
#[derive(Debug, Clone, PartialEq)]
pub struct StopFile {
    /// `stop_time`: an aware UTC isoformat string.
    pub stop_time: Option<String>,
    /// `action`: `quit`, `kill` or `pause`.
    pub action: Option<String>,
}

/// `Daemon._write_stop_file(action)`.
pub fn write_stop_file(path: &Path, action: &str) -> SuccessTuple {
    if !matches!(action, "quit" | "kill" | "pause") {
        return SuccessTuple::fail(format!("Unsupported action '{action}'."));
    }
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let mut data = Map::new();
    data.insert(
        "stop_time".into(),
        Value::Str(crate::pytime::aware_isoformat(crate::pytime::utcnow())),
    );
    data.insert("action".into(), Value::Str(action.to_string()));
    match fs::write(path, dumps(&Value::Map(data))) {
        Ok(()) => SuccessTuple::success(),
        Err(e) => SuccessTuple::fail(e.to_string()),
    }
}

/// `Daemon._read_stop_file()`: `{}` (here `None`) when missing or unreadable.
pub fn read_stop_file(path: &Path) -> Option<Map> {
    if !path.exists() {
        return None;
    }
    match Value::from_json_str(&fs::read_to_string(path).ok()?) {
        Ok(Value::Map(m)) => Some(m),
        _ => None,
    }
}

/// `Daemon._remove_stop_file()`.
pub fn remove_stop_file(path: &Path) -> SuccessTuple {
    if !path.exists() {
        return SuccessTuple::ok("Stop file does not exist.");
    }
    match fs::remove_file(path) {
        Ok(()) => SuccessTuple::success(),
        Err(e) => SuccessTuple::fail(format!("Failed to remove stop file:\n{e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::mpsc::{self, SyncSender};
    use std::thread::JoinHandle;
    use std::time::Duration;

    fn map(json: &str) -> Map {
        Value::from_json_str(json).unwrap().to_map()
    }

    #[test]
    fn merge_semantics() {
        let file = map(r#"{"label": "x", "result": [true, "Success"], "process": {"began": "a"}}"#);
        let mem = map(r#"{"label": "x", "result": null, "process": {"began": "b"}}"#);
        let merged = merge_properties(&mem, Some(&file));
        assert_eq!(
            dumps(&Value::Map(merged)),
            r#"{"label": "x", "result": [true, "Success"], "process": {"began": "b"}}"#
        );
        let merged = merge_properties(&Map::new(), Some(&file));
        assert_eq!(merged, file);
    }

    #[test]
    fn the_file_owns_who_a_job_belongs_to() {
        // A team renamed on disk while a supervisor held the old one in memory.
        let file = map(r#"{"label": "x", "owner": "alice", "team": "Analytics"}"#);
        let mem = map(r#"{"label": "y", "owner": "alice", "team": "Data"}"#);
        let merged = merge_properties(&mem, Some(&file));
        assert_eq!(
            dumps(&Value::Map(merged)),
            r#"{"label": "y", "owner": "alice", "team": "Analytics"}"#
        );
        // Moved to Everyone (`null`), and a new job's record before its first write.
        let file = map(r#"{"team": null}"#);
        let merged = merge_properties(&mem, Some(&file));
        assert_eq!(merged.get("team"), Some(&Value::Null));
        let merged = merge_properties(&mem, None);
        assert_eq!(merged.get("team").and_then(Value::as_str), Some("Data"));
    }

    #[test]
    fn write_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("jobs").join("x").join("properties.json");
        let props = map(r#"{"label": "é", "result": null}"#);
        assert!(write_properties(&path, &props).success);
        assert!(write_properties(&path, &props).success);
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            "{\"label\": \"\\u00e9\", \"result\": null}"
        );
        assert_eq!(fs::read_dir(path.parent().unwrap()).unwrap().count(), 1);
        assert_eq!(read_properties(&path), Some(props));
    }

    #[test]
    fn existing_writes_never_create_the_directory() {
        let dir = tempfile::tempdir().unwrap();
        let job_dir = dir.path().join("jobs").join("gone");
        let path = job_dir.join("properties.json");
        let props = map(r#"{"label": "x"}"#);
        assert!(!write_existing_properties(&path, &props).success);
        assert!(!job_dir.exists());
        fs::create_dir_all(&job_dir).unwrap();
        assert!(write_existing_properties(&path, &props).success);
        assert_eq!(read_properties(&path), Some(props));
    }

    fn paused_writer(
        path: PathBuf,
        props: Map,
    ) -> (JoinHandle<SuccessTuple>, PathBuf, SyncSender<()>) {
        let (staged_tx, staged_rx) = mpsc::sync_channel(0);
        let (resume_tx, resume_rx) = mpsc::sync_channel(0);
        let writer = std::thread::spawn(move || {
            write_properties_with(&path, &props, false, |from, to| {
                staged_tx
                    .send(from.to_path_buf())
                    .map_err(std::io::Error::other)?;
                resume_rx
                    .recv_timeout(Duration::from_secs(10))
                    .map_err(std::io::Error::other)?;
                fs::rename(from, to)
            })
        });
        let staged = staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        (writer, staged, resume_tx)
    }

    #[test]
    fn readers_and_job_listings_keep_the_complete_file_until_publication() {
        let dir = tempfile::tempdir().unwrap();
        let paths = crate::config::JobPaths::for_root(dir.path(), dir.path());
        let path = paths.jobs_dir.join("x/properties.json");
        let previous = map(r#"{"owner":"alice","team":"Private","result":null}"#);
        let next = map(
            r#"{"owner":"alice","team":"Private","result":[true,"é"],"process":{"ended":"now"}}"#,
        );
        assert!(write_properties(&path, &previous).success);
        let previous_bytes = fs::read(&path).unwrap();

        let (writer, staged, resume) = paused_writer(path.clone(), next.clone());
        assert_eq!(staged.parent(), path.parent());
        assert_eq!(read_properties(&staged), Some(next.clone()));
        for _ in 0..64 {
            assert!(path.exists());
            assert_eq!(fs::read(&path).unwrap(), previous_bytes);
            assert_eq!(read_properties(&path), Some(previous.clone()));
            assert_eq!(crate::daemon::get_daemon_ids(&paths), ["x"]);
        }
        resume.send(()).unwrap();
        let result = writer.join().unwrap();
        assert!(result.success, "{result}");
        assert_eq!(read_properties(&path), Some(next));
        assert_eq!(crate::daemon::get_daemon_ids(&paths), ["x"]);
        assert!(!staged.exists());
        assert_eq!(fs::read_dir(path.parent().unwrap()).unwrap().count(), 1);
    }

    #[test]
    fn failed_publication_preserves_the_old_file_and_unrelated_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("properties.json");
        let previous = map(r#"{"owner":"alice","result":[true,"previous"]}"#);
        assert!(write_properties(&path, &previous).success);
        let previous_bytes = fs::read(&path).unwrap();
        let unrelated = dir.path().join(".other-writer.tmp");
        fs::write(&unrelated, "untouched").unwrap();
        let mut staged = None;
        let result = write_properties_with(
            &path,
            &map(r#"{"result":[false,"new"]}"#),
            false,
            |from, _| {
                staged = Some(from.to_path_buf());
                assert_eq!(read_properties(&path), Some(previous.clone()));
                Err(std::io::Error::other("injected publication failure"))
            },
        );
        assert!(!result.success);
        assert!(result.message.contains("injected publication failure"));
        assert_eq!(fs::read(&path).unwrap(), previous_bytes);
        assert_eq!(read_properties(&path), Some(previous));
        assert!(!staged.unwrap().exists());
        assert_eq!(fs::read_to_string(unrelated).unwrap(), "untouched");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn staged_and_published_properties_preserve_restrictive_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("properties.json");
        let previous = map(r#"{"owner":"alice","result":"previous"}"#);
        let next = map(r#"{"owner":"alice","result":"next"}"#);
        assert!(write_properties(&path, &previous).success);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let result = write_properties_with(&path, &next, false, |from, to| {
            assert_eq!(fs::metadata(from)?.permissions().mode() & 0o777, 0o600);
            assert_eq!(fs::metadata(to)?.permissions().mode() & 0o777, 0o600);
            fs::rename(from, to)
        });
        assert!(result.success, "{result}");
        assert_eq!(read_properties(&path), Some(next));
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn concurrent_writers_publish_only_their_own_complete_staging_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("properties.json");
        let previous = map(r#"{"result":"previous"}"#);
        let first = map(r#"{"result":"first","env":{"unicode":"é"}}"#);
        let second = map(r#"{"result":"second","process":{"began":"now"}}"#);
        assert!(write_properties(&path, &previous).success);
        let (writer_a, staged_a, resume_a) = paused_writer(path.clone(), first.clone());
        let (writer_b, staged_b, resume_b) = paused_writer(path.clone(), second.clone());
        assert_ne!(staged_a, staged_b);
        assert_eq!(read_properties(&staged_a), Some(first.clone()));
        assert_eq!(read_properties(&staged_b), Some(second.clone()));
        assert_eq!(read_properties(&path), Some(previous));

        resume_a.send(()).unwrap();
        let result = writer_a.join().unwrap();
        assert!(result.success, "{result}");
        assert_eq!(read_properties(&path), Some(first));
        assert_eq!(read_properties(&staged_b), Some(second.clone()));
        resume_b.send(()).unwrap();
        let result = writer_b.join().unwrap();
        assert!(result.success, "{result}");
        assert_eq!(read_properties(&path), Some(second));
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn a_deleted_job_is_not_recreated_or_replaced_by_a_pending_write() {
        for recreate in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let job_dir = dir.path().join("jobs/x");
            let path = job_dir.join("properties.json");
            assert!(write_properties(&path, &map(r#"{"owner":"old"}"#)).success);
            let replacement = map(r#"{"owner":"new","result":"new registration"}"#);
            let (writer, _, resume) =
                paused_writer(path.clone(), map(r#"{"owner":"old","result":"finished"}"#));
            fs::remove_dir_all(&job_dir).unwrap();
            if recreate {
                assert!(write_properties(&path, &replacement).success);
            }
            resume.send(()).unwrap();
            let result = writer.join().unwrap();
            assert!(!result.success);
            if recreate {
                assert_eq!(read_properties(&path), Some(replacement));
                assert_eq!(fs::read_dir(&job_dir).unwrap().count(), 1);
            } else {
                assert!(!job_dir.exists());
            }
        }
    }
}
