//! `properties.json` and `.stop.json`: reading, writing (with Python's backup-rename dance) and
//! the cascading merge `Daemon.properties` performs on every access.

use std::fs;
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

/// `Daemon.write_properties()`: move the existing file to `<random 8>.json`, write the new one,
/// then delete the backup (or restore it if writing failed).
pub fn write_properties(path: &Path, props: &Map) -> SuccessTuple {
    write_properties_with(path, props, true)
}

/// [`write_properties`] into a job directory that must still exist: a directory that was deleted
/// (the job was deleted while its supervisor ran) is never created again, and the write fails.
pub fn write_existing_properties(path: &Path, props: &Map) -> SuccessTuple {
    write_properties_with(path, props, false)
}

fn write_properties_with(path: &Path, props: &Map, create_dir: bool) -> SuccessTuple {
    let Some(dir) = path.parent() else {
        return SuccessTuple::fail("Invalid properties path.");
    };
    if !create_dir && !dir.is_dir() {
        return SuccessTuple::fail("The job's directory does not exist.");
    }
    let backup = dir.join(format!("{}.json", random_alnum(8)));
    let mut result = (|| -> std::io::Result<()> {
        if create_dir {
            fs::create_dir_all(dir)?;
        }
        if path.exists() {
            fs::rename(path, &backup)?;
        }
        fs::write(path, dumps(&Value::Map(props.clone())))?;
        Ok(())
    })()
    .map(|_| SuccessTuple::success())
    .unwrap_or_else(|e| SuccessTuple::fail(e.to_string()));
    if backup.exists() {
        let cleanup = if result.success {
            fs::remove_file(&backup)
        } else {
            fs::rename(&backup, path)
        };
        if let Err(e) = cleanup {
            result = SuccessTuple::fail(e.to_string());
        }
    }
    result
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
}
