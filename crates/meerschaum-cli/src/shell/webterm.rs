//! Read-only readiness for the web terminal, independent of tmux's escape filtering.
use std::path::PathBuf;
use std::sync::{LazyLock, Mutex};

static STATE_PATH: LazyLock<Option<PathBuf>> = LazyLock::new(|| {
    std::env::var_os("MRSM_WEBTERM_STATE_DIR")
        .map(|dir| PathBuf::from(dir).join(format!("{}.json", std::process::id())))
});
static LAST_STATE: Mutex<Option<(bool, bool)>> = Mutex::new(None);
static PROCESS_START: LazyLock<String> = LazyLock::new(|| {
    std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|text| {
            text.rsplit_once(") ")
                .and_then(|(_, fields)| fields.split_whitespace().nth(19))
                .map(str::to_string)
        })
        .unwrap_or_default()
});

/// Publish only editor state; never the command or buffer contents.
pub fn update(idle: bool, empty: bool) {
    let Some(path) = STATE_PATH.as_ref() else {
        return;
    };
    let mut last = LAST_STATE.lock().unwrap_or_else(|e| e.into_inner());
    if *last == Some((idle, empty)) {
        return;
    }
    let data = serde_json::json!({"idle": idle, "empty": empty, "pid": std::process::id(), "start": *PROCESS_START});
    let temporary = path.with_extension("pending");
    if std::fs::write(&temporary, data.to_string()).is_ok()
        && std::fs::rename(&temporary, path).is_ok()
    {
        *last = Some((idle, empty));
    }
}

/// Remove readiness when this shell exits normally.
pub struct Guard;
impl Drop for Guard {
    fn drop(&mut self) {
        if let Some(path) = STATE_PATH.as_ref() {
            let _ = std::fs::remove_file(path);
        }
    }
}
