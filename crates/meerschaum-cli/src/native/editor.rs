//! The one way an action opens a file in the user's editor (`meerschaum.utils.misc.edit_file`):
//! `$VISUAL`, else `$EDITOR`, else `vi`, started directly (an argument vector, never a shell;
//! `EDITOR="code -w"` is split into words). v4 falls back to installing and running `pyvim` when
//! the editor cannot be started; v5 reports it instead, so the user sees why nothing opened.
//!
//! Used by `edit plugins`, `bootstrap plugins`, `edit definition|pipes` and `edit config`.

use std::path::Path;

use crate::dispatch::{CommandSpec, Runtime};

/// What a restricted web terminal says instead of opening an editor.
pub const EDITOR_UNAVAILABLE: &str = "This terminal does not open an editor. Pass the new values \
     with `--params` / `-P` (for example `edit pipes -c sql:main -m weather -P '{\"tags\": [\"daily\"]}'`), \
     or use the web console's editor.";

/// The editor command: `$VISUAL`, else `$EDITOR` (ignoring blanks), else `vi`.
pub fn editor_command(rt: &mut Runtime<'_>) -> (String, Vec<String>) {
    let configured = ["VISUAL", "EDITOR"]
        .iter()
        .find_map(|var| (rt.env)(var).filter(|v| !v.trim().is_empty()))
        .unwrap_or_else(|| "vi".into());
    match crate::shlex::split(&configured) {
        Ok(mut words) if !words.is_empty() => {
            let program = words.remove(0);
            (program, words)
        }
        _ => (configured, Vec::new()),
    }
}

/// Open `path` in the editor and wait for it. `Ok(true)` when the editor exited with 0; `Err` with
/// the message to show when it could not be started at all.
pub fn edit_file(rt: &mut Runtime<'_>, path: &Path) -> Result<bool, String> {
    // A restricted web terminal opens no editor (an editor runs commands and writes files).
    if crate::shell::restricted::active() {
        return Err(EDITOR_UNAVAILABLE.to_string());
    }
    let (program, mut args) = editor_command(rt);
    args.push(path.to_string_lossy().into_owned());
    let spec = CommandSpec {
        scrub_secrets: true,
        program: program.clone(),
        args,
        ..CommandSpec::default()
    };
    match rt.host.run(&spec) {
        Ok(code) => Ok(code == 0),
        Err(e) => Err(format!(
            "Unable to open editor '{program}' for '{}': {e}\n    Set `$EDITOR` to an editor that is installed.",
            path.display()
        )),
    }
}
