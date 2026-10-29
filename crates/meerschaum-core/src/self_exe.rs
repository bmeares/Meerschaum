//! How this process starts another copy of `mrsm`.
//!
//! The standalone `mrsm` binary (`cargo install meerschaum`, release archives, Docker) starts
//! itself with `current_exe()`. When the CLI runs inside the Python extension (`pip install
//! meerschaum`: the `mrsm` console script calls `_native.main`), `current_exe()` is the Python
//! interpreter, so the host registers the command that re-runs the CLI instead
//! (`[sys.executable, "-P", "-m", "meerschaum"]`). Every place that re-launches Meerschaum (job
//! supervisors and workers, `systemd` units, compose, pipeline steps, the web terminal, the GUI
//! server) asks [`self_command`], so jobs run from both kinds of install.

use std::sync::{OnceLock, RwLock};

static OVERRIDE: RwLock<Option<Vec<String>>> = RwLock::new(None);

/// The module arguments an interpreter needs to run the CLI: `python -m meerschaum`.
pub const PYTHON_MODULE_ARGS: [&str; 2] = ["-m", "meerschaum"];

/// The interpreter flag every Meerschaum-started Python process gets (Python 3.11+, which the
/// package requires): `-P` keeps the working directory off `sys.path`. `-m` and `-c` put it first, and
/// jobs, compose steps, pipeline steps, workers and the API host run in the user's directory, where
/// a `meerschaum.py` or a `meerschaum/` would replace the installed package. The `mrsm` console
/// script never had the directory on the path (v4: `sys.path[0]` is the scripts directory).
pub const PYTHON_SAFE_PATH_FLAG: &str = "-P";

/// The environment variable the Python launcher sets (to the path) when it replaces itself with the
/// standalone binary `MRSM_BIN` names. A Python process which sees it with the same `MRSM_BIN`
/// has been started *by* that "binary", which can only be a launcher for the console script
/// (pip's `mrsm.exe` on Windows): it ignores `MRSM_BIN` instead of starting itself again forever.
pub const BIN_GUARD_ENV: &str = "MRSM_BIN_EXEC";

/// `[interpreter, "-P", "-m", "meerschaum"]`: the command which runs this CLI in Python.
pub fn python_command(interpreter: &str) -> Vec<String> {
    let mut command = vec![interpreter.to_string(), PYTHON_SAFE_PATH_FLAG.to_string()];
    command.extend(PYTHON_MODULE_ARGS.map(String::from));
    command
}

/// Whether `path` names a Python interpreter (`python`, `python3.13`, `python.exe`, `pypy3`, ...)
/// and not the program embedding Python (`uwsgi`, `httpd`, a frozen application), whose
/// `sys.executable` it can be.
pub fn is_python_executable(path: &str) -> bool {
    let name = path
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    let stem = name.strip_suffix(".exe").unwrap_or(&name);
    stem.starts_with("python") || stem.starts_with("pypy")
}

/// The interpreter which runs the CLI again: `sys.executable` when it is a Python, else
/// `sys._base_executable` when that is, else `None`.
pub fn pick_interpreter(executable: &str, base_executable: Option<&str>) -> Option<String> {
    [Some(executable), base_executable]
        .into_iter()
        .flatten()
        .find(|p| is_python_executable(p))
        .map(str::to_string)
}

/// Whether `MRSM_BIN` (`bin`) must be ignored because the guard names it too (see
/// [`BIN_GUARD_ENV`]).
pub fn bin_is_guarded(bin: &str, guard: Option<&str>) -> bool {
    guard.is_some_and(|g| !g.is_empty() && g == bin)
}

/// Register the command which re-runs this program (replaces an earlier registration). An empty
/// command clears it.
pub fn set_self_command(command: Vec<String>) {
    *OVERRIDE.write().unwrap_or_else(|e| e.into_inner()) = (!command.is_empty()).then_some(command);
}

/// Register `command` unless one is already set; returns whether it was installed.
pub fn set_self_command_if_unset(command: Vec<String>) -> bool {
    let mut slot = OVERRIDE.write().unwrap_or_else(|e| e.into_inner());
    if slot.is_some() || command.is_empty() {
        return false;
    }
    *slot = Some(command);
    true
}

/// The command (program and leading arguments) which runs this CLI again: what the host
/// registered, else the current executable, else `mrsm` (resolved through `PATH`).
pub fn self_command() -> Vec<String> {
    if let Some(command) = OVERRIDE.read().unwrap_or_else(|e| e.into_inner()).clone() {
        return command;
    }
    vec![current_exe_string()]
}

/// `self_command()` as `(program, leading arguments)`.
pub fn self_program() -> (String, Vec<String>) {
    let mut command = self_command();
    let program = command.remove(0);
    (program, command)
}

/// Whether the registered command runs the CLI through a Python interpreter (`-m meerschaum`).
pub fn runs_in_python() -> bool {
    OVERRIDE
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .as_deref()
        .is_some_and(ends_with_module_args)
}

/// Whether `command` ends with `-m meerschaum`.
pub fn ends_with_module_args(command: &[String]) -> bool {
    command.len() >= 3 && command[command.len() - 2..] == PYTHON_MODULE_ARGS
}

/// The program of a command without a trailing `-m meerschaum`: what a `systemd` unit's
/// `ExecStart=` names before ` -m meerschaum <sysargs>` (v4 pins `sys.executable`).
pub fn without_module_args(command: &[String]) -> Vec<String> {
    if ends_with_module_args(command) {
        command[..command.len() - 2].to_vec()
    } else {
        command.to_vec()
    }
}

fn current_exe_string() -> String {
    static EXE: OnceLock<String> = OnceLock::new();
    EXE.get_or_init(|| {
        std::env::current_exe()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| "mrsm".to_string())
    })
    .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn module_arguments_are_detected_and_stripped() {
        let python = v(&["/usr/bin/python3", "-m", "meerschaum"]);
        assert!(ends_with_module_args(&python));
        assert_eq!(without_module_args(&python), v(&["/usr/bin/python3"]));
        let binary = v(&["/usr/bin/mrsm"]);
        assert!(!ends_with_module_args(&binary));
        assert_eq!(without_module_args(&binary), binary);
        // A bare `-m meerschaum` has no program to strip to.
        assert!(!ends_with_module_args(&v(&["-m", "meerschaum"])));
    }

    #[test]
    fn the_python_command_keeps_the_working_directory_off_the_path() {
        let command = python_command("/usr/bin/python3");
        assert_eq!(command, v(&["/usr/bin/python3", "-P", "-m", "meerschaum"]));
        assert!(ends_with_module_args(&command));
        assert_eq!(
            without_module_args(&command),
            v(&["/usr/bin/python3", "-P"])
        );
    }

    #[test]
    fn only_a_python_interpreter_runs_the_cli_again() {
        for yes in [
            "/usr/bin/python3",
            "/venv/bin/python3.13",
            "C:\\Python313\\python.exe",
            "python",
            "/opt/pypy3.11/bin/pypy3",
        ] {
            assert!(is_python_executable(yes), "{yes}");
        }
        for no in [
            "/usr/sbin/uwsgi",
            "/usr/sbin/httpd",
            "",
            "C:\\app\\app.exe",
            "/bin/mrsm",
        ] {
            assert!(!is_python_executable(no), "{no}");
        }
        assert_eq!(
            pick_interpreter("/usr/bin/python3", None).as_deref(),
            Some("/usr/bin/python3")
        );
        // A host program: fall back to the interpreter the build was made from.
        assert_eq!(
            pick_interpreter("/usr/sbin/uwsgi", Some("/usr/bin/python3.12")).as_deref(),
            Some("/usr/bin/python3.12")
        );
        assert_eq!(
            pick_interpreter("/usr/sbin/uwsgi", Some("/usr/sbin/uwsgi")),
            None
        );
        assert_eq!(pick_interpreter("", None), None);
    }

    #[test]
    fn the_guard_blocks_only_the_binary_it_names() {
        assert!(bin_is_guarded(
            "/venv/Scripts/mrsm.exe",
            Some("/venv/Scripts/mrsm.exe")
        ));
        assert!(!bin_is_guarded("/venv/Scripts/mrsm.exe", None));
        assert!(!bin_is_guarded("/venv/Scripts/mrsm.exe", Some("")));
        assert!(!bin_is_guarded("/opt/mrsm", Some("/venv/Scripts/mrsm.exe")));
    }

    #[test]
    fn a_registered_command_wins_over_the_executable() {
        // One test mutates the process-wide slot, so everything lives here.
        set_self_command(Vec::new());
        assert_eq!(self_command().len(), 1);
        assert!(!runs_in_python());
        assert!(set_self_command_if_unset(python_command("py")));
        assert!(!set_self_command_if_unset(v(&["other"])));
        assert_eq!(self_command(), v(&["py", "-P", "-m", "meerschaum"]));
        assert!(runs_in_python());
        assert_eq!(
            self_program(),
            ("py".to_string(), v(&["-P", "-m", "meerschaum"]))
        );
        set_self_command(Vec::new());
        assert!(!runs_in_python());
    }
}
