//! `_native.main(argv)`: the whole `mrsm` command line inside the extension.
//!
//! `pip install meerschaum` installs one wheel. Its `mrsm` console script (and `python -m
//! meerschaum`) call [`main`], which runs the same program as the standalone binary
//! (`meerschaum_cli::run_cli`): the reedline shell, every native action, `start api`, the job
//! supervisor. It writes to the real file descriptors (not Python's streams: that is
//! `_core.entry`'s job), reads the terminal directly and returns the exit code.
//!
//! Processes the CLI starts again (job supervisors and workers, compose steps, `systemd` units,
//! the web terminal) are started as `<sys.executable> -P -m meerschaum` ([`ensure_self_command`]),
//! so they run from this extension too. `MRSM_BIN` makes them use a standalone binary instead.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyList;

/// The command which runs this CLI again from this process: the standalone binary named by
/// `MRSM_BIN` when it is an executable program (and not the launcher this very process was started
/// from, see [`meerschaum_core::self_exe::BIN_GUARD_ENV`]), else `<python> -P -m meerschaum`.
///
/// `<python>` is `sys.executable` when that is a Python interpreter, else `sys._base_executable`
/// when that is (under mod_wsgi or uWSGI `sys.executable` is the host program, and `uwsgi -m
/// meerschaum` would start something else). With neither there is no such command: it is an error,
/// not a guess.
fn python_self_command(py: Python<'_>) -> PyResult<Vec<String>> {
    let guard = std::env::var(meerschaum_core::self_exe::BIN_GUARD_ENV).ok();
    if let Some(bin) = std::env::var_os("MRSM_BIN").filter(|b| !b.is_empty()) {
        let guarded =
            meerschaum_core::self_exe::bin_is_guarded(&bin.to_string_lossy(), guard.as_deref());
        let path = std::path::PathBuf::from(bin);
        if !guarded && is_program(&path) {
            return Ok(vec![path.to_string_lossy().into_owned()]);
        }
    }
    let sys = py.import("sys")?;
    let exe: String = sys.getattr("executable")?.extract()?;
    let base: Option<String> = sys
        .getattr("_base_executable")
        .ok()
        .and_then(|b| b.extract().ok());
    let Some(python) = meerschaum_core::self_exe::pick_interpreter(&exe, base.as_deref()) else {
        return Err(PyRuntimeError::new_err(format!(
            "Cannot start another Meerschaum process: `sys.executable` ({exe:?}) is not a Python \
             interpreter (an embedded Python, such as mod_wsgi or uWSGI). Set `MRSM_BIN` to a \
             standalone `mrsm` binary (`cargo install meerschaum`)."
        )));
    };
    Ok(meerschaum_core::self_exe::python_command(&python))
}

/// An executable file which is a program, not a script (a console script named `mrsm` would run
/// `python -m meerschaum` and so start itself again).
fn is_program(path: &std::path::Path) -> bool {
    #[cfg(unix)]
    fn executable(path: &std::path::Path) -> bool {
        use std::os::unix::fs::PermissionsExt;
        path.metadata()
            .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    fn executable(path: &std::path::Path) -> bool {
        path.is_file()
    }
    if !executable(path) {
        return false;
    }
    let mut magic = [0u8; 2];
    std::fs::File::open(path)
        .and_then(|mut f| std::io::Read::read_exact(&mut f, &mut magic))
        .map(|()| &magic != b"#!")
        .unwrap_or(false)
}

/// Register how this process starts another `mrsm` (see [`meerschaum_core::self_exe`]); cheap,
/// and re-evaluated on every call so that a changed `MRSM_BIN` is seen.
pub fn ensure_self_command(py: Python<'_>) -> PyResult<()> {
    meerschaum_core::self_exe::set_self_command(python_self_command(py)?);
    Ok(())
}

/// The OS-level handlers of SIGINT and SIGTERM which the CLI's runtime installed in an earlier run.
///
/// tokio registers its signal handlers with the OS once per process. Python's own
/// `signal.signal(SIGINT, ...)` on return replaces SIGINT's for good, and SIGTERM's would stay
/// installed with nobody listening (a SIGTERM swallowed in the caller). So a run saves the
/// handlers when it ends and puts the caller's actions back exactly (`SIG_DFL` included); the next
/// run reinstalls the saved ones, so its runtime's `ctrl_c()` fires again and a Ctrl-C stops only
/// the CLI.
#[cfg(unix)]
static CLI_ACTIONS: std::sync::Mutex<Vec<(libc::c_int, libc::sigaction)>> =
    std::sync::Mutex::new(Vec::new());

/// The current OS-level action of `signal`.
#[cfg(unix)]
fn os_action(signal: libc::c_int) -> Option<libc::sigaction> {
    // SAFETY: `sigaction` with a null new action only reads the current one into the zeroed struct.
    unsafe {
        let mut current: libc::sigaction = std::mem::zeroed();
        (libc::sigaction(signal, std::ptr::null(), &mut current) == 0).then_some(current)
    }
}

/// Install an OS-level action for `signal`.
#[cfg(unix)]
fn set_os_action(signal: libc::c_int, action: &libc::sigaction) {
    // SAFETY: `action` was read from `sigaction(2)` for this signal earlier in this process, or is
    // zeroed with `SIG_DFL` (the default action).
    unsafe {
        libc::sigaction(signal, action, std::ptr::null_mut());
    }
}

/// The default action of `signal`.
#[cfg(unix)]
fn default_action() -> libc::sigaction {
    // SAFETY: an all-zero `sigaction` is `SIG_DFL` with no flags and an empty mask.
    unsafe { std::mem::zeroed() }
}

/// Whether `action` is a handler (neither the default action nor ignore).
#[cfg(unix)]
fn is_handler(action: &libc::sigaction) -> bool {
    action.sa_sigaction != libc::SIG_DFL && action.sa_sigaction != libc::SIG_IGN
}

/// Puts the process's signal handling and `MRSM_PYTHON` back when the CLI returns to a Python
/// caller which keeps running (`Shell.cmdloop()` from a REPL).
struct Restore<'py> {
    py: Python<'py>,
    sigint: Option<Py<PyAny>>,
    /// The caller's OS-level action of SIGTERM (SIGINT's is Python's, put back through `signal`).
    #[cfg(unix)]
    sigterm: Option<libc::sigaction>,
    python_env_set: bool,
}

impl<'py> Restore<'py> {
    /// Make the process behave like the standalone binary: SIGINT has its default action (the
    /// handlers the CLI installs, such as tokio's, replace it), and workers use this
    /// interpreter unless `MRSM_PYTHON` says otherwise.
    fn enter(py: Python<'py>) -> Restore<'py> {
        let mut sigint = None;
        if let Ok(signal) = py.import("signal") {
            let swap = || -> PyResult<Option<Py<PyAny>>> {
                let number = signal.getattr("SIGINT")?;
                let current = signal.call_method1("getsignal", (&number,))?;
                // Only Python's own `KeyboardInterrupt` handler is replaced: an inherited
                // `SIG_IGN` (a job started in the background) stays, as in the binary.
                if current.is(&signal.getattr("default_int_handler")?) {
                    signal.call_method1("signal", (number, signal.getattr("SIG_DFL")?))?;
                    return Ok(Some(current.unbind()));
                }
                Ok(None)
            };
            // `signal.signal` only works in the main thread of the main interpreter.
            sigint = swap().ok().flatten();
        }
        #[cfg(unix)]
        let sigterm = {
            let saved = CLI_ACTIONS
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            let action_of = |signal| {
                saved
                    .iter()
                    .find(|(number, _)| *number == signal)
                    .map(|(_, action)| *action)
            };
            // The runtime of this run registers with tokio's existing handlers again, else starts
            // from the default action (SIGINT's was just set by `signal.signal`).
            if sigint.is_some()
                && let Some(action) = action_of(libc::SIGINT)
            {
                set_os_action(libc::SIGINT, &action);
            }
            let caller = os_action(libc::SIGTERM);
            set_os_action(
                libc::SIGTERM,
                &action_of(libc::SIGTERM).unwrap_or_else(default_action),
            );
            caller
        };
        let mut python_env_set = false;
        let _ = (|| -> PyResult<()> {
            let sys_executable: String = py.import("sys")?.getattr("executable")?.extract()?;
            let environ = py.import("os")?.getattr("environ")?;
            let unset = environ
                .call_method1("get", ("MRSM_PYTHON", ""))?
                .extract::<String>()?
                .trim()
                .is_empty();
            if unset && !sys_executable.is_empty() {
                environ.set_item("MRSM_PYTHON", sys_executable)?;
                python_env_set = true;
            }
            Ok(())
        })();
        Restore {
            py,
            sigint,
            #[cfg(unix)]
            sigterm,
            python_env_set,
        }
    }
}

impl Drop for Restore<'_> {
    fn drop(&mut self) {
        let py = self.py;
        #[cfg(unix)]
        {
            // The handlers the CLI's runtime installed during this run: keep them for the next
            // one, then give the caller its own SIGTERM action back (SIGINT's is below).
            let mut ours = CLI_ACTIONS.lock().unwrap_or_else(|e| e.into_inner());
            let mut keep = |signal: libc::c_int| {
                if let Some(current) = os_action(signal)
                    && is_handler(&current)
                {
                    ours.retain(|(number, _)| *number != signal);
                    ours.push((signal, current));
                }
            };
            if self.sigint.is_some() {
                keep(libc::SIGINT);
            }
            if let Some(caller) = &self.sigterm {
                keep(libc::SIGTERM);
                set_os_action(libc::SIGTERM, caller);
            }
        }
        if let Some(handler) = self.sigint.take()
            && let Ok(signal) = py.import("signal")
            && let Ok(number) = signal.getattr("SIGINT")
        {
            let _ = signal.call_method1("signal", (number, handler));
        }
        if self.python_env_set
            && let Ok(environ) = py.import("os").and_then(|os| os.getattr("environ"))
        {
            let _ = environ.call_method1("pop", ("MRSM_PYTHON", py.None()));
        }
        crate::api_client_api::install_warning_sink();
    }
}

/// Run `mrsm <argv>` and return its exit code. The interpreter lock is released for the whole
/// run; nothing is written through Python's streams.
#[pyfunction]
pub fn main(py: Python<'_>, argv: Bound<'_, PyList>) -> PyResult<i32> {
    let argv: Vec<String> = argv
        .iter()
        .map(|a| a.extract::<String>().or_else(|_| Ok(a.str()?.to_string())))
        .collect::<PyResult<_>>()?;
    // The run swaps the process's signal actions and `set_embedded(false)` is process-wide: only
    // the main thread of the main interpreter may do that.
    let threading = py.import("threading")?;
    if !threading
        .call_method0("current_thread")?
        .is(&threading.call_method0("main_thread")?)
    {
        return Err(PyRuntimeError::new_err(
            "`_native.main` runs the whole `mrsm` command line and owns the process's signals: \
             call it from the main thread (use `meerschaum.entry()` from other threads).",
        ));
    }
    ensure_self_command(py)?;
    // A compose runner's in-process child (`__mrsm_entry_child__`): take its marker out of the
    // environment so the command's own children never inherit it.
    let os = py.import("os")?;
    let popped = os.getattr("environ")?.call_method1(
        "pop",
        (meerschaum_cli::dispatch::ENTRY_CHILD_ENV, py.None()),
    )?;
    let marker = if popped.is_none() {
        None
    } else {
        // The variable's bytes as the OS gave them (`os.fsencode` undoes `surrogateescape`).
        let bytes: Vec<u8> = os.call_method1("fsencode", (popped,))?.extract()?;
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            Some(std::ffi::OsString::from_vec(bytes))
        }
        #[cfg(not(unix))]
        {
            Some(std::ffi::OsString::from(
                String::from_utf8_lossy(&bytes).into_owned(),
            ))
        }
    };
    meerschaum_cli::set_entry_child_marker(marker);
    crate::logging::enter_cli_mode();
    let restore = Restore::enter(py);
    // The CLI owns the process's signals for the run (its first SIGINT / SIGTERM asks a long
    // action to stop, the second exits), as in the standalone binary; the extension's default is
    // that Python owns them.
    meerschaum_cli::native::jobs::set_embedded(false);
    let rc = py.detach(|| {
        let rc = meerschaum_cli::run_cli(&argv);
        let _ = std::io::Write::flush(&mut std::io::stdout());
        rc
    });
    meerschaum_cli::native::jobs::set_embedded(true);
    meerschaum_cli::native::jobs::release_stop_handler();
    crate::logging::leave_cli_mode();
    drop(restore);
    Ok(rc)
}
