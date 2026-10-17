//! Package installation into virtual environments (`meerschaum/utils/packages/__init__.py`
//! `pip_install`, `get_pip_install_plan`, `_get_pip_os_env`, `meerschaum/utils/venv/__init__.py`
//! `init_venv`, `venv_target_path`, and `meerschaum/utils/venv/_requests.py` `record_request`).
//!
//! This is the native half of `install packages` and `install required`: Rust owns the
//! orchestration (which installer, the venv, the arguments, the lock, the request ledger) and
//! only `uv` / `pip` / the `venv` module run as child processes. The interpreter is the one the
//! Python worker uses (`MRSM_PYTHON`, else `python3`), asked once for its version, `sys.executable`,
//! whether `pip` is importable and where `uv` lives.
//!
//! # Differences from v4
//!
//! - v4 bootstraps `uv` into the host interpreter (`pip install uv`, `--break-system-packages`)
//!   when `pip` exists and `uv` does not. v5 never changes the host interpreter: it uses `pip`.
//! - v4 bootstraps `wheel`/`setuptools`/`PyYAML` into a venv before a `pip` (not `uv`) install and
//!   updates `pip` itself first. Both run as v4 does when `pip` is the installer, except that
//!   the `pip` self-update (a PyPI query) is not made.
//! - `--prerelease=allow` is never added (the install-name table has no pre-release pins).
//! - `get_pip` (downloading `get-pip.py`) is not reproduced; the installer is reported missing.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Mutex, OnceLock};

use serde_json::{Value as Json, json};
use sha2::{Digest, Sha256};

use crate::dispatch::Runtime;

use super::venvs::{CORE_REQUESTER, REQUESTS_FILE_NAME, normalize};
use super::warn;

/// `_MRSM_PACKAGE_ARCHIVES_PREFIX`.
const PACKAGE_ARCHIVES_PREFIX: &str = "https://meerschaum.io/files/archives/wheels/";

/// The facts about the interpreter that runs plugins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Interpreter {
    /// `sys.executable`.
    pub executable: PathBuf,
    /// `sys.version_info.major`.
    pub major: u32,
    /// `sys.version_info.minor`.
    pub minor: u32,
    /// Whether `import pip` works.
    pub has_pip: bool,
    /// `uv.find_uv_bin()` when the `uv` module is importable.
    pub uv: Option<PathBuf>,
}

const PROBE: &str = "\
import sys
print(sys.executable)
print(sys.version_info[0])
print(sys.version_info[1])
try:
    import pip
    print(1)
except Exception:
    print(0)
try:
    import uv
    print(uv.find_uv_bin())
except Exception:
    print()
";

/// Ask `python` about itself.
pub fn probe_interpreter(python: &str) -> Result<Interpreter, String> {
    // `PYTHONSAFEPATH` keeps the current directory off `sys.path` (a `pip.py` there must not run);
    // unlike `-P` it is ignored by interpreters older than 3.11 instead of failing the probe.
    let output = Command::new(python)
        .args(["-c", PROBE])
        .env("PYTHONSAFEPATH", "1")
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .map_err(|e| format!("Unable to run the Python interpreter '{python}': {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "The Python interpreter '{python}' exited with {}.",
            output.status
        ));
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = text.lines().collect();
    let number = |i: usize| lines.get(i).and_then(|l| l.trim().parse::<u32>().ok());
    let (Some(major), Some(minor)) = (number(1), number(2)) else {
        return Err(format!("Unable to read the version of '{python}'."));
    };
    Ok(Interpreter {
        executable: PathBuf::from(lines.first().copied().unwrap_or(python).trim()),
        major,
        minor,
        has_pip: number(3) == Some(1),
        uv: lines
            .get(4)
            .map(|l| l.trim())
            .filter(|l| !l.is_empty())
            .map(PathBuf::from)
            .filter(|p| p.is_file()),
    })
}

/// `shutil.which(name)` over `path`.
pub fn which(name: &str, path: Option<&str>) -> Option<PathBuf> {
    let path = path?;
    std::env::split_paths(path)
        .map(|dir| dir.join(name))
        .find(|candidate| is_executable(candidate))
}

fn is_executable(path: &Path) -> bool {
    let Ok(meta) = std::fs::metadata(path) else {
        return false;
    };
    if !meta.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        meta.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

/// A Python `repr` of a list of strings (for `--debug` command echoes).
pub fn py_list(items: &[String]) -> String {
    let quoted: Vec<String> = items.iter().map(|s| crate::value::repr_str(s)).collect();
    format!("[{}]", quoted.join(", "))
}

/// The environment and directories package installs depend on, read once per action.
pub struct PipEnv {
    /// `VIRTENV_RESOURCES_PATH`.
    pub venvs_dir: PathBuf,
    /// `VENVS_CACHE_RESOURCES_PATH` (the creation locks).
    pub venv_locks_dir: PathBuf,
    /// `MRSM_PYTHON`, else `python3`.
    pub python: String,
    /// `system:experimental:uv_pip`.
    pub uv_enabled: bool,
    /// `MRSM_NO_AUTO_INSTALL` is not set: the installers may bootstrap themselves (`uv venv --seed`,
    /// `wheel`/`setuptools`/`PyYAML`/`uv` into a venv). The explicit install itself always runs.
    pub auto_install: bool,
    /// `MRSM_DAEMON_ID` is set: installs are silent.
    pub inside_daemon: bool,
    /// `PATH`.
    pub path: Option<String>,
    interpreter: OnceLock<Result<Interpreter, String>>,
}

impl PipEnv {
    /// Read the settings from the runtime.
    pub fn new(rt: &mut Runtime<'_>) -> PipEnv {
        let paths = rt.config().paths();
        let venvs_dir = paths
            .get("VIRTENV_RESOURCES_PATH")
            .map(|p| PathBuf::from(p.as_posix()))
            .unwrap_or_else(|| paths.root_dir.join("venvs"));
        let venv_locks_dir = paths
            .get("VENVS_CACHE_RESOURCES_PATH")
            .map(|p| PathBuf::from(p.as_posix()))
            .unwrap_or_else(|| paths.root_dir.join(".cache").join("venvs"));
        let uv_enabled = rt
            .config()
            .get(&["system", "experimental", "uv_pip"])
            .is_none_or(|v| v.truthy());
        let daemon_var = meerschaum_core::static_config()
            .environment
            .daemon_id
            .clone();
        let python = (rt.env)("MRSM_PYTHON")
            .filter(|p| !p.trim().is_empty())
            .unwrap_or_else(|| if cfg!(windows) { "python" } else { "python3" }.into());
        let auto_install = !(rt.env)("MRSM_NO_AUTO_INSTALL")
            .is_some_and(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"));
        PipEnv {
            auto_install,
            venvs_dir,
            venv_locks_dir,
            python,
            uv_enabled,
            inside_daemon: (rt.env)(&daemon_var).is_some(),
            path: (rt.env)("PATH"),
            interpreter: OnceLock::new(),
        }
    }

    /// The interpreter (probed on first use).
    pub fn interpreter(&self) -> Result<&Interpreter, String> {
        self.interpreter
            .get_or_init(|| probe_interpreter(&self.python))
            .as_ref()
            .map_err(Clone::clone)
    }

    /// `uv`, when it is installed and enabled (`use_uv_pip`'s inputs).
    pub fn uv_bin(&self) -> Option<PathBuf> {
        self.interpreter()
            .ok()
            .and_then(|i| i.uv.clone())
            .or_else(|| which("uv", self.path.as_deref()))
    }

    /// `venvs/<venv>`.
    pub fn venv_root(&self, venv: &str) -> PathBuf {
        self.venvs_dir.join(venv)
    }

    /// `venv_target_path(venv, allow_nonexistent=True)`: the `site-packages` directory.
    pub fn target_path(&self, venv: &str) -> Result<PathBuf, String> {
        let interpreter = self.interpreter()?;
        Ok(target_path_for(&self.venv_root(venv), interpreter))
    }

    /// `venv_exists(venv)`.
    pub fn venv_exists(&self, venv: &str) -> bool {
        self.target_path(venv).is_ok_and(|p| p.exists())
    }

    /// `venv_contains_package(name, venv)`: a directory with `__init__.py`, a `.py` module or a
    /// compiled module named like the package under any `site-packages` of the venv.
    pub fn venv_contains_package(&self, install_name: &str, venv: &str) -> bool {
        let root = install_name
            .split('.')
            .next()
            .unwrap_or_default()
            .to_lowercase()
            .replace('-', "_");
        if root.is_empty() {
            return false;
        }
        for site in super::venvs::site_packages(&self.venv_root(venv)) {
            let Ok(entries) = std::fs::read_dir(&site) else {
                continue;
            };
            for entry in entries.flatten() {
                let file_name = entry.file_name().to_string_lossy().to_string();
                let lower = file_name.to_lowercase().replace('-', "_");
                if !lower.starts_with(&root) || file_name.ends_with("dist_info") {
                    continue;
                }
                let path = entry.path();
                if (lower == root && path.is_dir() && path.join("__init__.py").exists())
                    || lower == format!("{root}.py")
                    || lower.starts_with(&format!("{root}."))
                {
                    return true;
                }
            }
        }
        false
    }

    /// `_get_pip_os_env(color=True)`: the environment of installer processes.
    pub fn installer_env(&self) -> Vec<(String, String)> {
        let mut env = vec![
            ("PIP_BREAK_SYSTEM_PACKAGES".to_string(), "true".to_string()),
            ("UV_BREAK_SYSTEM_PACKAGES".to_string(), "true".to_string()),
            ("FORCE_COLOR".to_string(), "1".to_string()),
        ];
        if let Ok(interpreter) = self.interpreter() {
            env.push((
                "UV_PYTHON".into(),
                interpreter.executable.to_string_lossy().into_owned(),
            ));
            let path = self.path.clone().unwrap_or_default();
            let bin = interpreter
                .executable
                .parent()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default();
            if !bin.is_empty() && !path.contains(&bin) {
                let separator = if cfg!(windows) { ";" } else { ":" };
                env.push(("PATH".into(), format!("{bin}{separator}{path}")));
            }
        }
        env
    }

    /// `init_venv(venv, force)`: create the environment with `uv venv`, else `python -m venv`.
    /// Returns whether it exists afterwards.
    pub fn init_venv(&self, rt: &mut Runtime<'_>, venv: &str, force: bool, debug: bool) -> bool {
        if !force && self.venv_exists(venv) {
            return true;
        }
        let Ok(interpreter) = self.interpreter() else {
            return false;
        };
        let venv_path = self.venv_root(venv);
        let lock_path = self.venv_locks_dir.join(format!("{venv}.lock"));
        // `wait_for_lock`: another process is creating this venv.
        let started = std::time::Instant::now();
        while lock_path.exists() && started.elapsed().as_secs_f64() < 30.0 {
            if debug {
                rt.print(&format!("Lock exists for venv '{venv}', sleeping...\n"));
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        let _ = std::fs::create_dir_all(&self.venv_locks_dir);
        let _ = std::fs::write(&lock_path, b"");
        let created = self.create_venv(rt, interpreter, &venv_path, debug);
        let _ = std::fs::remove_file(&lock_path);
        created && self.venv_exists(venv)
    }

    fn create_venv(
        &self,
        rt: &mut Runtime<'_>,
        interpreter: &Interpreter,
        venv_path: &Path,
        debug: bool,
    ) -> bool {
        let mut created = false;
        if self.uv_enabled
            && let Some(uv) = self.uv_bin()
        {
            let args: Vec<String> = [
                "venv",
                &venv_path.to_string_lossy(),
                "--python",
                &interpreter.executable.to_string_lossy(),
                "-q",
                "--no-project",
                "--allow-existing",
            ]
            .iter()
            .map(|s| s.to_string())
            .chain(self.auto_install.then(|| "--seed".to_string()))
            .collect();
            created = self.run(rt, &uv.to_string_lossy(), &args, true, debug) == 0;
        }
        if !created {
            let mut args = vec![
                "-m".to_string(),
                "venv".to_string(),
                venv_path.to_string_lossy().into_owned(),
            ];
            if !cfg!(windows) {
                args.push("--symlinks".into());
            }
            if !self.auto_install {
                // No `ensurepip` bootstrap while installs are disabled.
                args.push("--without-pip".into());
            }
            // The module's own output goes nowhere (v4 redirects stdout while it runs).
            let status = Command::new(&interpreter.executable)
                .args(&args)
                .stdout(Stdio::null())
                .status();
            created = status.is_ok_and(|s| s.success());
            if !created {
                rt.print(
                    "Please install python3-venv.\n\nFalling back to virtualenv...\n\
                     Failed to import `venv` or `virtualenv`! \
                     Please install `virtualenv` via pip then restart Meerschaum.\n",
                );
            }
        }
        created
    }

    /// Run an installer process with the installer environment and the terminal attached.
    fn run(
        &self,
        rt: &mut Runtime<'_>,
        program: &str,
        args: &[String],
        quiet_stdout: bool,
        debug: bool,
    ) -> i32 {
        if debug {
            let mut command = vec![program.to_string()];
            command.extend(args.iter().cloned());
            rt.eprint(&format!("{}\n", py_list(&command)));
        }
        let _ = std::io::Write::flush(&mut std::io::stdout());
        let mut command = Command::new(program);
        command.args(args).envs(self.installer_env());
        if quiet_stdout {
            command.stdout(Stdio::null());
        }
        match command.status() {
            Ok(status) => status.code().unwrap_or(1),
            Err(e) => {
                warn(
                    rt,
                    &format!(
                        "Failed to execute {}:\n{e}",
                        py_list(&[program.to_string()])
                    ),
                );
                1
            }
        }
    }
}

/// `venv_target_path` for a venv root and an interpreter, without requiring it to exist.
pub fn target_path_for(venv_root: &Path, interpreter: &Interpreter) -> PathBuf {
    let lib = venv_root.join(if cfg!(windows) { "Lib" } else { "lib" });
    let folder = format!("python{}.{}", interpreter.major, interpreter.minor);
    let base = if lib.exists() {
        let listed = std::fs::read_dir(&lib)
            .map(|entries| {
                entries
                    .flatten()
                    .any(|e| e.file_name().to_string_lossy() == folder)
            })
            .unwrap_or(false);
        if listed { lib.join(&folder) } else { lib }
    } else if cfg!(windows) {
        lib
    } else {
        lib.join(&folder)
    };
    base.join("site-packages")
}

/// The options of one `pip_install()` call.
#[derive(Debug, Clone, Default)]
pub struct PipInstall {
    /// The install names (`pandas>=2`).
    pub names: Vec<String>,
    /// `args` (`None`: `--upgrade`).
    pub args: Option<Vec<String>>,
    /// `requirements_file_path`.
    pub requirements_file: Option<PathBuf>,
    /// The virtual environment (`mrsm` by default).
    pub venv: String,
    /// Print the plan instead of installing.
    pub dry_run: bool,
    /// `--debug`.
    pub debug: bool,
    /// Skip the wheel/setuptools bootstrap (the recursive call sets it).
    pub skip_wheel_check: bool,
    /// `_uninstall=True` (`pip_uninstall`): remove the packages instead.
    pub uninstall: bool,
    /// `venv=None`: the host interpreter instead of a virtual environment (`upgrade meerschaum`).
    /// Nothing is created, no `--target` is passed, the request ledger is not touched.
    pub host: bool,
}

/// `get_pip_install_lock_path(venv)`: the cross-process lock of an environment.
pub fn install_lock_path(env: &PipEnv, venv: &str) -> PathBuf {
    let target = env
        .target_path(venv)
        .unwrap_or_else(|_| env.venv_root(venv).join("lib").join("site-packages"));
    let resolved = meerschaum_core::config::paths::resolve_path(&target);
    let digest = Sha256::digest(resolved.to_string_lossy().as_bytes());
    let hash = hex::encode(digest);
    std::env::temp_dir()
        .join("meerschaum-package-installs")
        .join(format!("{}.lock", &hash[..16]))
}

type HeldLocks = Mutex<HashMap<PathBuf, (usize, std::fs::File)>>;

fn held_locks() -> &'static HeldLocks {
    static HELD: OnceLock<HeldLocks> = OnceLock::new();
    HELD.get_or_init(|| Mutex::new(HashMap::new()))
}

/// `_pip_install_lock(venv)`: the lock is re-entrant within the process and an advisory
/// `flock` across processes, at the path Python uses (so workers and the CLI exclude each other).
pub struct InstallLock {
    path: PathBuf,
}

impl InstallLock {
    /// Wait for the lock of `venv`.
    pub fn acquire(env: &PipEnv, venv: &str) -> InstallLock {
        let path = install_lock_path(env, venv);
        let mut held = held_locks().lock().expect("install locks");
        if let Some((depth, _)) = held.get_mut(&path) {
            *depth += 1;
            return InstallLock { path };
        }
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Ok(file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
        {
            // Never hold the in-process table while waiting on another process.
            drop(held);
            let _ = fs4::fs_std::FileExt::lock_exclusive(&file);
            held = held_locks().lock().expect("install locks");
            held.insert(path.clone(), (1, file));
        }
        InstallLock { path }
    }
}

impl Drop for InstallLock {
    fn drop(&mut self) {
        let mut held = held_locks().lock().expect("install locks");
        if let Some((depth, _)) = held.get_mut(&self.path) {
            if *depth > 1 {
                *depth -= 1;
            } else if let Some((_, file)) = held.remove(&self.path) {
                let _ = fs4::fs_std::FileExt::unlock(&file);
            }
        }
    }
}

/// `get_install_no_version(name)`: everything before the first of `[=<>,! ]`.
pub fn install_name_no_version(name: &str) -> &str {
    name.split(['[', '=', '<', '>', ',', '!', ' ', ']'])
        .next()
        .unwrap_or(name)
}

/// `get_pip_install_plan(...)`.
pub fn install_plan(env: &PipEnv, opts: &PipInstall, rt: &Runtime<'_>) -> Json {
    let interpreter = env.interpreter().ok();
    let have_pip = interpreter.is_some_and(|i| i.has_pip);
    let uv_bin = env.uv_bin();
    let use_uv = uv_bin.is_some() && env.uv_enabled;
    let packages: Vec<&str> = opts
        .names
        .iter()
        .map(|name| {
            if opts.uninstall || name.starts_with(PACKAGE_ARCHIVES_PREFIX) {
                install_name_no_version(name)
            } else {
                name.as_str()
            }
        })
        .collect();
    let auto_install = !(rt.env)("MRSM_NO_AUTO_INSTALL")
        .is_some_and(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"));
    let target = env
        .target_path(&opts.venv)
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default();
    json!({
        "operation": if opts.uninstall { "uninstall" } else { "install" },
        "environment": if opts.host { Json::Null } else { json!(opts.venv) },
        "target": target,
        "packages": packages,
        "requirements_file": opts.requirements_file.as_ref().map(|p| {
            meerschaum_core::config::paths::resolve_path(p).to_string_lossy().into_owned()
        }),
        "args": opts.args.clone().unwrap_or_else(|| {
            if opts.uninstall { Vec::new() } else { vec!["--upgrade".to_string()] }
        }),
        "installer": if use_uv { "uv" } else { "pip" },
        "installer_available": use_uv || have_pip,
        "pip_fallback": use_uv && have_pip,
        "auto_install_enabled": auto_install,
        "would_bootstrap_uv": !use_uv && uv_bin.is_none() && have_pip && env.uv_enabled,
        "lock_path": install_lock_path(env, &opts.venv).to_string_lossy(),
    })
}

/// `json.dumps(plan, indent=2)`.
fn dump_plan(plan: &Json) -> String {
    serde_json::to_string_pretty(plan).unwrap_or_default()
}

/// `pip_install(*names, ...)` and `pip_uninstall(*names, ...)` (`opts.uninstall`): install into
/// (or remove from) `opts.venv`, or the host interpreter with `opts.host` (`venv=None`).
/// Returns success.
pub fn pip_install(env: &PipEnv, rt: &mut Runtime<'_>, opts: &PipInstall) -> bool {
    let silent = env.inside_daemon;
    if opts.dry_run {
        let plan = install_plan(env, opts, rt);
        if !silent {
            rt.print(&format!("{}\n", dump_plan(&plan)));
        }
        return true;
    }
    // The host interpreter is not an environment this process locks.
    let _lock = (!opts.host).then(|| InstallLock::acquire(env, &opts.venv));
    let interpreter = match env.interpreter() {
        Ok(interpreter) => interpreter.clone(),
        Err(message) => {
            if !silent {
                warn(rt, &message);
            }
            return false;
        }
    };
    let uv_bin = env.uv_bin();
    let use_uv = uv_bin.is_some() && env.uv_enabled;
    let have_pip = interpreter.has_pip;
    let verb = if opts.uninstall {
        "uninstall"
    } else {
        "install"
    };

    // The interpreter that runs `pip`: the host's, else the one in the `mrsm` environment.
    let mut pip_venv: Option<&str> = None;
    if !have_pip && !use_uv {
        if env.venv_contains_package("pip", "mrsm") {
            pip_venv = Some("mrsm");
        } else {
            rt.print(&format!(
                "\nFailed to import `pip` and `ensurepip`.\n\
                 If you are running Ubuntu/Debian, \
                 you might need to install `python3.{minor}-distutils`:\n\n    \
                 sudo apt install python3.{minor}-pip python3.{minor}-venv\n\n\
                 Please install pip and restart Meerschaum.\n\n\
                 You can find instructions on installing `pip` here:\n\
                 https://pip.pypa.io/en/stable/installing/\n",
                minor = interpreter.minor
            ));
            return false;
        }
    }

    // `with Venv(venv)`: create the environment on first use.
    let target = if opts.host {
        None
    } else {
        if !env.init_venv(rt, &opts.venv, false, opts.debug) {
            return false;
        }
        match env.target_path(&opts.venv) {
            Ok(target) => Some(target),
            Err(message) => {
                warn(rt, &message);
                return false;
            }
        }
    };

    let given: Vec<String> = opts.args.clone().unwrap_or_else(|| {
        if opts.uninstall {
            Vec::new()
        } else {
            vec!["--upgrade".into()]
        }
    });
    let mut args = given.clone();
    if !opts.host && !opts.uninstall {
        if !given.iter().any(|a| a == "--ignore-installed")
            && !given.iter().any(|a| a == "-I")
            && !use_uv
        {
            args.push("--ignore-installed".into());
        }
        if !given.iter().any(|a| a == "--cache-dir") {
            args.push("--cache-dir".into());
            args.push(
                env.venv_root(&opts.venv)
                    .join("cache")
                    .to_string_lossy()
                    .into_owned(),
            );
        }
    }
    let mut full: Vec<String> = vec![verb.into()];
    full.extend(args);

    // `pip` (not `uv`) needs `wheel` and `setuptools` to build; install them first.
    if !opts.uninstall
        && !opts.host
        && !use_uv
        && env.auto_install
        && !opts.skip_wheel_check
        && !env.venv_contains_package("wheel", &opts.venv)
    {
        let setup_packages: Vec<String> = ["setuptools", "wheel", "PyYAML"]
            .iter()
            .chain(env.uv_enabled.then_some(&"uv"))
            .map(|s| s.to_string())
            .collect();
        let bootstrap = PipInstall {
            names: setup_packages.clone(),
            venv: opts.venv.clone(),
            debug: opts.debug,
            skip_wheel_check: true,
            ..PipInstall::default()
        };
        if !pip_install(env, rt, &bootstrap) && !silent {
            warn(
                rt,
                &format!(
                    "Failed to install {} for virtual environment '{}'.",
                    super::items_str(&setup_packages),
                    opts.venv
                ),
            );
        }
    }

    if let Some(path) = &opts.requirements_file {
        full.push("-r".into());
        full.push(
            meerschaum_core::config::paths::resolve_path(path)
                .to_string_lossy()
                .replace('\\', "/"),
        );
    }
    if !use_uv {
        if !full.iter().any(|a| a == "--no-input") {
            full.push("--no-input".into());
        }
        if opts.uninstall && !full.iter().any(|a| a == "-y") {
            full.push("-y".into());
        }
        if !opts.uninstall && !full.iter().any(|a| a == "--no-warn-conflicts") {
            full.push("--no-warn-conflicts".into());
        }
        if !full.iter().any(|a| a == "--disable-pip-version-check") {
            full.push("--disable-pip-version-check".into());
        }
    } else if opts.uninstall {
        // `uv pip uninstall` has no `-y` (it never prompts) and rejects it.
        full.retain(|a| a != "-y" && a != "--yes");
    }
    let has_target = full.iter().any(|a| a == "--target" || a == "-t");
    if !has_target
        && !(!use_uv && opts.uninstall)
        && let Some(target) = &target
    {
        full.push("--target".into());
        full.push(target.to_string_lossy().replace('\\', "/"));
    }
    // A host install goes to the interpreter chosen for it. `uv` would otherwise look for
    // an environment of its own (`VIRTUAL_ENV`, `.venv`, the first `python` on `PATH`).
    // `--break-system-packages` is never added here: pass it explicitly when it is wanted.
    if opts.host && use_uv && !full.iter().any(|a| a == "--python" || a == "-p") {
        full.push("--python".into());
        full.push(interpreter.executable.to_string_lossy().into_owned());
    }
    if opts.debug && use_uv {
        full.push("--verbose".into());
    }

    let packages: Vec<String> = opts
        .names
        .iter()
        .map(|name| {
            if opts.uninstall || name.starts_with(PACKAGE_ARCHIVES_PREFIX) {
                install_name_no_version(name).to_string()
            } else {
                name.clone()
            }
        })
        .collect();
    if !silent {
        let mut message = if opts.uninstall {
            "Uninstalling packages:"
        } else {
            "Installing packages:"
        }
        .to_string();
        for package in &packages {
            message.push_str(&format!("\n  - {package}"));
        }
        rt.print(&format!("{message}\n"));
    }

    let rc = if opts.uninstall
        && !use_uv
        && let Some(target) = &target
    {
        // PARITY: v4 runs `pip uninstall` from the host interpreter, which does not see the
        // virtual environment's `--target` directory: nothing is removed and
        // `completely_uninstall_package` warns "Failed to clean up package". The distribution
        // files are removed directly here (the `uv` path is unchanged).
        let mut failed = false;
        for package in &packages {
            if matches!(package.as_str(), "pip" | "wheel" | "uv") {
                continue;
            }
            if !remove_distribution(target, package) && !silent {
                warn(rt, &format!("Failed to clean up package '{package}'."));
                failed = true;
            }
        }
        i32::from(failed)
    } else {
        let (program, mut command_args): (String, Vec<String>) = match (&uv_bin, use_uv) {
            (Some(uv), true) => (uv.to_string_lossy().into_owned(), vec!["pip".to_string()]),
            _ => {
                let executable = match pip_venv {
                    Some(venv) => venv_python(env, venv, &interpreter),
                    None => interpreter.executable.clone(),
                };
                (
                    executable.to_string_lossy().into_owned(),
                    vec!["-m".to_string(), "pip".to_string()],
                )
            }
        };
        command_args.extend(full);
        command_args.extend(packages.iter().cloned());
        env.run(rt, &program, &command_args, false, opts.debug)
    };
    if opts.debug {
        rt.print(&format!("rc={rc}\n"));
    }
    let success = rc == 0;
    if success && !opts.host {
        let root = env.venv_root(&opts.venv);
        if opts.uninstall {
            let _ = super::venvs::forget_packages(&root, &packages);
        } else {
            record_requests(&root, &packages, CORE_REQUESTER);
        }
    }
    let past = if opts.uninstall {
        "uninstalled"
    } else {
        "installed"
    };
    if !silent {
        rt.print(&format!(
            "{}\n",
            if success {
                format!("Successfully {past} packages.")
            } else {
                format!("Failed to {verb} packages.")
            }
        ));
        if opts.debug {
            rt.print(&format!("pip {verb} returned: {}\n", py_bool(success)));
        }
    }
    success
}

/// Remove an installed distribution from a `--target` directory: every file its `RECORD` lists,
/// the emptied directories, then the `*.dist-info` itself. A package that is not installed
/// counts as removed (as `pip uninstall` does, with a warning of its own).
pub fn remove_distribution(target: &Path, install_name: &str) -> bool {
    let wanted = normalize(install_name);
    let Ok(entries) = std::fs::read_dir(target) else {
        return true;
    };
    let mut ok = true;
    for entry in entries.flatten() {
        let file_name = entry.file_name().to_string_lossy().into_owned();
        let Some(stem) = file_name.strip_suffix(".dist-info") else {
            continue;
        };
        let dist_name = stem.rsplit_once('-').map_or(stem, |(name, _)| name);
        if normalize(dist_name) != wanted {
            continue;
        }
        let dist_info = entry.path();
        let record = std::fs::read_to_string(dist_info.join("RECORD")).unwrap_or_default();
        let mut directories: Vec<PathBuf> = Vec::new();
        for line in record.lines() {
            let relative = line.split(',').next().unwrap_or_default();
            if relative.is_empty() || relative.starts_with("..") || relative.starts_with('/') {
                continue;
            }
            let path = target.join(relative);
            if std::fs::symlink_metadata(&path).is_ok_and(|m| !m.is_dir()) {
                ok &= std::fs::remove_file(&path).is_ok();
            }
            let mut parent = path.parent().map(Path::to_path_buf);
            while let Some(dir) = parent {
                if dir == target || !dir.starts_with(target) {
                    break;
                }
                if !directories.contains(&dir) {
                    directories.push(dir.clone());
                }
                parent = dir.parent().map(Path::to_path_buf);
            }
        }
        // Deepest first; `remove_dir` only removes empty directories.
        directories.sort_by_key(|d| std::cmp::Reverse(d.components().count()));
        for dir in directories {
            let _ = std::fs::remove_dir(&dir);
        }
        ok &= std::fs::remove_dir_all(&dist_info).is_ok();
    }
    ok
}

fn py_bool(value: bool) -> &'static str {
    if value { "True" } else { "False" }
}

/// `venv_executable(venv)`: `venvs/<venv>/bin/python3.X`.
fn venv_python(env: &PipEnv, venv: &str, interpreter: &Interpreter) -> PathBuf {
    env.venv_root(venv)
        .join(if cfg!(windows) { "Scripts" } else { "bin" })
        .join(format!("python{}.{}", interpreter.major, interpreter.minor))
}

/// `_update_request_ledger(names, venv, uninstall=False)`: record who asked for each package.
pub fn record_requests(venv_root: &Path, install_names: &[String], requester: &str) {
    for name in install_names {
        if name.starts_with(['-', '/', '.']) || name.contains("://") {
            continue;
        }
        record_request(venv_root, name, requester);
    }
}

/// `record_request(venv, install_name, requester)`: `venvs/<venv>/.mrsm_requests.json`.
/// Accounting never breaks an install: every failure is swallowed.
pub fn record_request(venv_root: &Path, install_name: &str, requester: &str) -> bool {
    let name = normalize(install_name);
    if name.is_empty() || !venv_root.exists() {
        return false;
    }
    let path = venv_root.join(REQUESTS_FILE_NAME);
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(venv_root.join(".mrsm_requests.lock"))
        .ok();
    if let Some(file) = &lock_file {
        let _ = fs4::fs_std::FileExt::lock_exclusive(file);
    }
    let mut data: Json = std::fs::read_to_string(&path)
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .filter(|data: &Json| data.get("packages").is_some_and(Json::is_object))
        .unwrap_or_else(|| json!({"schema_version": 1, "packages": {}}));
    let now = chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S+00:00")
        .to_string();
    let existing_first = data["packages"][&name]["requesters"][requester]["first_requested"]
        .as_str()
        .map(str::to_string);
    data["packages"][&name]["requesters"][requester] = json!({
        "spec": install_name,
        "first_requested": existing_first.unwrap_or_else(|| now.clone()),
        "last_requested": now,
    });
    data["schema_version"] = json!(1);
    let temp = venv_root.join(format!(
        ".mrsm_requests.{}.{}.tmp",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let mut text = sorted_pretty(&data, 2);
    text.push('\n');
    let written = std::fs::write(&temp, text).and_then(|()| std::fs::rename(&temp, &path));
    if written.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    if let Some(file) = &lock_file {
        let _ = fs4::fs_std::FileExt::unlock(file);
    }
    written.is_ok()
}

/// `json.dump(data, f, indent=indent, sort_keys=True)` (`ensure_ascii` on, as Python's default).
pub fn sorted_pretty(value: &Json, indent: usize) -> String {
    fn string(text: &str) -> String {
        let mut out = String::from("\"");
        for c in text.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                '\u{8}' => out.push_str("\\b"),
                '\u{c}' => out.push_str("\\f"),
                c if (' '..='~').contains(&c) => out.push(c),
                c => {
                    let mut units = [0u16; 2];
                    for unit in c.encode_utf16(&mut units) {
                        out.push_str(&format!("\\u{unit:04x}"));
                    }
                }
            }
        }
        out.push('"');
        out
    }
    fn dump(value: &Json, level: usize, indent: usize, out: &mut String) {
        let pad = " ".repeat(indent * (level + 1));
        let close = " ".repeat(indent * level);
        match value {
            Json::String(s) => out.push_str(&string(s)),
            Json::Array(items) if items.is_empty() => out.push_str("[]"),
            Json::Array(items) => {
                out.push_str("[\n");
                for (i, item) in items.iter().enumerate() {
                    out.push_str(&pad);
                    dump(item, level + 1, indent, out);
                    out.push_str(if i + 1 < items.len() { ",\n" } else { "\n" });
                }
                out.push_str(&close);
                out.push(']');
            }
            Json::Object(map) if map.is_empty() => out.push_str("{}"),
            Json::Object(map) => {
                let mut entries: Vec<(&String, &Json)> = map.iter().collect();
                entries.sort_by(|a, b| a.0.cmp(b.0));
                out.push_str("{\n");
                for (i, (key, item)) in entries.iter().enumerate() {
                    out.push_str(&pad);
                    out.push_str(&string(key));
                    out.push_str(": ");
                    dump(item, level + 1, indent, out);
                    out.push_str(if i + 1 < entries.len() { ",\n" } else { "\n" });
                }
                out.push_str(&close);
                out.push('}');
            }
            other => out.push_str(&other.to_string()),
        }
    }
    let mut out = String::new();
    dump(value, 0, indent, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn install_names_lose_their_versions() {
        assert_eq!(install_name_no_version("pandas>=2.0"), "pandas");
        assert_eq!(install_name_no_version("pkg[extra]==1"), "pkg");
        assert_eq!(install_name_no_version("plain"), "plain");
    }

    #[test]
    fn target_path_follows_the_interpreter_version() {
        let dir = tempfile::tempdir().unwrap();
        let interpreter = Interpreter {
            executable: "/usr/bin/python3".into(),
            major: 3,
            minor: 13,
            has_pip: true,
            uv: None,
        };
        // No `lib` yet: the versioned folder is assumed.
        assert_eq!(
            target_path_for(dir.path(), &interpreter),
            dir.path().join("lib/python3.13/site-packages")
        );
        // A `lib` without the folder keeps `lib` (v4's quirk).
        std::fs::create_dir_all(dir.path().join("lib/python3.9")).unwrap();
        assert_eq!(
            target_path_for(dir.path(), &interpreter),
            dir.path().join("lib/site-packages")
        );
        std::fs::create_dir_all(dir.path().join("lib/python3.13")).unwrap();
        assert_eq!(
            target_path_for(dir.path(), &interpreter),
            dir.path().join("lib/python3.13/site-packages")
        );
    }

    #[test]
    fn requests_are_recorded_once_per_requester_and_keep_the_first_time() {
        let dir = tempfile::tempdir().unwrap();
        assert!(record_request(dir.path(), "Some_Pkg>=1", "meerschaum"));
        let path = dir.path().join(REQUESTS_FILE_NAME);
        let first: Json = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        let entry = &first["packages"]["some-pkg"]["requesters"]["meerschaum"];
        assert_eq!(entry["spec"], "Some_Pkg>=1");
        let first_requested = entry["first_requested"].as_str().unwrap().to_string();
        assert!(record_request(dir.path(), "some-pkg", "plugin_a"));
        let second: Json = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        let requesters = &second["packages"]["some-pkg"]["requesters"];
        assert_eq!(requesters["meerschaum"]["first_requested"], first_requested);
        assert_eq!(requesters["plugin_a"]["spec"], "some-pkg");
        // The Rust reader sees both.
        let ledger = super::super::venvs::read_ledger(dir.path());
        assert_eq!(ledger["some-pkg"].len(), 2);
        // A missing venv directory is never created for accounting.
        assert!(!record_request(
            &dir.path().join("absent"),
            "x",
            "meerschaum"
        ));
    }

    #[test]
    fn sorted_pretty_matches_python_json_dump() {
        let value = json!({"b": [1, 2], "a": {"z": null, "y": "é"}, "c": [], "d": {}});
        assert_eq!(
            sorted_pretty(&value, 4),
            "{\n    \"a\": {\n        \"y\": \"\\u00e9\",\n        \"z\": null\n    },\n    \"b\": [\n        1,\n        2\n    ],\n    \"c\": [],\n    \"d\": {}\n}"
        );
    }
}
