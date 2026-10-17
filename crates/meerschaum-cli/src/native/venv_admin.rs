//! `delete venvs` and `verify venvs`, ported from `meerschaum/actions/{delete,verify}.py` and
//! `meerschaum/utils/venv/__init__.py` (`verify_venv`, `get_venvs`, `venv_exists`).
//!
//! # Differences from v4
//!
//! - `delete venvs` without names lists the venvs sorted (v4 uses `os.listdir` order).
//! - `verify venvs` works on the files: `<venv>/bin/python<major>.<minor>` must exist and answer
//!   `-V` within a second; a missing or dangling interpreter re-creates the venv
//!   ([`PipEnv::init_venv`]) and links the host interpreter, a link that names another version is
//!   renamed, as v4 does. The interpreter is the one the Python worker uses (`MRSM_PYTHON`).

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use meerschaum_core::SuccessTuple;

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::format::OptionsArgs;
use crate::value::ArgsMap;

use super::pip::PipEnv;
use super::{flag, plural, warn, words, yes_no};

/// `venv_exists(venv)`: the `site-packages` of this interpreter's version exists. When no
/// interpreter can be started the version is unknown and any `site-packages` counts.
fn venv_exists(env: &PipEnv, venv: &str) -> bool {
    if env.interpreter().is_ok() {
        env.venv_exists(venv)
    } else {
        !super::venvs::site_packages(&env.venv_root(venv)).is_empty()
    }
}

/// `get_venvs()`: the directories of `venvs/` that hold a `site-packages` for this interpreter.
fn get_venvs(env: &PipEnv) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(&env.venvs_dir) else {
        return Vec::new();
    };
    let mut venvs: Vec<String> = entries
        .flatten()
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|name| venv_exists(env, name))
        .collect();
    venvs.sort();
    venvs
}

/// One plain directory name: not empty, no `/`, `\` or NUL, no `..`, no leading `.`.
fn valid_venv_name(name: &str) -> bool {
    !(name.is_empty()
        || name.contains(['/', '\\', '\0'])
        || name.contains("..")
        || name.starts_with('.'))
}

/// `delete venvs [names]`: remove virtual environments (never `mrsm`).
pub fn delete_venvs(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let env = PipEnv::new(rt);
    let action = words(args, "action");
    // PARITY: v4 joins the name into `venvs/<name>` unchecked (`delete venvs ../x -y` removes a
    // directory outside `venvs/`); names that are not one plain directory name are refused.
    if let Some(bad) = action.iter().find(|name| !valid_venv_name(name)) {
        return Ok(SuccessTuple::fail(format!("Invalid venv name: '{bad}'")));
    }
    let candidates = if action.is_empty() {
        get_venvs_listing(&env)
    } else {
        action.clone()
    };
    let venvs: Vec<String> = candidates
        .into_iter()
        .filter(|v| venv_exists(&env, v) && v != "mrsm")
        .collect();
    if venvs.is_empty() {
        let message = "No venvs to delete.";
        return Ok(if action.is_empty() {
            SuccessTuple::ok(message)
        } else {
            SuccessTuple::fail(message)
        });
    }
    let opts = OptionsArgs {
        nopretty: flag(args, "nopretty"),
        header: Some("Venvs to Delete:".into()),
        ..OptionsArgs::default()
    };
    let text = rt.with_formatter(|f| f.print_options(&venvs, &opts));
    rt.print(&text);
    // PARITY: v4 passes `force` to `yes_no`, which ignores it: only `--yes` skips the question.
    let question = format!(
        "Remove the above venv{}?\n    Run `mrsm upgrade packages` and `mrsm install required` to reinstall dependencies.\n",
        plural(venvs.len())
    );
    if !yes_no(rt, &question, "n", flag(args, "yes"), false)? {
        return Ok(SuccessTuple::ok("Nothing was deleted."));
    }
    for venv in &venvs {
        let path = env.venv_root(venv);
        if let Err(e) = std::fs::remove_dir_all(&path) {
            return Ok(SuccessTuple::fail(format!(
                "Failed to remove '{}':\n{e}",
                path.display()
            )));
        }
    }
    Ok(SuccessTuple::ok(format!(
        "Removed {} venv{}.",
        venvs.len(),
        plural(venvs.len())
    )))
}

/// `os.listdir(VIRTENV_RESOURCES_PATH)`, sorted.
fn get_venvs_listing(env: &PipEnv) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(&env.venvs_dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

/// `verify venvs`: check every virtual environment's interpreter links.
pub fn verify_venvs(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let env = PipEnv::new(rt);
    let debug = flag(args, "debug");
    for venv in get_venvs(&env) {
        verify_venv(&env, rt, &venv, debug)?;
    }
    Ok(SuccessTuple::ok("Success"))
}

/// `python -V` with a one-second timeout: `3.14.2`, or `None` when the binary does not run.
fn python_version(rt: &mut Runtime<'_>, path: &Path, debug: bool) -> Option<String> {
    if debug {
        rt.print(&format!("Getting python version for {}\n", path.display()));
    }
    let mut child = Command::new(path)
        .arg("-V")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .ok();
    let Some(process) = child.as_mut() else {
        // A broken symlink cannot be run: remove it.
        remove_broken_link(rt, path);
        return None;
    };
    let started = Instant::now();
    loop {
        match process.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if started.elapsed() < Duration::from_secs(1) => {
                std::thread::sleep(Duration::from_millis(10));
            }
            _ => {
                let _ = process.kill();
                let _ = process.wait();
                remove_broken_link(rt, path);
                return None;
            }
        }
    }
    let output = child.take()?.wait_with_output().ok()?;
    Some(
        String::from_utf8_lossy(&output.stdout)
            .trim()
            .replace("Python ", ""),
    )
}

fn remove_broken_link(rt: &mut Runtime<'_>, path: &Path) {
    if std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink())
        && let Err(e) = std::fs::remove_file(path)
    {
        rt.print(&format!(
            "Unable to remove broken symlink {}:\n{e}\n",
            path.display()
        ));
    }
}

fn make_symlink(real: &Path, link: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(real, link)
    }
    #[cfg(not(unix))]
    {
        std::fs::copy(real, link).map(|_| ())
    }
}

/// `verify_venv(venv)`.
fn verify_venv(
    env: &PipEnv,
    rt: &mut Runtime<'_>,
    venv: &str,
    debug: bool,
) -> Result<(), ActionError> {
    let Ok(interpreter) = env.interpreter() else {
        return Ok(());
    };
    let (major, minor) = (interpreter.major, interpreter.minor);
    let bin = env
        .venv_root(venv)
        .join(if cfg!(windows) { "Scripts" } else { "bin" });
    let exe_suffix = if cfg!(windows) { ".exe" } else { "" };
    let current = format!("python{major}.{minor}{exe_suffix}");

    let files: Vec<String> = if !bin.join(&current).exists() {
        env.init_venv(rt, venv, true, debug);
        let in_venv = bin.join(format!("python{major}.{minor}"));
        let on_host = interpreter.executable.clone();
        if !in_venv.exists() {
            remove_broken_link(rt, &in_venv);
            if let Err(e) = make_symlink(&on_host, &in_venv) {
                let _ = e;
                rt.print(&format!(
                    "Unable to create symlink {} to {}.\n",
                    in_venv.display(),
                    on_host.display()
                ));
            }
        }
        let mut listing: Vec<String> = std::fs::read_dir(&bin)
            .map(|entries| {
                entries
                    .flatten()
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        listing.sort();
        listing.reverse();
        listing
    } else {
        vec![current]
    };

    for filename in files {
        if !filename.starts_with("python") {
            continue;
        }
        let python_path = bin.join(&filename);
        let Some(version) = python_version(rt, &python_path, debug) else {
            continue;
        };
        let mut parts = version.splitn(3, '.');
        let (Some(major_version), Some(minor_version)) = (parts.next(), parts.next()) else {
            return Ok(());
        };
        let versioned = format!("python{major_version}.{minor_version}{exe_suffix}");

        // E.g. python3.10 actually links to Python 3.10.
        if filename == versioned {
            let real_path = std::fs::canonicalize(&python_path).ok();
            if real_path.is_none() {
                let _ = std::fs::remove_file(&python_path);
                env.init_venv(rt, venv, true, debug);
                if !python_path.exists() {
                    return Err(ActionError::new(
                        "FileNotFoundError",
                        format!(
                            "Unable to verify Python symlink:\n{}",
                            python_path.display()
                        ),
                    ));
                }
            }
            let real_path = real_path
                .or_else(|| std::fs::canonicalize(&python_path).ok())
                .unwrap_or_else(|| python_path.clone());
            if python_path == real_path {
                continue;
            }
            let _ = std::fs::remove_file(&python_path);
            if let Err(e) = make_symlink(&real_path, &python_path) {
                warn(
                    rt,
                    &format!("Failed to symlink {}: {e}", real_path.display()),
                );
            }
            continue;
        }

        let versioned_path: PathBuf = bin.join(&versioned);
        if versioned_path.exists() {
            // Avoid circular symlinks.
            if python_version(rt, &versioned_path, debug).as_deref() == Some(version.as_str()) {
                continue;
            }
            let _ = std::fs::remove_file(&versioned_path);
        }
        let _ = std::fs::rename(&python_path, &versioned_path);
    }
    Ok(())
}
