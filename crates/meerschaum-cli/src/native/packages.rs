//! The package actions: `show packages`, `verify packages`, `uninstall packages`,
//! `upgrade packages` and `upgrade meerschaum`, ported from `meerschaum/actions/{show,verify,
//! uninstall,upgrade}.py` and the helpers they call in `meerschaum/utils/packages/__init__.py`
//! (`pip_uninstall`, `get_prerelease_dependencies`, `need_update`, `manually_import_module`).
//!
//! The installers run through [`super::pip`] (`uv pip` / `pip`); the dependency groups are the
//! generated `super::packages_table`.
//!
//! # Differences from v4
//!
//! - `verify packages` never imports a module (v4 executes every package to find its version).
//!   The version of a package in the `mrsm` environment is read from its `*.dist-info` and an
//!   unmet minimum is repaired with an installer run, as v4's `check_update` does, unless
//!   `MRSM_NO_AUTO_INSTALL` is set (then only a warning). A package found in the host
//!   interpreter (one `importlib.util.find_spec` probe, nothing imported) is counted but never
//!   changed: v5 does not modify the host interpreter outside `upgrade meerschaum`.
//! - `--debug` prints the installer's command line (as v4) but not v4's `dprint` traces.
//! - `upgrade meerschaum` ends with the native `reload` instead of v4's `reload_meerschaum`.

use std::path::Path;
use std::process::{Command, Stdio};

use meerschaum_core::SuccessTuple;

use crate::builtin::ActionResult;
use crate::dispatch::Runtime;
use crate::format::rich::pretty::PyObj;
use crate::value::{ArgsMap, PyValue};

pub(crate) use super::packages_table::ALL_PACKAGES;
use super::packages_table::{self, GROUPS};
use super::pip::{PipEnv, PipInstall, install_name_no_version, pip_install};
use super::venvs::normalize;
use super::{flag, items_str, pprint, string, warn, words, yes_no};

/// `packages[group]` as the dictionary `pprint` shows.
fn group_object(group: packages_table::Group) -> PyObj {
    PyObj::Dict(
        group
            .iter()
            .map(|(import_name, install_name)| {
                (
                    PyObj::Str((*import_name).to_string()),
                    PyObj::Str((*install_name).to_string()),
                )
            })
            .collect(),
    )
}

// ---------------------------------------------------------------------------------------------
// show packages
// ---------------------------------------------------------------------------------------------

/// `show packages [group]`: the packages of a dependency group (`full` by default), pretty
/// printed, or one install name per line with `--nopretty`.
pub fn show_packages(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let nopretty = flag(args, "nopretty");
    let key = action.first().cloned().unwrap_or_else(|| "full".into());
    match packages_table::group(&key) {
        Some(group) if nopretty => {
            for (_, install_name) in group {
                rt.print(&format!("{install_name}\n"));
            }
        }
        Some(group) => pprint(rt, &group_object(group), false),
        None => warn(rt, &format!("'{key}' is not a valid group.")),
    }
    Ok(SuccessTuple::success())
}

// ---------------------------------------------------------------------------------------------
// uninstall packages
// ---------------------------------------------------------------------------------------------

/// `uninstall packages <names> [--venv name]`: `pip_uninstall` from a virtual environment.
pub fn uninstall_packages(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    if action.is_empty() {
        return Ok(SuccessTuple::fail("No packages to uninstall."));
    }
    let yes = flag(args, "yes") || flag(args, "force");
    if !yes && flag(args, "noask") {
        return Ok(SuccessTuple::fail(
            "Skipping uninstallation. Add `-y` or `-f` to agree to the uninstall prompt.",
        ));
    }
    let venv = string(args, "venv").unwrap_or_else(|| "mrsm".into());
    let mut installer_args = words(args, "sub_args");
    if yes {
        installer_args.push("-y".into());
    }
    let env = PipEnv::new(rt);
    let removed = pip_install(
        &env,
        rt,
        &PipInstall {
            names: action.clone(),
            args: Some(installer_args),
            venv: venv.clone(),
            uninstall: true,
            debug: flag(args, "debug"),
            ..PipInstall::default()
        },
    );
    if removed {
        return Ok(SuccessTuple::ok(format!(
            "Successfully removed packages from virtual environment '{venv}':\n{}",
            items_str(&action)
        )));
    }
    Ok(SuccessTuple::fail(format!(
        "Failed to uninstall packages:\n{}",
        action.join(", ")
    )))
}

// ---------------------------------------------------------------------------------------------
// upgrade packages
// ---------------------------------------------------------------------------------------------

/// The reverse of `all_packages` (`get_install_names()`): the name without its version to the
/// import name, a later entry replacing an earlier one.
fn install_to_import_name(install_name: &str) -> String {
    let no_version = install_name_no_version(install_name);
    ALL_PACKAGES
        .iter()
        .rev()
        .find(|(_, install)| install_name_no_version(install) == no_version)
        .map(|(import, _)| (*import).to_string())
        .unwrap_or_else(|| no_version.to_string())
}

/// `get_prerelease_dependencies(packages)`: the packages whose pinned version text contains
/// `dev`, `rc` or `a`.
pub fn prerelease_dependencies(packages: &[String]) -> Vec<String> {
    packages
        .iter()
        .filter(|install_name| {
            let no_version = install_name_no_version(install_name).to_lowercase();
            let import_name = install_to_import_name(install_name);
            let with_version = ALL_PACKAGES
                .iter()
                .rev()
                .find(|(import, _)| *import == import_name)
                .map(|(_, install)| (*install).to_string())
                .unwrap_or(import_name);
            let version_only = with_version
                .to_lowercase()
                .replace(&no_version, "")
                .rsplit(']')
                .next()
                .unwrap_or_default()
                .to_string();
            ["dev", "rc", "a"].iter().any(|p| version_only.contains(p))
        })
        .cloned()
        .collect()
}

/// `upgrade packages [group]`: reinstall a dependency group (`api` by default) into `mrsm`.
pub fn upgrade_packages(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    // PARITY: `venv` is accepted but never passed on; v4 always installs into `mrsm`
    // (`pip_install(*to_install, debug=debug)`, actions/upgrade.py).
    let group_name = action.first().cloned().unwrap_or_else(|| "api".into());
    let Some(group) = packages_table::group(&group_name) else {
        let invalid = format!("Invalid dependency group '{group_name}'.");
        let mut available = rt.with_formatter(|f| f.make_header("Available groups:"));
        for (name, _) in GROUPS {
            available.push_str(&format!("\n  - {name}"));
        }
        warn(rt, &format!("{invalid}\n\n{available}"));
        return Ok(SuccessTuple::fail(invalid));
    };
    let header = rt.with_formatter(|f| f.make_header("Packages to Upgrade:"));
    rt.print(&format!("{header}\n"));
    pprint(rt, &group_object(group), false);

    let force = flag(args, "force");
    let question = format!(
        "Are you sure you want to upgrade {} packages (dependency group '{group_name}')?",
        group.len()
    );
    let all: Vec<String> = group
        .iter()
        .map(|(_, install)| (*install).to_string())
        .collect();
    let prereleases = prerelease_dependencies(&all);
    let to_install: Vec<String> = all
        .iter()
        .filter(|name| !prereleases.contains(name) || group_name == "_internal")
        .cloned()
        .collect();

    let (mut success, mut message) = (false, "Nothing installed.".to_string());
    if force || yes_no(rt, &question, "y", flag(args, "yes"), flag(args, "noask"))? {
        let env = PipEnv::new(rt);
        let debug = flag(args, "debug");
        let install = |rt: &mut Runtime<'_>, names: Vec<String>| {
            pip_install(
                &env,
                rt,
                &PipInstall {
                    names,
                    venv: "mrsm".into(),
                    debug,
                    ..PipInstall::default()
                },
            )
        };
        success = install(rt, to_install);
        if success && !prereleases.is_empty() {
            success = install(rt, prereleases);
        }
        message = if success {
            format!("Successfully installed {} packages.", group.len())
        } else {
            format!("Failed to install packages in dependency group '{group_name}'.")
        };
    }
    if !success {
        return Ok(SuccessTuple::fail(message));
    }
    let reloaded = crate::builtin::reload(args, rt)?;
    if !reloaded.success {
        return Ok(reloaded);
    }
    Ok(SuccessTuple::ok(message))
}

// ---------------------------------------------------------------------------------------------
// upgrade meerschaum
// ---------------------------------------------------------------------------------------------

/// `is_docker_available()`: `docker ps` succeeds.
fn docker_available() -> bool {
    Command::new("docker")
        .arg("ps")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Upgrade the active installation, then optionally refresh its Docker stack.
pub fn upgrade_meerschaum(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let result = super::self_upgrade::upgrade(args, rt)?;
    if result.success && docker_available() {
        let (force, yes, noask) = (flag(args, "force"), flag(args, "yes"), flag(args, "noask"));
        let debug = flag(args, "debug");
        run_stack(rt, &["pull"], debug);
        if force || yes_no(rt, "Bring up the stack after upgrading?", "n", yes, noask)? {
            super::extensions::without_stack_hint(|| run_stack(rt, &["up", "-d"], debug));
            offer_extension_upgrade(rt, yes, noask, debug);
        }
    }
    Ok(result)
}

/// v5: once the stack is up again, wait (bounded) for its database to be healthy and offer
/// `upgrade extensions` when an extension lags the freshly pulled image. Never fails the
/// upgrade: errors and timeouts only skip the offer.
fn offer_extension_upgrade(rt: &mut Runtime<'_>, yes: bool, noask: bool, debug: bool) {
    use super::extensions;
    if !wait_for_stack_db(rt, std::time::Duration::from_secs(90)) {
        return;
    }
    let Some(hint) = extensions::lag_hint(rt, "sql:main", std::time::Duration::from_secs(5)) else {
        return;
    };
    super::info(rt, &hint);
    let noask = super::check_noask(rt, noask);
    if !yes && noask {
        return;
    }
    let run = yes || yes_no(rt, "Update the extensions now?", "y", yes, noask).unwrap_or(false);
    if !run {
        return;
    }
    let args = extensions::upgrade_args("sql:main", yes, noask, debug);
    match extensions::upgrade_extensions(&args, rt) {
        Ok(result) => super::print_tuple(rt, &result, Default::default()),
        Err(e) => warn(rt, &e.message),
    }
}

/// The `db` container's `{{.State.Health.Status}}`, or `{{.State.Status}}` without a
/// healthcheck: ready when `healthy`, or `running` with no healthcheck to wait for.
fn db_ready(state: Option<&str>) -> bool {
    matches!(state, Some("healthy" | "running"))
}

/// Wait until the stack's `db` container is healthy, or running when it has no healthcheck (a
/// user's `stack.yaml` may drop it), by the Compose labels, at most `limit`.
fn wait_for_stack_db(rt: &mut Runtime<'_>, limit: std::time::Duration) -> bool {
    let project = rt
        .config()
        .get(&["stack", "project_name"])
        .map(|v| v.py_str())
        .unwrap_or_else(|| "mrsm".into());
    let start = std::time::Instant::now();
    loop {
        let ids = Command::new("docker")
            .args([
                "ps",
                "-q",
                "--filter",
                &format!("label=com.docker.compose.project={project}"),
                "--filter",
                "label=com.docker.compose.service=db",
            ])
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default();
        let state = ids.lines().next().and_then(|id| {
            Command::new("docker")
                .args([
                    "inspect",
                    "-f",
                    "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
                    id,
                ])
                .stdin(Stdio::null())
                .stderr(Stdio::null())
                .output()
                .ok()
                .filter(|o| o.status.success())
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        });
        if db_ready(state.as_deref()) {
            return true;
        }
        if start.elapsed() >= limit {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_secs(2));
    }
}

/// `actions['stack'](words, debug=debug)`; the result is ignored, as in v4.
pub(super) fn run_stack(rt: &mut Runtime<'_>, words: &[&str], debug: bool) {
    let mut stack_args = ArgsMap::new();
    let mut action = vec!["stack".to_string()];
    action.extend(words.iter().map(|w| (*w).to_string()));
    stack_args.insert("action".into(), PyValue::str_list(&action));
    stack_args.insert("debug".into(), PyValue::Bool(debug));
    let _ = crate::dispatch::entry_with_args(stack_args, rt, 1);
}

// ---------------------------------------------------------------------------------------------
// verify packages
// ---------------------------------------------------------------------------------------------

/// The minimum a requirement asks for: `psycopg[binary]>=3.2.9` gives `>=3.2.9`.
fn required_version(install_name: &str) -> String {
    let no_version = install_name_no_version(install_name);
    let rest = install_name.replacen(no_version, "", 1);
    rest.rsplit(']')
        .next()
        .unwrap_or_default()
        .trim()
        .to_string()
}

/// Import names the host interpreter can find (`importlib.util.find_spec`, nothing imported).
fn host_packages(env: &PipEnv, import_names: &[&str]) -> Vec<String> {
    let Ok(interpreter) = env.interpreter() else {
        return Vec::new();
    };
    const PROBE: &str = "\
import importlib.util, sys
for name in sys.argv[1:]:
    try:
        if importlib.util.find_spec(name) is not None:
            print(name)
    except Exception:
        pass
";
    let output = Command::new(&interpreter.executable)
        .args(["-c", PROBE])
        .args(import_names)
        .env("PYTHONSAFEPATH", "1")
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output();
    output
        .map(|o| {
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// The version of an installed distribution in a venv's `site-packages`.
fn installed_version(venv_dir: &Path, install_name: &str) -> Option<String> {
    let wanted = normalize(install_name);
    super::venvs::scan_venv(venv_dir)?
        .dists
        .into_iter()
        .find(|d| d.name == wanted)
        .map(|d| d.version)
}

/// `verify packages [--venv name]`: check every known package against its minimum version.
pub fn verify_packages(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let venv = string(args, "venv").unwrap_or_else(|| "mrsm".into());
    let debug = flag(args, "debug");
    let env = PipEnv::new(rt);
    let mut in_venv: Vec<(&str, &str)> = Vec::new();
    let mut elsewhere: Vec<&str> = Vec::new();
    for (import_name, install_name) in ALL_PACKAGES {
        if env.venv_contains_package(import_name, &venv) {
            in_venv.push((import_name, install_name));
        } else {
            elsewhere.push(import_name);
        }
    }
    let on_host = host_packages(&env, &elsewhere);

    let venv_dir = env.venv_root(&venv);
    let mut verified = on_host.len();
    for (import_name, install_name) in in_venv {
        verified += 1;
        let minimum = required_version(install_name);
        if minimum.is_empty() {
            continue;
        }
        // PARITY: v4 drops `.dev...` from both versions before comparing.
        let minimum = minimum.split(".dev").next().unwrap_or_default().to_string();
        let Some(version) = installed_version(&venv_dir, install_name) else {
            continue;
        };
        let version = version.split(".dev").next().unwrap_or_default().to_string();
        if meerschaum_core::version::version_satisfies(&version, &minimum) != Some(false) {
            continue;
        }
        if !env.auto_install {
            warn(
                rt,
                &format!("There's an update available for '{import_name}'."),
            );
            continue;
        }
        let installed = pip_install(
            &env,
            rt,
            &PipInstall {
                names: vec![install_name.to_string()],
                venv: venv.clone(),
                debug,
                ..PipInstall::default()
            },
        );
        if !installed {
            warn(
                rt,
                &format!(
                    "There's an update available for '{}', but it failed to install. Try installig via Meerschaum with `install packages '{{install_name}}'`.",
                    install_name_no_version(install_name)
                ),
            );
        }
    }
    Ok(SuccessTuple::ok(format!("Verified {verified} packages.")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_stack_db_is_ready_when_healthy_or_running_without_a_healthcheck() {
        assert!(db_ready(Some("healthy")));
        // No healthcheck in the user's `stack.yaml`: the inspect template prints the state.
        assert!(db_ready(Some("running")));
        assert!(!db_ready(Some("starting")));
        assert!(!db_ready(Some("unhealthy")));
        assert!(!db_ready(Some("restarting")));
        assert!(!db_ready(None));
    }

    #[test]
    fn prereleases_are_pins_with_dev_rc_or_a() {
        let names = vec![
            "pandas[parquet]>=3.0.0".to_string(),
            "foo>=1.0.0rc1".to_string(),
        ];
        // `foo` is not a known package: its pin is looked up as itself and has no version text.
        assert!(prerelease_dependencies(&names).is_empty());
    }

    #[test]
    fn required_versions_follow_the_extras() {
        assert_eq!(required_version("psycopg[binary]>=3.2.9"), ">=3.2.9");
        assert_eq!(required_version("pytz"), "");
        assert_eq!(required_version("pyinstaller>6.6.0"), ">6.6.0");
    }

    #[test]
    fn show_packages_groups_exist() {
        for group in ["full", "api", "sql", "polars", "formatting", "cli-tools"] {
            assert!(packages_table::group(group).is_some(), "{group}");
        }
        assert!(packages_table::group("bogus").is_none());
    }
}
