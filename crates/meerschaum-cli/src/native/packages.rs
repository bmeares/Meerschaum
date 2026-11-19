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
    meerschaum_core::child_env::scrubbed(Command::new("docker"))
        .arg("ps")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// The question about the stack's images (default yes: v4 always pulled).
pub(super) const PULL_QUESTION: &str = "Pull the stack's new Docker images?";
/// v4's question about the stack (default no; `--force` answers yes).
pub(super) const UP_QUESTION: &str = "Bring up the stack after upgrading?";

/// What [`UpgradeSteps::plan`] found.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum PlanOutcome {
    /// Refused or cancelled: nothing changed, nothing else is asked or run.
    Stop(SuccessTuple),
    /// Nothing to install (the release is already installed); the stack steps may still run.
    Current(SuccessTuple),
    /// An approved installation is ready to run.
    Ready,
}

/// The flags of `upgrade meerschaum` that shape its questions.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct UpgradeFlags {
    pub yes: bool,
    pub noask: bool,
    pub force: bool,
    /// `--no-pull`: skip pulling the stack's images without asking.
    pub no_pull: bool,
}

/// The steps of `upgrade meerschaum`, so that their order can be tested with a fake.
pub(super) trait UpgradeSteps {
    /// Choose the release and the installer, refuse what cannot be upgraded, and ask "Upgrade
    /// this installation?" (and whether to align a selected Python host). Nothing slow runs.
    fn plan(&mut self) -> PlanOutcome;
    /// Docker answers and this root has a stack (`mrsm stack` wrote its compose file).
    fn stack_applies(&mut self) -> bool;
    /// `yes_no(question, default, yes, noask)`.
    fn ask(
        &mut self,
        question: &str,
        default: &str,
        yes: bool,
        noask: bool,
    ) -> Result<bool, crate::builtin::ActionError>;
    /// Download, install and verify the approved installation.
    fn install(&mut self) -> SuccessTuple;
    /// `stack pull`: success.
    fn pull(&mut self) -> bool;
    /// `stack up -d`: success.
    fn up(&mut self) -> bool;
    /// After a successful `stack up`: offer the extension updates the new images ship.
    fn after_up(&mut self);
    /// Print a result on its own (the installation's, before the stack steps).
    fn report(&mut self, result: &SuccessTuple);
    fn info(&mut self, text: &str);
    fn warn(&mut self, text: &str);
}

/// `upgrade meerschaum`: every question first (the installation, then the stack's images, then
/// bringing the stack up), then the work, unattended: install, pull, bring up. The stack steps
/// never run after a refused, cancelled or failed installation, and a failed pull never turns
/// a successful installation into a failure.
pub(super) fn run_upgrade(
    steps: &mut impl UpgradeSteps,
    flags: UpgradeFlags,
) -> Result<SuccessTuple, crate::builtin::ActionError> {
    let planned = steps.plan();
    if let PlanOutcome::Stop(result) = planned {
        return Ok(result);
    }
    let (pull, up) = if steps.stack_applies() {
        let pull = !flags.no_pull && steps.ask(PULL_QUESTION, "y", flags.yes, flags.noask)?;
        let up = flags.force || steps.ask(UP_QUESTION, "n", flags.yes, flags.noask)?;
        (pull, up)
    } else {
        (false, false)
    };
    let installed = match planned {
        PlanOutcome::Current(result) => result,
        _ => steps.install(),
    };
    if !installed.success || !(pull || up) {
        return Ok(installed);
    }
    steps.report(&installed);
    let mut outcome = Vec::new();
    let pulled = pull.then(|| steps.pull());
    match pulled {
        Some(true) => outcome.push("Pulled the stack's new images."),
        Some(false) => {
            steps.warn(
                "Failed to pull the stack's new Docker images. Meerschaum itself was upgraded; retry with `mrsm stack pull`.",
            );
            outcome.push("Pulling the stack's new images failed (retry with `mrsm stack pull`).");
        }
        None => {}
    }
    if up {
        if pulled == Some(false) {
            steps.info(
                "Not bringing up the stack, because pulling its images failed. Run `mrsm stack pull`, then `mrsm stack up -d`.",
            );
            outcome.push("The stack was not brought up.");
        } else {
            if pulled.is_none() {
                steps.info(
                    "Bringing up the stack with the images already on this host (no new images were pulled).",
                );
            }
            if steps.up() {
                outcome.push("Brought up the stack.");
                steps.after_up();
            } else {
                steps.warn("Failed to bring up the stack. Retry with `mrsm stack up -d`.");
                outcome.push("Bringing up the stack failed (retry with `mrsm stack up -d`).");
            }
        }
    }
    Ok(SuccessTuple::ok(format!(
        "Meerschaum is upgraded. {}",
        outcome.join(" ")
    )))
}

/// The real steps: the installation planner, the stack and the terminal.
struct LiveSteps<'a, 'r> {
    rt: &'a mut Runtime<'r>,
    args: &'a ArgsMap,
    job: Option<super::self_upgrade::Job>,
    flags: UpgradeFlags,
    debug: bool,
}

impl UpgradeSteps for LiveSteps<'_, '_> {
    fn plan(&mut self) -> PlanOutcome {
        use super::self_upgrade::Planned;
        match super::self_upgrade::plan(self.args, self.rt) {
            Planned::Stop(result) => PlanOutcome::Stop(result),
            Planned::Current(result) => PlanOutcome::Current(result),
            Planned::Ready(job) => {
                self.job = Some(*job);
                PlanOutcome::Ready
            }
        }
    }

    fn stack_applies(&mut self) -> bool {
        // A root that never used the stack has no compose file: nothing to pull or bring up,
        // and `mrsm stack` would write one first.
        self.rt.config().paths().path("STACK_COMPOSE_PATH").exists() && docker_available()
    }

    fn ask(
        &mut self,
        question: &str,
        default: &str,
        yes: bool,
        noask: bool,
    ) -> Result<bool, crate::builtin::ActionError> {
        yes_no(self.rt, question, default, yes, noask)
    }

    fn install(&mut self) -> SuccessTuple {
        match self.job.take() {
            Some(job) => super::self_upgrade::execute(job, self.rt),
            None => SuccessTuple::fail("No installation was planned."),
        }
    }

    fn pull(&mut self) -> bool {
        run_stack(self.rt, &["pull"], self.debug)
    }

    fn up(&mut self) -> bool {
        let (rt, debug) = (&mut *self.rt, self.debug);
        super::extensions::without_stack_hint(|| run_stack(rt, &["up", "-d"], debug))
    }

    fn after_up(&mut self) {
        offer_extension_upgrade(self.rt, self.flags.yes, self.flags.noask, self.debug);
    }

    fn report(&mut self, result: &SuccessTuple) {
        super::print_tuple(self.rt, result, Default::default());
    }

    fn info(&mut self, text: &str) {
        super::info(self.rt, text);
    }

    fn warn(&mut self, text: &str) {
        warn(self.rt, text);
    }
}

/// Upgrade the active installation, then optionally refresh its Docker stack.
pub fn upgrade_meerschaum(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let flags = UpgradeFlags {
        yes: flag(args, "yes"),
        noask: flag(args, "noask"),
        force: flag(args, "force"),
        no_pull: flag(args, "no_pull"),
    };
    let mut steps = LiveSteps {
        rt,
        args,
        job: None,
        flags,
        debug: flag(args, "debug"),
    };
    run_upgrade(&mut steps, flags)
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
        let ids = meerschaum_core::child_env::scrubbed(Command::new("docker"))
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
            meerschaum_core::child_env::scrubbed(Command::new("docker"))
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

/// `actions['stack'](words, debug=debug)`: whether it succeeded (v4 ignores the result).
pub(super) fn run_stack(rt: &mut Runtime<'_>, words: &[&str], debug: bool) -> bool {
    let mut stack_args = ArgsMap::new();
    let mut action = vec!["stack".to_string()];
    action.extend(words.iter().map(|w| (*w).to_string()));
    stack_args.insert("action".into(), PyValue::str_list(&action));
    stack_args.insert("debug".into(), PyValue::Bool(debug));
    crate::dispatch::entry_with_args(stack_args, rt, 1).success
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
    let output = meerschaum_core::child_env::scrubbed(Command::new(&interpreter.executable))
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

    /// A fake `upgrade meerschaum`: questions are answered from a script (emulating `yes_no`:
    /// `--yes` answers yes and `--noask` the default, without asking), and every step is recorded.
    struct Fake {
        plan: PlanOutcome,
        stack: bool,
        answers: Vec<bool>,
        install_ok: bool,
        pull_ok: bool,
        up_ok: bool,
        events: Vec<String>,
    }

    impl Fake {
        fn new(stack: bool, answers: &[bool]) -> Self {
            Fake {
                plan: PlanOutcome::Ready,
                stack,
                answers: answers.to_vec(),
                install_ok: true,
                pull_ok: true,
                up_ok: true,
                events: Vec::new(),
            }
        }
    }

    impl UpgradeSteps for Fake {
        fn plan(&mut self) -> PlanOutcome {
            self.events.push("plan".into());
            self.plan.clone()
        }
        fn stack_applies(&mut self) -> bool {
            self.stack
        }
        fn ask(
            &mut self,
            question: &str,
            default: &str,
            yes: bool,
            noask: bool,
        ) -> Result<bool, crate::builtin::ActionError> {
            let answer = if yes {
                true
            } else if noask {
                default == "y"
            } else {
                assert!(!self.answers.is_empty(), "unexpected question {question}");
                self.answers.remove(0)
            };
            let short = if question == PULL_QUESTION {
                "pull"
            } else {
                "up"
            };
            self.events.push(format!("ask {short}={answer}"));
            Ok(answer)
        }
        fn install(&mut self) -> SuccessTuple {
            self.events.push("install".into());
            if self.install_ok {
                SuccessTuple::ok("Installed Meerschaum 5.1.0.")
            } else {
                SuccessTuple::fail("Failed to upgrade Meerschaum.")
            }
        }
        fn pull(&mut self) -> bool {
            self.events.push("pull".into());
            self.pull_ok
        }
        fn up(&mut self) -> bool {
            self.events.push("up".into());
            self.up_ok
        }
        fn after_up(&mut self) {
            self.events.push("extensions".into());
        }
        fn report(&mut self, result: &SuccessTuple) {
            self.events.push(format!("report {}", result.message));
        }
        fn info(&mut self, text: &str) {
            self.events.push(format!("info {text}"));
        }
        fn warn(&mut self, text: &str) {
            self.events.push(format!("warn {text}"));
        }
    }

    fn flags(yes: bool, noask: bool, force: bool, no_pull: bool) -> UpgradeFlags {
        UpgradeFlags {
            yes,
            noask,
            force,
            no_pull,
        }
    }

    /// The steps that ran, without the messages' text.
    fn steps(fake: &Fake) -> Vec<&str> {
        fake.events
            .iter()
            .map(|e| match e.split_once(' ') {
                Some((head, _)) if head != "ask" => head,
                _ => e.as_str(),
            })
            .collect()
    }

    /// No slow step (install, pull, up) starts before the last answer.
    fn assert_questions_first(fake: &Fake) {
        let last_question = fake.events.iter().rposition(|e| e.starts_with("ask "));
        let first_work = fake
            .events
            .iter()
            .position(|e| matches!(e.as_str(), "install" | "pull" | "up"));
        if let (Some(q), Some(w)) = (last_question, first_work) {
            assert!(q < w, "{:?}", fake.events);
        }
    }

    #[test]
    fn every_question_comes_before_any_work() {
        // (stack, scripted answers, flags) -> the exact steps.
        let cases: &[(bool, &[bool], UpgradeFlags, &[&str])] = &[
            // Interactive: pull yes, up yes.
            (
                true,
                &[true, true],
                flags(false, false, false, false),
                &[
                    "plan",
                    "ask pull=true",
                    "ask up=true",
                    "install",
                    "report",
                    "pull",
                    "up",
                    "extensions",
                ],
            ),
            // Pull yes, up no (the default).
            (
                true,
                &[true, false],
                flags(false, false, false, false),
                &[
                    "plan",
                    "ask pull=true",
                    "ask up=false",
                    "install",
                    "report",
                    "pull",
                ],
            ),
            // Pull no, up yes: brought up with the existing images, and it says so.
            (
                true,
                &[false, true],
                flags(false, false, false, false),
                &[
                    "plan",
                    "ask pull=false",
                    "ask up=true",
                    "install",
                    "report",
                    "info",
                    "up",
                    "extensions",
                ],
            ),
            // Neither: just the installation, its result is the action's result.
            (
                true,
                &[false, false],
                flags(false, false, false, false),
                &["plan", "ask pull=false", "ask up=false", "install"],
            ),
            // `--yes`: pull and up without asking.
            (
                true,
                &[],
                flags(true, false, false, false),
                &[
                    "plan",
                    "ask pull=true",
                    "ask up=true",
                    "install",
                    "report",
                    "pull",
                    "up",
                    "extensions",
                ],
            ),
            // `--noask`: the defaults (pull yes, up no).
            (
                true,
                &[],
                flags(false, true, false, false),
                &[
                    "plan",
                    "ask pull=true",
                    "ask up=false",
                    "install",
                    "report",
                    "pull",
                ],
            ),
            // `--force`: up without asking (v4); the pull question is still asked.
            (
                true,
                &[true],
                flags(false, false, true, false),
                &[
                    "plan",
                    "ask pull=true",
                    "install",
                    "report",
                    "pull",
                    "up",
                    "extensions",
                ],
            ),
            // `--no-pull`: never asked, never pulled.
            (
                true,
                &[false],
                flags(false, false, false, true),
                &["plan", "ask up=false", "install"],
            ),
            (
                true,
                &[],
                flags(true, false, false, true),
                &[
                    "plan",
                    "ask up=true",
                    "install",
                    "report",
                    "info",
                    "up",
                    "extensions",
                ],
            ),
            (
                true,
                &[],
                flags(false, true, true, true),
                &["plan", "install", "report", "info", "up", "extensions"],
            ),
            // No stack on this root (or no Docker): no stack question at all.
            (
                false,
                &[],
                flags(false, false, false, false),
                &["plan", "install"],
            ),
            (
                false,
                &[],
                flags(true, false, true, false),
                &["plan", "install"],
            ),
        ];
        for (stack, answers, flags, expected) in cases {
            let mut fake = Fake::new(*stack, answers);
            let result = run_upgrade(&mut fake, *flags).unwrap();
            assert!(result.success, "{result:?}");
            assert_eq!(steps(&fake), *expected, "{flags:?} {answers:?}");
            assert!(fake.answers.is_empty(), "unused answers: {flags:?}");
            assert_questions_first(&fake);
        }
    }

    #[test]
    fn a_refused_cancelled_or_failed_installation_runs_no_stack_step() {
        // Refused or cancelled while planning: nothing else is asked.
        let mut fake = Fake::new(true, &[]);
        fake.plan = PlanOutcome::Stop(SuccessTuple::fail("Upgrade cancelled."));
        let result = run_upgrade(&mut fake, flags(true, false, true, false)).unwrap();
        assert!(!result.success);
        assert_eq!(steps(&fake), ["plan"]);
        // The installation fails after the questions: no pull, no up, its failure is the result.
        let mut fake = Fake::new(true, &[]);
        fake.install_ok = false;
        let result = run_upgrade(&mut fake, flags(true, false, false, false)).unwrap();
        assert_eq!(result.message, "Failed to upgrade Meerschaum.");
        assert_eq!(
            steps(&fake),
            ["plan", "ask pull=true", "ask up=true", "install"]
        );
    }

    #[test]
    fn a_failed_pull_is_reported_after_the_installation_and_skips_the_stack() {
        let mut fake = Fake::new(true, &[]);
        fake.pull_ok = false;
        let result = run_upgrade(&mut fake, flags(true, false, false, false)).unwrap();
        // The upgrade itself succeeded and says so first.
        assert!(result.success, "{result:?}");
        assert_eq!(
            fake.events[..5],
            [
                "plan",
                "ask pull=true",
                "ask up=true",
                "install",
                "report Installed Meerschaum 5.1.0."
            ]
        );
        assert_eq!(steps(&fake)[5..], ["pull", "warn", "info"]);
        assert!(fake.events[6].contains("Meerschaum itself was upgraded"));
        assert!(
            fake.events[7].contains("Not bringing up the stack, because pulling its images failed")
        );
        assert!(
            result.message.contains("The stack was not brought up."),
            "{}",
            result.message
        );
        // A failed `stack up` is reported the same way, without failing the upgrade.
        let mut fake = Fake::new(true, &[]);
        fake.up_ok = false;
        let result = run_upgrade(&mut fake, flags(true, false, false, false)).unwrap();
        assert!(result.success);
        assert_eq!(steps(&fake)[4..], ["report", "pull", "up", "warn"]);
    }

    #[test]
    fn an_installation_that_is_already_current_still_offers_the_stack_steps() {
        let mut fake = Fake::new(true, &[true, false]);
        fake.plan =
            PlanOutcome::Current(SuccessTuple::ok("Meerschaum 5.1.0 is already installed."));
        let result = run_upgrade(&mut fake, flags(false, false, false, false)).unwrap();
        assert!(result.success);
        assert_eq!(
            steps(&fake),
            ["plan", "ask pull=true", "ask up=false", "report", "pull"]
        );
    }
}
