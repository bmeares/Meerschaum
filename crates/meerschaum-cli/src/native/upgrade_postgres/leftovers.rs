//! What an interrupted run left behind, found and put back by the next one.
//!
//! A run that ends, even after a Ctrl-C or a closed terminal, puts things back itself. One that is
//! killed (`kill -9`, a power cut, a session whose processes are all killed) leaves its manifest
//! in a state that is not final, and maybe: its temporary containers, the new volume, the stack's
//! `db` and the services that use it stopped, `stack.yaml` edited, TimescaleDB's background jobs
//! paused on the old server. Every one of these is recorded in the manifest before it is done.
//!
//! The next `upgrade postgres` or `restore postgres` (which holds the locks, so no run is live)
//! says what it found and puts it back after asking; `rollback` does the same; `backup postgres`
//! only warns. A run killed after its switch went through (the new database runs and was
//! checked) is recorded as switched instead: its rollback stays available.

use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use regex::Regex;
use serde_json::{Value as Json, json};

use crate::dispatch::Runtime;

use super::docker::{self, PgContainer, Res};
use super::signals;
use super::switch::{self, OldSide, Undo, check_running};
use super::{DB_SERVICE, LABEL, Stack, same_path};

/// An interrupted run of this root.
#[derive(Debug, Clone)]
pub(crate) struct Interrupted {
    pub dir: PathBuf,
    pub manifest: Json,
    pub run_id: String,
    /// Its temporary containers that are still there.
    pub containers: Vec<String>,
    /// Its new volume, when it exists and carries the run's label.
    pub volume: Option<String>,
}

impl Interrupted {
    fn text(&self, keys: &[&str]) -> String {
        let mut value = &self.manifest;
        for key in keys {
            match value.get(key) {
                Some(v) => value = v,
                None => return String::new(),
            }
        }
        value.as_str().unwrap_or_default().to_string()
    }

    fn list(&self, key: &str) -> Vec<String> {
        self.manifest
            .get(key)
            .and_then(Json::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Json::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default()
    }

    fn flag(&self, key: &str) -> bool {
        self.manifest.get(key).and_then(Json::as_bool) == Some(true)
    }

    /// The stack's `db` was down when the run began (only a restore records it).
    fn db_down_before(&self) -> bool {
        self.manifest.get("db_running").and_then(Json::as_bool) == Some(false)
    }

    fn major(&self, side: &str) -> u32 {
        self.manifest
            .get(side)
            .and_then(|s| s.get("major"))
            .and_then(Json::as_u64)
            .unwrap_or(0) as u32
    }
}

/// What interrupted runs left.
#[derive(Debug, Clone, Default)]
pub(crate) struct Leftovers {
    pub runs: Vec<Interrupted>,
    /// Temporary containers of runs this root has no manifest of (a killed preflight).
    pub orphans: Vec<String>,
}

impl Leftovers {
    pub fn is_empty(&self) -> bool {
        self.runs.is_empty() && self.orphans.is_empty()
    }
}

/// The names of the temporary containers of a project's runs: `<project>-pg<role>-<run id>`.
fn temporary_name(project: &str) -> Regex {
    Regex::new(&format!(
        r"^{}-pg(check|upgrade|restore|dump)-\d{{8}}t\d{{6}}z(-\d+)?$",
        regex::escape(project)
    ))
    .expect("regex")
}

/// What interrupted runs of this root's stack left.
pub(crate) fn find(stack: &Stack) -> Res<Leftovers> {
    let names = temporary_name(&stack.project_label());
    let ours: Vec<(String, String)> = docker::containers_with(&format!("label={LABEL}"))?
        .into_iter()
        .filter(|c| names.is_match(&c.name))
        .map(|c| {
            let run = c.labels.get(LABEL).cloned().unwrap_or_default();
            (c.name, run)
        })
        .collect();
    let mut runs = Vec::new();
    for (dir, manifest) in super::runs(&stack.backups) {
        if !super::interrupted(&manifest)
            || manifest.get("project").and_then(Json::as_str) != Some(&stack.compose.project)
        {
            continue;
        }
        let run_id = manifest
            .get("run_id")
            .and_then(Json::as_str)
            .unwrap_or_default()
            .to_string();
        let containers = ours
            .iter()
            .filter(|(_, run)| *run == run_id)
            .map(|(name, _)| name.clone())
            .collect();
        let volume = manifest
            .pointer("/new/volume")
            .and_then(Json::as_str)
            .filter(|v| {
                manifest.get("rollback_pending").and_then(Json::as_bool) != Some(true)
                    && !v.is_empty()
                    && docker::volume_has_label(v, LABEL, &run_id)
            })
            .map(str::to_string);
        runs.push(Interrupted {
            dir,
            manifest,
            run_id,
            containers,
            volume,
        });
    }
    let orphans = ours
        .iter()
        .filter(|(_, run)| !runs.iter().any(|r| r.run_id == *run))
        .map(|(name, _)| name.clone())
        .collect();
    Ok(Leftovers { runs, orphans })
}

/// The stack's `db` runs the run's new volume at its new major (its switch went through).
fn switched(stack: &Stack, run: &Interrupted) -> bool {
    if run.manifest.get("config").is_none()
        || run.manifest.get("state").and_then(Json::as_str) != Some("verified")
    {
        return false;
    }
    let volume = run.text(&["new", "volume"]);
    matches!(stack.compose.container_id(DB_SERVICE), Ok(Some(id))
        if check_running(&id, &volume, run.major("new")).is_ok())
}

/// What was found, for the user.
pub(crate) fn describe(stack: &Stack, found: &Leftovers) -> String {
    let mut out = String::new();
    let db_running = matches!(stack.compose.container_id(DB_SERVICE), Ok(Some(_)));
    for run in &found.runs {
        let kind = run.text(&["kind"]);
        if run.flag("rollback_pending") {
            out.push_str(&format!("An interrupted rollback (run {}) will be completed on the old volume '{}'; both database volumes are kept.\n", run.run_id, run.text(&["old", "volume"])));
            continue;
        }
        out.push_str(&format!(
            "An earlier `{kind} postgres` (run {}, {}) was interrupted ({}).\n",
            run.run_id,
            run.dir.display(),
            run.text(&["state"])
        ));
        if switched(stack, run) {
            out.push_str(&format!(
                "  - Its switch went through: the stack's database runs on '{}'. That is kept, and \
                 the run is recorded as done (`mrsm {kind} postgres rollback` goes back).\n",
                run.text(&["new", "volume"])
            ));
            let stopped = run.list("stopped");
            if !stopped.is_empty() {
                out.push_str(&format!("  - {} are started again.\n", stopped.join(", ")));
            }
            continue;
        }
        out.push_str("  Put back as it was before that run:\n");
        if switch::recovery_starts_old_db(
            run.flag("old_db_stopped"),
            run.db_down_before(),
            run.manifest.get("config").is_some(),
            db_running,
        ) {
            out.push_str(&format!(
                "  - the stack's `db` is started on '{}'\n",
                run.text(&["old", "volume"])
            ));
        } else if run.db_down_before() {
            out.push_str("  - the stack's `db` stays stopped, as it was before that run\n");
        }
        let stopped = run.list("stopped");
        if !stopped.is_empty() {
            out.push_str(&format!("  - {} are started again\n", stopped.join(", ")));
        }
        if run.manifest.get("config").is_some() {
            out.push_str("  - the stack config is put back from the run's copy\n");
        }
        let paused = run.list("paused");
        if !paused.is_empty() && !run.flag("old_db_stopped") && db_running {
            out.push_str(&format!(
                "  - TimescaleDB's background jobs resume in {}\n",
                paused.join(", ")
            ));
        }
        if !run.containers.is_empty() {
            out.push_str(&format!(
                "  - its temporary containers are removed: {}\n",
                run.containers.join(", ")
            ));
        }
        if let Some(volume) = &run.volume {
            out.push_str(&format!(
                "  - its new volume '{volume}' (an unfinished copy) is removed\n"
            ));
        }
    }
    if !found.orphans.is_empty() {
        out.push_str(&format!(
            "Temporary containers of interrupted runs are removed: {}\n",
            found.orphans.join(", ")
        ));
    }
    out
}

/// The stack config file a manifest says its run changed, checked against this root (it must be
/// this root's stack config file, and its copy a file of the run's directory): `(path, original
/// bytes or None when there was no file, the file written)`.
pub(crate) type ConfigCopy = (PathBuf, Option<Vec<u8>>, PathBuf);

pub(crate) fn manifest_config(
    stack: &Stack,
    dir: &Path,
    manifest: &Json,
) -> Result<Option<ConfigCopy>, String> {
    let Some(config) = manifest.get("config").filter(|c| c.is_object()) else {
        return Ok(None);
    };
    let text = |k: &str| config.get(k).and_then(Json::as_str).unwrap_or_default();
    let path = PathBuf::from(text("path"));
    let written = match text("written") {
        "" => path.clone(),
        w => PathBuf::from(w),
    };
    let current = stack.config_file();
    let ours = |p: &Path| {
        same_path(p, &current)
            || (p.parent().is_some_and(|d| same_path(d, &stack.config_dir))
                && p.file_name().is_some_and(|n| {
                    ["stack.yaml", "stack.yml", "stack.json"]
                        .contains(&n.to_string_lossy().as_ref())
                }))
    };
    if path.as_os_str().is_empty() || !same_path(&path, &current) || !ours(&written) {
        return Err(format!(
            "The run in {} changed '{}', which is not this root's stack config ('{}'): it is not \
             put back from here.",
            dir.display(),
            path.display(),
            current.display()
        ));
    }
    let existed = config.get("existed").and_then(Json::as_bool) == Some(true);
    let original = if existed {
        let backup = text("backup");
        let plain = !backup.is_empty()
            && Path::new(backup).file_name().map(|n| n.to_string_lossy()) == Some(backup.into());
        if !plain {
            return Err(format!(
                "The manifest in {} does not name its copy of the stack config.",
                dir.display()
            ));
        }
        let copy = dir.join(backup);
        Some(std::fs::read(&copy).map_err(|e| {
            format!(
                "Cannot read the stack config copy '{}': {e}",
                copy.display()
            )
        })?)
    } else {
        None
    };
    Ok(Some((current, original, written)))
}

/// Put back what one interrupted run left. Returns what could not be put back.
fn put_back_run(rt: &mut Runtime<'_>, stack: &Stack, run: &Interrupted) -> Vec<String> {
    let mut manifest = run.manifest.clone();
    if run.flag("rollback_pending") {
        // A forced interruption of rollback completes its requested move to the old side.
        // The newer volume is a verified database, never disposable cleanup material.
        let result = (|| -> Result<(), String> {
            let (path, original, written) = manifest_config(stack, &run.dir, &manifest)?
                .ok_or_else(|| "The interrupted rollback has no stack config copy.".to_string())?;
            let mut services = run.list("stopped");
            services.push(DB_SERVICE.to_string());
            stack
                .compose
                .stop(&services, switch::STOP_TIMEOUT_SECONDS)
                .map_err(|e| e.message)?;
            super::restore_config_file(&path, original.as_deref(), Some(&written))?;
            super::forget_stack_config(rt);
            super::super::os_sql_stack::write_stack(rt)?;
            stack
                .compose
                .up(&[DB_SERVICE.to_string()])
                .map_err(|e| e.message)?;
            let id = docker::wait_healthy(&stack.compose, DB_SERVICE, switch::START_TIMEOUT)
                .map_err(|e| e.message)?;
            check_running(&id, &run.text(&["old", "volume"]), run.major("old"))
                .map_err(|e| e.message)?;
            stack
                .compose
                .up(&run.list("stopped"))
                .map_err(|e| e.message)?;
            Ok(())
        })();
        match result {
            Ok(()) => {
                manifest["rollback_pending"] = json!(false);
                super::set_state(&run.dir, &mut manifest, "rolled_back", None);
                return Vec::new();
            }
            Err(e) => {
                super::set_state(&run.dir, &mut manifest, "recovery_failed", Some(&e));
                return vec![e];
            }
        }
    }
    if switched(stack, run) {
        let mut problems = Vec::new();
        let stopped = run.list("stopped");
        if let Err(e) = stack.compose.up(&stopped) {
            problems.push(e.message);
        }
        manifest["note"] = json!("Its end was not recorded; recorded by a later run.");
        super::set_state(&run.dir, &mut manifest, "switched", None);
        return problems;
    }
    let config = match manifest_config(stack, &run.dir, &manifest) {
        Ok(config) => config,
        // Without its config, nothing else is put back either (the old `db` would start on the
        // edited config).
        Err(e) => return vec![e],
    };
    let db_running = matches!(stack.compose.container_id(DB_SERVICE), Ok(Some(_)));
    let mut undo = Undo {
        stopped: run.list("stopped"),
        isolated: if run.flag("networks_restored") {
            Vec::new()
        } else {
            run.manifest
                .get("isolated_networks")
                .cloned()
                .and_then(|v| serde_json::from_value(v).ok())
                .unwrap_or_default()
        },
        isolated_container: run
            .manifest
            .get("isolated_container")
            .and_then(Json::as_str)
            .map(str::to_string),
        paused: run.list("paused"),
        new_volume: run.volume.clone(),
        old_db_stopped: switch::recovery_starts_old_db(
            run.flag("old_db_stopped"),
            run.db_down_before(),
            config.is_some(),
            db_running,
        ),
        db_down_before: run.db_down_before(),
        config,
        new_volume_outcome: None,
    };
    let server = match stack.compose.container_id(DB_SERVICE) {
        Ok(Some(id)) if !undo.old_db_stopped => Some(PgContainer::new(id)),
        _ => None,
    };
    let old_volume = run.text(&["old", "volume"]);
    let old = OldSide {
        volume: &old_volume,
        major: run.major("old"),
        server: server.as_ref(),
    };
    let problems = switch::put_back(rt, false, stack, &old, &mut undo, &run.run_id);
    if let Some(outcome) = &undo.new_volume_outcome {
        super::super::info(rt, outcome);
    }
    super::set_state(
        &run.dir,
        &mut manifest,
        if problems.is_empty() {
            "failed"
        } else {
            "recovery_failed"
        },
        Some("Interrupted; recovery attempted by a later run."),
    );
    problems
}

/// Put back everything `found` lists (signals are ignored meanwhile). Returns what could not be
/// put back.
pub(crate) fn put_back(rt: &mut Runtime<'_>, stack: &Stack, found: &Leftovers) -> Vec<String> {
    signals::shielded(|| {
        let mut problems = Vec::new();
        for run in &found.runs {
            problems.extend(put_back_run(rt, stack, run));
        }
        for name in &found.orphans {
            let run = docker::container_label(name, LABEL).unwrap_or_default();
            if let Err(e) = docker::remove_run_container(name, LABEL, &run) {
                problems.push(e.message);
            }
        }
        problems
    })
}

/// Before an upgrade or a restore: put back what interrupted runs left, after asking. `Err` with
/// the command's result when the user declines or something could not be put back.
pub(crate) fn settle(
    rt: &mut Runtime<'_>,
    opts: &super::Options,
    stack: &Stack,
    command: &str,
) -> Result<bool, meerschaum_core::SuccessTuple> {
    let found = find(stack).map_err(|e| meerschaum_core::SuccessTuple::fail(e.message))?;
    if found.is_empty() {
        return Ok(false);
    }
    rt.print(&format!("\n{}\n", describe(stack, &found)));
    let yes = super::ask(
        rt,
        "Put these back now (this comes before anything else)?",
        "y",
        opts,
    )
    .map_err(|e| meerschaum_core::SuccessTuple::fail(e.message))?;
    if !yes {
        return Err(meerschaum_core::SuccessTuple::fail(format!(
            "Nothing was changed. An interrupted run must be put back before `{command}` (answer \
             yes, or run `mrsm upgrade postgres rollback`)."
        )));
    }
    let problems = put_back(rt, stack, &found);
    if !problems.is_empty() {
        return Err(meerschaum_core::SuccessTuple::fail(format!(
            "Putting back what the interrupted run left did not fully work:\n  - {}\nCheck the \
             stack with `mrsm stack ps` before trying again.",
            problems.join("\n  - ")
        )));
    }
    Ok(true)
}

/// Remove `<timestamp>.partial` directories (killed backups; no backup is live under the locks).
/// Returns the removed directories and the problems.
pub(crate) fn remove_partial_backups(backups: &Path) -> (Vec<PathBuf>, Vec<String>) {
    static PARTIAL: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^\d{8}T\d{6}Z\.partial$").expect("regex"));
    let mut removed = Vec::new();
    let mut problems = Vec::new();
    let Ok(entries) = std::fs::read_dir(backups) else {
        return (removed, problems);
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if !PARTIAL.is_match(&name) || !path.is_dir() || path.parent() != Some(backups) {
            continue;
        }
        match std::fs::remove_dir_all(&path) {
            Ok(()) => removed.push(path),
            Err(e) => problems.push(format!("Cannot remove '{}': {e}", path.display())),
        }
    }
    removed.sort();
    (removed, problems)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temporary_containers_are_named_by_project_role_and_run() {
        let names = temporary_name("mrsm");
        for name in [
            "mrsm-pgcheck-20261018t120000z",
            "mrsm-pgupgrade-20261018t120000z",
            "mrsm-pgrestore-20261018t120000z",
            "mrsm-pgdump-20261018t120000z-3",
        ] {
            assert!(names.is_match(name), "{name}");
        }
        for name in [
            "mrsm-db-1",
            "mrsm-pgx-pgupgrade-20261018t120000z",
            "other-pgupgrade-20261018t120000z",
            "mrsm-pgupgrade-20261018t120000z-extra",
        ] {
            assert!(!names.is_match(name), "{name}");
        }
    }

    #[test]
    fn manifest_paths_cannot_write_outside_this_roots_stack_config() {
        let dir = tempfile::tempdir().unwrap();
        let config_dir = dir.path().join("config");
        std::fs::create_dir(&config_dir).unwrap();
        let config = config_dir.join("stack.yaml");
        std::fs::write(&config, "project_name: scratch\n").unwrap();
        let run = dir.path().join("run");
        std::fs::create_dir(&run).unwrap();
        std::fs::write(run.join("stack.original"), "original\n").unwrap();
        let stack = Stack {
            compose: docker::Compose {
                project: "scratch".into(),
                cwd: dir.path().join("stack"),
                env: Vec::new(),
            },
            resolved: json!({}),
            raw: meerschaum_core::config::ConfigValue::Map(Default::default()),
            file: Some(config.clone()),
            compose_path: dir.path().join("stack/docker-compose.yaml"),
            config_dir: config_dir.clone(),
            backups: dir.path().join("backups"),
        };
        let valid = json!({"config": {"path": config, "written": config, "existed": true, "backup": "stack.original"}});
        assert_eq!(
            manifest_config(&stack, &run, &valid)
                .unwrap()
                .unwrap()
                .1
                .unwrap(),
            b"original\n"
        );
        for (key, value) in [
            (
                "path",
                dir.path()
                    .join("foreign.yaml")
                    .to_string_lossy()
                    .into_owned(),
            ),
            (
                "written",
                dir.path()
                    .join("foreign.yaml")
                    .to_string_lossy()
                    .into_owned(),
            ),
            ("backup", "../foreign.yaml".to_string()),
            (
                "backup",
                dir.path()
                    .join("foreign.yaml")
                    .to_string_lossy()
                    .into_owned(),
            ),
        ] {
            let mut manifest = valid.clone();
            manifest["config"][key] = json!(value);
            assert!(
                manifest_config(&stack, &run, &manifest).is_err(),
                "{key}: {value}"
            );
        }
        assert_eq!(
            std::fs::read_to_string(&config).unwrap(),
            "project_name: scratch\n"
        );
    }

    #[test]
    fn only_killed_backups_are_removed() {
        let dir = tempfile::tempdir().unwrap();
        for name in [
            "20261001T000000Z.partial",
            "20261001T000000Z",
            "notes.partial",
            "20261002T000000Z.partial.keep",
        ] {
            std::fs::create_dir_all(dir.path().join(name)).unwrap();
        }
        std::fs::write(dir.path().join("20261003T000000Z.partial"), "a file").unwrap();
        let (removed, problems) = remove_partial_backups(dir.path());
        assert!(problems.is_empty());
        assert_eq!(removed, vec![dir.path().join("20261001T000000Z.partial")]);
        for kept in [
            "20261001T000000Z",
            "notes.partial",
            "20261002T000000Z.partial.keep",
            "20261003T000000Z.partial",
        ] {
            assert!(dir.path().join(kept).exists(), "{kept}");
        }
    }

    #[test]
    fn a_run_is_interrupted_until_its_state_is_final() {
        for (kind, state, interrupted) in [
            ("upgrade", "dumped", true),
            ("restore", "verified", true),
            ("upgrade", "started", true),
            ("upgrade", "switched", false),
            ("restore", "failed", false),
            ("upgrade", "rolled_back", false),
            ("backup", "complete", false),
        ] {
            assert_eq!(
                super::super::interrupted(&json!({"kind": kind, "state": state})),
                interrupted,
                "{kind} {state}"
            );
        }
    }
}
