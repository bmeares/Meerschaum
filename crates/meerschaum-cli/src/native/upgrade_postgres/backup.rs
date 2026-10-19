//! `backup postgres`: an ONLINE backup of the stack's database. Nothing is stopped: each
//! database's dump is one consistent snapshot (`pg_dump --snapshot` of a snapshot a session
//! exports, with the database's structure and a row estimate recorded in the same snapshot).
//!
//! The backup goes to `<root>/backups/postgres/<UTC timestamp>.partial/` and is renamed to
//! `<UTC timestamp>/` once every file is written, hashed and listed with `pg_restore --list`;
//! a failed backup removes its `.partial` directory. Directories are 0700 and files 0600 (the
//! roles' dump holds password hashes). After a successful backup, the oldest backups beyond
//! `system:backups:postgres:keep` are removed (only directories whose manifest says they are
//! complete backups; an upgrade's or a restore's directory is never pruned).
//!
//! It asks nothing, so it runs as a scheduled job: `mrsm backup postgres -s 'daily' -d`.

use std::path::{Path, PathBuf};

use serde_json::{Value as Json, json};

use meerschaum_core::SuccessTuple;
use meerschaum_core::config::GetOptions;

use crate::builtin::ActionResult;
use crate::dispatch::Runtime;

use super::docker::{self, PgContainer, Res, StepError};
use super::dump::{self, Counts, DumpSet};
use super::server::{ServerInfo, Tools};
use super::stackcfg;
use super::{DB_SERVICE, DOCS_HINT, Options, Steps, format_duration, human_bytes};

/// The config key that keeps only the newest N backups.
pub(crate) const KEEP_KEYS: &[&str] = &["system", "backups", "postgres", "keep"];

/// `system:backups:postgres:keep` (`None`: keep every backup).
fn keep(rt: &mut Runtime<'_>) -> Option<usize> {
    let (valid, value) = rt.config().get_with(
        KEEP_KEYS,
        GetOptions {
            patch: false,
            substitute: true,
            write_missing: false,
            warn: false,
        },
    );
    if !valid {
        return None;
    }
    meerschaum_core::pyfmt::py_int(&value)
        .and_then(|n| usize::try_from(n).ok())
        .filter(|n| *n > 0)
}

/// The complete backups in `backups`, newest first.
pub(crate) fn complete_backups(backups: &Path) -> Vec<(PathBuf, Json)> {
    let Ok(entries) = std::fs::read_dir(backups) else {
        return Vec::new();
    };
    let mut dirs: Vec<PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    dirs.sort();
    dirs.into_iter()
        .rev()
        .filter_map(|dir| {
            let manifest = super::read_manifest(&dir)?;
            let complete = manifest.get("kind").and_then(Json::as_str) == Some("backup")
                && manifest.get("state").and_then(Json::as_str) == Some("complete")
                && manifest.get("dumps").is_some();
            complete.then_some((dir, manifest))
        })
        .collect()
}

/// Remove the oldest complete backups beyond `keep`. Returns the removed directories and the
/// problems.
pub(crate) fn prune(backups: &Path, keep: usize) -> (Vec<PathBuf>, Vec<String>) {
    let mut removed = Vec::new();
    let mut problems = Vec::new();
    for (dir, _) in complete_backups(backups).into_iter().skip(keep) {
        // Only what this module wrote: a directory directly under `backups`.
        if dir.parent() != Some(backups) {
            continue;
        }
        match std::fs::remove_dir_all(&dir) {
            Ok(()) => removed.push(dir),
            Err(e) => problems.push(format!("Cannot remove '{}': {e}", dir.display())),
        }
    }
    (removed, problems)
}

/// `backup postgres`.
pub(crate) fn backup(rt: &mut Runtime<'_>, opts: &Options) -> ActionResult {
    let _signals = super::signals::Session::begin(rt);
    let (stack, _locks) = match super::prologue(rt, false) {
        Ok(ready) => ready,
        Err(e) => return Ok(SuccessTuple::fail(e)),
    };
    let mut warnings = Vec::new();
    // Killed backups (no backup is live under the locks), and what interrupted runs left.
    let (removed, problems) = super::leftovers::remove_partial_backups(&stack.backups);
    for dir in &removed {
        super::super::info(
            rt,
            &format!("Removed an unfinished backup: {}", dir.display()),
        );
    }
    warnings.extend(problems);
    if let Ok(found) = super::leftovers::find(&stack)
        && !found.is_empty()
    {
        warnings.push(format!(
            "{}Run `mrsm upgrade postgres rollback` to put this back.",
            super::leftovers::describe(&stack, &found)
        ));
    }
    let Some(db) = stack.db().cloned() else {
        return Ok(SuccessTuple::fail(
            "The stack has no `db` service, so there is no database to back up.",
        ));
    };
    let container = match stack.compose.container_id(DB_SERVICE) {
        Ok(Some(id)) => id,
        Ok(None) => return Ok(SuccessTuple::fail(super::plan::not_running(&stack, &db))),
        Err(e) => return Ok(SuccessTuple::fail(e.message)),
    };
    let server = PgContainer::new(container);
    let info = match ServerInfo::read(&server) {
        Ok(info) => info,
        Err(e) => return Ok(SuccessTuple::fail(e.message)),
    };
    if !info.superuser || info.major < super::server::MIN_MAJOR {
        let mut checked = info.clone();
        checked.tablespaces.clear();
        checked.unmovable.clear();
        if let Some(refusal) = checked.refusal(DOCS_HINT) {
            return Ok(SuccessTuple::fail(refusal));
        }
    }
    if !info.unmovable.is_empty() {
        warnings.push(format!(
            "Not backed up (templates, or databases that refuse connections): {}.",
            info.unmovable.join(", ")
        ));
    }
    if !info.tablespaces.is_empty() {
        warnings.push(format!(
            "The server has tablespaces ({}): a restore needs them to exist.",
            info.tablespaces.join(", ")
        ));
    }
    if info.databases.is_empty() {
        return Ok(SuccessTuple::fail("The server has no database to back up."));
    }
    let total = info.total_size();
    if let Some(free) = super::server::host_free_bytes(&stack.backups)
        && free < total / 2
        && !opts.force
    {
        return Ok(SuccessTuple::fail(format!(
            "Only {} free in {} for the backup of {} of databases. Free some space, or pass \
             `--force` to try anyway.",
            human_bytes(free),
            stack.backups.display(),
            human_bytes(total)
        )));
    }
    let image = db
        .get("image")
        .and_then(Json::as_str)
        .unwrap_or_default()
        .to_string();
    let volume = stackcfg::data_volume(
        &db,
        stack.resolved.get("volumes").unwrap_or(&Json::Null),
        &info.data_directory,
    )
    .ok()
    .map(|v| v.name);

    let run_id = super::fresh_run_id(&stack.backups);
    let partial = stack.backups.join(format!("{run_id}.partial"));
    let finished = stack.backups.join(&run_id);
    let mut steps = Steps::new();
    let result = (|| -> Res<DumpSet> {
        docker::private_dir(&partial)?;
        let names: Vec<String> = info.databases.iter().map(|d| d.name.clone()).collect();
        let tools = Tools::Container(server.clone());
        let set = dump::dump(
            rt,
            &mut steps,
            &server,
            &tools,
            &names,
            &partial,
            Counts::Estimate,
        )?;
        let manifest = json!({
            "format": 1,
            "kind": "backup",
            "run_id": run_id,
            "created": chrono::Utc::now().to_rfc3339(),
            "meerschaum": crate::VERSION,
            "state": "complete",
            "project": stack.compose.project,
            "server": {
                "image": image,
                "server_version": info.version,
                "major": info.major,
                "volume": volume,
            },
            "settings": info.settings,
            "seconds": steps.started.elapsed().as_secs_f64(),
            "dumps": set.to_json(),
        });
        super::write_manifest(&partial, &manifest)?;
        std::fs::rename(&partial, &finished).map_err(|e| {
            StepError::new(format!(
                "Cannot rename '{}' to '{}': {e}",
                partial.display(),
                finished.display()
            ))
        })?;
        Ok(set)
    })();
    let set = match result {
        Ok(set) => set,
        Err(e) => {
            // Everything in the `.partial` directory is this run's.
            let removed = super::signals::shielded(|| {
                let _ = docker::remove_run_containers(super::LABEL, &run_id);
                std::fs::remove_dir_all(&partial).is_ok() || !partial.exists()
            });
            return Ok(SuccessTuple::fail(format!(
                "The backup {}:\n{}\n{}",
                if e.interrupted {
                    "was stopped"
                } else {
                    "failed"
                },
                e.message,
                if removed {
                    "Its partial files were removed.".to_string()
                } else {
                    format!("Remove the partial files in {}.", partial.display())
                }
            )));
        }
    };
    let mut text = format!(
        "Backed up {} database{} of PostgreSQL {} in {} to\n  {}\n",
        set.databases.len(),
        if set.databases.len() == 1 { "" } else { "s" },
        info.version,
        format_duration(steps.started.elapsed()),
        finished.display()
    );
    text.push_str(&format!(
        "  {:<28} {:>10}\n",
        set.globals.file,
        human_bytes(set.globals.bytes as i64)
    ));
    for db in &set.databases {
        let estimate = db
            .fingerprint
            .get("row_estimate")
            .and_then(Json::as_i64)
            .map(|n| format!("  (~{} rows)", super::super::thousands(n)))
            .unwrap_or_default();
        text.push_str(&format!(
            "  {:<28} {:>10}{estimate}\n",
            db.dump.file,
            human_bytes(db.dump.bytes as i64)
        ));
    }
    text.push_str(&format!(
        "  {:<28} {:>10}  ({})",
        "total",
        human_bytes(set.total_bytes() as i64),
        set.compression
    ));
    if let Some(keep) = keep(rt) {
        let (removed, problems) = prune(&stack.backups, keep);
        if !removed.is_empty() {
            text.push_str(&format!(
                "\nRemoved {} older backup{} (keeping {keep}, `system:backups:postgres:keep`):",
                removed.len(),
                if removed.len() == 1 { "" } else { "s" }
            ));
            for dir in &removed {
                text.push_str(&format!("\n  {}", dir.display()));
            }
        }
        warnings.extend(problems);
    }
    for warning in &warnings {
        super::super::warn(rt, warning);
    }
    Ok(SuccessTuple::ok(text))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make(dir: &Path, name: &str, kind: &str, state: &str) {
        let run = dir.join(name);
        std::fs::create_dir_all(&run).unwrap();
        super::super::write_manifest(&run, &json!({"kind": kind, "state": state, "dumps": {}}))
            .unwrap();
    }

    #[test]
    fn retention_removes_only_the_oldest_complete_backups() {
        let dir = tempfile::tempdir().unwrap();
        make(dir.path(), "20261001T000000Z", "backup", "complete");
        make(dir.path(), "20261002T000000Z", "upgrade", "switched");
        make(dir.path(), "20261003T000000Z", "backup", "complete");
        make(dir.path(), "20261004T000000Z", "restore", "switched");
        make(dir.path(), "20261005T000000Z", "backup", "complete");
        std::fs::create_dir_all(dir.path().join("20261006T000000Z.partial")).unwrap();
        std::fs::create_dir_all(dir.path().join("not-a-backup")).unwrap();
        assert_eq!(complete_backups(dir.path()).len(), 3);
        let (removed, problems) = prune(dir.path(), 2);
        assert!(problems.is_empty());
        assert_eq!(removed, vec![dir.path().join("20261001T000000Z")]);
        for kept in [
            "20261002T000000Z",
            "20261003T000000Z",
            "20261004T000000Z",
            "20261005T000000Z",
            "20261006T000000Z.partial",
            "not-a-backup",
        ] {
            assert!(dir.path().join(kept).exists(), "{kept}");
        }
        assert!(prune(dir.path(), 2).0.is_empty());
    }
}
