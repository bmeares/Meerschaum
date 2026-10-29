//! `upgrade postgres rollback` (also `restore postgres rollback`): bring the stack back to what
//! the latest upgrade or restore replaced: the old image on the old volume (which neither ever
//! modifies), with the stack config file it had, byte for byte. The new volume is kept.

use serde_json::Value as Json;

use meerschaum_core::SuccessTuple;

use crate::builtin::ActionResult;
use crate::dispatch::Runtime;

use super::docker::{self, Res, StepError};
use super::switch::{START_TIMEOUT, STOP_TIMEOUT_SECONDS, check_running};
use super::{DB_SERVICE, Options, leftovers, signals};

/// The Docker names of the volumes the resolved `db` service mounts.
fn db_volume_names(resolved: &Json) -> Vec<String> {
    let Some(db) = resolved.get("services").and_then(|s| s.get(DB_SERVICE)) else {
        return Vec::new();
    };
    db.get("volumes")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .filter(|m| m.get("type").and_then(Json::as_str) == Some("volume"))
        .filter_map(|m| m.get("source").and_then(Json::as_str))
        .map(|key| {
            resolved
                .get("volumes")
                .and_then(|v| v.get(key))
                .and_then(|v| v.get("name"))
                .and_then(Json::as_str)
                .unwrap_or(key)
                .to_string()
        })
        .collect()
}

/// Whether the stack config file is no longer what the switch wrote: `expected` is the SHA-256 the
/// run's manifest records (runs of earlier versions record none), `current` the file's now.
pub(crate) fn edited_since_switch(expected: Option<&str>, current: Option<&str>) -> bool {
    matches!((expected, current), (Some(e), Some(c)) if e != c)
}

fn text<'a>(value: &'a Json, keys: &[&str]) -> &'a str {
    let mut current = value;
    for key in keys {
        match current.get(key) {
            Some(v) => current = v,
            None => return "",
        }
    }
    current.as_str().unwrap_or("")
}

/// `upgrade postgres rollback` / `restore postgres rollback`.
pub(crate) fn rollback(rt: &mut Runtime<'_>, opts: &Options) -> ActionResult {
    let _signals = signals::Session::begin(rt);
    let (stack, _locks) = match super::prologue(rt, true) {
        Ok(value) => value,
        Err(e) => return Ok(SuccessTuple::fail(e)),
    };
    match leftovers::settle(rt, opts, &stack, "rollback") {
        Ok(true) => {
            return Ok(SuccessTuple::ok(
                "The interrupted run was put back. Check the stack before rolling back a completed run.",
            ));
        }
        Ok(false) => {}
        Err(result) => return Ok(result),
    }
    let Some((dir, mut manifest)) = super::latest_run(&stack.backups, "switched") else {
        return Ok(SuccessTuple::fail(format!(
            "Nothing to roll back: no completed `upgrade postgres` or `restore postgres` was \
             found in {}.",
            stack.backups.display()
        )));
    };
    let project = text(&manifest, &["project"]).to_string();
    if project != stack.compose.project {
        return Ok(SuccessTuple::fail(format!(
            "The latest switch ({}) was made for the stack project '{project}', but this root's \
             stack is '{}'.",
            dir.display(),
            stack.compose.project
        )));
    }
    let old_image = text(&manifest, &["old", "image"]).to_string();
    let old_volume = text(&manifest, &["old", "volume"]).to_string();
    let new_volume = text(&manifest, &["new", "volume"]).to_string();
    let old_major = manifest
        .get("old")
        .and_then(|o| o.get("major"))
        .and_then(Json::as_u64)
        .unwrap_or(0) as u32;
    if old_volume.is_empty() || !docker::volume_exists(&old_volume) {
        return Ok(SuccessTuple::fail(format!(
            "The old volume '{old_volume}' no longer exists, so there is nothing to go back to."
        )));
    }
    let (config_path, original, written) = match leftovers::manifest_config(&stack, &dir, &manifest)
    {
        Ok(Some(config)) => config,
        Ok(None) => {
            return Ok(SuccessTuple::fail(format!(
                "The manifest in {} does not name its stack config.",
                dir.display()
            )));
        }
        Err(e) => return Ok(SuccessTuple::fail(e)),
    };
    let existed = original.is_some();
    // Whether the config file is what the switch wrote: a rollback replaces the whole file with
    // the copy from before the switch, so edits made since would otherwise vanish unannounced.
    let edited_since = edited_since_switch(
        manifest
            .pointer("/config/written_sha256")
            .and_then(Json::as_str),
        docker::sha256_file(&written)
            .ok()
            .map(|(sha, _)| sha)
            .as_deref(),
    );
    let saved_name = stack.file.as_ref().map(|file| {
        format!(
            "{}.before-rollback-{}",
            file.file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| "stack.yaml".into()),
            super::run_id()
        )
    });

    let current_image = stack
        .db()
        .and_then(|d| d.get("image"))
        .and_then(Json::as_str)
        .unwrap_or_default()
        .to_string();
    let current_volumes = db_volume_names(&stack.resolved);
    let running = match stack.compose.running_services() {
        Ok(r) => r,
        Err(e) => return Ok(SuccessTuple::fail(e.message)),
    };
    let dependents: Vec<String> = stack
        .dependents()
        .into_iter()
        .filter(|s| running.contains(s))
        .collect();
    let mut text_plan = format!(
        "\nRoll back the stack's database (project '{project}'):\n\n  - the stack config \
         goes back to the copy in {} ({}),\n  - `db` runs '{old_image}' on the old volume \
         '{old_volume}' again{}.\n\nWhat was written to the database since then (in \
         '{new_volume}') is NOT in the old volume. The volume '{new_volume}' is kept.\n",
        dir.display(),
        if existed {
            "byte for byte"
        } else {
            "there was no stack config file: it is removed"
        },
        if dependents.is_empty() {
            String::new()
        } else {
            format!(" ({} restarted)", dependents.join(", "))
        }
    );
    if current_image != text(&manifest, &["new", "image"]) || !current_volumes.contains(&new_volume)
    {
        text_plan.push_str(&format!(
            "\nWarning: the stack changed since then (it runs '{current_image}' on {}); \
             the rollback replaces that too.\n",
            current_volumes.join(", ")
        ));
    }
    if edited_since && let Some(name) = &saved_name {
        text_plan.push_str(&format!(
            "\nWarning: {} was changed after this run (it is not what the switch wrote). The \
             rollback replaces it with the copy from before the run, so those changes are not \
             kept in the stack config; the current file is saved as {}.\n",
            written.display(),
            dir.join(name).display()
        ));
    }
    rt.print(&format!("{text_plan}\n"));
    let question = format!("Roll the stack's database back to PostgreSQL {old_major} now?");
    if !super::ask(rt, &question, "n", opts)? {
        return Ok(SuccessTuple::fail("Nothing was changed."));
    }
    // A rollback has no disposable copy: shield the short stop/config/start transaction from
    // terminal signals, including its recovery path. Never leave it halfway through.
    signals::shielded(|| {
        // The current config is kept next to the run's files before it is replaced.
        let current_file = stack.file.clone();
        let saved = match &current_file {
            Some(file) => match std::fs::read(file) {
                Ok(bytes) => {
                    let name = saved_name
                        .clone()
                        .unwrap_or_else(|| "stack.yaml.before-rollback".into());
                    if let Err(e) = docker::write_private(&dir.join(&name), &bytes) {
                        return Ok(SuccessTuple::fail(format!(
                            "Cannot save the current stack config: {}",
                            e.message
                        )));
                    }
                    Some(bytes)
                }
                Err(e) => {
                    return Ok(SuccessTuple::fail(format!(
                        "Cannot read the current stack config: {e}"
                    )));
                }
            },
            None => None,
        };

        let mut services = dependents.clone();
        services.push(DB_SERVICE.to_string());
        // Journal before the first stop/config mutation. Forced interruption completes this
        // requested rollback on recovery, keeping both verified volumes.
        manifest["rollback_pending"] = serde_json::json!(true);
        manifest["stopped"] = serde_json::json!(dependents);
        manifest["state"] = serde_json::json!("rollback_started");
        if let Err(e) = super::write_manifest(&dir, &manifest) {
            return Ok(SuccessTuple::fail(e));
        }
        let mut login_warning: Option<String> = None;
        let result = (|| -> Res<()> {
            super::super::info(rt, &format!("Stopping {}...", services.join(", ")));
            stack.compose.stop(&services, STOP_TIMEOUT_SECONDS)?;
            super::super::info(rt, "Restoring the stack config...");
            super::restore_config_file(&config_path, original.as_deref(), Some(&written))?;
            super::set_state(&dir, &mut manifest, "rollback_config_restored", None);
            if std::env::var("MRSM_TEST_DOCKER").ok().as_deref() == Some("1")
                && std::env::var("MRSM_TEST_UPGRADE_POSTGRES_FAIL")
                    .ok()
                    .as_deref()
                    == Some("rollback_after_config")
            {
                std::thread::sleep(std::time::Duration::from_secs(90));
            }
            super::forget_stack_config(rt);
            super::super::os_sql_stack::write_stack(rt)
                .map_err(|e| StepError::new(format!("Cannot write the stack files: {e}")))?;
            let resolved = stack.compose.config()?;
            let image = resolved
                .get("services")
                .and_then(|s| s.get(DB_SERVICE))
                .and_then(|d| d.get("image"))
                .and_then(Json::as_str)
                .unwrap_or_default()
                .to_string();
            if image != old_image || !db_volume_names(&resolved).contains(&old_volume) {
                return Err(StepError::new(format!(
                    "The restored stack config resolves to '{image}' on {}, not '{old_image}' on \
                 '{old_volume}'.",
                    db_volume_names(&resolved).join(", ")
                )));
            }
            super::super::info(
                rt,
                &format!("Starting the database on '{old_volume}' (PostgreSQL {old_major})..."),
            );
            stack.compose.up(&[DB_SERVICE.to_string()])?;
            let id = docker::wait_healthy(&stack.compose, DB_SERVICE, START_TIMEOUT)?;
            check_running(&id, &old_volume, old_major)?;
            login_warning = super::docker::PgContainer::new(id.clone())
                .refused_login()
                .map(|refused| super::switch::login_warning(&refused));
            if !dependents.is_empty() {
                super::super::info(rt, &format!("Starting {}...", dependents.join(", ")));
                stack.compose.up(&dependents)?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                manifest["rollback_pending"] = serde_json::json!(false);
                super::set_state(&dir, &mut manifest, "rolled_back", None);
                if let Some(warning) = &login_warning {
                    super::super::warn(rt, warning);
                }
                let kept_config = match (&saved_name, saved.is_some()) {
                    (Some(name), true) => format!(
                        "\nThe stack config from before the rollback is saved as {}.",
                        dir.join(name).display()
                    ),
                    _ => String::new(),
                };
                Ok(SuccessTuple::ok(format!(
                    "The stack's database runs PostgreSQL {old_major} ('{old_image}') on '{old_volume}' \
                 again.\nThe volume '{new_volume}' was kept; remove it with `docker volume rm \
                 {new_volume}` once you no longer need what was written to it.{kept_config}"
                )))
            }
            Err(e) => {
                // Put the config the stack had before the rollback back, and start it again.
                let mut problems = Vec::new();
                if let Err(err) = super::restore_config_file(
                    &stack.config_file(),
                    saved.as_deref(),
                    Some(&config_path),
                ) {
                    problems.push(err);
                }
                super::forget_stack_config(rt);
                if let Err(err) = super::super::os_sql_stack::write_stack(rt) {
                    problems.push(err);
                }
                if let Err(err) = stack.compose.up(&services) {
                    problems.push(err.message);
                }
                if problems.is_empty() {
                    match docker::wait_healthy(&stack.compose, DB_SERVICE, START_TIMEOUT).and_then(
                        |id| {
                            if docker::container_image(&id)? != current_image
                                || !docker::container_volumes(&id)?
                                    .iter()
                                    .any(|v| current_volumes.contains(v))
                            {
                                return Err(StepError::new(
                                    "The stack did not return to its previous image and volume.",
                                ));
                            }
                            Ok(())
                        },
                    ) {
                        Ok(()) => {}
                        Err(err) => problems.push(err.message),
                    }
                }
                if problems.is_empty() {
                    manifest["rollback_pending"] = serde_json::json!(false);
                    super::set_state(&dir, &mut manifest, "switched", Some(&e.message));
                } else {
                    super::set_state(&dir, &mut manifest, "recovery_failed", Some(&e.message));
                }
                let mut message = format!("The rollback failed:\n{}\n", e.message);
                if problems.is_empty() {
                    message.push_str("The stack was put back as it was before the rollback.");
                } else {
                    message.push_str("Putting the stack back did not fully work:\n");
                    for p in problems {
                        message.push_str(&format!("  - {p}\n"));
                    }
                }
                Ok(SuccessTuple::fail(message))
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn edits_made_after_the_switch_are_noticed() {
        assert!(!edited_since_switch(Some("abc"), Some("abc")));
        assert!(edited_since_switch(Some("abc"), Some("abd")));
        // Nothing recorded (an older run) or nothing readable: no claim either way.
        assert!(!edited_since_switch(None, Some("abc")));
        assert!(!edited_since_switch(Some("abc"), None));
    }

    #[test]
    fn the_db_volume_names_are_the_resolved_ones() {
        let resolved = json!({
            "services": {"db": {"volumes": [
                {"type": "volume", "source": "meerschaum_db_data_pg18", "target": "/home/postgres/pgdata"},
                {"type": "bind", "source": "/x", "target": "/y"}
            ]}},
            "volumes": {"meerschaum_db_data_pg18": {"name": "mrsm_meerschaum_db_data_pg18"}}
        });
        assert_eq!(
            db_volume_names(&resolved),
            vec!["mrsm_meerschaum_db_data_pg18"]
        );
        assert!(db_volume_names(&json!({})).is_empty());
    }
}
