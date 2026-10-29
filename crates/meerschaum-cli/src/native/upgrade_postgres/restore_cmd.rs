//! `restore postgres [<timestamp> | <path>]`: put a backup (default: the newest complete one) into
//! a NEW volume, verify it against the backup's manifest, and switch the stack's `db` to it, the
//! same way `upgrade postgres` does. The current volume is never touched: `restore postgres
//! rollback` (or `upgrade postgres rollback`) switches back to it.
//!
//! The backup is restored into the stack's current image. A backup of an older PostgreSQL major
//! is restored the way an upgrade restores its dump (TimescaleDB at the dump's version first); a
//! backup of a newer major than the image is refused. A backup whose files do not match their
//! recorded size and SHA-256 is refused before anything changes.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde_json::{Value as Json, json};

use meerschaum_core::SuccessTuple;

use crate::builtin::ActionResult;
use crate::dispatch::Runtime;

use super::docker::{self, PgContainer, Res};
use super::dump::DumpSet;
use super::fingerprint::{self, Check};
use super::plan::{Halt, ServiceSpec};
use super::restore::{self, NewServer};
use super::server::{self, ServerInfo, Target};
use super::stackcfg::{self, DataVolume, Switch};
use super::switch::{self, OldSide, SwitchTo, Undo};
use super::{DB_SERVICE, DOCS_HINT, Options, Stack, Steps, format_duration, human_bytes};

/// Everything the restore does, decided before it starts.
struct Restore {
    run_id: String,
    run_dir: PathBuf,
    source: PathBuf,
    manifest: Json,
    set: DumpSet,
    backup_version: String,
    image: String,
    target: Target,
    volume: DataVolume,
    /// The current server, when it runs and answers.
    current: Option<(PgContainer, ServerInfo)>,
    /// Whether the stack's `db` container runs (answering or not).
    db_running: bool,
    switch: Switch,
    new_volume: String,
    volume_labels: Vec<(String, String)>,
    dependents: Vec<String>,
    service: ServiceSpec,
    project_label: String,
    warnings: Vec<String>,
}

/// The backup directory a word names: a directory with a manifest, or a timestamp under the
/// backups; the newest complete backup without a word.
fn source_dir(backups: &Path, word: Option<&str>) -> Result<PathBuf, String> {
    match word {
        None => super::backup::complete_backups(backups)
            .into_iter()
            .next()
            .map(|(dir, _)| dir)
            .ok_or_else(|| {
                format!(
                    "No complete backup in {}: take one with `mrsm backup postgres`.",
                    backups.display()
                )
            }),
        Some(word) => {
            let as_path = PathBuf::from(word);
            let dir = if as_path.join("manifest.json").is_file() {
                as_path
            } else {
                backups.join(word)
            };
            if dir.join("manifest.json").is_file() {
                Ok(dir)
            } else {
                Err(format!(
                    "'{word}' is neither a backup directory nor the name of one in {}.",
                    backups.display()
                ))
            }
        }
    }
}

fn preflight(
    rt: &mut Runtime<'_>,
    opts: &Options,
    stack: &Stack,
    word: Option<&str>,
) -> Result<Restore, Halt> {
    let source = source_dir(&stack.backups, word)?;
    let manifest = super::read_manifest(&source)
        .ok_or_else(|| format!("Cannot read the manifest of {}.", source.display()))?;
    let kind = manifest.get("kind").and_then(Json::as_str).unwrap_or("");
    if !matches!(kind, "backup" | "upgrade") || manifest.get("dumps").is_none() {
        return Err(format!(
            "{} does not hold a complete backup (its manifest says '{kind}').",
            source.display()
        )
        .into());
    }
    let set = DumpSet::from_json(&manifest["dumps"])?;
    super::super::info(
        rt,
        &format!("Checking the files of {}...", source.display()),
    );
    set.verify_files(&source)?;
    let backup_major = manifest
        .pointer("/server/major")
        .and_then(Json::as_u64)
        .unwrap_or(0) as u32;
    let backup_version = manifest
        .pointer("/server/server_version")
        .and_then(Json::as_str)
        .unwrap_or("?")
        .to_string();

    let db = stack
        .db()
        .cloned()
        .ok_or("The stack has no `db` service to restore into.")?;
    let image = db
        .get("image")
        .and_then(Json::as_str)
        .unwrap_or_default()
        .to_string();
    let running_id = stack.compose.container_id(DB_SERVICE)?;
    let db_running = running_id.is_some();
    let current = match running_id {
        Some(id) => {
            let pg = PgContainer::new(id);
            let info = ServerInfo::read(&pg).ok();
            info.map(|info| (pg, info))
        }
        None => None,
    };
    let image_config = docker::image_config(&image)?;
    let data_directory = match &current {
        Some((_, info)) => info.data_directory.clone(),
        None => server::env_value(&server::service_env(&db), "PGDATA")
            .or_else(|| image_config.env_var("PGDATA"))
            .unwrap_or("/var/lib/postgresql/data")
            .to_string(),
    };
    let volume = stackcfg::data_volume(
        &db,
        stack.resolved.get("volumes").unwrap_or(&Json::Null),
        &data_directory,
    )?;
    stackcfg::check_editable(&stack.raw, &image, &volume)?;
    stackcfg::check_declaration(&stack.resolved, &volume.key)?;
    super::plan::check_volume_driver(&volume.name)?;

    let run_id = super::fresh_run_id(&stack.backups);
    let mut warnings = Vec::new();
    let pgdata = server::env_value(&server::service_env(&db), "PGDATA")
        .or_else(|| image_config.env_var("PGDATA"))
        .unwrap_or("/var/lib/postgresql/data")
        .to_string();
    let new_target = stackcfg::new_mount_target(&volume.target, &pgdata, &image_config.volumes);
    let mut data_dirs = vec![volume.target.clone(), pgdata.clone(), new_target.clone()];
    let mut sockets = Vec::new();
    if let Some((_, info)) = &current {
        data_dirs.push(info.data_directory.clone());
        sockets = info.socket_dirs.clone();
    }
    let service = ServiceSpec::from_service(
        &db,
        &volume.target,
        &data_dirs,
        &super::plan::socket_dirs(&sockets),
    )?;
    let project_label = stack.project_label();
    let locales: Vec<(String, super::fingerprint::DbLocale)> = set
        .databases
        .iter()
        .filter_map(|d| {
            Some((
                d.name.clone(),
                super::fingerprint::database_locale(&d.fingerprint)?,
            ))
        })
        .collect();
    let versions: BTreeSet<String> = set
        .databases
        .iter()
        .filter_map(|d| d.timescaledb())
        .collect();
    let dumps: Vec<PathBuf> = set
        .databases
        .iter()
        .map(|d| source.join(&d.dump.file))
        .collect();
    let dump_refs: Vec<&Path> = dumps.iter().map(PathBuf::as_path).collect();
    super::super::info(
        rt,
        &format!("Checking what '{image}' can restore (a throwaway container)..."),
    );
    let target = server::probe_target(
        &image,
        &service.superuser(),
        &format!("{project_label}-pgcheck-{}", run_id.to_lowercase()),
        &run_id,
        &versions,
        &locales,
        &dump_refs,
    )?;
    if backup_major > target.major {
        return Err(format!(
            "The backup is of PostgreSQL {backup_version}, newer than the stack's image \
             '{image}' (PostgreSQL {}): a newer major's dump cannot be restored into an older \
             server. Set a PostgreSQL {backup_major} image for `db` in the stack config first. \
             {DOCS_HINT}",
            target.version
        )
        .into());
    }
    let mut problems = Vec::new();
    for db_dump in &set.databases {
        for (name, (version, _)) in fingerprint::extensions(&db_dump.fingerprint) {
            if name != "plpgsql" && !target.available.contains_key(&name) {
                problems.push(format!(
                    "The extension '{name}' {version} (database '{}') is not available in \
                     '{image}'.",
                    db_dump.name
                ));
            }
        }
        if let Some(ts) = db_dump.timescaledb()
            && !target.timescaledb_usable.contains(&ts)
        {
            problems.push(format!(
                "The backup of '{}' carries TimescaleDB {ts}, which '{image}' cannot create (it \
                 restores only into the same version). Restore it into an image of PostgreSQL \
                 {backup_major} that ships TimescaleDB {ts}.",
                db_dump.name
            ));
        }
    }
    problems.extend(target.locale_problems.iter().cloned());
    if !problems.is_empty() {
        return Err(problems.join("\n").into());
    }

    let keys = vec![stackcfg::restore_volume_key(
        &volume.key,
        target.major,
        &run_id,
    )];
    let (new_key, new_volume, _) = super::plan::choose_volume(stack, &volume, &keys)?;
    let declared_name = stack
        .raw
        .get_path(&["docker-compose.yaml", "volumes", &volume.key, "name"])
        .is_some();
    let switch = Switch {
        image: image.clone(),
        old_key: volume.key.clone(),
        old_target: volume.target.clone(),
        new_key,
        new_target,
        new_declared_name: declared_name.then(|| new_volume.clone()),
    };
    let volume_labels = stackcfg::declared_labels(&stack.resolved, &volume.key);
    let running = stack.compose.running_services()?;
    let dependents: Vec<String> = stack
        .dependents()
        .into_iter()
        .filter(|s| running.contains(s))
        .collect();
    let needed: i64 = set
        .databases
        .iter()
        .filter_map(|d| d.fingerprint.get("size").and_then(Json::as_i64))
        .sum();
    if let Some(free) = target.free_bytes
        && free < needed
    {
        let text = format!(
            "Only {} free for Docker's volumes; the restored databases took {}.",
            human_bytes(free),
            human_bytes(needed)
        );
        if !opts.force {
            return Err(format!("{text} Free some space, or pass `--force` to try anyway.").into());
        }
        warnings.push(text);
    }
    if current.is_none() {
        warnings.push(
            "The stack's database is not running; the restored one replaces it as it is.".into(),
        );
    }
    let run_dir = stack.backups.join(&run_id);
    Ok(Restore {
        run_id,
        run_dir,
        source,
        manifest,
        set,
        backup_version,
        image,
        target,
        volume,
        current,
        db_running,
        switch,
        new_volume,
        volume_labels,
        dependents,
        service,
        project_label,
        warnings,
    })
}

fn render(plan: &Restore, project: &str) -> String {
    let mut out = format!("Restore of the stack's database (project '{project}'):\n\n");
    let line = |out: &mut String, label: &str, text: &str| {
        out.push_str(&format!("  {label:<18}{text}\n"));
    };
    let created = plan
        .manifest
        .get("created")
        .and_then(Json::as_str)
        .unwrap_or("?");
    line(
        &mut out,
        "Backup",
        &format!(
            "{} (taken {created}, PostgreSQL {}, files checked)",
            plan.source.display(),
            plan.backup_version
        ),
    );
    let dbs: Vec<String> = plan
        .set
        .databases
        .iter()
        .map(|d| {
            let rows = d
                .fingerprint
                .get("row_estimate")
                .and_then(Json::as_i64)
                .map(|n| format!(", ~{} rows", super::super::thousands(n)))
                .unwrap_or_default();
            format!("{} ({}{rows})", d.name, human_bytes(d.dump.bytes as i64))
        })
        .collect();
    line(&mut out, "Databases", &dbs.join(", "));
    line(
        &mut out,
        "Into",
        &format!(
            "a new volume '{}' with '{}' (PostgreSQL {})",
            plan.new_volume, plan.image, plan.target.version
        ),
    );
    line(
        &mut out,
        "Current volume",
        &format!(
            "{} (kept as it is: `mrsm restore postgres rollback` switches back to it)",
            plan.volume.name
        ),
    );
    line(
        &mut out,
        "Stopped meanwhile",
        &if plan.dependents.is_empty() {
            "nothing else (only `db`, for the switch)".to_string()
        } else {
            format!(
                "{} and `db`, only for the switch",
                plan.dependents.join(", ")
            )
        },
    );
    out.push_str(
        "\nThe restored database is what the backup holds: everything written since then stays \
         only in the current volume. That includes the roles: a password changed since the \
         backup (`POSTGRES_PASSWORD`, the `sql:main` password) is the backup's again, and the \
         stack's services cannot log in until the role and the stack config agree.\n",
    );
    for w in &plan.warnings {
        out.push_str(&format!("\nWarning: {w}\n"));
    }
    out
}

/// `restore postgres [<timestamp> | <path>]`.
pub(crate) fn restore(rt: &mut Runtime<'_>, opts: &Options, word: Option<&str>) -> ActionResult {
    let _signals = super::signals::Session::begin(rt);
    let (mut stack, _locks) = match super::prologue(rt, true) {
        Ok(ready) => ready,
        Err(e) => return Ok(SuccessTuple::fail(e)),
    };
    match super::leftovers::settle(rt, opts, &stack, "restore postgres") {
        Ok(false) => {}
        Ok(true) => {
            stack = match Stack::load(rt, true) {
                Ok(stack) => stack,
                Err(e) => return Ok(SuccessTuple::fail(e)),
            }
        }
        Err(result) => return Ok(result),
    }
    let plan = match preflight(rt, opts, &stack, word) {
        Ok(ready) => ready,
        Err(Halt::Stop(result)) => return Ok(result),
        Err(Halt::Action(e)) => return Err(e),
    };
    rt.print(&format!("\n{}\n", render(&plan, &stack.compose.project)));
    if !super::ask(
        rt,
        "Restore the stack's database from this backup now?",
        "n",
        opts,
    )? {
        return Ok(SuccessTuple::fail("Nothing was changed."));
    }
    let mut undo = Undo {
        db_down_before: !plan.db_running,
        ..Undo::default()
    };
    let mut steps = Steps::new();
    let current_major = plan
        .current
        .as_ref()
        .map(|(_, info)| info.major)
        .unwrap_or(plan.target.major);
    let mut manifest = json!({
        "format": 1,
        "kind": "restore",
        "run_id": plan.run_id,
        "created": chrono::Utc::now().to_rfc3339(),
        "meerschaum": crate::VERSION,
        "state": "started",
        "project": stack.compose.project,
        // Whether the stack's `db` runs: a stopped one stays stopped when this run is put back.
        "db_running": plan.db_running,
        "source": plan.source,
        "old": {
            "image": plan.image,
            "major": current_major,
            "volume": plan.volume.name,
            "volume_key": plan.volume.key,
            "mount": plan.volume.target,
        },
        "new": {
            "image": plan.image,
            "server_version": plan.target.version,
            "major": plan.target.major,
            "volume": plan.new_volume,
            "volume_key": plan.switch.new_key,
            "mount": plan.switch.new_target,
        },
    });
    let result = run(rt, &stack, &plan, &mut undo, &mut steps, &mut manifest);
    match result {
        Ok(warnings) => {
            for w in &warnings {
                super::super::warn(rt, w);
            }
            Ok(SuccessTuple::ok(format!(
                "The stack's database now runs the backup of {} on the volume '{}' ({} in \
                 total).\n\n{}\nThe previous volume '{}' was kept: `mrsm restore postgres \
                 rollback` switches back to it, `docker volume rm {}` frees its space.",
                plan.source.display(),
                plan.new_volume,
                format_duration(steps.started.elapsed()),
                steps.timings_text(),
                plan.volume.name,
                plan.volume.name
            )))
        }
        Err(e) => {
            let old = OldSide {
                volume: &plan.volume.name,
                major: current_major,
                server: plan.current.as_ref().map(|(pg, _)| pg),
            };
            let problems = switch::put_back(rt, opts.debug, &stack, &old, &mut undo, &plan.run_id);
            let note = super::finish_state(
                &plan.run_dir,
                &mut manifest,
                if problems.is_empty() {
                    "failed"
                } else {
                    "recovery_failed"
                },
                Some(&e.message),
            );
            let mut text = format!(
                "The restore {}:\n{}\n",
                if e.interrupted {
                    "was stopped"
                } else {
                    "failed"
                },
                e.message
            );
            if problems.is_empty() {
                text.push_str(&format!(
                    "\nThe stack's database was not switched: it stays on '{}'.",
                    plan.volume.name
                ));
                if let Some(outcome) = &undo.new_volume_outcome {
                    text.push(' ');
                    text.push_str(outcome);
                }
            } else {
                text.push_str("\nPutting the stack back did not fully work:\n");
                for p in problems {
                    text.push_str(&format!("  - {p}\n"));
                }
            }
            if let Some(note) = note {
                text.push('\n');
                text.push_str(&note);
            }
            Ok(SuccessTuple::fail(text))
        }
    }
}

fn run(
    rt: &mut Runtime<'_>,
    stack: &Stack,
    plan: &Restore,
    undo: &mut Undo,
    steps: &mut Steps,
    manifest: &mut Json,
) -> Res<Vec<String>> {
    docker::private_dir(&plan.run_dir)?;
    super::write_manifest(&plan.run_dir, manifest)?;
    let server = NewServer {
        image: &plan.image,
        run_id: &plan.run_id,
        name: format!(
            "{}-pgrestore-{}",
            plan.project_label,
            plan.run_id.to_lowercase()
        ),
        project_label: &plan.project_label,
        external: plan.volume.external,
        volume: &plan.new_volume,
        volume_key: &plan.switch.new_key,
        mount: &plan.switch.new_target,
        labels: &plan.volume_labels,
        env: &plan.service.env,
        mount_args: &plan.service.mount_args,
        command: &plan.service.command,
        entrypoint: &plan.service.entrypoint,
        shm_size: plan.service.shm_size.as_deref(),
        user: plan.service.user.as_deref(),
    };
    let settings: Vec<(String, String)> = plan
        .manifest
        .get("settings")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .filter_map(|pair| {
            let pair = pair.as_array()?;
            Some((
                pair.first()?.as_str()?.to_string(),
                pair.get(1)?.as_str()?.to_string(),
            ))
        })
        .collect();
    let check = if plan.set.exact_counts {
        Check::Restored
    } else {
        Check::RestoredFromBackup
    };
    restore::restore(
        rt,
        steps,
        undo,
        &server,
        &plan.source,
        &plan.set,
        &settings,
        check,
    )?;
    super::set_state(&plan.run_dir, manifest, "verified", None);
    if !plan.dependents.is_empty() {
        switch::record_stopped(&plan.run_dir, manifest, undo, &plan.dependents)?;
        steps.run(
            rt,
            &format!("Stopping {}", plan.dependents.join(", ")),
            |_| stack.compose.stop(&plan.dependents, 60),
        )?;
    }
    let to = SwitchTo {
        image: &plan.image,
        switch: &plan.switch,
        volume: &plan.new_volume,
        major: plan.target.major,
        run_dir: &plan.run_dir,
        db_running: plan.db_running,
        old_stop: None,
    };
    switch::switch_over(rt, steps, stack, &to, undo, manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backups_are_named_by_timestamp_or_path() {
        let dir = tempfile::tempdir().unwrap();
        let backups = dir.path().join("backups");
        for (name, kind) in [
            ("20261001T000000Z", "backup"),
            ("20261002T000000Z", "backup"),
        ] {
            let run = backups.join(name);
            std::fs::create_dir_all(&run).unwrap();
            super::super::write_manifest(
                &run,
                &json!({"kind": kind, "state": "complete", "dumps": {}}),
            )
            .unwrap();
        }
        assert_eq!(
            source_dir(&backups, None).unwrap(),
            backups.join("20261002T000000Z")
        );
        assert_eq!(
            source_dir(&backups, Some("20261001T000000Z")).unwrap(),
            backups.join("20261001T000000Z")
        );
        let path = backups.join("20261001T000000Z");
        assert_eq!(
            source_dir(&backups, Some(path.to_str().unwrap())).unwrap(),
            path
        );
        assert!(source_dir(&backups, Some("nope")).is_err());
        assert!(source_dir(&dir.path().join("empty"), None).is_err());
    }
}
