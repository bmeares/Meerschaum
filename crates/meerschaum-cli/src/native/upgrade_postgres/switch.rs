//! The switch `upgrade postgres` and `restore postgres` end with, and what puts the stack back
//! when anything fails before (or during) it.
//!
//! The switch stops the stack's `db`, copies the stack config file byte for byte into the run's
//! directory, edits only the `db` image, its data volume entry and that volume's declaration (as
//! an edit of those lines when the file allows it, else by writing the config through the config
//! layer), regenerates the compose file, checks what Compose now resolves, starts `db` on the new
//! volume and checks it runs the expected major on it. The old volume is never touched.
//!
//! Every change is recorded in the run's manifest BEFORE it is made, so a run that is killed
//! (`kill -9`, a power cut) can be put back by the next one ([`super::leftovers`]).

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Value as Json, json};

use crate::dispatch::Runtime;

use super::docker::{self, Docker, PgContainer, Res, StepError};
use super::signals;
use super::stackcfg;
use super::yamledit;
use super::{DB_SERVICE, LABEL, Stack, Steps};

/// How long a starting server may take (a new volume's setup included).
pub(crate) const START_TIMEOUT: Duration = Duration::from_secs(600);

/// How long a server may take to stop cleanly.
pub(crate) const STOP_TIMEOUT_SECONDS: u32 = 120;

/// What a command changed so far, in order to put it back.
#[derive(Debug, Default)]
pub(crate) struct Undo {
    /// Services stopped by the command (started again).
    pub stopped: Vec<String>,
    /// Old db network attachments disconnected to fence external writers.
    pub isolated: Vec<super::fence::Attachment>,
    pub isolated_container: Option<String>,
    /// Databases of the old server whose TimescaleDB background workers were paused.
    pub paused: Vec<String>,
    /// The new volume, once created by this run.
    pub new_volume: Option<String>,
    /// The old `db` was stopped for the switch.
    pub old_db_stopped: bool,
    /// The stack's `db` was not running when the run began (a `restore postgres` of a broken or
    /// stopped database): putting back leaves it stopped, and stops one that the switch (or a
    /// service that depends on it) started on the way.
    pub db_down_before: bool,
    /// The stack config file was rewritten: its path, original bytes (`None`: there was no
    /// file), and the file written.
    pub config: Option<(PathBuf, Option<Vec<u8>>, PathBuf)>,
    /// What happened to the new volume when the stack was put back.
    pub new_volume_outcome: Option<String>,
}

impl Undo {
    /// The switch went through: nothing is put back any more.
    fn commit(&mut self) {
        self.config = None;
        self.new_volume = None;
        self.old_db_stopped = false;
        self.paused.clear();
    }
}

/// `SELECT _timescaledb_functions.<what>_background_workers()` (older versions keep it in
/// `_timescaledb_internal`): not persistent, a server restart starts them again.
pub(crate) fn background_workers(what: &str) -> String {
    format!(
        "DO $mrsm$BEGIN
  IF pg_catalog.to_regprocedure('_timescaledb_functions.{what}_background_workers()') IS NOT NULL THEN
    PERFORM _timescaledb_functions.{what}_background_workers();
  ELSIF pg_catalog.to_regprocedure('_timescaledb_internal.{what}_background_workers()') IS NOT NULL THEN
    PERFORM _timescaledb_internal.{what}_background_workers();
  END IF;
END$mrsm$;"
    )
}

/// Whether putting back an interrupted run starts the old `db`: the run stopped it, or it is down
/// after the run edited the config (a kill between the stop and its record). Never when it was
/// down before the run: that database is left as it was (starting it would run PostgreSQL's
/// crash recovery on a volume the plan promised not to modify).
pub(crate) fn recovery_starts_old_db(
    stopped_by_run: bool,
    down_before: bool,
    config_edited: bool,
    db_running: bool,
) -> bool {
    stopped_by_run || (!down_before && config_edited && !db_running)
}

/// A `db` that was down before the run stays down: stop the one that is running now, and remove
/// a container that mounts the run's new volume (the switch started it; the new volume is about
/// to be removed). Returns what could not be done.
fn keep_db_down(stack: &Stack, new_volume: Option<&str>) -> Vec<String> {
    let mut problems = Vec::new();
    let Ok(Some(id)) = stack.compose.any_container_id(DB_SERVICE) else {
        return problems;
    };
    let on_new = new_volume.is_some_and(|new| {
        docker::container_volumes(&id).is_ok_and(|volumes| volumes.iter().any(|v| v == new))
    });
    let done = if on_new {
        Docker::new(&["rm", "-f", &id])
            .check("Removing the database container of the new volume")
            .map(|_| ())
    } else if matches!(stack.compose.container_id(DB_SERVICE), Ok(Some(_))) {
        stack
            .compose
            .stop(&[DB_SERVICE.to_string()], STOP_TIMEOUT_SECONDS)
    } else {
        return problems;
    };
    if let Err(e) = done {
        problems.push(format!("Cannot leave the database stopped: {}", e.message));
    } else if on_new {
        // The stopped container the stack had is gone with it: make one again, from the config
        // that was put back (best effort: `stack up` makes it too).
        let _ = stack.compose.create(&[DB_SERVICE.to_string()]);
    }
    problems
}

/// The started `db` container must mount `volume` and run `major`.
pub(crate) fn check_running(id: &str, volume: &str, major: u32) -> Res<()> {
    let volumes = docker::container_volumes(id)?;
    if !volumes.iter().any(|v| v == volume) {
        return Err(StepError::new(format!(
            "The started database does not mount '{volume}' (it mounts {}).",
            volumes.join(", ")
        )));
    }
    let pg = PgContainer::new(id);
    let num: i64 = pg
        .psql("template1", "SHOW server_version_num;")?
        .trim()
        .parse()
        .unwrap_or(0);
    if num / 10000 != i64::from(major) {
        return Err(StepError::new(format!(
            "The started database runs PostgreSQL {num}, not {major}."
        )));
    }
    Ok(())
}

/// Where the switch goes.
pub(crate) struct SwitchTo<'a> {
    pub image: &'a str,
    pub switch: &'a stackcfg::Switch,
    pub volume: &'a str,
    pub major: u32,
    /// The run's directory (the stack config's copy goes there).
    pub run_dir: &'a Path,
    /// Whether the stack's `db` was running before the run.
    pub db_running: bool,
    /// The old `db` container as the upgrade left it stopped (its dump is what the new volume
    /// holds): when it ran again before the switch, the writes it took are not in the copy.
    pub old_stop: Option<&'a docker::RunMarks>,
}

/// Why a container that was stopped after the dump is not as it was left, or `None` when it is
/// (still the same container, still exited, not started since).
pub(crate) fn old_db_ran_again(left: &docker::RunMarks, now: &docker::RunMarks) -> Option<String> {
    if left.id != now.id {
        return Some("it was removed and created again".into());
    }
    if now.status != "exited" && now.status != "created" {
        return Some(format!("it is {} again", now.status));
    }
    if left.started != now.started || left.finished != now.finished {
        return Some("it was started and stopped again".into());
    }
    None
}

/// Before the switch: the old database must not have run since it was stopped for the copy.
fn check_old_stayed_stopped(old_stop: &docker::RunMarks) -> Res<()> {
    let now = docker::container_run_marks(&old_stop.id).map_err(|_| {
        StepError::new(
            "The old database container was removed after it was stopped: whatever the stack \
             did with it since may have written to the old volume, which the new copy does not \
             hold. Nothing was switched.",
        )
    })?;
    match old_db_ran_again(old_stop, &now) {
        None => Ok(()),
        Some(why) => Err(StepError::new(format!(
            "The old database ran again while the new copy was being restored ({why}; \
             `mrsm stack up`, `stack start`/`restart` or a Docker restart start a stopped \
             `db`). What clients wrote to it after the dump is not in the copy, so nothing was \
             switched. Stop the stack's clients, then run this again."
        ))),
    }
}

/// Record in the manifest that `services` are stopped by the run (before stopping them).
pub(crate) fn record_stopped(
    run_dir: &Path,
    manifest: &mut Json,
    undo: &mut Undo,
    services: &[String],
) -> Res<()> {
    undo.stopped = services.to_vec();
    manifest["stopped"] = json!(services);
    super::write_manifest(run_dir, manifest)?;
    Ok(())
}

/// Stop the stack's `db` (recorded first: whatever happens next, putting back starts it again).
pub(crate) fn stop_db(
    rt: &mut Runtime<'_>,
    steps: &mut Steps,
    stack: &Stack,
    run_dir: &Path,
    undo: &mut Undo,
    manifest: &mut Json,
    what: &str,
) -> Res<()> {
    undo.old_db_stopped = true;
    manifest["old_db_stopped"] = json!(true);
    super::write_manifest(run_dir, manifest)?;
    steps.run(rt, what, |_| {
        stack
            .compose
            .stop(&[DB_SERVICE.to_string()], STOP_TIMEOUT_SECONDS)
    })
}

/// Switch the stack to the new volume. Returns the warnings of the steps after the point of no
/// return (starting the dependents again never undoes the switch).
pub(crate) fn switch_over(
    rt: &mut Runtime<'_>,
    steps: &mut Steps,
    stack: &Stack,
    to: &SwitchTo<'_>,
    undo: &mut Undo,
    manifest: &mut Json,
) -> Res<Vec<String>> {
    if to.db_running && !undo.old_db_stopped {
        stop_db(
            rt,
            steps,
            stack,
            to.run_dir,
            undo,
            manifest,
            "Stopping the stack's database",
        )?;
    }
    if let Some(old_stop) = to.old_stop {
        check_old_stayed_stopped(old_stop)?;
    }
    steps.run(rt, "Editing the stack config", |rt| {
        edit_config(rt, stack, to, undo, manifest)
    })?;
    steps.run(
        rt,
        &format!(
            "Starting the stack's database on '{}' (PostgreSQL {})",
            to.volume, to.major
        ),
        |_| {
            stack.compose.up(&[DB_SERVICE.to_string()])?;
            let id = docker::wait_healthy(&stack.compose, DB_SERVICE, START_TIMEOUT)?;
            check_running(&id, to.volume, to.major)
        },
    )?;
    // The new database is up and checked: from here on nothing is put back.
    undo.commit();
    super::set_state(to.run_dir, manifest, "switched", None);
    let mut warnings = Vec::new();
    if let Ok(Some(id)) = stack.compose.container_id(DB_SERVICE)
        && let Some(refused) = PgContainer::new(id).refused_login()
    {
        warnings.push(login_warning(&refused));
    }
    let stopped = std::mem::take(&mut undo.stopped);
    if !stopped.is_empty() {
        let started = steps.run(rt, &format!("Starting {}", stopped.join(", ")), |_| {
            stack.compose.up(&stopped)
        });
        if let Err(e) = started {
            warnings.push(format!(
                "{} could not be started again ({}); start them with `mrsm stack up -d {}`.",
                stopped.join(", "),
                e.message,
                stopped.join(" ")
            ));
        }
    }
    Ok(warnings)
}

/// What to tell when the stack's own login is refused by the database that now runs: its roles
/// keep the passwords they had when the dump, backup or old volume was made.
pub(crate) fn login_warning(refused: &str) -> String {
    format!(
        "The database now refuses the stack's own login ({refused}). The roles come with the data, \
         with the passwords they had at that time, and `POSTGRES_PASSWORD` (the stack's \
         `sql:main` password) differs: the stack's services cannot log in until the two agree. \
         Set the role's password to the configured one (`ALTER ROLE ... PASSWORD ...`), or put \
         the old password back in the config."
    )
}

/// How the switch changes the stack config file.
#[derive(Debug, Clone)]
pub(crate) enum ConfigEdit {
    /// Only these lines change (`(before, after)`); the new text.
    Lines {
        text: String,
        changed: Vec<(String, String)>,
    },
    /// The file is written again through the config layer (its comments and formatting are not
    /// kept; the original is kept byte for byte in the run's directory).
    Rewrite { reason: String },
    /// There is no file yet: one is written with the stack's whole config.
    Create,
}

/// Parse a `stack.yaml` text the way the config layer reads it.
fn parse_stack_text(text: &str, name: &str) -> Option<meerschaum_core::config::ConfigValue> {
    use meerschaum_core::yaml::loader::{LoadOptions, load_text};
    let node = load_text(
        text,
        name,
        false,
        LoadOptions {
            safe: false,
            project: false,
        },
    )
    .ok()?;
    Some(meerschaum_core::config::ConfigValue::from_node(&node))
}

/// How the switch will change `stack.file` (from its current bytes).
pub(crate) fn config_edit(stack: &Stack, switch: &stackcfg::Switch) -> ConfigEdit {
    let Some(file) = &stack.file else {
        return ConfigEdit::Create;
    };
    let rewrite = |reason: &str| ConfigEdit::Rewrite {
        reason: reason.to_string(),
    };
    let is_yaml = file
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("yaml") || e.eq_ignore_ascii_case("yml"));
    if !is_yaml {
        return rewrite("it is not a YAML file");
    }
    let Ok(text) = std::fs::read_to_string(file) else {
        return rewrite("it cannot be read as text");
    };
    let name = file.to_string_lossy();
    let Some(original) = parse_stack_text(&text, &name) else {
        return rewrite("it does not parse");
    };
    let Ok(expected) = stackcfg::apply(&original, switch) else {
        return rewrite("its `db` service is not where the edit expects it");
    };
    let shared = stackcfg::volume_shared(&original, &switch.old_key);
    match yamledit::edit(&text, switch, shared) {
        // The edited text must mean exactly the original with the switch applied.
        Some(edited) if parse_stack_text(&edited, &name).as_ref() == Some(&expected) => {
            ConfigEdit::Lines {
                changed: yamledit::changed_lines(&text, &edited),
                text: edited,
            }
        }
        _ => rewrite("the lines to change are not in plain block style"),
    }
}

/// Write `text` over `path` (a temporary file in the same directory, then a rename), with the
/// file's permissions.
fn replace_file(path: &Path, text: &str) -> Res<()> {
    let tmp = path.with_extension("mrsm-upgrade.tmp");
    std::fs::write(&tmp, text)
        .map_err(|e| StepError::new(format!("Cannot write '{}': {e}", tmp.display())))?;
    if let Ok(meta) = std::fs::metadata(path) {
        let _ = std::fs::set_permissions(&tmp, meta.permissions());
    }
    docker::sync_file(&tmp)?;
    std::fs::rename(&tmp, path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        StepError::new(format!("Cannot replace '{}': {e}", path.display()))
    })?;
    if let Some(parent) = path.parent() {
        docker::sync_dir(parent)?;
    }
    Ok(())
}

/// Copy the stack config file into the run's directory byte for byte, write the edited config,
/// regenerate the stack files, and check what Compose now resolves.
fn edit_config(
    rt: &mut Runtime<'_>,
    stack: &Stack,
    to: &SwitchTo<'_>,
    undo: &mut Undo,
    manifest: &mut Json,
) -> Res<()> {
    let path = stack.config_file();
    let original = match &stack.file {
        Some(file) => {
            let bytes = std::fs::read(file)
                .map_err(|e| StepError::new(format!("Cannot read '{}': {e}", file.display())))?;
            docker::write_private(&to.run_dir.join(config_backup_name(file)), &bytes)?;
            Some(bytes)
        }
        None => None,
    };
    let edit = config_edit(stack, to.switch);
    // Recorded before the write: a failed (or killed) write may still have replaced the file.
    undo.config = Some((path.clone(), original.clone(), path.clone()));
    manifest["config"] = json!({
        "path": path,
        "existed": original.is_some(),
        "backup": stack.file.as_deref().map(config_backup_name),
        "written": path,
        "edit": match &edit {
            ConfigEdit::Lines { .. } => "lines",
            ConfigEdit::Rewrite { .. } => "rewrite",
            ConfigEdit::Create => "create",
        },
    });
    super::write_manifest(to.run_dir, manifest)?;
    let written = match edit {
        ConfigEdit::Lines { text, .. } => {
            replace_file(&path, &text)?;
            super::forget_stack_config(rt);
            path.clone()
        }
        ConfigEdit::Rewrite { .. } | ConfigEdit::Create => {
            let edited = stackcfg::apply(&stack.raw, to.switch)?;
            super::write_stack_config(rt, &edited)?
        }
    };
    if written != path {
        undo.config = Some((path.clone(), original.clone(), written.clone()));
        manifest["config"]["written"] = json!(written);
    }
    // What the switch wrote, to tell a later rollback whether the file was edited since.
    if let Ok((sha, _)) = docker::sha256_file(&written) {
        manifest["config"]["written_sha256"] = json!(sha);
    }
    if written != path || manifest["config"].get("written_sha256").is_some() {
        super::write_manifest(to.run_dir, manifest)?;
    }
    super::super::os_sql_stack::write_stack(rt)
        .map_err(|e| StepError::new(format!("Cannot write the stack files: {e}")))?;
    // What Compose resolves now must be the new image on the new volume.
    let resolved = stack.compose.config()?;
    let db = resolved
        .get("services")
        .and_then(|s| s.get(DB_SERVICE))
        .ok_or_else(|| StepError::new("The edited stack has no `db` service."))?;
    let image = db.get("image").and_then(Json::as_str).unwrap_or_default();
    let mounted = db
        .get("volumes")
        .and_then(Json::as_array)
        .is_some_and(|mounts| {
            mounts.iter().any(|m| {
                m.get("source").and_then(Json::as_str) == Some(to.switch.new_key.as_str())
                    && m.get("target").and_then(Json::as_str) == Some(to.switch.new_target.as_str())
            })
        });
    let name = resolved
        .get("volumes")
        .and_then(|v| v.get(&to.switch.new_key))
        .and_then(|v| v.get("name"))
        .and_then(Json::as_str)
        .unwrap_or_default();
    if image != to.image || !mounted || name != to.volume {
        return Err(StepError::new(format!(
            "After the edit, Docker Compose resolves the `db` service to '{image}' on '{name}' \
             instead of '{}' on '{}'.",
            to.image, to.volume
        )));
    }
    Ok(())
}

/// The name of the stack config file's copy in a run's directory.
pub(crate) fn config_backup_name(file: &Path) -> String {
    file.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "stack.yaml".into())
}

/// The stack before the run.
pub(crate) struct OldSide<'a> {
    pub volume: &'a str,
    pub major: u32,
    /// The old server, to resume its TimescaleDB background workers.
    pub server: Option<&'a PgContainer>,
}

/// Resume TimescaleDB's background jobs in the databases they were paused in (`psql` runs a
/// statement in a database). Returns what could not be done.
fn resume_workers(
    paused: &mut Vec<String>,
    psql: &dyn Fn(&str, &str) -> Res<String>,
) -> Vec<String> {
    let mut problems = Vec::new();
    for database in std::mem::take(paused) {
        if let Err(e) = psql(&database, &background_workers("start")) {
            problems.push(format!(
                "Cannot resume TimescaleDB's background jobs in '{database}' (they resume when \
                 the database restarts): {}",
                e.message
            ));
        }
    }
    problems
}

/// Put the stack back after a failure: remove this run's containers, restore the config, start
/// the old database and the stopped services again, and remove the new volume (kept with
/// `--debug`; only a volume that carries the run's label). Signals are ignored meanwhile: it runs
/// to its end. Returns what could not be put back.
pub(crate) fn put_back(
    rt: &mut Runtime<'_>,
    debug: bool,
    stack: &Stack,
    old: &OldSide<'_>,
    undo: &mut Undo,
    run_id: &str,
) -> Vec<String> {
    signals::shielded(|| put_back_now(rt, debug, stack, old, undo, run_id))
}

fn put_back_now(
    rt: &mut Runtime<'_>,
    debug: bool,
    stack: &Stack,
    old: &OldSide<'_>,
    undo: &mut Undo,
    run_id: &str,
) -> Vec<String> {
    super::super::warn(rt, "Putting the stack back as it was...");
    let mut problems = docker::remove_run_containers(LABEL, run_id);
    if let Some((path, original, written)) = undo.config.take() {
        match super::restore_config_file(&path, original.as_deref(), Some(&written)) {
            Ok(()) => {
                super::forget_stack_config(rt);
                if let Err(e) = super::super::os_sql_stack::write_stack(rt) {
                    problems.push(format!("Cannot rewrite the stack files: {e}"));
                }
            }
            Err(e) => problems.push(e),
        }
    }
    if let Some(container) = &undo.isolated_container {
        problems.extend(super::fence::reconnect(container, &undo.isolated));
    }
    if undo.old_db_stopped {
        match stack
            .compose
            .up(&[DB_SERVICE.to_string()])
            .and_then(|_| docker::wait_healthy(&stack.compose, DB_SERVICE, START_TIMEOUT))
        {
            Ok(id) => match check_running(&id, old.volume, old.major) {
                // The database may never have stopped (the record of its stop failed before the
                // stop itself) with the jobs paused; after a real restart they run already.
                Ok(()) => {
                    let server = PgContainer::new(id);
                    problems.extend(resume_workers(&mut undo.paused, &|database, sql| {
                        server.psql(database, sql)
                    }));
                }
                Err(e) => problems.push(e.message),
            },
            Err(e) => problems.push(format!("Cannot start the old database: {}", e.message)),
        }
    } else if let Some(server) = old.server {
        problems.extend(resume_workers(&mut undo.paused, &|database, sql| {
            server.psql(database, sql)
        }));
    }
    if undo.db_down_before && !undo.old_db_stopped {
        problems.extend(keep_db_down(stack, undo.new_volume.as_deref()));
    }
    if !undo.stopped.is_empty()
        && let Err(e) = stack.compose.up(&undo.stopped)
    {
        problems.push(e.message);
    }
    if undo.db_down_before && !undo.old_db_stopped && !undo.stopped.is_empty() {
        // Compose starts what a service depends on: `db` goes down again.
        problems.extend(keep_db_down(stack, None));
    }
    if let Some(volume) = undo.new_volume.take() {
        if !docker::volume_has_label(&volume, LABEL, run_id) {
            // Not this run's volume (or gone): never removed here.
            undo.new_volume_outcome = docker::volume_exists(&volume)
                .then(|| format!("The volume '{volume}' is not this run's; it was left as it is."));
        } else if debug {
            undo.new_volume_outcome = Some(format!(
                "The new volume '{volume}' was kept for inspection (`--debug`); remove it with \
                 `docker volume rm {volume}`."
            ));
        } else {
            match docker::volume_users(&volume) {
                Ok(users) if users.is_empty() => {
                    match Docker::new(&["volume", "rm", &volume]).check("Removing the new volume") {
                        Ok(_) => {
                            undo.new_volume_outcome =
                                Some(format!("The new volume '{volume}' was removed."))
                        }
                        Err(e) => problems.push(e.message),
                    }
                }
                Ok(users) => problems.push(format!(
                    "The new volume '{volume}' is still used by {}; it was kept.",
                    users.join(", ")
                )),
                Err(e) => problems.push(e.message),
            }
        }
    }
    problems
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn background_worker_calls_cover_both_schemas() {
        let sql = background_workers("stop");
        assert!(sql.contains("_timescaledb_functions.stop_background_workers()"));
        assert!(sql.contains("_timescaledb_internal.stop_background_workers()"));
    }

    fn marks(id: &str, status: &str, started: &str, finished: &str) -> docker::RunMarks {
        docker::RunMarks {
            id: id.into(),
            status: status.into(),
            started: started.into(),
            finished: finished.into(),
        }
    }

    #[test]
    fn an_old_database_that_ran_again_is_not_switched_away_from() {
        let left = marks(
            "abc",
            "exited",
            "2026-10-19T10:00:00Z",
            "2026-10-19T10:05:00Z",
        );
        // Untouched: same container, still stopped, same start and stop times.
        assert_eq!(old_db_ran_again(&left, &left.clone()), None);
        // Started again by `stack up` (running), or started and stopped again (new times).
        assert!(
            old_db_ran_again(
                &left,
                &marks(
                    "abc",
                    "running",
                    "2026-10-19T10:09:00Z",
                    "2026-10-19T10:05:00Z"
                )
            )
            .unwrap()
            .contains("running")
        );
        assert!(
            old_db_ran_again(
                &left,
                &marks(
                    "abc",
                    "exited",
                    "2026-10-19T10:09:00Z",
                    "2026-10-19T10:10:00Z"
                )
            )
            .unwrap()
            .contains("again")
        );
        // Removed and created again by Compose (another ID, even when it is stopped).
        assert!(
            old_db_ran_again(
                &left,
                &marks(
                    "def",
                    "exited",
                    "2026-10-19T10:00:00Z",
                    "2026-10-19T10:05:00Z"
                )
            )
            .unwrap()
            .contains("created again")
        );
        // A restarting or paused container is never "still stopped".
        for status in ["restarting", "paused", "dead"] {
            assert!(
                old_db_ran_again(&left, &marks("abc", status, &left.started, &left.finished))
                    .is_some(),
                "{status}"
            );
        }
    }

    #[test]
    fn paused_jobs_resume_in_every_database_and_failures_are_reported() {
        use std::cell::RefCell;
        let calls = RefCell::new(Vec::new());
        let mut paused = vec!["meerschaum".to_string(), "postgres".to_string()];
        let problems = resume_workers(&mut paused, &|database, sql| {
            calls
                .borrow_mut()
                .push((database.to_string(), sql.to_string()));
            if database == "postgres" {
                Err(StepError::new("connection refused"))
            } else {
                Ok(String::new())
            }
        });
        // Every database is tried (one failing does not skip the next), each with the `start`
        // statement, and the list is emptied.
        let calls = calls.borrow();
        assert_eq!(calls.len(), 2);
        assert!(
            calls
                .iter()
                .all(|(_, sql)| sql.contains("start_background_workers"))
        );
        assert!(paused.is_empty());
        assert_eq!(problems.len(), 1);
        assert!(problems[0].contains("'postgres'") && problems[0].contains("connection refused"));
    }

    #[test]
    fn recovery_never_starts_a_database_that_was_down_before_the_run() {
        // (stopped by the run, down before, config edited, running now) -> starts the old db
        for (args, starts) in [
            // An upgrade stopped it: always.
            ((true, false, true, false), true),
            ((true, false, false, false), true),
            // Killed after the config edit with `db` down and no record of a stop: started.
            ((false, false, true, false), true),
            // Still running, or the config was never edited: nothing to start.
            ((false, false, true, true), false),
            ((false, false, false, false), false),
            // A restore that began with `db` down: left down (this was the F6 case).
            ((false, true, true, false), false),
            ((false, true, false, false), false),
        ] {
            let (stopped, down, edited, running) = args;
            assert_eq!(
                recovery_starts_old_db(stopped, down, edited, running),
                starts,
                "{args:?}"
            );
        }
    }

    #[test]
    fn a_committed_switch_puts_nothing_back() {
        let mut undo = Undo {
            stopped: vec!["api".into()],
            paused: vec!["meerschaum".into()],
            new_volume: Some("v".into()),
            isolated: Vec::new(),
            isolated_container: None,
            old_db_stopped: true,
            db_down_before: false,
            config: Some((PathBuf::from("a"), None, PathBuf::from("a"))),
            new_volume_outcome: None,
        };
        undo.commit();
        assert!(undo.config.is_none() && undo.new_volume.is_none() && !undo.old_db_stopped);
        assert!(undo.paused.is_empty());
        // The services are started by the switch itself, not by a put-back.
        assert_eq!(undo.stopped, vec!["api"]);
    }
}
