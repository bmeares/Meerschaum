//! `upgrade postgres [<major> | <image>]` after the preflight: quiesce, take a full backup (the
//! dump), restore it into a new volume, verify, switch. Every change is recorded in [`Undo`], and
//! any failure (or a Ctrl-C) puts the old stack back exactly as it was: the old volume is never
//! written by anything but its own server, and never removed.

use std::time::{Duration, Instant};

use serde_json::{Value as Json, json};

use meerschaum_core::SuccessTuple;

use crate::builtin::ActionResult;
use crate::dispatch::Runtime;

use super::docker::{self, PgContainer, Res, StepError};
use super::dump::{self, Counts, DumpSet};
use super::fingerprint::{self, Check};
use super::leftovers;
use super::plan::{self, Halt, Plan};
use super::restore::{self, NewServer, verification_error};
use super::signals;
use super::switch::{self, OldSide, SwitchTo, Undo, background_workers};
use super::{Options, Stack, Steps, format_duration};

/// `upgrade postgres [<major> | <image>]`.
pub(crate) fn upgrade(rt: &mut Runtime<'_>, opts: &Options, word: Option<&str>) -> ActionResult {
    // From here on a signal stops the running step, and the command puts the old stack back.
    let _signals = signals::Session::begin(rt);
    let (mut stack, _locks) = match super::prologue(rt, true) {
        Ok(ready) => ready,
        Err(e) => return Ok(SuccessTuple::fail(e)),
    };
    match leftovers::settle(rt, opts, &stack, "upgrade postgres") {
        Ok(false) => {}
        Ok(true) => {
            stack = match Stack::load(rt, true) {
                Ok(stack) => stack,
                Err(e) => return Ok(SuccessTuple::fail(e)),
            }
        }
        Err(result) => return Ok(result),
    }
    let plan = match plan::preflight(rt, opts, &stack, word) {
        Ok(ready) => ready,
        Err(Halt::Stop(result)) => return Ok(result),
        Err(Halt::Action(e)) => return Err(e),
    };
    let text = plan::render(&plan, &stack.compose.project);
    rt.print(&format!("\n{text}\n"));
    let question = format!(
        "Upgrade the stack's database from PostgreSQL {} to {} now?\n    Stop Meerschaum jobs and \
         other clients that write to it first: what they write after the dump is not copied.",
        plan.info.major, plan.target.major
    );
    if !super::ask(rt, &question, "n", opts)? {
        return Ok(SuccessTuple::fail("Nothing was changed."));
    }
    let mut undo = Undo::default();
    let mut steps = Steps::new();
    let mut manifest = initial_manifest(&stack, &plan);
    let result = run(
        rt,
        opts,
        &stack,
        &plan,
        &mut undo,
        &mut steps,
        &mut manifest,
    );
    match result {
        Ok(warnings) => {
            for warning in &warnings {
                super::super::warn(rt, warning);
            }
            Ok(SuccessTuple::ok(success_message(&plan, &steps, &manifest)))
        }
        Err(e) => {
            let old = OldSide {
                volume: &plan.volume.name,
                major: plan.info.major,
                server: Some(&plan.old),
            };
            let problems = switch::put_back(rt, opts.debug, &stack, &old, &mut undo, &plan.run_id);
            super::set_state(
                &plan.run_dir,
                &mut manifest,
                if problems.is_empty() {
                    "failed"
                } else {
                    "recovery_failed"
                },
                Some(&e.message),
            );
            Ok(SuccessTuple::fail(failure_message(
                &plan, &e, &problems, &undo,
            )))
        }
    }
}

fn initial_manifest(stack: &Stack, plan: &Plan) -> Json {
    json!({
        "format": 1,
        "kind": "upgrade",
        "run_id": plan.run_id,
        "created": chrono::Utc::now().to_rfc3339(),
        "meerschaum": crate::VERSION,
        "state": "started",
        "project": stack.compose.project,
        "old": {
            "image": plan.old_image,
            "server_version": plan.info.version,
            "major": plan.info.major,
            "volume": plan.volume.name,
            "volume_key": plan.volume.key,
            "mount": plan.volume.target,
        },
        "new": {
            "image": plan.new_image,
            "server_version": plan.target.version,
            "major": plan.target.major,
            "volume": plan.new_volume,
            "volume_key": plan.switch.new_key,
            "mount": plan.switch.new_target,
        },
        "server": {
            "image": plan.old_image,
            "server_version": plan.info.version,
            "major": plan.info.major,
            "volume": plan.volume.name,
        },
        "dependents": plan.dependents,
        "settings": plan.info.settings,
    })
}

/// Other client connections, after waiting up to `timeout` for them to go away (the
/// connections of the services just stopped take a moment to close).
fn wait_for_clients(pg: &PgContainer, timeout: Duration) -> Res<Vec<String>> {
    let started = Instant::now();
    loop {
        let value = pg.json(
            "template1",
            "SELECT coalesce(json_agg(coalesce(usename, '?') || '@' || coalesce(datname, '?') \
             || coalesce(' (' || nullif(application_name, '') || ')', '') || coalesce(' from ' \
             || client_addr::text, '')), '[]') FROM pg_catalog.pg_stat_activity WHERE \
             backend_type = 'client backend' AND pid <> pg_catalog.pg_backend_pid();",
        )?;
        let clients: Vec<String> = value
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(Json::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        if clients.is_empty() || started.elapsed() >= timeout {
            return Ok(clients);
        }
        if docker::stop_requested() {
            return Err(StepError::interrupted());
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// The steps, in order. Each change is recorded in `undo` (and the manifest) before it is made.
/// Returns the warnings of the steps after the switch (which never undo it).
fn run(
    rt: &mut Runtime<'_>,
    opts: &Options,
    stack: &Stack,
    plan: &Plan,
    undo: &mut Undo,
    steps: &mut Steps,
    manifest: &mut Json,
) -> Res<Vec<String>> {
    docker::private_dir(&plan.run_dir)?;
    super::write_manifest(&plan.run_dir, manifest)?;
    let old = &plan.old;

    // 1. Quiesce: the services that use the database stop.
    if !plan.dependents.is_empty() {
        switch::record_stopped(&plan.run_dir, manifest, undo, &plan.dependents)?;
        steps.run(
            rt,
            &format!("Stopping {}", plan.dependents.join(", ")),
            |_| stack.compose.stop(&plan.dependents, 60),
        )?;
    }

    // 2. Nobody else may write from now on (checked before anything changes on the old server).
    let clients = wait_for_clients(old, Duration::from_secs(15))?;
    if !clients.is_empty() {
        let text = format!(
            "Other clients are connected to the database: {}.",
            clients.join("; ")
        );
        if !opts.force {
            return Err(StepError::new(format!(
                "{text}\nStop them (what they write after the dump would be lost), or pass \
                 `--force` to go on anyway."
            )));
        }
        super::super::warn(rt, &format!("{text} Going on (`--force`)."));
    }

    if old
        .psql("template1", "SELECT pg_catalog.pg_is_in_recovery();")?
        .trim()
        == "t"
    {
        return Err(StepError::new(
            "The source is a PostgreSQL standby and may replay buffered WAL. Upgrading a standby into a standalone primary is not supported; use the manual procedure.",
        ));
    }

    // Fence new external TCP clients, including ones arriving after the client check. Tools
    // share this container's network namespace and reach PostgreSQL through loopback/socket.
    let attachments = match super::fence::attachments(&old.container) {
        Ok(attachments) => attachments,
        Err(e) if opts.force => {
            super::super::warn(
                rt,
                &format!(
                    "{} Going on without an external-write fence (--force); concurrent writes may be lost.",
                    e.message
                ),
            );
            Vec::new()
        }
        Err(e) => return Err(e),
    };
    undo.isolated = attachments;
    undo.isolated_container = Some(old.container.clone());
    manifest["isolated_networks"] = json!(undo.isolated);
    manifest["isolated_container"] = json!(old.container);
    manifest["networks_restored"] = json!(false);
    super::write_manifest(&plan.run_dir, manifest)?;
    steps.run(
        rt,
        "Isolating the old database from external writers",
        |_| super::fence::disconnect(&old.container, &undo.isolated),
    )?;
    super::set_state(&plan.run_dir, manifest, "isolated", None);
    if let Some((database, sql)) = restore::test_hook("after_isolate") {
        old.psql(&database, &sql)?;
    }
    let clients = wait_for_clients(old, Duration::from_secs(15))?;
    if !clients.is_empty() && !opts.force {
        return Err(StepError::new(
            "A client connected while the old database was being isolated. Stop external writers and retry; nothing was dumped or switched.",
        ));
    }

    // 3. TimescaleDB on the old server: updated only where the new image cannot create the
    //    installed version, then its background jobs paused (until the server restarts).
    for db in &plan.databases {
        let Some(ts) = &db.timescaledb else {
            continue;
        };
        if ts.update_old() {
            steps.run(
                rt,
                &format!(
                    "Updating TimescaleDB {} -> {} in '{}' (old server)",
                    ts.installed, ts.dump, db.name
                ),
                |_| {
                    // The first statement of a new session (TimescaleDB requires it).
                    old.psql(&db.name, "ALTER EXTENSION timescaledb UPDATE;")?;
                    Ok(())
                },
            )?;
        }
        undo.paused.push(db.name.clone());
        manifest["paused"] = json!(undo.paused);
        super::write_manifest(&plan.run_dir, manifest)?;
        old.psql(&db.name, &background_workers("stop"))?;
    }

    // Logical replication can apply already-buffered changes without a client backend or
    // network. Refuse it and unhandled extension workers; Timescale workers were stopped above.
    let mut writers = Vec::new();
    for db in &plan.databases {
        if old
            .psql(
                &db.name,
                "SELECT count(*) FROM pg_catalog.pg_subscription WHERE subenabled;",
            )?
            .trim()
            != "0"
        {
            writers.push(format!(
                "{} has an enabled logical replication subscription",
                db.name
            ));
        }
    }
    let worker_wait = Instant::now();
    let workers = loop {
        let workers = old.psql("template1", "SELECT coalesce(string_agg(DISTINCT backend_type, ', '), '') FROM pg_catalog.pg_stat_activity WHERE backend_type NOT IN ('client backend', 'autovacuum launcher', 'autovacuum worker', 'background writer', 'checkpointer', 'walwriter', 'walreceiver', 'walsender', 'parallel worker', 'logical replication launcher', 'standalone backend', 'startup') AND lower(backend_type) <> 'timescaledb background worker launcher';")?;
        if workers.trim().is_empty() || worker_wait.elapsed() >= Duration::from_secs(15) {
            break workers;
        }
        if docker::stop_requested() {
            return Err(StepError::interrupted());
        }
        std::thread::sleep(Duration::from_millis(250));
    };
    if !workers.trim().is_empty() {
        writers.push(format!("unhandled background workers: {}", workers.trim()));
    }
    if !writers.is_empty() {
        let message = format!(
            "The source can still write without external clients: {}. Disable these writers before upgrading.",
            writers.join("; ")
        );
        if !opts.force {
            return Err(StepError::new(message));
        }
        super::super::warn(
            rt,
            &format!("{message} Going on (--force); concurrent writes may be lost."),
        );
    }

    // 4. A full backup: the roles and every database, each with its exact row counts and
    //    structure in the dump's own snapshot (and what writes leave behind, read before it).
    let names: Vec<String> = plan.databases.iter().map(|d| d.name.clone()).collect();
    let set = dump::dump(
        rt,
        steps,
        old,
        &plan.tools,
        &names,
        &plan.run_dir,
        Counts::Exact,
    )?;
    manifest["dumps"] = set.to_json();
    super::set_state(&plan.run_dir, manifest, "dumped", None);
    if let Some((database, sql)) = restore::test_hook("after_dump") {
        // The end-to-end tests' failure injection: a client writes after the dump.
        old.psql(&database, &sql)?;
    }

    // 5. Nothing may have written since the dump: no other client, and every database's write
    //    counters, tables and structure as they were before its snapshot. Then the old server
    //    stops right away (it starts again if anything fails).
    steps.run(
        rt,
        "Checking that nothing wrote to the old database since its dump",
        |rt| check_quiet(rt, opts, plan, &set),
    )?;
    super::set_state(&plan.run_dir, manifest, "checked", None);
    if let Some((database, sql)) = restore::test_hook("after_check") {
        old.psql(&database, &sql)?;
    }
    switch::stop_db(
        rt,
        steps,
        stack,
        &plan.run_dir,
        undo,
        manifest,
        "Stopping the old database",
    )?;

    // Restore networking only while PostgreSQL is stopped. The original volume needs no
    // persistent configuration changes, and rollback can start it with its original endpoints.
    if !matches!(
        docker::container_state(&old.container)?.status.as_str(),
        "exited" | "created"
    ) {
        return Err(StepError::new(
            "The old database container did not stop; its network fence remains in place until recovery.",
        ));
    }
    let problems = super::fence::reconnect(&old.container, &undo.isolated);
    if !problems.is_empty() {
        return Err(StepError::new(problems.join("\n")));
    }
    undo.isolated.clear();
    undo.isolated_container = None;
    manifest["networks_restored"] = json!(true);
    super::write_manifest(&plan.run_dir, manifest)?;

    // 6. Restore into a new volume and verify it there.
    let server = NewServer {
        image: &plan.new_image,
        run_id: &plan.run_id,
        name: plan.container("upgrade"),
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
    restore::restore(
        rt,
        steps,
        undo,
        &server,
        &plan.run_dir,
        &set,
        &plan.info.settings,
        Check::Restored,
    )?;
    super::set_state(&plan.run_dir, manifest, "verified", None);

    // 7. Switch (the old database is already stopped).
    let to = SwitchTo {
        image: &plan.new_image,
        switch: &plan.switch,
        volume: &plan.new_volume,
        major: plan.target.major,
        run_dir: &plan.run_dir,
        db_running: false,
    };
    switch::switch_over(rt, steps, stack, &to, undo, manifest)
}

/// The old server right before it stops: no client but ours (unless `--force`), and per
/// database the write counters, tables and structure as before its dump's snapshot.
fn check_quiet(rt: &mut Runtime<'_>, opts: &Options, plan: &Plan, set: &DumpSet) -> Res<()> {
    let old = &plan.old;
    let clients = wait_for_clients(old, Duration::ZERO)?;
    if !clients.is_empty() {
        let text = format!(
            "Clients connected to the database after its dump: {}.",
            clients.join("; ")
        );
        if !opts.force {
            return Err(StepError::new(format!(
                "{text}\nWhat they write would be lost: nothing was switched. Stop them, then run \
                 this again (or pass `--force`)."
            )));
        }
        super::super::warn(rt, &format!("{text} Going on (`--force`)."));
    }
    if plan.info.major < 15 {
        // Before PostgreSQL 15 the statistics reach the collector a moment after a session ends.
        std::thread::sleep(Duration::from_millis(1500));
    }
    let mut problems = Vec::new();
    for db in &set.databases {
        let now = dump::activity_of(old, &db.name)?;
        if let Some(before) = &db.activity
            && *before != now
        {
            problems.push(format!(
                "{}: written to after its dump (write counters, tables or files changed: {} -> \
                 {})",
                db.name, before, now
            ));
        }
        let fp = dump::fingerprint_of(old, &db.name, None, Counts::Skip)?;
        for d in fingerprint::differences(&db.fingerprint, &fp, Check::Unchanged) {
            problems.push(format!("{}: {d}", db.name));
        }
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(verification_error(
            "The old database changed after its dump (a client wrote to it); nothing was \
             switched",
            &problems,
        ))
    }
}

fn totals(set: &Option<DumpSet>, manifest: &Json) -> (usize, i64) {
    if let Some(set) = set {
        return (
            set.databases
                .iter()
                .map(|d| {
                    d.fingerprint
                        .get("rowcounts")
                        .and_then(Json::as_object)
                        .map_or(0, |m| m.len())
                })
                .sum(),
            set.databases
                .iter()
                .map(|d| fingerprint::total_rows(&d.fingerprint))
                .sum(),
        );
    }
    manifest
        .get("dumps")
        .and_then(|d| DumpSet::from_json(d).ok())
        .map(|s| totals(&Some(s), &Json::Null))
        .unwrap_or((0, 0))
}

fn success_message(plan: &Plan, steps: &Steps, manifest: &Json) -> String {
    let (tables, rows) = totals(&None, manifest);
    let mut text = format!(
        "The stack's database now runs PostgreSQL {} ({}) on the volume '{}': {} database{}, \
         {} tables and {} rows verified ({} in total).\n\n",
        plan.target.version,
        plan.new_image,
        plan.new_volume,
        plan.databases.len(),
        if plan.databases.len() == 1 { "" } else { "s" },
        super::super::thousands(tables as i64),
        super::super::thousands(rows),
        format_duration(steps.started.elapsed())
    );
    text.push_str(&steps.timings_text());
    text.push_str(&format!(
        "\nThe old volume '{}' and the backup in {} were kept.\n\
         - To go back to PostgreSQL {}: `mrsm upgrade postgres rollback`\n\
         - Once you are satisfied, free the space with `docker volume rm {}` and by deleting \
         {}",
        plan.volume.name,
        plan.run_dir.display(),
        plan.info.major,
        plan.volume.name,
        plan.run_dir.display()
    ));
    text
}

fn failure_message(plan: &Plan, error: &StepError, problems: &[String], undo: &Undo) -> String {
    let mut text = format!(
        "The upgrade to PostgreSQL {} {}:\n{}\n",
        plan.target.major,
        if error.interrupted {
            "was stopped"
        } else {
            "failed"
        },
        error.message
    );
    if problems.is_empty() {
        text.push_str(&format!(
            "\nNothing was switched: the stack runs PostgreSQL {} on '{}' as before.",
            plan.info.major, plan.volume.name
        ));
        if let Some(outcome) = &undo.new_volume_outcome {
            text.push(' ');
            text.push_str(outcome);
        }
    } else {
        text.push_str(
            "\nPutting the old stack back did not fully work; check it with `mrsm stack ps`:\n",
        );
        for p in problems {
            text.push_str(&format!("  - {p}\n"));
        }
        text.push_str(&format!(
            "The old volume '{}' was not modified.",
            plan.volume.name
        ));
    }
    if plan.run_dir.exists() {
        text.push_str(&format!(
            "\nThe files made so far are in {}.",
            plan.run_dir.display()
        ));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn totals_come_from_the_dump_set() {
        let manifest = json!({"dumps": {
            "compression": "zstd", "exact_counts": true,
            "globals": {"file": "globals.sql", "bytes": 1, "sha256": "a"},
            "databases": [{"name": "a", "file": "01-a.dump", "bytes": 1, "sha256": "b",
                           "fingerprint": {"rowcounts": {"public.x": 3, "public.y": 4}}}]
        }});
        assert_eq!(totals(&None, &manifest), (2, 7));
        assert_eq!(totals(&None, &json!({})), (0, 0));
    }
}
