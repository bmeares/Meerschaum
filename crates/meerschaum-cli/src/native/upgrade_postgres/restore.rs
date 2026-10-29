//! Restoring a dump set into a NEW volume, shared by `upgrade postgres` and `restore postgres`.
//!
//! A temporary container of the target image sets the new volume up with the stack's
//! environment, command and setup mounts (read-only; no network, no published port: nothing else
//! can write to it; TimescaleDB's background workers off). Then the roles, and per database: the
//! database as the dump creates it (one the image's setup created with another encoding or locale
//! is dropped and created again), TimescaleDB created at the dump's version (it restores only
//! into the same version; its schema first when it is not `public`), the other extensions
//! matched to the dump's set, `timescaledb_pre_restore()`, `pg_restore --exit-on-error` with a
//! restore list that skips only what exists, `timescaledb_post_restore()`, the database's own
//! settings and `ANALYZE`. The old server's `ALTER SYSTEM` settings follow (but those that act on
//! the old server's files or replication), and the temporary server is restarted to prove it
//! still starts with them. The copy must then match the dump's fingerprints; its extensions are
//! updated to the image's versions and its structure is checked again. Nothing here touches the
//! stack's current volume.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use serde_json::Value as Json;

use crate::dispatch::Runtime;

use super::docker::{self, Docker, Input, Output, PULL_NEVER, PgContainer, Res, StepError};
use super::dump::{Counts, DumpSet, DumpedDb, fingerprint_of};
use super::fingerprint::{self, Check, DbLocale};
use super::server::{EXTENSIONS_QUERY, parse_extensions};
use super::sqltext::{self, quote_ident, quote_literal};
use super::switch::{START_TIMEOUT, STOP_TIMEOUT_SECONDS, Undo};
use super::{LABEL, Steps};

/// The temporary server that fills the new volume: the target image with the `db` service's
/// environment, command and bind mounts.
pub(crate) struct NewServer<'a> {
    pub image: &'a str,
    pub run_id: &'a str,
    /// The container's name.
    pub name: String,
    /// Compose's project name (the new volume's label, so `stack up` adopts it).
    pub project_label: &'a str,
    /// The data volume is declared `external: true` (no Compose labels then).
    pub external: bool,
    /// The new volume's Docker name and Compose key, and where it is mounted.
    pub volume: &'a str,
    pub volume_key: &'a str,
    pub mount: &'a str,
    /// The labels the volume's declaration gives it.
    pub labels: &'a [(String, String)],
    pub env: &'a [(String, String)],
    /// The `db` service's other mounts (read-only binds).
    pub mount_args: &'a [String],
    pub command: &'a [String],
    pub entrypoint: &'a [String],
    pub shm_size: Option<&'a str>,
    pub user: Option<&'a str>,
}

impl NewServer<'_> {
    fn pg(&self) -> PgContainer {
        PgContainer::new(self.name.clone())
    }

    fn create_volume(&self, undo: &mut Undo) -> Res<()> {
        let mut args = vec!["volume".to_string(), "create".to_string()];
        if !self.external {
            // What Compose would have put on it, so that `stack up` adopts it without a warning.
            args.extend([
                "--label".to_string(),
                format!("com.docker.compose.project={}", self.project_label),
                "--label".to_string(),
                format!("com.docker.compose.volume={}", self.volume_key),
            ]);
        }
        for (key, value) in self.labels {
            args.extend(["--label".to_string(), format!("{key}={value}")]);
        }
        args.extend([
            "--label".to_string(),
            format!("{LABEL}={}", self.run_id),
            self.volume.to_string(),
        ]);
        if docker::volume_exists(self.volume) {
            return Err(StepError::new(format!(
                "The volume '{}' already exists; nothing was written to it.",
                self.volume
            )));
        }
        Docker::new(&args).check("Creating the new volume")?;
        // `docker volume create` of an existing name succeeds and keeps that volume's labels:
        // only a volume that carries this run's label is ours (to fill, and to remove again).
        if !docker::volume_has_label(self.volume, LABEL, self.run_id) {
            return Err(StepError::new(format!(
                "The volume '{}' existed before this run (it does not carry its label); nothing \
                 was written to it.",
                self.volume
            )));
        }
        undo.new_volume = Some(self.volume.to_string());
        Ok(())
    }

    /// Start the container: the setup scripts of the image and the stack run on the empty volume.
    fn start(&self) -> Res<()> {
        let mut args = vec![
            "run".to_string(),
            "-d".to_string(),
            PULL_NEVER[0].to_string(),
            PULL_NEVER[1].to_string(),
            "--name".to_string(),
            self.name.clone(),
            "--label".to_string(),
            format!("{LABEL}={}", self.run_id),
            "--network".to_string(),
            "none".to_string(),
        ];
        if let Some(shm) = self.shm_size {
            args.extend(["--shm-size".to_string(), shm.to_string()]);
        }
        if let Some(user) = self.user {
            args.extend(["--user".to_string(), user.to_string()]);
        }
        for (key, _) in self.env {
            args.extend(["-e".to_string(), key.clone()]);
        }
        args.extend(["-v".to_string(), format!("{}:{}", self.volume, self.mount)]);
        args.extend(self.mount_args.iter().cloned());
        let mut command = self.command.to_vec();
        if let Some((first, rest)) = self.entrypoint.split_first() {
            args.extend(["--entrypoint".to_string(), first.clone()]);
            let mut full = rest.to_vec();
            full.extend(command);
            command = full;
        }
        args.push(self.image.to_string());
        if command.is_empty() {
            command.push("postgres".into());
        }
        if command.first().map(String::as_str) == Some("postgres") {
            command.extend([
                "-c".to_string(),
                "timescaledb.max_background_workers=0".to_string(),
            ]);
        }
        args.extend(command);
        Docker::new(&args)
            .env(self.env)
            .check("Starting the new server's container")?;
        Ok(())
    }

    /// Stop the container cleanly (a fast shutdown with a checkpoint), then remove it.
    pub fn stop(&self) -> Res<()> {
        Docker::new(&[
            "stop",
            "--time",
            &STOP_TIMEOUT_SECONDS.to_string(),
            &self.name,
        ])
        .check("Stopping the temporary server")?;
        docker::remove_container(&self.name)
    }

    /// Restart the container and wait for it.
    fn restart(&self) -> Res<()> {
        Docker::new(&[
            "restart",
            "--time",
            &STOP_TIMEOUT_SECONDS.to_string(),
            &self.name,
        ])
        .check("Restarting the temporary server")?;
        docker::wait_ready(&self.pg(), Duration::from_secs(300))
    }
}

/// Restore `set` (its files in `dir`) into the new volume, carry `settings` over, and verify the
/// copy with `check`. The temporary server is stopped (cleanly) at the end.
#[allow(clippy::too_many_arguments)]
pub(crate) fn restore(
    rt: &mut Runtime<'_>,
    steps: &mut Steps,
    undo: &mut Undo,
    server: &NewServer<'_>,
    dir: &Path,
    set: &DumpSet,
    settings: &[(String, String)],
    check: Check,
) -> Res<()> {
    let new = server.pg();
    steps.run(
        rt,
        &format!("Creating the volume '{}'", server.volume),
        |_| server.create_volume(undo),
    )?;
    steps.run(
        rt,
        &format!("Starting '{}' on it (temporary container)", server.image),
        |_| {
            server.start()?;
            docker::wait_ready(&new, START_TIMEOUT)
        },
    )?;
    steps.run(rt, "Restoring the roles", |_| {
        let path = dir.join(&set.globals.file);
        let sql = std::fs::read_to_string(&path)
            .map_err(|e| StepError::new(format!("Cannot read '{}': {e}", path.display())))?;
        new.psql("template1", &sqltext::rewrite_globals(&sql)?)?;
        Ok(())
    })?;
    let existing = databases_of(&new)?;
    for db in &set.databases {
        steps.run(rt, &format!("Restoring '{}'", db.name), |_| {
            restore_database(
                &new,
                db,
                &dir.join(&db.dump.file),
                existing.contains(&db.name),
            )
        })?;
    }
    if !settings.is_empty() {
        steps.run(rt, "Carrying over the server settings", |rt| {
            for warning in carry_settings(&new, settings)? {
                super::super::warn(rt, &warning);
            }
            // The settings take effect at a start: prove the server still starts with them.
            server.restart()
        })?;
    }
    if let Some((database, sql)) = test_hook("after_restore") {
        // The end-to-end tests' failure injection: change the copy before it is verified.
        new.psql(&database, &sql)?;
    }

    let counts = if check == Check::Restored {
        Counts::Exact
    } else {
        Counts::Skip
    };
    let mut problems = Vec::new();
    for db in &set.databases {
        let fp = steps.run(
            rt,
            &format!(
                "Verifying '{}' ({})",
                db.name,
                if counts == Counts::Exact {
                    "row counts, structure"
                } else {
                    "structure"
                }
            ),
            |_| fingerprint_of(&new, &db.name, None, counts),
        )?;
        for d in fingerprint::differences(&db.fingerprint, &fp, check) {
            problems.push(format!("{}: {d}", db.name));
        }
    }
    if !problems.is_empty() {
        return Err(verification_error(
            "The restored copy does not match the dump",
            &problems,
        ));
    }
    for db in &set.databases {
        steps.run(
            rt,
            &format!(
                "Updating the extensions of '{}' (then its structure is checked again)",
                db.name
            ),
            |_| update_extensions(&new, &db.name),
        )?;
        let fp = fingerprint_of(&new, &db.name, None, Counts::Skip)?;
        for d in fingerprint::differences(&db.fingerprint, &fp, Check::Updated) {
            problems.push(format!("{}: {d}", db.name));
        }
    }
    if !problems.is_empty() {
        return Err(verification_error(
            "The copy changed when its extensions were updated",
            &problems,
        ));
    }
    steps.run(rt, "Stopping the temporary server", |_| server.stop())
}

/// `MRSM_TEST_UPGRADE_POSTGRES_FAIL=<point>:<database>:<sql>` runs SQL at a point of a run: the
/// end-to-end tests' failure injection (`after_restore` changes the restored copy before it is
/// verified, `after_dump` writes to the old server after its dump). Only read when
/// `MRSM_TEST_DOCKER` is set as well.
pub(crate) fn test_hook(point: &str) -> Option<(String, String)> {
    std::env::var("MRSM_TEST_DOCKER").ok()?;
    let value = std::env::var("MRSM_TEST_UPGRADE_POSTGRES_FAIL").ok()?;
    let mut parts = value.splitn(3, ':');
    (parts.next()? == point).then_some(())?;
    Some((parts.next()?.to_string(), parts.next()?.to_string()))
}

pub(crate) fn verification_error(title: &str, problems: &[String]) -> StepError {
    let listed: Vec<String> = problems
        .iter()
        .take(20)
        .map(|p| format!("  - {p}"))
        .collect();
    let more = if problems.len() > 20 {
        format!("\n  ... and {} more", problems.len() - 20)
    } else {
        String::new()
    };
    StepError::new(format!("{title}:\n{}{more}", listed.join("\n")))
}

fn databases_of(pg: &PgContainer) -> Res<BTreeSet<String>> {
    let value = pg.json(
        "template1",
        "SELECT coalesce(json_agg(datname), '[]') FROM pg_catalog.pg_database;",
    )?;
    Ok(value
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(Json::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default())
}

/// Write a file inside a container (the restore lists `pg_restore -L` reads).
fn put_file(container: &str, path: &str, contents: &[u8]) -> Res<()> {
    let done = Docker::new(&[
        "exec",
        "-i",
        container,
        "sh",
        "-c",
        "mkdir -p \"$(dirname \"$1\")\" && cat > \"$1\"",
        "mrsm-upgrade",
        path,
    ])
    .run_with(Input::Bytes(contents), Output::Capture)?;
    if !done.ok() {
        return Err(StepError::new(format!(
            "Cannot write '{path}' in '{container}':\n{}",
            done.stderr_tail(10)
        )));
    }
    Ok(())
}

/// Run `pg_restore` in the new server's container with the dump on standard input.
fn pg_restore(new: &PgContainer, args: &[String], dump: &Path) -> Res<String> {
    let done = new.tool("pg_restore", args, Input::File(dump), Output::Capture)?;
    if !done.ok() {
        return Err(StepError::new(format!(
            "`pg_restore {}` failed (exit code {}):\n{}",
            args.join(" "),
            done.code,
            done.stderr_tail(20)
        )));
    }
    Ok(done.stdout_text())
}

/// The encoding and locale of a database of the new server.
fn locale_of(new: &PgContainer, database: &str) -> Res<Option<DbLocale>> {
    let fp = new.json(database, fingerprint::BASE_QUERY)?;
    Ok(fingerprint::database_locale(&fp))
}

/// Restore one database into the new server.
fn restore_database(new: &PgContainer, db: &DumpedDb, dump: &Path, exists: bool) -> Res<()> {
    let list_dir = "/tmp/mrsm-upgrade";
    let stem = db.dump.file.trim_end_matches(".dump");

    // The database itself (encoding, locale, owner as in the dump), and its properties.
    let create_list = pg_restore(new, &["-C".into(), "-l".into()], dump)?;
    let entries = sqltext::database_entries(&create_list);
    let db_list = format!("{list_dir}/{stem}.database.list");
    put_file(&new.container, &db_list, entries.as_bytes())?;
    let script = pg_restore(
        new,
        &["-C".into(), "-f".into(), "-".into(), "-L".into(), db_list],
        dump,
    )?;
    let statements = sqltext::split_database_script(&script);
    let mut create = !exists;
    if exists
        && let Some(dumped) = fingerprint::database_locale(&db.fingerprint)
        && let Some(current) = locale_of(new, &db.name)?
        && !dumped.same_as(&current)
    {
        // The image's setup created it (`postgres`, `POSTGRES_DB`) with its own encoding or
        // locale: it holds nothing of the dump yet, so it is created again as the dump says.
        if statements.create.is_empty() {
            return Err(StepError::new(format!(
                "The dump of '{}' does not say how to create the database.",
                db.name
            )));
        }
        new.psql(
            "template1",
            &format!("DROP DATABASE {} WITH (FORCE);", quote_ident(&db.name)),
        )?;
        create = true;
    }
    let mut setup = Vec::new();
    if create {
        setup.extend(statements.create.iter().cloned());
    }
    setup.extend(statements.alter.iter().cloned());
    if !setup.is_empty() {
        new.psql("template1", &format!("{}\n", setup.join("\n")))?;
    }

    // The extensions: exactly the dump's set; TimescaleDB at the dump's version.
    let dumped = fingerprint::extensions(&db.fingerprint);
    let current = parse_extensions(&new.json(&db.name, EXTENSIONS_QUERY)?);
    let mut drops = Vec::new();
    for (name, ext) in &current {
        if name == "plpgsql" {
            continue;
        }
        match dumped.get(name) {
            None => drops.push(name.clone()),
            Some((_, schema)) if *schema != ext.schema => drops.push(name.clone()),
            Some((version, _)) if name == "timescaledb" && *version != ext.version => {
                drops.push(name.clone())
            }
            _ => {}
        }
    }
    if !drops.is_empty() {
        let sql: Vec<String> = drops
            .iter()
            .map(|n| format!("DROP EXTENSION IF EXISTS {} CASCADE;", quote_ident(n)))
            .collect();
        new.psql(&db.name, &sql.join("\n"))?;
    }
    let full_list = pg_restore(new, &["-l".into()], dump)?;
    let mut created_schemas = BTreeSet::new();
    let timescale = dumped.get("timescaledb").cloned();
    if let Some((version, schema)) = &timescale {
        let now = parse_extensions(&new.json(&db.name, EXTENSIONS_QUERY)?);
        if !now.contains_key("timescaledb") {
            // Its schema first, as the dump has it (owner included), when it is not there yet:
            // the restore then skips that schema's entry.
            let missing = new
                .psql(
                    &db.name,
                    &format!(
                        "SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname = {};",
                        quote_literal(schema)
                    ),
                )?
                .trim()
                == "0";
            if missing {
                let owner = sqltext::schema_owner(&full_list, schema)
                    .map(|o| format!(" AUTHORIZATION {}", quote_ident(&o)))
                    .unwrap_or_default();
                new.psql(
                    &db.name,
                    &format!("CREATE SCHEMA {}{owner};", quote_ident(schema)),
                )?;
                created_schemas.insert(schema.clone());
            }
            // The first statement of a new session.
            new.psql(
                &db.name,
                &format!(
                    "CREATE EXTENSION timescaledb VERSION {} SCHEMA {};",
                    quote_literal(version),
                    quote_ident(schema)
                ),
            )?;
        }
    }
    let now = parse_extensions(&new.json(&db.name, EXTENSIONS_QUERY)?);
    let skip: BTreeSet<String> = now
        .iter()
        .filter(|(name, ext)| {
            name.as_str() == "plpgsql"
                || dumped
                    .get(name.as_str())
                    .is_some_and(|(_, schema)| *schema == ext.schema)
        })
        .map(|(name, _)| name.clone())
        .collect();

    // The data.
    let (list, _) = sqltext::filter_restore_list(&full_list, &skip, &created_schemas);
    let list_path = format!("{list_dir}/{stem}.list");
    put_file(&new.container, &list_path, list.as_bytes())?;
    if let Some((_, schema)) = &timescale {
        new.psql(
            &db.name,
            &format!("SELECT {}.timescaledb_pre_restore();", quote_ident(schema)),
        )?;
    }
    pg_restore(
        new,
        &[
            "--exit-on-error".into(),
            "-L".into(),
            list_path,
            "-d".into(),
            docker::conninfo(&db.name),
        ],
        dump,
    )?;
    if let Some((_, schema)) = &timescale {
        new.psql(
            &db.name,
            &format!("SELECT {}.timescaledb_post_restore();", quote_ident(schema)),
        )?;
    }
    if !statements.properties.is_empty() {
        new.psql(&db.name, &statements.properties)?;
    }
    new.psql(&db.name, "ANALYZE;")?;
    Ok(())
}

/// `ALTER SYSTEM` for the old server's `postgresql.auto.conf` settings; the preload list keeps
/// the new server's libraries and adds the old ones the new image has. Returns what the new
/// server refused or lacks, as warnings.
fn carry_settings(new: &PgContainer, settings: &[(String, String)]) -> Res<Vec<String>> {
    let mut warnings = Vec::new();
    for (name, value) in settings {
        if let Some(why) = sqltext::not_carried(name) {
            warnings.push(format!(
                "Not carried over to the new server: {name} = '{value}' ({why} belong to the old \
                 server). Set it again if the new server needs it."
            ));
            continue;
        }
        let sql = if name == "shared_preload_libraries" {
            // Only the libraries the new image has: a missing one would keep it from starting.
            let files = new.json(
                "template1",
                "SELECT coalesce(json_agg(f), '[]') FROM pg_catalog.pg_ls_dir((SELECT setting \
                 FROM pg_catalog.pg_config WHERE name = 'PKGLIBDIR')) AS f;",
            )?;
            let files: BTreeSet<String> = files
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Json::as_str)
                .map(str::to_string)
                .collect();
            let (kept, missing) = sqltext::available_libraries(value, &files);
            if !missing.is_empty() {
                warnings.push(format!(
                    "Not preloaded on the new server (the new image does not have them): {}.",
                    missing.join(", ")
                ));
            }
            let current = new.psql("template1", "SHOW shared_preload_libraries;")?;
            sqltext::alter_system_list(name, &sqltext::merge_preload(current.trim(), &kept))
        } else {
            sqltext::alter_system(name, value)
        };
        if let Err(e) = new.psql("template1", &sql) {
            warnings.push(format!(
                "The setting {name} = '{value}' was not carried over: {}",
                e.message
            ));
        }
    }
    Ok(warnings)
}

/// Update every extension to the new server's default version: TimescaleDB as the first
/// statement of its own session, the PostGIS family with `postgis_extensions_upgrade()`.
fn update_extensions(new: &PgContainer, database: &str) -> Res<()> {
    let extensions = parse_extensions(&new.json(database, EXTENSIONS_QUERY)?);
    let lags = |name: &str| {
        extensions
            .get(name)
            .is_some_and(|e| e.default.as_ref().is_some_and(|d| *d != e.version))
    };
    if lags("timescaledb") {
        new.psql(database, "ALTER EXTENSION timescaledb UPDATE;")?;
    }
    let postgis_family = |name: &str| name.starts_with("postgis") || name == "address_standardizer";
    if extensions.keys().any(|n| postgis_family(n) && lags(n)) && extensions.contains_key("postgis")
    {
        new.psql(database, "SELECT postgis_extensions_upgrade();")?;
    }
    for name in extensions.keys() {
        if name == "timescaledb" || name == "plpgsql" || postgis_family(name) || !lags(name) {
            continue;
        }
        new.psql(
            database,
            &format!("ALTER EXTENSION {} UPDATE;", quote_ident(name)),
        )?;
    }
    Ok(())
}
