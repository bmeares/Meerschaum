//! The upgrade's preflight: everything is read and checked before anything changes, then the
//! plan is shown. The only things created here are a throwaway container of the target image (to
//! ask it which extensions and locales it can create; removed again whatever happens) and, after
//! asking, the pulled image.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use serde_json::Value as Json;

use meerschaum_core::SuccessTuple;

use crate::builtin::ActionError;
use crate::dispatch::Runtime;

use super::docker::{self, Docker, Input, Output, PULL_NEVER, PgContainer, StepError};
use super::fingerprint::DbLocale;
use super::images::{self, TargetSpec};
use super::server::{
    self, EXTENSIONS_QUERY, Extension, ServerInfo, Target, Tools, env_value, parse_extensions,
};
use super::sqltext::{self, version_cmp};
use super::stackcfg::{self, DataVolume, Switch};
use super::switch::{ConfigEdit, config_edit};
use super::{DB_SERVICE, DOCS_HINT, LABEL, Options, Stack, human_bytes};

/// The TimescaleDB versions of one database across the upgrade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TsPlan {
    /// Installed in the old database.
    pub installed: String,
    /// What the dump carries (the old server is updated to it first when it differs).
    pub dump: String,
    /// The new server's version, after the update.
    pub target: String,
}

impl TsPlan {
    pub fn update_old(&self) -> bool {
        self.installed != self.dump
    }
}

/// One database to move.
#[derive(Debug, Clone)]
pub(crate) struct DbPlan {
    pub name: String,
    pub size: i64,
    pub extensions: BTreeMap<String, Extension>,
    pub timescaledb: Option<TsPlan>,
}

/// How the `db` service runs (what the temporary server reproduces).
#[derive(Debug, Clone, Default)]
pub(crate) struct ServiceSpec {
    pub env: Vec<(String, String)>,
    /// The other mounts (read-only binds).
    pub mount_args: Vec<String>,
    pub command: Vec<String>,
    pub entrypoint: Vec<String>,
    pub shm_size: Option<String>,
    pub user: Option<String>,
}

impl ServiceSpec {
    /// The service as Compose resolved it; `data_dirs` and `socket_dirs` are the places no other
    /// mount may share (see [`server::mount_args`]).
    pub fn from_service(
        db: &Json,
        data_target: &str,
        data_dirs: &[String],
        socket_dirs: &[String],
    ) -> Result<ServiceSpec, String> {
        Ok(ServiceSpec {
            env: server::service_env(db),
            mount_args: server::mount_args(db, data_target, data_dirs, socket_dirs)?,
            command: server::str_list(db.get("command")),
            entrypoint: server::str_list(db.get("entrypoint")),
            shm_size: db.get("shm_size").and_then(|v| match v {
                Json::String(s) => Some(s.clone()),
                Json::Number(n) => Some(n.to_string()),
                _ => None,
            }),
            user: db
                .get("user")
                .and_then(Json::as_str)
                .filter(|u| !u.is_empty())
                .map(str::to_string),
        })
    }

    /// The superuser the image creates (`POSTGRES_USER`, else `postgres`).
    pub fn superuser(&self) -> String {
        env_value(&self.env, "POSTGRES_USER")
            .filter(|u| !u.is_empty())
            .unwrap_or("postgres")
            .to_string()
    }

    /// The variables of a client tool run from another image against the server (passed by
    /// name: `-e PGPASSWORD` takes the value from the `docker` process's environment).
    pub fn tool_env(&self, port: &str) -> Vec<(String, String)> {
        let mut env = vec![
            ("PGHOST".to_string(), "127.0.0.1".to_string()),
            ("PGPORT".to_string(), port.to_string()),
            ("PGUSER".to_string(), self.superuser()),
        ];
        if let Some(password) = env_value(&self.env, "POSTGRES_PASSWORD").filter(|p| !p.is_empty())
        {
            env.push(("PGPASSWORD".to_string(), password.to_string()));
        }
        env
    }
}

/// Everything the upgrade does, decided before it starts.
pub(crate) struct Plan {
    pub run_id: String,
    pub run_dir: PathBuf,
    /// The running `db` container (the old server).
    pub old: PgContainer,
    pub old_image: String,
    pub info: ServerInfo,
    pub new_image: String,
    pub target: Target,
    pub volume: DataVolume,
    pub switch: Switch,
    pub new_volume: String,
    /// Why the new volume does not have the usual name, when it does not.
    pub volume_note: Option<String>,
    /// The labels the volume's declaration gives it.
    pub volume_labels: Vec<(String, String)>,
    /// How the stack config file changes.
    pub config_edit: ConfigEdit,
    /// The stack config file.
    pub config_file: PathBuf,
    pub databases: Vec<DbPlan>,
    /// Running services that depend on `db`: stopped meanwhile, started again afterwards.
    pub dependents: Vec<String>,
    pub service: ServiceSpec,
    /// Compose's project name (the volume label).
    pub project_label: String,
    /// The tools that dump the old server.
    pub tools: Tools,
    pub warnings: Vec<String>,
}

impl Plan {
    /// A container name of this run.
    pub fn container(&self, role: &str) -> String {
        format!(
            "{}-pg{role}-{}",
            self.project_label,
            self.run_id.to_lowercase()
        )
    }
}

/// Why a preflight stopped.
pub(crate) enum Halt {
    Stop(SuccessTuple),
    Action(ActionError),
}

impl From<String> for Halt {
    fn from(message: String) -> Halt {
        Halt::Stop(SuccessTuple::fail(message))
    }
}

impl From<&str> for Halt {
    fn from(message: &str) -> Halt {
        Halt::Stop(SuccessTuple::fail(message))
    }
}

impl From<StepError> for Halt {
    fn from(e: StepError) -> Halt {
        Halt::Stop(SuccessTuple::fail(e.message))
    }
}

impl From<ActionError> for Halt {
    fn from(e: ActionError) -> Halt {
        Halt::Action(e)
    }
}

/// The PostgreSQL major of the data in a volume (`PG_VERSION`), read through a throwaway
/// container of `image` that mounts the volume READ-ONLY and runs only `cat`.
pub(crate) fn volume_major(volume: &str, image: &str) -> Option<u32> {
    let mount = format!("{volume}:/mrsm-data:ro");
    let mut args = vec!["run", "--rm"];
    args.extend(PULL_NEVER);
    args.extend([
        "--network",
        "none",
        "-v",
        &mount,
        "--entrypoint",
        "sh",
        image,
        "-c",
        "for f in /mrsm-data/PG_VERSION /mrsm-data/*/PG_VERSION /mrsm-data/*/*/PG_VERSION; do \
         [ -f \"$f\" ] && exec cat \"$f\"; done; exit 3",
    ]);
    let done = Docker::new(&args).run().ok()?;
    if !done.ok() {
        return None;
    }
    done.stdout_text().trim().parse().ok()
}

/// The Docker names of the named volumes a resolved service mounts.
pub(crate) fn service_volume_names(stack: &Stack, service: &Json) -> Vec<String> {
    service
        .get("volumes")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .filter(|m| m.get("type").and_then(Json::as_str) == Some("volume"))
        .filter_map(|m| m.get("source").and_then(Json::as_str))
        .map(|key| {
            stack
                .resolved
                .get("volumes")
                .and_then(|v| v.get(key))
                .and_then(|v| v.get("name"))
                .and_then(Json::as_str)
                .unwrap_or(key)
                .to_string()
        })
        .collect()
}

/// Why the stack's `db` is not running, and what to do (the data's major is read from the
/// volume when the configured image cannot be its server).
pub(crate) fn not_running(stack: &Stack, db: &Json) -> String {
    let project = &stack.compose.project;
    let generic = format!(
        "The stack's database is not running (project '{project}'). Start it with `mrsm stack up \
         -d db`, then run this again."
    );
    let image = db.get("image").and_then(Json::as_str).unwrap_or_default();
    let volumes = service_volume_names(stack, db);
    let [volume] = volumes.as_slice() else {
        return generic;
    };
    if !docker::volume_exists(volume) {
        return format!(
            "The stack's database is not running and its volume '{volume}' does not exist yet: \
             there is no data to upgrade. `mrsm stack up -d db` creates it with '{image}'."
        );
    }
    let (Some(data), Some(configured)) = (volume_major(volume, image), images::tag_major(image))
    else {
        return generic;
    };
    if data >= configured {
        return generic;
    }
    let pinned = images::derive_target(image, data)
        .unwrap_or_else(|_| format!("<an image of PostgreSQL {data}>"));
    format!(
        "The stack's database is not running: its image '{image}' (PostgreSQL {configured}) \
         cannot start on the data in '{volume}', which is PostgreSQL {data}. This happens when \
         the stack config's image changes (e.g. a new default) while the data stays where it \
         was.\nFirst run the data's own major again: `mrsm edit config stack` and set \
         `docker-compose.yaml: services: db: image` to '{pinned}' (keep everything else), then \
         `mrsm stack up -d db`. Then `mrsm upgrade postgres {configured}` moves the data to \
         PostgreSQL {configured}."
    )
}

/// The data volume must be a plain local volume (see [`stackcfg::check_declaration`]).
pub(crate) fn check_volume_driver(name: &str) -> Result<(), String> {
    if let Some(info) = docker::volume_info(name)
        && (info.driver != "local" || !info.options.is_empty())
    {
        return Err(format!(
            "The data volume '{name}' uses the driver '{}'{}; this command creates the new volume \
             with Docker's default local driver and cannot reproduce that. Follow the manual \
             procedure in the docs (\"Upgrading PostgreSQL\").",
            info.driver,
            if info.options.is_empty() {
                ""
            } else {
                " with driver options"
            }
        ));
    }
    Ok(())
}

/// Why an existing volume cannot be the new one, in words (who uses it, whose it is).
pub(crate) fn describe_volume(name: &str) -> String {
    let users = docker::volume_users(name).unwrap_or_default();
    if !users.is_empty() {
        return format!("'{name}' is used by {}", users.join(", "));
    }
    match docker::volume_info(name).and_then(|v| v.labels.get(LABEL).cloned()) {
        Some(run) => format!(
            "'{name}' was left by the run {run} (no container uses it; `docker volume rm {name}` \
             removes it once you no longer need it)"
        ),
        None => format!("'{name}' exists"),
    }
}

/// The new volume's key and name, and why the usual name was passed over (`keys`: in order of
/// preference).
pub(crate) fn choose_volume(
    stack: &Stack,
    volume: &DataVolume,
    keys: &[String],
) -> Result<(String, String, Option<String>), String> {
    let declared_name = stack
        .raw
        .get_path(&["docker-compose.yaml", "volumes", &volume.key, "name"])
        .is_some();
    let declared = stackcfg::Declared::from_resolved(&stack.resolved);
    let preferred = stackcfg::new_volume_name(volume, &keys[0], declared_name);
    match stackcfg::choose_new_volume(volume, keys, declared_name, &declared, &|name| {
        docker::volume_exists(name)
    }) {
        Ok((key, name, None)) => Ok((key, name, None)),
        Ok((key, name, Some(why))) => {
            let why = if docker::volume_exists(&preferred) && preferred != volume.name {
                describe_volume(&preferred)
            } else {
                why
            };
            Ok((key, name, Some(format!("not '{preferred}': {why}"))))
        }
        Err(reasons) => Err(format!(
            "There is no free name for the new volume ({}).",
            reasons.join("; ")
        )),
    }
}

/// The socket directories a temporary server must not share: the old server's and the image
/// defaults'.
pub(crate) fn socket_dirs(info_dirs: &[String]) -> Vec<String> {
    let mut dirs = info_dirs.to_vec();
    for default in ["/var/run/postgresql", "/run/postgresql"] {
        if !dirs.iter().any(|d| d == default) {
            dirs.push(default.to_string());
        }
    }
    dirs.retain(|d| !d.is_empty() && d != "/tmp");
    dirs
}

/// The preflight: the plan to execute, or why not to.
pub(crate) fn preflight(
    rt: &mut Runtime<'_>,
    opts: &Options,
    stack: &Stack,
    word: Option<&str>,
) -> Result<Plan, Halt> {
    let spec = images::parse_target(word)?;
    let db = stack
        .db()
        .cloned()
        .ok_or("The stack has no `db` service, so there is no database to upgrade.")?;
    let Some(container) = stack.compose.container_id(DB_SERVICE)? else {
        return Err(not_running(stack, &db).into());
    };
    let old_image = db
        .get("image")
        .and_then(Json::as_str)
        .unwrap_or_default()
        .to_string();
    let running_image = docker::container_image(&container)?;
    if running_image != old_image {
        return Err(format!(
            "The stack's `db` container runs '{running_image}', but the stack config says \
             '{old_image}'. Bring them in line first (`mrsm stack up -d db` applies the config), \
             then run this again."
        )
        .into());
    }
    let old = PgContainer::new(container);
    let info = ServerInfo::read(&old)?;
    if let Some(refusal) = info.refusal(DOCS_HINT) {
        return Err(refusal.into());
    }

    // The target image.
    let default_image = rt
        .config()
        .defaults()
        .get("stack")
        .and_then(|s| s.get_path(&["docker-compose.yaml", "services", DB_SERVICE, "image"]))
        .map(|v| v.py_str())
        .unwrap_or_default();
    let wanted_major = match &spec {
        TargetSpec::Default => Some(images::tag_major(&default_image).ok_or_else(|| {
            format!("Cannot tell the default PostgreSQL major from '{default_image}'.")
        })?),
        TargetSpec::Major(m) => Some(*m),
        TargetSpec::Image(_) => None,
    };
    if let Some(major) = wanted_major {
        if major == info.major {
            return Err(Halt::Stop(SuccessTuple::ok(format!(
                "The stack's database already runs PostgreSQL {}: nothing to do.",
                info.version
            ))));
        }
        if major < info.major {
            return Err(format!(
                "The stack's database runs PostgreSQL {}; moving it to an older major ({major}) \
                 is not supported.",
                info.version
            )
            .into());
        }
    }
    let new_image = match &spec {
        TargetSpec::Image(image) => image.clone(),
        _ => images::derive_target(&old_image, wanted_major.unwrap_or_default()).map_err(|e| {
            format!("{e}\nName the image instead (`mrsm upgrade postgres <image>`). {DOCS_HINT}")
        })?,
    };
    if new_image == old_image {
        return Err(Halt::Stop(SuccessTuple::ok(format!(
            "The stack's database already runs '{old_image}': nothing to do."
        ))));
    }

    // The data volume: a plain local volume, held by the config as Compose resolved it.
    let empty = Json::Object(Default::default());
    let volume = stackcfg::data_volume(
        &db,
        stack.resolved.get("volumes").unwrap_or(&empty),
        &info.data_directory,
    )?;
    stackcfg::check_editable(&stack.raw, &old_image, &volume)?;
    stackcfg::check_declaration(&stack.resolved, &volume.key)?;
    check_volume_driver(&volume.name)?;

    // The databases.
    let mut databases = Vec::new();
    let mut ts_installed: BTreeSet<String> = BTreeSet::new();
    for database in &info.databases {
        let extensions = parse_extensions(&old.json(&database.name, EXTENSIONS_QUERY)?);
        if let Some(ts) = extensions.get("timescaledb") {
            ts_installed.insert(ts.version.clone());
        }
        databases.push(DbPlan {
            name: database.name.clone(),
            size: database.size,
            extensions,
            timescaledb: None,
        });
    }

    // Pull the target image (after asking; `--noask` alone never pulls: the upgrade would not
    // start after it anyway).
    if !docker::image_exists(&new_image) {
        let pull = super::ask(
            rt,
            &format!("The image '{new_image}' is not on this host. Pull it now?"),
            if opts.noask { "n" } else { "y" },
            opts,
        )?;
        if !pull {
            return Err(
                format!("The image '{new_image}' was not pulled; nothing was changed.").into(),
            );
        }
        let done = Docker::new(&["pull", &new_image]).run_with(Input::Null, Output::Inherit)?;
        if !done.ok() {
            return Err(format!("Pulling '{new_image}' failed; nothing was changed.").into());
        }
    }
    let image_config = docker::image_config(&new_image)?;

    // How the temporary server runs: the stack's environment and command, its other mounts
    // read-only (refused where two servers would share a place).
    let run_id = super::fresh_run_id(&stack.backups);
    let mut warnings = Vec::new();
    let env = server::service_env(&db);
    let new_pgdata = env_value(&env, "PGDATA")
        .or_else(|| image_config.env_var("PGDATA"))
        .unwrap_or("/var/lib/postgresql/data")
        .to_string();
    let new_target = stackcfg::new_mount_target(&volume.target, &new_pgdata, &image_config.volumes);
    let data_dirs = vec![
        info.data_directory.clone(),
        volume.target.clone(),
        new_pgdata.clone(),
        new_target.clone(),
    ];
    let service = ServiceSpec::from_service(
        &db,
        &volume.target,
        &data_dirs,
        &socket_dirs(&info.socket_dirs),
    )?;

    // Ask the target image.
    let mut probe_versions = ts_installed.clone();
    if let Some(default) = info.available.get("timescaledb")
        && !ts_installed.is_empty()
    {
        probe_versions.insert(default.clone());
    }
    let locales: Vec<(String, DbLocale)> = info
        .databases
        .iter()
        .filter_map(|d| Some((d.name.clone(), d.locale.clone()?)))
        .collect();
    let project_label = stack.project_label();
    let probe_name = format!("{project_label}-pgcheck-{}", run_id.to_lowercase());
    super::super::info(
        rt,
        &format!("Checking what '{new_image}' can run (a throwaway container)..."),
    );
    let target = server::probe_target(
        &new_image,
        &service.superuser(),
        &probe_name,
        &run_id,
        &probe_versions,
        &locales,
        &[],
    )?;
    if target.major < info.major {
        return Err(format!(
            "'{new_image}' runs PostgreSQL {}, older than the stack's {}; moving to an older \
             major is not supported.",
            target.version, info.version
        )
        .into());
    }

    // TimescaleDB, the other extensions and the locales.
    let mut problems = Vec::new();
    for db_plan in &mut databases {
        for (name, ext) in &db_plan.extensions {
            if name != "plpgsql" && !target.available.contains_key(name) {
                problems.push(format!(
                    "The extension '{name}' {} (database '{}') is not available in '{new_image}'.",
                    ext.version, db_plan.name
                ));
            }
        }
        let Some(ts) = db_plan.extensions.get("timescaledb") else {
            continue;
        };
        let old_default = info.available.get("timescaledb");
        let dump = if target.timescaledb_usable.contains(&ts.version) {
            Some(ts.version.clone())
        } else {
            old_default
                .filter(|d| version_cmp(d, &ts.version).is_gt())
                .filter(|d| target.timescaledb_usable.contains(*d))
                .cloned()
        };
        match (dump, target.available.get("timescaledb")) {
            (Some(dump), Some(target_version)) => {
                db_plan.timescaledb = Some(TsPlan {
                    installed: ts.version.clone(),
                    dump,
                    target: target_version.clone(),
                });
            }
            _ => problems.push(format!(
                "TimescaleDB {} (database '{}') cannot be created in '{new_image}' (it can \
                 create: {}), and neither can the newest version of the current image ({}). \
                 Pull a newer '{old_image}' and run `mrsm upgrade extensions`, or name another \
                 image.",
                ts.version,
                db_plan.name,
                if target.timescaledb_usable.is_empty() {
                    "none of them".to_string()
                } else {
                    target
                        .timescaledb_usable
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                },
                old_default.cloned().unwrap_or_else(|| "none".into())
            )),
        }
    }
    if !target.locale_problems.is_empty() {
        problems.extend(target.locale_problems.iter().cloned());
        problems.push(format!(
            "A database's encoding and locale are kept as they are (they decide how text sorts \
             and compares). Use an image that has the locale, or follow the manual procedure. \
             {DOCS_HINT}"
        ));
    }
    if !problems.is_empty() {
        return Err(problems.join("\n").into());
    }

    // The new volume: a name nothing uses.
    let keys = vec![
        stackcfg::new_volume_key(&volume.key, target.major, &run_id),
        stackcfg::restore_volume_key(&volume.key, target.major, &run_id),
    ];
    let (new_key, new_volume, volume_note) = choose_volume(stack, &volume, &keys)?;
    let declared_name = stack
        .raw
        .get_path(&["docker-compose.yaml", "volumes", &volume.key, "name"])
        .is_some();
    let switch = Switch {
        image: new_image.clone(),
        old_key: volume.key.clone(),
        old_target: volume.target.clone(),
        new_key,
        new_target,
        new_declared_name: declared_name.then(|| new_volume.clone()),
    };
    let config_edit = config_edit(stack, &switch);
    let volume_labels = stackcfg::declared_labels(&stack.resolved, &volume.key);

    // What else would write, and the room the copies need.
    let running = stack.compose.running_services()?;
    let dependents: Vec<String> = stack
        .dependents()
        .into_iter()
        .filter(|s| running.contains(s))
        .collect();
    let jobs = server::running_jobs(rt);
    if !jobs.is_empty() {
        warnings.push(format!(
            "Meerschaum jobs are running in this root ({}): stop the ones that write to this \
             database first (`mrsm stop jobs`).",
            jobs.join(", ")
        ));
    }
    if info.clients > 0 {
        warnings.push(format!(
            "{} client connection{} to the database now; after the services above are \
             stopped, any connection still open stops the upgrade (unless `--force`).",
            info.clients,
            if info.clients == 1 {
                " is open"
            } else {
                "s are open"
            }
        ));
    }
    let total = info.total_size();
    let run_dir = stack.backups.join(&run_id);
    let mut space_problems = Vec::new();
    if let Some(free) = server::host_free_bytes(&run_dir)
        && free < total / 2
    {
        space_problems.push(format!(
            "only {} free for the dumps in {} (the databases take {}).",
            human_bytes(free),
            stack.backups.display(),
            human_bytes(total)
        ));
    }
    if let Some(free) = server::container_free_bytes(&old.container, &info.data_directory)
        && free < total
    {
        space_problems.push(format!(
            "only {} free for Docker's volumes (the new copy needs about {}).",
            human_bytes(free),
            human_bytes(total)
        ));
    }
    if !space_problems.is_empty() {
        let text = format!("Low disk space: {}", space_problems.join(" "));
        if !opts.force {
            return Err(format!("{text} Free some space, or pass `--force` to try anyway.").into());
        }
        warnings.push(text);
    }

    // The new image's client tools, when they reach the old server.
    let image_tools = Tools::Image {
        image: new_image.clone(),
        server: old.container.clone(),
        env: service.tool_env(&info.port),
        run_id: run_id.clone(),
        name_prefix: format!("{project_label}-pgdump-{}", run_id.to_lowercase()),
        count: Cell::new(0),
    };
    let tools = if image_tools.reach() {
        image_tools
    } else {
        Tools::Container(old.clone())
    };

    Ok(Plan {
        run_id,
        run_dir,
        old,
        old_image,
        info,
        new_image,
        target,
        volume,
        switch,
        new_volume,
        volume_note,
        volume_labels,
        config_edit,
        config_file: stack.config_file(),
        databases,
        dependents,
        service,
        project_label,
        tools,
        warnings,
    })
}

/// The stack config line of a plan.
pub(crate) fn render_config_edit(edit: &ConfigEdit, file: &std::path::Path) -> String {
    match edit {
        ConfigEdit::Lines { changed, .. } => {
            let mut text = format!(
                "{}: only these lines change (the file is copied into the backup first):",
                file.display()
            );
            for (before, after) in changed.iter().take(8) {
                if before.is_empty() {
                    text.push_str(&format!("\n{:20}+ {after}", ""));
                } else {
                    text.push_str(&format!("\n{:20}{before}  ->  {after}", ""));
                }
            }
            text
        }
        ConfigEdit::Rewrite { reason } => format!(
            "{} is written again with only the `db` image, its volume and that volume's \
             declaration changed, but without its comments and formatting ({reason}); the \
             original is copied byte for byte into the backup first",
            file.display()
        ),
        ConfigEdit::Create => format!(
            "{} is created with the stack's whole config (there is none now: the stack follows \
             Meerschaum's defaults, which will then no longer change it)",
            file.display()
        ),
    }
}

/// The server settings line of a plan: what is carried over, and what is not.
pub(crate) fn render_settings(settings: &[(String, String)]) -> Option<String> {
    if settings.is_empty() {
        return None;
    }
    let carried: Vec<&str> = settings
        .iter()
        .filter(|(k, _)| sqltext::not_carried(k).is_none())
        .map(|(k, _)| k.as_str())
        .collect();
    let kept_back: Vec<&str> = settings
        .iter()
        .filter(|(k, _)| sqltext::not_carried(k).is_some())
        .map(|(k, _)| k.as_str())
        .collect();
    let mut text = String::new();
    if !carried.is_empty() {
        text.push_str(&format!(
            "{} (from postgresql.auto.conf)",
            carried.join(", ")
        ));
    }
    if !kept_back.is_empty() {
        if !text.is_empty() {
            text.push_str("; ");
        }
        text.push_str(&format!(
            "not carried over: {} (they act on the old server's files or replication)",
            kept_back.join(", ")
        ));
    }
    Some(text)
}

/// The plan, as the user reads it before confirming.
pub(crate) fn render(plan: &Plan, compose_project: &str) -> String {
    let mut out = String::new();
    let line = |out: &mut String, label: &str, text: &str| {
        out.push_str(&format!("  {label:<18}{text}\n"));
    };
    out.push_str(&format!(
        "Upgrade of the stack's database (project '{compose_project}', service `{DB_SERVICE}`):\n\n"
    ));
    line(
        &mut out,
        "PostgreSQL",
        &format!(
            "{} -> {} ({} -> {})",
            plan.info.version, plan.target.version, plan.old_image, plan.new_image
        ),
    );
    let updates_old = plan
        .databases
        .iter()
        .any(|d| d.timescaledb.as_ref().is_some_and(TsPlan::update_old));
    line(
        &mut out,
        "Old volume",
        &if updates_old {
            format!(
                "{} (kept for a rollback and never removed; only TimescaleDB is updated in it, \
                 see below)",
                plan.volume.name
            )
        } else {
            format!(
                "{} (kept for a rollback: the upgrade never writes to it or removes it)",
                plan.volume.name
            )
        },
    );
    line(
        &mut out,
        "New volume",
        &match &plan.volume_note {
            Some(note) => format!(
                "{} (at {}, created and filled now; {note})",
                plan.new_volume, plan.switch.new_target
            ),
            None => format!(
                "{} (at {}, created and filled now)",
                plan.new_volume, plan.switch.new_target
            ),
        },
    );
    line(
        &mut out,
        "Backup",
        &format!(
            "{} (a full backup first: dumps, stack config, manifest)",
            plan.run_dir.display()
        ),
    );
    line(
        &mut out,
        "Stack config",
        &render_config_edit(&plan.config_edit, &plan.config_file),
    );
    let dbs: Vec<String> = plan
        .databases
        .iter()
        .map(|d| format!("{} ({})", d.name, human_bytes(d.size)))
        .collect();
    line(&mut out, "Databases", &dbs.join(", "));
    for d in &plan.databases {
        if let Some(ts) = &d.timescaledb {
            let text = if ts.update_old() {
                format!(
                    "{}: {} -> {} on the old server first (the new image cannot create {}), \
                     restored at {}, then {}",
                    d.name, ts.installed, ts.dump, ts.installed, ts.dump, ts.target
                )
            } else if ts.dump == ts.target {
                format!("{}: {} (restored at {})", d.name, ts.installed, ts.dump)
            } else {
                format!(
                    "{}: {} (restored at {}, then updated to {})",
                    d.name, ts.installed, ts.dump, ts.target
                )
            };
            line(&mut out, "TimescaleDB", &text);
        }
    }
    if let Some(settings) = render_settings(&plan.info.settings) {
        line(&mut out, "Server settings", &settings);
    }
    line(
        &mut out,
        "Stopped meanwhile",
        &if plan.dependents.is_empty() {
            "nothing (no service that uses the database is running)".to_string()
        } else {
            format!("{} (started again at the end)", plan.dependents.join(", "))
        },
    );
    line(&mut out, "Dump tools", &plan.tools.describe());
    out.push_str(
        "\nThe services above stop before the dump. The old database is disconnected from its \
         Docker networks to block new external writers (local administrative/socket writers \
         must also be stopped). After the dump it is checked for writes and stopped before \
         restoring. Its network attachments are recorded first and restored after the stop \
         or during recovery. Host/shared namespaces and unsupported network attachments, \
         enabled subscriptions and unhandled background workers are refused unless --force \
         explicitly accepts concurrent-write risk; standbys are always refused. It stays down \
         until the switch and starts again if anything fails. Nothing is switched unless every \
         database's row counts and structure match.\n",
    );
    for w in &plan.warnings {
        out.push_str(&format!("\nWarning: {w}\n"));
    }
    out
}
