//! What the commands read from a PostgreSQL server, ask of a target image, and run the client
//! tools with.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Duration;

use serde_json::Value as Json;

use crate::dispatch::Runtime;

use super::LABEL;
use super::docker::{self, Docker, Done, Input, Output, PULL_NEVER, PgContainer, Res, StepError};
use super::fingerprint::DbLocale;
use super::signals;
use super::sqltext::{create_database_sql, quote_ident, quote_literal};
use super::stackcfg::is_ancestor;

/// The oldest PostgreSQL the commands move (`psql`'s `\warn`, which the snapshot export uses,
/// came in 13; 12 is out of support).
pub(crate) const MIN_MAJOR: u32 = 13;

/// The server-wide facts of a PostgreSQL server (one JSON object, on `template1`).
pub(crate) const CLUSTER_QUERY: &str = r#"
SELECT json_build_object(
  'version_num', current_setting('server_version_num')::int,
  'version', current_setting('server_version'),
  'superuser', (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user),
  'data_directory', current_setting('data_directory'),
  'port', current_setting('port'),
  'socket_dirs', current_setting('unix_socket_directories'),
  'databases', (
    SELECT json_agg(json_build_object(
      'name', d.datname, 'template', d.datistemplate, 'allow_connections', d.datallowconn,
      'size', pg_catalog.pg_database_size(d.oid),
      'locale', json_build_object(
        'encoding', pg_catalog.pg_encoding_to_char(d.encoding),
        'collate', d.datcollate, 'ctype', d.datctype,
        'provider', coalesce(to_jsonb(d) ->> 'datlocprovider', 'c'),
        'locale', coalesce(to_jsonb(d) ->> 'datlocale', to_jsonb(d) ->> 'daticulocale'),
        'icu_rules', to_jsonb(d) ->> 'daticurules')) ORDER BY d.datname)
    FROM pg_catalog.pg_database d
  ),
  'tablespaces', (
    SELECT coalesce(json_agg(spcname ORDER BY spcname), '[]') FROM pg_catalog.pg_tablespace
    WHERE spcname NOT IN ('pg_default', 'pg_global')
  ),
  'auto_conf', pg_catalog.pg_read_file('postgresql.auto.conf', 0, 10000000, true),
  'clients', (
    SELECT count(*) FROM pg_catalog.pg_stat_activity
    WHERE backend_type = 'client backend' AND pid <> pg_catalog.pg_backend_pid()
  ),
  'available', (
    SELECT coalesce(json_object_agg(name, default_version), '{}')
    FROM pg_catalog.pg_available_extensions
  )
);
"#;

/// The extensions of the current database.
pub(crate) const EXTENSIONS_QUERY: &str = r#"
SELECT coalesce(json_agg(json_build_object(
  'name', e.extname, 'version', e.extversion, 'schema', n.nspname,
  'default', a.default_version) ORDER BY e.extname), '[]')
FROM pg_catalog.pg_extension e
JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
LEFT JOIN pg_catalog.pg_available_extensions a ON a.name = e.extname;
"#;

/// An extension in a database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Extension {
    pub version: String,
    pub schema: String,
    /// The server's default version (`None` when the server does not ship it).
    pub default: Option<String>,
}

pub(crate) fn parse_extensions(value: &Json) -> BTreeMap<String, Extension> {
    value
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|e| {
                    Some((
                        e.get("name")?.as_str()?.to_string(),
                        Extension {
                            version: e.get("version")?.as_str()?.to_string(),
                            schema: e.get("schema")?.as_str()?.to_string(),
                            default: e.get("default").and_then(Json::as_str).map(str::to_string),
                        },
                    ))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// `17.11 (Ubuntu 17.11-1.pgdg22.04+2)` -> `17.11`.
pub(crate) fn short_version(version: &str) -> String {
    version
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_string()
}

/// A database of a server.
#[derive(Debug, Clone)]
pub(crate) struct Database {
    pub name: String,
    pub size: i64,
    /// Its encoding and locale (`None` when the server did not say).
    pub locale: Option<DbLocale>,
}

/// The facts of a running server.
#[derive(Debug, Clone)]
pub(crate) struct ServerInfo {
    pub version: String,
    pub major: u32,
    pub superuser: bool,
    pub data_directory: String,
    pub port: String,
    /// `unix_socket_directories`.
    pub socket_dirs: Vec<String>,
    /// Every database but `template0` and `template1`.
    pub databases: Vec<Database>,
    /// Databases that are templates or refuse connections (not moved).
    pub unmovable: Vec<String>,
    pub tablespaces: Vec<String>,
    /// `postgresql.auto.conf` (`ALTER SYSTEM`): `(name, value)`.
    pub settings: Vec<(String, String)>,
    pub clients: i64,
    /// `name -> default version`.
    pub available: BTreeMap<String, String>,
}

impl ServerInfo {
    pub fn parse(info: &Json) -> ServerInfo {
        let text = |k: &str| {
            info.get(k)
                .and_then(Json::as_str)
                .unwrap_or_default()
                .to_string()
        };
        let mut databases = Vec::new();
        let mut unmovable = Vec::new();
        for entry in info
            .get("databases")
            .and_then(Json::as_array)
            .into_iter()
            .flatten()
        {
            let name = entry
                .get("name")
                .and_then(Json::as_str)
                .unwrap_or_default()
                .to_string();
            if name == "template0" || name == "template1" {
                continue;
            }
            let template = entry
                .get("template")
                .and_then(Json::as_bool)
                .unwrap_or(false);
            let allow = entry
                .get("allow_connections")
                .and_then(Json::as_bool)
                .unwrap_or(true);
            if template || !allow {
                unmovable.push(name);
                continue;
            }
            databases.push(Database {
                size: entry.get("size").and_then(Json::as_i64).unwrap_or(0),
                locale: entry.get("locale").and_then(DbLocale::from_json),
                name,
            });
        }
        ServerInfo {
            version: short_version(&text("version")),
            major: u32::try_from(
                info.get("version_num").and_then(Json::as_i64).unwrap_or(0) / 10000,
            )
            .unwrap_or(0),
            superuser: info.get("superuser").and_then(Json::as_bool) == Some(true),
            data_directory: text("data_directory"),
            port: info
                .get("port")
                .and_then(Json::as_str)
                .unwrap_or("5432")
                .to_string(),
            socket_dirs: text("socket_dirs")
                .split(',')
                .map(str::trim)
                .filter(|d| !d.is_empty())
                .map(str::to_string)
                .collect(),
            databases,
            unmovable,
            tablespaces: str_list(info.get("tablespaces")),
            // The file itself: `pg_file_settings` reports a setting that waits for a restart
            // (e.g. a new `shared_preload_libraries`) as an error.
            settings: super::sqltext::parse_auto_conf(
                info.get("auto_conf")
                    .and_then(Json::as_str)
                    .unwrap_or_default(),
            ),
            clients: info.get("clients").and_then(Json::as_i64).unwrap_or(0),
            available: info
                .get("available")
                .and_then(Json::as_object)
                .map(|m| {
                    m.iter()
                        .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                        .collect()
                })
                .unwrap_or_default(),
        }
    }

    pub fn read(pg: &PgContainer) -> Res<ServerInfo> {
        Ok(ServerInfo::parse(&pg.json("template1", CLUSTER_QUERY)?))
    }

    /// What keeps the server from being dumped and moved, if anything.
    pub fn refusal(&self, docs: &str) -> Option<String> {
        if !self.superuser {
            return Some(
                "The database user of the stack's container is not a superuser, so it cannot \
                 dump the roles and every database."
                    .into(),
            );
        }
        if self.major < MIN_MAJOR {
            return Some(format!(
                "PostgreSQL {} is too old for this command ({MIN_MAJOR} or newer is needed). \
                 {docs}",
                self.version
            ));
        }
        if !self.tablespaces.is_empty() {
            return Some(format!(
                "The server has tablespaces outside its data directory ({}); they cannot be \
                 moved with the volume. {docs}",
                self.tablespaces.join(", ")
            ));
        }
        if !self.unmovable.is_empty() {
            return Some(format!(
                "The database{} {} {} a template or {} not allow connections, which this \
                 command does not move. {docs}",
                if self.unmovable.len() == 1 { "" } else { "s" },
                self.unmovable
                    .iter()
                    .map(|d| format!("'{d}'"))
                    .collect::<Vec<_>>()
                    .join(", "),
                if self.unmovable.len() == 1 {
                    "is"
                } else {
                    "are"
                },
                if self.unmovable.len() == 1 {
                    "does"
                } else {
                    "do"
                },
            ));
        }
        None
    }

    pub fn total_size(&self) -> i64 {
        self.databases.iter().map(|d| d.size).sum()
    }
}

pub(crate) fn env_value<'a>(env: &'a [(String, String)], name: &str) -> Option<&'a str> {
    env.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
}

pub(crate) fn str_list(value: Option<&Json>) -> Vec<String> {
    match value {
        Some(Json::Array(a)) => a
            .iter()
            .filter_map(Json::as_str)
            .map(str::to_string)
            .collect(),
        Some(Json::String(s)) => s.split_whitespace().map(str::to_string).collect(),
        _ => Vec::new(),
    }
}

/// The resolved environment of a Compose service.
pub(crate) fn service_env(service: &Json) -> Vec<(String, String)> {
    let mut env: Vec<(String, String)> = service
        .get("environment")
        .and_then(Json::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                .collect()
        })
        .unwrap_or_default();
    env.sort();
    env
}

/// `docker run` arguments for the `db` service's other mounts in a temporary server of the same
/// stack: bind mounts READ-ONLY (the setup scripts and configuration it reads; the old server
/// keeps them), a `tmpfs` as a `tmpfs`. Refused, with the reason: another named volume (it would
/// be shared by two servers), a bind mount inside a data directory (`data_dirs`) or over a
/// socket directory (`socket_dirs`: two servers' sockets and lock files in one place), and any
/// other kind of mount.
pub(crate) fn mount_args(
    service: &Json,
    data_target: &str,
    data_dirs: &[String],
    socket_dirs: &[String],
) -> Result<Vec<String>, String> {
    let mut args = Vec::new();
    let refuse = |what: String| {
        Err(format!(
            "{what}; this command cannot keep it apart between the old and the new server. \
             Follow the manual procedure in the docs (\"Upgrading PostgreSQL\")."
        ))
    };
    for mount in service
        .get("volumes")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
    {
        let kind = mount.get("type").and_then(Json::as_str).unwrap_or("");
        let source = mount.get("source").and_then(Json::as_str).unwrap_or("");
        let target = mount.get("target").and_then(Json::as_str).unwrap_or("");
        if target == data_target {
            continue;
        }
        match kind {
            "bind" => {}
            "tmpfs" => {
                args.extend(["--tmpfs".to_string(), target.to_string()]);
                continue;
            }
            "volume" => {
                return refuse(format!(
                    "The `db` service also mounts the volume '{source}' at {target}"
                ));
            }
            other => {
                return refuse(format!(
                    "The `db` service mounts {target} as a '{other}' mount"
                ));
            }
        }
        if let Some(dir) = data_dirs
            .iter()
            .filter(|d| !d.is_empty())
            .find(|d| is_ancestor(d, target))
        {
            return refuse(format!(
                "The `db` service mounts '{source}' at {target}, inside the data directory {dir}"
            ));
        }
        if let Some(dir) = socket_dirs
            .iter()
            .find(|d| is_ancestor(d, target) || is_ancestor(target, d))
        {
            return refuse(format!(
                "The `db` service mounts '{source}' at {target}, over the socket directory {dir}"
            ));
        }
        // `Z` would relabel the files for the temporary server alone (SELinux): `z` shares them.
        let selinux = mount
            .get("bind")
            .and_then(|b| b.get("selinux"))
            .and_then(Json::as_str)
            .map(|l| if l == "Z" { "z" } else { l })
            .unwrap_or("");
        if cfg!(windows) || source.contains(':') || target.contains(':') {
            args.extend([
                "--mount".to_string(),
                format!("type=bind,source={source},target={target},readonly"),
            ]);
        } else {
            let mut options = vec!["ro"];
            if !selinux.is_empty() {
                options.push(selinux);
            }
            args.extend([
                "-v".to_string(),
                format!("{source}:{target}:{}", options.join(",")),
            ]);
        }
    }
    Ok(args)
}

/// Free bytes of the filesystem holding `path` inside a container (`df -Pk`).
pub(crate) fn container_free_bytes(container: &str, path: &str) -> Option<i64> {
    let done = Docker::new(&["exec", container, "df", "-Pk", path])
        .run()
        .ok()?;
    let text = done.stdout_text();
    let line = text.lines().nth(1)?;
    line.split_whitespace()
        .nth(3)?
        .parse::<i64>()
        .ok()
        .map(|kb| kb * 1024)
}

/// Free bytes for files under `dir` (the nearest existing parent).
pub(crate) fn host_free_bytes(dir: &Path) -> Option<i64> {
    let mut probe = Some(dir);
    while let Some(p) = probe {
        if p.exists() {
            return fs4::available_space(p).ok().map(|b| b as i64);
        }
        probe = p.parent();
    }
    None
}

/// The Meerschaum jobs running in this root.
pub(crate) fn running_jobs(rt: &mut Runtime<'_>) -> Vec<String> {
    let ctx = crate::native::jobs::jobs_context(rt);
    let Ok(listing) = meerschaum_jobs::executor::get_jobs_for(&ctx, None, true, true) else {
        return Vec::new();
    };
    listing
        .jobs
        .into_iter()
        .filter_map(|(name, mut job)| {
            (job.status() == meerschaum_jobs::Status::Running).then_some(name)
        })
        .collect()
}

/// What a target image can do, asked of a throwaway container of it.
#[derive(Debug, Clone)]
pub(crate) struct Target {
    pub version: String,
    pub major: u32,
    pub available: BTreeMap<String, String>,
    /// The TimescaleDB versions `CREATE EXTENSION timescaledb VERSION '...'` succeeded with.
    pub timescaledb_usable: BTreeSet<String>,
    /// The databases whose encoding and locale the image's server cannot create.
    pub locale_problems: Vec<String>,
    /// Free bytes in Docker's storage, seen from inside the container.
    pub free_bytes: Option<i64>,
}

/// Start a throwaway server of `image` (no network, no volume of ours), ask it which
/// extensions and TimescaleDB versions it can create, list each of `dumps` with its
/// `pg_restore --list` (which proves the new tools read them), and remove it.
pub(crate) fn probe_target(
    image: &str,
    superuser: &str,
    name: &str,
    run_id: &str,
    timescaledb_versions: &BTreeSet<String>,
    locales: &[(String, DbLocale)],
    dumps: &[&Path],
) -> Res<Target> {
    signals::shielded(|| docker::remove_run_container(name, LABEL, run_id))?;
    let password = uuid::Uuid::new_v4().simple().to_string();
    let env = vec![
        ("POSTGRES_USER".to_string(), superuser.to_string()),
        ("POSTGRES_PASSWORD".to_string(), password),
    ];
    let label = format!("{LABEL}={run_id}");
    let mut args = vec!["run", "-d"];
    args.extend(PULL_NEVER);
    args.extend([
        "--name",
        name,
        "--label",
        &label,
        "--network",
        "none",
        "-e",
        "POSTGRES_USER",
        "-e",
        "POSTGRES_PASSWORD",
        image,
    ]);
    let started = Docker::new(&args)
        .env(&env)
        .check(&format!("Starting a throwaway container of '{image}'"));
    let result = started.and_then(|_| {
        let pg = PgContainer::new(name);
        docker::wait_ready(&pg, Duration::from_secs(300))?;
        let info = ServerInfo::read(&pg)?;
        let mut usable = BTreeSet::new();
        if info.available.contains_key("timescaledb") {
            for version in timescaledb_versions {
                pg.psql(
                    "template1",
                    "DROP DATABASE IF EXISTS mrsm_upgrade_probe;\n\
                     CREATE DATABASE mrsm_upgrade_probe TEMPLATE template0;\n",
                )?;
                // The first statement of a new session, as for a restore.
                if pg
                    .psql(
                        "mrsm_upgrade_probe",
                        &format!(
                            "CREATE EXTENSION timescaledb VERSION {};",
                            quote_literal(version)
                        ),
                    )
                    .is_ok()
                {
                    usable.insert(version.clone());
                }
            }
        }
        // Every database's encoding and locale must be one the image's server can create.
        let mut locale_problems = Vec::new();
        let mut tried: Vec<(DbLocale, Option<String>)> = Vec::new();
        for (database, locale) in locales {
            let failure = match tried.iter().find(|(l, _)| l == locale) {
                Some((_, failure)) => failure.clone(),
                None => {
                    let probe = format!("mrsm_upgrade_locale_probe_{}", tried.len());
                    let sql = format!(
                        "{}\nDROP DATABASE {};\n",
                        create_database_sql(&probe, locale, info.major),
                        quote_ident(&probe)
                    );
                    let failure = pg
                        .psql("template1", &sql)
                        .err()
                        .map(|e| e.message.lines().last().unwrap_or_default().to_string());
                    tried.push((locale.clone(), failure.clone()));
                    failure
                }
            };
            if let Some(error) = failure {
                locale_problems.push(format!(
                    "The database '{database}' uses the locale {}, which '{image}' cannot create \
                     ({}).",
                    locale.describe(),
                    error.trim()
                ));
            }
        }
        for dump in dumps {
            let done = pg.tool(
                "pg_restore",
                &["--list".into()],
                Input::File(dump),
                Output::Capture,
            )?;
            if !done.ok() {
                return Err(StepError::new(format!(
                    "'{image}' cannot read the dump '{}':\n{}",
                    dump.display(),
                    done.stderr_tail(10)
                )));
            }
        }
        Ok(Target {
            version: info.version,
            major: info.major,
            available: info.available,
            timescaledb_usable: usable,
            locale_problems,
            free_bytes: container_free_bytes(name, "/"),
        })
    });
    // Whatever happened (a stop request included), the throwaway container goes.
    let removed = signals::shielded(|| docker::remove_run_container(name, LABEL, run_id));
    let target = result?;
    removed?;
    Ok(target)
}

/// The PostgreSQL client tools a command dumps and lists with.
pub(crate) enum Tools {
    /// The server's own tools, in its container (`docker exec`).
    Container(PgContainer),
    /// Another image's tools (the newer ones, for an upgrade) in throwaway containers that share
    /// the server container's network namespace, so `127.0.0.1` is the server.
    Image {
        image: String,
        server: String,
        /// `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` (passed by name).
        env: Vec<(String, String)>,
        run_id: String,
        name_prefix: String,
        count: Cell<u32>,
    },
}

impl Tools {
    /// Run a client tool (`psql` reads its SQL from `input`).
    pub fn run(
        &self,
        program: &str,
        args: &[String],
        input: Input<'_>,
        output: Output<'_>,
    ) -> Res<Done> {
        match self {
            Tools::Container(pg) => pg.tool(program, args, input, output),
            Tools::Image {
                image,
                server,
                env,
                run_id,
                name_prefix,
                count,
            } => {
                count.set(count.get() + 1);
                let name = format!("{name_prefix}-{}", count.get());
                let mut full = vec![
                    "run".to_string(),
                    "--rm".to_string(),
                    PULL_NEVER[0].to_string(),
                    PULL_NEVER[1].to_string(),
                    "-i".to_string(),
                    "--name".to_string(),
                    name.clone(),
                    "--label".to_string(),
                    format!("{LABEL}={run_id}"),
                    "--network".to_string(),
                    format!("container:{server}"),
                    "--entrypoint".to_string(),
                    program.to_string(),
                ];
                for (k, _) in env {
                    full.extend(["-e".to_string(), k.clone()]);
                }
                full.push(image.clone());
                full.extend(args.iter().cloned());
                let done = Docker::new(&full).env(env).run_with(input, output);
                // `--rm` removes it once it exits; an interrupted run may leave it behind (only
                // a container of this run is removed).
                let _ = signals::shielded(|| docker::remove_run_container(&name, LABEL, run_id));
                done
            }
        }
    }

    /// Whether these tools reach the server (`SELECT 1`).
    pub fn reach(&self) -> bool {
        self.run(
            "psql",
            &[
                "-X".into(),
                "-q".into(),
                "-A".into(),
                "-t".into(),
                "-d".into(),
                docker::conninfo("template1"),
            ],
            Input::Bytes(b"SELECT 1;\n"),
            Output::Capture,
        )
        .is_ok_and(|d| d.ok() && d.stdout_text().trim() == "1")
    }

    /// `zstd` when these tools' `pg_dump` can compress with it (PostgreSQL 16+ built with zstd),
    /// else `gzip` (the custom format's default).
    pub fn compression(&self) -> &'static str {
        let zstd = self
            .run(
                "pg_dump",
                &[
                    "-Fc".into(),
                    "--compress=zstd".into(),
                    "--schema-only".into(),
                    "-d".into(),
                    docker::conninfo("template1"),
                ],
                Input::Null,
                Output::Capture,
            )
            .is_ok_and(|d| d.ok());
        if zstd { "zstd" } else { "gzip" }
    }

    pub fn describe(&self) -> String {
        match self {
            Tools::Container(_) => "the server's own pg_dump".into(),
            Tools::Image { image, .. } => format!("{image}'s pg_dump (as PostgreSQL recommends)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn server_facts_parse_and_explain_refusals() {
        let info = ServerInfo::parse(&json!({
            "version_num": 170011, "version": "17.11 (Ubuntu 17.11-1.pgdg22.04+2)",
            "superuser": true, "data_directory": "/home/postgres/pgdata/data", "port": "5432",
            "databases": [
                {"name": "meerschaum", "template": false, "allow_connections": true, "size": 100},
                {"name": "template0", "template": true, "allow_connections": false, "size": 1},
                {"name": "template1", "template": true, "allow_connections": true, "size": 1},
                {"name": "postgres", "template": false, "allow_connections": true, "size": 10}
            ],
            "tablespaces": [],
            "socket_dirs": "/var/run/postgresql, /tmp",
            "auto_conf": "# Do not edit this file manually!\ntimescaledb.telemetry_level = 'off'\n",
            "clients": 2, "available": {"timescaledb": "2.30.2"}
        }));
        assert_eq!(info.version, "17.11");
        assert_eq!(info.major, 17);
        assert_eq!(info.socket_dirs, vec!["/var/run/postgresql", "/tmp"]);
        assert_eq!(info.total_size(), 110);
        assert_eq!(
            info.settings,
            vec![("timescaledb.telemetry_level".into(), "off".into())]
        );
        assert!(info.refusal("docs").is_none());
        let mut old = info.clone();
        old.major = 12;
        assert!(old.refusal("docs").unwrap().contains("too old"));
        let mut templated = info;
        templated.unmovable = vec!["tmpl".into()];
        assert!(
            templated
                .refusal("docs")
                .unwrap()
                .contains("'tmpl' is a template")
        );
    }

    #[test]
    fn other_mounts_are_read_only_and_shared_places_refused() {
        let db = json!({"volumes": [
            {"type": "volume", "source": "meerschaum_db_data", "target": "/home/postgres/pgdata"},
            {"type": "bind", "source": "/root/db/initdb/create-extensions.sql",
             "target": "/docker-entrypoint-initdb.d/100_meerschaum.sql", "read_only": true,
             "bind": {"selinux": "z"}},
            {"type": "bind", "source": "/root/conf", "target": "/etc/pg", "bind": {"selinux": "Z"}},
            {"type": "tmpfs", "target": "/run/scratch"}
        ]});
        let data = vec!["/home/postgres/pgdata/data".to_string()];
        let sockets = vec!["/var/run/postgresql".to_string()];
        let args = mount_args(&db, "/home/postgres/pgdata", &data, &sockets).unwrap();
        if cfg!(windows) {
            assert_eq!(args.len(), 6);
        } else {
            assert_eq!(
                args,
                vec![
                    "-v",
                    "/root/db/initdb/create-extensions.sql:/docker-entrypoint-initdb.d/100_meerschaum.sql:ro,z",
                    "-v",
                    "/root/conf:/etc/pg:ro,z",
                    "--tmpfs",
                    "/run/scratch",
                ]
            );
        }
        let with = |mount: Json| {
            let mut db = db.clone();
            db["volumes"].as_array_mut().unwrap().push(mount);
            mount_args(&db, "/home/postgres/pgdata", &data, &sockets).unwrap_err()
        };
        assert!(
            with(json!({"type": "volume", "source": "wal_archive", "target": "/archive"}))
                .contains("wal_archive")
        );
        assert!(
            with(json!({"type": "bind", "source": "/srv/wal",
                            "target": "/home/postgres/pgdata/data/pg_wal"}))
            .contains("inside the data directory")
        );
        assert!(
            with(json!({"type": "bind", "source": "/srv/run", "target": "/var/run"}))
                .contains("socket directory")
        );
        assert!(with(json!({"type": "npipe", "source": "x", "target": "/y"})).contains("npipe"));
    }

    #[test]
    fn extensions_parse_with_their_defaults() {
        let parsed = parse_extensions(&json!([
            {"name": "timescaledb", "version": "2.29.2", "schema": "public", "default": "2.30.2"},
            {"name": "plpgsql", "version": "1.0", "schema": "pg_catalog", "default": null}
        ]));
        assert_eq!(
            parsed["timescaledb"],
            Extension {
                version: "2.29.2".into(),
                schema: "public".into(),
                default: Some("2.30.2".into()),
            }
        );
        assert_eq!(parsed["plpgsql"].default, None);
    }

    #[test]
    fn lists_and_environments_come_from_compose() {
        assert_eq!(
            str_list(Some(&json!(["postgres", "-c", "x=1"]))),
            vec!["postgres", "-c", "x=1"]
        );
        assert_eq!(
            str_list(Some(&json!("postgres -c x=1"))),
            vec!["postgres", "-c", "x=1"]
        );
        assert!(str_list(None).is_empty());
        let env = service_env(&json!({"environment": {"B": "2", "A": "1", "C": null}}));
        assert_eq!(
            env,
            vec![("A".into(), "1".into()), ("B".into(), "2".into())]
        );
        assert_eq!(env_value(&env, "B"), Some("2"));
    }
}
