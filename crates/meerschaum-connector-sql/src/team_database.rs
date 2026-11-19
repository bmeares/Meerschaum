//! A team's own database (or schema) on an existing PostgreSQL-family server (5.1).
//!
//! [`create`] makes, with the administrative connector `base` (the one the team branches from,
//! `sql:main` by default):
//!
//! - a login role for the team (`mrsm_<slug>`) with a generated password: the owner of the
//!   team's database (or schema), without superuser, `CREATEDB` or `CREATEROLE`;
//! - a read-only login role (`mrsm_<slug>_ro`) for dashboards (Grafana), which may only `SELECT`
//!   what the team's role creates;
//! - **a database** (`CREATE DATABASE ... OWNER`), which nobody else may connect to, or **a
//!   schema** in the base database (`CREATE SCHEMA ... AUTHORIZATION`), which becomes the roles'
//!   `search_path` (the team's role may also create schemas of its own there).
//!
//! It also takes `CONNECT` on the base database away from `PUBLIC` (every login role that could
//! connect before keeps an explicit grant), so a team's role cannot open the databases of the
//! others. The database server's roles are the isolation boundary: Meerschaum's own access rules
//! decide what the console, the HTTP API and MCP show.
//!
//! The result ([`TeamDatabase`]) carries the attributes of the team's new connector; the caller
//! writes them to the configuration (`meerschaum:connectors:sql:<label>`).

use meerschaum_core::{Map, Value};

use crate::flavor::Flavor;
use crate::instance::SqlConnector;

/// What to create for the team.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeamDatabaseKind {
    /// A database of its own.
    Database,
    /// A schema in the base connector's database.
    Schema,
}

impl TeamDatabaseKind {
    /// `database` or `schema`.
    pub fn name(&self) -> &'static str {
        match self {
            TeamDatabaseKind::Database => "database",
            TeamDatabaseKind::Schema => "schema",
        }
    }

    /// Parse `database` or `schema` (any case).
    pub fn parse(text: &str) -> Option<TeamDatabaseKind> {
        match text.trim().to_lowercase().as_str() {
            "database" | "db" => Some(TeamDatabaseKind::Database),
            "schema" => Some(TeamDatabaseKind::Schema),
            _ => None,
        }
    }
}

/// What [`create`] made.
#[derive(Debug, Clone, PartialEq)]
pub struct TeamDatabase {
    /// A database or a schema.
    pub kind: TeamDatabaseKind,
    /// The database the team's connector opens.
    pub database: String,
    /// The team's schema (`Schema` kind only).
    pub schema: Option<String>,
    /// The team's role and its password.
    pub role: String,
    pub password: String,
    /// The read-only role and its password.
    pub readonly_role: String,
    pub readonly_password: String,
    /// The server's host and port, as the base connector reaches it.
    pub host: Option<String>,
    pub port: Option<u16>,
    /// The base connector's flavor.
    pub flavor: Flavor,
    /// What could not be done and does not prevent using the database (a missing extension).
    pub notes: Vec<String>,
}

impl TeamDatabase {
    /// The attributes of the team's connector (`meerschaum:connectors:sql:<label>`), reserved
    /// for `team`.
    pub fn connector_attributes(&self, team: &str) -> Map {
        let mut attrs = Map::new();
        attrs.insert("flavor".into(), Value::Str(self.flavor.as_str().into()));
        if let Some(host) = &self.host {
            attrs.insert("host".into(), Value::Str(host.clone()));
        }
        if let Some(port) = self.port {
            attrs.insert("port".into(), Value::Int(i64::from(port)));
        }
        attrs.insert("username".into(), Value::Str(self.role.clone()));
        attrs.insert("password".into(), Value::Str(self.password.clone()));
        attrs.insert("database".into(), Value::Str(self.database.clone()));
        if let Some(schema) = &self.schema {
            attrs.insert("schema".into(), Value::Str(schema.clone()));
        }
        attrs.insert(
            "teams".into(),
            Value::List(vec![Value::Str(team.to_string())]),
        );
        attrs
    }

    /// What the team's record remembers (`attributes['database']`): no credentials.
    pub fn summary(&self, connector_keys: &str, base_keys: &str) -> Map {
        let mut out = Map::new();
        out.insert("kind".into(), Value::Str(self.kind.name().into()));
        out.insert("connector".into(), Value::Str(connector_keys.into()));
        out.insert("base".into(), Value::Str(base_keys.into()));
        out.insert("database".into(), Value::Str(self.database.clone()));
        if let Some(schema) = &self.schema {
            out.insert("schema".into(), Value::Str(schema.clone()));
        }
        out.insert("role".into(), Value::Str(self.role.clone()));
        out.insert(
            "readonly_role".into(),
            Value::Str(self.readonly_role.clone()),
        );
        out
    }
}

/// Whether a team database can be made on `flavor` (the PostgreSQL family).
pub fn supported(flavor: Flavor) -> bool {
    matches!(
        flavor,
        Flavor::Postgresql
            | Flavor::Timescaledb
            | Flavor::TimescaledbHa
            | Flavor::Postgis
            | Flavor::Citus
    )
}

/// The longest slug: `mrsm_` + slug + `_ro` stays under PostgreSQL's 63-byte identifiers.
const MAX_SLUG: usize = 48;

/// A team (or database) name as a plain SQL identifier: lowercase letters, digits and `_`,
/// starting with a letter (`River Watch-North` → `river_watch_north`). `None` when nothing
/// usable is left.
pub fn slug(name: &str) -> Option<String> {
    let mut out = String::new();
    for c in name.trim().chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else if !out.ends_with('_') && !out.is_empty() {
            out.push('_');
        }
    }
    let out = out.trim_matches('_');
    let out = match out.chars().next() {
        Some(c) if c.is_ascii_digit() => format!("t_{out}"),
        Some(_) => out.to_string(),
        None => return None,
    };
    Some(out.chars().take(MAX_SLUG).collect::<String>())
        .map(|s| s.trim_end_matches('_').to_string())
        .filter(|s| !s.is_empty())
}

/// Whether `name` may be used as is for a database, schema or role: [`slug`] leaves it alone and
/// it is not a reserved name.
pub fn valid_identifier(name: &str) -> Result<(), String> {
    if slug(name).as_deref() != Some(name) {
        return Err(format!(
            "'{name}' is not a valid name here: use lowercase letters, digits and '_' (at most \
             {MAX_SLUG} characters, starting with a letter)."
        ));
    }
    const RESERVED: &[&str] = &[
        "postgres",
        "template0",
        "template1",
        "public",
        "information_schema",
        "pg_catalog",
        "pg_toast",
        "mrsm",
        "meerschaum",
    ];
    if RESERVED.contains(&name) || name.starts_with("pg_") || name.starts_with("_timescaledb") {
        return Err(format!("'{name}' is reserved: choose another name."));
    }
    Ok(())
}

/// The roles of a team whose slug is `slug`: `(role, read-only role)`.
pub fn role_names(slug: &str) -> (String, String) {
    (format!("mrsm_{slug}"), format!("mrsm_{slug}_ro"))
}

/// A generated password: 64 hexadecimal characters (two random UUIDs), safe inside a SQL
/// literal, a URI and YAML.
pub fn generate_password() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

fn quoted(identifier: &str) -> String {
    // Identifiers are validated ([`valid_identifier`]) or come from the server; doubling the
    // quotes keeps a database name with one intact.
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// Keep `CONNECT` on the current database for the login roles that have it through `PUBLIC`,
/// then take it away from `PUBLIC`: new roles (the teams') cannot connect to it.
const WALL_OFF_DATABASE: &str = r#"DO $mrsm$
DECLARE r record;
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_database d, aclexplode(coalesce(d.datacl, acldefault('d', d.datdba))) a
        WHERE d.datname = current_database() AND a.grantee = 0 AND a.privilege_type = 'CONNECT'
    ) THEN
        FOR r IN
            SELECT rolname FROM pg_roles
            WHERE rolcanlogin AND NOT rolsuper AND rolname !~ '^pg_'
        LOOP
            IF r.rolname = 'mrsm__main_ro' THEN
                -- The stack's main read-only role keeps CONNECT only (no temporary tables).
                EXECUTE format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), r.rolname);
            ELSE
                EXECUTE format('GRANT CONNECT, TEMPORARY ON DATABASE %I TO %I', current_database(), r.rolname);
            END IF;
        END LOOP;
        EXECUTE format('REVOKE CONNECT, TEMPORARY ON DATABASE %I FROM PUBLIC', current_database());
    END IF;
END
$mrsm$"#;

async fn exists(conn: &SqlConnector, sql: &str) -> Result<bool, String> {
    conn.driver()
        .value(sql)
        .await
        .map(|v| v.is_some())
        .map_err(|e| e.to_string())
}

async fn run(conn: &SqlConnector, sql: &str) -> Result<(), String> {
    conn.driver()
        .exec(sql)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Create the database (or schema) and roles of `team` on `base`'s server. `name` is the
/// database's or schema's name (default: the team's [`slug`]).
///
/// Nothing is reused: an existing database, schema or role of that name is an error, and what
/// was created before a failure is dropped again.
pub async fn create(
    base: &SqlConnector,
    team: &str,
    kind: TeamDatabaseKind,
    name: Option<&str>,
) -> Result<TeamDatabase, String> {
    let flavor = base.flavor();
    if !supported(flavor) {
        return Err(format!(
            "A team database needs a PostgreSQL-family connector (not '{}').",
            flavor.as_str()
        ));
    }
    let Some(team_slug) = slug(team) else {
        return Err(format!("The team name '{team}' has no letters or digits."));
    };
    let name = match name.map(str::trim).filter(|n| !n.is_empty()) {
        Some(name) => name.to_string(),
        None => team_slug.clone(),
    };
    valid_identifier(&name)?;
    let (role, readonly_role) = role_names(&team_slug);
    let spec = base.spec().clone();
    let Some(base_database) = spec.database.clone().filter(|d| !d.is_empty()) else {
        return Err("The base connector has no database.".to_string());
    };

    for existing in [&role, &readonly_role] {
        if exists(
            base,
            &format!(
                "SELECT 1 FROM pg_roles WHERE rolname = {}",
                literal(existing)
            ),
        )
        .await?
        {
            return Err(format!(
                "The role '{existing}' already exists on this server: the team '{team}' (or one \
                 with a similar name) already has a database."
            ));
        }
    }
    let taken = match kind {
        TeamDatabaseKind::Database => {
            exists(
                base,
                &format!(
                    "SELECT 1 FROM pg_database WHERE datname = {}",
                    literal(&name)
                ),
            )
            .await?
        }
        TeamDatabaseKind::Schema => {
            exists(
                base,
                &format!(
                    "SELECT 1 FROM information_schema.schemata WHERE schema_name = {}",
                    literal(&name)
                ),
            )
            .await?
        }
    };
    if taken {
        return Err(format!("The {} '{name}' already exists.", kind.name()));
    }

    let mut made = TeamDatabase {
        kind,
        database: match kind {
            TeamDatabaseKind::Database => name.clone(),
            TeamDatabaseKind::Schema => base_database.clone(),
        },
        schema: (kind == TeamDatabaseKind::Schema).then(|| name.clone()),
        role: role.clone(),
        password: generate_password(),
        readonly_role: readonly_role.clone(),
        readonly_password: generate_password(),
        host: spec.host.clone(),
        port: spec.port,
        flavor,
        notes: Vec::new(),
    };

    // Before the team's roles exist: the roles that connect today keep an explicit grant.
    if let Err(e) = run(base, WALL_OFF_DATABASE).await {
        made.notes.push(format!(
            "CONNECT on the database '{base_database}' could not be taken from PUBLIC ({e}): \
             the team's role may open it (it cannot read the tables of others)."
        ));
    }

    let mut undo: Vec<String> = Vec::new();
    let result = build(base, &made, &name, &base_database, &mut undo).await;
    match result {
        Ok(notes) => {
            made.notes.extend(notes);
            Ok(made)
        }
        Err(e) => {
            for statement in undo.iter().rev() {
                let _ = run(base, statement).await;
            }
            Err(e)
        }
    }
}

async fn build(
    base: &SqlConnector,
    made: &TeamDatabase,
    name: &str,
    base_database: &str,
    undo: &mut Vec<String>,
) -> Result<Vec<String>, String> {
    let mut notes = Vec::new();
    let role = quoted(&made.role);
    let readonly = quoted(&made.readonly_role);
    run(
        base,
        &format!(
            "CREATE ROLE {role} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD {}",
            literal(&made.password)
        ),
    )
    .await?;
    undo.push(format!("DROP ROLE IF EXISTS {role}"));
    run(
        base,
        &format!(
            "CREATE ROLE {readonly} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD {}",
            literal(&made.readonly_password)
        ),
    )
    .await?;
    undo.push(format!("DROP ROLE IF EXISTS {readonly}"));
    // Marks both as a team's: the stack's main read-only role is never granted what they own.
    for (r, what) in [(&role, "role"), (&readonly, "read-only role")] {
        run(
            base,
            &format!(
                "COMMENT ON ROLE {r} IS {}",
                literal(&format!("{TEAM_ROLE_COMMENT}{}: {what}", made.database))
            ),
        )
        .await?;
    }

    match made.kind {
        TeamDatabaseKind::Database => {
            let database = quoted(name);
            // The administrator must be a member of the owner to create a database for it
            // (managed servers have no superuser).
            let _ = run(base, &format!("GRANT {role} TO CURRENT_USER")).await;
            run(base, &format!("CREATE DATABASE {database} OWNER {role}")).await?;
            undo.push(format!("DROP DATABASE IF EXISTS {database}"));
            // Nothing reaches it through PUBLIC, nor the stack's main read-only role.
            shut_out_main_readonly(base, made, name).await?;
            run(
                base,
                &format!("GRANT CONNECT ON DATABASE {database} TO {readonly}"),
            )
            .await?;

            // Inside the new database, as the administrator: extensions and the public schema.
            let mut spec = base.spec().clone();
            spec.database = Some(name.to_string());
            spec.schema = None;
            spec.uri = None;
            let inside = SqlConnector::connect(&format!("{name}_setup"), spec)
                .await
                .map_err(|e| format!("Could not connect to the new database '{name}': {e}"))?;
            let mut extensions = Vec::new();
            if matches!(made.flavor, Flavor::Timescaledb | Flavor::TimescaledbHa)
                || installed(base, "timescaledb").await
            {
                extensions.push("timescaledb");
            }
            if made.flavor == Flavor::Postgis || installed(base, "postgis").await {
                extensions.push("postgis");
            }
            for extension in extensions {
                if let Err(e) = run(
                    &inside,
                    &format!("CREATE EXTENSION IF NOT EXISTS {extension}"),
                )
                .await
                {
                    notes.push(format!(
                        "The extension '{extension}' was not installed in '{name}': {e}"
                    ));
                }
            }
            for statement in [
                format!("ALTER SCHEMA public OWNER TO {role}"),
                format!("GRANT USAGE ON SCHEMA public TO {readonly}"),
                format!(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE {role} GRANT SELECT ON TABLES TO {readonly}"
                ),
                format!(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE {role} GRANT USAGE ON SCHEMAS TO {readonly}"
                ),
            ] {
                run(&inside, &statement).await?;
            }
        }
        TeamDatabaseKind::Schema => {
            let schema = quoted(name);
            let database = quoted(base_database);
            run(
                base,
                &format!("GRANT CONNECT ON DATABASE {database} TO {role}, {readonly}"),
            )
            .await?;
            let _ = run(base, &format!("GRANT {role} TO CURRENT_USER")).await;
            run(
                base,
                &format!("CREATE SCHEMA {schema} AUTHORIZATION {role}"),
            )
            .await?;
            undo.push(format!("DROP SCHEMA IF EXISTS {schema} CASCADE"));
            shut_out_main_readonly(base, made, name).await?;
            for statement in [
                // Meerschaum runs `CREATE SCHEMA IF NOT EXISTS` before creating a table, which
                // PostgreSQL refuses without `CREATE` on the database even when the schema
                // exists. The role still reads nothing it does not own.
                format!("GRANT CREATE, TEMPORARY ON DATABASE {database} TO {role}"),
                format!("ALTER ROLE {role} IN DATABASE {database} SET search_path = {schema}"),
                format!("ALTER ROLE {readonly} IN DATABASE {database} SET search_path = {schema}"),
                format!("GRANT USAGE ON SCHEMA {schema} TO {readonly}"),
                format!(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE {role} IN SCHEMA {schema} \
                     GRANT SELECT ON TABLES TO {readonly}"
                ),
            ] {
                run(base, &statement).await?;
            }
        }
    }
    Ok(notes)
}

// ---------------------------------------------------------------------------------------------
// The stack's main read-only role (5.1)
// ---------------------------------------------------------------------------------------------

/// The read-only login role the stack's provisioned Grafana data source ("Meerschaum Main")
/// connects as. A team's roles are `mrsm_<slug>` and `mrsm_<slug>_ro`, and a slug never holds
/// `__` nor starts with `_`, so no team can ever be given this name.
pub const MAIN_READONLY_ROLE: &str = meerschaum_core::config::grafana_readonly::MAIN_READONLY_ROLE;

/// How the comment on a team's roles starts (`COMMENT ON ROLE`): the main read-only role is never
/// granted a schema such a role owns.
pub const TEAM_ROLE_COMMENT: &str = "Meerschaum team ";

/// Whether the role whose oid is the SQL expression `owner` is a team's: it is not the current
/// user, and it is named like one (`mrsm_...`, also the roles made before they were commented)
/// or carries [`TEAM_ROLE_COMMENT`].
fn team_owner_sql(owner: &str) -> String {
    format!(
        "({owner} <> (SELECT oid FROM pg_roles WHERE rolname = current_user) AND EXISTS (\
         SELECT 1 FROM pg_roles tr WHERE tr.oid = {owner} AND tr.rolname <> {main} AND \
         (tr.rolname LIKE 'mrsm\\_%' OR coalesce(shobj_description(tr.oid, 'pg_authid'), '') \
         LIKE {comment})))",
        main = literal(MAIN_READONLY_ROLE),
        comment = literal(&format!("{TEAM_ROLE_COMMENT}%")),
    )
}

/// Schemas that belong to PostgreSQL or an extension (left alone: neither granted nor revoked).
const SYSTEM_SCHEMAS_SQL: &str = "(n.nspname LIKE 'pg\\_%' OR n.nspname = 'information_schema' \
     OR n.nspname LIKE '\\_timescaledb%' OR n.nspname LIKE 'timescaledb\\_%' \
     OR n.nspname LIKE 'toolkit\\_%')";

/// The statement that creates (or corrects) [`MAIN_READONLY_ROLE`] in the current database, run
/// by the instance's owner (`sql:main`'s user). Idempotent; safe on every start.
///
/// - A login role without superuser, `CREATEDB`, `CREATEROLE`, replication, `BYPASSRLS` or any
///   role membership (removed if found), with `default_transaction_read_only`.
/// - `CONNECT` on the current database and nothing else on it (no `CREATE`, no explicit
///   `TEMPORARY`); explicit `CONNECT` on any other database is revoked.
/// - Readable: every schema that is not PostgreSQL's or an extension's and whose owner is the
///   current user or `pg_database_owner`, plus `extra_schemas` (`stack:grafana:readonly_schemas`),
///   but never a schema or table owned by a team's role. `USAGE` on the schema, `SELECT` on its
///   tables, views and hypertables, and default privileges for the tables the current user
///   creates there later. Everything else is revoked from the role.
/// - `CREATE` granted to `PUBLIC` on a readable schema (a database made before PostgreSQL 15) is
///   taken from `PUBLIC`; every other login role that had it keeps an explicit grant.
/// - Finally the role is checked: it fails (`RAISE EXCEPTION`) if it is privileged, a member of a
///   role, may create in any schema or database, or may read a team's schema.
///
/// The password is set separately ([`main_readonly_password_sql`]) so a psql variable can carry
/// it.
pub fn main_readonly_sql(extra_schemas: &[String]) -> String {
    let extra = if extra_schemas.is_empty() {
        "ARRAY[]::text[]".to_string()
    } else {
        format!(
            "ARRAY[{}]::text[]",
            extra_schemas
                .iter()
                .map(|s| literal(s))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let team_schema = team_owner_sql("n.nspowner");
    let team_table = team_owner_sql("c.relowner");
    let team_schema_any = team_owner_sql("n2.nspowner");
    format!(
        r#"DO $mrsm$
DECLARE
    ro text := {role};
    ro_oid oid;
    extra text[] := {extra};
    r record;
    t record;
    readable boolean;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = ro) THEN
        EXECUTE format('CREATE ROLE %I LOGIN', ro);
    END IF;
    EXECUTE format('ALTER ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT', ro);
    EXECUTE format('ALTER ROLE %I SET default_transaction_read_only = on', ro);
    EXECUTE format('COMMENT ON ROLE %I IS %L', ro, 'Meerschaum: the read-only role of the stack''s main Grafana data source');
    SELECT oid INTO ro_oid FROM pg_roles WHERE rolname = ro;
    FOR r IN SELECT g.rolname FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.roleid WHERE m.member = ro_oid LOOP
        EXECUTE format('REVOKE %I FROM %I', r.rolname, ro);
    END LOOP;

    -- Databases: CONNECT on this one only.
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), ro);
    EXECUTE format('REVOKE CREATE, TEMPORARY ON DATABASE %I FROM %I', current_database(), ro);
    FOR r IN
        SELECT d.datname FROM pg_database d, aclexplode(d.datacl) a
        WHERE d.datname <> current_database() AND a.grantee = ro_oid
    LOOP
        EXECUTE format('REVOKE ALL ON DATABASE %I FROM %I', r.datname, ro);
    END LOOP;

    -- Schemas.
    FOR r IN
        SELECT n.oid, n.nspname,
            {team_schema} AS team,
            (n.nspowner = (SELECT oid FROM pg_roles WHERE rolname = current_user)
             OR n.nspowner = (SELECT oid FROM pg_roles WHERE rolname = 'pg_database_owner')
             OR n.nspname = ANY(extra)) AS wanted
        FROM pg_namespace n
        WHERE NOT {system}
    LOOP
        readable := r.wanted AND NOT r.team;
        IF r.team AND r.nspname = ANY(extra) THEN
            RAISE WARNING 'Meerschaum: the schema % belongs to a team and is not granted to %.', r.nspname, ro;
        END IF;
        IF readable THEN
            IF has_schema_privilege('public', r.oid, 'CREATE') THEN
                FOR t IN
                    SELECT pr.rolname FROM pg_roles pr
                    WHERE pr.rolcanlogin AND NOT pr.rolsuper AND pr.rolname !~ '^pg_'
                      AND pr.rolname <> ro
                      AND NOT EXISTS (SELECT 1 FROM pg_roles x WHERE x.oid = pr.oid AND (x.rolname LIKE 'mrsm\_%' OR coalesce(shobj_description(x.oid, 'pg_authid'), '') LIKE {comment}))
                LOOP
                    EXECUTE format('GRANT CREATE ON SCHEMA %I TO %I', r.nspname, t.rolname);
                END LOOP;
                EXECUTE format('REVOKE CREATE ON SCHEMA %I FROM PUBLIC', r.nspname);
            END IF;
            EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', r.nspname, ro);
            EXECUTE format('REVOKE CREATE ON SCHEMA %I FROM %I', r.nspname, ro);
            FOR t IN
                SELECT c.oid::regclass AS rel, {team_table} AS team
                FROM pg_class c
                WHERE c.relnamespace = r.oid AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
            LOOP
                IF t.team THEN
                    EXECUTE format('REVOKE ALL ON %s FROM %I', t.rel, ro);
                ELSE
                    EXECUTE format('GRANT SELECT ON %s TO %I', t.rel, ro);
                END IF;
            END LOOP;
            EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT SELECT ON TABLES TO %I', current_user, r.nspname, ro);
        ELSE
            EXECUTE format('REVOKE ALL ON SCHEMA %I FROM %I', r.nspname, ro);
            EXECUTE format('REVOKE ALL ON ALL TABLES IN SCHEMA %I FROM %I', r.nspname, ro);
            EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I REVOKE ALL ON TABLES FROM %I', current_user, r.nspname, ro);
        END IF;
    END LOOP;

    -- What the role must never be able to do.
    IF EXISTS (SELECT 1 FROM pg_roles WHERE oid = ro_oid AND (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls))
       OR EXISTS (SELECT 1 FROM pg_auth_members WHERE member = ro_oid) THEN
        RAISE EXCEPTION 'Meerschaum: the role % is privileged or a member of another role.', ro;
    END IF;
    IF has_database_privilege(ro, current_database(), 'CREATE') THEN
        RAISE EXCEPTION 'Meerschaum: the role % may create schemas in %.', ro, current_database();
    END IF;
    FOR r IN SELECT n2.nspname FROM pg_namespace n2 WHERE has_schema_privilege(ro, n2.oid, 'CREATE') LOOP
        RAISE EXCEPTION 'Meerschaum: the role % may create in the schema %.', ro, r.nspname;
    END LOOP;
    FOR r IN SELECT n2.nspname FROM pg_namespace n2 WHERE {team_schema_any} AND has_schema_privilege(ro, n2.oid, 'USAGE') LOOP
        RAISE EXCEPTION 'Meerschaum: the role % may read the team schema %.', ro, r.nspname;
    END LOOP;
END
$mrsm$"#,
        role = literal(MAIN_READONLY_ROLE),
        system = SYSTEM_SCHEMAS_SQL,
        comment = literal(&format!("{TEAM_ROLE_COMMENT}%")),
    )
}

/// `ALTER ROLE` [`MAIN_READONLY_ROLE`] `PASSWORD <password>`: `password` is SQL (a literal, or a
/// psql variable such as `:'ro_password'`).
pub fn main_readonly_password_sql(password: &str) -> String {
    format!(
        "ALTER ROLE {} PASSWORD {password}",
        quoted(MAIN_READONLY_ROLE)
    )
}

/// The psql script the stack runs before Grafana starts: [`main_readonly_sql`], then the password
/// from the psql variable `ro_password`. Stops at the first error.
pub fn main_readonly_psql_script(extra_schemas: &[String]) -> String {
    format!(
        "\\set ON_ERROR_STOP on\n{};\n{};\n",
        main_readonly_sql(extra_schemas),
        main_readonly_password_sql(":'ro_password'")
    )
}

/// Create or correct [`MAIN_READONLY_ROLE`] in `conn`'s database with `password` (what the stack
/// runs, for a connection Meerschaum holds).
pub async fn ensure_main_readonly(
    conn: &SqlConnector,
    password: &str,
    extra_schemas: &[String],
) -> Result<(), String> {
    if !supported(conn.flavor()) {
        return Err(format!(
            "The main read-only role needs a PostgreSQL-family database (not '{}').",
            conn.flavor().as_str()
        ));
    }
    run(conn, &main_readonly_sql(extra_schemas)).await?;
    run(conn, &main_readonly_password_sql(&literal(password))).await
}

/// Revoke from [`MAIN_READONLY_ROLE`] (when it exists) what a team's new schema or database
/// could give it, and anything granted to `PUBLIC` there.
async fn shut_out_main_readonly(
    conn: &SqlConnector,
    made: &TeamDatabase,
    name: &str,
) -> Result<(), String> {
    let main = quoted(MAIN_READONLY_ROLE);
    let main_exists = exists(
        conn,
        &format!(
            "SELECT 1 FROM pg_roles WHERE rolname = {}",
            literal(MAIN_READONLY_ROLE)
        ),
    )
    .await?;
    let thing = match made.kind {
        TeamDatabaseKind::Database => format!("DATABASE {}", quoted(name)),
        TeamDatabaseKind::Schema => format!("SCHEMA {}", quoted(name)),
    };
    run(conn, &format!("REVOKE ALL ON {thing} FROM PUBLIC")).await?;
    if main_exists {
        run(conn, &format!("REVOKE ALL ON {thing} FROM {main}")).await?;
        if made.kind == TeamDatabaseKind::Schema {
            run(
                conn,
                &format!(
                    "REVOKE ALL ON ALL TABLES IN SCHEMA {} FROM {main}",
                    quoted(name)
                ),
            )
            .await?;
        }
    }
    Ok(())
}

async fn installed(conn: &SqlConnector, extension: &str) -> bool {
    exists(
        conn,
        &format!(
            "SELECT 1 FROM pg_extension WHERE extname = {}",
            literal(extension)
        ),
    )
    .await
    .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slugs_are_plain_identifiers() {
        assert_eq!(slug("Wetlands").as_deref(), Some("wetlands"));
        assert_eq!(
            slug(" River Watch-North ").as_deref(),
            Some("river_watch_north")
        );
        assert_eq!(slug("a.b@c").as_deref(), Some("a_b_c"));
        assert_eq!(slug("2024 crew").as_deref(), Some("t_2024_crew"));
        assert_eq!(slug("--"), None);
        let long = slug(&"x".repeat(200)).unwrap();
        assert_eq!(long.len(), MAX_SLUG);
        let (role, readonly) = role_names(&long);
        assert!(role.len() <= 63 && readonly.len() <= 63);
    }

    #[test]
    fn identifiers_are_validated() {
        assert!(valid_identifier("wetlands").is_ok());
        assert!(valid_identifier("team_2").is_ok());
        for bad in [
            "Wetlands",
            "wet lands",
            "wet\"lands",
            "x;drop",
            "",
            "9lives",
            "postgres",
            "public",
            "pg_temp",
            "template1",
        ] {
            assert!(valid_identifier(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn passwords_are_long_and_plain() {
        let a = generate_password();
        let b = generate_password();
        assert_eq!(a.len(), 64);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b);
    }

    #[test]
    fn only_the_postgres_family_is_supported() {
        assert!(supported(Flavor::TimescaledbHa) && supported(Flavor::Postgresql));
        assert!(!supported(Flavor::Sqlite) && !supported(Flavor::Mssql));
        assert_eq!(
            TeamDatabaseKind::parse("Schema"),
            Some(TeamDatabaseKind::Schema)
        );
        assert_eq!(TeamDatabaseKind::parse("table"), None);
    }

    #[test]
    fn connector_attributes_and_summary() {
        let made = TeamDatabase {
            kind: TeamDatabaseKind::Schema,
            database: "meerschaum".into(),
            schema: Some("wetlands".into()),
            role: "mrsm_wetlands".into(),
            password: "secret".into(),
            readonly_role: "mrsm_wetlands_ro".into(),
            readonly_password: "other".into(),
            host: Some("db".into()),
            port: Some(5432),
            flavor: Flavor::TimescaledbHa,
            notes: Vec::new(),
        };
        let attrs = made.connector_attributes("Wetlands");
        assert_eq!(attrs.get("flavor").unwrap().py_str(), "timescaledb-ha");
        assert_eq!(attrs.get("username").unwrap().py_str(), "mrsm_wetlands");
        assert_eq!(attrs.get("schema").unwrap().py_str(), "wetlands");
        assert_eq!(
            attrs.get("teams"),
            Some(&Value::List(vec![Value::Str("Wetlands".into())]))
        );
        let summary = made.summary("sql:wetlands", "sql:main");
        assert!(!summary.contains_key("password"));
        assert_eq!(summary.get("kind").unwrap().py_str(), "schema");
        assert!(
            !format!("{summary:?}").contains("secret") && !format!("{summary:?}").contains("other")
        );
    }
}
