//! A team's database, schema and roles on a real server (`team_database::create`). Run only
//! when `MRSM_TEST_TIMESCALE_HA_URI` or `MRSM_TEST_PG_URI` names a server whose user may create
//! databases and roles, e.g. `MRSM_TEST_PG_URI=postgresql://test:test1234@localhost:5559/testdb`.
//!
//! What is asserted is the isolation the feature promises: a team's role owns and writes its
//! database, the read-only role only reads it, and neither opens the base database or the
//! database of another team.

use meerschaum_connector_sql::instance::SqlConnector;
use meerschaum_connector_sql::team_database::{self, TeamDatabase, TeamDatabaseKind};
use meerschaum_connector_sql::uri::ConnectionSpec;

async fn admin() -> Option<(SqlConnector, &'static str)> {
    for var in ["MRSM_TEST_TIMESCALE_HA_URI", "MRSM_TEST_PG_URI"] {
        if let Ok(uri) = std::env::var(var)
            && !uri.is_empty()
        {
            let spec = ConnectionSpec::from_uri(&uri, None, None).unwrap();
            return Some((
                SqlConnector::connect("team_admin", spec).await.unwrap(),
                var,
            ));
        }
    }
    eprintln!("Skipping: set MRSM_TEST_TIMESCALE_HA_URI or MRSM_TEST_PG_URI.");
    None
}

fn unique(tag: &str) -> String {
    format!(
        "rs test {tag} {}",
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    )
}

/// Connect to `database` on the admin's server as `user`.
async fn connect_as(
    admin: &SqlConnector,
    database: &str,
    user: &str,
    password: &str,
) -> Result<SqlConnector, String> {
    let mut spec = admin.spec().clone();
    spec.uri = None;
    spec.database = Some(database.to_string());
    spec.username = Some(user.to_string());
    spec.password = Some(password.to_string());
    spec.schema = None;
    let conn = SqlConnector::connect("team_user", spec)
        .await
        .map_err(|e| e.to_string())?;
    // The pool connects lazily on some drivers: make it prove the login.
    conn.driver()
        .value("SELECT 1")
        .await
        .map_err(|e| e.to_string())?;
    Ok(conn)
}

async fn exec(conn: &SqlConnector, sql: &str) -> Result<(), String> {
    conn.driver()
        .exec(sql)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

async fn int(conn: &SqlConnector, sql: &str) -> Option<i64> {
    conn.driver()
        .value(sql)
        .await
        .ok()
        .flatten()
        .and_then(|v| v.as_i64())
}

async fn drop_team(admin: &SqlConnector, made: &TeamDatabase) {
    match made.kind {
        TeamDatabaseKind::Database => {
            let _ = exec(
                admin,
                &format!("DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)", made.database),
            )
            .await;
        }
        TeamDatabaseKind::Schema => {
            let schema = made.schema.as_deref().unwrap();
            let _ = exec(
                admin,
                &format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"),
            )
            .await;
        }
    }
    for role in [&made.role, &made.readonly_role] {
        let _ = exec(admin, &format!("DROP OWNED BY \"{role}\"")).await;
        let _ = exec(admin, &format!("DROP ROLE IF EXISTS \"{role}\"")).await;
    }
}

#[tokio::test]
async fn teams_get_isolated_databases_and_schemas() {
    let Some((admin, _)) = admin().await else {
        return;
    };
    let base = admin.database().unwrap().to_string();
    let public_could_connect = int(
        &admin,
        "SELECT COUNT(*) FROM pg_database d, \
         aclexplode(coalesce(d.datacl, acldefault('d', d.datdba))) a \
         WHERE d.datname = current_database() AND a.grantee = 0 AND a.privilege_type = 'CONNECT'",
    )
    .await
        == Some(1);

    // A database of its own.
    let wet_team = unique("wet");
    let wet = team_database::create(&admin, &wet_team, TeamDatabaseKind::Database, None)
        .await
        .unwrap();
    let dry_team = unique("dry");
    let dry = team_database::create(&admin, &dry_team, TeamDatabaseKind::Database, None)
        .await
        .unwrap();
    // A schema in the base database.
    let fen_team = unique("fen");
    let fen = team_database::create(&admin, &fen_team, TeamDatabaseKind::Schema, None)
        .await
        .unwrap();

    let result = async {
        assert_eq!(wet.kind, TeamDatabaseKind::Database);
        assert_eq!(
            Some(wet.database.clone()),
            team_database::slug(&wet_team),
            "the database is named after the team"
        );
        assert_ne!(wet.password, wet.readonly_password);

        // The team's role owns its database: it creates, writes and reads.
        let owner = connect_as(&admin, &wet.database, &wet.role, &wet.password)
            .await
            .expect("the team's role connects to its database");
        exec(&owner, "CREATE SCHEMA IF NOT EXISTS public")
            .await
            .unwrap();
        exec(&owner, "CREATE TABLE readings (id INT PRIMARY KEY, v TEXT)")
            .await
            .unwrap();
        exec(&owner, "INSERT INTO readings VALUES (1, 'a'), (2, 'b')")
            .await
            .unwrap();
        // The read-only role reads what the team creates and writes nothing.
        let reader = connect_as(
            &admin,
            &wet.database,
            &wet.readonly_role,
            &wet.readonly_password,
        )
        .await
        .expect("the read-only role connects to the team's database");
        assert_eq!(int(&reader, "SELECT COUNT(*) FROM readings").await, Some(2));
        assert!(
            exec(&reader, "INSERT INTO readings VALUES (3, 'c')")
                .await
                .is_err()
        );
        assert!(exec(&reader, "CREATE TABLE sneaky (id INT)").await.is_err());
        assert!(exec(&reader, "DROP TABLE readings").await.is_err());

        // Neither role is privileged.
        for role in [&wet.role, &wet.readonly_role] {
            assert_eq!(
                int(
                    &admin,
                    &format!(
                        "SELECT COUNT(*) FROM pg_roles WHERE rolname = '{role}' \
                         AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole"
                    )
                )
                .await,
                Some(1),
                "{role}"
            );
        }

        // Another team's roles do not open this team's database, nor it theirs.
        assert!(
            connect_as(&admin, &wet.database, &dry.role, &dry.password)
                .await
                .is_err()
        );
        assert!(
            connect_as(
                &admin,
                &wet.database,
                &dry.readonly_role,
                &dry.readonly_password
            )
            .await
            .is_err()
        );
        assert!(
            connect_as(&admin, &dry.database, &wet.role, &wet.password)
                .await
                .is_err()
        );
        // Nor the base database (its PUBLIC `CONNECT` was taken away).
        if wet.notes.iter().all(|note| !note.contains("PUBLIC")) {
            assert!(
                connect_as(&admin, &base, &wet.role, &wet.password)
                    .await
                    .is_err()
            );
            assert!(
                connect_as(&admin, &base, &wet.readonly_role, &wet.readonly_password)
                    .await
                    .is_err()
            );
        }

        // A schema: the role works in it by default and reads nothing of the administrator's.
        assert_eq!(fen.kind, TeamDatabaseKind::Schema);
        assert_eq!(fen.database, base);
        let schema = fen.schema.clone().unwrap();
        let private = format!("rs_private_{}", &schema[schema.len() - 8..]);
        exec(
            &admin,
            &format!("CREATE TABLE public.\"{private}\" (id INT)"),
        )
        .await
        .unwrap();
        let fen_owner = connect_as(&admin, &base, &fen.role, &fen.password)
            .await
            .expect("a schema team's role connects to the base database");
        // What a sync runs before creating a pipe's table.
        exec(
            &fen_owner,
            &format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""),
        )
        .await
        .expect("a schema team's role passes `CREATE SCHEMA IF NOT EXISTS` on its schema");
        exec(&fen_owner, "CREATE TABLE plots (id INT)")
            .await
            .unwrap();
        exec(&fen_owner, "INSERT INTO plots VALUES (7)")
            .await
            .unwrap();
        assert_eq!(
            int(&admin, &format!("SELECT COUNT(*) FROM \"{schema}\".plots")).await,
            Some(1),
            "the table landed in the team's schema"
        );
        assert!(
            int(
                &fen_owner,
                &format!("SELECT COUNT(*) FROM public.\"{private}\"")
            )
            .await
            .is_none(),
            "the team's role must not read the administrator's tables"
        );
        let fen_reader = connect_as(&admin, &base, &fen.readonly_role, &fen.readonly_password)
            .await
            .unwrap();
        assert_eq!(
            int(&fen_reader, "SELECT COUNT(*) FROM plots").await,
            Some(1)
        );
        assert!(
            exec(&fen_reader, "INSERT INTO plots VALUES (8)")
                .await
                .is_err()
        );
        // A database team does not open the base database a schema team lives in.
        if wet.notes.iter().all(|note| !note.contains("PUBLIC")) {
            assert!(
                connect_as(&admin, &base, &dry.role, &dry.password)
                    .await
                    .is_err()
            );
        }
        let _ = exec(&admin, &format!("DROP TABLE public.\"{private}\"")).await;

        // Nothing is reused: the same team again is refused, and nothing of it is dropped.
        let again = team_database::create(&admin, &wet_team, TeamDatabaseKind::Database, None)
            .await
            .unwrap_err();
        assert!(again.contains("already"), "{again}");
        assert_eq!(int(&owner, "SELECT COUNT(*) FROM readings").await, Some(2));
        // Names are validated before anything runs.
        let bad = team_database::create(
            &admin,
            &unique("bad"),
            TeamDatabaseKind::Database,
            Some("Robert\"; DROP DATABASE postgres; --"),
        )
        .await
        .unwrap_err();
        assert!(bad.contains("not a valid name"), "{bad}");
    }
    .await;

    for made in [&wet, &dry, &fen] {
        drop_team(&admin, made).await;
    }
    if public_could_connect {
        let _ = exec(
            &admin,
            &format!("GRANT CONNECT, TEMPORARY ON DATABASE \"{base}\" TO PUBLIC"),
        )
        .await;
    }
    result
}

async fn text(conn: &SqlConnector, sql: &str) -> Option<String> {
    conn.driver()
        .value(sql)
        .await
        .ok()
        .flatten()
        .and_then(|v| match v {
            meerschaum_connector_sql::driver::SqlValue::Text(s) => Some(s),
            _ => None,
        })
}

/// The stack's main read-only role (`ensure_main_readonly`): it reads the instance's own
/// schemas (tables, views and hypertables, also those created later) and the extra schemas it
/// is given, and nothing of a team's, including a team created after the role; it writes,
/// creates and installs nothing, and is not privileged. Run twice: idempotent.
#[tokio::test]
async fn the_main_read_only_role_reads_public_data_only() {
    let Some((admin, _)) = admin().await else {
        return;
    };
    let base = admin.database().unwrap().to_string();
    let tag = &uuid::Uuid::new_v4().simple().to_string()[..8];
    let open = format!("rs_open_{tag}");
    let other = format!("rs_other_{tag}");
    let other_owner = format!("rs_owner_{tag}");
    let main = team_database::MAIN_READONLY_ROLE;
    let password = team_database::generate_password();
    let timescale = int(
        &admin,
        "SELECT COUNT(*) FROM pg_extension WHERE extname = 'timescaledb'",
    )
    .await
        == Some(1);
    let public_could_connect = int(
        &admin,
        "SELECT COUNT(*) FROM pg_database d, \
         aclexplode(coalesce(d.datacl, acldefault('d', d.datdba))) a \
         WHERE d.datname = current_database() AND a.grantee = 0 AND a.privilege_type = 'CONNECT'",
    )
    .await
        == Some(1);
    for sql in [
        format!("CREATE SCHEMA \"{open}\""),
        format!("CREATE TABLE \"{open}\".readings (ts TIMESTAMPTZ NOT NULL, v INT)"),
        format!("INSERT INTO \"{open}\".readings VALUES (now(), 1), (now(), 2)"),
        format!("CREATE VIEW \"{open}\".readings_view AS SELECT * FROM \"{open}\".readings"),
        format!("CREATE TABLE public.\"rs_public_{tag}\" (id INT)"),
        format!("INSERT INTO public.\"rs_public_{tag}\" VALUES (1)"),
        format!("CREATE ROLE \"{other_owner}\" NOLOGIN"),
        format!("CREATE SCHEMA \"{other}\" AUTHORIZATION \"{other_owner}\""),
        format!("CREATE TABLE \"{other}\".extra (id INT)"),
    ] {
        exec(&admin, &sql).await.unwrap();
    }
    if timescale {
        exec(
            &admin,
            &format!("SELECT create_hypertable('\"{open}\".readings', 'ts', migrate_data => true)"),
        )
        .await
        .unwrap();
    }
    // A team made before the role.
    let before_team = unique("before");
    let before = team_database::create(&admin, &before_team, TeamDatabaseKind::Schema, None)
        .await
        .unwrap();
    let before_schema = before.schema.clone().unwrap();
    exec(
        &admin,
        &format!("CREATE TABLE \"{before_schema}\".secret (id INT)"),
    )
    .await
    .unwrap();

    let mut teams = vec![before.clone()];
    let result = async {
        // The extra list names a team's schema too: it is never granted.
        let extra = vec![other.clone(), before_schema.clone()];
        team_database::ensure_main_readonly(&admin, &password, &extra)
            .await
            .unwrap();
        team_database::ensure_main_readonly(&admin, &password, &extra)
            .await
            .expect("idempotent");

        // Teams made after the role exists.
        let schema_team = team_database::create(
            &admin,
            &unique("after schema"),
            TeamDatabaseKind::Schema,
            None,
        )
        .await
        .unwrap();
        teams.push(schema_team.clone());
        let db_team = team_database::create(
            &admin,
            &unique("after db"),
            TeamDatabaseKind::Database,
            None,
        )
        .await
        .unwrap();
        teams.push(db_team.clone());
        let after_schema = schema_team.schema.clone().unwrap();
        let team_owner = connect_as(&admin, &base, &schema_team.role, &schema_team.password)
            .await
            .unwrap();
        exec(&team_owner, "CREATE TABLE plots (id INT)")
            .await
            .unwrap();
        exec(&team_owner, "INSERT INTO plots VALUES (1)")
            .await
            .unwrap();
        // The administrator writes into the team's schema too (as a superuser could).
        exec(
            &admin,
            &format!("CREATE TABLE \"{after_schema}\".admin_made (id INT)"),
        )
        .await
        .unwrap();
        // A table the instance's owner creates later in a readable schema.
        exec(&admin, &format!("CREATE TABLE \"{open}\".later (id INT)"))
            .await
            .unwrap();
        exec(&admin, &format!("INSERT INTO \"{open}\".later VALUES (5)"))
            .await
            .unwrap();

        let reader = connect_as(&admin, &base, main, &password)
            .await
            .expect("the main read-only role connects to the main database");
        assert_eq!(
            text(&reader, "SELECT current_user::text").await.as_deref(),
            Some(main)
        );
        assert_eq!(
            text(
                &reader,
                "SELECT rolsuper::text FROM pg_roles WHERE rolname = current_user"
            )
            .await
            .as_deref(),
            Some("false")
        );
        // Reads.
        for (sql, n) in [
            (format!("SELECT COUNT(*) FROM \"{open}\".readings"), 2),
            (format!("SELECT COUNT(*) FROM \"{open}\".readings_view"), 2),
            (format!("SELECT COUNT(*) FROM \"{open}\".later"), 1),
            (
                format!("SELECT COUNT(*) FROM public.\"rs_public_{tag}\""),
                1,
            ),
            (format!("SELECT COUNT(*) FROM \"{other}\".extra"), 0),
        ] {
            assert_eq!(int(&reader, &sql).await, Some(n), "{sql}");
        }
        // Never a team's.
        for sql in [
            format!("SELECT COUNT(*) FROM \"{before_schema}\".secret"),
            format!("SELECT COUNT(*) FROM \"{after_schema}\".plots"),
            format!("SELECT COUNT(*) FROM \"{after_schema}\".admin_made"),
        ] {
            assert!(int(&reader, &sql).await.is_none(), "{sql}");
        }
        assert!(
            connect_as(&admin, &db_team.database, main, &password)
                .await
                .is_err(),
            "no CONNECT on a team's database"
        );
        // Writes, DDL and extensions are refused.
        for sql in [
            format!("INSERT INTO \"{open}\".readings VALUES (now(), 3)"),
            format!("DELETE FROM \"{open}\".readings"),
            format!("CREATE TABLE \"{open}\".sneaky (id INT)"),
            "CREATE TABLE public.sneaky (id INT)".to_string(),
            "CREATE SCHEMA sneaky".to_string(),
            "CREATE EXTENSION IF NOT EXISTS dblink".to_string(),
            "SET default_transaction_read_only = off; CREATE TABLE public.sneaky2 (id INT)"
                .to_string(),
        ] {
            assert!(exec(&reader, &sql).await.is_err(), "{sql}");
        }
        assert_eq!(
            int(
                &admin,
                "SELECT COUNT(*) FROM pg_extension WHERE extname = 'dblink'"
            )
            .await,
            Some(0)
        );
        // The checks the role setup enforces hold.
        assert_eq!(
            int(
                &admin,
                &format!(
                    "SELECT COUNT(*) FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.member \
                     WHERE r.rolname = '{main}'"
                )
            )
            .await,
            Some(0)
        );
        // Running it again after the teams exist still grants nothing of theirs.
        team_database::ensure_main_readonly(&admin, &password, &extra)
            .await
            .unwrap();
        assert!(
            int(
                &reader,
                &format!("SELECT COUNT(*) FROM \"{after_schema}\".plots")
            )
            .await
            .is_none()
        );
        // The script the stack runs has the same statement and takes the password from psql.
        let script = team_database::main_readonly_psql_script(&extra);
        assert!(script.contains(&team_database::main_readonly_sql(&extra)));
        assert!(script.contains(":'ro_password'"));
    }
    .await;

    for made in &teams {
        drop_team(&admin, made).await;
    }
    for sql in [
        format!("DROP SCHEMA IF EXISTS \"{open}\" CASCADE"),
        format!("DROP SCHEMA IF EXISTS \"{other}\" CASCADE"),
        format!("DROP TABLE IF EXISTS public.\"rs_public_{tag}\""),
        format!("DROP ROLE IF EXISTS \"{other_owner}\""),
        format!("DROP OWNED BY \"{main}\""),
        format!("DROP ROLE IF EXISTS \"{main}\""),
    ] {
        if let Err(e) = exec(&admin, &sql).await {
            eprintln!("cleanup: {sql}: {e}");
        }
    }
    if public_could_connect {
        let _ = exec(
            &admin,
            &format!("GRANT CONNECT, TEMPORARY ON DATABASE \"{base}\" TO PUBLIC"),
        )
        .await;
    }
    result
}
