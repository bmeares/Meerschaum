//! `bootstrap teams` adds the team's connector to `meerschaum.yaml` and changes nothing else:
//! `MRSM{...}` references stay references, and what only the environment holds (an
//! `MRSM_SQL_<LABEL>` connector, `MRSM_CONFIG` / `MRSM_PATCH` patches) is never written to the
//! file.
//!
//! Runs only when `MRSM_TEST_PG_URI` names a PostgreSQL server whose user may create schemas and
//! roles (e.g. a throwaway container); the team's schema and roles are dropped afterwards. The
//! data is synthetic; nothing outside the temporary directory and that server is touched.

use std::path::PathBuf;

use meerschaum_cli::dispatch::{self, Runtime};
use meerschaum_cli::types::DateContext;
use meerschaum_core::SuccessTuple;
use meerschaum_core::config::{Config, ConfigOptions};

fn no_env(_: &str) -> Option<String> {
    None
}

const PATCH_SECRET: &str = "patch-only-k3y-31337";
const MRSM_PATCH_SECRET: &str = "mrsm-patch-only-s3cret";
const REFERENCE: &str = "MRSM{meerschaum:connectors:sql:local:database}";

/// The file as the user wrote it: a connector holding a reference, and one unrelated to teams.
const WRITTEN: &str = "\
connectors:
  sql:
    shared:
      flavor: sqlite
      database: MRSM{meerschaum:connectors:sql:local:database}
  api:
    upstream:
      host: api.example.com
      port: 8000
";

fn env(root: &std::path::Path, db: &std::path::Path, pg_uri: &str) -> Vec<(String, String)> {
    vec![
        ("MRSM_ROOT_DIR".into(), root.to_string_lossy().into_owned()),
        (
            "MRSM_PLUGINS_DIR".into(),
            root.join("plugins").to_string_lossy().into_owned(),
        ),
        ("MRSM_SQL_MAIN".into(), pg_uri.to_string()),
        (
            "MRSM_SQL_P2".into(),
            format!("sqlite:///{}", db.to_string_lossy()),
        ),
        (
            "MRSM_CONFIG".into(),
            format!(
                r#"{{"meerschaum": {{"connectors": {{"api": {{"upstream": {{"password": "{PATCH_SECRET}"}}}}}}}}}}"#
            ),
        ),
        (
            "MRSM_PATCH".into(),
            format!(
                r#"{{"meerschaum": {{"connectors": {{"api": {{"upstream": {{"username": "{MRSM_PATCH_SECRET}"}}}}}}}}}}"#
            ),
        ),
    ]
}

fn run(
    env: Vec<(String, String)>,
    root: &std::path::Path,
    sysargs: &[&str],
) -> (SuccessTuple, String) {
    let mut out = Vec::new();
    let result = {
        let mut rt = Runtime::new(DateContext::system(), 160, &mut out, no_env);
        rt.err = Box::new(std::io::sink());
        let mut opts = ConfigOptions::with_env(env);
        opts.allow_writes = true;
        opts.home = root.to_path_buf();
        rt.config = Some(Config::new(opts).unwrap());
        rt.cwd = Some(root.to_path_buf());
        let sysargs: Vec<String> = sysargs.iter().map(|s| s.to_string()).collect();
        dispatch::entry(&sysargs, &mut rt)
    };
    (result, String::from_utf8_lossy(&out).into_owned())
}

/// Parse a YAML file into JSON for comparisons.
fn parse(text: &str) -> serde_json::Value {
    let node = meerschaum_core::yaml::loader::load_text(
        text,
        "meerschaum.yaml",
        false,
        meerschaum_core::yaml::loader::LoadOptions {
            safe: true,
            project: false,
        },
    )
    .unwrap();
    serde_json::to_value(meerschaum_core::config::ConfigValue::from_node(&node).to_value()).unwrap()
}

#[test]
fn bootstrap_teams_writes_only_the_team_connector() {
    let pg_uri = match std::env::var("MRSM_TEST_PG_URI") {
        Ok(uri) if !uri.is_empty() => uri,
        _ => {
            eprintln!("Skipping: set MRSM_TEST_PG_URI.");
            return;
        }
    };
    let dir = tempfile::tempdir().unwrap();
    let root: PathBuf = dir.path().join("root");
    std::fs::create_dir_all(root.join("plugins")).unwrap();
    std::fs::create_dir_all(root.join("config")).unwrap();
    let file = root.join("config").join("meerschaum.yaml");
    std::fs::write(&file, WRITTEN).unwrap();
    let db = dir.path().join("instance.db");

    let tag = &uuid::Uuid::new_v4().simple().to_string()[..8];
    let team = format!("Tpr{tag}");
    let label = format!("tpr{tag}");
    let params = r#"{"isolated": true, "database": "schema", "base": "sql:main", "connectors": "", "plugins": ""}"#;
    let (result, out) = run(
        env(&root, &db, &pg_uri),
        &root,
        &[
            "bootstrap",
            "teams",
            &team,
            "--params",
            params,
            "--yes",
            "-i",
            "sql:p2",
        ],
    );

    let written = std::fs::read_to_string(&file).unwrap();

    // Drop what was made on the server before asserting anything.
    let schema_and_roles = {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let admin = meerschaum_connector_sql::SqlConnector::from_uri("tpr_admin", &pg_uri)
                .await
                .unwrap();
            let (role, readonly) = meerschaum_connector_sql::team_database::role_names(&label);
            for sql in [
                format!("DROP SCHEMA IF EXISTS \"{label}\" CASCADE"),
                format!("DROP OWNED BY \"{role}\""),
                format!("DROP ROLE IF EXISTS \"{role}\""),
                format!("DROP OWNED BY \"{readonly}\""),
                format!("DROP ROLE IF EXISTS \"{readonly}\""),
            ] {
                let _ = admin.driver().exec(&sql).await;
            }
            (role, readonly)
        })
    };

    assert!(
        result.success,
        "bootstrap teams failed: {}\n{out}",
        result.message
    );

    // Nothing the environment holds is in the file.
    let spec =
        meerschaum_connector_sql::uri::ConnectionSpec::from_uri(&pg_uri, None, None).unwrap();
    let main_password = spec.password.clone().unwrap();
    assert!(
        !written.contains(&main_password),
        "sql:main's password (from MRSM_SQL_MAIN) was written:\n{written}"
    );
    assert!(
        !written.contains(&pg_uri),
        "MRSM_SQL_MAIN was written:\n{written}"
    );
    assert!(
        !written.contains(PATCH_SECRET),
        "an MRSM_CONFIG patch was written:\n{written}"
    );
    assert!(
        !written.contains(MRSM_PATCH_SECRET),
        "an MRSM_PATCH patch was written:\n{written}"
    );
    // References stay references.
    assert!(
        written.contains(REFERENCE),
        "a reference was substituted:\n{written}"
    );

    // Only the team's connector was added.
    let mut after = parse(&written);
    let added = after["connectors"]["sql"]
        .as_object_mut()
        .unwrap()
        .remove(&label)
        .unwrap_or_else(|| panic!("sql:{label} is not in the file:\n{written}"));
    assert_eq!(added["username"], schema_and_roles.0.as_str());
    assert_eq!(added["schema"], label.as_str());
    assert_eq!(added["teams"], serde_json::json!([team]));
    assert!(added["password"].as_str().is_some_and(|p| !p.is_empty()));
    assert_eq!(after, parse(WRITTEN), "other keys changed:\n{written}");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&file).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode & 0o077,
            0,
            "the file holding the team's password is readable by others ({mode:o})"
        );
    }
}
