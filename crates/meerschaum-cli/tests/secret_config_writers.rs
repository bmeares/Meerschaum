//! 5.1: the CLI's config writers keep the file as written plus their change, with secrets in
//! every environment layer (`MRSM_<TYPE>_<LABEL>`, `MRSM_CONFIG`, `MRSM_PATCH`) and secret
//! references (`MRSM{file:...}`, `MRSM{env:...}`) in the file: no environment value and no
//! resolved secret reaches `meerschaum.yaml`. Also the masking of values a reference produced in
//! `show config` / `show connectors`, and the error of a connector whose reference cannot be
//! resolved.
//!
//! Every value is synthetic; each test works in its own temporary root and opens no database.
//!
//! A test binary of its own: `MRSM{env:...}` reads the process environment, which these tests
//! set, and a variable set in a shared suite would reach every other test of that suite.

use std::path::{Path, PathBuf};
use std::sync::Once;

use meerschaum_cli::dispatch::{self, Runtime};
use meerschaum_cli::types::DateContext;
use meerschaum_core::SuccessTuple;
use meerschaum_core::config::{Config, ConfigOptions};

const ENV_VAR: &str = "MRSM_TEST_CLI_SECRET_WRITERS";
const ENV_SECRET: &str = "cli-env-s3cret-value";
const FILE_SECRET: &str = "cli-file-s3cret-value";
const URI_ONLY: &str = "cli-uri-only-s3cret";
const CONFIG_ONLY: &str = "cli-config-only-s3cret";
const PATCH_ONLY: &str = "cli-patch-only-s3cret";
static SET_ENV: Once = Once::new();

fn no_env(_: &str) -> Option<String> {
    None
}

#[allow(unsafe_code)] // sets the process environment once, below
fn setup() -> (tempfile::TempDir, PathBuf, PathBuf) {
    SET_ENV.call_once(|| {
        // SAFETY: these tests are the only ones in this binary (kept out of the suites for
        // this reason): the variable is set once, before any of them reads it, never changed,
        // and private to them.
        unsafe { std::env::set_var(ENV_VAR, ENV_SECRET) };
    });
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    std::fs::create_dir_all(root.join("plugins")).unwrap();
    std::fs::create_dir_all(root.join("config")).unwrap();
    std::fs::write(root.join("pg-password"), format!("{FILE_SECRET}\n")).unwrap();
    let file = root.join("config").join("meerschaum.yaml");
    std::fs::write(
        &file,
        format!(
            "instance: sql:main\nconnectors:\n  sql:\n    written:\n      flavor: postgresql\n      \
             host: db.example\n      password: MRSM{{file:pg-password}}\n      username: \
             MRSM{{env:{ENV_VAR}}}\n    deletable:\n      flavor: postgresql\n      host: h\n      \
             username: u\n      database: d\n      password: MRSM{{file:pg-password}}\n    \
             linked:\n      flavor: sqlite\n      database: \
             MRSM{{meerschaum:connectors:sql:local:database}}\n"
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    (dir, root, file)
}

fn env(root: &Path) -> Vec<(String, String)> {
    vec![
        ("MRSM_ROOT_DIR".into(), root.to_string_lossy().into_owned()),
        (
            "MRSM_PLUGINS_DIR".into(),
            root.join("plugins").to_string_lossy().into_owned(),
        ),
        (
            "MRSM_SQL_ENVCONN".into(),
            format!("postgresql://u:{URI_ONLY}@h:5432/db"),
        ),
        (
            "MRSM_CONFIG".into(),
            format!(
                r#"{{"meerschaum": {{"connectors": {{"api": {{"cfg": {{"host": "h", "password": "{CONFIG_ONLY}"}}}}}}}}}}"#
            ),
        ),
        (
            "MRSM_PATCH".into(),
            format!(
                r#"{{"meerschaum": {{"connectors": {{"sql": {{"written": {{"options": {{"token": "{PATCH_ONLY}"}}}}}}}}}}}}"#
            ),
        ),
    ]
}

fn run(root: &Path, sysargs: &[&str]) -> (SuccessTuple, String) {
    let mut out = Vec::new();
    let result = {
        let mut rt = Runtime::new(DateContext::system(), 200, &mut out, no_env);
        rt.err = Box::new(std::io::sink());
        let mut opts = ConfigOptions::with_env(env(root));
        opts.allow_writes = true;
        opts.home = root.to_path_buf();
        rt.config = Some(Config::new(opts).unwrap());
        rt.cwd = Some(root.to_path_buf());
        let sysargs: Vec<String> = sysargs.iter().map(|s| s.to_string()).collect();
        dispatch::entry(&sysargs, &mut rt)
    };
    (result, String::from_utf8_lossy(&out).into_owned())
}

fn assert_clean(file: &Path) -> String {
    let text = std::fs::read_to_string(file).unwrap();
    for secret in [ENV_SECRET, FILE_SECRET, URI_ONLY, CONFIG_ONLY, PATCH_ONLY] {
        assert!(!text.contains(secret), "{secret} reached the file:\n{text}");
    }
    assert!(text.contains("MRSM{file:pg-password}"), "{text}");
    assert!(text.contains(&format!("MRSM{{env:{ENV_VAR}}}")), "{text}");
    assert!(
        text.contains("MRSM{meerschaum:connectors:sql:local:database}"),
        "{text}"
    );
    assert!(
        !text.contains("envconn") && !text.contains("cfg:"),
        "{text}"
    );
    text
}

/// `register connectors` wrote the whole substituted section (as v4 did); 5.1 writes the file
/// as written plus the new connector (PARITY.md 51-SEC-001).
#[test]
fn register_connectors_writes_only_the_new_connector() {
    let (_dir, root, file) = setup();
    let (result, out) = run(
        &root,
        &[
            "register",
            "connectors",
            "sql:brandnew",
            "--params",
            r#"{"flavor": "sqlite", "database": "/tmp/brandnew.db"}"#,
            "--yes",
        ],
    );
    assert!(result.success, "{result:?} {out}");
    let text = assert_clean(&file);
    assert!(text.contains("brandnew:"), "{text}");
}

#[test]
fn delete_connectors_keeps_the_rest_as_written() {
    let (_dir, root, file) = setup();
    let (result, out) = run(
        &root,
        &["delete", "connectors", "sql:deletable", "--yes", "--force"],
    );
    assert!(result.success, "{result:?} {out}");
    let text = assert_clean(&file);
    assert!(!text.contains("deletable:"), "{text}");
}

#[test]
fn show_config_and_show_connectors_mask_values_a_reference_produced() {
    let (_dir, root, _file) = setup();
    for args in [
        &["show", "config", "meerschaum", "--nopretty"][..],
        &["show", "config", "meerschaum"][..],
        &["show", "connectors", "--nopretty"][..],
        &["show", "connectors", "sql"][..],
    ] {
        let (result, out) = run(&root, args);
        assert!(result.success, "{args:?}: {result:?}");
        for secret in [ENV_SECRET, FILE_SECRET] {
            assert!(!out.contains(secret), "{args:?} printed {secret}:\n{out}");
        }
    }
    // `username` is shown by `show connectors` for an ordinary value; a referenced one is not.
    let (_, out) = run(&root, &["show", "connectors", "sql", "--nopretty"]);
    assert!(out.contains("db.example"), "{out}");
}

#[test]
fn a_connector_with_an_unresolved_reference_fails_with_the_reference_and_key() {
    let (_dir, root, file) = setup();
    std::fs::write(
        &file,
        "connectors:\n  sql:\n    broken:\n      flavor: sqlite\n      database: \
         MRSM{file:missing-database-path}\n",
    )
    .unwrap();
    // Unrelated work still runs.
    let (result, _) = run(&root, &["show", "config", "meerschaum", "--nopretty"]);
    assert!(result.success);
    let (result, out) = run(&root, &["show", "pipes", "-i", "sql:broken"]);
    let text = format!("{} {out}", result.message);
    assert!(text.contains("MRSM{file:missing-database-path}"), "{text}");
    assert!(
        text.contains("meerschaum:connectors:sql:broken:database"),
        "{text}"
    );
}
