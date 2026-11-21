use super::*;

/// The probe prints one line of JSON; the fixtures below are wrapped for reading.
fn probe(json: &str) -> Result<PyProbe, String> {
    parse_probe(&json.replace('\n', " "))
}

const OK_PROBE: &str = r#"{"version": "3.13.1", "executable": "/usr/bin/python3",
    "prefix": "/usr", "base_prefix": "/usr",
    "modules": {"pandas": {"found": true, "version": "3.0.1"},
                "fastapi": {"found": false, "version": null}},
    "meerschaum": {"found": true, "dir": "/site/meerschaum", "version": "5.0.0rc1",
                   "copies": [{"dir": "/site/meerschaum", "version": "5.0.0rc1"}],
                   "user_site": "/home/u/.local/lib/python3.13/site-packages"}}"#;

#[test]
fn a_missing_unique_key_names_the_duplicates_and_the_rows_to_delete() {
    use meerschaum_connector_sql::instance::unique_keys::USERS_KEY;
    let ok = |status| judge_unique_key(&PIPES_KEY, &Ok(status), "mrsm_pipes");
    assert_eq!(ok(KeyStatus::Present).status, Status::Pass);
    assert_eq!(ok(KeyStatus::NoTable).status, Status::Skip);
    let pending = ok(KeyStatus::Missing { duplicates: vec![] });
    assert_eq!(pending.status, Status::Warn);
    let dup = |values: &[Option<&str>], ids: &[i64]| Duplicate {
        values: values.iter().map(|v| v.map(str::to_string)).collect(),
        ids: ids.to_vec(),
    };
    let v = ok(KeyStatus::Missing {
        duplicates: vec![
            dup(&[Some("sql:a"), Some("m"), None], &[3, 7, 9]),
            dup(&[Some("sql:a"), Some("m"), Some("x")], &[4, 8]),
        ],
    });
    assert_eq!(v.status, Status::Warn);
    assert!(
        v.detail
            .starts_with("2 pipe(s) are registered more than once, so mrsm_pipes"),
        "{}",
        v.detail
    );
    assert!(
        v.detail.contains("Pipe('sql:a', 'm') (pipe_id 3, 7, 9)"),
        "{}",
        v.detail
    );
    assert!(
        v.detail.contains("Pipe('sql:a', 'm', 'x') (pipe_id 4, 8)"),
        "{}",
        v.detail
    );
    let fix = v.fix.unwrap();
    assert!(
        fix.contains("DELETE FROM mrsm_pipes WHERE pipe_id IN (7, 9, 8)"),
        "{fix}"
    );
    let users = judge_unique_key(
        &USERS_KEY,
        &Ok(KeyStatus::Missing {
            duplicates: vec![dup(&[Some("alice")], &[2, 5])],
        }),
        "mrsm_users",
    );
    assert!(
        users
            .detail
            .contains("1 user(s) are registered more than once, so mrsm_users has no unique key on the username: 'alice' (user_id 2, 5)"),
        "{}",
        users.detail
    );
    assert!(
        users
            .fix
            .unwrap()
            .contains("DELETE FROM mrsm_users WHERE user_id IN (5)")
    );
    assert_eq!(unique_key_check_name(&PIPES_KEY), "pipes key");
    assert_eq!(unique_key_check_name(&USERS_KEY), "users key");
    let err = judge_unique_key(&PIPES_KEY, &Err("timeout".into()), "mrsm_pipes");
    assert_eq!(err.status, Status::Warn);
}

#[test]
fn statuses_print_and_sort() {
    assert_eq!(Status::Pass.as_str(), "PASS");
    assert_eq!(Status::Fail.as_str(), "FAIL");
    assert!(Status::Pass < Status::Warn && Status::Warn < Status::Fail);
}

#[test]
fn human_bytes_uses_binary_units() {
    assert_eq!(human_bytes(512), "512 B");
    assert_eq!(human_bytes(1536), "1.5 KB");
    assert_eq!(human_bytes(GIB), "1.0 GB");
    assert_eq!(human_bytes(5 * GIB / 2), "2.5 GB");
}

#[test]
fn versions_compare_as_pep_440() {
    assert!(versions_match("5.0.0-rc.1", "5.0.0rc1"));
    assert!(versions_match("5.0.0-a1", "5.0.0a1"));
    assert!(!versions_match("4.1.4", "5.0.0rc1"));
    assert!(!versions_match("5.0.0rc1", "5.0.0"));
    assert!(versions_match("not a version", "not a version"));
}

#[test]
fn python_versions_parse() {
    assert_eq!(parse_major_minor("3.13.1"), Some((3, 13)));
    assert_eq!(parse_major_minor("3.14.0rc2"), Some((3, 14)));
    assert_eq!(parse_major_minor("3"), None);
    assert!(parse_major_minor("3.10.9").unwrap() < MIN_PYTHON);
}

#[test]
fn the_probe_output_is_read_from_its_last_json_line() {
    let noisy = format!(
        "a banner from sitecustomize\n{}\n",
        OK_PROBE.replace('\n', " ")
    );
    let p = parse_probe(&noisy).unwrap();
    assert_eq!(p.version, "3.13.1");
    assert!(p.modules["pandas"].found);
    assert_eq!(p.modules["pandas"].version.as_deref(), Some("3.0.1"));
    assert!(!p.modules["fastapi"].found);
    assert_eq!(p.meerschaum.copies.len(), 1);
    assert!(probe("nothing useful").is_err());
    assert!(probe("{\"executable\": \"x\"}").is_err());
}

#[test]
fn the_interpreter_must_exist_and_be_new_enough() {
    let ok = probe(OK_PROBE);
    assert_eq!(judge_interpreter(&ok, "python3", 0).status, Status::Pass);
    let old = probe(&OK_PROBE.replace("3.13.1", "3.9.2"));
    let v = judge_interpreter(&old, "python3", 0);
    assert_eq!(v.status, Status::Fail);
    assert!(v.fix.unwrap().contains("MRSM_PYTHON"));
    let missing: Result<PyProbe, String> = Err("No such file".into());
    assert_eq!(
        judge_interpreter(&missing, "python3", 0).status,
        Status::Warn
    );
    // A Python plugin cannot run without it.
    assert_eq!(
        judge_interpreter(&missing, "python3", 2).status,
        Status::Fail
    );
}

#[test]
fn a_matching_package_passes() {
    let v = judge_package(&probe(OK_PROBE), "5.0.0-rc.1", 1, 0);
    assert_eq!(v.status, Status::Pass, "{v:?}");
    assert!(v.detail.contains("/site/meerschaum"));
}

#[test]
fn a_stale_package_is_a_failure_naming_its_path() {
    let stale = OK_PROBE
        .replace("\"dir\": \"/site/meerschaum\", \"version\": \"5.0.0rc1\",\n", "\"dir\": \"/home/u/.local/lib/python3.13/site-packages/meerschaum\", \"version\": \"4.1.4\",\n");
    // With nothing that runs in that interpreter, a stale copy is a warning.
    let idle = judge_package(&probe(&stale), "5.0.0-rc.1", 1, 0);
    assert_eq!(idle.status, Status::Warn, "{idle:?}");
    assert!(idle.detail.contains("only plugins need it"));
    let v = judge_package(&probe(&stale), "5.0.0-rc.1", 1, 2);
    assert_eq!(v.status, Status::Fail, "{v:?}");
    assert!(v.detail.contains("2 Python plugins cannot run"));
    assert!(v.detail.contains("4.1.4"));
    assert!(
        v.detail
            .contains(".local/lib/python3.13/site-packages/meerschaum")
    );
    assert!(v.detail.contains("user site"));
    assert!(v.fix.unwrap().contains("MRSM_PYTHON"));
}

fn with_package(fields: &str) -> Result<PyProbe, String> {
    probe(&OK_PROBE.replace(
        "\"found\": true, \"dir\": \"/site/meerschaum\", \"version\": \"5.0.0rc1\",",
        &format!("\"found\": true, \"dir\": \"/site/meerschaum\", {fields},"),
    ))
}

#[test]
fn another_release_of_the_same_major_warns_and_starts() {
    let p = with_package("\"version\": \"5.0.3\", \"has_worker\": true, \"protocol\": 1");
    for plugins in [0, 4] {
        let v = judge_package(&p, "5.0.0-rc.1", 1, plugins);
        assert_eq!(v.status, Status::Warn, "{v:?}");
        assert!(v.detail.contains("5.0.3") && v.detail.contains("5.0.0-rc.1"));
        assert!(v.detail.contains("the worker starts"));
        assert!(v.fix.unwrap().contains("pip install 'meerschaum=="));
    }
}

#[test]
fn another_worker_protocol_is_refused() {
    let p = with_package("\"version\": \"5.0.0rc1\", \"has_worker\": true, \"protocol\": 2");
    let idle = judge_package(&p, "5.0.0rc1", 1, 0);
    assert_eq!(idle.status, Status::Warn);
    assert!(idle.detail.contains("worker protocol 2") && idle.detail.contains("refuses"));
    assert_eq!(judge_package(&p, "5.0.0rc1", 1, 1).status, Status::Fail);
}

#[test]
fn a_copy_that_looks_like_v4_is_refused() {
    let p = with_package("\"version\": null, \"has_worker\": false, \"protocol\": null");
    let v = judge_package(&p, "5.0.0rc1", 1, 1);
    assert_eq!(v.status, Status::Fail, "{v:?}");
    assert!(v.detail.contains("looks like Meerschaum 4"));
}

#[test]
fn the_advice_depends_on_whether_another_copy_exists() {
    let stale = "\"version\": \"4.1.4\", \"has_worker\": false, \"protocol\": null";
    // The only copy: install the matching release.
    let only = judge_package(&with_package(stale), "5.0.0rc1", 1, 1);
    let fix = only.fix.unwrap();
    assert!(fix.starts_with("install the matching release"), "{fix}");
    assert!(!fix.contains("uninstall"));
    // Another copy behind it: uninstall this one.
    let two = OK_PROBE.replace(
        "\"found\": true, \"dir\": \"/site/meerschaum\", \"version\": \"5.0.0rc1\",",
        &format!("\"found\": true, \"dir\": \"/site/meerschaum\", {stale},"),
    ).replace(
        "\"copies\": [{\"dir\": \"/site/meerschaum\", \"version\": \"5.0.0rc1\"}]",
        "\"copies\": [{\"dir\": \"/site/meerschaum\", \"version\": \"4.1.4\"}, {\"dir\": \"/other/meerschaum\", \"version\": \"5.0.0rc1\"}]",
    );
    let fix = judge_package(&probe(&two), "5.0.0rc1", 1, 1).fix.unwrap();
    assert!(
        fix.starts_with("uninstall this copy (delete /site/meerschaum"),
        "{fix}"
    );
}

#[test]
fn an_older_copy_in_front_names_the_copies_it_shadows() {
    let stale = OK_PROBE
        .replace(
            "\"found\": true, \"dir\": \"/site/meerschaum\", \"version\": \"5.0.0rc1\",",
            "\"found\": true, \"dir\": \"/home/u/.local/lib/python3.15/site-packages/meerschaum\", \"version\": \"5.0.0rc1\",",
        )
        .replace(
            "\"copies\": [{\"dir\": \"/site/meerschaum\", \"version\": \"5.0.0rc1\"}]",
            "\"copies\": [{\"dir\": \"/home/u/.local/lib/python3.15/site-packages/meerschaum\", \"version\": \"5.0.0rc1\"}, {\"dir\": \"/usr/local/lib64/python3.15/site-packages/meerschaum\", \"version\": \"5.0.0rc2\"}]",
        );
    let v = judge_package(&probe(&stale), "5.0.0rc2", 1, 0);
    assert_eq!(v.status, Status::Warn);
    assert!(
        v.detail.contains(
            "; it shadows /usr/local/lib64/python3.15/site-packages/meerschaum (5.0.0rc2)"
        ),
        "{}",
        v.detail
    );
    assert!(
        v.fix
            .unwrap()
            .starts_with("remove the copy in /home/u/.local")
    );
}

#[test]
fn extra_copies_on_the_path_warn() {
    let two = OK_PROBE.replace(
        "\"copies\": [{\"dir\": \"/site/meerschaum\", \"version\": \"5.0.0rc1\"}]",
        "\"copies\": [{\"dir\": \"/site/meerschaum\", \"version\": \"5.0.0rc1\"}, {\"dir\": \"/other/meerschaum\", \"version\": \"5.0.0rc1\"}]",
    );
    let v = judge_package(&probe(&two), "5.0.0rc1", 1, 0);
    assert_eq!(v.status, Status::Warn);
    assert!(v.detail.contains("/other/meerschaum"));
}

#[test]
fn a_missing_package_fails_only_when_plugins_need_it() {
    let none = OK_PROBE.replace(
        "\"found\": true, \"dir\": \"/site/meerschaum\"",
        "\"found\": false, \"dir\": null",
    );
    assert_eq!(
        judge_package(&probe(&none), "5.0.0rc1", 1, 0).status,
        Status::Warn
    );
    assert_eq!(
        judge_package(&probe(&none), "5.0.0rc1", 1, 1).status,
        Status::Fail
    );
    let err: Result<PyProbe, String> = Err("x".into());
    assert_eq!(judge_package(&err, "5.0.0rc1", 1, 3).status, Status::Skip);
}

#[test]
fn the_manifest_is_judged_without_being_built() {
    let none: Vec<String> = Vec::new();
    assert_eq!(
        judge_manifest(false, false, &none, 0, 0).status,
        Status::Pass
    );
    let missing = judge_manifest(false, false, &none, 2, 0);
    assert_eq!(missing.status, Status::Warn);
    assert!(missing.detail.contains("missing") && missing.fix.unwrap().contains("mrsm reload"));
    let stale = judge_manifest(true, true, &["a".to_string(), "b".to_string()], 2, 2);
    assert_eq!(stale.status, Status::Warn);
    assert!(stale.detail.contains("stale (changed: a, b)"));
    assert_eq!(
        judge_manifest(true, false, &none, 1, 1).status,
        Status::Pass
    );
}

#[test]
fn plugins_not_in_the_manifest_are_not_reported_as_imported() {
    let mut f = plugin_facts(&manifest(MANIFEST));
    f.import_errors.clear();
    f.load_errors.clear();
    f.names.push("fresh".into());
    f.unbuilt.push("fresh".into());
    let checks = judge_plugins(&f);
    let imports = &checks.iter().find(|(n, _)| *n == "imports").unwrap().1;
    assert_eq!(imports.status, Status::Skip);
    assert!(imports.detail.contains("fresh"));
}

#[test]
fn connector_variables_are_told_from_lookalikes() {
    let names: Vec<String> = [
        "MRSM_SQL_MAIN",
        "MRSM_API_PROD",
        "MRSM_VALKEY_CACHE",
        "MRSM_PLUGIN_TOOLS",
        "MRSM_SYSTEMD_UNITS",
        "MRSM_DUCKDB_LIB",
        "MRSM_FOO_BAR",
        "MRSM_TEST_PG_URI",
    ]
    .map(String::from)
    .to_vec();
    let (connectors, others) = split_env_vars(&names, &["foo".to_string()]);
    assert_eq!(
        connectors,
        [
            "MRSM_SQL_MAIN",
            "MRSM_API_PROD",
            "MRSM_VALKEY_CACHE",
            "MRSM_PLUGIN_TOOLS",
            "MRSM_SYSTEMD_UNITS",
            "MRSM_FOO_BAR"
        ]
    );
    assert_eq!(others, ["MRSM_DUCKDB_LIB", "MRSM_TEST_PG_URI"]);
}

#[test]
fn python_duckdb_metadata_never_claims_import_or_connection_success() {
    let fact = |version: Option<&str>| DuckdbHostDiagnostic::Python {
        version: version.map(str::to_string),
        path: Some("/site".into()),
        pinned: false,
    };
    let ready = judge_duckdb_host(&fact(Some("99.0.0")), true, true);
    assert_eq!(ready.status, Status::Pass);
    assert!(ready.detail.contains("metadata") && ready.detail.contains("not probed"));
    assert!(ready.detail.contains("policy unchanged"));
    assert_eq!(
        judge_duckdb_host(&fact(None), true, true).status,
        Status::Warn
    );
    let old = judge_duckdb_host(&fact(Some("0.1.0")), true, true);
    assert_eq!(old.status, Status::Fail);
    assert!(old.detail.contains(duckdb_requirement()));
    assert!(old.fix.unwrap().contains("restart"));
    assert_eq!(
        judge_duckdb_host(&fact(Some("0.1.0")), true, false).status,
        Status::Warn
    );
    assert_eq!(
        judge_duckdb_host(&fact(Some("0.1.0")), false, false).status,
        Status::Skip
    );
    let refused = DuckdbHostDiagnostic::Refused("fresh interpreter required".into());
    assert_eq!(judge_duckdb_host(&refused, true, true).status, Status::Fail);
}

#[test]
fn duckdb_is_never_probed_without_its_library() {
    let gate = |problem: Option<&str>, auto_install| DuckdbGate {
        problem: problem.map(str::to_string),
        auto_install,
        host: None,
    };
    assert!(judge_duckdb_gate(&gate(None, true)).is_none());
    let first_use = judge_duckdb_gate(&gate(Some("not installed"), true)).unwrap();
    assert_eq!(first_use.status, Status::Warn);
    assert!(first_use.detail.contains("not probed") && first_use.detail.contains("never installs"));
    assert!(first_use.fix.unwrap().contains("install driver duckdb"));
    assert_eq!(
        judge_duckdb_gate(&gate(Some("not installed"), false))
            .unwrap()
            .status,
        Status::Fail
    );
    assert_eq!(
        judge_duckdb_gate(&gate(Some("not loadable: bad ELF"), true))
            .unwrap()
            .status,
        Status::Fail
    );
}

#[test]
fn disk_thresholds() {
    let fact = |free: u64, inodes: Option<(u64, u64)>| DiskFact {
        free_bytes: free,
        total_bytes: 100 * GIB,
        free_inodes: inodes.map(|i| i.0),
        total_inodes: inodes.map(|i| i.1),
    };
    assert_eq!(
        judge_disk("/", &fact(50 * GIB, Some((90, 100)))).status,
        Status::Pass
    );
    assert_eq!(judge_disk("/", &fact(GIB - 1, None)).status, Status::Warn);
    assert_eq!(judge_disk("/", &fact(GIB, None)).status, Status::Pass);
    assert_eq!(judge_disk("/", &fact(10 * MIB, None)).status, Status::Fail);
    // A tmpfs near its inode limit: plenty of bytes, few inodes.
    assert_eq!(
        judge_disk("/tmp", &fact(50 * GIB, Some((4, 100)))).status,
        Status::Warn
    );
    assert_eq!(
        judge_disk("/tmp", &fact(50 * GIB, Some((0, 100)))).status,
        Status::Fail
    );
    assert_eq!(
        judge_disk("/", &fact(50 * GIB, Some((0, 0)))).status,
        Status::Pass
    );
    let warned = judge_disk("/tmp", &fact(50 * GIB, Some((4, 100))));
    assert!(warned.detail.contains("4% free"), "{}", warned.detail);
}

#[test]
fn directories() {
    let fact = |exists, is_dir, writable| DirFact {
        exists,
        is_dir,
        writable,
    };
    assert_eq!(
        judge_dir("/r", DirRole::Root, &fact(true, true, true)).status,
        Status::Pass
    );
    assert_eq!(
        judge_dir("/r", DirRole::Root, &fact(false, false, false)).status,
        Status::Fail
    );
    assert_eq!(
        judge_dir("/r", DirRole::Root, &fact(true, true, false)).status,
        Status::Fail
    );
    assert_eq!(
        judge_dir("/r", DirRole::Root, &fact(true, false, false)).status,
        Status::Fail
    );
    assert_eq!(
        judge_dir("/p", DirRole::Plugins, &fact(true, true, false)).status,
        Status::Warn
    );
    assert_eq!(
        judge_dir("/p", DirRole::Plugins, &fact(false, false, false)).status,
        Status::Pass
    );
    assert_eq!(
        judge_dir("/c", DirRole::Config, &fact(false, false, false)).status,
        Status::Pass
    );
}

fn file(name: &str, content: Option<&str>, error: Option<&str>) -> ConfigFile {
    ConfigFile {
        name: name.into(),
        stem: name.split('.').next().unwrap().into(),
        error: error.map(str::to_string),
        value: content.map(|c| Value::from_json_str(c).unwrap()),
    }
}

fn known() -> (Vec<String>, Vec<String>) {
    (
        ["meerschaum", "system", "plugins"]
            .map(String::from)
            .to_vec(),
        ["instance", "connectors"].map(String::from).to_vec(),
    )
}

#[test]
fn config_files_that_do_not_parse_fail() {
    let (top, mrsm) = known();
    let files = vec![
        file("meerschaum.yaml", Some("{}"), None),
        file(
            "system.json",
            None,
            Some("Expecting value: line 1 column 1"),
        ),
    ];
    let checks = judge_config(&files, &top, &mrsm);
    let files_check = checks.iter().find(|(n, _)| *n == "files").unwrap();
    assert!(
        files_check
            .1
            .detail
            .starts_with("1 config file does not parse"),
        "{}",
        files_check.1.detail
    );
    assert_eq!(files_check.1.status, Status::Fail);
    assert!(files_check.1.detail.contains("system.json"));
    assert!(files_check.1.fix.is_some());
}

#[test]
fn unknown_and_deprecated_keys_warn() {
    let (top, mrsm) = known();
    let files = vec![
        file(
            "meerschaum.yaml",
            Some(r#"{"instance": "sql:x", "weird": 1}"#),
            None,
        ),
        file("stray.json", Some("{}"), None),
        file(
            "system.json",
            Some(r#"{"experimental": {"join_fetch": true}}"#),
            None,
        ),
    ];
    let checks = judge_config(&files, &top, &mrsm);
    let keys = &checks.iter().find(|(n, _)| *n == "keys").unwrap().1;
    assert_eq!(keys.status, Status::Warn);
    assert!(keys.detail.contains("stray.json"));
    assert!(keys.detail.contains("meerschaum:weird"));
    assert!(keys.detail.contains("join_fetch"));
}

#[test]
fn clean_config_passes() {
    let (top, mrsm) = known();
    // v5's own default file holds `join_fetch: false`: not a problem.
    let files = vec![
        file(
            "meerschaum.yaml",
            Some(r#"{"instance": "sql:x", "connectors": {}}"#),
            None,
        ),
        file(
            "system.json",
            Some(r#"{"experimental": {"join_fetch": false}}"#),
            None,
        ),
    ];
    let checks = judge_config(&files, &top, &mrsm);
    assert!(
        checks.iter().all(|(_, v)| v.status == Status::Pass),
        "{checks:?}"
    );
    assert_eq!(judge_config(&[], &top, &mrsm)[0].1.status, Status::Pass);
}

#[test]
fn flavors_come_from_the_attribute_or_the_uri() {
    let attrs = |json: &str| Value::from_json_str(json).unwrap().to_map();
    assert_eq!(
        flavor_of(&attrs(r#"{"flavor": "PostgreSQL"}"#)),
        "postgresql"
    );
    assert_eq!(
        flavor_of(&attrs(r#"{"uri": "oracle+oracledb://u:p@h/db"}"#)),
        "oracle"
    );
    assert_eq!(
        flavor_of(&attrs(r#"{"uri": "postgres://u@h/db"}"#)),
        "postgresql"
    );
    assert_eq!(flavor_of(&attrs(r#"{}"#)), "");
}

#[test]
fn sqlite_paths() {
    let attrs = |json: &str| Value::from_json_str(json).unwrap().to_map();
    assert_eq!(
        sqlite_path(&attrs(r#"{"uri": "sqlite:////abs/x.db"}"#), "x", "/root/db").as_deref(),
        Some("/abs/x.db")
    );
    assert_eq!(
        sqlite_path(
            &attrs(r#"{"uri": "sqlite:///rel.db?mode=ro"}"#),
            "x",
            "/root/db"
        )
        .as_deref(),
        Some("rel.db")
    );
    assert_eq!(
        sqlite_path(
            &attrs(r#"{"database": "{SQLITE_DB_PATH}"}"#),
            "x",
            "/root/db"
        )
        .as_deref(),
        Some("/root/db")
    );
    assert_eq!(
        sqlite_path(&attrs("{}"), "local", "/root/db").as_deref(),
        Some("/root/db")
    );
    assert_eq!(sqlite_path(&attrs("{}"), "other", "/root/db"), None);
}

fn driver(name: &'static str, source: &str) -> DriverFact {
    DriverFact {
        name,
        source: source.into(),
        version: "v1.5.6".into(),
        path: "/lib/x.so".into(),
    }
}

#[test]
fn drivers_matter_only_when_a_connector_needs_them() {
    let d = driver("oracle", "missing");
    assert_eq!(
        judge_driver(&d, &[], Some("main"), true).status,
        Status::Skip
    );
    assert_eq!(
        judge_driver(&d, &["ora"], Some("main"), true).status,
        Status::Warn
    );
    assert_eq!(
        judge_driver(&d, &["main"], Some("main"), true).status,
        Status::Fail
    );
    assert_eq!(
        judge_driver(&driver("oracle", "system"), &["main"], Some("main"), true).status,
        Status::Pass
    );
}

#[test]
fn duckdb_is_fetched_on_first_use_unless_installs_are_off() {
    let d = driver("duckdb", "missing");
    let auto = judge_driver(&d, &["main"], Some("main"), true);
    assert_eq!(auto.status, Status::Warn);
    assert!(auto.detail.contains("first use"));
    assert_eq!(
        judge_driver(&d, &["main"], Some("main"), false).status,
        Status::Fail
    );
    // A library that cannot be loaded is not fetched again.
    assert_eq!(
        judge_driver(&driver("duckdb", "error"), &["main"], Some("main"), true).status,
        Status::Fail
    );
}

fn manifest(json: &str) -> Manifest {
    serde_json::from_str(json).unwrap()
}

const MANIFEST: &str = r#"{
    "schema_version": 3, "meerschaum_version": "5.0.0rc1", "python_version": "3.13.1",
    "fingerprint": null,
    "plugins": {
        "api_one": {"name": "api_one", "api_plugins": ["setup_api"]},
        "dash_one": {"name": "dash_one", "dash_plugins": ["init"]},
        "page_one": {"name": "page_one", "web_pages": [{"path": "/p", "mode": "html"}]},
        "broken": {"name": "broken", "import_error": "ModuleNotFoundError: no module named 'x'\n  at line 3",
                   "venv_warnings": ["pandas"]},
        "plain": {"name": "plain", "data_functions": ["fetch"]}
    },
    "load_errors": {"all": "boom\nsecond line"}
}"#;

#[test]
fn the_manifest_says_what_plugins_need() {
    let f = plugin_facts(&manifest(MANIFEST));
    assert_eq!(f.names.len(), 5);
    assert_eq!(f.needs_api, ["api_one", "dash_one", "page_one"]);
    assert_eq!(f.needs_dash, ["dash_one"]);
    assert_eq!(
        f.import_errors,
        [(
            "broken".to_string(),
            "ModuleNotFoundError: no module named 'x'".to_string()
        )]
    );
    assert_eq!(f.venv_warnings, ["broken"]);
    assert_eq!(f.load_errors, [("all".to_string(), "boom".to_string())]);
}

#[test]
fn plugin_checks_report_import_errors_and_undeclared_packages() {
    let checks = judge_plugins(&plugin_facts(&manifest(MANIFEST)));
    let by_name = |n: &str| &checks.iter().find(|(name, _)| *name == n).unwrap().1;
    assert_eq!(by_name("installed").status, Status::Pass);
    assert_eq!(by_name("imports").status, Status::Warn);
    assert!(
        by_name("imports")
            .detail
            .contains("broken: ModuleNotFoundError")
    );
    assert_eq!(by_name("requirements").status, Status::Warn);
    let none = judge_plugins(&PluginFacts::default());
    assert_eq!(none[0].1.status, Status::Skip);
}

fn borrowed<'a>(
    v: &'a [(&'static str, Option<ModuleProbe>)],
) -> Vec<(&'static str, Option<&'a ModuleProbe>)> {
    v.iter().map(|(n, m)| (*n, m.as_ref())).collect()
}

#[test]
fn extras_warn_when_a_plugin_needs_a_missing_one() {
    let p = probe(OK_PROBE).unwrap();
    let mods = |names: &[&'static str]| -> Vec<(&'static str, Option<ModuleProbe>)> {
        names
            .iter()
            .map(|n| (*n, p.modules.get(*n).cloned()))
            .collect()
    };
    let pandas = mods(&["pandas"]);
    assert_eq!(
        judge_extra("pandas", &borrowed(&pandas), &[], "py").status,
        Status::Pass
    );
    let api = mods(&["fastapi"]);
    assert_eq!(
        judge_extra("api", &borrowed(&api), &[], "py").status,
        Status::Skip
    );
    let needed = judge_extra(
        "api",
        &borrowed(&api),
        &["my_plugin".to_string()],
        "/usr/bin/python3",
    );
    assert_eq!(needed.status, Status::Warn);
    assert!(needed.fix.unwrap().contains("meerschaum[api]"));
    // Partly installed and needed by nothing: a PASS that names what is missing.
    let partial = mods(&["pandas", "fastapi"]);
    let v = judge_extra("x", &borrowed(&partial), &[], "py");
    assert_eq!(v.status, Status::Pass);
    assert!(v.detail.contains("pandas 3.0.1") && v.detail.contains("not installed: fastapi"));
    assert!(v.fix.is_none());
    // ... and a WARN once an installed plugin needs the extra.
    assert_eq!(
        judge_extra("x", &borrowed(&partial), &["p".to_string()], "py").status,
        Status::Warn
    );
}

#[test]
fn venvs_for_another_python_are_stale() {
    let venv = |name: &str, minors: &[(u32, u32)], dangling: &[&str]| VenvFact {
        name: name.into(),
        minors: minors.to_vec(),
        dangling: dangling.iter().map(|s| s.to_string()).collect(),
    };
    assert_eq!(judge_venvs(&[], Some((3, 13))).status, Status::Skip);
    let fine = [
        venv("mrsm", &[(3, 13)], &[]),
        venv("a", &[(3, 12), (3, 13)], &[]),
    ];
    assert_eq!(judge_venvs(&fine, Some((3, 13))).status, Status::Pass);
    let old = [venv("a", &[(3, 11)], &[])];
    let v = judge_venvs(&old, Some((3, 13)));
    assert_eq!(v.status, Status::Warn);
    assert!(
        v.detail
            .contains("a (built for Python 3.11, the worker is 3.13)")
    );
    let gone = [venv("b", &[(3, 13)], &["python3"])];
    assert_eq!(judge_venvs(&gone, Some((3, 13))).status, Status::Warn);
    // Without a worker interpreter only a dangling link is known.
    assert_eq!(judge_venvs(&old, None).status, Status::Pass);
}

#[test]
fn jobs() {
    let job = |name: &str, pid: Option<i32>, process, result: Option<(bool, &str)>| JobFact {
        name: name.into(),
        pid,
        process,
        result: result.map(|(s, m)| (s, m.to_string())),
    };
    assert_eq!(judge_jobs(&[])[0].1.status, Status::Skip);
    let facts = vec![
        job("up", Some(10), Some(ProcessState::Alive), None),
        job("paused", Some(11), Some(ProcessState::Paused), None),
        job("ok", None, None, Some((true, "Success"))),
        job("bad", None, None, Some((false, "Failed to sync\nmore"))),
        job(
            "crashed",
            Some(12),
            Some(ProcessState::Gone),
            Some((false, "x")),
        ),
    ];
    let checks = judge_jobs(&facts);
    let by_name = |n: &str| checks.iter().find(|(name, _)| *name == n).map(|(_, v)| v);
    assert_eq!(by_name("state").unwrap().status, Status::Pass);
    assert!(
        by_name("state")
            .unwrap()
            .detail
            .contains("1 running, 1 paused, 3 stopped")
    );
    let failed = by_name("failed jobs").unwrap();
    assert!(failed.detail.contains("bad (Failed to sync)") && failed.detail.contains("crashed"));
    assert!(!failed.detail.contains("ok"));
    assert!(by_name("pid files").unwrap().detail.contains("crashed"));
}

#[test]
fn stack_services() {
    let text = "db\tmrsm-db-1\ttimescale/timescaledb-ha:pg18\trunning\tUp 3 hours (healthy)\n\
                api\tmrsm-api-1\tbmeares/meerschaum:api\trunning\tUp 3 hours\n\
                valkey\tmrsm-valkey-1\tvalkey/valkey:latest\texited\tExited (1) 2 minutes ago\n\
                grafana\tmrsm-grafana-1\tgrafana\trunning\tUp 1 minute (unhealthy)\n\
                junk line\n";
    let services = parse_services(text);
    assert_eq!(services.len(), 4);
    assert_eq!(services[0].image, "timescale/timescaledb-ha:pg18");
    assert_eq!(judge_service(&services[0], "mrsm").status, Status::Pass);
    assert_eq!(judge_service(&services[1], "mrsm").status, Status::Pass);
    let exited = judge_service(&services[2], "mrsm");
    assert_eq!(exited.status, Status::Fail);
    assert!(exited.fix.unwrap().contains("mrsm stack up -d valkey"));
    assert_eq!(judge_service(&services[3], "mrsm").status, Status::Fail);
    // Stopped on purpose (exit 0, or the signal that stopped it): a warning, not a failure.
    for status in [
        "Exited (0) 2 minutes ago",
        "Exited (137) 1 hour ago",
        "Exited (143) 3 days ago",
    ] {
        let stopped = ServiceFact {
            state: "exited".into(),
            status: status.into(),
            ..services[2].clone()
        };
        let v = judge_service(&stopped, "mrsm");
        assert_eq!(v.status, Status::Warn, "{status}");
        assert!(v.detail.starts_with("stopped:"));
    }
    assert_eq!(exit_code("Exited (1) 2 minutes ago"), Some(1));
    assert_eq!(exit_code("Up 3 hours"), None);
    let starting = ServiceFact {
        status: "Up 2 seconds (health: starting)".into(),
        ..services[0].clone()
    };
    assert_eq!(judge_service(&starting, "mrsm").status, Status::Warn);
    // One-shot `grafana_readonly` init container exiting 0 is healthy; non-zero still fails.
    let init_ok = ServiceFact {
        service: "grafana_readonly".into(),
        container: "mrsm-grafana_readonly-1".into(),
        state: "exited".into(),
        status: "Exited (0) 2 minutes ago".into(),
        ..services[0].clone()
    };
    let init_verdict = judge_service(&init_ok, "mrsm");
    assert_eq!(init_verdict.status, Status::Pass);
    assert!(init_verdict.detail.starts_with("completed:"));
    let init_fail = ServiceFact {
        status: "Exited (1) 2 minutes ago".into(),
        ..init_ok
    };
    assert_eq!(judge_service(&init_fail, "mrsm").status, Status::Fail);
}

#[test]
fn api_versions() {
    assert_eq!(
        judge_api_version(Some("5.0.0rc1"), "5.0.0-rc.1").status,
        Status::Pass
    );
    assert_eq!(
        judge_api_version(Some("4.1.4"), "5.0.0-rc.1").status,
        Status::Warn
    );
    assert_eq!(judge_api_version(None, "5.0.0-rc.1").status, Status::Warn);
}

fn report() -> Report {
    let mut r = Report::default();
    r.extend(vec![
        Check::warn("paths", "disk: temp", "low", "free it"),
        Check::pass("install", "meerschaum", "5.0.0"),
        Check::skip("stack", "services", "no stack file"),
        Check::fail("instance", "connection", "refused", "start it"),
    ]);
    r
}

#[test]
fn a_report_sorts_by_group_and_counts() {
    let r = report();
    let groups: Vec<&str> = r.checks.iter().map(|c| c.group).collect();
    assert_eq!(groups, ["install", "paths", "instance", "stack"]);
    let s = r.summary();
    assert_eq!((s.pass, s.warn, s.fail, s.skip), (1, 1, 1, 1));
    assert_eq!(s.text(), "1 passed, 1 warning, 1 failed, 1 skipped");
}

#[test]
fn the_tuple_fails_only_on_a_failure() {
    let t = report().tuple();
    assert!(!t.success);
    assert!(
        t.message.contains("1 check failed (instance: connection)"),
        "{}",
        t.message
    );
    let mut ok = Report::default();
    ok.extend(vec![
        Check::warn("paths", "x", "d", "f"),
        Check::pass("install", "y", "d"),
    ]);
    assert!(ok.tuple().success);
}

#[test]
fn the_json_document_has_stable_keys() {
    let mut r = report();
    r.checks[0].millis = Some(12);
    let j = r.to_json("5.0.0-rc.1");
    assert_eq!(j["version"], "5.0.0-rc.1");
    assert_eq!(j["summary"]["ok"], false);
    assert_eq!(j["summary"]["fail"], 1);
    assert_eq!(j["summary"]["warn"], 1);
    let checks = j["checks"].as_array().unwrap();
    assert_eq!(checks.len(), 4);
    for key in ["group", "name", "status", "detail", "fix"] {
        assert!(checks[0].get(key).is_some(), "{key}");
    }
    assert_eq!(checks[0]["status"], "PASS");
    assert!(checks[0]["fix"].is_null());
    assert_eq!(checks[0]["duration_ms"], 12);
    assert_eq!(checks[1]["fix"], "free it");
    assert!(checks[1].get("duration_ms").is_none());
}
