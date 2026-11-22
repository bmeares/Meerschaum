//! `show venvs` and `prune venvs` against a synthetic `venvs/` directory in a temporary root.
//! The real `~/.config/meerschaum/venvs` and `./test_root/venvs` are never read: every test
//! points `MRSM_ROOT_DIR` at its own temporary directory with an empty plugins directory.

use std::path::Path;

use meerschaum_cli::dispatch::{self, RecordingHost, Runtime};
use meerschaum_cli::format::FormatSettings;
use meerschaum_cli::types::DateContext;
use meerschaum_core::SuccessTuple;
use meerschaum_core::config::{Config, ConfigOptions};

fn make_dist(site: &Path, name: &str, version: &str) {
    let dir = site.join(format!("{name}-{version}.dist-info"));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("METADATA"),
        format!("Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n"),
    )
    .unwrap();
    std::fs::write(site.join(format!("{name}.py")), "x = 1\n").unwrap();
}

/// A root with a `mrsm` venv holding `pandas` (requested by an installed-or-not plugin).
fn synthetic_root() -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(tmp.path().join("plugins")).unwrap();
    let venv = tmp.path().join("venvs").join("mrsm");
    let site = venv.join("lib").join("python3.11").join("site-packages");
    std::fs::create_dir_all(&site).unwrap();
    make_dist(&site, "pypdf", "4.0.0");
    make_dist(&site, "requests", "2.31.0");
    make_dist(&site, "legacy", "1.0");
    std::fs::write(
        venv.join(".mrsm_requests.json"),
        serde_json::json!({
            "schema_version": 1,
            "packages": {
                "pypdf": {"requesters": {"removed_plugin": {
                    "spec": "pypdf", "first_requested": "t", "last_requested": "t"}}},
                "requests": {"requesters": {"meerschaum": {
                    "spec": "requests", "first_requested": "t", "last_requested": "t"}}},
            }
        })
        .to_string(),
    )
    .unwrap();
    tmp
}

fn run(root: &Path, sysargs: &[&str], input: &[&str]) -> (SuccessTuple, String) {
    let mut out = Vec::new();
    let result = {
        let mut rt = Runtime::new(DateContext::system(), 80, &mut out, |_| None);
        let mut opts = ConfigOptions::with_env(vec![
            ("MRSM_ROOT_DIR".into(), root.to_string_lossy().into_owned()),
            (
                "MRSM_PLUGINS_DIR".into(),
                root.join("plugins").to_string_lossy().into_owned(),
            ),
        ]);
        opts.allow_writes = true;
        opts.home = root.to_path_buf();
        rt.config = Some(Config::new(opts).unwrap());
        rt.format = Some(FormatSettings {
            ansi: false,
            unicode: true,
            width: 80,
            cols: 80,
            no_color: false,
        });
        rt.host = Box::new(RecordingHost {
            input: input.iter().map(|s| s.to_string()).collect(),
            ..RecordingHost::default()
        });
        rt.cwd = Some(root.to_path_buf());
        let sysargs: Vec<String> = sysargs.iter().map(|s| s.to_string()).collect();
        dispatch::entry(&sysargs, &mut rt)
    };
    (result, String::from_utf8(out).unwrap())
}

#[test]
fn show_venvs_lists_packages_with_requesters() {
    let root = synthetic_root();
    let (result, out) = run(root.path(), &["show", "venvs"], &[]);
    assert!(result.success, "{result:?}");
    assert!(out.contains("mrsm"), "{out}");
    assert!(out.contains("Size:"), "{out}");
    assert!(out.contains("Packages: 3"), "{out}");
    assert!(out.contains("pypdf 4.0.0"), "{out}");
    assert!(out.contains("[removed_plugin]"), "{out}");
    assert!(out.contains("[meerschaum]"), "{out}");
    assert!(out.contains("[unattributed]"), "{out}");

    let (_, plain) = run(root.path(), &["show", "venvs", "--nopretty"], &[]);
    assert_eq!(plain, "mrsm\n");
}

#[test]
fn prune_venvs_dry_run_lists_without_removing() {
    let root = synthetic_root();
    let (result, out) = run(root.path(), &["prune", "venvs", "--dry"], &[]);
    assert!(result.success, "{result:?}");
    assert!(
        result.message.contains("Would remove 1 package"),
        "{result:?}"
    );
    assert!(
        out.contains("pypdf 4.0.0 (requested by: removed_plugin)"),
        "{out}"
    );
    assert!(!out.contains("requests 2.31.0"), "{out}");
    assert!(
        root.path()
            .join("venvs/mrsm/lib/python3.11/site-packages/pypdf-4.0.0.dist-info")
            .exists()
    );
}

#[test]
fn prune_venvs_asks_first_and_never_removes_without_consent() {
    let root = synthetic_root();
    let site = root.path().join("venvs/mrsm/lib/python3.11/site-packages");
    let (result, _) = run(root.path(), &["prune", "venvs", "--noask"], &[]);
    assert!(!result.success);
    assert!(result.message.contains("Skipping prune"), "{result:?}");
    let (result, out) = run(root.path(), &["prune", "venvs"], &["n"]);
    assert!(!result.success);
    assert!(result.message.contains("Nothing was pruned"), "{result:?}");
    assert!(out.contains("pypdf 4.0.0"), "{out}");
    assert!(site.join("pypdf-4.0.0.dist-info").exists());
    assert!(
        root.path().join("venvs/mrsm/.mrsm_requests.json").exists(),
        "the ledger is untouched"
    );
}

#[test]
fn prune_venvs_with_nothing_to_remove_succeeds() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(root.path().join("plugins")).unwrap();
    std::fs::create_dir_all(root.path().join("venvs")).unwrap();
    let (result, _) = run(root.path(), &["prune", "venvs", "--dry"], &[]);
    assert!(result.success);
    assert!(
        result.message.starts_with("No packages to prune."),
        "{result:?}"
    );
}
