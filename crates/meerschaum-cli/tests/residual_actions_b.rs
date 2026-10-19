//! Wave 15 (N2): the plugin, package and venv actions that used to fall through to the Python
//! worker's `core_action`: `uninstall plugins|packages`, `delete plugins|venvs`, `edit plugins`,
//! `show packages`, `verify packages|plugins|venvs`, `bootstrap plugins`, `start worker`,
//! `upgrade meerschaum|packages|plugins` and `register plugins`.
//!
//! Every test uses a temporary root, plugins directory and offline installer (a local wheel), and
//! a plugin repository on `127.0.0.1` where one is needed. The expected messages are the ones
//! released v4.1.4 prints for the same inputs (compared by hand against `.venv-ref`, see
//! `crates/meerschaum-py/PARITY.md`, "Wave 15 (task N2)"). The forms that need no Python plugin
//! also run with `MRSM_PYTHON` pointing at nothing: they must never start the worker.

mod plugin_lifecycle_support;

use plugin_lifecycle_support::{Home, Repo, archive, install_native_demo};

const NO_PYTHON: (&str, &str) = ("MRSM_PYTHON", "/nonexistent/mrsm-test-python");
/// `edit_file` must not open a real editor.
const NO_EDITOR: (&str, &str) = ("EDITOR", "true");

fn flat(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn installer_available() -> bool {
    let has = |program: &str, args: &[&str]| {
        std::process::Command::new(program)
            .args(args)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .is_ok_and(|s| s.success())
    };
    has("uv", &["--version"]) || has("python3", &["-m", "pip", "--version"])
}

fn python_available() -> bool {
    std::process::Command::new("python3")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
}

fn with_repo(
    home: &Home,
    repo: &Repo,
    args: &[&str],
    extra: &[(&str, &str)],
) -> plugin_lifecycle_support::Run {
    let (key, value) = repo.connector("loop");
    let mut full: Vec<&str> = args.to_vec();
    full.extend(["-r", "api:loop"]);
    let mut env: Vec<(&str, &str)> = vec![(key.as_str(), value.as_str())];
    env.extend_from_slice(extra);
    home.run_with(&full, &env)
}

// --- show packages -----------------------------------------------------------------------------

#[test]
fn show_packages_prints_a_group_without_python() {
    let home = Home::new();
    let run = home.run_with(&["show", "packages", "api", "--nopretty"], &[NO_PYTHON]);
    assert!(run.success, "{}", run.text());
    let text = run.text();
    assert!(text.lines().any(|l| l.starts_with("fastapi>=")), "{text}");
    assert!(!text.contains("Python worker"), "{text}");

    // The default group is `full`; the pretty form is a colored dictionary.
    let run = home.run_with(&["show", "packages"], &[NO_PYTHON]);
    let text = run.text();
    assert!(run.success, "{text}");
    assert!(text.trim_start().starts_with('{'), "{text}");
    assert!(text.contains("'fastapi': 'fastapi>="), "{text}");

    // An unknown group warns and still succeeds (v4 returns `True, "Success"`).
    let run = home.run_with(&["show", "packages", "bogus"], &[NO_PYTHON]);
    assert!(run.success);
    assert!(
        run.text().contains("'bogus' is not a valid group."),
        "{}",
        run.text()
    );
}

// --- start worker ------------------------------------------------------------------------------

#[test]
fn start_worker_checks_its_index_like_v4() {
    let home = Home::new();
    let run = home.run_with(&["start", "worker"], &[NO_PYTHON]);
    assert!(!run.success);
    assert!(
        run.text().contains("No worker index is provided."),
        "{}",
        run.text()
    );
    let run = home.run_with(&["start", "worker", "abc"], &[NO_PYTHON]);
    assert!(!run.success);
    assert!(
        run.text().contains("Invalid worker index."),
        "{}",
        run.text()
    );
    let run = home.run_with(&["start", "worker", "1"], &[NO_PYTHON]);
    assert!(!run.success);
    assert!(
        run.text().contains("not used in Meerschaum v5"),
        "{}",
        run.text()
    );
}

// --- the parent actions without a subaction ----------------------------------------------------

#[test]
fn parent_actions_list_their_subactions_without_python() {
    let home = Home::new();
    for (parent, listed) in [
        ("uninstall", "uninstall packages"),
        ("verify", "verify venvs"),
        ("upgrade", "upgrade mrsm"),
    ] {
        let run = home.run_with(&[parent], &[NO_PYTHON]);
        let text = run.text();
        assert!(!run.success, "{text}");
        assert!(
            text.contains(&format!("Cannot {parent} ''. Choose one:")),
            "{text}"
        );
        assert!(text.contains(listed), "{text}");
        assert!(text.contains("Invalid choice ''."), "{text}");
    }
}

// --- uninstall / verify / delete venvs ---------------------------------------------------------

#[test]
fn uninstall_plugins_removes_the_files_the_venv_and_the_origin() {
    let home = Home::new();
    home.write_plugin("foo.py", "__version__ = '1.0.0'\n");
    home.write_plugin("keep.py", "x = 1\n");
    std::fs::write(
        home.plugins.join(".mrsm_origins.json"),
        "{\n    \"foo\": \"api:loop\",\n    \"keep\": \"api:loop\"\n}",
    )
    .unwrap();
    std::fs::create_dir_all(home.root.join("venvs/foo/lib")).unwrap();

    // No names, then a name nobody installed.
    let run = home.run(&["uninstall", "plugins"]);
    assert!(!run.success);
    assert!(
        run.text().contains("No plugins to uninstall."),
        "{}",
        run.text()
    );
    let run = home.run(&["uninstall", "plugins", "nope"]);
    let text = flat(&run.text());
    assert!(!run.success, "{text}");
    assert!(
        text.contains("Plugin 'nope' is not installed. Add `--force` to try anyway."),
        "{text}"
    );

    // A refusal keeps everything.
    let run = home.run_input(&["uninstall", "plugins", "foo"], "n\n", &[]);
    let text = flat(&run.text());
    assert!(!run.success, "{text}");
    assert!(
        text.contains("Are you sure you want to remove these plugins? - foo [y/N]"),
        "{text}"
    );
    assert!(text.contains("No plugins were uninstalled."), "{text}");
    assert!(home.plugins.join("foo.py").exists());

    // `--yes` removes the source, the venv and the origin of that plugin only.
    let run = home.run(&["uninstall", "plugins", "foo", "--yes"]);
    let text = flat(&run.text());
    assert!(run.success, "{text}");
    assert!(
        text.contains("Removed source files for plugin 'foo'."),
        "{text}"
    );
    assert!(
        text.contains("Removed virtual environment from plugin 'foo'."),
        "{text}"
    );
    assert!(
        text.contains("Done uninstalling 1 plugins (1 succeeded, 0 failed)."),
        "{text}"
    );
    assert!(!home.plugins.join("foo.py").exists());
    assert!(!home.root.join("venvs/foo").exists());
    assert!(home.plugins.join("keep.py").exists());
    let origins = std::fs::read_to_string(home.plugins.join(".mrsm_origins.json")).unwrap();
    assert!(
        !origins.contains("foo") && origins.contains("keep"),
        "{origins}"
    );

    // `--force` tries a plugin that is not installed and still cleans its artifacts.
    std::fs::create_dir_all(home.root.join("venvs/ghost/lib")).unwrap();
    let run = home.run(&["uninstall", "plugins", "ghost", "--force"]);
    let text = flat(&run.text());
    assert!(run.success, "{text}");
    assert!(
        text.contains("Plugin 'ghost' doesn't seem to be installed. Checking for artifacts..."),
        "{text}"
    );
    assert!(!home.root.join("venvs/ghost").exists());
}

#[test]
fn uninstall_plugins_removes_a_native_plugin_without_python() {
    let home = Home::new();
    install_native_demo(&home.plugins);
    let run = home.run_with(&["uninstall", "plugins", "native_demo", "-y"], &[NO_PYTHON]);
    let text = flat(&run.text());
    assert!(run.success, "{text}");
    assert!(
        text.contains("Done uninstalling 1 plugins (1 succeeded, 0 failed)."),
        "{text}"
    );
    assert!(!home.plugins.join("native_demo").exists());
}

#[test]
fn delete_and_verify_venvs() {
    let home = Home::new();
    let run = home.run_with(&["delete", "venvs"], &[NO_PYTHON]);
    assert!(run.success, "{}", run.text());
    assert!(run.text().contains("No venvs to delete."), "{}", run.text());
    let run = home.run_with(&["delete", "venvs", "abc", "-y"], &[NO_PYTHON]);
    assert!(!run.success);
    assert!(run.text().contains("No venvs to delete."), "{}", run.text());

    std::fs::create_dir_all(home.root.join("venvs/abc/lib/python3.13/site-packages")).unwrap();
    std::fs::create_dir_all(home.root.join("venvs/def/lib/python3.13/site-packages")).unwrap();
    let run = home.run_with(&["verify", "venvs"], &[NO_PYTHON]);
    assert!(run.success, "{}", run.text());

    let run = home.run_input(&["delete", "venvs", "abc"], "n\n", &[]);
    let text = flat(&run.text());
    assert!(!run.success, "{text}");
    assert!(home.root.join("venvs/abc").exists());
    let run = home.run_with(&["delete", "venvs", "abc", "-y"], &[NO_PYTHON]);
    let text = flat(&run.text());
    assert!(run.success, "{text}");
    assert!(!home.root.join("venvs/abc").exists());
    assert!(home.root.join("venvs/def").exists());
}

#[test]
fn install_verify_and_uninstall_packages_in_a_temporary_venv() {
    if !installer_available() || !python_available() {
        return;
    }
    let home = Home::new();
    let run = home.run(&["install", "packages", "mrsm_syn_pkg", "--venv", "synthetic"]);
    assert!(run.success, "{}", run.text());
    assert!(home.has_package("synthetic", "mrsm_syn_pkg"));

    // A refusal under `--noask` without `--yes`.
    let run = home.run(&[
        "uninstall",
        "packages",
        "mrsm_syn_pkg",
        "--venv",
        "synthetic",
    ]);
    let text = flat(&run.text());
    assert!(!run.success, "{text}");
    assert!(
        text.contains(
            "Skipping uninstallation. Add `-y` or `-f` to agree to the uninstall prompt."
        ),
        "{text}"
    );
    assert!(home.has_package("synthetic", "mrsm_syn_pkg"));

    let run = home.run(&[
        "uninstall",
        "packages",
        "mrsm_syn_pkg",
        "--venv",
        "synthetic",
        "--yes",
    ]);
    let text = flat(&run.text());
    assert!(run.success, "{text}");
    assert!(
        text.contains(
            "Successfully removed packages from virtual environment 'synthetic': - mrsm_syn_pkg"
        ) || text.contains("Successfully removed packages from virtual environment 'synthetic':"),
        "{text}"
    );
    assert!(!home.has_package("synthetic", "mrsm_syn_pkg"));

    let run = home.run(&["uninstall", "packages"]);
    assert!(!run.success);
    assert!(
        run.text().contains("No packages to uninstall."),
        "{}",
        run.text()
    );
}

#[test]
fn verify_packages_counts_what_it_checked_and_changes_nothing_offline() {
    if !python_available() {
        return;
    }
    let home = Home::new();
    let run = home.run(&["verify", "packages"]);
    let text = flat(&run.text());
    assert!(run.success, "{text}");
    assert!(text.contains("Verified "), "{text}");
    assert!(text.contains(" packages."), "{text}");
    // Nothing was installed into the venv by a verification with nothing in it.
    assert!(home.site_packages("mrsm").is_none_or(|s| {
        std::fs::read_dir(s)
            .map(|d| d.flatten().count() == 0)
            .unwrap_or(true)
    }));
}

// --- upgrade -----------------------------------------------------------------------------------

#[test]
fn upgrade_packages_rejects_an_unknown_group_and_honors_a_refusal() {
    let home = Home::new();
    let run = home.run_with(&["upgrade", "packages", "bogus"], &[NO_PYTHON]);
    let text = flat(&run.text());
    assert!(!run.success, "{text}");
    assert!(text.contains("Invalid dependency group 'bogus'."), "{text}");
    assert!(text.contains("Available groups:"), "{text}");
    assert!(text.contains("- required"), "{text}");
    assert!(text.contains("Invalid dependency group 'bogus'."), "{text}");

    // The group is listed and the question refused: nothing is installed.
    let run = home.run_input(&["upgrade", "packages", "required"], "n\n", &[NO_PYTHON]);
    let text = flat(&run.text());
    assert!(!run.success, "{text}");
    assert!(text.contains("Packages to Upgrade:"), "{text}");
    assert!(
        text.contains("dependency group 'required')? [Y/n]"),
        "{text}"
    );
    assert!(text.contains("Nothing installed."), "{text}");
}

#[test]
fn upgrade_plugins_reinstalls_from_the_repository() {
    if !python_available() || !installer_available() {
        return;
    }
    let home = Home::new();
    let repo = Repo::start();
    let run = home.run_with(&["upgrade", "plugins"], &[NO_PYTHON]);
    assert!(!run.success);
    assert!(
        run.text().contains("No plugins to upgrade."),
        "{}",
        run.text()
    );

    repo.publish(
        "foo",
        archive(&[(
            "foo.py",
            "__version__ = '2.0.0'\ndef setup(**kw):\n    return True, 'foo ready'\n",
        )]),
    );
    home.write_plugin("foo.py", "__version__ = '1.0.0'\n");
    let run = with_repo(&home, &repo, &["upgrade", "plugins", "foo", "--yes"], &[]);
    let text = flat(&run.text());
    assert!(run.success, "{text}");
    assert!(text.contains("Plugins to Upgrade:"), "{text}");
    assert!(
        text.contains("Successfully installed plugin 'foo'."),
        "{text}"
    );
    let installed = std::fs::read_to_string(home.plugins.join("foo.py")).unwrap();
    assert!(
        installed.starts_with("__version__ = '2.0.0'"),
        "{installed}"
    );

    // A refusal changes nothing.
    repo.publish("foo", archive(&[("foo.py", "__version__ = '3.0.0'\n")]));
    let run = home.run_input(
        &["upgrade", "plugins", "foo", "-r", "api:loop"],
        "n\n",
        &[(
            repo.connector("loop").0.as_str(),
            repo.connector("loop").1.as_str(),
        )],
    );
    let text = flat(&run.text());
    assert!(!run.success, "{text}");
    assert!(text.contains("Upgrade 1 plugins? [Y/n]"), "{text}");
    assert!(text.contains("No plugins upgraded."), "{text}");
}

/// A scratch virtual environment under the home's root (`None` when `venv` is unavailable).
fn scratch_venv(home: &Home, name: &str) -> Option<std::path::PathBuf> {
    let venv = home.root.join(name);
    let status = std::process::Command::new("python3")
        .args(["-I", "-m", "venv"])
        .arg(&venv)
        .env_remove("PYTHONPATH")
        .env_remove("PYTHONHOME")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .ok()?;
    status.success().then(|| venv.join("bin/python"))
}

/// The installed version in the scratch interpreter, excluding source-tree / user metadata.
fn dist_version(python: &std::path::Path, dist: &str) -> Option<String> {
    let output = std::process::Command::new(python)
        .args([
            "-I",
            "-c",
            &format!("import importlib.metadata as m; print(m.version('{dist}'))"),
        ])
        .env_remove("PYTHONPATH")
        .env_remove("PYTHONHOME")
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// A `PATH` whose first entry holds a `docker` that always fails: `upgrade meerschaum` must not
/// pull or start anything on the machine running the tests.
fn path_without_docker(home: &Home) -> String {
    use std::os::unix::fs::PermissionsExt;
    let bin = home.root.join("fakebin");
    std::fs::create_dir_all(&bin).unwrap();
    let docker = bin.join("docker");
    std::fs::write(&docker, "#!/bin/sh\nexit 1\n").unwrap();
    std::fs::set_permissions(&docker, std::fs::Permissions::from_mode(0o755)).unwrap();
    format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    )
}

#[test]
fn upgrade_meerschaum_never_guesses_an_interpreter() {
    if !python_available() || !installer_available() {
        return;
    }
    let home = Home::new();
    let path = path_without_docker(&home);

    // No `MRSM_PYTHON` and no interpreter next to the binary (`target/debug/mrsm`): refused, and
    // the `python3` on `PATH` is never touched.
    let run = home.run_with(&["upgrade", "meerschaum", "--yes"], &[("PATH", &path)]);
    let text = flat(&run.text());
    assert!(!run.success, "{text}");
    assert!(
        text.contains("Cannot tell which Python environment Meerschaum is installed in"),
        "{text}"
    );

    // An interpreter without `meerschaum` is refused too.
    let Some(python) = scratch_venv(&home, "bare-venv") else {
        return;
    };
    let run = home.run_with(
        &["upgrade", "meerschaum", "--yes"],
        &[("MRSM_PYTHON", python.to_str().unwrap()), ("PATH", &path)],
    );
    let text = flat(&run.text());
    assert!(!run.success, "{text}");
    assert!(
        text.contains("Meerschaum is not installed in the Python environment"),
        "{text}"
    );
    assert!(dist_version(&python, "meerschaum").is_none());
}

#[test]
fn upgrade_meerschaum_ignores_source_metadata_in_the_working_directory() {
    if !python_available() {
        return;
    }
    let home = Home::new();
    let path = path_without_docker(&home);
    let Some(python) = scratch_venv(&home, "cwd-bare-venv") else {
        return;
    };
    assert!(dist_version(&python, "meerschaum").is_none());
    let cwd = home.root.join("foreign-cwd");
    let metadata = cwd.join("meerschaum.egg-info");
    std::fs::create_dir_all(&metadata).unwrap();
    std::fs::write(
        metadata.join("PKG-INFO"),
        "Metadata-Version: 2.1\nName: meerschaum\nVersion: 99.0.0\n",
    )
    .unwrap();
    let mut env = home.env();
    env.retain(|(key, _)| key != "PATH" && key != "MRSM_PYTHON");
    env.push(("PATH".into(), path));
    env.push(("MRSM_PYTHON".into(), python.display().to_string()));
    // Home owns this cwd and all metadata. run_binary explicitly sets current_dir(cwd),
    // so ignoring PYTHONPATH alone cannot make this refusal pass.
    let run =
        plugin_lifecycle_support::run_binary(&["upgrade", "meerschaum", "--yes"], &env, Some(&cwd));
    let text = flat(&run.text());
    assert!(!run.success, "{text}");
    assert!(
        text.contains("Meerschaum is not installed in the Python environment"),
        "{text}"
    );
    assert!(dist_version(&python, "meerschaum").is_none());
}

#[test]
fn upgrade_meerschaum_upgrades_the_environment_that_has_it_and_adds_no_system_flag() {
    if !python_available() || !installer_available() {
        return;
    }
    let home = Home::new();
    let path = path_without_docker(&home);
    let Some(python) = scratch_venv(&home, "mrsm-venv") else {
        return;
    };
    // Version 1.0.0 is installed; 2.0.0 is offered by the offline wheel directory.
    plugin_lifecycle_support::write_wheel(&home.wheels, "meerschaum", "1.0.0");
    let installed = std::process::Command::new(&python)
        .args([
            "-I",
            "-m",
            "pip",
            "install",
            "--no-index",
            "--find-links",
            home.wheels.to_str().unwrap(),
            "meerschaum==1.0.0",
        ])
        .env_remove("PYTHONPATH")
        .env_remove("PYTHONHOME")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if !installed {
        // `venv` without `ensurepip` has no pip to seed the fixture with.
        eprintln!("skipped: no pip in the scratch venv");
        return;
    }
    plugin_lifecycle_support::write_wheel(&home.wheels, "meerschaum", "2.0.0");
    assert_eq!(
        dist_version(&python, "meerschaum").as_deref(),
        Some("1.0.0")
    );

    let run = home.run_with(
        &["upgrade", "meerschaum", "--yes", "--debug"],
        &[("MRSM_PYTHON", python.to_str().unwrap()), ("PATH", &path)],
    );
    let text = flat(&run.text());
    assert!(!text.contains("--break-system-packages"), "{text}");
    assert!(
        !text.contains("Failed to upgrade Meerschaum via pip."),
        "{text}"
    );
    assert_eq!(
        dist_version(&python, "meerschaum").as_deref(),
        Some("2.0.0"),
        "{text}"
    );

    // An installer that cannot find the requested version fails with v4's message, and the
    // user's own arguments are passed through (nothing is added behind their back).
    let run = home.run_with(
        &[
            "upgrade",
            "meerschaum",
            "nonexistent-extra",
            "--yes",
            "--debug",
        ],
        &[("MRSM_PYTHON", python.to_str().unwrap()), ("PATH", &path)],
    );
    let text = flat(&run.text());
    assert!(!text.contains("--break-system-packages"), "{text}");
}

// --- repository actions ------------------------------------------------------------------------

#[test]
fn delete_plugins_asks_then_deletes_from_the_repository() {
    let home = Home::new();
    let repo = Repo::start();

    let (key, value) = repo.connector("loop");
    let run = home.run_input(
        &["delete", "plugins", "foo", "-r", "api:loop"],
        "n\n",
        &[(key.as_str(), value.as_str())],
    );
    let text = flat(&run.text());
    assert!(!run.success, "{text}");
    assert!(
        text.contains(
            "Are you sure you want to delete the following plugins from the repository 'api:loop'? THIS CANNOT BE UNDONE! - foo [y/N]"
        ),
        "{text}"
    );
    assert!(text.contains("No plugins deleted."), "{text}");
    assert!(repo.deleted().is_empty());

    let run = with_repo(
        &home,
        &repo,
        &["delete", "plugins", "foo", "bar", "--yes"],
        &[NO_PYTHON],
    );
    let text = flat(&run.text());
    assert!(run.success, "{text}");
    assert!(
        text.contains("Deleting plugin 'foo' from Meerschaum repository 'api:loop'..."),
        "{text}"
    );
    assert!(
        text.contains("Deleting plugin 'bar' from Meerschaum repository 'api:loop'..."),
        "{text}"
    );
    assert_eq!(repo.deleted(), vec!["foo".to_string(), "bar".to_string()]);
}

#[test]
fn register_plugins_uploads_the_archive_with_its_description() {
    if !python_available() {
        return;
    }
    let home = Home::new();
    let repo = Repo::start();
    home.write_plugin(
        "foo.py",
        "\"\"\"A synthetic plugin.\"\"\"\n__version__ = '1.2.3'\n",
    );
    repo.set_attributes("foo", "{}");
    let run = with_repo(&home, &repo, &["register", "plugins", "foo"], &[NO_PYTHON]);
    let text = flat(&run.text());
    assert!(run.success, "{text}");
    assert!(
        text.contains("Registering plugin 'foo' to Meerschaum API 'api:loop'..."),
        "{text}"
    );
    assert!(
        text.contains("Finished registering 1 plugins (1 succeeded, 0 failed)."),
        "{text}"
    );
    let upload = repo.upload("foo").expect("the plugin was uploaded");
    let body = String::from_utf8_lossy(&upload).into_owned();
    assert!(body.contains("POST /plugins/foo"), "{body}");
    assert!(body.contains("1.2.3"), "{body}");
    assert!(body.contains("A+synthetic+plugin."), "{body}");
    // The body is a gzip archive.
    assert!(upload.windows(2).any(|w| w == [0x1f, 0x8b]));

    // A plugin nobody has installed is skipped, as in v4.
    let run = with_repo(&home, &repo, &["register", "plugins", "nope"], &[NO_PYTHON]);
    let text = flat(&run.text());
    assert!(!run.success, "{text}");
    assert!(
        text.contains("Plugin 'nope' is not installed and cannot be registered. Ignoring..."),
        "{text}"
    );
}

// --- verify plugins / edit plugins / bootstrap plugins -----------------------------------------

#[test]
fn verify_plugins_imports_each_plugin_in_the_worker() {
    if !python_available() {
        return;
    }
    let home = Home::new();
    let run = home.run(&["verify", "plugins"]);
    assert!(run.success, "{}", run.text());
    assert!(
        run.text()
            .contains("There are no installed plugins; nothing to do."),
        "{}",
        run.text()
    );

    home.write_plugin("good.py", "x = 1\n");
    let run = home.run(&["verify", "plugins", "good"]);
    let text = flat(&run.text());
    assert!(run.success, "{text}");
    assert!(text.contains("Verifying 1 plugins"), "{text}");
    assert!(text.contains("Imported plugin 'good'."), "{text}");
    assert!(text.contains("Successfully imported 1 plugins."), "{text}");

    home.write_plugin("bad.py", "import zz_nonexistent_module_zz\n");
    let run = home.run(&["verify", "plugins"]);
    let text = flat(&run.text());
    assert!(!run.success, "{text}");
    assert!(text.contains("Imported plugin 'good'."), "{text}");
    assert!(text.contains("Failed to import plugin 'bad'."), "{text}");
    assert!(text.contains("Failed to import plugin 'bad'."), "{text}");
}

#[test]
fn edit_plugins_opens_the_editor_for_an_installed_plugin_only() {
    if !python_available() {
        return;
    }
    let home = Home::new();
    let run = home.run_with(&["edit", "plugins"], &[NO_PYTHON, NO_EDITOR]);
    assert!(!run.success);
    assert!(
        run.text().contains("Specify which plugin to edit."),
        "{}",
        run.text()
    );

    home.write_plugin("foo.py", "x = 1\n");
    let receipt = home.root.join("editor-argv.json");
    let receipt_path = receipt.to_string_lossy().into_owned();
    // Record the actual argument, independent of wrapping a long scratch path in the prompt.
    // The editor is an argument vector, never a shell; Python writes only this owned receipt.
    let editor = r#"python3 -I -c 'import json, os, pathlib, sys; pathlib.Path(os.environ["MRSM_TEST_EDITOR_RECEIPT"]).write_text(json.dumps(sys.argv[1:]))'"#;
    let editor_env = [
        ("EDITOR", editor),
        ("MRSM_TEST_EDITOR_RECEIPT", receipt_path.as_str()),
        // This dependency-free plugin needs no pip seed packages in the offline scratch venv.
        ("MRSM_NO_AUTO_INSTALL", "1"),
        ("COLUMNS", "40"),
    ];
    let run = home.run_input(&["edit", "plugins", "foo"], "\n", &editor_env);
    let text = flat(&run.text());
    assert!(run.success, "{text}");
    assert!(text.contains("Press [Enter] to open"), "{text}");
    assert!(text.contains("Reloaded 1 plugin"), "{text}");
    let recorded: Vec<String> = serde_json::from_slice(&std::fs::read(&receipt).unwrap()).unwrap();
    let expected = home.plugins.join("foo.py").canonicalize().unwrap();
    assert_eq!(
        recorded,
        vec![expected.to_string_lossy().into_owned()],
        "{text}"
    );

    // An unknown plugin offers to create one; a refusal must not invoke the editor.
    std::fs::remove_file(&receipt).unwrap();
    let run = home.run_input(&["edit", "plugins", "unknown"], "n\n", &editor_env);
    let text = flat(&run.text());
    assert!(!run.success, "{text}");
    assert!(
        text.contains("Plugin 'unknown' is not installed."),
        "{text}"
    );
    assert!(
        text.contains("Would you like to create a new plugin 'unknown'?"),
        "{text}"
    );
    assert!(text.contains("Plugin 'unknown' does not exist."), "{text}");
    assert!(
        !receipt.exists(),
        "a refused unknown plugin invoked the editor"
    );
}

#[test]
fn bootstrap_plugins_writes_the_scaffold_from_v4s_templates() {
    let home = Home::new();
    // Feature 1 (fetch), then the editor prompt is answered with Enter.
    let run = home.run_input(
        &["bootstrap", "plugins", "scaffolded"],
        "1\n\n",
        &[NO_EDITOR, NO_PYTHON],
    );
    let text = flat(&run.text());
    assert!(run.success, "{text}");
    assert!(
        text.contains("Which of the following features would you like to add to your plugin?"),
        "{text}"
    );
    assert!(text.contains("Successfully created file"), "{text}");
    let source = std::fs::read_to_string(home.plugins.join("scaffolded.py")).unwrap();
    assert!(source.contains("__version__"), "{source}");
    assert!(source.contains("def fetch("), "{source}");

    // An invalid choice asks again.
    let run = home.run_input(
        &["bootstrap", "plugins", "second"],
        "zzz\n1\n\n",
        &[NO_EDITOR, NO_PYTHON],
    );
    let text = flat(&run.text());
    assert!(run.success, "{text}");
    assert!(text.contains("Please pick a valid choice."), "{text}");
    assert!(home.plugins.join("second.py").exists());
}

// --- plugin names that would name a path -------------------------------------------------------

#[test]
fn plugin_names_that_name_a_path_are_refused_by_every_action() {
    let home = Home::new();
    let canary = home.root.join("canary");
    std::fs::create_dir_all(&canary).unwrap();
    std::fs::write(canary.join("keep.txt"), "x").unwrap();
    let shared = home.root.join("venvs/mrsm");
    std::fs::create_dir_all(&shared).unwrap();
    let outside = home.root.parent().unwrap().join("outside-plugin.py");

    // `venvs/../canary` is the root's own `canary` directory.
    for name in [
        "../canary",
        "..",
        "../../x",
        "a/b",
        "a\\b",
        ".hidden",
        "mrsm",
    ] {
        for action in [
            vec!["uninstall", "plugins", name, "--force", "--yes"],
            vec!["delete", "plugins", name, "--yes"],
            vec!["install", "plugins", name, "--force"],
            vec!["setup", "plugins", name],
            vec!["edit", "plugins", name, "--yes"],
        ] {
            let run = home.run_with(&action, &[NO_PYTHON, NO_EDITOR]);
            let text = flat(&run.text());
            assert!(!run.success, "{action:?}: {text}");
            assert!(
                text.contains(&format!("Invalid plugin name: '{name}'")),
                "{action:?}: {text}"
            );
        }
        let run = home.run_input(
            &["bootstrap", "plugins", name],
            "\n",
            &[NO_PYTHON, NO_EDITOR],
        );
        let text = flat(&run.text());
        assert!(!run.success, "bootstrap {name}: {text}");
        assert!(
            text.contains(&format!("Invalid plugin name: '{name}'")),
            "bootstrap {name}: {text}"
        );
    }
    assert!(
        canary.join("keep.txt").exists(),
        "the root's directory survived"
    );
    assert!(shared.exists(), "the shared `mrsm` environment survived");
    assert!(!outside.exists());
}

// --- the editor --------------------------------------------------------------------------------

#[test]
fn an_editor_that_cannot_start_is_an_error_everywhere() {
    let home = Home::new();
    home.write_plugin("foo.py", "x = 1\n");
    let bad = ("EDITOR", "/nonexistent/mrsm-test-editor");

    let run = home.run_input(&["edit", "plugins", "foo"], "\n", &[bad, NO_PYTHON]);
    let text = flat(&run.text());
    assert!(!run.success, "{text}");
    assert!(
        text.contains("Unable to open editor '/nonexistent/mrsm-test-editor'"),
        "{text}"
    );

    let run = home.run_input(
        &["bootstrap", "plugins", "fresh"],
        "1\n\n\n",
        &[bad, NO_PYTHON],
    );
    let text = flat(&run.text());
    assert!(!run.success, "{text}");
    assert!(text.contains("Unable to open editor"), "{text}");

    let run = home.run_with(&["edit", "config", "system"], &[bad, NO_PYTHON]);
    let text = flat(&run.text());
    assert!(!run.success, "{text}");
    assert!(text.contains("Unable to open editor"), "{text}");

    // `$VISUAL` wins over `$EDITOR`, and the command is split into words without a shell.
    let run = home.run_input(
        &["edit", "plugins", "foo"],
        "\n",
        &[bad, ("VISUAL", "true --"), NO_PYTHON],
    );
    assert!(run.success, "{}", run.text());
    let run = home.run_input(
        &["edit", "plugins", "foo"],
        "\n",
        &[("VISUAL", "true; touch pwned"), NO_PYTHON],
    );
    assert!(!home.root.join("pwned").exists());
    assert!(!std::path::Path::new("pwned").exists(), "{}", run.text());
}

#[test]
fn creating_a_plugin_from_edit_plugins_reports_the_wizard_failure() {
    let home = Home::new();
    let bad = ("EDITOR", "/nonexistent/mrsm-test-editor");
    // `y` creates it, `1` picks the fetch feature, the next two lines answer the column and the
    // "open it" prompt; the editor cannot be started, which is the action's result.
    let run = home.run_input(
        &["edit", "plugins", "brandnew"],
        "y\n1\n\n\n",
        &[bad, NO_PYTHON],
    );
    let text = flat(&run.text());
    assert!(!run.success, "{text}");
    assert!(text.contains("Unable to open editor"), "{text}");
    assert!(home.plugins.join("brandnew.py").exists());
}

#[test]
fn delete_venvs_refuses_names_that_are_not_one_directory_name() {
    let home = Home::new();
    let canary = home.root.join("canary");
    std::fs::create_dir_all(&canary).unwrap();
    std::fs::create_dir_all(home.root.join("venvs/ok/lib/python3.13/site-packages")).unwrap();
    for name in ["../canary", "..", "a/b", ".hidden", "a\\b"] {
        let run = home.run_with(&["delete", "venvs", name, "-y"], &[NO_PYTHON]);
        let text = flat(&run.text());
        assert!(!run.success, "{name}: {text}");
        assert!(
            text.contains(&format!("Invalid venv name: '{name}'")),
            "{name}: {text}"
        );
    }
    assert!(canary.exists());
    assert!(home.root.join("venvs/ok").exists());
}
