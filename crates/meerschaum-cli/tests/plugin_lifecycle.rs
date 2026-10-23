//! The plugin lifecycle actions through the built `mrsm` binary: `setup plugins`,
//! `install required`, `install packages` and `install plugins`.
//!
//! Every test uses synthetic plugins, a temporary root and plugins directory, an offline installer
//! (a local wheel, `UV_OFFLINE`) and, for `install plugins`, a repository on `127.0.0.1`. The
//! expected messages are the ones released v4.1.4 prints for the same plugins and repository
//! (compared by hand against `.venv-ref`; see `crates/meerschaum-cli/README.md`).

mod plugin_lifecycle_support;

use plugin_lifecycle_support::{
    Home, Repo, archive, closed_port, install_native_demo, traversal_archive,
};

const NO_PYTHON: (&str, &str) = ("MRSM_PYTHON", "/nonexistent/mrsm-test-python");

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

// --- setup plugins -----------------------------------------------------------------------------

const SETUP_PLUGINS: &[(&str, &str)] = &[
    (
        "good.py",
        "__version__ = '1.0.0'\ndef setup(**kw):\n    return True, 'good setup done'\n",
    ),
    ("falsy.py", "def setup(**kw):\n    return False\n"),
    (
        "tup.py",
        "def setup(**kw):\n    return False, 'tuple says no'\n",
    ),
    ("plain.py", "x = 1\n"),
    ("nonep.py", "def setup(**kw):\n    return None\n"),
    (
        "raises.py",
        "def setup(**kw):\n    raise RuntimeError('kaboom')\n",
    ),
    ("weird.py", "def setup(**kw):\n    return 5\n"),
];

#[test]
fn setup_plugins_reports_every_result_like_v4() {
    if !python_available() {
        return;
    }
    let home = Home::new();
    for (file, text) in SETUP_PLUGINS {
        home.write_plugin(file, text);
    }
    let run = home.run(&[
        "setup", "plugins", "good", "falsy", "tup", "plain", "nonep", "raises", "weird", "nosuch",
    ]);
    let text = run.text();
    // A failing tuple is printed; a bare `False` gets Plugin.setup's own message.
    assert!(
        text.contains("Setup for Plugin 'falsy' did not return a message."),
        "{text}"
    );
    assert!(text.contains("tuple says no"), "{text}");
    assert!(
        text.contains("Setup for Plugin 'nonep' returned None."),
        "{text}"
    );
    assert!(text.contains("kaboom"), "{text}");
    assert!(
        text.contains("Unknown return value from setup for Plugin 'weird': 5"),
        "{text}"
    );
    for name in ["falsy", "tup", "nonep", "raises", "weird"] {
        assert!(
            text.contains(&format!("Failed to set up plugin '{name}'.")),
            "{name}: {text}"
        );
    }
    // A success prints nothing of its own; unknown plugins are skipped with a warning.
    assert!(!text.contains("good setup done"), "{text}");
    assert!(
        text.contains("Plugin 'nosuch' is not installed. Skipping..."),
        "{text}"
    );
    assert!(
        text.contains("Completed setting up 7 plugins\n    (2 succeeded, 5 failed)."),
        "{text}"
    );
    assert!(run.success, "{text}");
}

#[test]
fn setup_plugins_without_names_sets_up_every_installed_plugin_and_fails_when_none_succeed() {
    if !python_available() {
        return;
    }
    let home = Home::new();
    home.write_plugin("good.py", SETUP_PLUGINS[0].1);
    home.write_plugin("tup.py", SETUP_PLUGINS[2].1);
    let run = home.run(&["setup", "plugins"]);
    let text = run.text();
    assert!(
        text.contains("Completed setting up 2 plugins\n    (1 succeeded, 1 failed)."),
        "{text}"
    );
    assert!(run.success);

    // Only the failing one: `success = success_count > 0`.
    let failing = Home::new();
    failing.write_plugin("tup.py", SETUP_PLUGINS[2].1);
    let run = failing.run(&["setup", "plugins"]);
    let text = run.text();
    assert!(
        text.contains("Completed setting up 1 plugins\n    (0 succeeded, 1 failed)."),
        "{text}"
    );
    assert!(!run.success);

    // Nothing installed at all: zero of zero, which is not a success either.
    let empty = Home::new();
    let run = empty.run_with(&["setup", "plugins"], &[NO_PYTHON]);
    assert!(
        run.text()
            .contains("Completed setting up 0 plugins\n    (0 succeeded, 0 failed)."),
        "{}",
        run.text()
    );
    assert!(!run.success);
}

#[test]
fn setup_plugins_rejects_a_name_with_two_repository_separators_and_a_non_api_repository() {
    let home = Home::new();
    let run = home.run_with(&["setup", "plugins", "a@b@c"], &[NO_PYTHON]);
    assert!(!run.success);
    assert!(
        run.text().contains("Invalid plugin name: 'a@b@c'"),
        "{}",
        run.text()
    );
    let run = home.run_with(&["setup", "plugins", "x", "-r", "sql:main"], &[NO_PYTHON]);
    assert!(!run.success);
    assert!(
        run.text()
            .contains("Only APIConnectors may be treated as repositories."),
        "{}",
        run.text()
    );
}

#[test]
fn a_native_only_plugins_directory_starts_no_python_for_any_lifecycle_action() {
    let home = Home::new();
    install_native_demo(&home.plugins);
    // The interpreter does not exist: any worker would fail to start.
    let run = home.run_with(&["setup", "plugins"], &[NO_PYTHON]);
    let text = run.text();
    assert!(run.success, "{text}");
    assert!(
        text.contains("Completed setting up 1 plugins\n    (1 succeeded, 0 failed)."),
        "{text}"
    );
    let run = home.run_with(&["setup", "plugins", "native_demo", "nosuch"], &[NO_PYTHON]);
    let text = run.text();
    assert!(run.success, "{text}");
    assert!(
        text.contains("Plugin 'nosuch' is not installed. Skipping..."),
        "{text}"
    );
    assert!(
        text.contains("Completed setting up 1 plugins\n    (1 succeeded, 0 failed)."),
        "{text}"
    );

    // `install required` has nothing to install for a native plugin.
    let run = home.run_with(&["install", "required"], &[NO_PYTHON]);
    let text = run.text();
    assert!(run.success, "{text}");
    assert!(
        text.contains("Installed packages for 1 plugins\n    (1 succeeded, 0 failed)."),
        "{text}"
    );
    // The repository-less compose notice never needs one either.
    let run = home.run_with(&["install", "plugins", "compose"], &[NO_PYTHON]);
    assert!(run.success, "{}", run.text());
}

// --- install required --------------------------------------------------------------------------

const NEEDS: &str = "__version__ = '1.0.0'\nrequired = ['mrsm_syn_pkg']\n";

#[test]
fn install_required_installs_into_the_plugin_venv_and_records_the_request() {
    if !python_available() || !installer_available() {
        return;
    }
    let home = Home::new();
    home.write_plugin("needs.py", NEEDS);
    // `MRSM_NO_AUTO_INSTALL` only stops implicit `attempt_import` installs: explicit installs
    // (v4's `pip_install`) ignore it.
    let run = home.run_with(
        &["install", "required", "needs"],
        &[("MRSM_NO_AUTO_INSTALL", "1")],
    );
    let text = run.text();
    assert!(run.success, "{text}");
    assert!(
        text.contains("Installing required packages for plugin 'needs'..."),
        "{text}"
    );
    assert!(
        text.contains("Installing packages:\n  - mrsm_syn_pkg"),
        "{text}"
    );
    assert!(text.contains("Successfully installed packages."), "{text}");
    assert!(
        text.contains("Installed packages for 1 plugins\n    (1 succeeded, 0 failed)."),
        "{text}"
    );
    assert!(home.has_package("needs", "mrsm_syn_pkg"));

    // Who asked for it is recorded where `show venvs` and `prune venvs` read it.
    let ledger: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(home.root.join("venvs/needs/.mrsm_requests.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        ledger["packages"]["mrsm-syn-pkg"]["requesters"]["meerschaum"]["spec"],
        "mrsm_syn_pkg"
    );
    let shown = home.run(&["show", "venvs"]);
    assert!(shown.text().contains("mrsm-syn-pkg"), "{}", shown.text());

    // Running it again is a no-op install, still a success.
    let again = home.run(&["install", "required", "needs"]);
    assert!(again.success, "{}", again.text());
}

#[test]
fn install_required_reports_an_offline_failure_and_continues_with_force() {
    if !python_available() || !installer_available() {
        return;
    }
    let home = Home::new();
    home.write_plugin(
        "bad.py",
        "__version__ = '1.0.0'\nrequired = ['nonexistent-pkg-zzz']\n",
    );
    home.write_plugin("fine.py", "x = 1\n");
    let run = home.run(&["install", "required", "bad", "nosuch", "fine"]);
    let text = flat(&run.text());
    assert!(!run.success, "{text}");
    assert!(
        text.contains("Installing required packages for plugin 'bad'..."),
        "{text}"
    );
    assert!(text.contains("Failed to install packages."), "{text}");
    assert!(
        text.contains("Failed to install required package 'nonexistent-pkg-zzz' for plugin 'bad'."),
        "{text}"
    );
    assert!(
        text.contains("Try installing with `--force` to continue anyway."),
        "{text}"
    );
    assert!(
        text.contains("Failed to install required packages for plugin 'bad'."),
        "{text}"
    );
    assert!(
        text.contains("Plugin 'nosuch' is not installed. Skipping..."),
        "{text}"
    );
    assert!(
        text.contains("Installed packages for 2 plugins (1 succeeded, 1 failed)."),
        "{text}"
    );

    // `--force` goes on after a failed package.
    let forced = home.run(&["install", "required", "bad", "--force"]);
    let text = flat(&forced.text());
    assert!(forced.success, "{text}");
    assert!(
        text.contains(
            "Continuing with installation despite the failure (careful, things might be broken!)..."
        ),
        "{text}"
    );
    assert!(
        text.contains("Installed packages for 1 plugins (1 succeeded, 0 failed)."),
        "{text}"
    );
}

#[test]
fn install_required_reads_the_list_of_a_plugin_whose_import_fails_from_its_source() {
    if !python_available() || !installer_available() {
        return;
    }
    let home = Home::new();
    // Importing it needs the package it declares, so the manifest has no list for it.
    home.write_plugin(
        "needsimport.py",
        "import mrsm_syn_pkg\nrequired = [\n    'mrsm_syn_pkg==1.0.0',  # a comment ]\n]\n",
    );
    let run = home.run(&["install", "required", "needsimport"]);
    let text = run.text();
    assert!(run.success, "{text}");
    // `==` survives: v4's text reader would turn it into `=`.
    assert!(
        text.contains("Installing packages:\n  - mrsm_syn_pkg==1.0.0"),
        "{text}"
    );
    assert!(home.has_package("needsimport", "mrsm_syn_pkg"));
}

#[test]
fn install_required_with_a_requirements_file_next_to_the_plugin() {
    if !python_available() || !installer_available() {
        return;
    }
    let home = Home::new();
    home.write_plugin("filed/__init__.py", "__version__ = '1.0.0'\n");
    home.write_plugin("filed/requirements.txt", "mrsm_syn_pkg\n");
    let run = home.run(&["install", "required", "filed"]);
    let text = run.text();
    assert!(run.success, "{text}");
    assert!(home.has_package("filed", "mrsm_syn_pkg"), "{text}");
}

// --- install packages --------------------------------------------------------------------------

#[test]
fn install_packages_installs_into_a_named_venv() {
    if !installer_available() || !python_available() {
        return;
    }
    let home = Home::new();
    let run = home.run(&["install", "packages", "mrsm_syn_pkg", "--venv", "synthetic"]);
    let text = flat(&run.text());
    assert!(run.success, "{text}");
    assert!(
        text.contains(
            "Successfully installed package 'mrsm_syn_pkg' into the virtual environment 'synthetic'."
        ),
        "{text}"
    );
    assert!(home.has_package("synthetic", "mrsm_syn_pkg"));

    // The default environment is `mrsm`; two packages are listed with "and".
    let run = home.run(&["install", "packages", "mrsm_syn_pkg", "mrsm_syn_pkg"]);
    let text = flat(&run.text());
    assert!(run.success, "{text}");
    assert!(
        text.contains(
            "Successfully installed packages 'mrsm_syn_pkg' and 'mrsm_syn_pkg' into the virtual environment 'mrsm'."
        ),
        "{text}"
    );
    assert!(home.has_package("mrsm", "mrsm_syn_pkg"));
}

#[test]
fn install_packages_failures_and_empty_requests() {
    if !installer_available() || !python_available() {
        return;
    }
    let home = Home::new();
    let run = home.run(&["install", "packages"]);
    assert!(!run.success);
    assert!(
        run.text().contains("No packages to install."),
        "{}",
        run.text()
    );
    let run = home.run(&[
        "install",
        "packages",
        "nonexistent-pkg-zzz",
        "--venv",
        "offline",
    ]);
    let text = flat(&run.text());
    assert!(!run.success, "{text}");
    assert!(
        text.contains("Failed to install package 'nonexistent-pkg-zzz'."),
        "{text}"
    );
    assert!(text.contains("Failed to install packages."), "{text}");
}

#[test]
fn install_packages_dry_run_prints_the_plan_and_changes_nothing() {
    if !python_available() {
        return;
    }
    let home = Home::new();
    let run = home.run_with(
        &[
            "install",
            "packages",
            "foo",
            "bar>=1",
            "--venv",
            "planned",
            "--dry-run",
        ],
        &[("MRSM_NO_AUTO_INSTALL", "true")],
    );
    assert!(run.success, "{}", run.text());
    let json_text: String = run
        .stdout
        .lines()
        .skip_while(|l| !l.starts_with('{'))
        .take_while(|l| !l.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    let plan: serde_json::Value = serde_json::from_str(&json_text).expect(&run.stdout);
    assert_eq!(plan["operation"], "install");
    assert_eq!(plan["environment"], "planned");
    assert_eq!(plan["packages"], serde_json::json!(["foo", "bar>=1"]));
    assert_eq!(plan["args"], serde_json::json!(["--upgrade"]));
    assert_eq!(plan["auto_install_enabled"], false);
    assert!(
        plan["target"]
            .as_str()
            .unwrap()
            .ends_with("venvs/planned/lib/python3.".to_string().as_str())
            || plan["target"]
                .as_str()
                .unwrap()
                .contains("venvs/planned/lib/python3.")
    );
    assert!(plan["installer"] == "uv" || plan["installer"] == "pip");
    for key in [
        "requirements_file",
        "installer_available",
        "pip_fallback",
        "would_bootstrap_uv",
        "lock_path",
    ] {
        assert!(plan.get(key).is_some(), "{key} in {plan}");
    }
    assert!(
        run.text()
            .contains("Dependency preflight completed without changing the environment."),
        "{}",
        run.text()
    );
    assert!(!home.root.join("venvs/planned").exists());
}

// --- install plugins ---------------------------------------------------------------------------

fn with_repo(home: &Home, repo: &Repo, args: &[&str]) -> plugin_lifecycle_support::Run {
    let (key, value) = repo.connector("loop");
    let mut full: Vec<&str> = args.to_vec();
    full.extend(["-r", "api:loop"]);
    home.run_with(&full, &[(key.as_str(), value.as_str())])
}

fn module_archive(version: &str, extra: &str) -> Vec<u8> {
    archive(&[(
        "foo.py",
        &format!(
            "__version__ = '{version}'\n{extra}\ndef setup(**kw):\n    return True, 'foo ready'\n"
        ),
    )])
}

fn installed_version(home: &Home, name: &str) -> String {
    let text = std::fs::read_to_string(home.plugins.join(format!("{name}.py"))).unwrap();
    text.lines().next().unwrap().to_string()
}

#[test]
fn install_plugins_installs_updates_and_skips_like_v4() {
    if !python_available() || !installer_available() {
        return;
    }
    let home = Home::new();
    let repo = Repo::start();
    repo.publish(
        "foo",
        module_archive("1.0.0", "required = ['mrsm_syn_pkg']"),
    );

    // A fresh install: archive, venv, dependencies, setup, origin.
    let run = with_repo(&home, &repo, &["install", "plugins", "foo"]);
    let text = flat(&run.text());
    assert!(run.success, "{text}");
    assert!(
        text.contains("Installing plugin 'foo' from Meerschaum repository 'api:loop'..."),
        "{text}"
    );
    assert!(
        text.contains("Successfully installed plugin 'foo'."),
        "{text}"
    );
    assert_eq!(installed_version(&home, "foo"), "__version__ = '1.0.0'");
    assert!(home.has_package("foo", "mrsm_syn_pkg"));
    let origins = std::fs::read_to_string(home.plugins.join(".mrsm_origins.json")).unwrap();
    assert_eq!(origins, "{\n    \"foo\": \"api:loop\"\n}");
    assert_eq!(repo.count("/plugins/foo"), 1);

    // The same version: up to date.
    let run = with_repo(&home, &repo, &["install", "plugins", "foo"]);
    let text = run.text();
    assert!(run.success, "{text}");
    assert!(
        text.contains(
            "Plugin 'foo' is up-to-date (version 1.0.0).\n    Install again with `-f` or `--force` to reinstall."
        ),
        "{text}"
    );

    // `--force` replaces it.
    repo.publish("foo", module_archive("1.0.0", "# replaced"));
    let run = with_repo(&home, &repo, &["install", "plugins", "foo", "--force"]);
    assert!(
        flat(&run.text()).contains("Successfully installed plugin 'foo'."),
        "{}",
        run.text()
    );
    assert!(
        std::fs::read_to_string(home.plugins.join("foo.py"))
            .unwrap()
            .contains("# replaced")
    );

    // A newer version replaces it; an older one is refused and the file stays.
    repo.publish("foo", module_archive("2.0.0", ""));
    let run = with_repo(&home, &repo, &["install", "plugins", "foo"]);
    assert!(
        flat(&run.text()).contains("Successfully installed plugin 'foo'."),
        "{}",
        run.text()
    );
    assert_eq!(installed_version(&home, "foo"), "__version__ = '2.0.0'");
    repo.publish("foo", module_archive("1.5.0", ""));
    let run = with_repo(&home, &repo, &["install", "plugins", "foo"]);
    let text = flat(&run.text());
    assert!(
        text.contains(
            "Your installed version of plugin 'foo' (2.0.0) is higher than attempted version 1.5.0."
        ),
        "{text}"
    );
    assert_eq!(installed_version(&home, "foo"), "__version__ = '2.0.0'");
    // Each run printed its own result and still ended with v4's `Success`.
    assert!(run.success, "{text}");
}

#[test]
fn install_plugins_reports_a_missing_archive_and_an_unreachable_repository() {
    if !python_available() {
        return;
    }
    let home = Home::new();
    let repo = Repo::start();
    let run = with_repo(&home, &repo, &["install", "plugins", "nosuch"]);
    let text = run.text();
    assert!(
        text.contains("Archive for plugin 'nosuch' could not be found."),
        "{text}"
    );
    // v4 returns `reload`'s success whatever happened to each plugin.
    assert!(run.success, "{text}");
    assert!(!home.plugins.join("nosuch.py").exists());

    let port = closed_port();
    let run = home.run_with(
        &["install", "plugins", "foo", "-r", "api:dead"],
        &[("MRSM_API_DEAD", &format!("http://127.0.0.1:{port}"))],
    );
    let text = run.text();
    assert!(!run.success, "{text}");
    assert!(
        text.contains("Failed to execute `install plugins foo` with `Exception`"),
        "{text}"
    );
    assert!(
        text.contains(&format!(
            "Failed to download from 'http://127.0.0.1:{port}/plugins/foo'."
        )),
        "{text}"
    );

    let run = home.run(&["install", "plugins"]);
    assert!(!run.success);
    assert!(
        run.text().contains("No plugins to install."),
        "{}",
        run.text()
    );
}

#[test]
fn install_plugins_compose_is_a_noop_that_never_contacts_the_repository() {
    let home = Home::new();
    let repo = Repo::start();
    let run = with_repo(&home, &repo, &["install", "plugins", "compose"]);
    let text = flat(&run.text());
    assert!(run.success, "{text}");
    assert!(
        text.contains("Compose is built into Meerschaum; the 'compose' plugin is not installed."),
        "{text}"
    );
    assert!(repo.requests().is_empty(), "{:?}", repo.requests());
    assert!(!home.plugins.join("compose.py").exists());
}

#[test]
fn install_plugins_installs_package_plugins_and_the_plugins_they_require() {
    if !python_available() || !installer_available() {
        return;
    }
    let home = Home::new();
    let repo = Repo::start();
    repo.publish(
        "parent",
        archive(&[
            (
                "parent/__init__.py",
                "__version__ = '1.0.0'\nrequired = ['plugin:child@api:loop']\n",
            ),
            ("parent/data/readme.txt", "bundled data"),
        ]),
    );
    repo.publish("child", archive(&[("child.py", "__version__ = '0.3.0'\n")]));
    let run = with_repo(&home, &repo, &["install", "plugins", "parent"]);
    let text = flat(&run.text());
    assert!(run.success, "{text}");
    assert!(
        text.contains("Successfully installed plugin 'parent'."),
        "{text}"
    );
    assert!(home.plugins.join("parent/__init__.py").is_file());
    assert_eq!(
        std::fs::read_to_string(home.plugins.join("parent/data/readme.txt")).unwrap(),
        "bundled data"
    );
    // The required plugin came from the repository named in the requirement.
    assert!(home.plugins.join("child.py").is_file(), "{text}");
    assert_eq!(repo.count("/plugins/child"), 1);
    let origins = std::fs::read_to_string(home.plugins.join(".mrsm_origins.json")).unwrap();
    assert!(origins.contains("\"child\": \"api:loop\""), "{origins}");
    assert!(origins.contains("\"parent\": \"api:loop\""), "{origins}");
}

#[test]
fn a_failed_post_install_setup_restores_the_previous_plugin() {
    if !python_available() || !installer_available() {
        return;
    }
    let home = Home::new();
    let repo = Repo::start();
    repo.publish("foo", module_archive("1.0.0", ""));
    let first = with_repo(&home, &repo, &["install", "plugins", "foo"]);
    assert!(first.success, "{}", first.text());
    // Version 2's `setup()` fails after the files were replaced.
    repo.publish(
        "foo",
        archive(&[(
            "foo.py",
            "__version__ = '2.0.0'\ndef setup(**kw):\n    return False, 'setup refused'\n",
        )]),
    );
    let run = with_repo(&home, &repo, &["install", "plugins", "foo"]);
    let text = flat(&run.text());
    assert!(text.contains("setup refused"), "{text}");
    assert!(
        !text.contains("Successfully installed plugin 'foo'."),
        "{text}"
    );
    assert_eq!(installed_version(&home, "foo"), "__version__ = '1.0.0'");
}

#[test]
fn a_dependency_failure_rolls_the_install_back_and_force_keeps_it() {
    if !python_available() || !installer_available() {
        return;
    }
    let home = Home::new();
    let repo = Repo::start();
    repo.publish(
        "foo",
        archive(&[(
            "foo.py",
            "__version__ = '1.0.0'\nrequired = ['nonexistent-pkg-zzz']\n",
        )]),
    );
    let run = with_repo(&home, &repo, &["install", "plugins", "foo"]);
    let text = flat(&run.text());
    assert!(
        text.contains("Failed to install dependencies for plugin 'foo'."),
        "{text}"
    );
    assert!(!home.plugins.join("foo.py").exists(), "{text}");
    assert!(!home.root.join("venvs/foo").exists());

    let run = with_repo(&home, &repo, &["install", "plugins", "foo", "--force"]);
    let text = flat(&run.text());
    assert!(
        text.contains("Successfully installed plugin 'foo'."),
        "{text}"
    );
    assert!(home.plugins.join("foo.py").exists());

    // `--skip-deps` never looks at the requirements.
    let skipping = Home::new();
    let run = with_repo(
        &skipping,
        &repo,
        &["install", "plugins", "foo", "--skip-deps"],
    );
    let text = run.text();
    assert!(
        text.contains("Successfully installed plugin 'foo'\n    (skipped dependencies)."),
        "{text}"
    );
}

#[test]
fn install_plugins_refuses_an_archive_that_escapes_its_directory() {
    if !python_available() {
        return;
    }
    let home = Home::new();
    let repo = Repo::start();
    repo.publish("evil", traversal_archive());
    let run = with_repo(&home, &repo, &["install", "plugins", "evil"]);
    let text = run.text();
    assert!(text.contains("Failed to extract plugin 'evil'."), "{text}");
    assert!(
        text.contains("Attempted Path Traversal in Tar File"),
        "{text}"
    );
    assert!(!home.root.join(".internal/plugins/evil.py").exists());
    assert!(!home.root.join(".internal/plugins/.tmp/evil.txt").exists());
}

// --- compose -----------------------------------------------------------------------------------

/// A compose project in `dir` whose root and plugins directory are inside it.
fn compose_project(dir: &std::path::Path, repo_port: Option<u16>, plugins: &[&str]) {
    let mut text = String::from(
        "project_name: lifecycle\nroot_dir: ./root\nplugins_dir: [./plugins]\nconfig:\n  \
         meerschaum:\n    instance: sql:local\n",
    );
    if let Some(port) = repo_port {
        text.push_str(&format!(
            "    connectors:\n      api:\n        loop:\n          protocol: http\n          \
             host: 127.0.0.1\n          port: {port}\n"
        ));
    }
    text.push_str("  system:\n    cli:\n      check_updates: false\n");
    if !plugins.is_empty() {
        text.push_str("plugins:\n");
        for plugin in plugins {
            text.push_str(&format!("  - {plugin}\n"));
        }
    }
    std::fs::write(dir.join("mrsm-compose.yaml"), text).unwrap();
}

#[test]
fn compose_init_installs_requirements_and_sets_up_a_local_plugin_in_a_fresh_root() {
    if !python_available() || !installer_available() {
        return;
    }
    let home = Home::new();
    let project = home.root.parent().unwrap().join("project");
    std::fs::create_dir_all(project.join("plugins")).unwrap();
    std::fs::write(
        project.join("plugins/localp.py"),
        "__version__ = '1.0.0'\nrequired = ['mrsm_syn_pkg']\n\
         def setup(**kw):\n    return True, 'localp ready'\n",
    )
    .unwrap();
    compose_project(&project, None, &[]);
    let run =
        plugin_lifecycle_support::run_binary(&["compose", "init"], &home.env(), Some(&project));
    let text = flat(&run.text());
    assert!(run.success, "{text}");
    assert!(
        text.contains("Finished initializing project 'lifecycle'."),
        "{text}"
    );
    // The package went into the plugin's venv under the project root, not the host root.
    let site = project
        .join("root/venvs/localp/lib")
        .read_dir()
        .unwrap()
        .flatten()
        .map(|e| e.path().join("site-packages/mrsm_syn_pkg/__init__.py"))
        .find(|p| p.is_file());
    assert!(site.is_some(), "{text}");
    assert!(!home.root.join("venvs/localp").exists());
}

#[test]
fn compose_reinstalls_the_whole_repository_group_when_one_plugin_is_missing() {
    if !python_available() || !installer_available() {
        return;
    }
    let home = Home::new();
    let repo = Repo::start();
    repo.publish("foo", module_archive("1.0.0", ""));
    repo.publish("bar", archive(&[("bar.py", "__version__ = '1.0.0'\n")]));
    let project = home.root.parent().unwrap().join("project");
    std::fs::create_dir_all(project.join("plugins")).unwrap();
    // `foo` is already there; only `bar` is missing.
    std::fs::write(
        project.join("plugins/foo.py"),
        "__version__ = '1.0.0'\ndef setup(**kw):\n    return True, 'foo ready'\n",
    )
    .unwrap();
    compose_project(&project, Some(repo.port), &["foo@api:loop", "bar@api:loop"]);
    let run =
        plugin_lifecycle_support::run_binary(&["compose", "init"], &home.env(), Some(&project));
    let text = flat(&run.text());
    assert!(run.success, "{text}");
    // v4's quirk: the whole group is requested, the installed member included.
    assert_eq!(repo.count("/plugins/bar"), 1, "{:?}", repo.requests());
    assert_eq!(repo.count("/plugins/foo"), 1, "{:?}", repo.requests());
    assert!(project.join("plugins/bar.py").is_file(), "{text}");
    assert!(
        text.contains("Plugin 'foo' is up-to-date (version 1.0.0)."),
        "{text}"
    );
}

#[test]
fn install_plugins_refuses_a_symlink_chain_that_would_write_outside() {
    if !python_available() {
        return;
    }
    let home = Home::new();
    let repo = Repo::start();
    // `x -> .`, `t -> x/..` (the parent of the extraction directory), then `t/escaped.txt`.
    let mut tar_bytes = Vec::new();
    for (name, kind, link, data) in [
        ("x", tar::EntryType::Symlink, Some("."), &b""[..]),
        ("t", tar::EntryType::Symlink, Some("x/.."), &b""[..]),
        (
            "t/escaped.txt",
            tar::EntryType::Regular,
            None,
            &b"owned"[..],
        ),
    ] {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(kind);
        header.set_path(name).unwrap();
        if let Some(link) = link {
            header.set_link_name(link).unwrap();
        }
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar_bytes.extend_from_slice(header.as_bytes());
        tar_bytes.extend_from_slice(data);
        tar_bytes.resize(tar_bytes.len().div_ceil(512) * 512, 0);
    }
    tar_bytes.extend([0u8; 1024]);
    repo.publish("evil", plugin_lifecycle_support::gzip(&tar_bytes));
    let run = with_repo(&home, &repo, &["install", "plugins", "evil"]);
    let text = run.text();
    assert!(text.contains("Links are not allowed"), "{text}");
    assert!(text.contains("Failed to extract plugin 'evil'."), "{text}");
    let temp = home.root.join(".internal/plugins/.tmp");
    // Nothing landed outside the extraction directory, nothing was installed, and the temporary
    // extraction directory is gone.
    assert!(!temp.join("escaped.txt").exists());
    assert!(!home.root.join(".internal/plugins/escaped.txt").exists());
    assert!(!temp.join("evil").exists(), "{text}");
    assert!(!home.plugins.join("evil.py").exists());
    assert!(!home.plugins.join("evil").exists());
}

#[test]
fn install_packages_skips_every_bootstrap_while_auto_install_is_disabled() {
    if !installer_available() || !python_available() {
        return;
    }
    let home = Home::new();
    let run = home.run_with(
        &["install", "packages", "mrsm_syn_pkg", "--venv", "locked"],
        &[("MRSM_NO_AUTO_INSTALL", "1")],
    );
    let text = flat(&run.text());
    // The explicit install still happens...
    assert!(run.success, "{text}");
    assert!(home.has_package("locked", "mrsm_syn_pkg"), "{text}");
    // ...but nothing seeded the environment (`uv venv --seed`, `ensurepip`, wheel/setuptools).
    for bootstrap in ["pip", "wheel", "setuptools", "yaml"] {
        assert!(
            !home.has_package("locked", bootstrap),
            "{bootstrap}: {text}"
        );
    }
}

#[test]
fn install_rust_plugins_builds_without_python_and_rolls_back_failed_updates() {
    let home = Home::new();
    let repo = Repo::start();
    let (key, value) = repo.connector("loop");
    let user_home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .unwrap();
    let cargo_home = std::env::var("CARGO_HOME").unwrap_or_else(|_| format!("{user_home}/.cargo"));
    let rustup_home =
        std::env::var("RUSTUP_HOME").unwrap_or_else(|_| format!("{user_home}/.rustup"));
    let run = |args: &[&str]| {
        home.run_with(
            args,
            &[
                (key.as_str(), value.as_str()),
                NO_PYTHON,
                ("CARGO_HOME", &cargo_home),
                ("RUSTUP_HOME", &rustup_home),
                ("CARGO_NET_OFFLINE", "true"),
            ],
        )
    };
    let good = "fn fetch() -> Vec<Value> { vec![json!({\"id\": 1})] }";
    repo.publish("rusty", archive(&[("rusty.rs", good)]));
    let installed = run(&["install", "plugins", "rusty", "-r", "api:loop"]);
    assert!(
        installed
            .text()
            .contains("Successfully installed plugin 'rusty'"),
        "{}",
        installed.text()
    );
    assert_eq!(
        std::fs::read_to_string(home.plugins.join("rusty.rs")).unwrap(),
        good
    );
    assert!(!home.root.join("venvs/rusty").exists());
    let origins = std::fs::read_to_string(home.plugins.join(".mrsm_origins.json")).unwrap();
    assert!(origins.contains("api:loop"));

    repo.publish(
        "rusty",
        archive(&[("rusty.rs", "fn fetch() -> Vec<Value> { vec![]; }")]),
    );
    let failed = run(&["install", "plugins", "rusty", "-r", "api:loop"]);
    assert!(
        !failed
            .text()
            .contains("Successfully installed plugin 'rusty'"),
        "{}",
        failed.text()
    );
    assert!(
        failed.text().contains("remove this semicolon"),
        "{}",
        failed.text()
    );
    assert_eq!(
        std::fs::read_to_string(home.plugins.join("rusty.rs")).unwrap(),
        good
    );
    assert_eq!(
        std::fs::read_to_string(home.plugins.join(".mrsm_origins.json")).unwrap(),
        origins
    );

    // A full Cargo directory has a descriptor and its own version; it needs no __init__.py.
    let generated = home.root.join("generated");
    meerschaum_plugin::source::prepare_single_file(
        &home.plugins.join("rusty.rs"),
        "rusty",
        &generated,
    )
    .unwrap();
    std::fs::write(
        generated.join("plugin.toml"),
        "name = \"rusty\"\nversion = \"0.0.0\"\n",
    )
    .unwrap();
    let files: Vec<(String, String)> = std::fs::read_dir(&generated)
        .unwrap()
        .flatten()
        .map(|entry| {
            (
                format!("rusty/{}", entry.file_name().to_string_lossy()),
                std::fs::read_to_string(entry.path()).unwrap(),
            )
        })
        .collect();
    repo.publish(
        "rusty",
        archive(
            &files
                .iter()
                .map(|(n, s)| (n.as_str(), s.as_str()))
                .collect::<Vec<_>>(),
        ),
    );
    let installed = run(&["install", "plugins", "rusty", "-r", "api:loop"]);
    assert!(
        installed
            .text()
            .contains("Successfully installed plugin 'rusty'"),
        "{}",
        installed.text()
    );
    assert!(home.plugins.join("rusty/plugin.toml").exists());
    assert!(!home.plugins.join("rusty.rs").exists());
    assert!(!home.root.join("venvs/rusty").exists());
    let same = run(&["install", "plugins", "rusty", "-r", "api:loop"]);
    assert!(same.text().contains("up-to-date"), "{}", same.text());
    // Prebuilt archives use the same install path, without a compiler or Python environment.
    let library = meerschaum_plugin::source::built_library(
        &home.root.join("cache/native_build/rusty"),
        "rusty",
    );
    let binary = std::fs::read(&library).unwrap();
    let descriptor = b"name = \"rusty\"\nversion = \"0.0.0\"\n";
    let mut tar = tar::Builder::new(Vec::new());
    let library_member = format!("rusty/{}", library.file_name().unwrap().to_string_lossy());
    for (name, data) in [
        ("rusty/plugin.toml", descriptor.as_slice()),
        (library_member.as_str(), binary.as_slice()),
    ] {
        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar.append_data(&mut header, name, data).unwrap();
    }
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    std::io::Write::write_all(&mut gzip, &tar.into_inner().unwrap()).unwrap();
    repo.publish("rusty", gzip.finish().unwrap());
    let prebuilt = run(&["install", "plugins", "rusty", "-r", "api:loop", "--force"]);
    assert!(
        prebuilt
            .text()
            .contains("Successfully installed plugin 'rusty'"),
        "{}",
        prebuilt.text()
    );
    assert!(!home.plugins.join("rusty/Cargo.toml").exists());
    assert!(!prebuilt.text().contains("Building native plugin"));
    let removed = run(&["uninstall", "plugins", "rusty", "--force"]);
    assert!(removed.success, "{}", removed.text());
    assert!(!home.plugins.join("rusty").exists());

    repo.publish("rusty", archive(&[("rusty.rs", good)]));
    let installed = run(&["install", "plugins", "rusty", "-r", "api:loop"]);
    assert!(
        installed
            .text()
            .contains("Successfully installed plugin 'rusty'"),
        "{}",
        installed.text()
    );
    let removed = run(&["uninstall", "plugins", "rusty", "--force"]);
    assert!(removed.success, "{}", removed.text());
    assert!(!home.plugins.join("rusty.rs").exists());
}
