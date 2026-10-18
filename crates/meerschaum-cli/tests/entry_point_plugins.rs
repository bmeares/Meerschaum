//! Entry-point plugins (pip-installed packages in the `meerschaum.plugins` group) are found
//! whatever the plugins directory holds, and a tree with no plugin at all still starts no Python
//! once the interpreter's answer is cached (`meerschaum_worker::entry_points`).
//!
//! Every test runs the built `mrsm-cli` binary against a temporary root, an EMPTY plugins
//! directory and a synthetic site directory on `PYTHONPATH`. The interpreter is a wrapper script
//! that logs each launch, so the log is the count of Python starts (unix only).
#![cfg(unix)]

use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

struct Setup {
    _dir: tempfile::TempDir,
    root: PathBuf,
    plugins: PathBuf,
    site: PathBuf,
    python: PathBuf,
    log: PathBuf,
}

impl Setup {
    fn new() -> Setup {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        let plugins = dir.path().join("plugins");
        let site = dir.path().join("site");
        for path in [&root, &plugins, &site] {
            std::fs::create_dir_all(path).unwrap();
        }
        let log = dir.path().join("python.log");
        let python = dir.path().join("python-wrapper");
        // Starts the real interpreter, noting that it was started.
        std::fs::write(
            &python,
            format!(
                "#!/bin/sh\necho launched >> '{}'\nexec python3 \"$@\"\n",
                log.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&python, std::fs::Permissions::from_mode(0o755)).unwrap();
        Setup {
            _dir: dir,
            root,
            plugins,
            site,
            python,
            log,
        }
    }

    /// Install an entry-point plugin `name` (an action `<name>_hello`) the way `pip` does.
    fn install(&self, name: &str) {
        let dist = self.site.join(format!("{name}-0.1.dist-info"));
        std::fs::create_dir_all(&dist).unwrap();
        std::fs::write(
            self.site.join(format!("{name}.py")),
            format!(
                "from meerschaum.plugins import make_action, add_plugin_argument\n\n\n\
                 add_plugin_argument('--{name}-loud', action='store_true', help='loud')\n\n\n\
                 @make_action\ndef {name}_hello(**kw):\n    \
                 return True, 'entry point says hi, loud=' + repr(kw.get('{name}_loud'))\n"
            ),
        )
        .unwrap();
        std::fs::write(
            dist.join("METADATA"),
            format!("Metadata-Version: 2.1\nName: {name}\nVersion: 0.1\n"),
        )
        .unwrap();
        std::fs::write(
            dist.join("entry_points.txt"),
            format!("[meerschaum.plugins]\n{name} = {name}\n"),
        )
        .unwrap();
        std::fs::write(dist.join("RECORD"), format!("{name}.py,,\n")).unwrap();
    }

    fn launches(&self) -> usize {
        std::fs::read_to_string(&self.log)
            .map(|text| text.lines().count())
            .unwrap_or(0)
    }

    fn run(&self, args: &[&str], stdin: &str) -> Output {
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let repo = repo.canonicalize().unwrap_or(repo);
        let python_path = std::env::join_paths([repo, self.site.clone()]).unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"))
            .args(args)
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", &self.root)
            .env("PYTHONPATH", python_path)
            .env("MRSM_PYTHON", &self.python)
            .env("MRSM_ROOT_DIR", &self.root)
            .env("MRSM_PLUGINS_DIR", &self.plugins)
            .env("MRSM_NONINTERACTIVE", "1")
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(stdin.as_bytes())
            .unwrap();
        child.wait_with_output().unwrap()
    }
}

fn text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn an_entry_point_plugin_runs_when_the_plugins_directory_is_empty() {
    let setup = Setup::new();
    setup.install("epw17b");
    for args in [&["epw17b", "hello"][..], &["epw17b_hello"]] {
        let out = setup.run(args, "");
        let out_text = text(&out);
        assert!(out.status.success(), "{args:?}: {out_text}");
        assert!(
            out_text.contains("entry point says hi, loud=None"),
            "{args:?}: {out_text}"
        );
    }
    let flagged = text(&setup.run(&["epw17b", "hello", "--epw17b-loud"], ""));
    assert!(flagged.contains("loud=True"), "{flagged}");
}

#[test]
fn a_tree_without_any_plugin_starts_no_python_once_the_probe_is_cached() {
    let setup = Setup::new();
    let first = text(&setup.run(&["show", "version"], ""));
    assert!(first.contains("Meerschaum v"), "{first}");
    // The probe asked the interpreter once (no entry point, so no worker).
    assert_eq!(setup.launches(), 1, "{first}");
    for _ in 0..3 {
        setup.run(&["show", "version"], "");
    }
    let shell = text(&setup.run(&[], "show version\nexit\n"));
    assert!(shell.contains("Meerschaum v"), "{shell}");
    assert_eq!(setup.launches(), 1, "the cached probe starts no Python");
}

#[test]
fn installing_an_entry_point_plugin_is_noticed() {
    let setup = Setup::new();
    let before = text(&setup.run(&["show", "version"], ""));
    assert!(before.contains("Meerschaum v"), "{before}");
    let launches = setup.launches();
    // `pip install` changes the site directory's modification time.
    std::thread::sleep(std::time::Duration::from_millis(30));
    setup.install("epw17c");
    let out = setup.run(&["epw17c", "hello"], "");
    assert!(text(&out).contains("entry point says hi"), "{}", text(&out));
    assert!(setup.launches() > launches);
    // And the next run reuses both the probe and the manifest: no further launch.
    let settled = setup.launches();
    setup.run(&["show", "version"], "");
    assert_eq!(setup.launches(), settled);
}
