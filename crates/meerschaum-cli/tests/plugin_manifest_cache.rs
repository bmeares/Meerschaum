//! The plugin manifest is built once and reused: an unchanged plugin tree never rebuilds it,
//! whatever Python discovered (a pip-style entry-point plugin has no fingerprint of its own in
//! the plugins directory), and the shell validates the tree once per prompt.
//!
//! Every test runs the built `mrsm` binary against synthetic plugins, a temporary root and a
//! `PYTHONPATH` that points the Python worker at this repository (a stale installed
//! Meerschaum would shadow it). A plugin that calls `attempt_import` of a package it does not
//! declare makes `build_manifest` warn once per build, so the warning count is the rebuild
//! count.
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

const WARNING: &str = "attempt_import('zzmarker_pkg')";

struct Setup {
    _dir: tempfile::TempDir,
    root: PathBuf,
    plugins: PathBuf,
    site: PathBuf,
}

impl Setup {
    /// A root, a plugins directory with `marker.py` (the warning) and `zz.py` (actions), and
    /// a site directory with a pip-style entry-point plugin `eppl`.
    fn new() -> Setup {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        let plugins = dir.path().join("plugins");
        let site = dir.path().join("site");
        let dist = site.join("eppl-0.1.dist-info");
        for path in [&root, &plugins, &dist] {
            std::fs::create_dir_all(path).unwrap();
        }
        std::fs::write(
            plugins.join("marker.py"),
            "from meerschaum.utils.packages import attempt_import\n\n\n\
             def fetch(pipe, **kw):\n    attempt_import('zzmarker_pkg')\n    return None\n",
        )
        .unwrap();
        std::fs::write(
            plugins.join("zz.py"),
            "import os\nfrom meerschaum.plugins import make_action\n\n\n\
             @make_action\ndef zz_greet(**kw):\n    return True, 'greeted from zz'\n\n\n\
             @make_action\ndef zz_spawn(**kw):\n    \
             open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'late.py'), 'w').write(\n        \
             \"from meerschaum.utils.packages import attempt_import\\n\"\n        \
             \"from meerschaum.plugins import make_action\\n\"\n        \
             \"def fetch(pipe, **kw):\\n    attempt_import('zzlate_pkg')\\n\"\n        \
             \"@make_action\\n\"\n        \
             \"def late_hello(**kw):\\n    return True, 'late says hi'\\n\")\n    \
             return True, 'spawned'\n",
        )
        .unwrap();
        std::fs::write(
            site.join("eppl.py"),
            "from meerschaum.plugins import make_action\n\n\n\
             @make_action\ndef ep_hello(**kw):\n    return True, 'entry point says hi'\n",
        )
        .unwrap();
        std::fs::write(
            dist.join("METADATA"),
            "Metadata-Version: 2.1\nName: eppl\nVersion: 0.1\n",
        )
        .unwrap();
        std::fs::write(
            dist.join("entry_points.txt"),
            "[meerschaum.plugins]\neppl = eppl\n",
        )
        .unwrap();
        std::fs::write(dist.join("RECORD"), "eppl.py,,\n").unwrap();
        Setup {
            _dir: dir,
            root,
            plugins,
            site,
        }
    }

    fn manifest_path(&self) -> PathBuf {
        self.root.join("cache").join("plugins_manifest.json")
    }

    /// Run `mrsm <args>` (or a shell fed `stdin` when `args` is empty).
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

fn count(haystack: &str, needle: &str) -> usize {
    haystack.matches(needle).count()
}

fn modified(path: &Path) -> std::time::SystemTime {
    std::fs::metadata(path).unwrap().modified().unwrap()
}

#[test]
fn entry_point_plugin_does_not_rebuild_the_manifest_every_run() {
    let setup = Setup::new();
    let first = text(&setup.run(&["show", "version"], ""));
    assert_eq!(count(&first, WARNING), 1, "{first}");
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(setup.manifest_path()).unwrap()).unwrap();
    // Python found the entry-point plugin, which is outside the plugins directory.
    assert!(
        manifest["plugins"]["eppl"]["path"].is_string(),
        "{manifest}"
    );
    let bytes = std::fs::read(setup.manifest_path()).unwrap();
    let stamp = modified(&setup.manifest_path());

    let second = text(&setup.run(&["show", "version"], ""));
    let third = text(&setup.run(&["show", "actions", "--nopretty"], ""));
    assert_eq!(count(&second, WARNING), 0, "{second}");
    assert_eq!(count(&third, WARNING), 0, "{third}");
    assert!(third.lines().any(|l| l.trim() == "ep_hello"), "{third}");
    assert_eq!(std::fs::read(setup.manifest_path()).unwrap(), bytes);
    assert_eq!(modified(&setup.manifest_path()), stamp);
}

#[test]
fn editing_an_entry_point_plugin_rebuilds_the_manifest_once() {
    let setup = Setup::new();
    setup.run(&["show", "version"], "");
    let stamp = modified(&setup.manifest_path());
    std::thread::sleep(std::time::Duration::from_millis(50));
    let edited = setup.site.join("eppl.py");
    let mut source = std::fs::read_to_string(&edited).unwrap();
    source.push_str("\n\n@make_action\ndef ep_again(**kw):\n    return True, 'again'\n");
    std::fs::write(&edited, source).unwrap();

    let rebuilt = text(&setup.run(&["show", "actions", "--nopretty"], ""));
    assert_eq!(count(&rebuilt, WARNING), 1, "{rebuilt}");
    assert!(rebuilt.lines().any(|l| l.trim() == "ep_again"), "{rebuilt}");
    assert!(modified(&setup.manifest_path()) > stamp);
    let after = text(&setup.run(&["show", "version"], ""));
    assert_eq!(count(&after, WARNING), 0, "{after}");
}

#[test]
fn shell_builds_the_manifest_once_and_runs_plugin_actions() {
    let setup = Setup::new();
    // PARITY: v4's shell imports plugins once per process; empty lines run nothing.
    let out = text(&setup.run(&[], "\n\n\nzz greet\n\nep hello\nshow version\n\n\n"));
    assert_eq!(count(&out, WARNING), 1, "{out}");
    assert!(out.contains("greeted from zz"), "{out}");
    assert!(out.contains("entry point says hi"), "{out}");
}

#[test]
fn shell_picks_up_a_plugin_written_by_a_command_at_the_next_prompt() {
    let setup = Setup::new();
    let out = text(&setup.run(&[], "zz spawn\n\nlate hello\nshow version\n"));
    assert!(out.contains("spawned"), "{out}");
    assert!(out.contains("late says hi"), "{out}");
    // The initial build, then one rebuild for `late.py`; `zz spawn` itself did not rebuild.
    assert_eq!(count(&out, WARNING), 2, "{out}");
    assert_eq!(count(&out, "attempt_import('zzlate_pkg')"), 1, "{out}");
    let after = text(&setup.run(&["show", "version"], ""));
    assert_eq!(count(&after, WARNING), 0, "{after}");
}

#[test]
fn reload_rebuilds_the_manifest_even_when_nothing_changed() {
    let setup = Setup::new();
    setup.run(&["show", "version"], "");
    let stamp = modified(&setup.manifest_path());
    std::thread::sleep(std::time::Duration::from_millis(50));
    let out = text(&setup.run(&["reload"], ""));
    assert_eq!(count(&out, WARNING), 1, "{out}");
    assert!(out.contains("Reloaded 3 plugins"), "{out}");
    assert!(modified(&setup.manifest_path()) > stamp);
    let after = text(&setup.run(&["show", "version"], ""));
    assert_eq!(count(&after, WARNING), 0, "{after}");
}

#[test]
fn plugin_flags_still_reach_actions_through_the_worker() {
    let setup = Setup::new();
    std::fs::write(
        setup.plugins.join("flags.py"),
        "from meerschaum.plugins import make_action, add_plugin_argument\n\n\n\
         add_plugin_argument('--zz-level', type=int, default=3, help='level')\n\
         add_plugin_argument('--zz-loud', action='store_true', help='loud')\n\n\n\
         @make_action\ndef flags_show(zz_level=None, zz_loud=None, **kw):\n    \
         return True, f'level={zz_level!r} loud={zz_loud!r}'\n",
    )
    .unwrap();
    let defaults = text(&setup.run(&["flags", "show"], ""));
    // v4 drops what was not given (`False`, `None`): the flag is absent, so `None`.
    assert!(defaults.contains("level=3 loud=None"), "{defaults}");
    let given = text(&setup.run(&["flags", "show", "--zz-level", "7", "--zz-loud"], ""));
    assert!(given.contains("level=7 loud=True"), "{given}");
    let abbreviated = text(&setup.run(&["flags", "show", "--zz-lev=9"], ""));
    assert!(abbreviated.contains("level=9"), "{abbreviated}");
}
