//! `prune venvs` end to end: the real uninstall path through the Python worker, on a throwaway
//! virtual environment in a temporary root (wave 9, task T).
//!
//! The test builds three tiny pure-Python wheels, installs them into `venvs/w9t_demo` of a
//! temporary `MRSM_ROOT_DIR` (offline, with `uv pip`), records who requested what with v4's own
//! `record_request` (the code `attempt_import` calls), and runs the `mrsm` binary against it.
//! Nothing outside the temporary root is written: the root, its empty plugins directory and the
//! worker environment are explicit, `venvs/mrsm` is a symlink to the reference environment
//! (`.venv-ref`) that is only read, and every run prunes the named throwaway environment only.
//!
//! The tests skip (with a message) when `uv`, `python3`, the `uv` Python package or the reference
//! environment is missing, and on non-Unix platforms.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

const VENV: &str = "w9t_demo";

/// Builds `w9t_wanted` (requires `w9t_dep`), `w9t_dep`, `w9t_stale` and `w9t_shared` wheels.
const BUILD_WHEELS: &str = r#"
import base64, hashlib, os, sys, zipfile
out = sys.argv[1]
def make(name, version, requires=()):
    dist = f"{name}-{version}"
    files = {
        f"{name}/__init__.py": f"VALUE = {version!r}\n",
        f"{dist}.dist-info/METADATA": f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n"
            + "".join(f"Requires-Dist: {r}\n" for r in requires),
        f"{dist}.dist-info/WHEEL": "Wheel-Version: 1.0\nGenerator: w9t\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
    }
    record = []
    for path, text in files.items():
        digest = base64.urlsafe_b64encode(hashlib.sha256(text.encode()).digest()).rstrip(b"=").decode()
        record.append(f"{path},sha256={digest},{len(text.encode())}")
    record.append(f"{dist}.dist-info/RECORD,,")
    files[f"{dist}.dist-info/RECORD"] = "\n".join(record) + "\n"
    with zipfile.ZipFile(os.path.join(out, f"{dist}-py3-none-any.whl"), "w") as z:
        for path, text in files.items():
            z.writestr(path, text)
make("w9t_wanted", "1.0", ["w9t_dep>=1.0"])
make("w9t_dep", "1.0")
make("w9t_stale", "1.0")
make("w9t_shared", "1.0")
"#;

/// Record the requests like `attempt_import` does.
const RECORD_REQUESTS: &str = r#"
from meerschaum.utils.venv._requests import record_request
assert record_request("w9t_demo", "w9t-wanted>=1.0", "meerschaum")
assert record_request("w9t_demo", "w9t_stale", "gone_plugin")
assert record_request("w9t_demo", "w9t_shared", "gone_plugin")
assert record_request("w9t_demo", "w9t_shared", "meerschaum")
"#;

fn repo_root() -> PathBuf {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    repo.canonicalize().unwrap_or(repo)
}

fn on_path(program: &str) -> Option<PathBuf> {
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|dir| dir.join(program))
            .find(|candidate| candidate.is_file())
    })
}

/// The temporary root and the facts the assertions need.
struct Fixture {
    _tmp: tempfile::TempDir,
    root: PathBuf,
    python: PathBuf,
    reference: PathBuf,
}

impl Fixture {
    fn venv(&self) -> PathBuf {
        self.root.join("venvs").join(VENV)
    }

    fn site(&self) -> PathBuf {
        std::fs::read_dir(self.venv().join("lib"))
            .unwrap()
            .flatten()
            .map(|e| e.path().join("site-packages"))
            .find(|p| p.is_dir())
            .expect("a site-packages directory")
    }

    fn ledger_path(&self) -> PathBuf {
        self.venv().join(".mrsm_requests.json")
    }

    fn ledger(&self) -> serde_json::Value {
        serde_json::from_str(&std::fs::read_to_string(self.ledger_path()).unwrap()).unwrap()
    }

    fn installed(&self) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(self.site())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter_map(|n| n.strip_suffix(".dist-info").map(str::to_string))
            .filter(|n| n.starts_with("w9t_"))
            .collect();
        names.sort();
        names
    }

    /// `python3` with this repository importable and the temporary root selected.
    fn python(&self, code: &str, cwd: &Path) -> Output {
        self.worker_env(Command::new(&self.python))
            .args(["-c", code])
            .current_dir(cwd)
            .output()
            .unwrap()
    }

    fn worker_env(&self, mut command: Command) -> Command {
        for (key, _) in std::env::vars() {
            if key.starts_with("MRSM_") {
                command.env_remove(key);
            }
        }
        command
            .env("MRSM_ROOT_DIR", &self.root)
            .env("MRSM_PLUGINS_DIR", self.root.join("plugins"))
            .env("PYTHONPATH", repo_root())
            .env("PYTHONDONTWRITEBYTECODE", "1");
        command
    }

    /// Run `mrsm ARGS` (the debug binary), feeding `stdin`.
    fn mrsm(&self, args: &[&str], stdin: &str) -> (i32, String) {
        use std::io::Write;
        let mut child = self
            .worker_env(Command::new(env!("CARGO_BIN_EXE_mrsm")))
            .args(args)
            .current_dir(&self.root)
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
        let output = child.wait_with_output().unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        (output.status.code().unwrap_or(-1), text)
    }
}

/// The names (not the contents) of the reference environment's top level, and whether it grew
/// a requests ledger: the one thing a worker could write there.
fn reference_snapshot(reference: &Path) -> (Vec<String>, bool) {
    let mut names: Vec<String> = std::fs::read_dir(reference)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    (names, reference.join(".mrsm_requests.json").exists())
}

fn skip(reason: &str) -> Option<Fixture> {
    eprintln!("skipping: {reason}");
    None
}

/// Build the throwaway environment, or `None` (with a message) when the tools are missing.
fn fixture() -> Option<Fixture> {
    let Some(uv) = on_path("uv") else {
        return skip("`uv` is not on PATH");
    };
    let Some(python) = on_path("python3") else {
        return skip("`python3` is not on PATH");
    };
    let reference = repo_root().join(".venv-ref");
    if !reference.is_dir() {
        return skip(".venv-ref is missing (run scripts/parity/setup_reference.sh)");
    }
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("root");
    std::fs::create_dir_all(root.join("plugins")).unwrap();
    std::fs::create_dir_all(root.join("venvs")).unwrap();
    // The worker imports Meerschaum from this repository, whose own dependencies live in the
    // `mrsm` environment; point it at the reference environment (read only) instead of letting
    // the first import install them from the network.
    std::os::unix::fs::symlink(&reference, root.join("venvs").join("mrsm")).unwrap();
    let fixture = Fixture {
        _tmp: tmp,
        root,
        python,
        reference,
    };

    // The worker's `uninstall packages` uses the `uv` Python package for `uv pip`.
    let probe = fixture.python("import uv, meerschaum.worker", &fixture.root);
    if !probe.status.success() {
        return skip(&format!(
            "the Python worker cannot start here: {}",
            String::from_utf8_lossy(&probe.stderr)
                .lines()
                .last()
                .unwrap_or_default()
        ));
    }

    let wheels = fixture.root.join("wheels");
    std::fs::create_dir_all(&wheels).unwrap();
    let built = Command::new(&fixture.python)
        .args(["-c", BUILD_WHEELS])
        .arg(&wheels)
        .status()
        .unwrap();
    assert!(built.success(), "building the wheels");
    // The venv uses the interpreter the worker runs, so `uv` finds its `site-packages`.
    let made = Command::new(&uv)
        .args(["venv", "--quiet", "--python"])
        .arg(&fixture.python)
        .arg(fixture.venv())
        .output()
        .unwrap();
    assert!(
        made.status.success(),
        "{}",
        String::from_utf8_lossy(&made.stderr)
    );
    let mut install = Command::new(&uv);
    install
        .args([
            "pip",
            "install",
            "--quiet",
            "--no-deps",
            "--offline",
            "--python",
        ])
        .arg(fixture.venv().join("bin").join("python"));
    for entry in std::fs::read_dir(&wheels).unwrap().flatten() {
        install.arg(entry.path());
    }
    let installed = install.output().unwrap();
    assert!(
        installed.status.success(),
        "{}",
        String::from_utf8_lossy(&installed.stderr)
    );
    assert_eq!(
        fixture.installed(),
        vec![
            "w9t_dep-1.0",
            "w9t_shared-1.0",
            "w9t_stale-1.0",
            "w9t_wanted-1.0"
        ]
    );
    let recorded = fixture.python(RECORD_REQUESTS, &fixture.root);
    assert!(
        recorded.status.success(),
        "{}",
        String::from_utf8_lossy(&recorded.stderr)
    );
    Some(fixture)
}

#[test]
fn prune_venvs_uninstalls_unrequested_packages_and_keeps_the_rest() {
    let Some(fixture) = fixture() else { return };
    let before = reference_snapshot(&fixture.reference);
    let ledger_before = std::fs::read_to_string(fixture.ledger_path()).unwrap();
    let everything: Vec<String> = fixture.installed();

    // Dry run: the plan names the package nobody wants; nothing changes. With no venv named the
    // reference environment (`mrsm`) is scanned too, and has nothing recorded, so nothing to prune.
    let (code, out) = fixture.mrsm(&["prune", "venvs", "--dry"], "");
    assert_eq!(code, 0, "{out}");
    assert!(
        out.contains("w9t-stale 1.0 (requested by: gone_plugin)"),
        "{out}"
    );
    assert!(out.contains("Would remove 1 package (dry run)."), "{out}");
    assert!(!out.contains("w9t-wanted 1.0 (requested by"), "{out}");
    assert!(!out.contains("w9t-shared 1.0 (requested by"), "{out}");
    assert!(!out.contains("Virtual environment 'mrsm'"), "{out}");
    assert_eq!(fixture.installed(), everything);
    assert_eq!(
        std::fs::read_to_string(fixture.ledger_path()).unwrap(),
        ledger_before
    );

    // `--noask` without `-y` refuses; answering no refuses too.
    let (code, out) = fixture.mrsm(&["prune", "venvs", VENV, "--noask"], "");
    assert_eq!(code, 1, "{out}");
    assert!(out.contains("Skipping prune. Add `-y` or `-f`"), "{out}");
    let (code, out) = fixture.mrsm(&["prune", "venvs", VENV], "n\n");
    assert_eq!(code, 1, "{out}");
    assert!(out.contains("Nothing was pruned."), "{out}");
    assert_eq!(fixture.installed(), everything);

    // `-y` uninstalls through the Python worker (`uv pip uninstall` in the throwaway venv).
    let (code, out) = fixture.mrsm(&["prune", "venvs", VENV, "-y"], "");
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("Removed 1 package."), "{out}");
    assert_eq!(
        fixture.installed(),
        vec!["w9t_dep-1.0", "w9t_shared-1.0", "w9t_wanted-1.0"],
        "{out}"
    );
    assert!(!fixture.site().join("w9t_stale").exists());
    assert!(
        fixture
            .site()
            .join("w9t_wanted")
            .join("__init__.py")
            .exists()
    );

    // The ledger lost exactly the removed package; the rest is untouched.
    let ledger = fixture.ledger();
    let packages = ledger["packages"].as_object().unwrap();
    assert_eq!(
        packages.keys().cloned().collect::<Vec<_>>(),
        vec!["w9t-shared", "w9t-wanted"]
    );
    let before_json: serde_json::Value = serde_json::from_str(&ledger_before).unwrap();
    assert_eq!(
        packages["w9t-wanted"],
        before_json["packages"]["w9t-wanted"]
    );
    assert_eq!(
        packages["w9t-shared"],
        before_json["packages"]["w9t-shared"]
    );
    assert_eq!(ledger["schema_version"], 1);
    let leftovers: Vec<_> = std::fs::read_dir(fixture.venv())
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");

    // A second run has nothing left to do (the dependency has no ledger entry and is kept).
    let (code, out) = fixture.mrsm(&["prune", "venvs", VENV, "-y"], "");
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("No packages to prune."), "{out}");

    // Nothing outside the temporary root changed: the reference environment is only read.
    assert_eq!(reference_snapshot(&fixture.reference), before);
    assert!(
        fixture
            .root
            .join("venvs")
            .join("mrsm")
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink()
    );
}

#[test]
fn prune_venvs_keeps_everything_a_core_request_or_a_dependency_needs() {
    let Some(fixture) = fixture() else { return };
    // Ask for the stale package as `meerschaum` too: nothing is left to remove.
    let recorded = fixture.python(
        "from meerschaum.utils.venv._requests import record_request\n\
         assert record_request('w9t_demo', 'w9t_stale', 'meerschaum')",
        &fixture.root,
    );
    assert!(recorded.status.success());
    let before = fixture.installed();
    let (code, out) = fixture.mrsm(&["prune", "venvs", VENV, "-y"], "");
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("No packages to prune."), "{out}");
    assert_eq!(fixture.installed(), before);
}
