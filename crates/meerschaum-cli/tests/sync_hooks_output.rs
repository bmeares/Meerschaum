//! Ordered native hook output compared with the released v4 CLI on disposable SQLite roots.
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const SOURCE: &str = "from meerschaum.plugins import pre_sync_hook, post_sync_hook\n@pre_sync_hook\ndef first(pipe, **kw):\n    return True, 'HOOK_PRE_FIRST'\n@pre_sync_hook\ndef second(pipe, **kw):\n    return False, 'HOOK_PRE_FAILED'\n@pre_sync_hook\ndef ignored(pipe, **kw):\n    return None\n@post_sync_hook\ndef after(pipe, **kw):\n    assert kw['success_tuple'][0]\n    return True, 'HOOK_POST_FIRST'\n@post_sync_hook\ndef raises(pipe, **kw):\n    raise ValueError('HOOK_RAISED')\n@post_sync_hook\ndef final(pipe, **kw):\n    return True, 'HOOK_POST_FINAL'\n";

struct Sandbox {
    temp: tempfile::TempDir,
    python: PathBuf,
    reference: bool,
}

impl Sandbox {
    fn new(python: PathBuf, reference: bool) -> Self {
        let temp = tempfile::tempdir().unwrap();
        for dir in ["root/venvs", "plugins", "home"] {
            std::fs::create_dir_all(temp.path().join(dir)).unwrap();
        }
        #[cfg(unix)]
        for name in ["mrsm", "hook_output"] {
            // Share installed packages read-only; all job/config/plugin resources stay local.
            let environment = python.parent().unwrap().parent().unwrap();
            std::os::unix::fs::symlink(environment, temp.path().join("root/venvs").join(name))
                .unwrap();
        }
        std::fs::write(temp.path().join("plugins/hook_output.py"), SOURCE).unwrap();
        Self {
            temp,
            python,
            reference,
        }
    }

    fn run(&self, args: &[&str]) -> Output {
        let mut command = if self.reference {
            let mut command = Command::new(&self.python);
            command.args(["-E", "-m", "meerschaum"]);
            command
        } else {
            Command::new(env!("CARGO_BIN_EXE_mrsm-cli"))
        };
        command.args(args).current_dir(self.temp.path()).env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("HOME", self.temp.path().join("home"))
            .env("MRSM_ROOT_DIR", self.temp.path().join("root"))
            .env("MRSM_PLUGINS_DIR", self.temp.path().join("plugins"))
            .env("MRSM_PYTHON", &self.python)
            .env("UV_PYTHON", &self.python)
            .env("PYTHONPATH", Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
            .env("MRSM_CONFIG", r#"{"meerschaum":{"instance":"sql:local"},"shell":{"updates":{"check":false}},"formatting":{"ansi":false,"unicode":false},"system":{"engine":{"python_workers":1}}}"#)
            .output().unwrap()
    }

    fn ok(&self, args: &[&str]) -> String {
        let output = self.run(args);
        assert!(
            output.status.success(),
            "{args:?}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).into_owned()
    }
}

#[test]
fn hook_success_failure_and_exception_output_matches_reference_order() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    let reference_python = repo.join(".venv-ref/bin/python");
    let python = std::env::var_os("MRSM_TEST_HOOKS_PYTHON")
        .map(PathBuf::from)
        .or_else(|| {
            [repo.join(".venv/bin/python"), reference_python.clone()]
                .into_iter()
                .find(|path| path.is_file())
        });
    let Some(python) = python.filter(|_| reference_python.is_file()) else {
        eprintln!("skipped: local and released-reference Python environments are required");
        return;
    };
    let mut transcripts = Vec::new();
    for sandbox in [
        Sandbox::new(reference_python, true),
        Sandbox::new(python, false),
    ] {
        sandbox.ok(&[
            "register",
            "pipes",
            "-c",
            "sql:local",
            "-m",
            "hook_rows",
            "--params",
            r#"{"sql":"SELECT 1 AS id","columns":{"id":"id"}}"#,
            "--noask",
        ]);
        let output = sandbox.ok(&[
            "sync",
            "pipes",
            "-c",
            "sql:local",
            "-m",
            "hook_rows",
            "--nopretty",
        ]);
        let markers = output
            .lines()
            .map(str::trim)
            .filter(|line| {
                [
                    "HOOK_PRE_FIRST",
                    "HOOK_PRE_FAILED",
                    "HOOK_POST_FIRST",
                    "HOOK_POST_FINAL",
                ]
                .iter()
                .any(|marker| line.ends_with(marker))
            })
            .map(str::to_string)
            .collect::<Vec<_>>();
        assert_eq!(markers.len(), 4, "{output}");
        assert!(
            output.contains("Failed to execute sync hook 'raises'"),
            "{output}"
        );
        assert!(output.contains("ValueError: HOOK_RAISED"), "{output}");
        transcripts.push(markers);
    }
    assert_eq!(transcripts[1], transcripts[0]);
}
