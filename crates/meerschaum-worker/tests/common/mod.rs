//! Shared harness for the end-to-end worker tests: a temporary root directory and a temporary
//! plugins directory populated with the synthetic plugins in `tests/plugins/`. Workers run the
//! repository's SDK; comparisons run the released v4.1.4 package from the reference environment.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;
use std::time::Duration;

use meerschaum_worker::{PoolConfig, WorkerOptions, WorkerPool};

/// The repository root.
pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root")
}

/// The interpreter for the tests.
pub fn python() -> PathBuf {
    meerschaum_worker::worker::default_python()
}

/// Whether Python with `pyarrow` and `pandas` is available (tests skip otherwise).
pub fn python_available() -> bool {
    static OK: OnceLock<bool> = OnceLock::new();
    *OK.get_or_init(|| {
        Command::new(python())
            .args(["-c", "import pyarrow, pandas"])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    })
}

/// Skip the test when Python is unavailable.
#[macro_export]
macro_rules! require_python {
    () => {
        if !$crate::common::python_available() {
            eprintln!("Skipping: Python with pyarrow and pandas is not available.");
            return;
        }
    };
}

fn copy_tree(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap().flatten() {
        let path = entry.path();
        let name = entry.file_name();
        if name == "__pycache__" {
            continue;
        }
        let target = dst.join(&name);
        if path.is_dir() {
            copy_tree(&path, &target);
        } else {
            std::fs::copy(&path, &target).unwrap();
        }
    }
}

/// An isolated Meerschaum environment.
pub struct TestEnv {
    pub root: tempfile::TempDir,
    pub plugins: tempfile::TempDir,
    pub extra_env: Vec<(String, String)>,
}

impl TestEnv {
    /// A fresh root and plugins directory with the test plugins; the root is initialized (config
    /// written, plugins symlinked) before any worker starts so concurrent workers never race.
    pub fn new() -> TestEnv {
        Self::with_env(Vec::new())
    }

    /// The directories without running Python (for tests that never start a worker).
    pub fn without_init() -> TestEnv {
        let root = tempfile::Builder::new()
            .prefix("mrsm-wk-root-")
            .tempdir()
            .unwrap();
        let plugins = tempfile::Builder::new()
            .prefix("mrsm-wk-plugins-")
            .tempdir()
            .unwrap();
        copy_tree(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/plugins"),
            plugins.path(),
        );
        // v4 imports pandas / pyarrow / SQLAlchemy from `<root>/venvs/mrsm`; reuse the repo's
        // test venv so nothing is installed per test.
        let shared_venv = repo_root().join("test_root/venvs/mrsm");
        #[cfg(unix)]
        if shared_venv.is_dir() {
            let venvs = root.path().join("venvs");
            std::fs::create_dir_all(&venvs).unwrap();
            std::os::unix::fs::symlink(&shared_venv, venvs.join("mrsm")).unwrap();
        }
        #[cfg(not(unix))]
        let _ = shared_venv;
        TestEnv {
            root,
            plugins,
            extra_env: Vec::new(),
        }
    }

    /// Like [`TestEnv::new`] with extra environment variables for every Python process.
    pub fn with_env(extra_env: Vec<(String, String)>) -> TestEnv {
        let mut env = Self::without_init();
        env.extra_env = extra_env;
        let out = env
            .python_command(
                "import meerschaum as mrsm\n\
                 from meerschaum.plugins import import_plugins\n\
                 mrsm.get_config('meerschaum')\n\
                 import_plugins()\n",
            )
            .output()
            .expect("python runs");
        assert!(
            out.status.success(),
            "initializing the root failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        env
    }

    /// The environment every Python process gets.
    pub fn env(&self) -> Vec<(String, String)> {
        let mut env = vec![
            (
                "PYTHONPATH".to_string(),
                repo_root().to_string_lossy().into_owned(),
            ),
            (
                "MRSM_ROOT_DIR".to_string(),
                self.root.path().to_string_lossy().into_owned(),
            ),
            (
                "MRSM_PLUGINS_DIR".to_string(),
                self.plugins.path().to_string_lossy().into_owned(),
            ),
            // Fetch-only tests use sql:main as an inert pipe key. Always resolve it to this
            // test's disposable SQLite file if v4 consults its instance capabilities.
            (
                "MRSM_SQL_MAIN".to_string(),
                format!(
                    "sqlite:///{}",
                    self.root.path().join("wiring_main.sqlite").display()
                ),
            ),
            (
                "UV_PYTHON".to_string(),
                python().to_string_lossy().into_owned(),
            ),
            ("MRSM_WORKER_ALLOW_CRASH".to_string(), "1".to_string()),
            ("PYTHONDONTWRITEBYTECODE".to_string(), "1".to_string()),
            // The test plugins are v4's (pandas): a fresh root would hand them Polars frames
            // (`meerschaum:dataframe`, rc15).
            ("MRSM_DATAFRAME".to_string(), "pandas".to_string()),
        ];
        env.extend(self.extra_env.iter().cloned());
        env
    }

    /// `python -c <code>` in this environment.
    pub fn python_command(&self, code: &str) -> Command {
        let mut cmd = Command::new(python());
        cmd.arg("-c").arg(code).current_dir(repo_root());
        for (k, v) in self.env() {
            cmd.env(k, v);
        }
        cmd
    }

    /// The released reference, outside the source tree and isolated from Python import overrides.
    pub fn reference_command(&self, code: &str) -> Command {
        let python = std::env::var_os("MRSM_TEST_PYTHON")
            .map(PathBuf::from)
            .unwrap_or_else(|| repo_root().join(".venv-ref/bin/python"));
        let guard = "import meerschaum as _reference\n\
            assert _reference.__version__ == '4.1.4', \
            f'Expected released Meerschaum 4.1.4, imported {_reference.__version__} from {_reference.__file__}'\n";
        let mut cmd = Command::new(python);
        cmd.args(["-s", "-E", "-c", &format!("{guard}{code}")])
            .current_dir(self.root.path())
            .env_remove("PYTHONPATH")
            .env_remove("PYTHONHOME")
            .env_remove("TMUX")
            .env_remove("TMUX_PANE");
        for (key, value) in self.env() {
            if key != "PYTHONPATH" {
                cmd.env(key, value);
            }
        }
        cmd
    }

    /// Run Python and return stdout (panicking with stderr on failure).
    pub fn run_python(&self, code: &str) -> String {
        let out = self.python_command(code).output().expect("python runs");
        assert!(
            out.status.success(),
            "python failed:\n{}\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    /// The worker options for this environment.
    pub fn worker_options(&self) -> WorkerOptions {
        WorkerOptions {
            python: python(),
            env: self.env(),
            current_dir: Some(repo_root()),
            startup_timeout: Duration::from_secs(120),
            quiet: std::env::var("MRSM_WORKER_TEST_VERBOSE").is_err(),
            ..WorkerOptions::default()
        }
    }

    /// A pool of `size` workers.
    pub fn pool(&self, size: usize) -> WorkerPool {
        WorkerPool::new(PoolConfig {
            size,
            worker: self.worker_options(),
            backoff_initial: Duration::from_millis(10),
            backoff_max: Duration::from_millis(100),
            ..PoolConfig::default()
        })
    }

    /// The plugins directories.
    pub fn plugins_dirs(&self) -> Vec<PathBuf> {
        vec![self.plugins.path().to_path_buf()]
    }
}

/// Sync with v4 into `v4_instance`, then dump both instances' `get_data()` through v4.
pub const V4_COMPARE: &str = r#"
import json, sys
import meerschaum as mrsm
from meerschaum.utils.dtypes import json_serialize_value
shapes = json.loads(sys.argv[1])
connector = sys.argv[2]
syncs = int(sys.argv[3])
v4_instance, rs_instance = sys.argv[4], sys.argv[5]
out = {'v4_sync': {}, 'data': {}}
for shape in shapes:
    pipe = mrsm.Pipe(connector, shape, instance=v4_instance)
    for _ in range(syncs):
        out['v4_sync'][shape] = list(pipe.sync())
for shape in shapes:
    out['data'][shape] = {}
    for inst in (v4_instance, rs_instance):
        pipe = mrsm.Pipe(connector, shape, instance=inst)
        df = pipe.get_data()
        if df is None:
            out['data'][shape][inst] = None
            continue
        out['data'][shape][inst] = {
            'columns': sorted(df.columns),
            'dtypes': {c: str(t) for c, t in sorted(df.dtypes.items())},
            'records': json.loads(json.dumps(
                [dict(sorted(r.items())) for r in df.to_dict(orient='records')],
                default=json_serialize_value,
            )),
            'parameters_columns': pipe.columns,
        }
print('\n@@JSON@@' + json.dumps(out))
"#;

/// Sync `connector`'s `metrics` with v4 into `v4_instance` (`syncs` times each), then read
/// both `v4_instance` and `rs_instance` back through v4's `get_data()`.
pub fn v4_compare(
    env: &TestEnv,
    connector: &str,
    metrics: &[&str],
    syncs: usize,
    v4_instance: &str,
    rs_instance: &str,
) -> serde_json::Value {
    let mut cmd = env.reference_command(V4_COMPARE);
    cmd.arg(serde_json::to_string(metrics).unwrap())
        .arg(connector)
        .arg(syncs.to_string())
        .arg(v4_instance)
        .arg(rs_instance);
    let out = cmd.output().expect("python runs");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "v4 comparison failed:\n{stdout}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let json = stdout.rsplit("@@JSON@@").next().unwrap();
    serde_json::from_str(json).expect("valid JSON")
}

/// Assert that every metric synced with v4 and that both instances hold the same data.
pub fn assert_same_data(
    result: &serde_json::Value,
    metrics: &[&str],
    v4_instance: &str,
    rs_instance: &str,
) {
    let mut failures = Vec::new();
    for shape in metrics {
        let v4_sync = &result["v4_sync"][shape];
        assert_eq!(v4_sync[0], true, "v4 failed to sync {shape}: {v4_sync}");
        let data = &result["data"][shape];
        let v4 = &data[v4_instance];
        let rs = &data[rs_instance];
        if v4 != rs {
            failures.push(format!(
                "{shape}:\n  v4: {}\n  rs: {}",
                serde_json::to_string(v4).unwrap(),
                serde_json::to_string(rs).unwrap()
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
