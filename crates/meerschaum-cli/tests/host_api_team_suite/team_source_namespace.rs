//! Actual native/SDK source namespace acceptance, no service/listener/default tmux.

use meerschaum_core::team_scope::{create_private_dir, sandbox, source_namespace, write_private};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn namespace(
    root: &Path,
    control_root: &Path,
    runtime: &sandbox::runtime::Runtime,
    runtime_rules: &[sandbox::Rule],
    digest_marker: char,
    sources: &[(&str, &Path)],
) -> PathBuf {
    let directory = control_root
        .join("execution-contexts")
        .join(uuid::Uuid::new_v4().simple().to_string())
        .join(format!("team-sources-{}", uuid::Uuid::new_v4().simple()));
    let selected = source_namespace::Namespace {
        root: root.into(),
        control_root: control_root.into(),
        view: directory.join("plugins"),
        package: directory.join("package/plugins"),
        cache: directory.join("cache"),
        catalog_sha256: Some(digest_marker.to_string().repeat(64)),
        names: sources
            .iter()
            .map(|(name, _)| (*name).to_string())
            .collect(),
    };
    for path in [&selected.view, &selected.package, &selected.cache] {
        create_private_dir(path).unwrap();
    }
    write_private(&selected.package.join("__init__.py"), "").unwrap();
    for (name, source) in sources {
        for path in [&selected.view, &selected.package] {
            std::os::unix::fs::symlink(source, path.join(format!("{name}.py"))).unwrap();
        }
    }
    let mut spec = sandbox::Spec {
        mode: sandbox::Mode::RequireSockets,
        team: "owned Team boundary fixture".into(),
        protected: vec![PathBuf::from("/"), control_root.into()],
        canary: sandbox::ensure_canary(control_root).unwrap(),
        runtime: Some(runtime.clone()),
        ..sandbox::Spec::default()
    };
    spec.rules = sandbox::SYSTEM_RULES
        .iter()
        .map(|(path, access, why)| sandbox::Rule::new(*path, *access, why))
        .chain(runtime_rules.iter().cloned())
        .chain([
            sandbox::Rule::new(root, sandbox::Access::ReadWrite, "durable Team work"),
            sandbox::Rule::new(&directory, sandbox::Access::Read, "own Team namespace"),
            sandbox::Rule::new(
                &selected.cache,
                sandbox::Access::ReadWrite,
                "own Team cache",
            ),
        ])
        .chain(sources.iter().map(|(_, source)| {
            sandbox::Rule::new(*source, sandbox::Access::ReadExec, "exact own source")
        }))
        .collect();
    assert!(spec.allowed(spec.rules.clone()).1.is_empty());
    write_private(
        &directory.join(sandbox::SPEC_FILE),
        &serde_json::to_string(&spec).unwrap(),
    )
    .unwrap();
    let launch = source_namespace::Launch {
        schema: 1,
        release: env!("CARGO_PKG_VERSION").into(),
        namespace: selected,
        runtime: runtime.clone(),
        sandbox: spec,
    };
    let path = directory.join(source_namespace::FILE);
    write_private(&path, &serde_json::to_string(&launch).unwrap()).unwrap();
    source_namespace::Launch::read(&path).unwrap();
    path
}
fn literal(path: &Path) -> String {
    serde_json::to_string(path.to_str().unwrap()).unwrap()
}

fn seed_venv(python: &Path, home: &Path, directory: &Path) -> PathBuf {
    let output = Command::new(python)
        .env_clear()
        .env("HOME", home)
        .env("PATH", "/usr/local/bin:/usr/bin:/bin")
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .args([
            "-I",
            "-B",
            "-c",
            r#"
import sys
import venv
from pathlib import Path

root = Path(sys.argv[1])
venv.EnvBuilder(with_pip=False, symlinks=True).create(root)
version = f'python{sys.version_info.major}.{sys.version_info.minor}'
print(root / 'lib' / version / 'site-packages')
"#,
        ])
        .arg(directory)
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr),
    );
    let site = PathBuf::from(std::str::from_utf8(&output.stdout).unwrap().trim());
    assert!(site.starts_with(directory) && site.is_dir());
    site
}

#[test]
#[ignore = "requires an explicitly selected owned MRSM_TEST_PYTHON and freshly built same-release SDK"]
fn native_team_sources_survive_site_mutation_and_reload_without_moving_durable_state() {
    let python = PathBuf::from(std::env::var_os("MRSM_TEST_PYTHON").expect(
        "Set MRSM_TEST_PYTHON to the owned same-release SDK interpreter; no ambient fallback.",
    ));
    assert!(python.is_absolute() && python.is_file());
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_mrsm-cli"))
        .canonicalize()
        .unwrap();
    let _ = meerschaum_core::self_exe::register_native_current_executable();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("work");
    create_private_dir(&root).unwrap();
    let root = root.canonicalize().unwrap();
    let control = temp.path().join("control");
    create_private_dir(&control).unwrap();
    let own_site = root.join("owned-site");
    create_private_dir(&own_site).unwrap();
    let home = root.join("home");
    create_private_dir(&home).unwrap();
    let venvs = root.join("venvs");
    let alpha_site = seed_venv(&python, &home, &venvs.join("alpha"));
    seed_venv(&python, &home, &venvs.join("mrsm"));
    write_private(
        &alpha_site.join("namespace_fixture_dependency.py"),
        "VALUE = 11\n",
    )
    .unwrap();
    let durable = root.join("durable-owned-state");
    write_private(&durable, "unchanged").unwrap();
    let shared = root.join(".internal/plugins");
    create_private_dir(&shared).unwrap();
    write_private(&shared.join("__init__.py"), "").unwrap();
    let object = control.join("plugins/.executor-objects/alpha/source");
    create_private_dir(&object).unwrap();
    let alpha = object.join("alpha.py");
    write_private(
        &alpha,
        "# Fixture with an installed dependency.\nrequired = ['namespace-fixture-dependency']\nimport namespace_fixture_dependency\nVALUE = namespace_fixture_dependency.VALUE\n",
    )
    .unwrap();
    let fresh = object.join("fresh.py");
    write_private(&fresh, "import json\nVALUE = json.loads('17')\n").unwrap();
    let needs = object.join("needs.py");
    write_private(
        &needs,
        "# Fixture with a dependency installed between launches.\nrequired = ['namespace-fixture-late-dependency']\nimport namespace_fixture_late_dependency\nVALUE = namespace_fixture_late_dependency.VALUE\n",
    )
    .unwrap();
    let broken = object.join("broken.py");
    write_private(&broken, "VALUE = 'must not run without a usable site'\n").unwrap();
    create_private_dir(&venvs.join("broken")).unwrap();
    let sibling = control.join("plugins/.executor-objects/beta/source");
    create_private_dir(&sibling).unwrap();
    let beta = sibling.join("beta.py");
    let beta_marker = root.join("beta-imported");
    write_private(
        &beta,
        &format!(
            "open({}, 'w').write('wrong source')\n",
            literal(&beta_marker)
        ),
    )
    .unwrap();
    std::os::unix::fs::symlink(&beta, shared.join("beta.py")).unwrap();
    let config = root.join("launch-config.json");
    write_private(&config,r#"{"system":{"packages":{"auto_install":false}},"meerschaum":{"instance":"sql:fixture","connectors":{"sql":{"fixture":{"flavor":"sqlite","database":":memory:"}}}}}"#).unwrap();
    let mut environment: Vec<_> = meerschaum_core::process_env::vars()
        .into_iter()
        .filter(|(key, _)| {
            matches!(
                key.as_str(),
                "PATH"
                    | "PYTHONPATH"
                    | "LD_LIBRARY_PATH"
                    | "DYLD_LIBRARY_PATH"
                    | "DYLD_FALLBACK_LIBRARY_PATH"
            )
        })
        .collect();
    environment.push(("MRSM_PYTHON".into(), python.display().to_string()));
    let mut model = sandbox::Spec::default();
    let mut runtime = sandbox::runtime::Runtime::prepare(&mut model, &environment).unwrap();
    runtime.launcher = binary.clone();
    runtime.site_paths.push(own_site.clone());
    model.rules.push(sandbox::Rule::new(
        &binary,
        sandbox::Access::ReadExec,
        "native fixture launcher",
    ));
    let original = namespace(
        &root,
        &control,
        &runtime,
        &model.rules,
        'a',
        &[
            ("alpha", &alpha),
            ("broken", &broken),
            ("fresh", &fresh),
            ("needs", &needs),
        ],
    );
    let other = namespace(
        &root,
        &control,
        &runtime,
        &model.rules,
        'b',
        &[("beta", &beta)],
    );
    let selected = source_namespace::Launch::read(&original).unwrap();
    let site_marker = root.join("site-ran");
    let secret_dir = temp.path().join("run/secrets");
    create_private_dir(&secret_dir).unwrap();
    let key = secret_dir.join("factory-private-key");
    write_private(&key, "fake-factory-authority-do-not-inherit").unwrap();
    let key_fd = std::fs::File::open(&key).unwrap();
    use std::os::fd::AsRawFd;
    let parent_fd = format!("/proc/{}/fd/{}", std::process::id(), key_fd.as_raw_fd());
    let own_fd = format!("/proc/self/fd/{}", key_fd.as_raw_fd());
    let api = control
        .join("execution-contexts")
        .join(uuid::Uuid::new_v4().simple().to_string())
        .join("generation-api");
    let metadata = control
        .join("execution-contexts")
        .join(uuid::Uuid::new_v4().simple().to_string())
        .join("generation-metadata");
    for path in [&api, &metadata] {
        create_private_dir(path).unwrap();
    }
    let api_secret = api.join("credential.json");
    let metadata_source = metadata.join("source.rs");
    write_private(&api_secret, "sibling-api-credential").unwrap();
    write_private(&metadata_source, "sibling-metadata-source").unwrap();
    let lifecycle = control.join("executor-retired-jobs");
    create_private_dir(&lifecycle).unwrap();
    let proof = lifecycle.join("sealed-proof.json");
    write_private(&proof, "sealed-evidence").unwrap();
    let pth = format!(
        "import os; os.environ['MRSM_PLUGINS_DIR']={}; os.environ['MRSM_EXECUTOR_PLUGINS']='[\"beta\"]'; os.environ['MRSM_TEAM_SOURCE_CONTEXT']={}; open({},'w').write('ran')\n",
        literal(&shared),
        literal(&other),
        literal(&site_marker)
    );
    write_private(&own_site.join("namespace_mutation.pth"), &pth).unwrap();
    let script = format!(
        r#"
import importlib, json, pathlib, os, stat
for target in {denied_reads}:
    try:
        descriptor = os.open(target, os.O_RDONLY | os.O_NONBLOCK)
    except (PermissionError, FileNotFoundError, OSError):
        pass
    else:
        os.close(descriptor)
        raise AssertionError('Team read a private Factory/sibling path')
for target in {denied_writes}:
    try:
        pathlib.Path(target).write_text('tampered')
    except (PermissionError, OSError):
        pass
    else:
        raise AssertionError('Team changed Factory source or lifecycle state')
try:
    descriptor = os.open({own_fd}, os.O_RDONLY | os.O_NONBLOCK)
except OSError:
    pass
else:
    try:
        if stat.S_ISREG(os.fstat(descriptor).st_mode):
            assert os.read(descriptor, 64) != b'fake-factory-authority-do-not-inherit'
    finally:
        os.close(descriptor)
for directory in {denied_directories}:
    try:
        os.listdir(directory)
    except OSError:
        pass
    else:
        raise AssertionError('Team enumerated private Factory storage')
import meerschaum as mrsm
from meerschaum import _core
import meerschaum.config.paths as paths
from meerschaum.plugins import Plugin, import_plugins, reload_plugins, sandboxed_plugins_names
from meerschaum.utils.venv import is_venv_active
assert pathlib.Path(paths.ROOT_DIR_PATH) == pathlib.Path({root})
assert pathlib.Path(paths.VIRTENV_RESOURCES_PATH) == pathlib.Path({venvs})
assert pathlib.Path(paths.PLUGINS_RESOURCES_PATH) == pathlib.Path({package})
names = ['alpha', 'broken', 'fresh', 'needs']
assert json.loads(_core.team_sources_namespace())['names'] == names
assert sandboxed_plugins_names() == names
assert Plugin('alpha').get_required_packages() == ['namespace-fixture-dependency']
assert import_plugins('alpha').VALUE == 11
assert import_plugins('fresh').VALUE == 17
assert not is_venv_active('fresh')
assert not (paths.VIRTENV_RESOURCES_PATH / 'fresh').exists()
needs = Plugin('needs')
assert needs.get_required_packages() == ['namespace-fixture-late-dependency']
if (paths.VIRTENV_RESOURCES_PATH / 'needs').exists():
    assert needs.module.VALUE == 29
else:
    import contextlib, io
    warnings = io.StringIO()
    with contextlib.redirect_stderr(warnings):
        assert import_plugins('needs') is None
    assert 'namespace_fixture_late_dependency' in warnings.getvalue()
    assert needs.module is None
    assert isinstance(needs.import_error, ModuleNotFoundError)
    assert needs.import_error.name == 'namespace_fixture_late_dependency'
    assert not (paths.VIRTENV_RESOURCES_PATH / 'needs').exists()
broken = Plugin('broken')
assert broken.module is None
assert isinstance(broken.import_error, OSError)
assert not is_venv_active('broken')
assert "Could not activate virtual environment 'broken'" in str(broken.import_error)
reload_plugins(['alpha'])
assert import_plugins('alpha').VALUE == 11
assert import_plugins('fresh').VALUE == 17
assert broken.module is None
assert isinstance(broken.import_error, OSError)
try:
    importlib.import_module('plugins.beta')
except ModuleNotFoundError:
    pass
else:
    raise AssertionError('a sibling or shared stale source entered the exact package')
assert pathlib.Path({durable}).read_text() == 'unchanged'
assert pathlib.Path({site_marker}).read_text() == 'ran'
assert not pathlib.Path({beta_marker}).exists()
print('native-team-namespace-ok')
"#,
        root = literal(&root),
        venvs = literal(&venvs),
        package = literal(&selected.namespace.package),
        durable = literal(&durable),
        site_marker = literal(&site_marker),
        beta_marker = literal(&beta_marker),
        denied_reads = serde_json::to_string(&[
            &key,
            &PathBuf::from(&parent_fd),
            &api_secret,
            &metadata_source,
            &beta,
            &proof
        ])
        .unwrap(),
        denied_writes = serde_json::to_string(&[
            &key,
            &api_secret,
            &metadata_source,
            &alpha,
            &beta,
            &fresh,
            &needs,
            &broken,
            &proof,
        ])
        .unwrap(),
        denied_directories =
            serde_json::to_string(&[&control, &control.join("execution-contexts"), &sibling])
                .unwrap(),
        own_fd = serde_json::to_string(&own_fd).unwrap()
    );
    let boundary_probe = script.split("\nimport meerschaum as mrsm").next().unwrap();
    let site_proof = root.join("site-boundary-proved");
    write_private(
        &own_site.join("namespace_boundary_probe.py"),
        &format!(
            "{boundary_probe}\npathlib.Path({}).write_text('proved before site mutation')\n",
            literal(&site_proof)
        ),
    )
    .unwrap();
    write_private(
        &own_site.join("namespace_mutation.pth"),
        &format!("import namespace_boundary_probe\n{pth}"),
    )
    .unwrap();
    let descendants = meerschaum_jobs::process_children::OwnedChildren::prepare().unwrap();
    for (entry, dependency_installed) in [
        (source_namespace::PYTHON_CHILD, false),
        (sandbox::runtime::PYTHON_CHILD, true),
    ] {
        assert!(!venvs.join("fresh").exists());
        if dependency_installed {
            assert!(!venvs.join("needs").exists());
            let needs_site = seed_venv(&python, &home, &venvs.join("needs"));
            write_private(
                &needs_site.join("namespace_fixture_late_dependency.py"),
                "VALUE = 29\n",
            )
            .unwrap();
        }
        let mut child = Command::new(&binary)
            .args([entry, python.to_str().unwrap(), "-c", &script])
            .env_clear()
            .env("PATH", "/usr/local/bin:/usr/bin:/bin")
            .env("HOME", &home)
            .env("MRSM_ROOT_DIR", &root)
            .env("MRSM_CONFIG_FILE", &config)
            .env("MRSM_NO_AUTO_INSTALL", "1")
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .env("MRSM_VENVS_DIR", &venvs)
            .env("MRSM_PYTHON", &python)
            .env("PIP_NO_INDEX", "1")
            .env("UV_OFFLINE", "1")
            .env(source_namespace::ENV, &original)
            .env(
                sandbox::SPEC_VAR,
                original.parent().unwrap().join(sandbox::SPEC_FILE),
            )
            .env("MRSM_PLUGINS_DIR", &selected.namespace.view)
            .env(
                "MRSM_EXECUTOR_PLUGINS",
                serde_json::to_string(&selected.namespace.names).unwrap(),
            )
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let until = Instant::now() + Duration::from_secs(20);
        let mut expired = false;
        loop {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            if Instant::now() >= until {
                expired = true;
                let _ = child.kill();
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        let _ = child.wait();
        let output = child.wait_with_output().unwrap();
        assert!(
            !expired,
            "the owned native source fixture exceeded its20s deadline"
        );
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr[..output.stderr.len().min(64 * 1024)])
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("native-team-namespace-ok"));
        assert_eq!(
            std::fs::read_to_string(&site_proof).unwrap(),
            "proved before site mutation"
        );
    }
    descendants.retire();
    assert!(!venvs.join("fresh").exists());
    assert!(shared.join("beta.py").is_symlink());
    assert!(!beta_marker.exists());
    assert_eq!(
        std::fs::read_to_string(&key).unwrap(),
        "fake-factory-authority-do-not-inherit"
    );
    assert_eq!(
        std::fs::read_to_string(&api_secret).unwrap(),
        "sibling-api-credential"
    );
    assert_eq!(
        std::fs::read_to_string(&metadata_source).unwrap(),
        "sibling-metadata-source"
    );
    assert_eq!(std::fs::read_to_string(&proof).unwrap(), "sealed-evidence");
}
