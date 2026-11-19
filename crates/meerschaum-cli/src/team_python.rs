//! Native trampoline: the kernel boundary is installed before Python or a wrapper can start.

use std::path::Path;

use meerschaum_core::team_scope::sandbox::{self, runtime};

// -I -S keeps site/.pth, user paths, Python environment and cwd out of interpreter startup.
// Every explicit import below runs after the inherited kernel boundary is already active.
const BOOTSTRAP: &str = r#"
import importlib.util, json, os, sys
runtime = json.loads(sys.argv[1])
args = sys.argv[2:]
# This optional path is fixed in the sealed native Runtime by the verified Factory,
# before launch. It is empty/read-only and never derived from Python environment.
# Redirect READS too: -B alone does not ignore an existing source-tree bytecode cache.
if runtime.get('sdk_pycache_prefix') is not None:
    if not runtime.get('api_only', False):
        raise SystemExit('A native SDK cache requires its private API runtime.')
    if sys.pycache_prefix != runtime['sdk_pycache_prefix'] or not sys.dont_write_bytecode:
        raise SystemExit('The native SDK cache was not selected before interpreter startup.')
class TrustedPackages:
    def find_spec(self, fullname, path=None, target=None):
        filename = runtime['packages'].get(fullname)
        if filename is None:
            return None
        locations = [os.path.dirname(filename)] if os.path.basename(filename) == '__init__.py' else None
        return importlib.util.spec_from_file_location(fullname, filename, submodule_search_locations=locations)
sys.meta_path.insert(0, TrustedPackages())
if runtime['prefix']:
    sys.prefix = sys.exec_prefix = runtime['prefix']
# Capture API provenance in the SDK's native module before a .pth hook can unset the
# environment. Only exact host-mapped package / site directories are searched here.
if runtime.get('api_only', False) or runtime.get('team_source') is not None:
    import importlib.machinery
    candidates = []
    package = runtime['packages'].get('meerschaum')
    if package:
        candidates.append(os.path.join(os.path.dirname(package), '_core'))
    # A measured Factory SDK must load its native module from the pinned package tree.
    if runtime.get('sdk_pycache_prefix') is None:
        candidates.extend(os.path.join(path, 'meerschaum', '_core') for path in runtime['site_paths'])
    native_path = next((os.path.join(directory, '_native' + suffix)
        for directory in candidates for suffix in importlib.machinery.EXTENSION_SUFFIXES
        if os.path.isfile(os.path.join(directory, '_native' + suffix))), None)
    if native_path is None:
        raise SystemExit('The API worker cannot capture its same-release native SDK policy.')
    name = 'meerschaum._core._native'
    loader = importlib.machinery.ExtensionFileLoader(name, native_path)
    native_spec = importlib.util.spec_from_file_location(name, native_path, loader=loader)
    native = importlib.util.module_from_spec(native_spec)
    loader.exec_module(native)
    if getattr(native, '__version__', None) != runtime['expected_release']:
        raise SystemExit('The API SDK release does not match its native launcher.')
    sys.modules[name] = native
    if runtime.get('api_only', False):
        report = json.loads(native.team_sandbox_enforce([]) or '{}')
        if not report.get('enforced') or report.get('mode') != 'require_sockets' or report.get('canary') != 'denied':
            raise SystemExit('The API SDK did not retain its mandatory native provenance.')
    else:
        selected = runtime['team_source']
        native.team_sources_capture(selected['path'], selected['sha256'])
import site
for path in runtime['site_paths']:
    site.addsitedir(path)
while args and args[0] in ('-P', '-u', '-B', '-I', '-S', '-s', '-E'):
    args.pop(0)
if len(args) < 2 or args[0] not in ('-c', '-m'):
    raise SystemExit('The confined worker needs one explicit Python module or script.')
kind, source, *remaining = args
sys.argv = ['-c' if kind == '-c' else source, *remaining]
if kind == '-c':
    exec(compile(source, '<string>', 'exec'), {'__name__': '__main__', '__builtins__': __builtins__})
else:
    import runpy
    runpy.run_module(source, run_name='__main__', alter_sys=True)
"#;

pub(super) fn exec_if_requested(arguments: &[String]) -> Option<i32> {
    if arguments.first().map(String::as_str) != Some(runtime::PYTHON_CHILD) {
        return None;
    }
    Some(match execute(arguments) {
        Ok(code) => code,
        Err(why) => {
            eprintln!("The isolated Python process was stopped before startup: {why}");
            1
        }
    })
}

fn execute(arguments: &[String]) -> Result<i32, String> {
    let report = sandbox::current().ok_or("No kernel sandbox was installed.")?;
    if !report.enforced || report.canary != sandbox::Canary::Denied {
        return Err("The native bootstrap did not prove its filesystem boundary.".into());
    }
    report.verdict()?;
    let path = std::env::var_os(sandbox::SPEC_VAR).ok_or("The host's sandbox spec is missing.")?;
    let spec = sandbox::Spec::read(Path::new(&path))
        .map_err(|_| "The host's sandbox spec is unavailable.")?;
    let runtime = spec
        .runtime
        .clone()
        .ok_or("The host did not prepare trusted interpreter provenance.")?;
    let current = std::env::current_exe()
        .and_then(std::fs::canonicalize)
        .map_err(|_| "The native bootstrap could not verify its executable.")?;
    if runtime.launcher != current
        || !runtime.launcher.is_absolute()
        || !runtime.python.is_absolute()
    {
        return Err("The native bootstrap does not match the host's runtime provenance.".into());
    }
    let requested = arguments
        .get(1)
        .filter(|path| !path.is_empty())
        .ok_or("No interpreter was requested.")?;
    let requested = Path::new(requested);
    let program = if requested.is_absolute() {
        requested.to_path_buf()
    } else if requested.components().count() == 1 {
        // The default worker uses 'python3'. Keep the host's venv selection and never use a
        // user-modified PATH to resolve a preselected interpreter.
        if runtime.python.file_name() == requested.file_name()
            || requested == Path::new("python3")
            || requested == Path::new("python")
        {
            runtime.python.clone()
        } else {
            return Err("A worker interpreter must have a host-selected absolute path.".into());
        }
    } else {
        return Err("A worker interpreter must have an absolute path.".into());
    };
    let mut bootstrap =
        serde_json::to_value(&runtime).map_err(|_| "Could not encode the trusted runtime.")?;
    bootstrap["expected_release"] = env!("CARGO_PKG_VERSION").into();
    if let Some(launch) = meerschaum_core::team_scope::source_namespace::launch()? {
        report.require_sockets_verdict()?;
        if launch.sandbox != spec {
            return Err(
                "The Team source namespace has another native confinement boundary.".into(),
            );
        }
        let descriptor = launch
            .namespace
            .cache
            .parent()
            .ok_or("The Team source namespace is unavailable.")?
            .join(meerschaum_core::team_scope::source_namespace::FILE);
        bootstrap["team_source"] =
            serde_json::json!({"path": descriptor, "sha256": launch.digest()?});
    }
    let mut command = std::process::Command::new(program);
    command.args(["-I", "-S", "-u"]);
    configure_sdk_cache(
        &mut command,
        runtime.api_only,
        runtime.sdk_pycache_prefix.as_deref(),
    )?;
    command
        .args(["-c", BOOTSTRAP])
        .arg(
            serde_json::to_string(&bootstrap)
                .map_err(|_| "Could not encode the trusted runtime.")?,
        )
        .args(&arguments[2..]);
    // A native loader never saw these values. They are restored only in the already-confined
    // interpreter's environment, from the host's read-only descriptor, not job properties.
    runtime::sanitize_command(&mut command);
    for (name, value) in runtime.environment {
        if !matches!(
            name.as_str(),
            "PATH"
                | "MRSM_BIN"
                | "MRSM_PYTHON"
                | "PYTHONPATH"
                | "PYTHONNOUSERSITE"
                | "PYTHONSAFEPATH"
                | "LD_LIBRARY_PATH"
                | "DYLD_LIBRARY_PATH"
                | "DYLD_FALLBACK_LIBRARY_PATH"
        ) {
            return Err(
                "The host runtime descriptor contains an unsupported environment setting.".into(),
            );
        }
        command.env(name, value);
    }
    command.env_remove("TMUX").env_remove("TMUX_PANE");
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        Err(format!(
            "The confined interpreter could not start: {}",
            command.exec()
        ))
    }
    #[cfg(not(unix))]
    {
        command
            .status()
            .map(|status| status.code().unwrap_or(1))
            .map_err(|error| format!("The confined interpreter could not start: {error}"))
    }
}

/// Only the sealed host Runtime selects this process-local prefix. Command-line -X is
/// active before ANY interpreter import, and -I prevents PYTHONPYCACHEPREFIX substitution.
fn configure_sdk_cache(
    command: &mut std::process::Command,
    api_only: bool,
    prefix: Option<&Path>,
) -> Result<(), String> {
    let Some(prefix) = prefix else { return Ok(()) };
    if !api_only || !prefix.is_absolute() {
        return Err("The attested SDK cache requires its sealed private API runtime.".into());
    }
    let mut value = std::ffi::OsString::from("pycache_prefix=");
    value.push(prefix);
    command.arg("-B").arg("-X").arg(value);
    Ok(())
}

#[cfg(all(test, target_os = "linux"))]
#[path = "team_python_sdk_cache_tests.rs"]
mod sdk_cache_tests;
