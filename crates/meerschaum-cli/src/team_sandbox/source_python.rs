//! Preserve a credentialed Team's durable root while capturing its selected
//! plugin package/cache in the same-release SDK before interpreter site hooks.

use meerschaum_core::team_scope::{sandbox::runtime, source_namespace};
use std::path::Path;

const BOOTSTRAP: &str = r#"
import importlib.util, json, os, sys
runtime = json.loads(sys.argv[1])
descriptor, descriptor_sha256 = sys.argv[2:4]
args = sys.argv[4:]
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
import importlib.machinery
candidates = []
package = runtime['packages'].get('meerschaum')
if package:
    candidates.append(os.path.join(os.path.dirname(package), '_core'))
candidates.extend(os.path.join(path, 'meerschaum', '_core') for path in runtime['site_paths'])
native_path = next((os.path.join(directory, '_native' + suffix)
    for directory in candidates for suffix in importlib.machinery.EXTENSION_SUFFIXES
    if os.path.isfile(os.path.join(directory, '_native' + suffix))), None)
if native_path is None:
    raise SystemExit('The Team worker cannot capture its same-release native SDK source namespace.')
name = 'meerschaum._core._native'
loader = importlib.machinery.ExtensionFileLoader(name, native_path)
native_spec = importlib.util.spec_from_file_location(name, native_path, loader=loader)
native = importlib.util.module_from_spec(native_spec)
loader.exec_module(native)
if getattr(native, '__version__', None) != runtime['expected_release']:
    raise SystemExit('The Team SDK release does not match its native launcher.')
sys.modules[name] = native
native.team_sources_capture(descriptor, descriptor_sha256)
import site
for path in runtime['site_paths']:
    site.addsitedir(path)
while args and args[0] in ('-P', '-u', '-B', '-I', '-S', '-s', '-E'):
    args.pop(0)
if len(args) < 2 or args[0] not in ('-c', '-m'):
    raise SystemExit('The Team worker needs one explicit Python module or script.')
kind, source, *remaining = args
sys.argv = ['-c' if kind == '-c' else source, *remaining]
if kind == '-c':
    exec(compile(source, '<string>', 'exec'), {'__name__': '__main__', '__builtins__': __builtins__})
else:
    import runpy
    runpy.run_module(source, run_name='__main__', alter_sys=True)
"#;

pub(super) fn exec_if_requested(arguments: &[String]) -> Option<i32> {
    if arguments.first().map(String::as_str) != Some(source_namespace::PYTHON_CHILD) {
        return None;
    }
    Some(match execute(arguments) {
        Ok(code) => code,
        Err(_) => {
            eprintln!(
                "The Team interpreter was stopped before source discovery because its native launch namespace is unavailable."
            );
            1
        }
    })
}

fn execute(arguments: &[String]) -> Result<i32, String> {
    meerschaum_core::team_scope::sandbox::current()
        .ok_or("No native Team confinement was installed.")?
        .require_sockets_verdict()?;
    source_namespace::capture_native()?;
    let launch =
        source_namespace::launch()?.ok_or("No native Team source namespace was captured.")?;
    let current = std::env::current_exe()
        .and_then(std::fs::canonicalize)
        .map_err(|_| "The Team launcher is unavailable.")?;
    if current != launch.runtime.launcher || launch.runtime.api_only {
        return Err(
            "This interpreter launcher differs from the owning native Team runtime.".into(),
        );
    }
    let requested = Path::new(
        arguments
            .get(1)
            .ok_or("No Team interpreter was requested.")?,
    );
    if requested != launch.runtime.python
        && requested != Path::new("python")
        && requested != Path::new("python3")
    {
        return Err("A Team source launch must use its host-selected interpreter.".into());
    }
    let descriptor = launch
        .namespace
        .cache
        .parent()
        .ok_or("Invalid source namespace.")?
        .join(source_namespace::FILE);
    let mut bootstrap =
        serde_json::to_value(&launch.runtime).map_err(|_| "Invalid Team runtime.")?;
    bootstrap["expected_release"] = env!("CARGO_PKG_VERSION").into();
    let mut command = std::process::Command::new(&launch.runtime.python);
    command
        .args(["-I", "-S", "-u", "-c", BOOTSTRAP])
        .arg(serde_json::to_string(&bootstrap).map_err(|_| "Invalid Team runtime.")?)
        .arg(&descriptor)
        .arg(launch.digest()?)
        .args(&arguments[2..]);
    runtime::sanitize_command(&mut command);
    for (name, value) in &launch.runtime.environment {
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
            return Err("The native Team runtime contains an unsupported loader setting.".into());
        }
        command.env(name, value);
    }
    command.env_remove("TMUX").env_remove("TMUX_PANE");
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        Err(format!(
            "The selected Team interpreter could not start: {}",
            command.exec()
        ))
    }
    #[cfg(not(unix))]
    {
        command
            .status()
            .map(|status| status.code().unwrap_or(1))
            .map_err(|_| "The selected Team interpreter could not start.".into())
    }
}
