//! Fixed transport captured by the first mandatory enforcement, before source/site discovery.

use super::Alias;
use crate::{
    Value,
    team_scope::sandbox::{Access, Mode, Rule, Spec},
};
#[cfg(target_os = "linux")]
use std::io::Read;
use std::path::{Path, PathBuf};

const MAX_SPEC_BYTES: usize = 2 * 1024 * 1024;
const MAX_CONFIG_BYTES: usize = 8 * 1024 * 1024;

fn unavailable() -> String {
    "This native API runtime has no immutable host-prepared graph transport. It cannot select one from later configuration or environment changes.".into()
}

fn regular(path: &Path, limit: usize) -> Result<Vec<u8>, String> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (path, limit);
        Err(unavailable())
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
            .map_err(|_| unavailable())?;
        let metadata = file.metadata().map_err(|_| unavailable())?;
        if !metadata.is_file()
            || metadata.len() > limit as u64
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err(unavailable());
        }
        let mut bytes = Vec::new();
        file.take(limit as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| unavailable())?;
        if bytes.len() > limit {
            return Err(unavailable());
        }
        Ok(bytes)
    }
}
fn canonical(path: &Path) -> Result<PathBuf, String> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(unavailable());
    }
    let real = std::fs::canonicalize(path).map_err(|_| unavailable())?;
    if real != path {
        return Err(unavailable());
    }
    Ok(real)
}
fn writable(spec: &Spec, extra: &[Rule], path: &Path) -> bool {
    spec.rules.iter().chain(extra).any(|rule| {
        matches!(
            rule.access,
            Access::ReadWrite
                | Access::DataFile
                | Access::Lock
                | Access::SharedMemory
                | Access::Device
        ) && (path == rule.path || path.starts_with(&rule.path))
    })
}

/// Called only by the FIRST irreversible sandbox enforcement, before source
/// discovery/site. The caller stores this result in private immutable Enforcement.
/// No re-enforcement or later environment/configuration can replace the result.
pub(crate) fn from_spec(spec: &Spec, extra: &[Rule]) -> Result<Alias, String> {
    let path = std::env::var_os(crate::team_scope::sandbox::SPEC_VAR)
        .filter(|path| !path.is_empty())
        .ok_or_else(unavailable)?;
    from_file(spec, extra, Path::new(&path))
}
fn from_file(spec: &Spec, extra: &[Rule], path: &Path) -> Result<Alias, String> {
    if spec.mode != Mode::RequireSockets
        || !spec
            .runtime
            .as_ref()
            .is_some_and(|runtime| runtime.api_only)
    {
        return Err(unavailable());
    }
    let spec_file = canonical(path)?;
    if !matches!(
        spec_file.file_name().and_then(|name| name.to_str()),
        Some(crate::team_scope::sandbox::SPEC_FILE | crate::team_scope::sandbox::JOB_SPEC_FILE)
    ) {
        return Err(unavailable());
    }
    let current: Spec =
        serde_json::from_slice(&regular(&spec_file, MAX_SPEC_BYTES)?).map_err(|_| unavailable())?;
    if &current != spec {
        return Err(unavailable());
    }
    let directory = spec_file.parent().ok_or_else(unavailable)?;
    let generation = directory
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("generation-"))
        .and_then(|value| uuid::Uuid::parse_str(value).ok())
        .filter(|id| !id.is_nil());
    if generation.is_none() {
        return Err(unavailable());
    }
    let config_file = canonical(&directory.join("config.json"))?;
    let credential_file = canonical(&directory.join("credential.json"))?;
    if [&spec_file, &config_file, &credential_file]
        .iter()
        .any(|path| writable(spec, extra, path))
        || !spec
            .rules
            .iter()
            .any(|rule| rule.path == directory && rule.access == Access::Read)
    {
        return Err(unavailable());
    }
    let config: Value = serde_json::from_slice(&regular(&config_file, MAX_CONFIG_BYTES)?)
        .map_err(|_| unavailable())?;
    let keys = config
        .get_path(&["meerschaum", "instance"])
        .and_then(Value::as_str)
        .ok_or_else(unavailable)?;
    let (typ, label) = super::split_keys(keys)?;
    let attributes = config
        .get_path(&["meerschaum", "connectors", typ, label])
        .and_then(Value::as_map)
        .ok_or_else(unavailable)?;
    let alias = Alias::from_attributes(attributes, keys)?.ok_or_else(unavailable)?;
    if alias.credential_file != credential_file {
        return Err(unavailable());
    }
    Ok(alias)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use crate::team_scope::{sandbox, write_private};

    struct Fixture {
        _root: tempfile::TempDir,
        spec: Spec,
        file: PathBuf,
        directory: PathBuf,
        state: PathBuf,
    }
    fn fixture() -> Fixture {
        let root = tempfile::tempdir().unwrap();
        let base = root.path().canonicalize().unwrap();
        let directory = base.join(format!("generation-{}", uuid::Uuid::new_v4().simple()));
        let state = directory.join("state");
        std::fs::create_dir_all(&state).unwrap();
        let credential = directory.join("credential.json");
        write_private(&credential, "{}").unwrap();
        let config = super::super::configuration(
            &crate::Map::new(),
            &std::collections::BTreeSet::from(["sql:main".into()]),
            "sql:main",
            "https://original-main.example",
            &credential,
        )
        .unwrap();
        write_private(
            &directory.join("config.json"),
            &Value::Map(config).to_json_string(),
        )
        .unwrap();
        let launcher = std::env::current_exe().unwrap().canonicalize().unwrap();
        let spec = Spec {
            mode: Mode::RequireSockets,
            rules: vec![
                Rule::new(&directory, Access::Read, "sealed generation"),
                Rule::new(&state, Access::ReadWrite, "own state"),
            ],
            canary: base.join("outside-secret"),
            runtime: Some(sandbox::runtime::Runtime {
                api_only: true,
                sdk_pycache_prefix: None,
                native_compiler: None,
                python: launcher.clone(),
                launcher,
                environment: Vec::new(),
                packages: Default::default(),
                site_paths: Vec::new(),
                prefix: None,
            }),
            ..Default::default()
        };
        std::fs::write(&spec.canary, "synthetic-private-secret").unwrap();
        let file = directory.join(sandbox::SPEC_FILE);
        write_private(&file, &serde_json::to_string(&spec).unwrap()).unwrap();
        Fixture {
            _root: root,
            spec,
            file,
            directory,
            state,
        }
    }

    #[test]
    fn only_the_exact_sealed_generation_can_supply_the_native_transport() {
        let fixture = fixture();
        let alias = from_file(&fixture.spec, &[], &fixture.file).unwrap();
        assert_eq!(alias.callback_url, "https://original-main.example");
        assert_eq!(
            alias.credential_file,
            fixture.directory.join("credential.json")
        );
        // An extra writable bootstrap rule must not turn a sealed descriptor into input.
        assert!(
            from_file(
                &fixture.spec,
                &[Rule::new(
                    &fixture.directory,
                    Access::ReadWrite,
                    "untrusted"
                )],
                &fixture.file
            )
            .is_err()
        );
        let mut changed = fixture.spec.clone();
        changed.runtime.as_mut().unwrap().api_only = false;
        assert!(from_file(&changed, &[], &fixture.file).is_err());
        // A caller cannot point the descriptor at an unrelated rotating bearer file.
        let other = fixture.state.join("peer-credential.json");
        write_private(&other, "{}").unwrap();
        let config = super::super::configuration(
            &crate::Map::new(),
            &std::collections::BTreeSet::from(["sql:main".into()]),
            "sql:main",
            "https://peer.example",
            &other,
        )
        .unwrap();
        write_private(
            &fixture.directory.join("config.json"),
            &Value::Map(config).to_json_string(),
        )
        .unwrap();
        assert!(from_file(&fixture.spec, &[], &fixture.file).is_err());
    }

    #[test]
    fn symlink_fifo_and_oversized_transport_inputs_are_refused_without_reading_them() {
        use std::os::unix::fs::symlink;
        let fixture = fixture();
        let config = fixture.directory.join("config.json");
        std::fs::remove_file(&config).unwrap();
        symlink(&fixture.file, &config).unwrap();
        assert!(from_file(&fixture.spec, &[], &fixture.file).is_err());
        std::fs::remove_file(&config).unwrap();
        rustix::fs::mkfifoat(
            rustix::fs::CWD,
            &config,
            rustix::fs::Mode::from_raw_mode(0o600),
        )
        .unwrap();
        let start = std::time::Instant::now();
        assert!(from_file(&fixture.spec, &[], &fixture.file).is_err());
        assert!(start.elapsed() < std::time::Duration::from_secs(1));
        std::fs::remove_file(&config).unwrap();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&config)
            .unwrap();
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .unwrap();
        file.set_len(MAX_CONFIG_BYTES as u64 + 1).unwrap();
        assert!(from_file(&fixture.spec, &[], &fixture.file).is_err());
    }

    #[test]
    #[allow(unsafe_code)] // Only the dedicated single-test child mutates its own environment.
    fn first_mandatory_enforcement_keeps_its_transport_after_environment_and_config_mutations() {
        const CHILD: &str = "MRSM_TEST_IMMUTABLE_GRAPH_TRANSPORT";
        if std::env::var_os(CHILD).is_none() {
            let fixture = fixture();
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .env_clear().env(CHILD, &fixture.file)
                .env_remove("TMUX").env_remove("TMUX_PANE")
                .args(["first_mandatory_enforcement_keeps_its_transport_after_environment_and_config_mutations", "--nocapture", "--test-threads=1"])
                .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped())
                .spawn().unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
            while child.try_wait().unwrap().is_none() {
                if std::time::Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("The owned immutable transport child exceeded its deadline.");
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            let output = child.wait_with_output().unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        let file = PathBuf::from(std::env::var_os(CHILD).unwrap());
        let spec = Spec::read(&file).unwrap();
        let directory = file.parent().unwrap();
        let state = directory.join("state");
        // SAFETY: this re-executed child runs only this test (`--test-threads=1` and its exact
        // name), and neither the harness nor `sandbox` starts a thread that reads the environment,
        // so no other thread can observe `set_var`.
        unsafe {
            std::env::set_var(sandbox::SPEC_VAR, &file);
        }
        let report = sandbox::enforce(&spec, &[]);
        report.require_sockets_verdict().unwrap();
        let original = sandbox::with_api_transport(|alias| {
            Ok((alias.callback_url.clone(), alias.credential_file.clone()))
        })
        .unwrap()
        .unwrap();
        let peer = state.join("peer-config.json");
        write_private(&peer, "{\"meerschaum\":{\"instance\":\"api:peer\"}}").unwrap();
        // SAFETY: still the single-threaded re-executed child above; `sandbox::enforce` and
        // `with_api_transport` start no threads.
        unsafe {
            std::env::set_var("MRSM_CONFIG_FILE", &peer);
            std::env::set_var(sandbox::SPEC_VAR, &peer);
            std::env::remove_var(super::super::PROFILE_VAR);
        }
        assert!(std::fs::write(directory.join("config.json"), "{}").is_err());
        // A later attempt to replace the first report/spec is inert.
        assert_eq!(sandbox::enforce(&Spec::default(), &[]), report);
        let retained = sandbox::with_api_transport(|alias| {
            Ok((alias.callback_url.clone(), alias.credential_file.clone()))
        })
        .unwrap()
        .unwrap();
        assert_eq!(retained, original);
        assert_eq!(retained.0, "https://original-main.example");
        // Do not ask TempDir to remove sealed state from inside its own confinement.
        std::process::exit(0);
    }
}
