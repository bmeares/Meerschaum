//! One release plan, with installation ownership independent of the Python extension host.
use super::{
    flag, info,
    pip::{PipEnv, PipInstall, pip_install},
    words, yes_no,
};
use crate::{builtin::ActionResult, dispatch::Runtime, value::ArgsMap};
use meerschaum_core::{SuccessTuple, python_guard, self_exe, version::parse_version};
use serde_json::Value as Json;
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

const RELEASES: &str = "https://api.github.com/repos/bmeares/Meerschaum/releases?per_page=100";
const DOWNLOADS: &str = "https://github.com/bmeares/Meerschaum/releases/download";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Installation {
    Python(PathBuf),
    Cargo {
        executable: PathBuf,
        prefix: PathBuf,
    },
    Binary(PathBuf),
    Managed(String),
}

impl std::fmt::Display for Installation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Python(p) => write!(f, "Python environment ({})", p.display()),
            Self::Cargo { executable, prefix } => write!(
                f,
                "Cargo {} (install root {})",
                executable.display(),
                prefix.display()
            ),
            Self::Binary(p) => write!(f, "standalone binary {}", p.display()),
            Self::Managed(manager) => write!(f, "{manager}"),
        }
    }
}

/// The exact launcher interpreter is retained (canonicalizing a venv python loses its venv).
pub fn detect(command: &[String], executable: &Path, docker: bool) -> Installation {
    if docker {
        return Installation::Managed(
            "Docker image; pull and recreate the container using its existing image variant".into(),
        );
    }
    if self_exe::ends_with_module_args(command) {
        return Installation::Python(PathBuf::from(&command[0]));
    }
    // The program that runs this CLI again (`MRSM_BIN` when Python execs a standalone binary),
    // not `current_exe()`, which is the interpreter when the CLI runs inside the extension.
    let executable = command.first().map_or(executable, Path::new);
    if self_exe::is_python_executable(&executable.to_string_lossy()) {
        return Installation::Managed(
            "the Python interpreter that embeds it; run `python -m meerschaum upgrade mrsm` from that environment"
                .into(),
        );
    }
    let launched = executable.to_path_buf();
    let executable = executable
        .canonicalize()
        .unwrap_or_else(|_| executable.into());
    if let Some(prefix) = executable
        .parent()
        .filter(|p| p.file_name().is_some_and(|s| s == "bin"))
        .and_then(Path::parent)
        && cargo_owns(prefix)
    {
        return Installation::Cargo {
            executable: executable.clone(),
            prefix: prefix.into(),
        };
    }
    let text = executable.to_string_lossy().replace('\\', "/");
    if [&launched, &executable]
        .into_iter()
        .any(|p| in_cargo_target(p))
        || text.contains("/target/debug/")
        || text.contains("/target/release/")
    {
        return Installation::Managed(
            "source checkout; rebuild with Cargo (the development binary will not replace itself)"
                .into(),
        );
    }
    if text.starts_with("/usr/bin/")
        || text.starts_with("/bin/")
        || text.starts_with("/nix/store/")
        || text.contains("/Cellar/")
    {
        return Installation::Managed(
            "system package manager; upgrade Meerschaum with the manager that installed it".into(),
        );
    }
    Installation::Binary(executable)
}

/// Whether `path` is under a Cargo target directory, whatever its name or profile (`target/` may be
/// a symlink, and `current_exe()` resolves it): an ancestor holding the `CACHEDIR.TAG` Cargo writes.
fn in_cargo_target(path: &Path) -> bool {
    let text = path.to_string_lossy();
    // A binary rebuilt while it runs reads back as `<path> (deleted)` on Linux.
    let path = Path::new(text.strip_suffix(" (deleted)").unwrap_or(&text));
    path.ancestors().any(|a| {
        std::fs::read_to_string(a.join("CACHEDIR.TAG"))
            .is_ok_and(|tag| tag.contains("created by cargo"))
    })
}

fn cargo_owns(prefix: &Path) -> bool {
    let Ok(text) = std::fs::read_to_string(prefix.join(".crates.toml")) else {
        return false;
    };
    let Ok(receipt) = toml::from_str::<toml::Value>(&text) else {
        return false;
    };
    receipt
        .get("v1")
        .and_then(toml::Value::as_table)
        .is_some_and(|packages| {
            packages.iter().any(|(name, bins)| {
                name.starts_with("meerschaum ")
                    && bins.as_array().is_some_and(|bins| {
                        bins.iter()
                            .any(|bin| matches!(bin.as_str(), Some("mrsm" | "mrsm.exe")))
                    })
            })
        })
}

#[derive(Debug, Default)]
struct Request {
    version: Option<String>,
    pre: bool,
    installer_args: Vec<String>,
}

fn request(arguments: &[String]) -> Result<Request, String> {
    let mut result = Request::default();
    let mut args = arguments.iter();
    while let Some(arg) = args.next() {
        if arg == "--pre" {
            result.pre = true;
        } else if arg == "--version" {
            result.version = Some(
                args.next()
                    .ok_or("--version requires a release version")?
                    .clone(),
            );
        } else if let Some(version) = arg.strip_prefix("--version=") {
            result.version = Some(version.into());
        } else {
            result.installer_args.push(arg.clone());
        }
    }
    if let Some(version) = &result.version {
        parse_version(version).map_err(|e| e.to_string())?;
    }
    Ok(result)
}

#[derive(Debug, Clone)]
struct Release {
    version: String,
    tag: String,
    assets: Vec<(String, String)>,
}

fn select_release(document: &Json, current: &str, pre: bool) -> Result<Release, String> {
    let current = parse_version(current).map_err(|e| e.to_string())?;
    let allow_pre = pre || current.any_prerelease();
    document
        .as_array()
        .ok_or("Invalid GitHub releases response")?
        .iter()
        .filter(|release| release["draft"] != true)
        .filter_map(|release| {
            let tag = release["tag_name"].as_str()?;
            let version = parse_version(tag.trim_start_matches('v')).ok()?;
            if !allow_pre && (version.any_prerelease() || release["prerelease"] == true) {
                return None;
            }
            let assets = release["assets"]
                .as_array()?
                .iter()
                .filter_map(|asset| {
                    Some((
                        asset["name"].as_str()?.into(),
                        asset["browser_download_url"].as_str()?.into(),
                    ))
                })
                .collect();
            Some((
                version.clone(),
                Release {
                    version: version.to_string(),
                    tag: tag.into(),
                    assets,
                },
            ))
        })
        .max_by(|(a, _), (b, _)| a.cmp(b))
        .map(|(_, release)| release)
        .ok_or_else(|| "No published Meerschaum release is available for this channel.".into())
}

fn client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .user_agent(concat!("meerschaum/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(180))
        .build()
        .map_err(|e| e.to_string())
}

pub fn current_installation(rt: &Runtime<'_>) -> Installation {
    detect(
        &self_exe::self_command(),
        &std::env::current_exe().unwrap_or_default(),
        (rt.env)("MRSM_RUNTIME").as_deref() == Some("docker"),
    )
}

/// The public upgrade action is shared by the wheel and the standalone executable.
pub fn upgrade(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    match upgrade_inner(args, rt) {
        Ok(result) => Ok(result),
        Err(message) => Ok(SuccessTuple::fail(message)),
    }
}

fn upgrade_inner(args: &ArgsMap, rt: &mut Runtime<'_>) -> Result<SuccessTuple, String> {
    let origin = current_installation(rt);
    if let Installation::Managed(manager) = &origin {
        return Err(format!("This installation is managed by {manager}."));
    }
    let _installation_lock = if let Installation::Binary(executable) = &origin {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(executable.with_extension("upgrade.lock"))
            .map_err(|e| format!("Cannot lock this installation for upgrade: {e}"))?;
        fs4::FileExt::try_lock(&file)
            .map_err(|e| format!("Another upgrade holds this installation's lock: {e}"))?;
        Some(file)
    } else {
        None
    };
    let request = request(&words(args, "sub_args"))?;
    let extras = words(args, "action")
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(",");
    if !extras.is_empty()
        && !matches!(origin, Installation::Python(_))
        && (rt.env)("MRSM_PYTHON").is_none()
    {
        return Err("Python extras require a selected Python host. Set MRSM_PYTHON to that interpreter, or omit the extras for a native-only upgrade.".into());
    }
    if matches!(origin, Installation::Binary(_)) && !request.installer_args.is_empty() {
        return Err(
            "Installer arguments are not supported when replacing a downloaded binary.".into(),
        );
    }
    let session = super::session(rt);
    let http = client()?;
    let release = if let Some(version) = &request.version {
        let version = parse_version(version)
            .map_err(|e| e.to_string())?
            .to_string();
        Release {
            tag: format!("v{version}"),
            version,
            assets: Vec::new(),
        }
    } else {
        let document: Json = session
            .block_on(async {
                http.get(RELEASES)
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await
            })
            .map_err(|e| format!("Unable to select a published release: {e}"))?;
        select_release(&document, crate::VERSION, request.pre)?
    };
    if parse_version(&release.version).map_err(|e| e.to_string())?
        < parse_version(crate::VERSION).map_err(|e| e.to_string())?
        && !flag(args, "force")
    {
        return Err(format!(
            "{} is older than this program ({}). Use --force for an intentional downgrade.",
            release.version,
            crate::VERSION
        ));
    }
    info(
        rt,
        &format!(
            "Upgrade installation: {origin}\nRelease: {}",
            release.version
        ),
    );
    let cli_current = release.version == python_guard::expected_version()
        && !flag(args, "force")
        && extras.is_empty();
    let owner = match &origin {
        Installation::Python(p) => Some(p),
        _ => None,
    };
    let selected_host = (rt.env)("MRSM_PYTHON")
        .filter(|s| !s.is_empty())
        .filter(|python| owner.is_none_or(|p| p != Path::new(python)));
    if cli_current && selected_host.is_none() {
        return Ok(SuccessTuple::ok(format!(
            "Meerschaum {} is already installed.",
            release.version
        )));
    }
    if !yes_no(
        rt,
        "Upgrade this installation?",
        "y",
        flag(args, "yes"),
        flag(args, "noask"),
    )
    .map_err(|e| e.message)?
    {
        return Ok(SuccessTuple::fail("Upgrade cancelled."));
    }

    // Only an explicitly selected host is considered for coordinated upgrades. An unused
    // interpreter on PATH is never probed or provisioned for native users.
    let mut host = None;
    if let Some(python) = selected_host {
        let probe = super::doctor::probe_python(&python);
        if let Ok(probe) = &probe
            && probe.meerschaum.found
        {
            let package = &probe.meerschaum;
            let verdict = python_guard::judge(
                &release.version,
                python_guard::WORKER_PROTOCOL_VERSION,
                package.version.as_deref(),
                package.has_worker,
                package.protocol,
            );
            let incompatible = matches!(verdict, python_guard::Verdict::Refuse(_));
            let differs = package.version.as_deref() != Some(&release.version);
            if differs || incompatible || !extras.is_empty() {
                info(
                    rt,
                    &format!(
                        "Selected Python host: {python} (Meerschaum {}).",
                        package.version.as_deref().unwrap_or("unknown")
                    ),
                );
                let align = !extras.is_empty()
                    || yes_no(
                        rt,
                        &format!(
                            "Also install Meerschaum {} in this Python environment?",
                            release.version
                        ),
                        "n",
                        false,
                        flag(args, "noask"),
                    )
                    .map_err(|e| e.message)?;
                if align {
                    host = Some(PathBuf::from(python));
                } else if incompatible {
                    return Err("The selected Python host is incompatible with the planned release. Align it or select a compatible host before upgrading.".into());
                }
            }
        } else if !extras.is_empty() {
            return Err(
                "The selected Python host cannot be inspected; no installation was changed.".into(),
            );
        }
    }

    if cli_current && host.is_none() {
        return Ok(SuccessTuple::ok(format!(
            "Meerschaum {} is already installed.",
            release.version
        )));
    }
    // Download, checksum, and execute the staged binary before changing any installation.
    let staged = if cli_current {
        None
    } else if let Installation::Binary(executable) = &origin {
        Some(stage_binary(&session, &http, &release, executable)?)
    } else {
        None
    };
    if let Some(python) = &host {
        install_python(
            rt,
            python,
            &release.version,
            &extras,
            &request.installer_args,
            flag(args, "debug"),
        )?;
    }
    let installed = match &origin {
        _ if cli_current => Ok(()),
        Installation::Python(python) => install_python(
            rt,
            python,
            &release.version,
            &extras,
            &request.installer_args,
            flag(args, "debug"),
        ),
        Installation::Cargo { executable, prefix } => (|| {
            let version = cargo_version(&release.version)?;
            let mut command = Command::new("cargo");
            command
                .args(["install", "--locked", "--force", "--root"])
                .arg(prefix)
                .args(["--version", &version, "meerschaum"])
                .args(&request.installer_args);
            #[cfg(windows)]
            let backup = {
                let backup =
                    executable.with_extension(format!("mrsm-old-{}", uuid::Uuid::new_v4()));
                std::fs::rename(executable, &backup).map_err(|e| e.to_string())?;
                backup
            };
            let result = run_command(&mut command);
            #[cfg(windows)]
            if result.is_err() && !executable.exists() {
                let _ = std::fs::rename(&backup, executable);
            }
            result?;
            verify_binary(executable, &release.version)
        })(),
        Installation::Binary(executable) => {
            replace_binary(executable, staged.expect("staged binary"))
        }
        Installation::Managed(_) => unreachable!(),
    };
    if let Err(error) = installed {
        return Err(format!(
            "{error}{}",
            if host.is_some() {
                " The selected Python host was upgraded; the CLI upgrade failed. Retry the CLI upgrade before continuing."
            } else {
                ""
            }
        ));
    }
    rt.restart_requested = rt.in_shell;
    Ok(SuccessTuple::ok(format!(
        "Installed Meerschaum {}. {}",
        release.version,
        if rt.in_shell {
            "Restarting the shell with the updated engine."
        } else {
            "The next command uses the updated engine; restart other running Meerschaum sessions."
        }
    )))
}

fn run_command(command: &mut Command) -> Result<(), String> {
    let status = command
        .stdin(Stdio::null())
        .status()
        .map_err(|e| e.to_string())?;
    status
        .success()
        .then_some(())
        .ok_or_else(|| format!("Installer exited with {status}."))
}

pub(super) fn install_python(
    rt: &mut Runtime<'_>,
    python: &Path,
    version: &str,
    extras: &str,
    passthrough: &[String],
    debug: bool,
) -> Result<(), String> {
    let environment = python.parent().and_then(Path::parent).unwrap_or(python);
    let package = format!(
        "meerschaum{}=={version}",
        if extras.is_empty() {
            String::new()
        } else {
            format!("[{extras}]")
        }
    );
    if environment.join("pipx_metadata.json").is_file() {
        use std::io::Write;
        let home = environment
            .parent()
            .filter(|p| p.file_name().is_some_and(|n| n == "venvs"))
            .and_then(Path::parent)
            .ok_or("Cannot identify the owning pipx home; use pipx to upgrade this environment")?;
        let name = environment
            .file_name()
            .ok_or("pipx environment has no name")?;
        let mut constraint = tempfile::NamedTempFile::new().map_err(|e| e.to_string())?;
        writeln!(constraint, "meerschaum=={version}").map_err(|e| e.to_string())?;
        let mut pip_args = vec![
            "--constraint".into(),
            constraint.path().to_string_lossy().into_owned(),
        ];
        pip_args.extend_from_slice(passthrough);
        if !extras.is_empty() {
            pip_args.push(package.clone());
        }
        let quote = |s: &String| format!("'{}'", s.replace('\'', "'\\''"));
        run_command(
            Command::new("pipx")
                .arg("upgrade")
                .arg(name)
                .arg("--pip-args")
                .arg(pip_args.iter().map(quote).collect::<Vec<_>>().join(" "))
                .env("PIPX_HOME", home),
        )?;
    } else if environment.join("uv-receipt.toml").is_file() {
        let tools = environment
            .parent()
            .ok_or("Cannot locate the owning uv tool directory")?;
        run_command(
            Command::new("uv")
                .args(["tool", "upgrade", &package])
                .env("UV_TOOL_DIR", tools)
                .args(passthrough),
        )?;
    } else {
        let mut env = PipEnv::new(rt);
        env.python = python.to_string_lossy().into_owned();
        let mut arguments = vec!["--upgrade".into()];
        arguments.extend_from_slice(passthrough);
        if !pip_install(
            &env,
            rt,
            &PipInstall {
                names: vec![package],
                args: Some(arguments),
                host: true,
                debug,
                ..Default::default()
            },
        ) {
            return Err(format!(
                "Failed to upgrade Meerschaum in {}.",
                python.display()
            ));
        }
    }
    let output = Command::new(python)
        .args([
            "-E",
            "-P",
            "-c",
            "import importlib.metadata; print(importlib.metadata.version('meerschaum'))",
        ])
        .output()
        .map_err(|e| e.to_string())?;
    let found = String::from_utf8_lossy(&output.stdout);
    if !output.status.success() || !super::doctor::versions_match(found.trim(), version) {
        return Err(format!(
            "Upgrade verification failed for {}: expected {version}, found {}.",
            python.display(),
            found.trim()
        ));
    }
    let output = Command::new(python)
        .args(["-E", "-P", "-m", "meerschaum", "--version"])
        .env_remove("MRSM_BIN")
        .output()
        .map_err(|e| e.to_string())?;
    let found = String::from_utf8_lossy(&output.stdout);
    if !output.status.success()
        || !found
            .split_whitespace()
            .any(|word| super::doctor::versions_match(word.trim_start_matches('v'), version))
    {
        return Err(format!(
            "The installed Python engine failed verification in {}. Restarting was cancelled: {}",
            python.display(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(())
}

fn cargo_version(version: &str) -> Result<String, String> {
    let re = regex::Regex::new(r"^(\d+\.\d+\.\d+)(?:(a|b|rc)(\d+))?$").expect("version regex");
    let c = re
        .captures(version)
        .ok_or_else(|| format!("{version} has no supported Cargo release spelling"))?;
    Ok(match c.get(2) {
        None => c[1].into(),
        Some(pre) => format!(
            "{}-{}.{}",
            &c[1],
            match pre.as_str() {
                "a" => "alpha",
                "b" => "beta",
                _ => "rc",
            },
            &c[3]
        ),
    })
}

fn platform() -> Result<String, String> {
    let arch = std::env::consts::ARCH;
    let suffix = match std::env::consts::OS {
        "linux" if cfg!(target_env = "musl") => "unknown-linux-musl",
        "linux" => "unknown-linux-gnu",
        "macos" => "apple-darwin",
        "windows" => "pc-windows-msvc",
        other => {
            return Err(format!(
                "No published standalone binary for {arch}-{other}. Use Cargo."
            ));
        }
    };
    Ok(format!("{arch}-{suffix}"))
}

fn verify_binary(binary: &Path, expected: &str) -> Result<(), String> {
    let spec = crate::dispatch::CommandSpec {
        program: binary.to_string_lossy().into_owned(),
        args: vec!["--version".into()],
        env: vec![("MRSM_NO_UPDATE_CHECK".into(), "1".into())],
        ..Default::default()
    };
    let output = super::terminal::capture(&spec, Some(Duration::from_secs(15)))
        .map_err(|e| format!("Cannot execute {}: {e}", binary.display()))?;
    let text = String::from_utf8_lossy(&output.stdout);
    if !output.timed_out
        && output.code == 0
        && text
            .split_whitespace()
            .any(|word| super::doctor::versions_match(word.trim_start_matches('v'), expected))
    {
        Ok(())
    } else {
        Err(format!(
            "Binary verification failed: expected Meerschaum {expected}, received {}",
            text.trim()
        ))
    }
}

fn stage_binary(
    session: &super::session::Session,
    http: &reqwest::Client,
    release: &Release,
    executable: &Path,
) -> Result<tempfile::TempPath, String> {
    let name = format!(
        "mrsm-{}-{}{}",
        release.version,
        platform()?,
        if cfg!(windows) { ".exe" } else { "" }
    );
    let base = format!("{DOWNLOADS}/{}", release.tag);
    let url = release
        .assets
        .iter()
        .find(|(n, _)| n == &name)
        .map(|(_, u)| u.clone())
        .unwrap_or_else(|| format!("{base}/{name}"));
    let checksum_url = release
        .assets
        .iter()
        .find(|(n, _)| n == &format!("{name}.sha256") || n == "SHA256SUMS")
        .map(|(_, u)| u.clone())
        .unwrap_or_else(|| format!("{base}/SHA256SUMS"));
    let mut staged = tempfile::Builder::new()
        .prefix(".mrsm-upgrade-")
        .suffix(if cfg!(windows) { ".exe" } else { "" })
        .tempfile_in(
            executable
                .parent()
                .ok_or("Executable has no parent directory")?,
        )
        .map_err(|e| {
            format!(
                "Cannot stage an upgrade beside {}: {e}",
                executable.display()
            )
        })?;
    let actual = session.block_on(async {
        use std::io::Write;
        let checksums = http
            .get(&checksum_url)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .error_for_status()
            .map_err(|e| e.to_string())?
            .text()
            .await
            .map_err(|e| e.to_string())?;
        let expected = checksums
            .lines()
            .find_map(|line| {
                let mut parts = line.split_whitespace();
                let hash = parts.next()?;
                (parts.next()?.trim_start_matches('*') == name).then_some(hash)
            })
            .ok_or("Release checksum does not name the selected binary")?;
        let mut response = http
            .get(url)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .error_for_status()
            .map_err(|e| e.to_string())?;
        let mut hash = Sha256::new();
        let mut size = 0usize;
        while let Some(chunk) = response.chunk().await.map_err(|e| e.to_string())? {
            size += chunk.len();
            if size > 256 * 1024 * 1024 {
                return Err("Release binary exceeds 256 MiB".into());
            }
            hash.update(&chunk);
            staged.write_all(&chunk).map_err(|e| e.to_string())?;
        }
        if hex::encode(hash.finalize()) != expected {
            return Err(
                "Release binary SHA-256 does not match; the installation was not changed.".into(),
            );
        }
        Ok::<(), String>(())
    });
    actual?;
    staged.as_file().sync_all().map_err(|e| e.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(staged.path(), std::fs::Permissions::from_mode(0o755))
            .map_err(|e| e.to_string())?;
    }
    let staged = staged.into_temp_path();
    verify_binary(&staged, &release.version)?;
    Ok(staged)
}

#[cfg(unix)]
fn replace_binary(executable: &Path, staged: tempfile::TempPath) -> Result<(), String> {
    // One `rename` over the running program: atomic, so a job or unit re-launching `mrsm` never
    // finds the path missing. The staged file (same directory) keeps the original's mode, plus
    // execute wherever reading is allowed.
    if let Ok(metadata) = std::fs::metadata(executable) {
        use std::os::unix::fs::PermissionsExt;
        let mode = metadata.permissions().mode() & 0o7777;
        std::fs::set_permissions(
            &staged,
            std::fs::Permissions::from_mode(mode | ((mode & 0o444) >> 2)),
        )
        .map_err(|e| e.to_string())?;
    }
    staged
        .persist(executable)
        .map_err(|e| format!("Cannot replace executable: {e}"))
}

#[cfg(not(unix))]
fn replace_binary(executable: &Path, staged: tempfile::TempPath) -> Result<(), String> {
    // Windows cannot replace a running image: rename it out of the way, restoring it if
    // installing the new executable fails. The open image keeps the backup locked.
    let backup = executable.with_extension(format!("mrsm-old-{}", uuid::Uuid::new_v4()));
    std::fs::rename(executable, &backup).map_err(|e| e.to_string())?;
    if let Err(error) = staged.persist(executable) {
        let rollback = std::fs::rename(&backup, executable);
        return Err(format!(
            "Cannot replace executable: {error}; restore previous executable: {rollback:?}"
        ));
    }
    let _ = std::fs::remove_file(backup);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_binary_in_any_cargo_target_directory_is_a_source_checkout() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("ci-cache-v5");
        std::fs::create_dir_all(target.join("debug")).unwrap();
        std::fs::write(
            target.join("CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55\n# This file is a cache directory tag created by cargo.\n",
        )
        .unwrap();
        let binary = target.join("debug").join("mrsm");
        std::fs::write(&binary, "binary").unwrap();
        let command = vec![binary.to_string_lossy().into_owned()];
        assert!(matches!(
            detect(&command, &binary, false),
            Installation::Managed(m) if m.contains("source checkout")
        ));
        let deleted = format!("{} (deleted)", binary.display());
        assert!(in_cargo_target(Path::new(&deleted)));
        // Another program's cache tag is not Cargo's.
        std::fs::write(
            target.join("CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55\n",
        )
        .unwrap();
        assert!(!in_cargo_target(&binary));
    }

    #[test]
    fn an_embedding_interpreter_is_never_the_binary_to_replace() {
        let dir = tempfile::tempdir().unwrap();
        let python = dir.path().join("python3.13");
        let binary = dir.path().join("mrsm");
        std::fs::write(&python, "interpreter").unwrap();
        std::fs::write(&binary, "binary").unwrap();
        // In the extension without a registered command, `current_exe()` is the interpreter.
        assert!(matches!(
            detect(&[], &python, false),
            Installation::Managed(_)
        ));
        // `MRSM_BIN`: Python registers the standalone binary, which is what gets replaced.
        let command = vec![binary.to_string_lossy().into_owned()];
        assert_eq!(
            detect(&command, &python, false),
            Installation::Binary(binary.canonicalize().unwrap())
        );
    }

    #[test]
    fn provenance_keeps_the_launcher_interpreter_and_requires_cargo_receipts() {
        let dir = tempfile::tempdir().unwrap();
        let executable = dir.path().join("bin/mrsm");
        std::fs::create_dir_all(executable.parent().unwrap()).unwrap();
        std::fs::write(&executable, "binary").unwrap();
        let python = dir.path().join("env/bin/python");
        let command = self_exe::python_command(python.to_str().unwrap());
        assert_eq!(
            detect(&command, &executable, false),
            Installation::Python(python)
        );
        // A neighboring interpreter does not make a standalone executable a wheel install.
        std::fs::write(dir.path().join("bin/python3"), "not the owner").unwrap();
        assert!(matches!(
            detect(&[], &executable, false),
            Installation::Binary(_)
        ));
        std::fs::write(dir.path().join(".crates.toml"), "[v1]\n'meerschaum 5.0.0 (registry+https://github.com/rust-lang/crates.io-index)' = ['mrsm']\n").unwrap();
        assert_eq!(
            detect(&[], &executable, false),
            Installation::Cargo {
                executable: executable.canonicalize().unwrap(),
                prefix: dir.path().to_owned()
            }
        );
        assert!(matches!(
            detect(&command, &executable, true),
            Installation::Managed(_)
        ));
    }

    #[test]
    fn every_backend_uses_the_same_release_channel_and_ordering() {
        let releases = json!([
            {"tag_name":"v5.0.1", "assets":[]},
            {"tag_name":"v5.1.0rc10", "prerelease":true, "assets":[]},
            {"tag_name":"v5.1.0rc2", "prerelease":true, "assets":[]},
            {"tag_name":"v99.0.0", "draft":true, "assets":[]}
        ]);
        assert_eq!(
            select_release(&releases, "5.0.0", false).unwrap().version,
            "5.0.1"
        );
        assert_eq!(
            select_release(&releases, "5.0.0", true).unwrap().version,
            "5.1.0rc10"
        );
        assert_eq!(
            select_release(&releases, "5.0.0-rc.11", false)
                .unwrap()
                .version,
            "5.1.0rc10"
        );
        assert_eq!(cargo_version("5.1.0rc10").unwrap(), "5.1.0-rc.10");
    }

    #[test]
    fn release_options_do_not_leak_to_installers() {
        let args = [
            "--pre",
            "--version",
            "5.0.1",
            "--index-url",
            "https://example.test/simple",
        ]
        .map(String::from);
        let request = request(&args).unwrap();
        assert!(request.pre);
        assert_eq!(request.version.as_deref(), Some("5.0.1"));
        assert_eq!(
            request.installer_args,
            ["--index-url", "https://example.test/simple"]
        );
    }

    #[cfg(unix)]
    #[test]
    fn binary_download_checks_checksum_and_version_before_replacement() {
        use std::{
            io::{Read, Write},
            net::TcpListener,
        };
        for (body, corrupt) in [
            ("#!/bin/sh\necho 'Meerschaum v5.0.1'\n", false),
            ("#!/bin/sh\necho 'Meerschaum v5.0.1'\n", true),
            ("#!/bin/sh\necho 'Meerschaum v4.0.0'\n", false),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let executable = dir.path().join("mrsm");
            std::fs::write(&executable, "previous program").unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let name = format!("mrsm-5.0.1-{}", platform().unwrap());
            let checksum = format!(
                "{}  {name}\n",
                if corrupt {
                    "0".repeat(64)
                } else {
                    hex::encode(Sha256::digest(body))
                }
            );
            let thread = std::thread::spawn(move || {
                for _ in 0..2 {
                    let (mut socket, _) = listener.accept().unwrap();
                    let mut request = [0u8; 4096];
                    let n = socket.read(&mut request).unwrap();
                    let request = String::from_utf8_lossy(&request[..n]);
                    let response = if request.starts_with("GET /sums ") {
                        checksum.as_bytes()
                    } else {
                        body.as_bytes()
                    };
                    write!(
                        socket,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        response.len()
                    )
                    .unwrap();
                    socket.write_all(response).unwrap();
                }
            });
            let release = Release {
                version: "5.0.1".into(),
                tag: "v5.0.1".into(),
                assets: vec![
                    (name, format!("{base}/binary")),
                    ("SHA256SUMS".into(), format!("{base}/sums")),
                ],
            };
            let session = super::super::session::Session::new();
            let result = stage_binary(&session, &client().unwrap(), &release, &executable);
            thread.join().unwrap();
            assert_eq!(
                std::fs::read_to_string(&executable).unwrap(),
                "previous program"
            );
            if corrupt || body.contains("v4.0.0") {
                assert!(result.is_err());
            } else {
                replace_binary(&executable, result.unwrap()).unwrap();
                verify_binary(&executable, "5.0.1").unwrap();
                assert_eq!(std::fs::read_to_string(&executable).unwrap(), body);
            }
        }
    }
}
