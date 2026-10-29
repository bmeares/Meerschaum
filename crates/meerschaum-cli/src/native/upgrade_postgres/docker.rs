//! The Docker side of `upgrade postgres`, `backup postgres` and `restore postgres`: `docker` and
//! `docker compose` processes, and the PostgreSQL client tools run inside containers
//! (`docker exec`), so nothing but Docker is needed on the host (Linux, macOS and Windows alike).
//!
//! While a command runs, every child runs in its own process group: a Ctrl-C (or a closed
//! terminal) reaches only `mrsm`, which turns it into a stop request ([`super::signals`]); the
//! running step then kills its child and the command puts things back.

use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, channel};
use std::time::{Duration, Instant};

use serde_json::Value as Json;
use sha2::{Digest, Sha256};

use super::signals::{self, Phase};

/// `--debug`: print each `docker` command line (they never hold credentials: values travel in
/// the environment and SQL on standard input).
static DEBUG: AtomicBool = AtomicBool::new(false);

pub fn set_debug(debug: bool) {
    DEBUG.store(debug, Ordering::SeqCst);
}

/// Whether a stop was requested while a step runs.
pub fn stop_requested() -> bool {
    signals::stop_requested()
}

/// `docker run` never pulls implicitly: the commands pull only after asking.
pub const PULL_NEVER: [&str; 2] = ["--pull", "never"];

/// A step that failed, or the user's stop request.
#[derive(Debug, Clone)]
pub struct StepError {
    pub message: String,
    pub interrupted: bool,
}

impl StepError {
    pub fn new(message: impl Into<String>) -> StepError {
        StepError {
            message: message.into(),
            interrupted: false,
        }
    }

    pub fn interrupted() -> StepError {
        StepError {
            message: format!("Stopped ({}).", signals::stop_reason()),
            interrupted: true,
        }
    }
}

impl From<String> for StepError {
    fn from(message: String) -> StepError {
        StepError::new(message)
    }
}

pub type Res<T> = Result<T, StepError>;

// --- Private files ---------------------------------------------------------------------------

/// Create `dir` (and its missing parents) readable by the owner only (0700 on Unix): the dumps
/// hold the roles' password hashes.
pub fn private_dir(dir: &Path) -> Res<()> {
    let mut missing = Vec::new();
    let mut probe = Some(dir);
    while let Some(p) = probe {
        if p.exists() {
            break;
        }
        missing.push(p.to_path_buf());
        probe = p.parent();
    }
    std::fs::create_dir_all(dir)
        .map_err(|e| StepError::new(format!("Cannot create '{}': {e}", dir.display())))?;
    #[cfg(unix)]
    for created in missing.iter().chain(std::iter::once(&dir.to_path_buf())) {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(created, std::fs::Permissions::from_mode(0o700));
    }
    for created in &missing {
        sync_dir(created)?;
        if let Some(parent) = created.parent() {
            sync_dir(parent)?;
        }
    }
    Ok(())
}

/// Persist directory entries on Unix after recovery-critical creates and renames.
pub fn sync_dir(path: &Path) -> Res<()> {
    #[cfg(unix)]
    std::fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|e| StepError::new(format!("Cannot sync directory '{}': {e}", path.display())))?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

pub fn sync_file(path: &Path) -> Res<()> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(cfg!(windows))
        .open(path)
        .and_then(|file| file.sync_all())
        .map_err(|e| StepError::new(format!("Cannot sync file '{}': {e}", path.display())))?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

/// Create (or truncate) a file readable by the owner only (0600 on Unix).
pub fn private_file(path: &Path) -> Res<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options
        .open(path)
        .map_err(|e| StepError::new(format!("Cannot write '{}': {e}", path.display())))?;
    #[cfg(unix)]
    {
        // An existing file keeps its mode on open: set it.
        use std::os::unix::fs::PermissionsExt;
        let _ = file.set_permissions(std::fs::Permissions::from_mode(0o600));
    }
    Ok(file)
}

/// Write `bytes` to a private file.
pub fn write_private(path: &Path, bytes: &[u8]) -> Res<()> {
    let mut file = private_file(path)?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|e| StepError::new(format!("Cannot write/sync '{}': {e}", path.display())))?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

/// The SHA-256 of a file (hex), and its size.
pub fn sha256_file(path: &Path) -> Res<(String, u64)> {
    let mut file = std::fs::File::open(path)
        .map_err(|e| StepError::new(format!("Cannot read '{}': {e}", path.display())))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1 << 20];
    let mut total = 0u64;
    loop {
        let n = file
            .read(&mut buffer)
            .map_err(|e| StepError::new(format!("Cannot read '{}': {e}", path.display())))?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
        total += n as u64;
    }
    Ok((hex::encode(hasher.finalize()), total))
}

// --- Processes -------------------------------------------------------------------------------

/// What a child reads.
pub enum Input<'a> {
    Null,
    Bytes(&'a [u8]),
    File(&'a Path),
}

/// Where a child's standard output goes.
pub enum Output<'a> {
    Capture,
    /// A private file, hashed (SHA-256) while it is written.
    Hashed(&'a Path),
    /// The terminal (`docker pull` progress).
    Inherit,
}

/// A finished child.
#[derive(Debug, Default)]
pub struct Done {
    pub code: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    /// For [`Output::Hashed`]: the SHA-256 (hex) and the size of what was written.
    pub sha256: Option<String>,
    pub bytes: u64,
}

impl Done {
    pub fn ok(&self) -> bool {
        self.code == 0
    }

    pub fn stdout_text(&self) -> String {
        String::from_utf8_lossy(&self.stdout).into_owned()
    }

    /// The last lines of stderr (what a failure message shows).
    pub fn stderr_tail(&self, lines: usize) -> String {
        let text = String::from_utf8_lossy(&self.stderr);
        let all: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
        all[all.len().saturating_sub(lines)..].join("\n")
    }
}

/// Copy a child's output into a file while hashing it. A failed write stops the copy (the child
/// then fails on its closed pipe) and is reported.
fn hash_into(
    mut stream: impl Read + Send + 'static,
    mut file: std::fs::File,
) -> std::thread::JoinHandle<Result<(String, u64), String>> {
    std::thread::spawn(move || {
        let mut hasher = Sha256::new();
        let mut buffer = vec![0u8; 1 << 20];
        let mut total = 0u64;
        loop {
            let n = stream.read(&mut buffer).map_err(|e| e.to_string())?;
            if n == 0 {
                break;
            }
            file.write_all(&buffer[..n]).map_err(|e| e.to_string())?;
            hasher.update(&buffer[..n]);
            total += n as u64;
        }
        file.sync_all().map_err(|e| e.to_string())?;
        Ok((hex::encode(hasher.finalize()), total))
    })
}

/// Isolate a child from the terminal's signals while a command runs.
fn isolate(cmd: &mut Command) -> bool {
    let isolated = signals::phase() != Phase::Idle;
    if isolated {
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            cmd.process_group(0);
        }
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            // CREATE_NEW_PROCESS_GROUP: the console's Ctrl-C is not delivered to it.
            cmd.creation_flags(0x0000_0200);
        }
    }
    isolated
}

/// A `docker` invocation.
pub struct Docker<'a> {
    pub args: Vec<String>,
    /// Variables set for the `docker` process only (values never go on the command line: a
    /// container's `-e NAME` takes the value from here).
    pub env: &'a [(String, String)],
    pub cwd: Option<&'a Path>,
}

impl<'a> Docker<'a> {
    pub fn new<S: AsRef<str>>(args: &[S]) -> Docker<'a> {
        Docker {
            args: args.iter().map(|a| a.as_ref().to_string()).collect(),
            env: &[],
            cwd: None,
        }
    }

    pub fn env(mut self, env: &'a [(String, String)]) -> Docker<'a> {
        self.env = env;
        self
    }

    pub fn cwd(mut self, cwd: &'a Path) -> Docker<'a> {
        self.cwd = Some(cwd);
        self
    }

    fn command(&self) -> Command {
        if DEBUG.load(Ordering::SeqCst) {
            // Never a panic on a closed terminal.
            let _ = writeln!(std::io::stderr(), "docker {}", shell_words(&self.args));
        }
        let mut cmd = Command::new("docker");
        cmd.args(&self.args);
        cmd.envs(self.env.iter().map(|(k, v)| (k, v)));
        if let Some(cwd) = self.cwd {
            cmd.current_dir(cwd);
        }
        cmd
    }

    /// Run with captured output.
    pub fn run(&self) -> Res<Done> {
        self.run_with(Input::Null, Output::Capture)
    }

    /// Run, and fail with `what` and the end of stderr unless it exits with 0.
    pub fn check(&self, what: &str) -> Res<Done> {
        let done = self.run()?;
        if !done.ok() {
            return Err(StepError::new(format!(
                "{what} failed (exit code {}):\n{}",
                done.code,
                done.stderr_tail(20)
            )));
        }
        Ok(done)
    }

    pub fn run_with(&self, input: Input<'_>, output: Output<'_>) -> Res<Done> {
        if stop_requested() {
            return Err(StepError::interrupted());
        }
        let mut cmd = self.command();
        let isolated = isolate(&mut cmd);
        let mut bytes_in: Option<Vec<u8>> = None;
        match input {
            Input::Null => {
                cmd.stdin(Stdio::null());
            }
            Input::Bytes(b) => {
                cmd.stdin(Stdio::piped());
                bytes_in = Some(b.to_vec());
            }
            Input::File(path) => {
                let file = std::fs::File::open(path).map_err(|e| {
                    StepError::new(format!("Cannot read '{}': {e}", path.display()))
                })?;
                cmd.stdin(Stdio::from(file));
            }
        }
        let mut hashed_file = None;
        match output {
            Output::Capture => {
                cmd.stdout(Stdio::piped());
            }
            Output::Hashed(path) => {
                hashed_file = Some((private_file(path)?, path.to_path_buf()));
                cmd.stdout(Stdio::piped());
            }
            Output::Inherit => {
                cmd.stdout(Stdio::inherit());
            }
        }
        let inherit_err = matches!(output, Output::Inherit);
        cmd.stderr(if inherit_err {
            Stdio::inherit()
        } else {
            Stdio::piped()
        });
        let mut child = cmd.spawn().map_err(|e| {
            StepError::new(format!(
                "Cannot run `docker`: {e}. Is Docker installed and on PATH?"
            ))
        })?;
        let writer = match (bytes_in, child.stdin.take()) {
            (Some(bytes), Some(mut stdin)) => Some(std::thread::spawn(move || {
                // A child that exits before reading everything closes the pipe: not an error.
                let _ = stdin.write_all(&bytes);
            })),
            _ => None,
        };
        let (stdout, hashed) = match hashed_file {
            Some((file, path)) => (
                None,
                child.stdout.take().map(|s| (hash_into(s, file), path)),
            ),
            None => (child.stdout.take().map(drain), None),
        };
        let stderr = child.stderr.take().map(drain);
        let started = Instant::now();
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) => {}
                Err(e) => return Err(StepError::new(format!("Waiting for `docker`: {e}"))),
            }
            if isolated && stop_requested() {
                let _ = child.kill();
                let _ = child.wait();
                return Err(StepError::interrupted());
            }
            let pause = if started.elapsed() < Duration::from_secs(2) {
                5
            } else {
                100
            };
            std::thread::sleep(Duration::from_millis(pause));
        };
        if let Some(writer) = writer {
            let _ = writer.join();
        }
        let mut done = Done {
            code: status.code().unwrap_or(-1),
            stdout: stdout
                .map(|h| h.join().unwrap_or_default())
                .unwrap_or_default(),
            stderr: stderr
                .map(|h| h.join().unwrap_or_default())
                .unwrap_or_default(),
            sha256: None,
            bytes: 0,
        };
        if let Some((handle, path)) = hashed {
            match handle.join() {
                Ok(Ok((sha, bytes))) => {
                    done.sha256 = Some(sha);
                    done.bytes = bytes;
                }
                Ok(Err(e)) => {
                    return Err(StepError::new(format!(
                        "Cannot write '{}': {e}",
                        path.display()
                    )));
                }
                Err(_) => {
                    return Err(StepError::new(format!(
                        "Cannot write '{}'.",
                        path.display()
                    )));
                }
            }
        }
        Ok(done)
    }
}

/// A command line for display (single quotes where needed).
fn shell_words(args: &[String]) -> String {
    args.iter()
        .map(|a| {
            if !a.is_empty()
                && a.chars()
                    .all(|c| c.is_ascii_alphanumeric() || "-_./:=@,+%".contains(c))
            {
                a.clone()
            } else {
                format!("'{}'", a.replace('\'', "'\\''"))
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn drain<R: Read + Send + 'static>(mut stream: R) -> std::thread::JoinHandle<Vec<u8>> {
    std::thread::spawn(move || {
        let mut buffer = Vec::new();
        let _ = stream.read_to_end(&mut buffer);
        buffer
    })
}

/// `docker ps` answers.
pub fn docker_available() -> bool {
    Docker::new(&["ps", "-q", "--no-trunc", "--filter", "label=none"])
        .run()
        .is_ok_and(|d| d.ok())
}

/// Whether an image is present locally.
pub fn image_exists(image: &str) -> bool {
    Docker::new(&["image", "inspect", "--format", "{{.Id}}", image])
        .run()
        .is_ok_and(|d| d.ok())
}

/// The image's configured environment and declared volumes.
pub struct ImageConfig {
    pub env: Vec<(String, String)>,
    pub volumes: Vec<String>,
}

impl ImageConfig {
    pub fn env_var(&self, name: &str) -> Option<&str> {
        self.env
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

pub fn image_config(image: &str) -> Res<ImageConfig> {
    let done = Docker::new(&["image", "inspect", "--format", "{{json .Config}}", image])
        .check(&format!("Reading the image '{image}'"))?;
    let config: Json = serde_json::from_slice(&done.stdout)
        .map_err(|e| StepError::new(format!("Cannot parse the configuration of '{image}': {e}")))?;
    let env = config
        .get("Env")
        .and_then(Json::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Json::as_str)
                .filter_map(|kv| kv.split_once('='))
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let volumes = config
        .get("Volumes")
        .and_then(Json::as_object)
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default();
    Ok(ImageConfig { env, volumes })
}

pub fn volume_exists(name: &str) -> bool {
    Docker::new(&["volume", "inspect", "--format", "{{.Name}}", name])
        .run()
        .is_ok_and(|d| d.ok())
}

/// The names of the containers (running or not) that mount volume `name`.
pub fn volume_users(name: &str) -> Res<Vec<String>> {
    let done = Docker::new(&[
        "ps",
        "-a",
        "--filter",
        &format!("volume={name}"),
        "--format",
        "{{.Names}}",
    ])
    .check("Listing the containers of a volume")?;
    Ok(lines(&done.stdout_text()))
}

fn lines(text: &str) -> Vec<String> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect()
}

/// Whether a container of that exact name exists.
pub fn container_exists(name: &str) -> bool {
    Docker::new(&["container", "inspect", "--format", "{{.Name}}", name])
        .run()
        .is_ok_and(|d| d.ok())
}

/// A label of a container (`None` when it is not set or the container does not exist).
pub fn container_label(name: &str, label: &str) -> Option<String> {
    let format = format!("{{{{index .Config.Labels \"{label}\"}}}}");
    let done = Docker::new(&["container", "inspect", "--format", &format, name])
        .run()
        .ok()?;
    let value = done.stdout_text().trim().to_string();
    (done.ok() && !value.is_empty() && value != "<no value>").then_some(value)
}

/// `docker rm -f` of one of OUR containers (by its exact name).
pub fn remove_container(name: &str) -> Res<()> {
    if !container_exists(name) {
        return Ok(());
    }
    Docker::new(&["rm", "-f", "-v", name]).check(&format!("Removing the container '{name}'"))?;
    Ok(())
}

/// Remove every container a run created (they carry `label=<run id>`), each by its exact name.
pub fn remove_run_containers(label: &str, run_id: &str) -> Vec<String> {
    let mut problems = Vec::new();
    let listed = Docker::new(&[
        "ps",
        "-a",
        "--filter",
        &format!("label={label}={run_id}"),
        "--format",
        "{{.Names}}",
    ])
    .run();
    if let Ok(done) = listed {
        for name in lines(&done.stdout_text()) {
            if let Err(e) = remove_container(&name) {
                problems.push(e.message);
            }
        }
    }
    problems
}

/// A container's state: status, restart count and health (`running 0 healthy`, ...).
pub struct State {
    pub status: String,
    pub restarts: u64,
    pub health: Option<String>,
}

pub fn container_state(id: &str) -> Res<State> {
    let done = Docker::new(&[
        "container",
        "inspect",
        "--format",
        "{{.State.Status}} {{.RestartCount}} {{if .State.Health}}{{.State.Health.Status}}{{end}}",
        id,
    ])
    .check("Reading a container's state")?;
    let text = done.stdout_text();
    let mut parts = text.split_whitespace();
    Ok(State {
        status: parts.next().unwrap_or_default().to_string(),
        restarts: parts.next().and_then(|r| r.parse().ok()).unwrap_or(0),
        health: parts.next().map(str::to_string),
    })
}

/// What identifies one stop of a container: its ID and when it last started and stopped. An
/// exited container that is started again (by `mrsm stack up`, a restart policy after a Docker
/// restart, ...) no longer matches the marks taken when it was stopped, and one that was removed
/// and created again has another ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunMarks {
    pub id: String,
    pub status: String,
    pub started: String,
    pub finished: String,
}

pub fn container_run_marks(container: &str) -> Res<RunMarks> {
    let done = Docker::new(&[
        "container",
        "inspect",
        "--format",
        "{{.Id}} {{.State.Status}} {{.State.StartedAt}} {{.State.FinishedAt}}",
        container,
    ])
    .check("Reading a container's start and stop times")?;
    let text = done.stdout_text();
    let mut parts = text.split_whitespace().map(str::to_string);
    let (Some(id), Some(status), Some(started), Some(finished)) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(StepError::new(format!(
            "Cannot read the start and stop times of '{container}'."
        )));
    };
    Ok(RunMarks {
        id,
        status,
        started,
        finished,
    })
}

/// The image a container was created from (as the compose file names it).
pub fn container_image(id: &str) -> Res<String> {
    let done = Docker::new(&["container", "inspect", "--format", "{{.Config.Image}}", id])
        .check("Reading a container's image")?;
    Ok(done.stdout_text().trim().to_string())
}

/// The names of the volumes a container mounts.
pub fn container_volumes(id: &str) -> Res<Vec<String>> {
    let done = Docker::new(&[
        "container",
        "inspect",
        "--format",
        "{{range .Mounts}}{{if eq .Type \"volume\"}}{{.Name}}\n{{end}}{{end}}",
        id,
    ])
    .check("Reading a container's volumes")?;
    Ok(lines(&done.stdout_text()))
}

/// The Docker daemon's ID (the same daemon reached through another endpoint has the same one).
pub fn daemon_id() -> Res<String> {
    let done = Docker::new(&["info", "--format", "{{.ID}}"]).check("`docker info`")?;
    let id = done.stdout_text().trim().to_string();
    if id.is_empty() || id == "<no value>" {
        return Err(StepError::new(
            "`docker info` does not name the Docker daemon.",
        ));
    }
    Ok(id)
}

/// A string map from a JSON object (`null`: empty).
fn string_map(value: Option<&Json>) -> std::collections::BTreeMap<String, String> {
    value
        .and_then(Json::as_object)
        .map(|m| {
            m.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                .collect()
        })
        .unwrap_or_default()
}

/// What `docker volume inspect` says about a volume.
#[derive(Debug, Clone, Default)]
pub struct VolumeInfo {
    pub driver: String,
    pub options: std::collections::BTreeMap<String, String>,
    pub labels: std::collections::BTreeMap<String, String>,
}

/// Inspect volumes (only the fields above; one JSON object per line).
fn inspect_volumes(names: &[String]) -> Res<Vec<VolumeInfo>> {
    if names.is_empty() {
        return Ok(Vec::new());
    }
    let mut args = vec![
        "volume".to_string(),
        "inspect".to_string(),
        "--format".to_string(),
        "{\"name\":{{json .Name}},\"driver\":{{json .Driver}},\"options\":{{json .Options}},\
         \"labels\":{{json .Labels}}}"
            .to_string(),
    ];
    args.extend(names.iter().cloned());
    let done = Docker::new(&args).check("Inspecting volumes")?;
    Ok(done
        .stdout_text()
        .lines()
        .filter_map(|line| serde_json::from_str::<Json>(line.trim()).ok())
        .map(|v| VolumeInfo {
            driver: v["driver"].as_str().unwrap_or_default().to_string(),
            options: string_map(v.get("options")),
            labels: string_map(v.get("labels")),
        })
        .collect())
}

/// One volume (`None` when it does not exist).
pub fn volume_info(name: &str) -> Option<VolumeInfo> {
    if !volume_exists(name) {
        return None;
    }
    inspect_volumes(&[name.to_string()])
        .ok()?
        .into_iter()
        .next()
}

/// Whether a volume carries `label=<value>` (that is: a run of these commands created it).
pub fn volume_has_label(name: &str, label: &str, value: &str) -> bool {
    volume_info(name).is_some_and(|v| v.labels.get(label).map(String::as_str) == Some(value))
}

/// A container and its labels (`docker container inspect`, only these fields).
#[derive(Debug, Clone, Default)]
pub struct ContainerLabels {
    pub name: String,
    pub labels: std::collections::BTreeMap<String, String>,
}

/// The containers (any state) whose labels match `filter` (`label=...`).
pub fn containers_with(filter: &str) -> Res<Vec<ContainerLabels>> {
    let listed = Docker::new(&["ps", "-a", "-q", "--no-trunc", "--filter", filter])
        .check("Listing containers")?;
    let ids = lines(&listed.stdout_text());
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut args = vec![
        "container".to_string(),
        "inspect".to_string(),
        "--format".to_string(),
        "{\"name\":{{json .Name}},\"labels\":{{json .Config.Labels}}}".to_string(),
    ];
    args.extend(ids);
    // A container removed meanwhile makes `inspect` fail for it alone: keep what it printed.
    let done = Docker::new(&args).run()?;
    Ok(done
        .stdout_text()
        .lines()
        .filter_map(|line| serde_json::from_str::<Json>(line.trim()).ok())
        .map(|v| ContainerLabels {
            name: v["name"]
                .as_str()
                .unwrap_or_default()
                .trim_start_matches('/')
                .to_string(),
            labels: string_map(v.get("labels")),
        })
        .collect())
}

/// Remove a container a run created: only when it carries `label=<run_id>`.
pub fn remove_run_container(name: &str, label: &str, run_id: &str) -> Res<()> {
    match container_label(name, label) {
        Some(value) if value == run_id => remove_container(name),
        _ => Ok(()),
    }
}

/// The last lines of a container's logs (for failure messages about our own containers and the
/// stack's `db`).
pub fn container_logs_tail(name: &str, lines: usize) -> String {
    Docker::new(&["logs", "--tail", &lines.to_string(), name])
        .run()
        .map(|d| {
            format!(
                "{}{}",
                String::from_utf8_lossy(&d.stdout),
                String::from_utf8_lossy(&d.stderr)
            )
        })
        .unwrap_or_default()
}

// --- Compose ---------------------------------------------------------------------------------

/// The stack's Compose project, run the way `mrsm stack` runs it (same working directory,
/// environment and `--project-name`), so this sees exactly what `mrsm stack ...` would.
pub struct Compose {
    pub project: String,
    pub cwd: PathBuf,
    pub env: Vec<(String, String)>,
}

impl Compose {
    fn docker<'a>(&'a self, args: &[&str]) -> Docker<'a> {
        let mut full = vec!["compose", "--project-name", &self.project];
        full.extend_from_slice(args);
        Docker::new(&full).env(&self.env).cwd(&self.cwd)
    }

    /// `docker compose config --format json`: the resolved project (it holds the services'
    /// credentials: keep it in memory, never print it).
    pub fn config(&self) -> Res<Json> {
        let done = self
            .docker(&["config", "--format", "json"])
            .check("`docker compose config`")?;
        serde_json::from_slice(&done.stdout)
            .map_err(|e| StepError::new(format!("Cannot parse `docker compose config`: {e}")))
    }

    /// The services with a running container.
    pub fn running_services(&self) -> Res<Vec<String>> {
        let done = self
            .docker(&["ps", "--services", "--status", "running"])
            .check("`docker compose ps`")?;
        Ok(lines(&done.stdout_text()))
    }

    /// The running container of a service.
    pub fn container_id(&self, service: &str) -> Res<Option<String>> {
        let done = self
            .docker(&["ps", "-q", service])
            .check("`docker compose ps`")?;
        Ok(lines(&done.stdout_text()).into_iter().next())
    }

    /// The container of a service in any state (created, restarting, exited, ...).
    pub fn any_container_id(&self, service: &str) -> Res<Option<String>> {
        let done = self
            .docker(&["ps", "-a", "-q", service])
            .check("`docker compose ps`")?;
        Ok(lines(&done.stdout_text()).into_iter().next())
    }

    pub fn stop(&self, services: &[String], timeout_seconds: u32) -> Res<()> {
        if services.is_empty() {
            return Ok(());
        }
        let timeout = timeout_seconds.to_string();
        let mut args = vec!["stop", "--timeout", &timeout];
        args.extend(services.iter().map(String::as_str));
        self.docker(&args)
            .check(&format!("Stopping {}", services.join(", ")))?;
        Ok(())
    }

    /// `create` for the services: their containers exist, stopped, as the config says.
    pub fn create(&self, services: &[String]) -> Res<()> {
        if services.is_empty() {
            return Ok(());
        }
        let mut args = vec!["create"];
        args.extend(services.iter().map(String::as_str));
        self.docker(&args)
            .check(&format!("Creating {}", services.join(", ")))?;
        Ok(())
    }

    /// `up -d` for the services (Compose also starts what they depend on).
    pub fn up(&self, services: &[String]) -> Res<()> {
        if services.is_empty() {
            return Ok(());
        }
        let mut args = vec!["up", "-d"];
        args.extend(services.iter().map(String::as_str));
        self.docker(&args)
            .check(&format!("Starting {}", services.join(", ")))?;
        Ok(())
    }
}

/// `docker compose` subcommands that change containers (an attached `up`, `logs -f` and the like
/// change nothing by themselves and are never waited for).
const COMPOSE_CHANGING: &[&str] = &[
    "up", "start", "restart", "stop", "create", "down", "kill", "rm",
];

/// The `docker compose` commands of `project` that change containers, in the output of
/// `ps -A -o pid=,args=` (pid, then the command line): `(pid, command line)`, never `own_pid`'s.
/// A killed `mrsm` leaves the compose command it was running behind (its children have their own
/// process group), and it goes on changing containers for a while.
pub fn compose_in_flight(ps: &str, project: &str, own_pid: u32) -> Vec<(u32, String)> {
    let mut found = Vec::new();
    for line in ps.lines() {
        let line = line.trim();
        let Some((pid, command)) = line.split_once(char::is_whitespace) else {
            continue;
        };
        let Ok(pid) = pid.parse::<u32>() else {
            continue;
        };
        let tokens: Vec<&str> = command.split_whitespace().collect();
        let Some(at) = tokens.iter().position(|t| *t == "compose") else {
            continue;
        };
        // `docker compose ...` or the plugin's `<path>/docker-compose compose ...`: the program
        // itself, not an editor or a shell that has the words in its arguments.
        if pid == own_pid
            || at != 1
            || !tokens[0]
                .rsplit('/')
                .next()
                .is_some_and(|name| matches!(name, "docker" | "docker.exe" | "docker-compose"))
        {
            continue;
        }
        let (mut named, mut subcommand, mut i) = (None, None, at + 1);
        while i < tokens.len() {
            let token = tokens[i];
            if token == "--project-name" || token == "-p" {
                named = tokens.get(i + 1).copied();
                i += 2;
            } else if let Some(value) = token.strip_prefix("--project-name=") {
                named = Some(value);
                i += 1;
            } else if token.starts_with('-') {
                i += 1;
            } else {
                subcommand = Some(token);
                break;
            }
        }
        let (Some(named), Some(subcommand)) = (named, subcommand) else {
            continue;
        };
        if named != project || !COMPOSE_CHANGING.contains(&subcommand) {
            continue;
        }
        let detached = tokens[i..].iter().any(|t| *t == "-d" || *t == "--detach");
        if subcommand == "up" && !detached {
            continue;
        }
        found.push((pid, command.to_string()));
    }
    found
}

/// The `docker compose` commands of `project` running now (`None`: the process list cannot be
/// read here, e.g. on Windows).
pub fn compose_commands_running(project: &str) -> Option<Vec<(u32, String)>> {
    let out = std::process::Command::new("ps")
        .args(["-A", "-o", "pid=,args="])
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    Some(compose_in_flight(
        &String::from_utf8_lossy(&out.stdout),
        project,
        std::process::id(),
    ))
}

// --- PostgreSQL inside a container -------------------------------------------------------------

/// Run a client tool inside a container as the database superuser the image created
/// (`POSTGRES_USER`, else `postgres`), over the container's local socket. The user and password
/// come from the container's own environment, so no credential is passed on a command line.
const PG_ENV: &str = "export PGUSER=\"${PGUSER:-${POSTGRES_USER:-postgres}}\"; \
if [ -z \"${PGPASSWORD:-}\" ] && [ -n \"${POSTGRES_PASSWORD:-}\" ]; then \
export PGPASSWORD=\"$POSTGRES_PASSWORD\"; fi; exec \"$@\"";

/// The server's refusal of a login, from the end of `psql`'s stderr (`None`: it worked, or
/// failed for a reason that says nothing about the credentials).
pub fn login_refusal(succeeded: bool, stderr: &str) -> Option<String> {
    (!succeeded && stderr.contains("authentication failed")).then(|| stderr.trim().to_string())
}

/// A `dbname='...'` connection string (a bare `-d` value with `=` would be read as one).
pub fn conninfo(database: &str) -> String {
    format!(
        "dbname='{}'",
        database.replace('\\', "\\\\").replace('\'', "\\'")
    )
}

/// A PostgreSQL server reachable through `docker exec` into its container.
#[derive(Debug, Clone)]
pub struct PgContainer {
    pub container: String,
}

impl PgContainer {
    pub fn new(container: impl Into<String>) -> PgContainer {
        PgContainer {
            container: container.into(),
        }
    }

    fn tool_args(&self, program: &str, args: &[String]) -> Vec<String> {
        let mut full = vec![
            "exec".to_string(),
            "-i".to_string(),
            self.container.clone(),
            "sh".to_string(),
            "-c".to_string(),
            PG_ENV.to_string(),
            "mrsm-upgrade".to_string(),
            program.to_string(),
        ];
        full.extend(args.iter().cloned());
        full
    }

    /// Run a client tool with the given input and output.
    pub fn tool(
        &self,
        program: &str,
        args: &[String],
        input: Input<'_>,
        output: Output<'_>,
    ) -> Res<Done> {
        Docker::new(&self.tool_args(program, args)).run_with(input, output)
    }

    fn psql_args(database: &str) -> Vec<String> {
        vec![
            "-X".into(),
            "-q".into(),
            "-A".into(),
            "-t".into(),
            "-v".into(),
            "ON_ERROR_STOP=1".into(),
            "-d".into(),
            conninfo(database),
        ]
    }

    /// Run SQL with `psql -X` in a NEW session: `ON_ERROR_STOP`, unaligned tuples only, no
    /// command tags (`-q`). The first statement of `sql` is the first statement of the session
    /// (no `psqlrc`).
    pub fn psql(&self, database: &str, sql: &str) -> Res<String> {
        let done = self.tool(
            "psql",
            &Self::psql_args(database),
            Input::Bytes(sql.as_bytes()),
            Output::Capture,
        )?;
        if !done.ok() {
            return Err(StepError::new(format!(
                "SQL failed in database '{database}':\n{}",
                done.stderr_tail(20)
            )));
        }
        Ok(done.stdout_text())
    }

    /// What the server says when the stack's own login (`POSTGRES_USER` with `POSTGRES_PASSWORD`
    /// from the container's environment) is refused over TCP from the container's own
    /// (non-loopback) address, the way the stack's services connect. `None`: it works, or the
    /// check could not be made (nothing is claimed then). A restored or rolled-back volume
    /// carries the roles' passwords of its own time, which the stack config may no longer say.
    pub fn refused_login(&self) -> Option<String> {
        const SCRIPT: &str = "export PGUSER=\"${POSTGRES_USER:-postgres}\" \
             PGPASSWORD=\"${POSTGRES_PASSWORD:-}\" PGCONNECT_TIMEOUT=10; \
             host=$(hostname -i 2>/dev/null | cut -d' ' -f1); [ -n \"$host\" ] || exit 0; \
             exec psql -X -At -h \"$host\" -d template1 -c 'SELECT 1'";
        let done = Docker::new(&["exec", &self.container, "sh", "-c", SCRIPT])
            .run()
            .ok()?;
        login_refusal(done.ok(), &done.stderr_tail(3))
    }

    /// One JSON value from a query that returns it (the last non-empty line of the output).
    pub fn json(&self, database: &str, sql: &str) -> Res<Json> {
        let out = self.psql(database, sql)?;
        let line = out
            .lines()
            .map(str::trim)
            .rfind(|l| !l.is_empty())
            .unwrap_or("null");
        serde_json::from_str(line).map_err(|e| {
            StepError::new(format!(
                "Cannot parse the server's answer in database '{database}': {e}"
            ))
        })
    }

    /// Whether the server answers SQL on its real listener (not the setup server the image's
    /// entrypoint runs with `listen_addresses=''` while it initializes a new volume).
    pub fn ready(&self) -> bool {
        self.psql(
            "template1",
            "SELECT current_setting('listen_addresses') <> '';",
        )
        .is_ok_and(|out| out.trim() == "t")
    }

    /// Open a session that exports a snapshot (`pg_export_snapshot()`) and keeps it until
    /// [`Snapshot::finish`]: `pg_dump --snapshot` and other sessions (`SET TRANSACTION
    /// SNAPSHOT`) then see exactly the same data.
    pub fn export_snapshot(&self, database: &str) -> Res<Snapshot> {
        let mut cmd = Docker::new(&self.tool_args("psql", &Self::psql_args(database))).command();
        isolate(&mut cmd);
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let mut child = cmd
            .spawn()
            .map_err(|e| StepError::new(format!("Cannot run `docker`: {e}")))?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| StepError::new("No input pipe to `psql`."))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| StepError::new("No error pipe from `psql`."))?;
        let (send, lines) = channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if send.send(line).is_err() {
                    break;
                }
            }
        });
        // `\warn` writes to stderr, which is not buffered: the ID arrives at once (psql 13+).
        stdin
            .write_all(
                b"BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;\n\
                  SELECT pg_catalog.pg_export_snapshot() AS mrsm_snapshot \\gset\n\
                  \\warn MRSM_SNAPSHOT :mrsm_snapshot\n",
            )
            .and_then(|_| stdin.flush())
            .map_err(|e| StepError::new(format!("Cannot talk to `psql`: {e}")))?;
        let mut snapshot = Snapshot {
            child,
            stdin: Some(stdin),
            lines,
            id: String::new(),
        };
        snapshot.id = snapshot.wait_for_id(database)?;
        Ok(snapshot)
    }
}

/// A session holding an exported snapshot.
pub struct Snapshot {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Receiver<String>,
    pub id: String,
}

impl Snapshot {
    fn wait_for_id(&mut self, database: &str) -> Res<String> {
        let deadline = Instant::now() + Duration::from_secs(120);
        let mut said = Vec::new();
        loop {
            if stop_requested() {
                return Err(StepError::interrupted());
            }
            match self.lines.recv_timeout(Duration::from_millis(200)) {
                Ok(line) => {
                    if let Some(id) = line.trim().strip_prefix("MRSM_SNAPSHOT ") {
                        let id = id.trim();
                        if !id.is_empty()
                            && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
                        {
                            return Ok(id.to_string());
                        }
                    }
                    said.push(line);
                }
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(StepError::new(format!(
                        "Cannot export a snapshot of '{database}':\n{}",
                        said.join("\n")
                    )));
                }
                Err(RecvTimeoutError::Timeout) => {}
            }
            if Instant::now() > deadline {
                return Err(StepError::new(format!(
                    "Exporting a snapshot of '{database}' timed out:\n{}",
                    said.join("\n")
                )));
            }
        }
    }

    /// End the snapshot's transaction and the session.
    pub fn finish(mut self) -> Res<()> {
        if let Some(mut stdin) = self.stdin.take() {
            let _ = stdin.write_all(b"COMMIT;\n\\q\n");
        }
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => return Ok(()),
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(50))
                }
                _ => {
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    return Ok(());
                }
            }
        }
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        self.stdin.take();
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// Wait until `pg` answers on its real listener, the container stops, or `timeout` passes.
pub fn wait_ready(pg: &PgContainer, timeout: Duration) -> Res<()> {
    let started = Instant::now();
    loop {
        if stop_requested() {
            return Err(StepError::interrupted());
        }
        if pg.ready() {
            return Ok(());
        }
        if let Ok(state) = container_state(&pg.container)
            && matches!(state.status.as_str(), "exited" | "dead")
        {
            return Err(StepError::new(format!(
                "The container '{}' stopped while PostgreSQL was starting:\n{}",
                pg.container,
                container_logs_tail(&pg.container, 30)
            )));
        }
        if started.elapsed() > timeout {
            return Err(StepError::new(format!(
                "PostgreSQL in '{}' did not become ready within {} seconds:\n{}",
                pg.container,
                timeout.as_secs(),
                container_logs_tail(&pg.container, 30)
            )));
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Wait until a Compose service's container is healthy (or answers SQL when it has no
/// healthcheck), failing early when it stops or keeps restarting.
pub fn wait_healthy(compose: &Compose, service: &str, timeout: Duration) -> Res<String> {
    let started = Instant::now();
    let mut last = String::new();
    let mut first_restarts: Option<u64> = None;
    loop {
        if stop_requested() {
            return Err(StepError::interrupted());
        }
        if let Some(id) = compose.any_container_id(service)? {
            let state = container_state(&id)?;
            last = format!("{} {}", state.status, state.health.as_deref().unwrap_or(""));
            let first = *first_restarts.get_or_insert(state.restarts);
            match (state.status.as_str(), state.health.as_deref()) {
                ("running", Some("healthy")) => return Ok(id),
                ("running", None) if PgContainer::new(id.clone()).ready() => return Ok(id),
                ("exited" | "dead", _) => {
                    return Err(StepError::new(format!(
                        "The '{service}' service stopped while starting:\n{}",
                        container_logs_tail(&id, 30)
                    )));
                }
                _ if state.restarts >= first + 2 => {
                    return Err(StepError::new(format!(
                        "The '{service}' service keeps restarting:\n{}",
                        container_logs_tail(&id, 30)
                    )));
                }
                _ => {}
            }
        }
        if started.elapsed() > timeout {
            return Err(StepError::new(format!(
                "The '{service}' service did not become healthy within {} seconds (last state: \
                 {}).",
                timeout.as_secs(),
                last.trim()
            )));
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_compose_command_left_behind_is_found_by_its_project() {
        let ps = "\
  101 /usr/bin/python3 -m meerschaum stack ps
  202 docker compose --project-name w18x up -d holder
  203 /usr/libexec/docker/cli-plugins/docker-compose compose --project-name w18x up -d holder
  204 docker compose --project-name w18x up
  205 docker compose --project-name w18x logs -f db
  206 docker compose --project-name other up -d db
  207 docker compose --project-name=w18x stop --timeout 60 db
  208 docker compose --project-name w18xy up -d db
  209 docker compose --project-name w18x ps
  210 vim docker compose --project-name w18x up -d
  211 docker compose --project-name w18x restart db
  bad line
";
        let found = compose_in_flight(ps, "w18x", 0);
        let pids: Vec<u32> = found.iter().map(|(p, _)| *p).collect();
        // Detached `up`, `stop`, `restart` change containers; an attached `up`, `logs`, `ps`,
        // another project, a project of a longer name and an editor are not waited for.
        assert_eq!(pids, vec![202, 203, 207, 211], "{found:?}");
        // The command of this very process is never its own obstacle.
        let own = compose_in_flight(ps, "w18x", 202);
        assert!(own.iter().all(|(p, _)| *p != 202));
        assert!(compose_in_flight("", "w18x", 0).is_empty());
    }

    #[test]
    fn connection_strings_quote_any_database_name() {
        assert_eq!(conninfo("meerschaum"), "dbname='meerschaum'");
        assert_eq!(conninfo("a=b c"), "dbname='a=b c'");
        assert_eq!(conninfo("it's\\"), "dbname='it\\'s\\\\'");
    }

    #[test]
    fn only_a_refused_password_says_something_about_the_credentials() {
        let refused = "psql: error: connection to server at \"172.18.0.2\", port 5432 failed: FATAL:  password authentication failed for user \"mrsm\"";
        assert_eq!(login_refusal(false, refused).as_deref(), Some(refused));
        // It worked, or it failed for another reason (no route, starting up, no such tool).
        assert_eq!(login_refusal(true, ""), None);
        assert_eq!(login_refusal(true, refused), None);
        assert_eq!(
            login_refusal(false, "psql: error: connection refused"),
            None
        );
        assert_eq!(login_refusal(false, "sh: psql: not found"), None);
    }

    #[test]
    fn stderr_tails_skip_blank_lines() {
        let done = Done {
            code: 1,
            stderr: b"one\n\ntwo\nthree\n".to_vec(),
            ..Done::default()
        };
        assert_eq!(done.stderr_tail(2), "two\nthree");
        assert_eq!(done.stderr_tail(10), "one\ntwo\nthree");
    }

    #[test]
    fn private_files_and_directories() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("backups").join("postgres").join("run");
        private_dir(&nested).unwrap();
        write_private(&nested.join("globals.sql"), b"CREATE ROLE x;\n").unwrap();
        let (sha, size) = sha256_file(&nested.join("globals.sql")).unwrap();
        assert_eq!(size, 15);
        assert_eq!(sha.len(), 64);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode(&nested), 0o700);
            assert_eq!(mode(&dir.path().join("backups")), 0o700);
            assert_eq!(mode(&nested.join("globals.sql")), 0o600);
        }
    }

    #[test]
    fn command_lines_are_quoted_for_display() {
        assert_eq!(
            shell_words(&["exec".into(), "-i".into(), "a b".into(), "it's".into()]),
            "exec -i 'a b' 'it'\\''s'"
        );
    }
}
