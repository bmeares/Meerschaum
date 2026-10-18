//! The `api:<label>` job executor against a real loopback `mrsm start api` (random port, never
//! 8989) with temporary roots: create / persist / start / ps / logs / pause / stop / delete,
//! idempotent start, authentication failure and unreachable endpoints as errors, and Compose
//! projects sharing one remote executor without touching each other's jobs.
//!
//! Everything is synthetic: temporary roots and plugin directories, scrubbed environments, jobs
//! that run `show version`. The jobs run on the *server's* root; the server's jobs are cleaned
//! up (and any worker still alive is killed) when the fixture drops.

use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

/// The job: prints a marker and sleeps until it is stopped.
const JOB_SCRIPT: &str = "#!/bin/sh\necho MRSM_TEST_MARK\nexec sleep 120\n";

struct Home {
    root: tempfile::TempDir,
    plugins: tempfile::TempDir,
}

impl Home {
    fn new() -> Home {
        Home {
            root: tempfile::tempdir().unwrap(),
            plugins: tempfile::tempdir().unwrap(),
        }
    }

    fn command(&self, endpoints: &[(&str, u16)]) -> Command {
        self.command_on(endpoints, None)
    }

    /// The command with `meerschaum:executor` configured: what v4 users do to create jobs on a
    /// remote API (`mrsm <action> -d` puts the job on the context's executor; an `-e` would be
    /// recorded in the job's own sysargs).
    fn command_on(&self, endpoints: &[(&str, u16)], executor: Option<&str>) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.root.path())
            .env("MRSM_ROOT_DIR", self.root.path())
            .env("MRSM_PLUGINS_DIR", self.plugins.path())
            .env("MRSM_PYTHON", self.root.path().join("no-python"))
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .env(
                "MRSM_CONFIG",
                match executor {
                    Some(keys) => format!(
                        r#"{{"meerschaum":{{"executor":"{keys}"}},"shell":{{"updates":{{"check":false}}}},"formatting":{{"ansi":false,"unicode":false}},"system":{{"cli":{{"check_updates":false}}}}}}"#
                    ),
                    None => r#"{"shell":{"updates":{"check":false}},"formatting":{"ansi":false,"unicode":false},"system":{"cli":{"check_updates":false}}}"#.to_string(),
                },
            );
        for (label, port) in endpoints {
            command.env(
                format!("MRSM_API_{}", label.to_uppercase()),
                format!("http://127.0.0.1:{port}"),
            );
        }
        command
    }

    fn run(&self, endpoints: &[(&str, u16)], args: &[&str]) -> Output {
        self.command(endpoints)
            .args(args)
            .stdin(Stdio::null())
            .output()
            .unwrap()
    }

    fn jobs_dir(&self) -> PathBuf {
        self.root.path().join("jobs")
    }

    /// The path of the job's script (under this root, so a server running the job can read it).
    fn script(&self) -> PathBuf {
        let path = self.root.path().join("job.sh");
        std::fs::write(&path, JOB_SCRIPT).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        path
    }

    /// `mrsm sh <script> -d --name <name> --noask` with `executor` as the context's executor:
    /// creates and starts the job there.
    fn create_job(
        &self,
        endpoints: &[(&str, u16)],
        executor: &str,
        script: &Path,
        name: &str,
    ) -> Output {
        self.command_on(endpoints, Some(executor))
            .arg("sh")
            .arg(script)
            .args(["-d", "--name", name, "--noask"])
            .stdin(Stdio::null())
            .output()
            .unwrap()
    }
}

fn text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn free_port() -> u16 {
    // The tests of this file start servers at the same time: a port which was released a moment
    // ago would be handed to a second test (whose client then talks to the first one's server),
    // so every port handed out is remembered.
    static HANDED_OUT: std::sync::Mutex<Vec<u16>> = std::sync::Mutex::new(Vec::new());
    loop {
        let socket = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = socket.local_addr().unwrap().port();
        let mut taken = HANDED_OUT.lock().unwrap();
        if port != 8989 && !taken.contains(&port) {
            taken.push(port);
            return port;
        }
    }
}

/// `mrsm start api` on a random loopback port over a temporary root.
struct Server {
    home: Home,
    child: Child,
    port: u16,
}

impl Server {
    /// A started server which answers HTTP. Under load a port which was free a moment ago can be
    /// taken by another test binary before this server binds it (then the server exits, or a
    /// foreign listener answers for it), so a failed attempt is retried on a new port.
    fn start(no_auth: bool) -> Server {
        let mut last = String::new();
        for _ in 0..5 {
            match Server::try_start(no_auth) {
                Ok(server) => return server,
                Err(why) => last = why,
            }
        }
        panic!("the server did not start in 5 attempts: {last}");
    }

    fn try_start(no_auth: bool) -> Result<Server, String> {
        let home = Home::new();
        let port = free_port();
        let mut command = home.command(&[]);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        command
            .args([
                "--noask",
                "start",
                "api",
                "-i",
                "sql:local",
                "--host",
                "127.0.0.1",
                "-p",
            ])
            .arg(port.to_string())
            .args(["--no-dash", "-w", "2"]);
        if no_auth {
            command.arg("--no-auth");
        }
        let log = std::fs::File::create(home.root.path().join("server.log")).unwrap();
        let child = command
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        let mut server = Server { home, child, port };
        // Ready = a whole HTTP answer, not merely an accepted connection: under load a listener
        // can accept before the app serves, and the early connection is reset.
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = server.child.try_wait().unwrap() {
                return Err(format!("the server exited ({status}) before it served"));
            }
            if try_raw_request(port, "GET", "/healthcheck")
                .is_ok_and(|status| status.starts_with("HTTP/1.1 200"))
            {
                break;
            }
            if Instant::now() >= deadline {
                return Err("the server did not serve in 60 s".into());
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        // The answer must be ours: had another process taken the port, our server would have
        // failed to bind and be gone by now.
        std::thread::sleep(Duration::from_millis(300));
        if let Some(status) = server.child.try_wait().unwrap() {
            return Err(format!("the server exited ({status}): its port was taken"));
        }
        Ok(server)
    }

    /// The PIDs of the job workers recorded under the server's root.
    fn worker_pids(&self) -> Vec<i32> {
        let Ok(entries) = std::fs::read_dir(self.home.jobs_dir()) else {
            return Vec::new();
        };
        entries
            .flatten()
            .filter_map(|e| std::fs::read_to_string(e.path().join("process.pid")).ok())
            .filter_map(|t| t.trim().parse().ok())
            .collect()
    }

    fn job_names(&self) -> Vec<String> {
        let Ok(entries) = std::fs::read_dir(self.home.jobs_dir()) else {
            return Vec::new();
        };
        let mut names: Vec<String> = entries
            .flatten()
            .filter(|e| e.path().join("properties.json").exists())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        names.sort();
        names
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        // Workers are detached sessions of their own: kill the ones still alive.
        // (and everything below them: the job's own processes)
        // Only supervisors which really run under this root's `jobs/` (a recycled PID in a
        // stale `process.pid` is somebody else's process).
        let jobs_dir = self.home.jobs_dir().to_string_lossy().to_string();
        let ours = |pid: i32| {
            std::fs::read(format!("/proc/{pid}/cmdline"))
                .map(|c| String::from_utf8_lossy(&c).contains(&jobs_dir))
                .unwrap_or(false)
        };
        let mut doomed: Vec<i32> = Vec::new();
        let mut queue: Vec<i32> = self
            .worker_pids()
            .into_iter()
            .filter(|p| ours(*p))
            .collect();
        while let Some(pid) = queue.pop() {
            doomed.push(pid);
            let out = Command::new("pgrep")
                .args(["-P", &pid.to_string()])
                .output()
                .unwrap();
            queue.extend(
                String::from_utf8_lossy(&out.stdout)
                    .split_whitespace()
                    .filter_map(|p| p.parse::<i32>().ok()),
            );
        }
        for pid in doomed {
            let _ = Command::new("kill")
                .args(["-9", &pid.to_string()])
                .stderr(Stdio::null())
                .status();
        }
        #[cfg(unix)]
        {
            let _ = Command::new("kill")
                .args(["-TERM", &format!("-{}", self.child.id())])
                .stderr(Stdio::null())
                .status();
            let deadline = Instant::now() + Duration::from_secs(3);
            while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn wait_until(secs: u64, what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("timed out waiting for {what}");
}

fn shown(client: &Home, port: u16, extra: &[&str]) -> String {
    let mut args = vec!["show", "jobs", "-e", "api:shared", "--nopretty"];
    args.extend_from_slice(extra);
    text(&client.run(&[("shared", port)], &args))
}

#[test]
fn api_executor_round_trip_against_a_loopback_server() {
    let server = Server::start(true);
    let client = Home::new();
    let endpoints = [("shared", server.port)];
    let ex = |args: &[&str]| {
        let mut full: Vec<&str> = args.to_vec();
        full.extend_from_slice(&["-e", "api:shared"]);
        client.run(&endpoints, &full)
    };

    // Nothing yet: an empty listing succeeds and says so.
    let empty = client.run(&endpoints, &["show", "jobs", "-e", "api:shared"]);
    assert!(empty.status.success(), "{}", text(&empty));
    assert!(text(&empty).contains("No running or stopped jobs."));

    // Create and start (the job runs on the server's root).
    let script = server.home.script();
    let created = client.create_job(&endpoints, "api:shared", &script, "alpha");
    assert!(created.status.success(), "{}", text(&created));
    assert!(text(&created).contains("alpha"), "{}", text(&created));
    assert_eq!(server.job_names(), vec!["alpha"]);
    // Nothing leaks into the client's own root.
    assert!(!client.jobs_dir().join("alpha").exists());

    // Listing and status come from the remote.
    wait_until(30, "the job to report running", || {
        let out = ex(&["show", "jobs"]);
        text(&out).contains("running")
    });
    let listing = shown(&client, server.port, &[]);
    assert!(listing.contains("alpha"), "{listing}");

    // Logs through `GET /logs/{name}`.
    wait_until(30, "the job's first log line", || {
        let out = ex(&["show", "logs", "alpha", "--nopretty"]);
        text(&out).contains("MRSM_TEST_MARK")
    });

    // Idempotent start: the running job is not touched.
    let pid_before = server.worker_pids();
    assert_eq!(pid_before.len(), 1);
    let again = ex(&["start", "jobs", "alpha", "-f"]);
    assert!(again.status.success(), "{}", text(&again));
    assert_eq!(server.job_names(), vec!["alpha"]);
    assert_eq!(server.worker_pids(), pid_before);

    // Pause and resume.
    let paused = ex(&["pause", "jobs", "alpha", "-f"]);
    assert!(paused.status.success(), "{}", text(&paused));
    wait_until(20, "the job to report paused", || {
        text(&ex(&["show", "jobs"])).contains("paused")
    });
    let resumed = ex(&["start", "jobs", "alpha", "-f"]);
    assert!(resumed.status.success(), "{}", text(&resumed));
    wait_until(20, "the job to report running again", || {
        let t = text(&ex(&["show", "jobs"]));
        t.contains("running") && !t.contains("paused")
    });

    // Stop, then delete.
    let stopped = ex(&["stop", "jobs", "alpha", "-f"]);
    assert!(stopped.status.success(), "{}", text(&stopped));
    wait_until(30, "the job to report stopped", || {
        text(&ex(&["show", "jobs"])).contains("stopped")
    });
    let deleted = ex(&["delete", "jobs", "alpha", "-f"]);
    assert!(deleted.status.success(), "{}", text(&deleted));
    assert!(server.job_names().is_empty());
    let after = client.run(&endpoints, &["show", "jobs", "-e", "api:shared"]);
    assert!(text(&after).contains("No running or stopped jobs."));
}

#[test]
fn unreachable_and_unauthorized_endpoints_are_errors_not_empty_listings() {
    // Unreachable: a port nobody listens on.
    let client = Home::new();
    let dead = free_port();
    // PARITY: `--nopretty` also suppresses the failing exit code, so the failure is read
    // from the printed result.
    let out = client.run(&[("dead", dead)], &["show", "jobs", "-e", "api:dead"]);
    let t = text(&out);
    assert!(!out.status.success(), "{t}");
    assert!(!t.contains("No running or stopped jobs."), "{t}");
    assert!(t.contains("api:dead"), "{t}");
    let started = client.create_job(&[("dead", dead)], "api:dead", &client.script(), "ghost");
    assert!(!started.status.success(), "{}", text(&started));
    assert!(text(&started).contains("api:dead"), "{}", text(&started));
    assert!(!client.jobs_dir().join("ghost").exists());

    // Unauthorized: a server with authentication and a client without credentials.
    let server = Server::start(false);
    let out = client.run(
        &[("shared", server.port)],
        &["show", "jobs", "-e", "api:shared"],
    );
    let t = text(&out);
    assert!(!out.status.success(), "{t}");
    assert!(!t.contains("No running or stopped jobs."), "{t}");
    assert!(
        t.contains("401") || t.to_lowercase().contains("authentication"),
        "{t}"
    );
    let started = client.create_job(
        &[("shared", server.port)],
        "api:shared",
        &client.script(),
        "ghost",
    );
    assert!(!started.status.success(), "{}", text(&started));
    // Nothing was created on the server.
    assert!(server.job_names().is_empty());
}

struct Project {
    dir: tempfile::TempDir,
    port: u16,
}

impl Project {
    fn new(name: &str, port: u16, jobs: &str) -> Project {
        let dir = tempfile::tempdir().unwrap();
        for sub in ["root", "plugins", "host", "host-plugins", "home"] {
            std::fs::create_dir(dir.path().join(sub)).unwrap();
        }
        let body = format!(
            "project_name: {name}\nroot_dir: ./root\nplugins_dir: [./plugins]\nconfig:\n  meerschaum:\n    instance: sql:local\n    executor: api:shared\n    connectors:\n      api:\n        shared: {{host: 127.0.0.1, port: {port}, protocol: http}}\n  system:\n    cli:\n      check_updates: false\njobs:\n{jobs}\n"
        );
        std::fs::write(dir.path().join("mrsm-compose.yaml"), body).unwrap();
        Project { dir, port }
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_mrsm-cli"))
            .args(args)
            .current_dir(self.path())
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.path().join("home"))
            .env("MRSM_ROOT_DIR", self.path().join("host"))
            .env("MRSM_PLUGINS_DIR", self.path().join("host-plugins"))
            .env("MRSM_PYTHON", self.path().join("no-python"))
            .env(
                "MRSM_CONFIG",
                r#"{"meerschaum":{"instance":"sql:local"},"system":{"cli":{"check_updates":false}},"formatting":{"ansi":false,"unicode":false}}"#,
            )
            .stdin(Stdio::null())
            .output()
            .unwrap()
    }

    fn ok(&self, args: &[&str]) -> String {
        let out = self.run(args);
        assert!(out.status.success(), "{args:?}: {}", text(&out));
        text(&out)
    }
}

#[test]
fn compose_projects_share_one_remote_executor_without_touching_each_other() {
    let server = Server::start(true);
    let p = Project::new("proj-p", server.port, "  p-job: show version");
    let q = Project::new("proj-q", server.port, "  q-job: show version");
    assert_eq!(p.port, q.port);

    p.ok(&["compose", "up"]);
    q.ok(&["compose", "up"]);
    assert_eq!(server.job_names(), vec!["p-job", "q-job"]);

    // PARITY: `compose ps` is `show jobs` on the project's executor (`subactions/ps.py`), so
    // both projects see every job of the shared executor.
    for project in [&p, &q] {
        let ps = project.ok(&["compose", "ps", "--nopretty"]);
        assert!(ps.contains("p-job") && ps.contains("q-job"), "{ps}");
    }

    // The project's name is stamped on the remote job's environment.
    let meta =
        std::fs::read_to_string(server.home.jobs_dir().join("p-job/properties.json")).unwrap();
    assert!(meta.contains("proj-p"), "{meta}");

    // Re-running `up` replaces the project's own job and leaves the other one alone.
    p.ok(&["compose", "up"]);
    assert_eq!(server.job_names(), vec!["p-job", "q-job"]);

    // A project may not take over a job another project owns.
    let r = Project::new("proj-r", server.port, "  p-job: show version");
    let refused = r.run(&["compose", "up"]);
    assert!(!refused.status.success());
    assert!(
        text(&refused).contains("Refusing to replace"),
        "{}",
        text(&refused)
    );

    // `down` removes only the project's jobs.
    p.ok(&["compose", "down"]);
    assert_eq!(server.job_names(), vec!["q-job"]);
    let r_down = r.run(&["compose", "down"]);
    assert!(r_down.status.success(), "{}", text(&r_down));
    assert_eq!(server.job_names(), vec!["q-job"]);
    q.ok(&["compose", "down"]);
    assert!(server.job_names().is_empty());
}

#[test]
fn compose_fails_when_the_shared_executor_is_unreachable() {
    let dead = free_port();
    let project = Project::new("proj-dead", dead, "  d-job: show version");
    let up = project.run(&["compose", "up"]);
    assert!(!up.status.success(), "{}", text(&up));
    let down = project.run(&["compose", "down"]);
    // `down` must not report success on the strength of an empty read.
    assert!(!down.status.success(), "{}", text(&down));
    assert!(
        text(&down).contains("Could not read the jobs of this project's executor")
            || text(&down).contains("executor"),
        "{}",
        text(&down)
    );
}

/// One raw HTTP/1.1 request (the path is sent as is, so `%2E%2E` stays encoded); the status line.
fn raw_request(port: u16, method: &str, path: &str) -> String {
    try_raw_request(port, method, path).unwrap()
}

fn try_raw_request(port: u16, method: &str, path: &str) -> std::io::Result<String> {
    use std::io::{Read, Write};
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(20)))?;
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    )?;
    let mut out = String::new();
    let _ = stream.read_to_string(&mut out);
    Ok(out.lines().next().unwrap_or_default().to_string())
}

#[test]
fn path_like_job_names_never_touch_the_servers_root() {
    let server = Server::start(true);
    let root = server.home.root.path().to_path_buf();
    std::fs::write(root.join("sentinel"), "keep").unwrap();
    std::fs::create_dir_all(root.join("jobs")).unwrap();
    // A job which exists must survive too.
    let client = Home::new();
    let created = client.create_job(
        &[("shared", server.port)],
        "api:shared",
        &server.home.script(),
        "alpha",
    );
    assert!(created.status.success(), "{}", text(&created));

    for name in [
        "%2E%2E",
        "..",
        "%2E",
        "%2E%2E%2F%2E%2E",
        "x%2F%2E%2E%2F%2E%2E",
        "%2Fetc",
        "%5C..",
    ] {
        for (method, path) in [
            ("DELETE", format!("/jobs/{name}")),
            ("POST", format!("/jobs/{name}/stop")),
            ("POST", format!("/jobs/{name}/start")),
            ("GET", format!("/jobs/{name}")),
        ] {
            let status = raw_request(server.port, method, &path);
            assert!(!status.is_empty(), "{method} {path}: no response");
            assert!(
                root.join("sentinel").exists() && root.join("jobs").exists(),
                "{method} {path} ({status}) removed part of the root"
            );
        }
    }
    assert!(root.join("config").exists() || root.join("sentinel").exists());
    assert_eq!(
        server.job_names(),
        vec!["alpha"],
        "the real job is untouched"
    );

    // The same names on the command line.
    for name in ["..", ".", "a/b", "../x"] {
        for args in [
            vec!["delete", "jobs", name, "-f"],
            vec!["start", "jobs", "--name", name, "--noask", "-f"],
            vec!["stop", "jobs", name, "-f"],
        ] {
            let out = client.run(&[("shared", server.port)], &args);
            let t = text(&out);
            assert!(
                !t.contains("Traceback") && !t.contains("panicked"),
                "{args:?}: {t}"
            );
            assert!(client.root.path().exists());
        }
    }
    let local_out = client.run(&[], &["start", "jobs", "--name", "../x", "--noask", "-f"]);
    assert!(!local_out.status.success(), "{}", text(&local_out));
    assert!(
        text(&local_out).contains("Invalid name"),
        "{}",
        text(&local_out)
    );
    assert!(client.root.path().exists());
}

/// `POST /jobs/{name}`: the job's environment is the server's; of what the client sends in
/// `properties.env` only the Compose project stamp is kept (v4 replaces the client's `env`
/// entirely and so loses the stamp).
#[test]
fn a_created_jobs_environment_is_the_servers_plus_the_compose_stamp() {
    use std::io::{Read, Write};
    let server = Server::start(true);
    let script = server.home.script();
    let body = serde_json::json!({
        "sysargs": ["sh", script.to_string_lossy()],
        "properties": {"env": {
            "LD_PRELOAD": "/tmp/evil.so",
            "MRSM_TEST_CLIENT_ONLY": "1",
            "MRSM__COMPOSE_PROJECT": "stamped-project",
        }},
    })
    .to_string();
    let mut stream = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    write!(
        stream,
        "POST /jobs/enved HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut response = String::new();
    let _ = stream.read_to_string(&mut response);
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    let props: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(server.home.jobs_dir().join("enved/properties.json")).unwrap(),
    )
    .unwrap();
    let env = props["env"].as_object().unwrap();
    assert_eq!(env["MRSM__COMPOSE_PROJECT"], "stamped-project");
    assert_eq!(env["MRSM_NONINTERACTIVE"], "1");
    assert!(!env.contains_key("LD_PRELOAD"), "{env:?}");
    assert!(!env.contains_key("MRSM_TEST_CLIENT_ONLY"), "{env:?}");
    assert_eq!(
        env["MRSM_ROOT_DIR"],
        server.home.root.path().to_str().unwrap()
    );
}

/// `mrsm api <label> <action...>` against a loopback server, with a deadline: a client which
/// never returns (the websocket never ends, or the result never arrives) is killed and fails
/// the test.
fn run_with_deadline(client: &Home, endpoints: &[(&str, u16)], args: &[&str]) -> Output {
    let mut child = client
        .command(endpoints)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let out = child.wait_with_output().unwrap();
            panic!("{args:?} did not return within 60 s:\n{}", text(&out));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn api_action_streams_the_remote_output_and_returns_the_remote_result() {
    let server = Server::start(true);
    let client = Home::new();
    let endpoints = [("srv", server.port)];

    // Output of a job which finishes quickly (before the log monitor connects).
    let out = run_with_deadline(
        &client,
        &endpoints,
        &["api", "srv", "show", "version", "--nopretty"],
    );
    assert!(out.status.success(), "{}", text(&out));
    assert!(
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .any(|l| l == env!("CARGO_PKG_VERSION")),
        "{}",
        text(&out)
    );

    // Several lines arrive in order, without the log timestamps.
    let script = server.home.root.path().join("lines.sh");
    std::fs::write(&script, "#!/bin/sh\necho first\necho second\necho third\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let script = script.to_string_lossy().to_string();
    let out = run_with_deadline(&client, &endpoints, &["api", "srv", "sh", &script]);
    assert!(out.status.success(), "{}", text(&out));
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let order: Vec<usize> = ["first", "second", "third"]
        .iter()
        .map(|w| {
            stdout
                .find(w)
                .unwrap_or_else(|| panic!("{w} missing: {stdout}"))
        })
        .collect();
    assert!(order.windows(2).all(|w| w[0] < w[1]), "{stdout}");
    assert!(
        !stdout.contains(" | "),
        "timestamps were not stripped: {stdout}"
    );

    // The remote job's failure is this command's failure, and the output is still shown.
    let failing = server.home.root.path().join("fails.sh");
    std::fs::write(&failing, "#!/bin/sh\necho before-the-failure\nexit 3\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&failing, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let out = run_with_deadline(
        &client,
        &endpoints,
        &["api", "srv", "sh", &failing.to_string_lossy()],
    );
    assert!(!out.status.success(), "{}", text(&out));
    assert!(text(&out).contains("before-the-failure"), "{}", text(&out));

    // A silent job (no output) still ends with its result, and nothing is left on the server.
    let silent = server.home.root.path().join("silent.sh");
    std::fs::write(&silent, "#!/bin/sh\nexit 0\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&silent, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let out = run_with_deadline(
        &client,
        &endpoints,
        &["api", "srv", "sh", &silent.to_string_lossy()],
    );
    assert!(out.status.success(), "{}", text(&out));
    assert!(
        server.job_names().is_empty()
            && std::fs::read_dir(server.home.jobs_dir())
                .map(|d| d.flatten().count() == 0)
                .unwrap_or(true),
        "temporary jobs were left on the server"
    );
}
