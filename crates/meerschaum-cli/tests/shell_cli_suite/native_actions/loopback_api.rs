//! Actions against a loopback `mrsm start api` on a random port: `login`, `api`, `start gui`,
//! `start connectors` and remote jobs.

use super::*;

pub(super) fn free_port() -> u16 {
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

/// The status line of `GET /healthcheck` on `port`, or the I/O error that ended the request.
fn healthcheck(port: u16) -> std::io::Result<String> {
    use std::io::Read;
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(20)))?;
    stream.write_all(
        b"GET /healthcheck HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    )?;
    let mut out = String::new();
    let _ = stream.read_to_string(&mut out);
    Ok(out.lines().next().unwrap_or_default().to_string())
}

/// A server started with `<args>` over a temporary root, stopped when dropped.
struct Server {
    home: Home,
    child: Child,
    port: u16,
}

impl Server {
    /// A started server which answers HTTP. The server cannot be asked to bind port 0 and report
    /// the port it got, so a port is chosen first; under load a port which was free a moment ago
    /// can be taken by another test binary before this server binds it (then the server exits, or
    /// a foreign listener answers for it until its own test ends). A failed attempt is therefore
    /// retried on a new port.
    fn start(prefix: &[&str]) -> Server {
        let mut last = String::new();
        for _ in 0..5 {
            match Server::try_start(prefix) {
                Ok(server) => return server,
                Err(why) => last = why,
            }
        }
        panic!("the server did not start in 5 attempts: {last}");
    }

    fn try_start(prefix: &[&str]) -> Result<Server, String> {
        let home = Home::new();
        let port = free_port();
        let mut command = home.command();
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        command
            .args(["--noask"])
            .args(prefix)
            .args(["-i", "sql:local", "--host", "127.0.0.1", "-p"])
            .arg(port.to_string())
            .args(["--no-dash", "-w", "2"]);
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        // Dropped (and the process group stopped) on every early return.
        let mut server = Server { home, child, port };
        // Ready = a whole HTTP answer, not merely an accepted connection: under load a listener
        // can accept before the app serves, and the early connection is reset.
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = server.child.try_wait().unwrap() {
                return Err(format!("the server exited ({status}) before it served"));
            }
            if healthcheck(port).is_ok_and(|status| {
                status.starts_with("HTTP/1.1 200") || status.starts_with("HTTP/1.0 200")
            }) {
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
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = Command::new("kill")
            .args(["-TERM", &format!("-{}", self.child.id())])
            .stderr(Stdio::null())
            .status();
        let deadline = Instant::now() + Duration::from_secs(3);
        while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn config_file(root: &Path) -> PathBuf {
    root.join("config").join("meerschaum.yaml")
}

#[test]
fn login_prompts_for_credentials_and_saves_them() {
    let server = Server::start(&["start", "api"]);
    // The first user of an instance is first offered the admin type (`n`: a regular user).
    let registered = server.home.ask(
        &["register", "users", "alice"],
        "n\npassword1\npassword1\n\n",
    );
    assert_eq!(registered.code, 0, "{}", registered.text);

    let client = Home::new().with_endpoint("srv", server.port);
    // Wrong password: asked whether to try again; declining fails.
    let wrong = client.run_in(&["login", "api:srv"], "alice\nnotthepassword\nn\n");
    no_worker(&wrong);
    assert_eq!(wrong.code, 1, "{}", wrong.text);
    assert!(
        wrong.text.contains("Logged into 0 connectors."),
        "{}",
        wrong.text
    );
    assert!(
        wrong
            .text
            .contains("Would you like to try different login credentials for connector 'api:srv'?"),
        "{}",
        wrong.text
    );

    // Right password: logs in and offers to save the credentials.
    let right = client.run_in(&["login", "api:srv"], "alice\npassword1\ny\n");
    assert_eq!(right.code, 0, "{}", right.text);
    assert!(
        right.text.contains("Username for connector 'api:srv':"),
        "{}",
        right.text
    );
    assert!(
        right.text.contains("Logged into 1 connector."),
        "{}",
        right.text
    );
    assert!(
        right
            .text
            .contains("Would you like to save the new credentials for connector 'api:srv'?"),
        "{}",
        right.text
    );
    let saved = std::fs::read_to_string(config_file(client.path())).unwrap_or_default();
    assert!(saved.contains("alice"), "{saved}\n{}", client.config_text());

    // Not an API connector.
    let other = client.run_in(&["login", "sql:local"], "");
    assert_eq!(other.code, 1);
    assert!(
        other
            .text
            .contains("Connector 'sql:local' is not an API connector. Skipping...")
    );
}

#[test]
fn api_start_is_an_alias_of_start_api() {
    let server = Server::start(&["api", "start", "--no-auth"]);
    let mut stream = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
    stream
        .write_all(b"GET /healthcheck HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n")
        .unwrap();
    let mut response = String::new();
    std::io::Read::read_to_string(&mut stream, &mut response).unwrap();
    assert!(
        response.starts_with("HTTP/1.0 200") || response.starts_with("HTTP/1.1 200"),
        "{response}"
    );
}

#[test]
fn start_gui_serves_on_loopback_and_stops_the_server_when_the_window_closes() {
    // A stand-in interpreter: `import webview` succeeds; the window script records its URL,
    // checks that the server answers on that port, and exits.
    let home = Home::new();
    let fake = home.path().join("fake-python");
    let record = home.path().join("window.txt");
    std::fs::write(
        &fake,
        format!(
            "#!/bin/bash\nif [ \"$2\" = \"import webview\" ]; then exit 0; fi\n\
             url=\"$3\"; port=\"${{url##*:}}\"; port=\"${{port%%/*}}\"\n\
             if (exec 3<>/dev/tcp/127.0.0.1/$port) 2>/dev/null; then up=listening; else up=closed; fi\n\
             echo \"$url $up\" > {}\nexit 0\n",
            record.display()
        ),
    )
    .unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let mut command = home.command();
    command.env("MRSM_PYTHON", &fake);
    let out = home.run_with(command, &["-i", "sql:local", "start", "gui"], "");
    assert_eq!(out.code, 0, "{}", out.text);
    let seen = std::fs::read_to_string(&record).expect("the window ran");
    let (url, state) = seen.trim().split_once(' ').unwrap();
    assert!(
        url.starts_with("http://127.0.0.1:") && url.ends_with("/dash/"),
        "{seen}"
    );
    assert_eq!(state, "listening", "{seen}");
    // The server is gone once the window is closed.
    let port: u16 = url
        .trim_start_matches("http://127.0.0.1:")
        .trim_end_matches("/dash/")
        .parse()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(("127.0.0.1", port)).is_ok() {
        assert!(
            Instant::now() < deadline,
            "the GUI's server is still running"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn start_connectors_retries_a_connection_that_comes_up_late() {
    let port = free_port();
    let listener = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(1800));
        let socket = TcpListener::bind(("127.0.0.1", port)).unwrap();
        // Answer every request with an empty JSON body until the test ends.
        let deadline = Instant::now() + Duration::from_secs(20);
        socket.set_nonblocking(true).unwrap();
        while Instant::now() < deadline {
            match socket.accept() {
                Ok((mut stream, _)) => {
                    let mut buffer = [0u8; 2048];
                    let _ = std::io::Read::read(&mut stream, &mut buffer);
                    let _ = stream.write_all(
                        b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}",
                    );
                }
                Err(_) => std::thread::sleep(Duration::from_millis(20)),
            }
            if DONE.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
        }
    });
    let home = Home::new().with_endpoint("late", port);
    let out = home.run_in(
        &[
            "start",
            "connectors",
            "api:late",
            "--min-seconds",
            "1",
            "--noask",
        ],
        "",
    );
    DONE.store(true, std::sync::atomic::Ordering::SeqCst);
    let _ = listener.join();
    assert_eq!(out.code, 0, "{}", out.text);
    assert!(
        out.text.contains("Connection to 'api:late' failed."),
        "{}",
        out.text
    );
    assert!(
        out.text.contains("Failed connection attempt (1 / 50)"),
        "{}",
        out.text
    );
    assert!(
        out.text.contains("Connected to 'api:late'."),
        "{}",
        out.text
    );
    assert!(
        out.text
            .contains("Successfully started connector 'api:late'."),
        "{}",
        out.text
    );
}

static DONE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[test]
fn start_connectors_gives_up_after_the_retries_on_a_port_that_never_opens() {
    let port = free_port();
    let home = Home::new().with_env(
        "MRSM_SQL_DEAD",
        format!("postgresql://user:pass@127.0.0.1:{port}/db"),
    );
    let out = home.run_in(
        &[
            "start",
            "connectors",
            "sql:dead",
            "--min-seconds",
            "0",
            "--noask",
        ],
        "",
    );
    no_worker(&out);
    assert_eq!(out.code, 1, "{}", out.text);
    // The keys are valid: the failure is the connection, retried every time.
    assert!(!out.text.contains("Invalid connector keys"), "{}", out.text);
    assert!(
        out.text.contains("Failed connection attempt (1 / 50)"),
        "{}",
        out.text
    );
    assert!(
        out.text.contains("Failed connection attempt (50 / 50)"),
        "{}",
        out.text
    );
    assert!(
        out.text.contains("Failed to start 1 connectors."),
        "{}",
        out.text
    );
    assert!(
        out.text.contains("Failed to start connector 'sql:dead'."),
        "{}",
        out.text
    );
}

#[test]
fn api_prints_the_remote_result_once_like_v4() {
    // v4's job daemon runs `entry()` and stores the tuple: the job's log has the action's own
    // output only, and the client prints the result tuple once.
    let server = Server::start(&["start", "api", "--no-auth"]);
    let client = Home::new().with_endpoint("srv", server.port);
    let failed = client.run_in(&["api", "srv", "sync", "pipes", "-m", "nonexistent"], "");
    assert_eq!(failed.code, 1, "{}", failed.text);
    assert_eq!(
        failed.text.matches("(0 succeeded, 0 failed)").count(),
        1,
        "{}",
        failed.text
    );
    let ok = client.run_in(&["api", "srv", "os", "echo", "hi"], "");
    assert_eq!(ok.code, 0, "{}", ok.text);
    assert_eq!(ok.text.matches("hi").count(), 2, "{}", ok.text);
    assert_eq!(ok.text.matches('\u{1f389}').count(), 1, "{}", ok.text);
}

#[test]
fn a_remote_job_without_an_action_fails_instead_of_opening_a_shell() {
    // `api srv` with nothing to run sends sysargs that end up as `mrsm -y`.
    let server = Server::start(&["start", "api", "--no-auth"]);
    let client = Home::new().with_endpoint("srv", server.port);
    let started = Instant::now();
    let out = client.run_in(&["api", "srv"], "");
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "the job is waiting on a shell"
    );
    assert_eq!(out.code, 1, "{}", out.text);
    assert!(out.text.contains("no action to run"), "{}", out.text);
    // Nothing is left running on the server's root.
    let jobs = server.home.run(&["show", "jobs", "--nopretty"]);
    assert!(!jobs.text.contains(".api-temp"), "{}", jobs.text);
}
