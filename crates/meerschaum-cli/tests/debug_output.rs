//! `--debug` prints debug output (dispatch, connector creation, SQL) on stderr in v4's `dprint`
//! format, never on stdout; without `--debug` nothing is printed.

use std::path::Path;
use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_mrsm-cli");

fn run(dir: &Path, args: &[&str]) -> (String, String) {
    let (success, stdout, stderr) = run_any(dir, args);
    assert!(success, "{stderr}");
    (stdout, stderr)
}

fn run_any(dir: &Path, args: &[&str]) -> (bool, String, String) {
    let root = dir.join("root");
    let plugins = dir.join("plugins");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&plugins).unwrap();
    let out = Command::new(BIN)
        .env_clear()
        .args(args)
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", &root)
        .env("MRSM_ROOT_DIR", &root)
        .env("MRSM_PLUGINS_DIR", &plugins)
        .env(
            "MRSM_SQL_DBGTEST",
            format!("sqlite:///{}", dir.join("debug.db").display()),
        )
        // The password must never be printed.
        .env(
            "MRSM_SQL_SECRETTEST",
            "postgresql://user:hunter2@localhost:1/db",
        )
        .env("MRSM_NO_AUTO_INSTALL", "1")
        .env("MRSM_NO_UPDATE_CHECK", "1")
        .current_dir(&root)
        .output()
        .unwrap();
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn debug_prints_dispatch_and_sql_to_stderr() {
    let dir = tempfile::tempdir().unwrap();
    let (stdout, stderr) = run(
        dir.path(),
        &[
            "show",
            "pipes",
            "-i",
            "sql:dbgtest",
            "--debug",
            "--password",
            "hunter2",
            "--params",
            r#"{"password": "hunter2", "fetch": {"uri": "postgresql://h/db?password=hunter2"}}"#,
        ],
    );
    assert!(!stdout.contains("meerschaum_"), "stdout: {stdout}");
    assert!(!stdout.contains("SELECT"), "stdout: {stdout}");
    // The header: icon, `HH:MM:SS.mmm | module:line`.
    let header =
        regex::Regex::new(r" \d\d:\d\d:\d\d\.\d{3} \| meerschaum_cli::dispatch:\d+").unwrap();
    assert!(header.is_match(&stderr), "stderr: {stderr}");
    assert!(stderr.contains("Parsed arguments:"), "stderr: {stderr}");
    assert!(stderr.contains("-> `show pipes`"), "stderr: {stderr}");
    assert!(
        stderr.contains("| meerschaum_connector_sql::instance:"),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("| meerschaum_connector_sql::driver:"),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("Connecting sql:dbgtest"),
        "stderr: {stderr}"
    );
    assert!(stderr.contains("SELECT"), "stderr: {stderr}");
    assert!(!stderr.contains("hunter2"), "stderr: {stderr}");
}

#[test]
fn no_debug_output_without_the_flag() {
    let dir = tempfile::tempdir().unwrap();
    let (stdout, stderr) = run(dir.path(), &["show", "pipes", "-i", "sql:dbgtest"]);
    assert!(!stderr.contains("meerschaum_"), "stderr: {stderr}");
    assert!(!stderr.contains("SELECT"), "stderr: {stderr}");
    assert!(!stdout.contains("meerschaum_"), "stdout: {stdout}");
}

#[test]
fn the_shell_toggle_turns_debug_output_on_and_off() {
    use std::io::Write as _;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    let plugins = dir.path().join("plugins");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&plugins).unwrap();
    let mut child = Command::new(BIN)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", &root)
        .env("MRSM_ROOT_DIR", &root)
        .env("MRSM_PLUGINS_DIR", &plugins)
        .env(
            "MRSM_SQL_DBGTEST",
            format!("sqlite:///{}", dir.path().join("debug.db").display()),
        )
        .env("MRSM_NO_AUTO_INSTALL", "1")
        .env("MRSM_NO_UPDATE_CHECK", "1")
        .current_dir(&root)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(
            b"debug on\nshow pipes -i sql:dbgtest\ndebug off\nshow tags -i sql:dbgtest\nexit\n",
        )
        .unwrap();
    let out = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stdout.contains("Debug mode is on."), "stdout: {stdout}");
    assert!(
        stderr.contains("-> `show pipes`"),
        "stderr: {stderr}\nstdout: {stdout}"
    );
    assert!(!stderr.contains("-> `show tags`"), "stderr: {stderr}");
    assert!(!stdout.contains("meerschaum_"), "stdout: {stdout}");
}

#[test]
fn connector_credentials_are_never_printed() {
    let dir = tempfile::tempdir().unwrap();
    // Port 1 refuses the connection: the command fails after printing what it tried.
    let (_, stdout, stderr) = run_any(
        dir.path(),
        &["show", "pipes", "-i", "sql:secrettest", "--debug"],
    );
    assert!(
        stderr.contains("Connecting sql:secrettest (flavor=postgresql, host=localhost, port=1"),
        "stderr: {stderr}"
    );
    assert!(!stderr.contains("hunter2"), "stderr: {stderr}");
    assert!(!stdout.contains("hunter2"), "stdout: {stdout}");
}

#[test]
fn a_shell_started_with_debug_starts_with_the_toggle_on() {
    use std::io::Write as _;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    let plugins = dir.path().join("plugins");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&plugins).unwrap();
    let mut child = Command::new(BIN)
        .env_clear()
        .arg("--debug")
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", &root)
        .env("MRSM_ROOT_DIR", &root)
        .env("MRSM_PLUGINS_DIR", &plugins)
        .env(
            "MRSM_SQL_DBGTEST",
            format!("sqlite:///{}", dir.path().join("debug.db").display()),
        )
        .env("MRSM_NO_AUTO_INSTALL", "1")
        .env("MRSM_NO_UPDATE_CHECK", "1")
        .current_dir(&root)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"show pipes -i sql:dbgtest\ndebug\nshow tags -i sql:dbgtest\nexit\n")
        .unwrap();
    let out = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("-> `show pipes`"),
        "stderr: {stderr}\nstdout: {stdout}"
    );
    // The bare toggle turns it off (it was on), and off means off.
    assert!(stdout.contains("Debug mode is off."), "stdout: {stdout}");
    assert!(!stderr.contains("-> `show tags`"), "stderr: {stderr}");
}
