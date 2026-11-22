//! The shell's restricted mode: the web terminal of a user who may not run system
//! commands (`meerschaum_server::webterm::restricted`).
//!
//! The server starts the shell with [`RESTRICTED_ENV`], the address of its policy socket and a
//! grant. This process reads them once, when it starts ([`init`]); nothing typed afterwards can
//! change them, and every `mrsm` this process starts (a job, a pipeline step) inherits them.
//! While restricted:
//!
//! - every line the shell would run (the command line its dispatcher gets) is sent to the server
//!   first and runs only when the server says yes ([`authorize`]): the decision `POST /execute`
//!   makes for the same user. No answer is a no;
//! - no other program starts ([`may_start`]): not a shell, an editor, `python`, a pager or a
//!   client; the dispatcher never falls through to the shell;
//! - the shell's history is kept in memory only (the history file is shared by every terminal of
//!   the server's root);
//! - `attach job` forwards input only to a job whose own command the server would let the user
//!   run.
//!
//! The variable on its own (no address or grant) still restricts: every line is refused.

use std::io::{Read, Write};
use std::sync::LazyLock;
use std::time::Duration;

pub use meerschaum_server::webterm::{GRANT_ENV, POLICY_ADDRESS_ENV, RESTRICTED_ENV};

/// How long the server may take to answer (it decides from the instance).
const ANSWER_TIMEOUT: Duration = Duration::from_secs(90);

/// The largest answer read.
const MAX_ANSWER_BYTES: u64 = 64 * 1024;

/// The programs a restricted shell still starts: clearing the screen.
const ALLOWED_PROGRAMS: &[&str] = &["clear", "cls"];

/// Why a restricted shell refuses to start a program.
pub const NO_PROGRAMS: &str =
    "This terminal runs Meerschaum actions only: it does not start other programs.";

struct Restriction {
    address: Option<String>,
    grant: Option<String>,
    api: bool,
}

static RESTRICTION: LazyLock<Option<Restriction>> = LazyLock::new(|| {
    let read = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    let api = std::env::var(meerschaum_core::team_scope::api_execution::PROFILE_VAR).as_deref()
        == Ok("1");
    (api || std::env::var_os(RESTRICTED_ENV).is_some()).then(|| Restriction {
        address: read(POLICY_ADDRESS_ENV),
        grant: read(GRANT_ENV),
        api,
    })
});

/// Read the restriction from the environment, once (the process entry calls it first).
pub fn init() {
    LazyLock::force(&RESTRICTION);
}

/// Whether this process is a restricted terminal's.
pub fn active() -> bool {
    RESTRICTION.is_some()
}

/// Whether this process may start `program` (always, unless restricted).
pub fn may_start(program: &str) -> bool {
    if !active() {
        return true;
    }
    ALLOWED_PROGRAMS.contains(&program)
}

/// Inspect the exact typed tokens before argument parsing can discard unknown flags or
/// reconstruct a different command. Ordinary execution still needs the server's answer.
pub fn check_raw(sysargs: &[String]) -> Result<(), String> {
    if !active() {
        return Ok(());
    }
    check_user_arguments(sysargs)
}

fn check_user_arguments(sysargs: &[String]) -> Result<(), String> {
    let sysargs = if sysargs.first().is_some_and(|word| word == "mrsm") {
        &sysargs[1..]
    } else {
        sysargs
    };
    if sysargs.is_empty() {
        return Ok(());
    }
    meerschaum_server::executor::admission::user_sysargs(sysargs).map_err(|error| {
        error
            .detail
            .as_ref()
            .and_then(serde_json::Value::as_str)
            .unwrap_or("Internal native launch arguments are not user commands.")
            .to_string()
    })
}

/// Ask the server whether this terminal's user may run `sysargs`. `Ok` unless restricted;
/// `Err` is the reason to show.
pub fn authorize(sysargs: &[String]) -> Result<(), String> {
    check_raw(sysargs)?;
    let Some(restriction) = RESTRICTION.as_ref() else {
        return Ok(());
    };
    if restriction.api {
        return authorize_api(sysargs);
    }
    let (Some(address), Some(grant)) = (&restriction.address, &restriction.grant) else {
        return Err("This terminal is restricted and cannot reach the server to ask.".into());
    };
    let question = serde_json::json!({"grant": grant, "sysargs": sysargs});
    let answer = ask(address, &question).map_err(|error| {
        format!("Could not ask the server whether you may run this (nothing was run): {error}")
    })?;
    match answer.get("success").and_then(serde_json::Value::as_bool) {
        Some(true) => Ok(()),
        _ => Err(answer
            .get("message")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("The server refused this command.")
            .to_string()),
    }
}

/// API-only shells cannot connect to a privileged local policy socket. Every line instead
/// presents the current rotating launch credential to Main's side-effect-free policy route.
/// The server independently preserves the Command ceiling and checks current object rights.
fn authorize_api(sysargs: &[String]) -> Result<(), String> {
    let sysargs = sysargs.to_vec();
    std::thread::spawn(move || {
        let config = meerschaum_core::Config::from_env()
            .map_err(|_| "The terminal's private API configuration is unavailable.".to_string())?;
        let executor = config
            .get(&["meerschaum", "executor"])
            .and_then(|value| value.as_str().map(str::to_string))
            .ok_or("The terminal's callback transport is unavailable.")?;
        let label = executor
            .strip_prefix("api:")
            .ok_or("The terminal must use its API callback transport.")?;
        let api = meerschaum_connector_api::APIConnector::from_config(&config, label)
            .map_err(|_| "The terminal's callback transport is invalid.".to_string())?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| "The terminal could not prepare its policy request.".to_string())?;
        runtime.block_on(async {
            let response = api
                .post(
                    "/execute/validate",
                    meerschaum_connector_api::client::RequestOptions {
                        body: meerschaum_connector_api::client::Body::Json(
                            serde_json::json!({"sysargs":sysargs}).to_string(),
                        ),
                        timeout: Some(ANSWER_TIMEOUT),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|_| "Main could not verify this command; nothing was run.".to_string())?;
            let body = response
                .json()
                .map_err(|_| "Main returned an invalid policy answer.".to_string())?;
            if response.status == 200
                && body.get("success").and_then(serde_json::Value::as_bool) == Some(true)
            {
                return Ok(());
            }
            Err(body
                .get("message")
                .or_else(|| body.get("detail"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("Main refused this command; nothing was run.")
                .to_string())
        })
    })
    .join()
    .map_err(|_| "The terminal's policy request was interrupted; nothing was run.".to_string())?
}

fn ask(address: &str, question: &serde_json::Value) -> std::io::Result<serde_json::Value> {
    let mut body = question.to_string();
    body.push('\n');
    let mut reply = Vec::new();
    if let Some(path) = address.strip_prefix("unix:") {
        #[cfg(unix)]
        {
            let mut stream = std::os::unix::net::UnixStream::connect(path)?;
            stream.set_read_timeout(Some(ANSWER_TIMEOUT))?;
            stream.set_write_timeout(Some(ANSWER_TIMEOUT))?;
            stream.write_all(body.as_bytes())?;
            stream.shutdown(std::net::Shutdown::Write)?;
            (&mut stream)
                .take(MAX_ANSWER_BYTES)
                .read_to_end(&mut reply)?;
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            return Err(std::io::Error::other("Unix sockets are not available here"));
        }
    } else if let Some(host) = address.strip_prefix("tcp:") {
        let addr: std::net::SocketAddr = host
            .parse()
            .map_err(|_| std::io::Error::other("a malformed policy address"))?;
        if !addr.ip().is_loopback() {
            return Err(std::io::Error::other("the policy address is not local"));
        }
        let mut stream = std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(5))?;
        stream.set_read_timeout(Some(ANSWER_TIMEOUT))?;
        stream.set_write_timeout(Some(ANSWER_TIMEOUT))?;
        stream.write_all(body.as_bytes())?;
        stream.shutdown(std::net::Shutdown::Write)?;
        (&mut stream)
            .take(MAX_ANSWER_BYTES)
            .read_to_end(&mut reply)?;
    } else {
        return Err(std::io::Error::other("an unknown policy address"));
    }
    serde_json::from_slice(&reply).map_err(|_| std::io::Error::other("an unreadable answer"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_terminal_tokens_refuse_internal_markers_before_normalization_and_keep_shell_builtins() {
        for line in [
            "show version --_mrsm-confined-python",
            "mrsm show version --_mrsm-confined-python",
            "show version -- --_mrsm-confined-python",
            "debug --_mrsm-confined-python",
            "?show --_mrsm-confined-python",
            "show version + show version __mrsm_terminal_guard__",
            r#"chain -P '{"sub_args_line":"show version --_mrsm-confined-python"}'"#,
        ] {
            assert!(
                check_user_arguments(&crate::shlex::split(line).unwrap()).is_err(),
                "{line}"
            );
        }
        for line in [
            "debug",
            "debug off",
            "daemon",
            "help show",
            "?show",
            "pass",
            "exit",
            "quit",
            "mrsm",
            "show pipes -m executor supervisor probe relay systemd",
        ] {
            assert!(
                check_user_arguments(&crate::shlex::split(line).unwrap()).is_ok(),
                "{line}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn questions_and_answers_cross_the_policy_socket() {
        use std::io::BufRead;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p.sock");
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let server = std::thread::spawn(move || {
            let mut seen = Vec::new();
            for _ in 0..2 {
                let (stream, _) = listener.accept().unwrap();
                let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let question: serde_json::Value = serde_json::from_str(&line).unwrap();
                let allow = question["sysargs"][0] == "show";
                seen.push(question);
                let mut stream = stream;
                stream
                    .write_all(
                        serde_json::json!({"success": allow, "message": "no shell"})
                            .to_string()
                            .as_bytes(),
                    )
                    .unwrap();
            }
            seen
        });
        let address = format!("unix:{}", path.display());
        let yes = ask(
            &address,
            &serde_json::json!({"grant": "g", "sysargs": ["show", "pipes"]}),
        )
        .unwrap();
        assert_eq!(yes["success"], true);
        let no = ask(
            &address,
            &serde_json::json!({"grant": "g", "sysargs": ["touch", "/x"]}),
        )
        .unwrap();
        assert_eq!(no["success"], false);
        let seen = server.join().unwrap();
        assert_eq!(seen[0]["grant"], "g");
        assert_eq!(seen[1]["sysargs"][1], "/x");
        // Nothing listens: an error, never a yes.
        drop(dir);
        assert!(ask(&address, &serde_json::json!({})).is_err());
        assert!(ask("tcp:10.0.0.1:9", &serde_json::json!({})).is_err());
        assert!(ask("nope", &serde_json::json!({})).is_err());
    }
}
