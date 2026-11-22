//! The process-launching actions `os`, `sql` and `stack` (`meerschaum/actions/{os,sql,stack}.py`).
//!
//! - `os <command ...>` runs a subprocess and returns its captured output.
//! - `sql` reads / executes queries on a `sql:` connector, or opens the flavor's interactive
//!   client (Rainfrog first, configurable per connector/flavor, then the Python and plain
//!   clients; `--nopretty` forces the plain client; nothing is installed implicitly).
//! - `stack <docker compose args>` writes the Docker Compose stack project from the `stack`
//!   config and runs `docker compose` against it.

use std::time::Duration;

use meerschaum_core::{SuccessTuple, pyfmt};

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::{CommandSpec, Runtime};
use crate::format::rich::pretty::PyObj;
use crate::value::ArgsMap;

use super::{
    check_noask, connectors, extensions, info, pip, show, stack_readonly, stack_secrets,
    upgrade_postgres, venvs,
};
use super::{session, words};

mod docker_stack;
mod grafana;
mod sql_client;

pub(crate) use docker_stack::stack_env_dict;
pub use docker_stack::{stack, write_stack};
pub(crate) use grafana::set_environment;
pub use grafana::{
    GRAFANA_PROXY_NETWORK, GrafanaSignIn, grafana_proxy_addresses, grafana_sign_in_environment,
};
pub use sql_client::{open_cli, sql};

/// The exception text Python raises when `Popen` cannot start `program`.
fn spawn_error(program: &str, error: &std::io::Error) -> ActionError {
    let quoted = pyfmt::str_repr(program);
    match error.kind() {
        std::io::ErrorKind::NotFound => ActionError::new(
            "FileNotFoundError",
            format!("[Errno 2] No such file or directory: {quoted}"),
        ),
        std::io::ErrorKind::PermissionDenied => ActionError::new(
            "PermissionError",
            format!("[Errno 13] Permission denied: {quoted}"),
        ),
        _ => ActionError::new("OSError", error.to_string()),
    }
}

/// `os <command ...>`: run the command and print its output (`shell=False`).
pub fn os(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    if action.is_empty() {
        return Ok(SuccessTuple::fail(
            crate::action_docs::docstring("os").unwrap_or_default(),
        ));
    }
    let mut command = action;
    command.extend(words(args, "sub_args"));
    let timeout = rt
        .config()
        .get(&["shell", "timeout"])
        .and_then(|v| pyfmt::py_int(&v).map(|n| (n, v.py_str())));
    // The user's own command: it keeps the whole environment, like `sh`.
    let spec = CommandSpec {
        scrub_secrets: false,
        program: command[0].clone(),
        args: command[1..].to_vec(),
        ..CommandSpec::default()
    };
    let out = rt
        .host
        .output(
            &spec,
            timeout
                .as_ref()
                .map(|(n, _)| Duration::from_secs((*n).max(0) as u64)),
        )
        .map_err(|e| spawn_error(&command[0], &e))?;
    if out.timed_out {
        let shown = PyObj::List(command.iter().map(|c| PyObj::Str(c.clone())).collect()).repr();
        let seconds = timeout.map(|(_, s)| s).unwrap_or_default();
        rt.print(&format!(
            "Command '{shown}' timed out after {seconds} seconds\n"
        ));
    }
    let output = String::from_utf8_lossy(&out.stdout).into_owned();
    let error_output = String::from_utf8_lossy(&out.stderr).into_owned();
    rt.print(&output);
    if out.code != 0 {
        return Ok(SuccessTuple::fail(error_output));
    }
    Ok(SuccessTuple::ok(output))
}

#[cfg(test)]
mod grafana_tests;

#[cfg(test)]
mod compose_writer_tests;
