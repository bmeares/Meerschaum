//! Each native API pane gets its own one-use pending launch inside the same sealed context.

use super::ShellCommand;
use std::io;
use std::path::Path;

pub(super) fn window_shell(shell: &ShellCommand) -> io::Result<ShellCommand> {
    let Some((spec, owner)) = native_context(shell)? else {
        return Ok(shell.clone());
    };
    let pending = meerschaum_jobs::supervisor::prepare_terminal_launch(&spec, &owner)?;
    let mut result = shell.clone();
    result.env.retain(|(key, _)| key != "MRSM_TERMINAL_LAUNCH");
    result.env.push((
        "MRSM_TERMINAL_LAUNCH".into(),
        pending.to_string_lossy().into_owned(),
    ));
    Ok(result)
}
/// Only called after launch proved an existing known tmux session and before handing
/// this newly prepared shell to tmux, argv construction or any other process launcher.
pub(super) fn cancel_unspawned(shell: &ShellCommand) -> io::Result<()> {
    let Some((spec, owner)) = native_context(shell)? else {
        return Ok(());
    };
    let pending = shell
        .env
        .iter()
        .rev()
        .find(|(key, _)| key == "MRSM_TERMINAL_LAUNCH")
        .map(|(_, value)| Path::new(value))
        .ok_or_else(|| {
            io::Error::other("The never-launched terminal command has no pending proof.")
        })?;
    meerschaum_jobs::supervisor::cancel_unlaunched_terminal(&spec, pending, &owner)
}
fn native_context(
    shell: &ShellCommand,
) -> io::Result<
    Option<(
        std::path::PathBuf,
        meerschaum_jobs::supervisor::LifecycleOwner,
    )>,
> {
    let environment = |key: &str| {
        shell
            .env
            .iter()
            .rev()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    };
    if environment(meerschaum_core::team_scope::api_execution::PROFILE_VAR) != Some("1") {
        return Ok(None);
    }
    let refused = || io::Error::other("The API window lost its exact native guardian context.");
    if !shell.clear_env
        || environment(meerschaum_core::team_scope::sandbox::runtime::LIFECYCLE_VAR)
            != Some("terminal")
        || shell.args.first().map(String::as_str) != Some("__mrsm_terminal_guard__")
        || shell.args.get(3).map(String::as_str) != Some("--")
    {
        return Err(refused());
    }
    let owner: meerschaum_jobs::supervisor::LifecycleOwner = serde_json::from_str(
        shell
            .args
            .get(1)
            .filter(|value| value.len() <= 1024)
            .ok_or_else(refused)?,
    )
    .map_err(|_| refused())?;
    if meerschaum_jobs::supervisor::LifecycleOwner::current()? != owner || !owner.alive() {
        return Err(refused());
    }
    let spec = environment(meerschaum_core::team_scope::sandbox::SPEC_VAR).ok_or_else(refused)?;
    if shell.args.get(2).map(String::as_str) != Some(spec) {
        return Err(refused());
    }
    let native = meerschaum_core::self_exe::native_executable().ok_or_else(refused)?;
    let selected = meerschaum_core::team_scope::sandbox::Spec::read(Path::new(spec))?;
    if selected.mode != meerschaum_core::team_scope::sandbox::Mode::RequireSockets
        || std::fs::canonicalize(&shell.program).ok().as_ref() != Some(&native)
        || selected
            .runtime
            .as_ref()
            .is_none_or(|runtime| runtime.launcher != native)
    {
        return Err(refused());
    }
    Ok(Some((Path::new(spec).to_path_buf(), owner)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ordinary_shells_keep_their_command_and_markers_cannot_replace_a_native_context() {
        let mut legacy = ShellCommand::new("/bin/sh");
        legacy.args = vec!["-l".into()];
        legacy.env = vec![("LANG".into(), "C.UTF-8".into())];
        let same = window_shell(&legacy).unwrap();
        cancel_unspawned(&legacy).unwrap();
        assert_eq!(same.program, legacy.program);
        assert_eq!(same.args, legacy.args);
        assert_eq!(same.env, legacy.env);
        legacy.clear_env = true;
        legacy.env.push((
            meerschaum_core::team_scope::api_execution::PROFILE_VAR.into(),
            "1".into(),
        ));
        assert!(window_shell(&legacy).is_err());
        assert!(cancel_unspawned(&legacy).is_err());
    }
}
