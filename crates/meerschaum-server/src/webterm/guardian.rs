//! Validated native lifetime guardian for API-only terminals.
//!
//! Replaces repeated linear scans of `ShellCommand::env` and repeated `Spec::read` /
//! executable canonicalization checks across launch, readiness polling, new-window
//! preparation, and pre-open cancellation with one typed value parsed once at the seam.

use std::io;
use std::path::{Path, PathBuf};

use meerschaum_core::team_scope::{api_execution, sandbox};
use meerschaum_jobs::supervisor::{
    LifecycleOwner, cancel_unlaunched_terminal, prepare_terminal_launch,
};

use super::ShellCommand;

const GUARD_ARG: &str = "__mrsm_terminal_guard__";
const PENDING_ENV: &str = "MRSM_TERMINAL_LAUNCH";

/// A validated native guardian context for an API-only terminal shell.
#[derive(Debug, Clone)]
pub(super) struct NativeGuardian {
    spec: PathBuf,
    owner: LifecycleOwner,
    pending: Option<PathBuf>,
}

impl NativeGuardian {
    /// Validate `shell`'s guardian metadata if it requests the API execution profile.
    ///
    /// Returns `Ok(None)` for ordinary (non-API) shells, `Ok(Some(guardian))` when every
    /// native confinement invariant holds, and `Err` when an API-profile shell is missing
    /// or has drifted from its mandatory native guardian context.
    pub(super) fn parse(shell: &ShellCommand) -> io::Result<Option<Self>> {
        if !shell.is_api_execution() {
            return Ok(None);
        }
        Self::validate_api_shell(shell).map(Some)
    }

    /// Require `shell` to carry a valid native guardian context (used by host API terminals).
    pub(super) fn require(shell: &ShellCommand) -> io::Result<Self> {
        if !shell.clear_env {
            return Err(io::Error::other(
                "The host API terminal has no approved private runtime.",
            ));
        }
        if !shell.is_api_execution()
            || shell.env_value(sandbox::runtime::LIFECYCLE_VAR) != Some("terminal")
        {
            return Err(io::Error::other(
                "The host API terminal has no native confinement guardian.",
            ));
        }
        Self::validate_api_shell(shell)
    }

    fn validate_api_shell(shell: &ShellCommand) -> io::Result<Self> {
        let refused = || io::Error::other("The API window lost its exact native guardian context.");
        if !shell.clear_env
            || shell.env_value(sandbox::runtime::LIFECYCLE_VAR) != Some("terminal")
            || shell.args.first().map(String::as_str) != Some(GUARD_ARG)
            || shell.args.get(3).map(String::as_str) != Some("--")
        {
            return Err(refused());
        }
        let owner: LifecycleOwner = serde_json::from_str(
            shell
                .args
                .get(1)
                .filter(|value| value.len() <= 1024)
                .ok_or_else(refused)?,
        )
        .map_err(|_| refused())?;
        if LifecycleOwner::current()? != owner || !owner.alive() {
            return Err(refused());
        }
        let spec = shell.env_value(sandbox::SPEC_VAR).ok_or_else(refused)?;
        if shell.args.get(2).map(String::as_str) != Some(spec) {
            return Err(refused());
        }
        let native = meerschaum_core::self_exe::native_executable().ok_or_else(refused)?;
        let selected = sandbox::Spec::read(Path::new(spec))?;
        if selected.mode != sandbox::Mode::RequireSockets
            || std::fs::canonicalize(&shell.program).ok().as_ref() != Some(&native)
            || selected
                .runtime
                .as_ref()
                .is_none_or(|runtime| runtime.launcher != native)
        {
            return Err(refused());
        }
        let pending = shell.env_value(PENDING_ENV).map(PathBuf::from);
        Ok(Self {
            spec: PathBuf::from(spec),
            owner,
            pending,
        })
    }

    /// Path to the validated sandbox specification file.
    pub(super) fn spec(&self) -> &Path {
        &self.spec
    }

    /// Mint a fresh one-use pending launch proof for a new tmux window in this sealed context.
    pub(super) fn prepare_window_shell(&self, shell: &ShellCommand) -> io::Result<ShellCommand> {
        let pending = prepare_terminal_launch(&self.spec, &self.owner)?;
        let mut result = shell.clone();
        result.env.retain(|(key, _)| key != PENDING_ENV);
        result
            .env
            .push((PENDING_ENV.into(), pending.to_string_lossy().into_owned()));
        Ok(result)
    }

    /// Retire an unspawned pending launch proof before any argv construction or launcher handoff.
    pub(super) fn cancel_unspawned(&self) -> io::Result<()> {
        let pending = self.pending.as_deref().ok_or_else(|| {
            io::Error::other("The never-launched terminal command has no pending proof.")
        })?;
        cancel_unlaunched_terminal(&self.spec, pending, &self.owner)
    }
}

/// Retire a host-prepared API shell's unspawned pending proof if it carries the API profile.
pub(super) fn cancel_unspawned_shell(shell: &ShellCommand) -> io::Result<()> {
    if let Some(guardian) = NativeGuardian::parse(shell)? {
        guardian.cancel_unspawned()?;
    }
    Ok(())
}

/// Environment key identifying the API execution profile (`MRSM_API_EXECUTION`).
pub(super) const API_PROFILE_VAR: &str = api_execution::PROFILE_VAR;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_shells_keep_their_command_and_markers_cannot_replace_a_native_context() {
        let mut legacy = ShellCommand::new("/bin/sh");
        legacy.args = vec!["-l".into()];
        legacy.env = vec![("LANG".into(), "C.UTF-8".into())];
        assert!(NativeGuardian::parse(&legacy).unwrap().is_none());
        assert!(NativeGuardian::require(&legacy).is_err());
        cancel_unspawned_shell(&legacy).unwrap();

        legacy.clear_env = true;
        legacy.env.push((API_PROFILE_VAR.into(), "1".into()));
        assert!(NativeGuardian::parse(&legacy).is_err());
        assert!(NativeGuardian::require(&legacy).is_err());
        assert!(cancel_unspawned_shell(&legacy).is_err());
    }
}
