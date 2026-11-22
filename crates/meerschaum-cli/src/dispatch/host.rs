//! The hosts that start processes and read input for the dispatcher: the real terminal
//! ([`SystemHost`]) and the recording host tests use ([`RecordingHost`]).

use super::{CommandOutput, CommandSpec, Host};
use std::io::Write;

/// The real host.
#[derive(Debug, Default)]
pub struct SystemHost;

/// A restricted terminal's process starts no other program ([`crate::shell::restricted`]).
fn check_restricted(spec: &CommandSpec) -> std::io::Result<()> {
    if crate::shell::restricted::may_start(&spec.program) {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            crate::shell::restricted::NO_PROGRAMS,
        ))
    }
}

impl Host for SystemHost {
    fn run(&mut self, spec: &CommandSpec) -> std::io::Result<i32> {
        check_restricted(spec)?;
        let mut cmd = std::process::Command::new(&spec.program);
        cmd.args(&spec.args);
        if spec.env_clear {
            cmd.env_clear();
        }
        cmd.envs(spec.env.iter().map(|(k, v)| (k, v)));
        if spec.scrub_secrets {
            meerschaum_core::child_env::scrub_command(&mut cmd);
        }
        if let Some(cwd) = &spec.cwd {
            cmd.current_dir(cwd);
        }
        if spec.quiet {
            cmd.stdout(std::process::Stdio::null());
            cmd.stderr(std::process::Stdio::null());
        }
        // The program shares the terminal's foreground process group, so Ctrl-C reaches it
        // directly; this process waits (see `shell::interrupt`). A second Ctrl-C within two
        // seconds kills it, unless it is an interactive session.
        let mut child = cmd.spawn()?;
        let interactive = spec.args.is_empty()
            || spec
                .args
                .iter()
                .any(|a| a == "-i" || a == "ptpython" || a == "--interactive");
        let scope = crate::shell::interrupt::ChildScope::enter(Some(child.id()), !interactive);
        let status = child.wait();
        drop(scope);
        Ok(status?.code().unwrap_or(1))
    }

    fn read_line(&mut self, prompt: &str) -> Option<String> {
        if std::io::IsTerminal::is_terminal(&std::io::stdin()) {
            let answer = crate::native::terminal::read_prefilled(prompt, "");
            if crate::native::terminal::take_interrupt() {
                std::panic::resume_unwind(Box::new(crate::shell::interrupt::Interrupted));
            }
            return answer;
        }
        print!("{prompt}");
        let _ = std::io::stdout().flush();
        let mut line = String::new();
        match std::io::stdin().read_line(&mut line) {
            Ok(0) | Err(_) => None,
            Ok(_) => Some(line.trim_end_matches(['\n', '\r']).to_string()),
        }
    }

    fn output(
        &mut self,
        spec: &CommandSpec,
        timeout: Option<std::time::Duration>,
    ) -> std::io::Result<CommandOutput> {
        check_restricted(spec)?;
        crate::native::terminal::capture(spec, timeout)
    }

    fn output_stream(
        &mut self,
        spec: &CommandSpec,
        timeout: std::time::Duration,
        maximum_stdout: u64,
        consume: &mut (dyn FnMut(&mut dyn std::io::Read) -> std::io::Result<()> + Send),
    ) -> std::io::Result<i32> {
        check_restricted(spec)?;
        crate::executor::factory::process::stream(spec, timeout, maximum_stdout, consume)
    }

    fn read_line_timeout(&mut self, timeout: std::time::Duration) -> Option<String> {
        crate::native::terminal::timed_input(timeout)
    }

    fn read_multiline(&mut self, prompt: &str) -> Option<String> {
        // PARITY: `prompt_toolkit` cannot submit a multiline prompt from piped input and ends
        // with `EOFError`.
        if !std::io::IsTerminal::is_terminal(&std::io::stdin()) {
            return None;
        }
        crate::native::terminal::read_multiline(prompt)
    }

    fn read_line_interruptible(&mut self, prompt: &str) -> Option<String> {
        if std::io::IsTerminal::is_terminal(&std::io::stdin()) {
            return crate::native::terminal::read_prefilled(prompt, "");
        }
        self.read_line(prompt)
    }

    fn read_secret(&mut self, prompt: &str) -> Option<String> {
        if std::io::IsTerminal::is_terminal(&std::io::stdin()) {
            crate::native::terminal::read_secret(prompt)
        } else {
            self.read_line(prompt)
        }
    }

    fn read_line_suggesting(
        &mut self,
        prompt: &str,
        items: &[String],
        prefill: &str,
    ) -> Option<String> {
        if std::io::IsTerminal::is_terminal(&std::io::stdin())
            && std::io::IsTerminal::is_terminal(&std::io::stdout())
        {
            let ansi = std::env::var_os("NO_COLOR").is_none_or(|v| v.is_empty());
            return crate::native::terminal::read_suggested(prompt, items, prefill, ansi);
        }
        self.read_line_prefilled(prompt, prefill)
    }

    fn read_line_prefilled(&mut self, prompt: &str, prefill: &str) -> Option<String> {
        if prefill.is_empty() || !std::io::IsTerminal::is_terminal(&std::io::stdin()) {
            return self
                .read_line(prompt)
                .map(|line| format!("{prefill}{line}"));
        }
        crate::native::terminal::read_prefilled(prompt, prefill)
    }
}

/// A host that runs nothing: every process "exits" with a fixed code and input is closed.
/// The commands are recorded (for tests).
#[derive(Debug, Default)]
pub struct RecordingHost {
    /// The processes that would have run.
    pub commands: Vec<CommandSpec>,
    /// The exit code every process returns.
    pub exit_code: i32,
    /// Lines to answer prompts with.
    pub input: Vec<String>,
}

impl Host for RecordingHost {
    fn run(&mut self, spec: &CommandSpec) -> std::io::Result<i32> {
        self.commands.push(spec.clone());
        Ok(self.exit_code)
    }

    fn read_line(&mut self, _prompt: &str) -> Option<String> {
        if self.input.is_empty() {
            None
        } else {
            Some(self.input.remove(0))
        }
    }

    fn read_line_timeout(&mut self, _timeout: std::time::Duration) -> Option<String> {
        if self.input.is_empty() {
            None
        } else {
            Some(self.input.remove(0))
        }
    }
}
