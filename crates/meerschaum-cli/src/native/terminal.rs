//! The terminal and process primitives behind the interactive actions: capturing a process's
//! output with a timeout (`subprocess.Popen(..., stdout=PIPE, stderr=PIPE).communicate`),
//! hidden password input and `prompt_toolkit.prompt(default=...)` with an editable prefill.

use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::dispatch::{CommandOutput, CommandSpec};

/// Set when the last interactive read ended with Ctrl-C (`KeyboardInterrupt` in v4) rather than
/// the end of input (`EOFError`).
static INTERRUPTED: AtomicBool = AtomicBool::new(false);

/// Whether the last read that returned `None` was a Ctrl-C (and reset the flag).
pub fn take_interrupt() -> bool {
    INTERRUPTED.swap(false, Ordering::SeqCst)
}

/// Build a [`Command`] from a [`CommandSpec`] (without the stdio configuration).
pub fn command(spec: &CommandSpec) -> Command {
    let mut cmd = Command::new(&spec.program);
    cmd.args(&spec.args);
    if spec.env_clear {
        cmd.env_clear();
    }
    cmd.envs(spec.env.iter().map(|(k, v)| (k, v)));
    if let Some(cwd) = &spec.cwd {
        cmd.current_dir(cwd);
    }
    cmd
}

fn exit_code(status: std::process::ExitStatus) -> i32 {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return -signal;
        }
    }
    status.code().unwrap_or(1)
}

fn drain<R: Read + Send + 'static>(mut stream: R) -> std::thread::JoinHandle<Vec<u8>> {
    std::thread::spawn(move || {
        let mut buffer = Vec::new();
        let _ = stream.read_to_end(&mut buffer);
        buffer
    })
}

/// Run a process, capture stdout and stderr, and kill it when `timeout` elapses
/// (`Popen.communicate(timeout=...)`, then `kill()` and a second `communicate()`).
pub fn capture(spec: &CommandSpec, timeout: Option<Duration>) -> std::io::Result<CommandOutput> {
    let mut cmd = command(spec);
    cmd.stdin(Stdio::inherit())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn()?;
    let stdout = child.stdout.take().map(drain);
    let stderr = child.stderr.take().map(drain);
    let start = Instant::now();
    let mut timed_out = false;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if timeout.is_some_and(|t| start.elapsed() >= t) {
            timed_out = true;
            let _ = child.kill();
            break child.wait()?;
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    Ok(CommandOutput {
        stdout: stdout
            .map(|h| h.join().unwrap_or_default())
            .unwrap_or_default(),
        stderr: stderr
            .map(|h| h.join().unwrap_or_default())
            .unwrap_or_default(),
        code: exit_code(status),
        timed_out,
    })
}

/// Read a line without echoing it. `None` on Ctrl-C / Ctrl-D.
pub fn read_secret(prompt: &str) -> Option<String> {
    use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
    print!("{prompt}");
    let _ = std::io::stdout().flush();
    if enable_raw_mode().is_err() {
        let mut line = String::new();
        return match std::io::stdin().read_line(&mut line) {
            Ok(0) | Err(_) => None,
            Ok(_) => Some(line.trim_end_matches(['\n', '\r']).to_string()),
        };
    }
    let answer = secret_from_events(crossterm::event::read);
    let _ = disable_raw_mode();
    println!();
    answer
}

/// The keys of a hidden prompt until Enter (the secret) or Ctrl-C / Ctrl-D (`None`). A failing
/// event source ends the prompt: it never spins on an error that will not go away.
pub fn secret_from_events(
    mut next: impl FnMut() -> std::io::Result<crossterm::event::Event>,
) -> Option<String> {
    use crossterm::event::{Event, KeyCode, KeyEventKind, KeyModifiers};
    let mut secret = String::new();
    loop {
        let Event::Key(key) = next().ok()? else {
            continue;
        };
        if key.kind == KeyEventKind::Release {
            continue;
        }
        match key.code {
            KeyCode::Enter => return Some(secret),
            KeyCode::Char('c') | KeyCode::Char('d') if key.modifiers == KeyModifiers::CONTROL => {
                INTERRUPTED.store(key.code == KeyCode::Char('c'), Ordering::SeqCst);
                return None;
            }
            KeyCode::Backspace => {
                secret.pop();
            }
            KeyCode::Char(c) => secret.push(c),
            _ => {}
        }
    }
}

/// A prompt with an editable prefill (`prompt_toolkit.prompt(default=prefill)`).
pub fn read_prefilled(prompt: &str, prefill: &str) -> Option<String> {
    use reedline::{EditCommand, Prompt, PromptEditMode, PromptHistorySearch, Reedline, Signal};
    use std::borrow::Cow;

    struct Question(String);
    impl Prompt for Question {
        fn render_prompt_left(&self) -> Cow<'_, str> {
            Cow::Borrowed(&self.0)
        }
        fn render_prompt_right(&self) -> Cow<'_, str> {
            Cow::Borrowed("")
        }
        fn render_prompt_indicator(&self, _mode: PromptEditMode) -> Cow<'_, str> {
            Cow::Borrowed("")
        }
        fn render_prompt_multiline_indicator(&self) -> Cow<'_, str> {
            Cow::Borrowed("")
        }
        fn render_prompt_history_search_indicator(
            &self,
            _history_search: PromptHistorySearch,
        ) -> Cow<'_, str> {
            Cow::Borrowed("")
        }
    }

    let mut editor = Reedline::create();
    editor.run_edit_commands(&[EditCommand::InsertString(prefill.to_string())]);
    match editor.read_line(&Question(prompt.to_string())) {
        Ok(Signal::Success(line)) => Some(line),
        Ok(Signal::CtrlC) => {
            INTERRUPTED.store(true, Ordering::SeqCst);
            None
        }
        _ => None,
    }
}

/// `timed_input(seconds)`: a line from stdin within `timeout` (`None` on timeout). At the end of
/// input v4 prints the prompt and sleeps the rest of the interval (`except EOFError`), so a loop
/// that waits on this keeps its cadence instead of spinning.
///
/// There is no reader thread: stdin is polled with the deadline and read only when it has data,
/// so nothing is consumed after the call returns.
pub fn timed_input(timeout: Duration) -> Option<String> {
    #[cfg(unix)]
    {
        use std::os::fd::AsFd;
        let stdin = std::io::stdin();
        let mut lock = stdin.lock();
        let fd = lock.as_fd().try_clone_to_owned();
        match fd {
            Ok(fd) => timed_line(&mut lock, fd.as_fd(), timeout),
            Err(_) => {
                std::thread::sleep(timeout);
                None
            }
        }
    }
    #[cfg(not(unix))]
    {
        std::thread::sleep(timeout);
        None
    }
}

/// [`timed_input`] over any buffered reader whose file descriptor is `fd`.
#[cfg(unix)]
pub fn timed_line(
    reader: &mut impl std::io::BufRead,
    fd: std::os::fd::BorrowedFd<'_>,
    timeout: Duration,
) -> Option<String> {
    use rustix::event::{PollFd, PollFlags, Timespec, poll};
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        // NOTE: bytes an earlier `read_line` already buffered (several lines of a pipe that is
        // still open) are invisible to `poll`; a terminal delivers one line per read, and a
        // pipe at its end polls as readable.
        let mut fds = [PollFd::new(&fd, PollFlags::IN)];
        let wait = Timespec {
            tv_sec: remaining.as_secs() as _,
            tv_nsec: remaining.subsec_nanos() as _,
        };
        match poll(&mut fds, Some(&wait)) {
            Ok(0) => return None,
            Ok(_) => {}
            Err(rustix::io::Errno::INTR) => continue,
            Err(_) => {
                std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
                return None;
            }
        }
        let mut line = String::new();
        return match reader.read_line(&mut line) {
            Ok(0) | Err(_) => {
                // The end of input: wait out the interval.
                std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
                None
            }
            Ok(_) => Some(line.trim_end_matches(['\n', '\r']).to_string()),
        };
    }
}

/// A multi-line prompt: Enter adds a line, Alt+Enter submits (`[Esc + Enter]` in
/// `prompt_toolkit`). `None` on Ctrl-C / Ctrl-D.
pub fn read_multiline(prompt: &str) -> Option<String> {
    use crossterm::event::{KeyCode, KeyModifiers};
    use reedline::{
        EditCommand, Emacs, Prompt, PromptEditMode, PromptHistorySearch, Reedline, ReedlineEvent,
        Signal, ValidationResult, Validator, default_emacs_keybindings,
    };
    use std::borrow::Cow;

    struct Plain(String);
    impl Prompt for Plain {
        fn render_prompt_left(&self) -> Cow<'_, str> {
            Cow::Borrowed(&self.0)
        }
        fn render_prompt_right(&self) -> Cow<'_, str> {
            Cow::Borrowed("")
        }
        fn render_prompt_indicator(&self, _mode: PromptEditMode) -> Cow<'_, str> {
            Cow::Borrowed("")
        }
        fn render_prompt_multiline_indicator(&self) -> Cow<'_, str> {
            Cow::Borrowed("")
        }
        fn render_prompt_history_search_indicator(
            &self,
            _history_search: PromptHistorySearch,
        ) -> Cow<'_, str> {
            Cow::Borrowed("")
        }
    }
    struct Never;
    impl Validator for Never {
        fn validate(&self, _line: &str) -> ValidationResult {
            ValidationResult::Incomplete
        }
    }

    let mut keybindings = default_emacs_keybindings();
    keybindings.add_binding(KeyModifiers::ALT, KeyCode::Enter, ReedlineEvent::Submit);
    keybindings.add_binding(
        KeyModifiers::NONE,
        KeyCode::Enter,
        ReedlineEvent::Edit(vec![EditCommand::InsertNewline]),
    );
    let mut editor = Reedline::create()
        .with_edit_mode(Box::new(Emacs::new(keybindings)))
        .with_validator(Box::new(Never));
    match editor.read_line(&Plain(prompt.to_string())) {
        Ok(Signal::Success(text)) => Some(text),
        Ok(Signal::CtrlC) => {
            INTERRUPTED.store(true, Ordering::SeqCst);
            None
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};

    fn key(code: KeyCode, modifiers: KeyModifiers) -> std::io::Result<Event> {
        Ok(Event::Key(KeyEvent::new(code, modifiers)))
    }

    #[test]
    fn secrets_end_at_enter_and_honor_backspace() {
        let mut events = vec![
            key(KeyCode::Char('a'), KeyModifiers::NONE),
            key(KeyCode::Char('x'), KeyModifiers::NONE),
            key(KeyCode::Backspace, KeyModifiers::NONE),
            Ok(Event::Resize(80, 24)),
            key(KeyCode::Char('b'), KeyModifiers::NONE),
            key(KeyCode::Enter, KeyModifiers::NONE),
        ]
        .into_iter();
        assert_eq!(
            secret_from_events(|| events.next().unwrap()),
            Some("ab".to_string())
        );
    }

    #[test]
    fn a_failing_event_source_ends_the_secret_prompt() {
        let mut calls = 0;
        let answer = secret_from_events(|| {
            calls += 1;
            assert!(calls < 5, "the prompt kept reading a failing source");
            Err(std::io::Error::other("closed"))
        });
        assert_eq!(answer, None);
        assert_eq!(calls, 1);
    }

    #[test]
    fn ctrl_c_is_an_interrupt_and_ctrl_d_is_not() {
        take_interrupt();
        let mut c = vec![key(KeyCode::Char('c'), KeyModifiers::CONTROL)].into_iter();
        assert_eq!(secret_from_events(|| c.next().unwrap()), None);
        assert!(take_interrupt());
        let mut d = vec![key(KeyCode::Char('d'), KeyModifiers::CONTROL)].into_iter();
        assert_eq!(secret_from_events(|| d.next().unwrap()), None);
        assert!(!take_interrupt());
    }

    #[cfg(unix)]
    mod timed {
        use super::super::*;
        use std::io::{BufReader, Write};
        use std::os::fd::AsFd;

        #[test]
        fn a_line_is_returned_as_soon_as_it_arrives() {
            let (reader, mut writer) = std::io::pipe().unwrap();
            writer.write_all(b"hello\n").unwrap();
            let mut buffered = BufReader::new(&reader);
            let started = Instant::now();
            let line = timed_line(&mut buffered, reader.as_fd(), Duration::from_secs(5));
            assert_eq!(line.as_deref(), Some("hello"));
            assert!(started.elapsed() < Duration::from_secs(2));
        }

        #[test]
        fn silence_times_out_and_leaves_later_input_unread() {
            let (reader, mut writer) = std::io::pipe().unwrap();
            let mut buffered = BufReader::new(&reader);
            let started = Instant::now();
            assert_eq!(
                timed_line(&mut buffered, reader.as_fd(), Duration::from_millis(250)),
                None
            );
            assert!(started.elapsed() >= Duration::from_millis(240));
            // Nothing is reading in the background: the next read sees the late line.
            writer.write_all(b"late\n").unwrap();
            let mut line = String::new();
            std::io::BufRead::read_line(&mut buffered, &mut line).unwrap();
            assert_eq!(line, "late\n");
        }

        #[test]
        fn the_end_of_input_waits_out_the_interval() {
            let (reader, writer) = std::io::pipe().unwrap();
            drop(writer);
            let mut buffered = BufReader::new(&reader);
            let started = Instant::now();
            assert_eq!(
                timed_line(&mut buffered, reader.as_fd(), Duration::from_millis(300)),
                None
            );
            assert!(
                started.elapsed() >= Duration::from_millis(290),
                "returned after {:?}",
                started.elapsed()
            );
        }
    }
}
