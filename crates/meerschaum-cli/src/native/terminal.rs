//! The terminal and process primitives behind the interactive actions: capturing a process's
//! output with a timeout (`subprocess.Popen(..., stdout=PIPE, stderr=PIPE).communicate`),
//! hidden password input and `prompt_toolkit.prompt(default=...)` with an editable prefill.

use std::io::{Read, Write};
use std::process::{Command, Stdio};
#[cfg(not(test))]
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::dispatch::{CommandOutput, CommandSpec};

/// Set when the last interactive read ended with Ctrl-C (`KeyboardInterrupt` in v4) rather than
/// the end of input (`EOFError`).
#[cfg(not(test))]
static INTERRUPTED: AtomicBool = AtomicBool::new(false);

// The unit tests run in parallel threads of one process, each with its own prompt: a flag of
// their own per thread keeps one test's Ctrl-C from being taken by another.
#[cfg(test)]
thread_local! {
    static INTERRUPTED_HERE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn set_interrupted(value: bool) {
    #[cfg(not(test))]
    INTERRUPTED.store(value, Ordering::SeqCst);
    #[cfg(test)]
    INTERRUPTED_HERE.with(|flag| flag.set(value));
}

/// Whether the last read that returned `None` was a Ctrl-C (and reset the flag).
pub fn take_interrupt() -> bool {
    #[cfg(not(test))]
    {
        INTERRUPTED.swap(false, Ordering::SeqCst)
    }
    #[cfg(test)]
    {
        INTERRUPTED_HERE.with(|flag| flag.replace(false))
    }
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
    // Ctrl-C reaches the program through the terminal; this process waits for it.
    let _waiting = crate::shell::interrupt::ChildScope::enter(Some(child.id()), true);
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

/// The character a secret prompt echoes for each character typed.
pub const MASK: char = '*';

/// The terminal in raw mode until dropped: it is restored on every way out of a prompt,
/// including a panic or an early return.
pub struct RawMode;

impl RawMode {
    /// Put the terminal in raw mode (an error when stdin is not a terminal).
    pub fn enable() -> std::io::Result<RawMode> {
        crossterm::terminal::enable_raw_mode()?;
        Ok(RawMode)
    }
}

impl Drop for RawMode {
    fn drop(&mut self) {
        let _ = crossterm::terminal::disable_raw_mode();
    }
}

/// Whether a `Char` key with these modifiers is text. Plain and Shift-ed characters are; Ctrl or
/// Alt alone are shortcuts. On Windows the AltGr key reaches the program as Ctrl+Alt (crossterm
/// reports `RIGHT_ALT | LEFT_CTRL` as `CONTROL | ALT`), and it types `@ \ { } [ ] ~ | €` on
/// German, Spanish and Nordic layouts: with a printable character that is text too.
pub fn is_text_modifiers(modifiers: crossterm::event::KeyModifiers) -> bool {
    is_text_modifiers_on(modifiers, cfg!(windows))
}

/// [`is_text_modifiers`] for either platform's AltGr convention.
pub fn is_text_modifiers_on(modifiers: crossterm::event::KeyModifiers, altgr: bool) -> bool {
    use crossterm::event::KeyModifiers as M;
    let m = modifiers - M::SHIFT;
    m.is_empty() || (altgr && m == (M::CONTROL | M::ALT))
}

/// What erases one echoed [`MASK`] from the terminal (back, overwrite, back).
const ERASE_ONE: &str = "\u{8} \u{8}";

/// Read a secret, echoing [`MASK`] for each character (Backspace erases one, Ctrl-U all of
/// them). `None` on Ctrl-C / Ctrl-D. With stdin not a terminal nothing is echoed at all: the
/// caller reads a plain line then (`Host::read_secret`).
pub fn read_secret(prompt: &str) -> Option<String> {
    print!("{prompt}");
    let _ = std::io::stdout().flush();
    let Ok(raw) = RawMode::enable() else {
        let mut line = String::new();
        return match std::io::stdin().read_line(&mut line) {
            Ok(0) | Err(_) => None,
            Ok(_) => Some(line.trim_end_matches(['\n', '\r']).to_string()),
        };
    };
    let answer = masked_from_events(crossterm::event::read, |echo| {
        let mut out = std::io::stdout();
        let _ = out.write_all(echo.as_bytes());
        let _ = out.flush();
    });
    drop(raw);
    println!();
    answer
}

/// The keys of a hidden prompt until Enter (the secret) or Ctrl-C / Ctrl-D (`None`), echoing
/// nothing. A failing event source ends the prompt: it never spins on an error that will not go
/// away.
pub fn secret_from_events(
    next: impl FnMut() -> std::io::Result<crossterm::event::Event>,
) -> Option<String> {
    masked_from_events(next, |_| {})
}

/// [`secret_from_events`] that tells `echo` what to write to the terminal: one [`MASK`] for each
/// character typed or pasted, the erasing sequence for Backspace (only when there is a character
/// to erase) and for Ctrl-U (one per character).
pub fn masked_from_events(
    next: impl FnMut() -> std::io::Result<crossterm::event::Event>,
    echo: impl FnMut(&str),
) -> Option<String> {
    masked_from_events_on(next, echo, cfg!(windows))
}

/// [`masked_from_events`] where `altgr_is_ctrl_alt` says whether Ctrl+Alt is the AltGr key
/// (Windows), whose characters are typed text.
pub fn masked_from_events_on(
    mut next: impl FnMut() -> std::io::Result<crossterm::event::Event>,
    mut echo: impl FnMut(&str),
    altgr_is_ctrl_alt: bool,
) -> Option<String> {
    use crossterm::event::{Event, KeyCode, KeyEventKind, KeyModifiers};
    let mut secret = String::new();
    loop {
        let event = next().ok()?;
        if let Event::Paste(text) = &event {
            for c in text.chars().filter(|c| !c.is_control()) {
                secret.push(c);
                echo(&MASK.to_string());
            }
            continue;
        }
        let Event::Key(key) = event else {
            continue;
        };
        if key.kind == KeyEventKind::Release {
            continue;
        }
        match key.code {
            KeyCode::Enter => return Some(secret),
            KeyCode::Char('c') | KeyCode::Char('d') if key.modifiers == KeyModifiers::CONTROL => {
                set_interrupted(key.code == KeyCode::Char('c'));
                return None;
            }
            KeyCode::Char('u') if key.modifiers == KeyModifiers::CONTROL => {
                for _ in secret.chars() {
                    echo(ERASE_ONE);
                }
                secret.clear();
            }
            KeyCode::Backspace => {
                if secret.pop().is_some() {
                    echo(ERASE_ONE);
                }
            }
            KeyCode::Char(c) if is_text_modifiers_on(key.modifiers, altgr_is_ctrl_alt) => {
                secret.push(c);
                echo(&MASK.to_string());
            }
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
            set_interrupted(true);
            None
        }
        _ => None,
    }
}

/// A prompt with a filtering dropdown of `items` (see [`crate::shell::suggest`]): `None` on Ctrl-C
/// (recorded for [`take_interrupt`]) or the end of input. Falls back to a plain prefilled prompt
/// when the terminal cannot be put in raw mode.
pub fn read_suggested(prompt: &str, items: &[String], prefill: &str, ansi: bool) -> Option<String> {
    use crate::shell::suggest::{Answer, read};
    match read(prompt, items, prefill, ansi) {
        Some(Answer::Line(line)) => Some(line),
        Some(Answer::Interrupted) => {
            set_interrupted(true);
            None
        }
        Some(Answer::Eof) => None,
        None => read_prefilled(prompt, prefill),
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
            set_interrupted(true);
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
    fn altgr_characters_are_typed_into_a_secret_on_windows_only() {
        let both = KeyModifiers::CONTROL | KeyModifiers::ALT;
        let events = || {
            vec![
                key(KeyCode::Char('p'), KeyModifiers::NONE),
                key(KeyCode::Char('@'), both),
                key(KeyCode::Char('\\'), both),
                key(KeyCode::Char('W'), KeyModifiers::SHIFT),
                // Plain Alt and plain Ctrl stay shortcuts.
                key(KeyCode::Char('x'), KeyModifiers::ALT),
                key(KeyCode::Char('x'), KeyModifiers::CONTROL),
                key(KeyCode::Enter, KeyModifiers::NONE),
            ]
            .into_iter()
        };
        let mut windows = events();
        let mut echoed = String::new();
        assert_eq!(
            masked_from_events_on(|| windows.next().unwrap(), |e| echoed.push_str(e), true),
            Some("p@\\W".to_string())
        );
        assert_eq!(echoed, "****");
        let mut unix = events();
        assert_eq!(
            masked_from_events_on(|| unix.next().unwrap(), |_| {}, false),
            Some("pW".to_string())
        );
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
    fn a_masked_prompt_echoes_one_star_per_character_and_erases_one_on_backspace() {
        let mut events = vec![
            key(KeyCode::Char('s'), KeyModifiers::NONE),
            key(KeyCode::Char('e'), KeyModifiers::NONE),
            key(KeyCode::Char('x'), KeyModifiers::NONE),
            key(KeyCode::Backspace, KeyModifiers::NONE),
            key(KeyCode::Char('c'), KeyModifiers::NONE),
            key(KeyCode::Char('\u{e9}'), KeyModifiers::SHIFT),
            Ok(Event::Paste("rt\n".into())),
            key(KeyCode::Enter, KeyModifiers::NONE),
        ]
        .into_iter();
        let mut echoed = String::new();
        let secret = masked_from_events(|| events.next().unwrap(), |text| echoed.push_str(text));
        assert_eq!(secret.as_deref(), Some("sec\u{e9}rt"));
        assert_eq!(echoed, "***\u{8} \u{8}****");
        assert!(
            !echoed.contains('s') && !echoed.contains('e'),
            "the secret itself is never echoed"
        );
    }

    #[test]
    fn backspace_on_an_empty_secret_echoes_nothing_and_ctrl_u_erases_all() {
        let mut events = vec![
            key(KeyCode::Backspace, KeyModifiers::NONE),
            key(KeyCode::Char('a'), KeyModifiers::NONE),
            key(KeyCode::Char('b'), KeyModifiers::NONE),
            key(KeyCode::Char('u'), KeyModifiers::CONTROL),
            key(KeyCode::Char('z'), KeyModifiers::NONE),
            key(KeyCode::Enter, KeyModifiers::NONE),
        ]
        .into_iter();
        let mut echoed = String::new();
        let secret = masked_from_events(|| events.next().unwrap(), |text| echoed.push_str(text));
        assert_eq!(secret.as_deref(), Some("z"));
        assert_eq!(echoed, "**\u{8} \u{8}\u{8} \u{8}*");
    }

    #[test]
    fn ctrl_c_cancels_a_masked_prompt_after_echoing_what_was_typed() {
        take_interrupt();
        let mut events = vec![
            key(KeyCode::Char('a'), KeyModifiers::NONE),
            key(KeyCode::Char('c'), KeyModifiers::CONTROL),
        ]
        .into_iter();
        let mut echoed = String::new();
        assert_eq!(
            masked_from_events(|| events.next().unwrap(), |text| echoed.push_str(text)),
            None
        );
        assert_eq!(echoed, "*");
        assert!(take_interrupt());
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
