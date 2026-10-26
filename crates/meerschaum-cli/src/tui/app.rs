//! The loop every full-screen view shares: the alternate screen and raw mode (restored on
//! return, on a panic and on Ctrl-C), resizes, a title bar and a footer, `q` / Esc to quit,
//! `?` for the key help, and a yes/no confirmation box for destructive keys.
//!
//! A view implements [`View`]: it pulls new data in [`View::tick`] (cheaply: background
//! threads or tasks feed it through channels, the loop never blocks on I/O), draws itself in
//! [`View::draw`] and answers keys in [`View::on_key`]. [`draw_frame`] renders one frame without a
//! terminal, which the unit tests use with ratatui's `TestBackend`.

use std::io::{self, Stdout, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, Once};
use std::thread::ThreadId;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crossterm::{cursor, execute, terminal};
use ratatui::Frame;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

use super::theme::{Theme, Tone, truncate};

/// What a key did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// Handled; keep going.
    Continue,
    /// Not handled: the loop applies the shared keys (`q` / Esc quit).
    Ignored,
    /// Leave the view.
    Quit,
    /// Leave the view, back to the caller's inline output.
    Detach,
    /// Ask before doing something: [`View::on_confirm`] gets `id` on a yes.
    Confirm {
        /// Passed back to [`View::on_confirm`].
        id: u32,
        /// The question.
        prompt: String,
    },
}

/// How a view ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    /// `q`, Esc or Ctrl-C.
    Quit,
    /// The view's detach key.
    Detach,
}

/// One full-screen view.
pub trait View {
    /// The title bar text.
    fn title(&self) -> String;
    /// The key help (`?`): `(keys, what they do)`.
    fn help(&self) -> Vec<(&'static str, &'static str)>;
    /// The footer's short key hints.
    fn hints(&self) -> String;
    /// A transient status message for the footer (the result of the last command).
    fn status(&self) -> Option<(String, Tone)> {
        None
    }
    /// Pull new data; `true` when the screen must be redrawn.
    fn tick(&mut self) -> bool;
    /// Draw the body.
    fn draw(&mut self, frame: &mut Frame<'_>, area: Rect, theme: &Theme);
    /// Answer a key.
    fn on_key(&mut self, key: KeyEvent) -> Step;
    /// The user confirmed the question of [`Step::Confirm`] `id`.
    fn on_confirm(&mut self, _id: u32) {}
}

/// A box drawn over the view.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Overlay {
    /// None.
    #[default]
    None,
    /// The key help.
    Help,
    /// A yes/no question.
    Confirm {
        /// For [`View::on_confirm`].
        id: u32,
        /// The question.
        prompt: String,
    },
}

/// Draw one frame: the title bar, the view, the footer and the overlay.
pub fn draw_frame(view: &mut dyn View, overlay: &Overlay, theme: &Theme, frame: &mut Frame<'_>) {
    let area = frame.area();
    let [top, body, bottom] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(0),
        Constraint::Length(1),
    ])
    .areas(area);
    let width = top.width as usize;
    let right = " ? help  q quit ";
    let title = truncate(
        &format!(" Meerschaum | {}", view.title()),
        width.saturating_sub(right.len()),
        theme.ellipsis(),
    );
    let pad =
        width.saturating_sub(unicode_width::UnicodeWidthStr::width(title.as_str()) + right.len());
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(title, theme.style(Tone::Accent)),
            Span::raw(" ".repeat(pad)),
            Span::styled(right, theme.style(Tone::Dim)),
        ])),
        top,
    );
    view.draw(frame, body, theme);
    let footer = match view.status() {
        Some((text, tone)) => Span::styled(
            truncate(&format!(" {text}"), width, theme.ellipsis()),
            theme.style(tone),
        ),
        None => Span::styled(
            truncate(&format!(" {}", view.hints()), width, theme.ellipsis()),
            theme.style(Tone::Dim),
        ),
    };
    frame.render_widget(Paragraph::new(Line::from(footer)), bottom);
    match overlay {
        Overlay::None => {}
        Overlay::Help => {
            let mut rows: Vec<(&str, &str)> = view.help();
            rows.push(("?", "show / hide this help"));
            rows.push(("q, Esc, Ctrl-C", "quit"));
            let key_w = rows
                .iter()
                .map(|(k, _)| k.chars().count())
                .max()
                .unwrap_or(0);
            let lines: Vec<Line> = rows
                .iter()
                .map(|(k, what)| {
                    Line::from(vec![
                        Span::styled(format!(" {k:<key_w$}  "), theme.style(Tone::Accent)),
                        Span::raw(what.to_string()),
                    ])
                })
                .collect();
            let text_w = rows
                .iter()
                .map(|(k, w)| key_w.max(k.chars().count()) + w.chars().count() + 5)
                .max()
                .unwrap_or(20) as u16;
            let rect = centered(area, text_w + 2, lines.len() as u16 + 2);
            frame.render_widget(Clear, rect);
            frame.render_widget(
                Paragraph::new(lines).block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_set(theme.border())
                        .title(" Keys "),
                ),
                rect,
            );
        }
        Overlay::Confirm { prompt, .. } => {
            let text = format!("{prompt} [y/N]");
            let w = (text.chars().count() as u16 + 4).min(area.width);
            let rect = centered(area, w, 3);
            frame.render_widget(Clear, rect);
            frame.render_widget(
                Paragraph::new(Line::from(Span::styled(text, theme.style(Tone::Warn))))
                    .wrap(Wrap { trim: true })
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .border_set(theme.border())
                            .title(" Confirm "),
                    ),
                rect,
            );
        }
    }
}

/// A `w` x `h` rectangle in the middle of `area` (clipped to it).
pub fn centered(area: Rect, w: u16, h: u16) -> Rect {
    let w = w.min(area.width);
    let h = h.min(area.height);
    Rect {
        x: area.x + (area.width - w) / 2,
        y: area.y + (area.height - h) / 2,
        width: w,
        height: h,
    }
}

/// The loop's answer to one key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyOutcome {
    /// Keep going.
    Continue,
    /// Leave with this exit.
    Exit(Exit),
}

/// Apply one key press to the overlay and the view.
pub fn handle_key(view: &mut dyn View, overlay: &mut Overlay, key: KeyEvent) -> KeyOutcome {
    let ctrl_c =
        key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c'));
    if ctrl_c {
        return KeyOutcome::Exit(Exit::Quit);
    }
    match std::mem::take(overlay) {
        Overlay::Help => return KeyOutcome::Continue,
        Overlay::Confirm { id, .. } => {
            if matches!(key.code, KeyCode::Char('y') | KeyCode::Char('Y')) {
                view.on_confirm(id);
            }
            return KeyOutcome::Continue;
        }
        Overlay::None => {}
    }
    if key.code == KeyCode::Char('?') {
        *overlay = Overlay::Help;
        return KeyOutcome::Continue;
    }
    match view.on_key(key) {
        Step::Continue => KeyOutcome::Continue,
        Step::Quit => KeyOutcome::Exit(Exit::Quit),
        Step::Detach => KeyOutcome::Exit(Exit::Detach),
        Step::Confirm { id, prompt } => {
            *overlay = Overlay::Confirm { id, prompt };
            KeyOutcome::Continue
        }
        Step::Ignored => match key.code {
            KeyCode::Char('q') | KeyCode::Char('Q') | KeyCode::Esc => KeyOutcome::Exit(Exit::Quit),
            _ => KeyOutcome::Continue,
        },
    }
}

/// Whether the terminal is in the view's mode (raw, alternate screen).
static ACTIVE: AtomicBool = AtomicBool::new(false);
static PANIC_HOOK: Once = Once::new();
/// The thread that runs the view's loop: only its panic ends the view.
static OWNER: Mutex<Option<ThreadId>> = Mutex::new(None);

/// Whether a panic on the current thread should restore the terminal: the view's own thread
/// does; a background thread of the view (a log follower, a listing) does not, since the loop
/// keeps drawing. Those threads run their work through [`guarded`], which hands the panic's
/// message to the view instead.
fn owns_terminal() -> bool {
    let owner = *OWNER.lock().unwrap_or_else(|e| e.into_inner());
    owner.is_none_or(|id| id == std::thread::current().id())
}

/// Run a view's background work, turning a panic into an error message (the panic hook prints
/// nothing over the view for a thread other than the view's own).
pub fn guarded<T>(work: impl FnOnce() -> T) -> Result<T, String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(work)).map_err(|payload| {
        payload
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "a background task panicked".to_string())
    })
}

/// Held by the view's loop while it writes a frame, so that [`restore_for_exit`] on another
/// thread never interleaves its escape sequences with half a frame, and no frame is drawn over
/// the main screen afterwards.
static DRAW: Mutex<()> = Mutex::new(());

/// The process is about to end (a signal's default action, a forced exit after Ctrl-C, SIGHUP):
/// give the terminal back if a full-screen view holds it. Safe from any thread and idempotent;
/// every exit path that bypasses the view's guard calls it (`shell::interrupt`, the SIGHUP
/// watcher below).
///
/// From another thread it waits (briefly) for the frame being drawn, restores, and keeps the
/// view from drawing again: the process does not go on.
pub fn restore_for_exit() {
    if !ACTIVE.load(Ordering::SeqCst) {
        return;
    }
    let owner = *OWNER.lock().unwrap_or_else(|e| e.into_inner());
    if owner.is_none_or(|id| id == std::thread::current().id()) {
        // The view's own thread (a panic mid-frame may hold `DRAW` already).
        restore_terminal();
        return;
    }
    let deadline = Instant::now() + Duration::from_millis(500);
    let held = loop {
        match DRAW.try_lock() {
            Ok(guard) => break Some(guard),
            Err(std::sync::TryLockError::Poisoned(e)) => break Some(e.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(std::sync::TryLockError::WouldBlock) => break None,
        }
    };
    restore_terminal();
    // Never released: the view's next frame would land on the main screen.
    std::mem::forget(held);
}

/// SIGHUP (the terminal closed, or `kill -HUP`) while a view is up: restore, then end the
/// process as the default action does. Watched from the first view on (an unregistered
/// `signal-hook` handler would leave the signal ignored, so it stays); not in an embedding host,
/// which owns the signals.
#[cfg(unix)]
fn watch_sighup() {
    static START: Once = Once::new();
    if crate::native::jobs::is_embedded() {
        return;
    }
    START.call_once(|| {
        use signal_hook::consts::SIGHUP;
        let Ok(mut signals) = signal_hook::iterator::Signals::new([SIGHUP]) else {
            return;
        };
        let _ = std::thread::Builder::new()
            .name("mrsm-tui-hup".into())
            .spawn(move || {
                for signal in signals.forever() {
                    crate::format::progress::clear_for_signal();
                    restore_for_exit();
                    let _ = signal_hook::low_level::emulate_default_handler(signal);
                }
            });
    });
}

/// Leave raw mode and the alternate screen if a view entered them (idempotent).
pub fn restore_terminal() {
    if ACTIVE.swap(false, Ordering::SeqCst) {
        *OWNER.lock().unwrap_or_else(|e| e.into_inner()) = None;
        let mut out = io::stdout();
        let _ = execute!(out, terminal::LeaveAlternateScreen, cursor::Show);
        let _ = terminal::disable_raw_mode();
        let _ = out.flush();
        crate::debug_log::hold_output(false);
    }
}

/// Raw mode and the alternate screen for as long as it lives.
struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TerminalGuard {
    fn enter() -> io::Result<TerminalGuard> {
        PANIC_HOOK.call_once(|| {
            let previous = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                // Print the panic on the main screen, in cooked mode.
                if owns_terminal() {
                    restore_terminal();
                } else if ACTIVE.load(Ordering::SeqCst) {
                    // A view's background thread: `guarded` reports it inside the view.
                    return;
                }
                previous(info);
            }));
        });
        #[cfg(unix)]
        watch_sighup();
        crate::debug_log::hold_output(true);
        if let Err(error) = terminal::enable_raw_mode() {
            crate::debug_log::hold_output(false);
            return Err(error);
        }
        ACTIVE.store(true, Ordering::SeqCst);
        *OWNER.lock().unwrap_or_else(|e| e.into_inner()) = Some(std::thread::current().id());
        let mut out = io::stdout();
        if let Err(error) = execute!(out, terminal::EnterAlternateScreen, cursor::Hide) {
            restore_terminal();
            return Err(error);
        }
        match Terminal::new(CrosstermBackend::new(io::stdout())) {
            Ok(mut terminal) => {
                let _ = terminal.clear();
                Ok(TerminalGuard { terminal })
            }
            Err(error) => {
                restore_terminal();
                Err(error)
            }
        }
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        restore_terminal();
    }
}

/// How often the screen is redrawn without input (elapsed times, running bars).
const REDRAW_EVERY: Duration = Duration::from_millis(500);
/// How long the loop waits for input before pulling new data.
const POLL: Duration = Duration::from_millis(100);

/// Run `view` full-screen until the user leaves it.
pub fn run(view: &mut dyn View, theme: &Theme) -> io::Result<Exit> {
    let mut guard = TerminalGuard::enter()?;
    let mut overlay = Overlay::None;
    let mut dirty = true;
    let mut last_draw = Instant::now();
    // A stop request (the first SIGTERM / SIGINT of an action that reads the flag itself, such as
    // a sync whose run this view follows) made while the view is up ends the view, as Ctrl-C does.
    let stop = crate::native::jobs::stop_requested_flag();
    let stopped_before = stop.load(Ordering::SeqCst);
    loop {
        // A SIGINT from outside the terminal (raw mode turns the terminal's Ctrl-C into a key).
        if crate::shell::interrupt::requested() || (!stopped_before && stop.load(Ordering::SeqCst))
        {
            return Ok(Exit::Quit);
        }
        if view.tick() {
            dirty = true;
        }
        if dirty || last_draw.elapsed() >= REDRAW_EVERY {
            let _drawing = DRAW.lock().unwrap_or_else(|e| e.into_inner());
            guard
                .terminal
                .draw(|frame| draw_frame(view, &overlay, theme, frame))?;
            dirty = false;
            last_draw = Instant::now();
        }
        if !event::poll(POLL)? {
            continue;
        }
        match event::read()? {
            // Windows reports releases too.
            Event::Key(key) if key.kind != KeyEventKind::Release => {
                dirty = true;
                if let KeyOutcome::Exit(exit) = handle_key(view, &mut overlay, key) {
                    return Ok(exit);
                }
            }
            Event::Resize(..) => {
                let _drawing = DRAW.lock().unwrap_or_else(|e| e.into_inner());
                guard.terminal.autoresize()?;
                dirty = true;
            }
            _ => {}
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use ratatui::backend::TestBackend;

    /// The text of a rendered buffer, one string per row.
    pub fn screen(
        view: &mut dyn View,
        overlay: &Overlay,
        theme: &Theme,
        w: u16,
        h: u16,
    ) -> Vec<String> {
        let mut terminal = Terminal::new(TestBackend::new(w, h)).unwrap();
        terminal
            .draw(|frame| draw_frame(view, overlay, theme, frame))
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        (0..h)
            .map(|y| {
                (0..w)
                    .map(|x| buffer[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect()
    }

    pub fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    #[derive(Default)]
    struct Dummy {
        confirmed: Vec<u32>,
    }

    impl View for Dummy {
        fn title(&self) -> String {
            "dummy".into()
        }
        fn help(&self) -> Vec<(&'static str, &'static str)> {
            vec![("x", "ask")]
        }
        fn hints(&self) -> String {
            "x ask".into()
        }
        fn tick(&mut self) -> bool {
            false
        }
        fn draw(&mut self, frame: &mut Frame<'_>, area: Rect, _theme: &Theme) {
            frame.render_widget(Paragraph::new("body"), area);
        }
        fn on_key(&mut self, key: KeyEvent) -> Step {
            match key.code {
                KeyCode::Char('x') => Step::Confirm {
                    id: 7,
                    prompt: "Really?".into(),
                },
                KeyCode::Char('d') => Step::Detach,
                _ => Step::Ignored,
            }
        }
        fn on_confirm(&mut self, id: u32) {
            self.confirmed.push(id);
        }
    }

    #[test]
    fn frame_has_title_body_footer_and_overlays() {
        let mut view = Dummy::default();
        let theme = Theme::plain();
        let rows = screen(&mut view, &Overlay::None, &theme, 40, 6);
        assert!(rows[0].starts_with(" Meerschaum | dummy"), "{rows:?}");
        assert!(rows[0].trim_end().ends_with("? help  q quit"), "{rows:?}");
        assert!(rows[1].starts_with("body"), "{rows:?}");
        assert!(rows[5].starts_with(" x ask"), "{rows:?}");
        let rows = screen(&mut view, &Overlay::Help, &theme, 40, 8);
        assert!(rows.iter().any(|r| r.contains("Keys")), "{rows:?}");
        assert!(
            rows.iter()
                .any(|r| r.starts_with("| x ") && r.contains(" ask")),
            "{rows:?}"
        );
        let rows = screen(
            &mut view,
            &Overlay::Confirm {
                id: 1,
                prompt: "Stop?".into(),
            },
            &theme,
            40,
            8,
        );
        assert!(rows.iter().any(|r| r.contains("Stop? [y/N]")), "{rows:?}");
        // Tiny terminals do not panic.
        let _ = screen(&mut view, &Overlay::Help, &theme, 3, 2);
        let _ = screen(&mut view, &Overlay::None, &theme, 1, 1);
    }

    #[test]
    fn keys_drive_overlays_and_exits() {
        let mut view = Dummy::default();
        let mut overlay = Overlay::None;
        assert_eq!(
            handle_key(&mut view, &mut overlay, key(KeyCode::Char('?'))),
            KeyOutcome::Continue
        );
        assert_eq!(overlay, Overlay::Help);
        // Any key closes the help, `q` included (it does not quit).
        assert_eq!(
            handle_key(&mut view, &mut overlay, key(KeyCode::Char('q'))),
            KeyOutcome::Continue
        );
        assert_eq!(overlay, Overlay::None);
        // A confirmation: `n` cancels, `y` confirms.
        handle_key(&mut view, &mut overlay, key(KeyCode::Char('x')));
        assert!(matches!(overlay, Overlay::Confirm { id: 7, .. }));
        handle_key(&mut view, &mut overlay, key(KeyCode::Char('n')));
        assert!(view.confirmed.is_empty());
        handle_key(&mut view, &mut overlay, key(KeyCode::Char('x')));
        handle_key(&mut view, &mut overlay, key(KeyCode::Char('y')));
        assert_eq!(view.confirmed, vec![7]);
        assert_eq!(
            handle_key(&mut view, &mut overlay, key(KeyCode::Char('d'))),
            KeyOutcome::Exit(Exit::Detach)
        );
        assert_eq!(
            handle_key(&mut view, &mut overlay, key(KeyCode::Esc)),
            KeyOutcome::Exit(Exit::Quit)
        );
        let ctrl_c = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
        overlay = Overlay::Help;
        assert_eq!(
            handle_key(&mut view, &mut overlay, ctrl_c),
            KeyOutcome::Exit(Exit::Quit)
        );
    }
}
