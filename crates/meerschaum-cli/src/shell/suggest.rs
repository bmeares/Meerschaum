//! A text prompt with a filtering dropdown of suggestions (wave 18, decision 4): what the
//! `bootstrap pipes` wizard asks connector, metric and location keys with.
//!
//! The dropdown lists the suggestions which contain what has been typed (those which *start* with it
//! first, case-insensitively) and narrows as the user types. Nothing is selected until the user
//! moves into the list, so Enter submits exactly what was typed (an unknown value is the caller's
//! to accept or warn about); with a row selected, Enter and Tab / Right take that row.
//!
//! [`Suggest`] is the editing and selection state, driven by key events and drawn by
//! [`Suggest::frame`], both free of any terminal so they are unit tested. [`read`] is the raw-mode
//! loop that connects them to `crossterm`; [`crate::native::terminal::read_suggested`] is its public
//! face (Ctrl-C bookkeeping included).

use std::io::Write;
use unicode_width::UnicodeWidthStr;

use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

/// The rows of the dropdown shown at once (a footer counts the others).
pub const MAX_ROWS: usize = 8;

/// What a key did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Keep reading.
    Continue,
    /// Enter: the answer.
    Submit(String),
    /// Ctrl-C (`KeyboardInterrupt` in v4).
    Cancel,
    /// Ctrl-D at an empty line (`EOFError`).
    Eof,
}

/// The editing and selection state of one prompt.
#[derive(Debug, Clone)]
pub struct Suggest {
    items: Vec<String>,
    buffer: Vec<char>,
    cursor: usize,
    selected: Option<usize>,
}

/// What to draw: the line with the typed text, the dropdown rows below it and where the cursor is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// The text of the input line (after the prompt).
    pub line: String,
    /// The dropdown rows, already styled.
    pub rows: Vec<String>,
    /// The cursor's column within the typed text, in characters.
    pub cursor: usize,
}

impl Suggest {
    /// A prompt over `items` (duplicates and empty strings dropped, order kept) with `prefill` typed.
    pub fn new(items: &[String], prefill: &str) -> Suggest {
        let mut seen = std::collections::HashSet::new();
        let items: Vec<String> = items
            .iter()
            .filter(|i| !i.is_empty() && seen.insert((*i).clone()))
            .cloned()
            .collect();
        let buffer: Vec<char> = prefill.chars().collect();
        Suggest {
            items,
            cursor: buffer.len(),
            buffer,
            selected: None,
        }
    }

    /// What has been typed.
    pub fn text(&self) -> String {
        self.buffer.iter().collect()
    }

    /// The selected row of [`Suggest::matches`], if the user moved into the list.
    pub fn selected(&self) -> Option<usize> {
        self.selected
    }

    /// The suggestions which contain the typed text, ignoring case: those which start with it
    /// first, the rest after, each group in the order given.
    pub fn matches(&self) -> Vec<&str> {
        if self.buffer.is_empty() {
            return Vec::new();
        }
        let needle = self.text().to_lowercase();
        let (mut starts, mut contains) = (Vec::new(), Vec::new());
        for item in &self.items {
            let lower = item.to_lowercase();
            if lower.starts_with(&needle) {
                starts.push(item.as_str());
            } else if lower.contains(&needle) {
                contains.push(item.as_str());
            }
        }
        starts.extend(contains);
        starts
    }

    fn set_text(&mut self, text: &str) {
        self.buffer = text.chars().collect();
        self.cursor = self.buffer.len();
        self.selected = None;
    }

    /// The text a Tab completes to: the whole of a single match, else the longest prefix the matches
    /// share (in the first match's spelling) when that is longer than what was typed.
    fn completion(&self) -> Option<String> {
        let matches = self.matches();
        let typed = self.text();
        match matches.as_slice() {
            [] => None,
            [only] => (*only != typed).then(|| (*only).to_string()),
            [first, rest @ ..] => {
                let mut common: Vec<char> = first.chars().collect();
                for other in rest {
                    let other: Vec<char> = other.chars().collect();
                    let keep = common
                        .iter()
                        .zip(&other)
                        .take_while(|(a, b)| a.to_lowercase().eq(b.to_lowercase()))
                        .count();
                    common.truncate(keep);
                }
                let common: String = common.into_iter().collect();
                (common.chars().count() > typed.chars().count()
                    && common.to_lowercase().starts_with(&typed.to_lowercase()))
                .then_some(common)
            }
        }
    }

    fn move_selection(&mut self, forward: bool) {
        let count = self.matches().len();
        if count == 0 {
            self.selected = None;
            return;
        }
        self.selected = match (self.selected, forward) {
            (None, true) => Some(0),
            (None, false) => Some(count - 1),
            (Some(i), true) if i + 1 < count => Some(i + 1),
            (Some(_), true) => None,
            (Some(0), false) => None,
            (Some(i), false) => Some(i - 1),
        };
    }

    /// Take the selected row into the buffer. Returns whether there was one.
    fn accept_selection(&mut self) -> bool {
        let Some(index) = self.selected else {
            return false;
        };
        let Some(text) = self.matches().get(index).map(|m| (*m).to_string()) else {
            self.selected = None;
            return false;
        };
        self.set_text(&text);
        true
    }

    /// Apply one key.
    pub fn apply(&mut self, key: KeyEvent) -> Outcome {
        self.apply_on(key, cfg!(windows))
    }

    /// [`Suggest::apply`] where `altgr` says whether Ctrl+Alt is the AltGr key (Windows).
    pub fn apply_on(&mut self, key: KeyEvent, altgr_is_ctrl_alt: bool) -> Outcome {
        if key.kind == KeyEventKind::Release {
            return Outcome::Continue;
        }
        // On Windows AltGr arrives as Ctrl+Alt: that is a typed character, not a shortcut.
        let altgr = matches!(key.code, KeyCode::Char(_))
            && key
                .modifiers
                .contains(KeyModifiers::CONTROL | KeyModifiers::ALT)
            && crate::native::terminal::is_text_modifiers_on(key.modifiers, altgr_is_ctrl_alt);
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL) && !altgr;
        match key.code {
            KeyCode::Enter => {
                self.accept_selection();
                return Outcome::Submit(self.text());
            }
            KeyCode::Char('c') if ctrl => return Outcome::Cancel,
            KeyCode::Char('d') if ctrl => {
                if self.buffer.is_empty() {
                    return Outcome::Eof;
                }
                if self.cursor < self.buffer.len() {
                    self.buffer.remove(self.cursor);
                    self.selected = None;
                }
            }
            KeyCode::Char('j') | KeyCode::Char('m') if ctrl => {
                self.accept_selection();
                return Outcome::Submit(self.text());
            }
            KeyCode::Tab => {
                if !self.accept_selection() {
                    if let Some(text) = self.completion() {
                        self.set_text(&text);
                    } else if !self.matches().is_empty() {
                        self.selected = Some(0);
                    }
                }
            }
            KeyCode::BackTab => self.move_selection(false),
            KeyCode::Down => self.move_selection(true),
            KeyCode::Up => self.move_selection(false),
            KeyCode::Char('n') if ctrl => self.move_selection(true),
            KeyCode::Char('p') if ctrl => self.move_selection(false),
            KeyCode::Esc => self.selected = None,
            KeyCode::Left => self.cursor = self.cursor.saturating_sub(1),
            KeyCode::Char('b') if ctrl => self.cursor = self.cursor.saturating_sub(1),
            KeyCode::Right => {
                if self.cursor == self.buffer.len() {
                    self.accept_selection();
                } else {
                    self.cursor += 1;
                }
            }
            KeyCode::End => self.cursor = self.buffer.len(),
            KeyCode::Char('f') if ctrl => {
                if self.cursor == self.buffer.len() {
                    self.accept_selection();
                } else {
                    self.cursor += 1;
                }
            }
            KeyCode::Home => self.cursor = 0,
            KeyCode::Char('a') if ctrl => self.cursor = 0,
            KeyCode::Char('e') if ctrl => self.cursor = self.buffer.len(),
            KeyCode::Backspace => {
                if self.cursor > 0 {
                    self.cursor -= 1;
                    self.buffer.remove(self.cursor);
                    self.selected = None;
                }
            }
            KeyCode::Delete => {
                if self.cursor < self.buffer.len() {
                    self.buffer.remove(self.cursor);
                    self.selected = None;
                }
            }
            KeyCode::Char('u') if ctrl => {
                self.buffer.drain(..self.cursor);
                self.cursor = 0;
                self.selected = None;
            }
            KeyCode::Char('k') if ctrl => {
                self.buffer.truncate(self.cursor);
                self.selected = None;
            }
            KeyCode::Char('w') if ctrl => {
                let mut start = self.cursor;
                while start > 0 && self.buffer[start - 1].is_whitespace() {
                    start -= 1;
                }
                while start > 0 && !self.buffer[start - 1].is_whitespace() {
                    start -= 1;
                }
                self.buffer.drain(start..self.cursor);
                self.cursor = start;
                self.selected = None;
            }
            KeyCode::Char(c)
                if crate::native::terminal::is_text_modifiers_on(
                    key.modifiers,
                    altgr_is_ctrl_alt,
                ) =>
            {
                self.buffer.insert(self.cursor, c);
                self.cursor += 1;
                self.selected = None;
            }
            _ => {}
        }
        Outcome::Continue
    }

    /// The frame to draw, with the dropdown rows at most `width` columns wide.
    pub fn frame(&self, width: usize, ansi: bool) -> Frame {
        let matches = self.matches();
        let visible = matches.len().min(MAX_ROWS);
        // Keep the selected row in view.
        let first = match self.selected {
            Some(i) if i >= visible => i + 1 - visible,
            _ => 0,
        };
        let mut rows = Vec::new();
        for (i, item) in matches.iter().enumerate().skip(first).take(visible) {
            let text = fit(&format!("  {item}"), width.saturating_sub(1));
            let row = if Some(i) == self.selected {
                if ansi {
                    format!("\u{1b}[7m{text}\u{1b}[0m")
                } else {
                    text.replacen("  ", "> ", 1)
                }
            } else {
                text
            };
            rows.push(row);
        }
        let hidden = matches.len().saturating_sub(first + visible);
        if hidden > 0 {
            rows.push(fit(
                &format!("  ... {hidden} more"),
                width.saturating_sub(1),
            ));
        }
        Frame {
            line: self.text(),
            rows,
            cursor: self.cursor,
        }
    }
}

/// `text` cut to `width` display columns.
fn fit(text: &str, width: usize) -> String {
    let mut out = String::new();
    let mut used = 0;
    for c in text.chars() {
        let w = c.to_string().width();
        if used + w > width {
            break;
        }
        used += w;
        out.push(c);
    }
    out
}

/// Draw `frame` after `prompt_line` (the last line of the question, already styled): the cursor
/// is on the input line before and after.
fn draw(
    out: &mut impl Write,
    prompt_line: &str,
    prefix_width: usize,
    frame: &Frame,
    below: &mut usize,
) -> std::io::Result<()> {
    // Back to the input line's start, then erase it and everything under it.
    write!(out, "\r\u{1b}[J{prompt_line}{}", frame.line)?;
    for row in &frame.rows {
        write!(out, "\r\n{row}")?;
    }
    *below = frame.rows.len();
    if *below > 0 {
        write!(out, "\u{1b}[{}A", *below)?;
    }
    let before_cursor: String = frame.line.chars().take(frame.cursor).collect();
    let column = prefix_width + before_cursor.width();
    write!(out, "\r")?;
    if column > 0 {
        write!(out, "\u{1b}[{column}C")?;
    }
    out.flush()
}

/// The result of [`read`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    /// The typed (or selected) text.
    Line(String),
    /// Ctrl-C.
    Interrupted,
    /// End of input.
    Eof,
}

/// Ask `prompt` (its last line is the input line) with `items` as suggestions, on the terminal.
/// `None` when the terminal cannot be put in raw mode (the caller reads a plain line instead).
pub fn read(prompt: &str, items: &[String], prefill: &str, ansi: bool) -> Option<Answer> {
    // Dropped on every way out, a panic included, so the terminal is never left raw.
    let _raw = crate::native::terminal::RawMode::enable().ok()?;
    let mut out = std::io::stdout();
    let (head, last) = match prompt.rsplit_once('\n') {
        Some((head, last)) => (Some(head), last),
        None => (None, prompt),
    };
    if let Some(head) = head {
        let _ = write!(out, "{}\r\n", head.replace('\n', "\r\n"));
    }
    let prefix_width = crate::format::remove_ansi(last).width();
    let mut state = Suggest::new(items, prefill);
    let mut below = 0usize;
    let answer = loop {
        let width = crate::format::terminal_width(1).unwrap_or(80);
        let frame = state.frame(width.saturating_sub(prefix_width).max(20), ansi);
        if draw(&mut out, last, prefix_width, &frame, &mut below).is_err() {
            break Answer::Eof;
        }
        let event = match crossterm::event::read() {
            Ok(event) => event,
            Err(_) => break Answer::Eof,
        };
        let Event::Key(key) = event else {
            continue;
        };
        match state.apply(key) {
            Outcome::Continue => {}
            Outcome::Submit(text) => break Answer::Line(text),
            Outcome::Cancel => break Answer::Interrupted,
            Outcome::Eof => break Answer::Eof,
        }
    };
    // Leave the answer on its line and the dropdown gone.
    let final_line = match &answer {
        Answer::Line(text) => text.clone(),
        _ => state.text(),
    };
    let _ = write!(out, "\r\u{1b}[J{last}{final_line}\r\n");
    let _ = out.flush();
    Some(answer)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn items() -> Vec<String> {
        [
            "sql:main",
            "sql:local",
            "api:main",
            "plugin:noaa",
            "valkey:main",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn ctrl(c: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
    }

    fn type_text(state: &mut Suggest, text: &str) {
        for c in text.chars() {
            assert_eq!(state.apply(key(KeyCode::Char(c))), Outcome::Continue);
        }
    }

    #[test]
    fn altgr_is_text_on_windows_and_a_shortcut_elsewhere() {
        let altgr = |c| KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL | KeyModifiers::ALT);
        let mut s = Suggest::new(&items(), "");
        assert_eq!(s.apply_on(altgr('@'), true), Outcome::Continue);
        assert_eq!(s.apply_on(altgr('{'), true), Outcome::Continue);
        assert_eq!(s.text(), "@{");
        // Ctrl+Alt+A is AltGr+A on a layout that types a letter there: text, not "home".
        assert_eq!(s.apply_on(altgr('a'), true), Outcome::Continue);
        assert_eq!(s.text(), "@{a");
        let mut s = Suggest::new(&items(), "");
        assert_eq!(s.apply_on(altgr('@'), false), Outcome::Continue);
        assert_eq!(s.text(), "");
    }

    #[test]
    fn the_dropdown_filters_as_the_user_types_prefix_matches_first() {
        let mut s = Suggest::new(&items(), "");
        assert!(s.matches().is_empty(), "wait until the user types");
        assert!(s.frame(80, false).rows.is_empty());
        s.apply(key(KeyCode::Down));
        assert_eq!(s.selected(), None);
        type_text(&mut s, "ma");
        assert_eq!(
            s.matches(),
            vec!["sql:main", "api:main", "valkey:main"],
            "contains, in the given order"
        );
        type_text(&mut s, "in");
        type_text(&mut s, "x");
        assert!(s.matches().is_empty());
        let mut s = Suggest::new(&items(), "");
        type_text(&mut s, "SQL");
        assert_eq!(s.matches(), vec!["sql:main", "sql:local"], "ignoring case");
        let mut s = Suggest::new(&items(), "");
        type_text(&mut s, "p");
        assert_eq!(
            s.matches(),
            vec!["plugin:noaa", "api:main"],
            "those starting with the text first"
        );
    }

    #[test]
    fn clearing_input_hides_the_dropdown_again() {
        let mut s = Suggest::new(&items(), "s");
        assert!(!s.frame(80, false).rows.is_empty());
        s.apply(key(KeyCode::Backspace));
        assert!(s.frame(80, false).rows.is_empty());
        assert_eq!(s.apply(key(KeyCode::Enter)), Outcome::Submit(String::new()));
    }

    #[test]
    fn enter_submits_what_was_typed_unless_a_row_is_selected() {
        let mut s = Suggest::new(&items(), "");
        type_text(&mut s, "sql:zzz");
        assert_eq!(
            s.apply(key(KeyCode::Enter)),
            Outcome::Submit("sql:zzz".into()),
            "an unknown value is submitted as typed"
        );
        let mut s = Suggest::new(&items(), "");
        type_text(&mut s, "sql");
        s.apply(key(KeyCode::Down));
        s.apply(key(KeyCode::Down));
        assert_eq!(s.selected(), Some(1));
        assert_eq!(
            s.apply(key(KeyCode::Enter)),
            Outcome::Submit("sql:local".into())
        );
    }

    #[test]
    fn selection_cycles_through_the_rows_and_back_to_the_typed_text() {
        let mut s = Suggest::new(&items(), "a");
        let count = s.matches().len();
        for expected in 0..count {
            s.apply(key(KeyCode::Down));
            assert_eq!(s.selected(), Some(expected));
        }
        s.apply(key(KeyCode::Down));
        assert_eq!(s.selected(), None);
        s.apply(key(KeyCode::Up));
        assert_eq!(s.selected(), Some(count - 1));
        s.apply(key(KeyCode::Esc));
        assert_eq!(s.selected(), None);
        type_text(&mut s, "p");
        assert_eq!(s.selected(), None, "typing leaves the list");
    }

    #[test]
    fn tab_completes_the_common_prefix_then_selects_then_accepts() {
        let mut s = Suggest::new(&items(), "s");
        s.apply(key(KeyCode::Tab));
        assert_eq!(s.text(), "sql:", "the prefix the matches share");
        s.apply(key(KeyCode::Tab));
        assert_eq!(
            s.selected(),
            Some(0),
            "nothing more to complete: into the list"
        );
        s.apply(key(KeyCode::Tab));
        assert_eq!(s.text(), "sql:main", "tab takes the selected row");
        let mut s = Suggest::new(&items(), "pl");
        s.apply(key(KeyCode::Tab));
        assert_eq!(s.text(), "plugin:noaa", "a single match completes whole");
    }

    #[test]
    fn right_at_the_end_takes_the_selected_row() {
        let mut s = Suggest::new(&items(), "api");
        s.apply(key(KeyCode::Down));
        s.apply(key(KeyCode::Right));
        assert_eq!(s.text(), "api:main");
        assert_eq!(s.selected(), None);
    }

    #[test]
    fn line_editing_keys() {
        let mut s = Suggest::new(&items(), "");
        type_text(&mut s, "sqx");
        s.apply(key(KeyCode::Left));
        s.apply(key(KeyCode::Backspace));
        assert_eq!(s.text(), "sx");
        type_text(&mut s, "l");
        assert_eq!(s.text(), "slx");
        s.apply(key(KeyCode::Home));
        s.apply(key(KeyCode::Delete));
        assert_eq!(s.text(), "lx");
        s.apply(ctrl('e'));
        s.apply(ctrl('w'));
        assert_eq!(s.text(), "");
        type_text(&mut s, "ab cd");
        s.apply(ctrl('w'));
        assert_eq!(s.text(), "ab ");
        s.apply(ctrl('u'));
        assert_eq!(s.text(), "");
    }

    #[test]
    fn ctrl_c_cancels_and_ctrl_d_ends_only_an_empty_line() {
        let mut s = Suggest::new(&items(), "x");
        assert_eq!(s.apply(ctrl('c')), Outcome::Cancel);
        assert_eq!(
            s.apply(ctrl('d')),
            Outcome::Continue,
            "nothing right of the cursor"
        );
        s.apply(key(KeyCode::Backspace));
        assert_eq!(s.apply(ctrl('d')), Outcome::Eof);
    }

    #[test]
    fn frames_show_at_most_eight_rows_and_count_the_rest() {
        let many: Vec<String> = (0..20).map(|i| format!("metric{i:02}")).collect();
        let s = Suggest::new(&many, "metric");
        let frame = s.frame(60, false);
        assert_eq!(frame.rows.len(), MAX_ROWS + 1);
        assert_eq!(frame.rows[0], "  metric00");
        assert_eq!(frame.rows[MAX_ROWS], "  ... 12 more");
        assert_eq!(frame.cursor, 6);
        // A selection past the window scrolls it.
        let mut s = Suggest::new(&many, "metric");
        for _ in 0..10 {
            s.apply(key(KeyCode::Down));
        }
        let frame = s.frame(60, false);
        assert!(frame.rows.iter().any(|r| r.starts_with("> metric09")));
        assert!(!frame.rows.iter().any(|r| r.contains("metric00")));
    }

    #[test]
    fn the_selected_row_is_in_reverse_video_when_color_is_on() {
        let mut s = Suggest::new(&items(), "api");
        s.apply(key(KeyCode::Down));
        let frame = s.frame(40, true);
        assert_eq!(frame.rows, vec!["\u{1b}[7m  api:main\u{1b}[0m".to_string()]);
    }

    #[test]
    fn rows_are_cut_to_the_width_and_duplicates_dropped() {
        let s = Suggest::new(
            &[
                "abcdefghijklmnop".to_string(),
                "abcdefghijklmnop".to_string(),
            ],
            "a",
        );
        let frame = s.frame(8, false);
        assert_eq!(frame.rows, vec!["  abcde".to_string()]);
    }

    #[test]
    fn cursor_uses_display_cells_for_wide_and_combining_text() {
        let prompt = " ❓ Connector: ";
        for input in ["", "x", "界", "e\u{301}", "👩‍💻"] {
            let state = Suggest::new(&[], input);
            let mut output = Vec::new();
            draw(
                &mut output,
                prompt,
                prompt.width(),
                &state.frame(60, false),
                &mut 0,
            )
            .unwrap();
            let output = String::from_utf8(output).unwrap();
            assert!(
                output.ends_with(&format!("\r\x1b[{}C", prompt.width() + input.width())),
                "{output:?}"
            );
        }
    }

    #[test]
    fn draw_keeps_the_cursor_on_the_input_line() {
        let s = Suggest::new(&items(), "sql");
        let frame = s.frame(60, false);
        let mut out = Vec::new();
        let mut below = 0;
        draw(&mut out, "Connector: ", 11, &frame, &mut below).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert_eq!(below, 2);
        assert!(text.starts_with("\r\u{1b}[JConnector: sql\r\n  sql:main\r\n  sql:local"));
        assert!(
            text.ends_with("\u{1b}[2A\r\u{1b}[14C"),
            "up two rows, then right of `Connector: sql`: {text:?}"
        );
    }
}
