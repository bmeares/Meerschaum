//! The interactive line editor: v4's prompt_toolkit `PromptSession` (`complete_while_typing=True`,
//! `reserve_space_for_menu=False`, `ValidAutoSuggest`, a bottom toolbar) reproduced on
//! `reedline`.
//!
//! # How it works
//!
//! reedline has no toolbar API, no complete-while-typing, and its menus only touch the buffer on
//! accept. Rather than fight those, the shell keeps prompt_toolkit's own state machine (the
//! buffer's `complete_state`, `suggestion`, working lines and incremental search) in [`Shared`]
//! and drives reedline through three of its public extension points:
//!
//! - **Hook menus** ([`HookMenu`]): menus that are never active. A `ReedlineEvent::Menu(name)`
//!   event "activates" one, and with quick completions on, reedline immediately calls its
//!   `update_values(&mut Editor, ..)`: a synchronous callback, in event order, with the editor.
//!   Tab, Shift-Tab, Up/Down, accepting a suggestion, every character insertion and the
//!   reverse-i-search keys are routed to hooks by [`ShellEditMode`], which wraps the emacs mode.
//! - **The hinter** ([`ShellHinter`]): reedline prints the hint after the buffer on every paint
//!   and counts its lines when it decides how far to scroll. The hint carries the auto-suggestion
//!   ghost text, the completion menu rows below the cursor (each on its own line, positioned with
//!   `CHA`), and a final line that jumps (`CUP`) to the bottom of the screen and draws the
//!   toolbar. Because reedline counts those lines, it scrolls the prompt up exactly as far as
//!   prompt_toolkit does to keep the input above the toolbar, and every repaint (typing, resize,
//!   Ctrl-L, `clear`) redraws the toolbar in the same frame, so it never flickers or goes stale.
//!   On submit reedline repaints without hints, which erases the menu and toolbar like
//!   prompt_toolkit does when input is accepted; command output then scrolls normally.
//! - **The prompt** ([`ShellPrompt`]): when the menu has to open *above* the cursor (no room
//!   below, e.g. after long output), its rows overlay the prompt's own leading lines, so they are
//!   drawn as part of the prompt string (which reedline paints first, without counting extra
//!   rows). The prompt also renders `(reverse-i-search)` while searching.
//!
//! To decide between above and below, the hinter tracks the prompt's screen row the way
//! reedline's painter does (measured once per prompt, then scrolled by the same rule), so it
//! applies prompt_toolkit's float placement (`FloatContainer._draw_float`) to the same geometry.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};

use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
use nu_ansi_term::Style;
use reedline::{
    Completer, EditCommand, EditMode, Editor, Emacs, Highlighter, Hinter, History, Keybindings,
    Menu, MenuEvent, MenuSettings, Painter, Prompt, PromptEditMode, PromptHistorySearch,
    ReedlineEvent, ReedlineRawEvent, StyledText, Suggestion, UndoBehavior,
};
use unicode_width::UnicodeWidthStr;

use super::completer::{Completion, CompletionData, auto_suggest};
use super::toolbar::ToolbarModel;
use crate::format::remove_ansi;
use crate::types::DateContext;

/// prompt_toolkit's `CompletionsMenu(max_height=16, scroll_offset=1)`.
const MENU_MAX_HEIGHT: usize = 16;
const MENU_SCROLL_OFFSET: usize = 1;
/// `CompletionsMenuControl.MIN_WIDTH`.
const MENU_MIN_WIDTH: usize = 7;

/// The SGR of each prompt_toolkit style class, as its 256-color renderer emits them.
mod sgr {
    /// `completion-menu`: `bg:#bbbbbb #000000`.
    pub const ITEM: &str = "\x1b[0;38;5;16;48;5;250m";
    /// `completion-menu.completion.current`: `fg:#888888 bg:#ffffff reverse`.
    pub const CURRENT: &str = "\x1b[0;38;5;102;48;5;231;7m";
    /// The typed prefix (`fg:ansiblue bold`) inside an item.
    pub const MATCH: &str = "\x1b[0;34;1;48;5;250m";
    /// The typed prefix inside the current item.
    pub const MATCH_CURRENT: &str = "\x1b[0;34;1;48;5;231;7m";
    /// `scrollbar.button`: `bg:#444444`.
    pub const SCROLL_BUTTON: &str = "\x1b[0;38;5;16;48;5;238m";
    /// `scrollbar.background`: `bg:#aaaaaa`.
    pub const SCROLL_BACKGROUND: &str = "\x1b[0;38;5;16;48;5;248m";
    /// `auto-suggestion`: `#666666`.
    pub const SUGGESTION: &str = "\x1b[0;38;5;241m";
    /// Reset.
    pub const RESET: &str = "\x1b[0m";
}

/// The text and cursor (a byte offset) of the buffer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Doc {
    /// The buffer text.
    pub text: String,
    /// The cursor, in bytes.
    pub cursor: usize,
}

impl Doc {
    fn new(text: impl Into<String>, cursor: usize) -> Doc {
        let text = text.into();
        let cursor = cursor.min(text.len());
        Doc { text, cursor }
    }

    fn before(&self) -> &str {
        &self.text[..self.cursor]
    }

    fn after(&self) -> &str {
        &self.text[self.cursor..]
    }

    fn cursor_chars(&self) -> usize {
        self.before().chars().count()
    }

    fn at_end(&self) -> bool {
        self.cursor >= self.text.len()
    }
}

/// The byte offset of the `n`th character.
fn byte_of(s: &str, n: usize) -> usize {
    s.char_indices().nth(n).map(|(i, _)| i).unwrap_or(s.len())
}

/// Python `s[:n]` for a possibly negative `n` (characters).
fn py_head(s: &str, n: i64) -> &str {
    let len = s.chars().count() as i64;
    let n = if n < 0 { (len + n).max(0) } else { n.min(len) };
    &s[..byte_of(s, n as usize)]
}

/// Python `s[n:]` for a possibly negative `n` (characters).
fn py_tail(s: &str, n: i64) -> &str {
    let len = s.chars().count() as i64;
    let n = if n < 0 { (len + n).max(0) } else { n.min(len) };
    &s[byte_of(s, n as usize)..]
}

/// `CompletionState`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteState {
    /// `original_document`.
    pub original: Doc,
    /// `completions`.
    pub completions: Vec<Completion>,
    /// `complete_index`.
    pub index: Option<usize>,
}

impl CompleteState {
    /// `new_text_and_position()`.
    fn new_doc(&self) -> Doc {
        let Some(i) = self.index else {
            return self.original.clone();
        };
        let c = &self.completions[i];
        let tbc = self.original.before();
        let before = if c.start_position == 0 {
            tbc
        } else {
            py_head(tbc, c.start_position)
        };
        let text = format!("{before}{}{}", c.text, self.original.after());
        Doc::new(text, before.len() + c.text.len())
    }
}

/// `completion_does_nothing(document, completion)`.
fn does_nothing(doc: &Doc, c: &Completion) -> bool {
    let tbc = doc.before();
    let len = tbc.chars().count() as i64;
    py_tail(tbc, len + c.start_position) == c.text
}

/// `get_common_complete_suffix(document, completions)`.
fn common_suffix(doc: &Doc, completions: &[Completion]) -> String {
    let tbc = doc.before();
    let unchanged = completions.iter().all(|c| {
        let end = py_head(&c.text, -c.start_position);
        tbc.ends_with(end)
    });
    if !unchanged || completions.is_empty() {
        return String::new();
    }
    let suffixes: Vec<&str> = completions
        .iter()
        .map(|c| py_tail(&c.text, -c.start_position))
        .collect();
    let s1 = suffixes.iter().min().copied().unwrap_or("");
    let s2 = suffixes.iter().max().copied().unwrap_or("");
    let mut out = String::new();
    for (a, b) in s1.chars().zip(s2.chars()) {
        if a != b {
            return out;
        }
        out.push(a);
    }
    s1.to_string()
}

/// `Completion.new_completion_from_position(position)`.
fn from_position(c: &Completion, position: usize) -> Completion {
    Completion {
        text: py_tail(&c.text, position as i64 - c.start_position).to_string(),
        start_position: 0,
        display: c.display.clone(),
        highlight: c.highlight,
    }
}

/// The incremental search (`SearchState` plus the `SearchBufferControl` text).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchState {
    /// The search text.
    pub term: String,
    /// `SearchDirection.FORWARD`.
    pub forward: bool,
    /// The buffer when the search started.
    pub original: Doc,
    /// The working index when the search started.
    pub original_index: usize,
    /// The current match: working index and character position.
    pub found: Option<(usize, usize)>,
}

/// Where the prompt sits on the screen (reedline's `prompt_start_row`, tracked alongside).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Layout {
    /// The prompt's first row.
    pub prompt_row: u16,
    /// The terminal size the row was computed for (columns, rows).
    pub size: (u16, u16),
    /// Ctrl-L cleared the screen (the prompt moves to the top).
    pub cleared: bool,
}

/// The editor state shared by the edit mode, the hook menus, the hinter, the prompt and the
/// highlighter (prompt_toolkit's `Buffer`).
#[derive(Debug)]
pub struct Shared {
    /// The completion candidates.
    pub data: CompletionData,
    /// Clock inputs for `parse_line`.
    pub dates: DateContext,
    /// `history.get_strings()`, oldest first.
    pub history: Vec<String>,
    /// `_working_lines`: the history plus the line being edited.
    pub working: Vec<String>,
    /// `working_index`.
    pub working_index: usize,
    /// The document the state below describes.
    pub doc: Doc,
    /// `complete_state`.
    pub complete: Option<CompleteState>,
    /// `suggestion`.
    pub suggestion: Option<String>,
    /// The completion menu's `vertical_scroll`.
    pub scroll: usize,
    /// The incremental search.
    pub search: Option<SearchState>,
    /// Characters typed into the search, waiting for their hook.
    pub queue: VecDeque<char>,
    /// Screen geometry.
    pub layout: Layout,
    /// The rendered prompt.
    pub prompt: String,
    /// The toolbar.
    pub toolbar: ToolbarModel,
    /// Menu rows drawn inside the prompt's leading lines (`(line index, text)`).
    pub above: Vec<(usize, String)>,
    /// A fixed terminal size (tests); the terminal is asked otherwise.
    pub size_override: Option<(u16, u16)>,
}

/// The shared state handle.
pub type SharedRef = Arc<Mutex<Shared>>;

fn lock(shared: &SharedRef) -> MutexGuard<'_, Shared> {
    shared.lock().unwrap_or_else(|e| e.into_inner())
}

impl Shared {
    /// A new state.
    pub fn new(data: CompletionData, dates: DateContext) -> Shared {
        Shared {
            data,
            dates,
            history: Vec::new(),
            working: vec![String::new()],
            working_index: 0,
            doc: Doc::default(),
            complete: None,
            suggestion: None,
            scroll: 0,
            search: None,
            queue: VecDeque::new(),
            layout: Layout::default(),
            prompt: String::new(),
            toolbar: ToolbarModel::default(),
            above: Vec::new(),
            size_override: None,
        }
    }

    /// Reset for a new prompt (`PromptSession.prompt()` builds fresh working lines).
    pub fn begin_line(&mut self) {
        self.working = self.history.clone();
        self.working.push(String::new());
        self.working_index = self.working.len() - 1;
        self.doc = Doc::default();
        self.complete = None;
        self.suggestion = None;
        self.scroll = 0;
        self.search = None;
        self.queue.clear();
        self.above.clear();
        let size = self.term_size();
        // reedline starts the prompt on the cursor's row, or the next one when the cursor is
        // not in the first column (and makes room at the bottom).
        let row = match crossterm::cursor::position() {
            Ok((col, row)) => {
                let row = if col > 0 { row + 1 } else { row };
                row.min(size.1.saturating_sub(1))
            }
            Err(_) => size.1.saturating_sub(1),
        };
        self.layout = Layout {
            prompt_row: row,
            size,
            cleared: false,
        };
    }

    fn term_size(&self) -> (u16, u16) {
        if let Some(s) = self.size_override {
            return s;
        }
        match crossterm::terminal::size() {
            Ok((0, 0)) | Err(_) => (80, 24),
            Ok(s) => s,
        }
    }

    /// Record the editor's document; a changed text or cursor clears the completion state, and a
    /// changed text the suggestion (`Buffer._text_changed` / `_cursor_position_changed`).
    fn observe(&mut self, doc: Doc) {
        if doc.text != self.doc.text {
            self.complete = None;
            self.suggestion = None;
            if self.search.is_none()
                && let Some(line) = self.working.get_mut(self.working_index)
            {
                *line = doc.text.clone();
            }
        } else if doc.cursor != self.doc.cursor {
            self.complete = None;
        }
        self.doc = doc;
    }

    /// Set a new document as `Buffer.document = ...` would.
    fn set_doc(&mut self, doc: Doc) {
        let keep = self.complete.take();
        self.observe(doc);
        self.complete = keep;
    }

    fn completions_for(&self, doc: &Doc) -> Vec<Completion> {
        let mut completions = self.data.get_completions(&doc.text, &self.dates);
        if completions.len() == 1 && does_nothing(doc, &completions[0]) {
            completions.clear();
        }
        completions
    }

    /// After `insert_text`: the completer (complete while typing) and the suggester run.
    fn after_insert(&mut self) {
        let doc = self.doc.clone();
        let completions = self.completions_for(&doc);
        self.complete = (!completions.is_empty()).then(|| CompleteState {
            original: doc.clone(),
            completions,
            index: None,
        });
        self.scroll = 0;
        self.suggestion = auto_suggest(&self.history, &self.data.do_names, &doc.text);
    }

    fn insert(&mut self, s: &str) -> Doc {
        let doc = &self.doc;
        let text = format!("{}{s}{}", doc.before(), doc.after());
        let new = Doc::new(text, doc.cursor + s.len());
        self.observe(new.clone());
        self.after_insert();
        new
    }

    fn go_to_completion(&mut self, index: Option<usize>) -> Option<Doc> {
        let state = self.complete.as_mut()?;
        state.index = index;
        let new = state.new_doc();
        self.set_doc(new.clone());
        Some(new)
    }

    /// `complete_next()`.
    fn complete_next(&mut self) -> Option<Doc> {
        let state = self.complete.as_ref()?;
        let count = state.completions.len();
        let index = match state.index {
            None => Some(0),
            Some(i) if i + 1 == count => None,
            Some(i) => Some((i + 1).min(count - 1)),
        };
        self.go_to_completion(index)
    }

    /// `complete_previous()`.
    fn complete_previous(&mut self) -> Option<Doc> {
        let state = self.complete.as_ref()?;
        let index = match state.index {
            Some(0) => None,
            None => Some(state.completions.len() - 1),
            Some(i) => Some(i - 1),
        };
        self.go_to_completion(index)
    }

    /// `start_completion(insert_common_part=True)` (Tab without a menu).
    fn start_completion_insert_common(&mut self) -> Option<Doc> {
        let doc = self.doc.clone();
        let completions = self.completions_for(&doc);
        if completions.is_empty() {
            self.complete = None;
            return None;
        }
        let common = common_suffix(&doc, &completions);
        if !common.is_empty() {
            let new = self.insert(&common);
            if completions.len() > 1 {
                let n = common.chars().count();
                self.complete = Some(CompleteState {
                    original: new.clone(),
                    completions: completions.iter().map(|c| from_position(c, n)).collect(),
                    index: None,
                });
                self.scroll = 0;
            }
            return Some(new);
        }
        self.complete = Some(CompleteState {
            original: doc,
            completions,
            index: None,
        });
        self.scroll = 0;
        if self
            .complete
            .as_ref()
            .is_some_and(|c| c.completions.len() == 1)
        {
            return self.go_to_completion(Some(0));
        }
        None
    }

    fn load_working(&mut self, index: usize, cursor_at_end_of_first_line: bool) -> Doc {
        self.working_index = index;
        let text = self.working[index].clone();
        let cursor = if cursor_at_end_of_first_line {
            text.find('\n').unwrap_or(text.len())
        } else {
            text.len()
        };
        let doc = Doc::new(text, cursor);
        self.complete = None;
        self.suggestion = None;
        self.doc = doc.clone();
        doc
    }

    /// `auto_up()`.
    fn auto_up(&mut self) -> Option<Doc> {
        if self.complete.is_some() {
            return self.complete_previous();
        }
        let (row, col) = row_col(&self.doc);
        if row > 0 {
            return Some(self.move_to_row(row - 1, col));
        }
        // `history_backward()`.
        (self.working_index > 0).then(|| self.load_working(self.working_index - 1, false))
    }

    /// `auto_down()`.
    fn auto_down(&mut self) -> Option<Doc> {
        if self.complete.is_some() {
            return self.complete_next();
        }
        let (row, col) = row_col(&self.doc);
        if row + 1 < self.doc.text.split('\n').count() {
            return Some(self.move_to_row(row + 1, col));
        }
        // `history_forward()`.
        (self.working_index + 1 < self.working.len())
            .then(|| self.load_working(self.working_index + 1, true))
    }

    fn move_to_row(&mut self, row: usize, col: usize) -> Doc {
        let mut offset = 0;
        for (i, line) in self.doc.text.split('\n').enumerate() {
            if i == row {
                let doc = Doc::new(self.doc.text.clone(), offset + byte_of(line, col));
                self.complete = None;
                self.doc = doc.clone();
                return doc;
            }
            offset += line.len() + 1;
        }
        self.doc.clone()
    }

    /// The suggestion text that Right / Ctrl-F / Ctrl-E would accept.
    pub fn suggestion_available(&self) -> bool {
        self.search.is_none() && self.suggestion.as_deref().is_some_and(|s| !s.is_empty())
    }

    fn search_from(
        &self,
        term: &str,
        forward: bool,
        start: (usize, usize),
        include_current: bool,
    ) -> Option<(usize, usize)> {
        if term.is_empty() {
            return None;
        }
        let n = self.working.len();
        let (wi, pos) = start;
        let line = &self.working[wi];
        let chars: Vec<char> = line.chars().collect();
        let needle: Vec<char> = term.chars().collect();
        let matches_at = |hay: &[char], i: usize| hay[i..].starts_with(&needle);
        if forward {
            // `Document.find(include_current_position=...)`.
            let from = if include_current { pos } else { pos + 1 };
            if let Some(i) = (from..chars.len()).find(|&i| matches_at(&chars, i)) {
                return Some((wi, i));
            }
            for k in 1..=n {
                let i = (wi + k) % n;
                let hay: Vec<char> = self.working[i].chars().collect();
                if let Some(p) = (0..hay.len()).find(|&p| matches_at(&hay, p)) {
                    return Some((i, p));
                }
            }
        } else {
            // `Document.find_backwards()`: in the text before the cursor.
            let before = &chars[..pos.min(chars.len())];
            if let Some(i) = (0..before.len())
                .rev()
                .find(|&i| before[i..].starts_with(&needle))
            {
                return Some((wi, i));
            }
            for k in 1..=n {
                let i = (wi + n - k) % n;
                let hay: Vec<char> = self.working[i].chars().collect();
                if let Some(p) = (0..hay.len()).rev().find(|&p| matches_at(&hay, p)) {
                    return Some((i, p));
                }
            }
        }
        None
    }

    fn show_match(&mut self) -> Option<Doc> {
        let (wi, pos) = self.search.as_ref()?.found?;
        let text = self.working[wi].clone();
        let doc = Doc::new(text.clone(), byte_of(&text, pos));
        self.doc = doc.clone();
        Some(doc)
    }

    fn search_research(&mut self, include_current: bool, forward: Option<bool>) -> Option<Doc> {
        let search = self.search.as_ref()?;
        let forward = forward.unwrap_or(search.forward);
        let start = if include_current {
            (search.original_index, search.original.cursor_chars())
        } else {
            search
                .found
                .unwrap_or((search.original_index, search.original.cursor_chars()))
        };
        let term = search.term.clone();
        let found = self.search_from(&term, forward, start, include_current);
        if let Some(s) = self.search.as_mut() {
            s.forward = forward;
            if found.is_some() {
                s.found = found;
            }
        }
        self.show_match()
    }

    fn run_hook(&mut self, hook: Hook) -> Option<Doc> {
        match hook {
            Hook::Typed => {
                self.after_insert();
                None
            }
            Hook::Tab => {
                if self.complete.is_some() {
                    self.complete_next()
                } else {
                    self.start_completion_insert_common()
                }
            }
            Hook::BackTab => self.complete_previous(),
            Hook::Up => self.auto_up(),
            Hook::Down => self.auto_down(),
            Hook::AcceptSuggestion => {
                let suggestion = self.suggestion.clone().filter(|s| !s.is_empty())?;
                Some(self.insert(&suggestion))
            }
            Hook::SearchStart => {
                if self.search.is_none() {
                    self.complete = None;
                    self.search = Some(SearchState {
                        term: String::new(),
                        forward: false,
                        original: self.doc.clone(),
                        original_index: self.working_index,
                        found: None,
                    });
                    None
                } else {
                    self.search_research(false, Some(false))
                }
            }
            Hook::SearchChar => {
                let c = self.queue.pop_front()?;
                self.search.as_mut()?.term.push(c);
                self.search_research(true, None)
            }
            Hook::SearchBack => {
                self.search.as_mut()?.term.pop();
                self.search_research(true, None)
            }
            Hook::SearchPrev => self.search_research(false, Some(false)),
            Hook::SearchNext => self.search_research(false, Some(true)),
            Hook::SearchAccept => {
                let search = self.search.take()?;
                let (wi, _) = search.found?;
                self.working_index = wi;
                self.complete = None;
                self.suggestion = None;
                None
            }
            Hook::SearchAbort => {
                let search = self.search.take()?;
                self.working_index = search.original_index;
                self.complete = None;
                self.suggestion = None;
                self.doc = search.original.clone();
                Some(search.original)
            }
        }
    }
}

/// The (row, column) of the cursor in a multi-line document (characters).
fn row_col(doc: &Doc) -> (usize, usize) {
    let before = doc.before();
    let row = before.matches('\n').count();
    let col = before.rsplit('\n').next().unwrap_or("").chars().count();
    (row, col)
}

/// The hooks a [`HookMenu`] runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Hook {
    /// Text was inserted (complete while typing, auto-suggest).
    Typed,
    /// Tab (`menu-complete`).
    Tab,
    /// Shift-Tab (`menu-complete-backward`).
    BackTab,
    /// Up (`auto_up`).
    Up,
    /// Down (`auto_down`).
    Down,
    /// Right / Ctrl-F / Ctrl-E with a suggestion.
    AcceptSuggestion,
    /// Ctrl-R.
    SearchStart,
    /// A character typed into the search.
    SearchChar,
    /// Backspace in the search.
    SearchBack,
    /// Ctrl-R / Up in the search.
    SearchPrev,
    /// Ctrl-S / Down in the search.
    SearchNext,
    /// Enter / Escape in the search.
    SearchAccept,
    /// Ctrl-C / Ctrl-G in the search.
    SearchAbort,
}

const HOOKS: &[Hook] = &[
    Hook::Typed,
    Hook::Tab,
    Hook::BackTab,
    Hook::Up,
    Hook::Down,
    Hook::AcceptSuggestion,
    Hook::SearchStart,
    Hook::SearchChar,
    Hook::SearchBack,
    Hook::SearchPrev,
    Hook::SearchNext,
    Hook::SearchAccept,
    Hook::SearchAbort,
];

impl Hook {
    fn name(self) -> String {
        format!("mrsm:{self:?}")
    }

    fn event(self) -> ReedlineEvent {
        ReedlineEvent::Menu(self.name())
    }
}

/// A menu that is never active: activating it runs its hook with the editor.
pub struct HookMenu {
    hook: Hook,
    shared: SharedRef,
    settings: MenuSettings,
}

impl HookMenu {
    /// One menu per hook.
    pub fn all(shared: &SharedRef) -> Vec<reedline::ReedlineMenu> {
        HOOKS
            .iter()
            .map(|h| {
                reedline::ReedlineMenu::HistoryMenu(Box::new(HookMenu {
                    hook: *h,
                    shared: shared.clone(),
                    settings: MenuSettings::default().with_name(&h.name()).with_marker(""),
                }))
            })
            .collect()
    }
}

fn read_doc(editor: &mut Editor) -> Doc {
    let text = editor.get_buffer().to_string();
    let mut cursor = text.len();
    editor.edit_buffer(|lb| cursor = lb.insertion_point(), UndoBehavior::NoOp);
    Doc::new(text, cursor)
}

fn write_doc(editor: &mut Editor, doc: &Doc) {
    let text = doc.text.clone();
    let cursor = doc.cursor;
    editor.edit_buffer(
        move |lb| {
            lb.set_buffer(text);
            lb.set_insertion_point(cursor);
        },
        UndoBehavior::CreateUndoPoint,
    );
}

impl Menu for HookMenu {
    fn settings(&self) -> &MenuSettings {
        &self.settings
    }
    fn is_active(&self) -> bool {
        false
    }
    fn set_active(&mut self, _active: bool) {}
    fn clear_input(&mut self) {}
    fn menu_event(&mut self, _event: MenuEvent) {}
    fn can_quick_complete(&self) -> bool {
        true
    }
    fn can_partially_complete(
        &mut self,
        _values_updated: bool,
        _editor: &mut Editor,
        _completer: &mut dyn Completer,
    ) -> bool {
        false
    }
    fn update_values(&mut self, editor: &mut Editor, _completer: &mut dyn Completer) {
        let mut s = lock(&self.shared);
        let doc = read_doc(editor);
        s.observe(doc);
        if let Some(new) = s.run_hook(self.hook) {
            write_doc(editor, &new);
            let index = s.working_index;
            if s.search.is_none()
                && let Some(line) = s.working.get_mut(index)
            {
                *line = new.text.clone();
            }
        }
    }
    fn reset_position(&mut self) {}
    fn update_working_details(
        &mut self,
        _editor: &mut Editor,
        _completer: &mut dyn Completer,
        _painter: &Painter,
    ) {
    }
    fn replace_in_buffer(&self, _editor: &mut Editor) {}
    fn menu_required_lines(&self, _terminal_columns: u16) -> u16 {
        0
    }
    fn menu_string(&self, _available_lines: u16, _use_ansi_coloring: bool) -> String {
        String::new()
    }
    fn min_rows(&self) -> u16 {
        0
    }
    fn get_values(&self) -> &[Suggestion] {
        &[]
    }
}

/// The key bindings: emacs mode with prompt_toolkit's completion, suggestion, history and
/// search keys routed to hooks.
pub struct ShellEditMode {
    inner: Emacs,
    shared: SharedRef,
}

/// The emacs bindings without reedline's own history search and hint keys.
pub fn keybindings() -> Keybindings {
    let mut kb = reedline::default_emacs_keybindings();
    kb.add_binding(KeyModifiers::NONE, KeyCode::Right, ReedlineEvent::Right);
    kb.add_binding(
        KeyModifiers::CONTROL,
        KeyCode::Char('f'),
        ReedlineEvent::Right,
    );
    kb.add_binding(KeyModifiers::NONE, KeyCode::Left, ReedlineEvent::Left);
    kb.add_binding(
        KeyModifiers::CONTROL,
        KeyCode::Char('b'),
        ReedlineEvent::Left,
    );
    // `end` / `c-e` only move the cursor; a suggestion is accepted by Ctrl-E through a hook.
    let to_end = ReedlineEvent::Edit(vec![EditCommand::MoveToLineEnd { select: false }]);
    kb.add_binding(KeyModifiers::NONE, KeyCode::End, to_end.clone());
    kb.add_binding(KeyModifiers::CONTROL, KeyCode::Char('e'), to_end);
    kb
}

impl ShellEditMode {
    /// Wrap the emacs mode.
    pub fn new(shared: SharedRef) -> ShellEditMode {
        ShellEditMode {
            inner: Emacs::new(keybindings()),
            shared,
        }
    }

    fn delegate(&mut self, event: Event) -> ReedlineEvent {
        let Ok(raw) = ReedlineRawEvent::try_from(event) else {
            return ReedlineEvent::None;
        };
        let parsed = self.inner.parse_event(raw);
        match parsed {
            ReedlineEvent::Edit(commands)
                if !commands.is_empty()
                    && commands.iter().all(|c| {
                        matches!(c, EditCommand::InsertChar(_) | EditCommand::InsertString(_))
                    }) =>
            {
                ReedlineEvent::Multiple(vec![ReedlineEvent::Edit(commands), Hook::Typed.event()])
            }
            other => other,
        }
    }

    fn search_key(&mut self, key: &KeyEvent, event: Event) -> ReedlineEvent {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Char('r') if ctrl => Hook::SearchPrev.event(),
            KeyCode::Char('s') if ctrl => Hook::SearchNext.event(),
            KeyCode::Char('c') | KeyCode::Char('g') if ctrl => Hook::SearchAbort.event(),
            KeyCode::Up => Hook::SearchPrev.event(),
            KeyCode::Down => Hook::SearchNext.event(),
            KeyCode::Enter | KeyCode::Esc => Hook::SearchAccept.event(),
            KeyCode::Backspace => Hook::SearchBack.event(),
            KeyCode::Char(c) if !ctrl && !key.modifiers.contains(KeyModifiers::ALT) => {
                lock(&self.shared).queue.push_back(c);
                Hook::SearchChar.event()
            }
            _ => {
                let rest = self.delegate(event);
                ReedlineEvent::Multiple(vec![Hook::SearchAccept.event(), rest])
            }
        }
    }
}

impl EditMode for ShellEditMode {
    fn parse_event(&mut self, raw: ReedlineRawEvent) -> ReedlineEvent {
        let event: Event = raw.into();
        let Event::Key(key) = &event else {
            if let Event::Paste(_) = &event {
                return self.delegate(event);
            }
            return self.delegate(event);
        };
        let key = *key;
        let (searching, suggestion) = {
            let s = lock(&self.shared);
            (s.search.is_some(), s.suggestion_available())
        };
        if searching {
            return self.search_key(&key, event);
        }
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        let none = key.modifiers.is_empty();
        match key.code {
            KeyCode::Tab if none => Hook::Tab.event(),
            KeyCode::BackTab => Hook::BackTab.event(),
            KeyCode::Tab if key.modifiers == KeyModifiers::SHIFT => Hook::BackTab.event(),
            KeyCode::Up if none => Hook::Up.event(),
            KeyCode::Down if none => Hook::Down.event(),
            KeyCode::Char('p') if ctrl => Hook::Up.event(),
            KeyCode::Char('n') if ctrl => Hook::Down.event(),
            KeyCode::Char('r') if ctrl => Hook::SearchStart.event(),
            KeyCode::Right if none && suggestion => Hook::AcceptSuggestion.event(),
            KeyCode::Char('f') | KeyCode::Char('e') if ctrl && suggestion => {
                Hook::AcceptSuggestion.event()
            }
            KeyCode::Char('l') if ctrl => {
                let mut s = lock(&self.shared);
                s.layout.cleared = true;
                drop(s);
                self.delegate(event)
            }
            KeyCode::Enter if none => {
                // The submit repaint has no hint; the prompt must not keep menu rows either.
                lock(&self.shared).above.clear();
                self.delegate(event)
            }
            KeyCode::Char('j') | KeyCode::Char('m') if ctrl => {
                // The submit repaint has no hint; the prompt must not keep menu rows either.
                lock(&self.shared).above.clear();
                self.delegate(event)
            }
            _ => self.delegate(event),
        }
    }

    fn edit_mode(&self) -> PromptEditMode {
        self.inner.edit_mode()
    }
}

/// The prompt: v4's rendered prompt, the `(reverse-i-search)` prompt while searching, and the
/// rows of a completion menu that opens above the cursor.
pub struct ShellPrompt {
    /// The shared state.
    pub shared: SharedRef,
}

impl Prompt for ShellPrompt {
    fn render_prompt_left(&self) -> Cow<'_, str> {
        let s = lock(&self.shared);
        let mut lines: Vec<String> = s.prompt.split('\n').map(str::to_string).collect();
        if let Some(search) = &s.search {
            // The search line replaces the prompt's last line (`SearchBufferControl`).
            let direction = if search.forward {
                "i-search"
            } else {
                "reverse-i-search"
            };
            if let Some(last) = lines.last_mut() {
                *last = format!("({direction})`{}': ", search.term);
            }
        } else {
            for (i, text) in &s.above {
                if let Some(line) = lines.get_mut(*i) {
                    line.push_str(text);
                }
            }
        }
        Cow::Owned(lines.join("\n"))
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

    fn render_prompt_history_search_indicator(&self, _search: PromptHistorySearch) -> Cow<'_, str> {
        Cow::Borrowed("")
    }

    fn get_prompt_color(&self) -> reedline::Color {
        reedline::Color::Default
    }

    fn get_indicator_color(&self) -> reedline::Color {
        reedline::Color::Default
    }

    fn get_prompt_right_color(&self) -> reedline::Color {
        reedline::Color::Default
    }
}

/// The buffer highlighter: plain text, with the incremental search's match in reverse video
/// (`incsearch.current`).
pub struct ShellHighlighter {
    /// The shared state.
    pub shared: SharedRef,
}

impl Highlighter for ShellHighlighter {
    fn highlight(&self, line: &str, _cursor: usize) -> StyledText {
        let mut out = StyledText::new();
        let s = lock(&self.shared);
        let span = s.search.as_ref().and_then(|search| {
            let (_, pos) = search.found?;
            let start = byte_of(line, pos);
            let end = byte_of(line, pos + search.term.chars().count());
            (line.get(start..end) == Some(search.term.as_str()) && !search.term.is_empty())
                .then_some((start, end))
        });
        match span {
            Some((start, end)) => {
                out.push((Style::new(), line[..start].to_string()));
                out.push((Style::new().reverse(), line[start..end].to_string()));
                out.push((Style::new(), line[end..].to_string()));
            }
            None => out.push((Style::new(), line.to_string())),
        }
        out
    }
}

/// Where printing `pieces` from column 0 leaves the cursor: `(column, rows)`; the column may be
/// `cols` (the deferred-wrap state), like reedline's `wrap_position`.
fn wrap_position(pieces: &[&str], cols: usize) -> (usize, usize) {
    let (mut row, mut col) = (0usize, 0usize);
    for piece in pieces {
        for g in unicode_segmentation::UnicodeSegmentation::graphemes(*piece, true) {
            match g {
                "\n" | "\r\n" => {
                    row += 1;
                    col = 0;
                }
                "\r" => col = 0,
                _ => {
                    let w = g.width();
                    if col >= cols || (w > 0 && col + w > cols) {
                        row += 1;
                        col = 0;
                    }
                    col += w;
                }
            }
        }
    }
    (col, row)
}

fn resolve_wrap((col, row): (usize, usize), cols: usize) -> (usize, usize) {
    if col >= cols {
        (0, row + 1)
    } else {
        (col, row)
    }
}

/// reedline's `estimate_required_lines`.
fn required_lines(input: &str, cols: usize) -> usize {
    input
        .lines()
        .map(|l| 1 + wrap_position(&[l], cols).1)
        .sum::<usize>()
        .max(1)
}

/// prompt_toolkit's `Window._scroll_without_linewrapping` for the menu.
fn scroll_to(current: usize, cursor: usize, window: usize, content: usize) -> usize {
    let start = MENU_SCROLL_OFFSET.min(window / 2).min(cursor);
    let end = MENU_SCROLL_OFFSET
        .min(window / 2)
        .min(content.saturating_sub(1).saturating_sub(cursor));
    let mut scroll = current;
    if scroll > content.saturating_sub(window) {
        scroll = content.saturating_sub(window);
    }
    if scroll as i64 > cursor as i64 - start as i64 {
        scroll = cursor.saturating_sub(start);
    }
    if (scroll as i64) < (cursor as i64 + 1) - window as i64 + end as i64 {
        scroll = cursor + 1 + end - window;
    }
    scroll
}

/// `_trim_formatted_text(display, max_width)` and the highlighted prefix length.
fn trim_display(display: &str, highlight: usize, max_width: usize) -> (String, usize, usize) {
    let width = display.width();
    if width <= max_width {
        return (display.to_string(), highlight, width);
    }
    let mut remaining = max_width.saturating_sub(3);
    let mut out = String::new();
    let mut kept = 0;
    for c in display.chars() {
        let w = unicode_width::UnicodeWidthChar::width(c).unwrap_or(0);
        if w <= remaining {
            out.push(c);
            remaining -= w;
            kept += 1;
        } else {
            break;
        }
    }
    out.push_str("...");
    (out, highlight.min(kept), max_width - remaining)
}

/// One menu row (`_get_menu_item_fragments` plus the scrollbar margin).
fn menu_row(c: &Completion, current: bool, menu_width: usize, scrollbar: Option<bool>) -> String {
    let (text, highlight, tw) = trim_display(&c.display, c.highlight, menu_width.saturating_sub(2));
    let (base, matched) = if current {
        (sgr::CURRENT, sgr::MATCH_CURRENT)
    } else {
        (sgr::ITEM, sgr::MATCH)
    };
    let split = byte_of(&text, highlight);
    let mut out = format!("{base} {matched}{}{base}{}", &text[..split], &text[split..]);
    out.push_str(&" ".repeat(menu_width.saturating_sub(1).saturating_sub(tw)));
    match scrollbar {
        Some(true) => out.push_str(&format!("{} ", sgr::SCROLL_BUTTON)),
        Some(false) => out.push_str(&format!("{} ", sgr::SCROLL_BACKGROUND)),
        None => {}
    }
    out.push_str(sgr::RESET);
    out
}

/// The hinter: the suggestion, the completion menu and the toolbar (see the module docs).
pub struct ShellHinter {
    /// The shared state.
    pub shared: SharedRef,
}

impl Hinter for ShellHinter {
    fn handle(
        &mut self,
        line: &str,
        pos: usize,
        _history: &dyn History,
        _use_ansi_coloring: bool,
        _cwd: &str,
    ) -> String {
        let mut s = lock(&self.shared);
        s.observe(Doc::new(line, pos));
        render(&mut s)
    }

    fn complete_hint(&self) -> String {
        let s = lock(&self.shared);
        if s.suggestion_available() && s.doc.at_end() {
            s.suggestion.clone().unwrap_or_default()
        } else {
            String::new()
        }
    }

    /// `escape f` with a suggestion: its first word (`re.split(r"([^\s/]+(?:\s+|/))", text)`).
    fn next_hint_token(&self) -> String {
        let s = lock(&self.shared);
        if !(s.suggestion_available() && s.doc.at_end()) {
            return String::new();
        }
        let text = s.suggestion.clone().unwrap_or_default();
        static WORD: std::sync::LazyLock<regex::Regex> =
            std::sync::LazyLock::new(|| regex::Regex::new(r"[^\s/]+(?:\s+|/)").expect("regex"));
        match WORD.find(&text) {
            Some(m) if m.start() > 0 => text[..m.start()].to_string(),
            Some(m) => m.as_str().to_string(),
            None => text,
        }
    }
}

/// Render the hint for the current state (and stage the menu rows the prompt draws).
pub fn render(s: &mut Shared) -> String {
    let (cols16, rows16) = s.term_size();
    let (cols, rows) = (cols16.max(1) as usize, rows16.max(1) as usize);
    s.above.clear();

    let prompt_plain = remove_ansi(&s.prompt);
    let prompt_lines: Vec<&str> = prompt_plain.split('\n').collect();
    let prompt_last = prompt_lines.last().copied().unwrap_or("");
    let prompt_rows_above: usize = prompt_lines[..prompt_lines.len() - 1]
        .iter()
        .map(|l| 1 + wrap_position(&[l], cols).1)
        .sum();

    if (cols16, rows16) != s.layout.size && s.layout.size != (0, 0) {
        // reedline re-measures the cursor on resize and puts the prompt above it.
        if let Ok((_, row)) = crossterm::cursor::position() {
            let cursor_rows = prompt_rows_above
                + resolve_wrap(wrap_position(&[prompt_last, s.doc.before()], cols), cols).1;
            s.layout.prompt_row = (row as usize).saturating_sub(cursor_rows) as u16;
        }
        s.layout.size = (cols16, rows16);
    }
    if s.layout.cleared {
        s.layout.prompt_row = 0;
        s.layout.cleared = false;
    }

    let mut toolbar = s.toolbar.render(cols).unwrap_or_default();
    if toolbar.len() >= rows {
        // No room for the input above the toolbar.
        toolbar.clear();
    }
    let tb = toolbar.len();

    let searching = s.search.is_some();
    let ghost = if !searching && s.doc.at_end() {
        s.suggestion.clone().filter(|g| !g.is_empty())
    } else {
        None
    };
    let ghost_text = ghost.clone().unwrap_or_default();

    // Rows of the prompt and buffer (with the ghost), as reedline counts them.
    let doc = s.doc.clone();
    let base_input = format!("{prompt_plain}{}{ghost_text}", doc.text);
    let cursor_rows =
        prompt_rows_above + resolve_wrap(wrap_position(&[prompt_last, doc.before()], cols), cols).1;
    let end_rows =
        prompt_rows_above + wrap_position(&[prompt_last, &doc.text, &ghost_text], cols).1;
    let base_required = required_lines(&base_input, cols).max(cursor_rows + 1) + tb;

    // reedline's scroll: content that does not fit below the prompt pushes it up.
    let mut prompt_row = (s.layout.prompt_row as usize).min(rows - 1);
    let remaining = rows - prompt_row;
    if base_required >= rows {
        prompt_row = 0;
    } else if base_required >= remaining {
        prompt_row = prompt_row.saturating_sub(base_required - remaining);
    }
    s.layout.prompt_row = prompt_row as u16;

    // The completion menu (`Float(xcursor=True, ycursor=True)` in the input container).
    let mut below: Vec<String> = Vec::new();
    if let Some(state) = s
        .complete
        .clone()
        .filter(|c| !c.completions.is_empty() && !searching)
    {
        let n = state.completions.len();
        let max_display = state
            .completions
            .iter()
            .map(|c| c.display.width())
            .max()
            .unwrap_or(0);
        let total_width = cols.min(MENU_MIN_WIDTH.max(max_display + 2) + 1);
        let menu_width = total_width.saturating_sub(1);
        let anchor = doc.cursor.min(state.original.cursor);
        let (anchor_col, _) = resolve_wrap(
            wrap_position(
                &[prompt_last, &doc.text[..anchor.min(doc.text.len())]],
                cols,
            ),
            cols,
        );
        let x = if anchor_col + total_width > cols {
            cols.saturating_sub(total_width)
        } else {
            anchor_col
        };
        let container = rows.saturating_sub(tb).saturating_sub(prompt_row) as i64;
        let cursor_y = cursor_rows as i64;
        let mut ypos = cursor_y + 1;
        let mut height = n.min(MENU_MAX_HEIGHT) as i64;
        let mut is_above = false;
        if height > container - ypos {
            if container - ypos + 1 >= ypos {
                height = container - ypos;
            } else {
                height = height.min(cursor_y);
                ypos = cursor_y - height;
                is_above = true;
            }
        }
        if !is_above {
            // The rows are printed after the ghost text, which may have wrapped.
            height = height.min(container - (end_rows as i64 + 1));
        } else if ypos + height > prompt_rows_above as i64 {
            // PARITY tolerance: rows above the cursor that are input (wrapped or multi-line
            // buffers) cannot be drawn over; prompt_toolkit would overlay them.
            height = 0;
        }
        let height = height.max(0) as usize;
        if height > 0 {
            let cursor = state.index.unwrap_or(0);
            s.scroll = scroll_to(s.scroll, cursor, height, n);
            let fraction_visible = height as f64 / n as f64;
            let sb_height = (height as f64)
                .min((height as f64 * fraction_visible).max(1.0))
                .floor() as usize;
            let sb_top = (height as f64 * (s.scroll as f64 / n as f64)).floor() as usize;
            let mut rendered: Vec<String> = Vec::new();
            for i in 0..height {
                let ix = s.scroll + i;
                let Some(c) = state.completions.get(ix) else {
                    break;
                };
                let button = sb_top <= i && i <= sb_top + sb_height;
                let scrollbar = (total_width > menu_width).then_some(button);
                rendered.push(menu_row(c, state.index == Some(ix), menu_width, scrollbar));
            }
            if is_above {
                for (i, row) in rendered.into_iter().enumerate() {
                    let line = ypos as usize + i;
                    s.above.push((line, format!("\x1b[{}G{row}", x + 1)));
                }
            } else {
                below = rendered
                    .into_iter()
                    .map(|row| format!("\x1b[{}G{row}", x + 1))
                    .collect();
            }
        }
    }

    let mut hint = String::new();
    if let Some(g) = &ghost {
        hint.push_str(sgr::SUGGESTION);
        hint.push_str(g);
        hint.push_str(sgr::RESET);
    }
    for row in &below {
        hint.push_str("\r\n");
        hint.push_str(row);
    }
    if tb > 0 {
        hint.push_str("\r\n");
        for (i, row) in toolbar.iter().enumerate() {
            hint.push_str(&format!("\x1b[{};1H{row}", rows - tb + i + 1));
        }
    }
    hint
}

#[cfg(test)]
mod tests {
    use super::*;

    fn comp(text: &str, start: i64) -> Completion {
        Completion {
            text: text.into(),
            start_position: start,
            display: text.into(),
            highlight: (-start) as usize,
        }
    }

    #[test]
    fn completion_state_applies_and_restores() {
        let state = CompleteState {
            original: Doc::new("show p", 6),
            completions: vec![comp("packages", -1), comp("pipes", -1)],
            index: Some(1),
        };
        assert_eq!(state.new_doc(), Doc::new("show pipes", 10));
        assert!(does_nothing(&Doc::new("show", 4), &comp("show", -4)));
        assert_eq!(
            common_suffix(&Doc::new("sh", 2), &[comp("sh", -2), comp("show", -2)]),
            ""
        );
        assert_eq!(
            common_suffix(
                &Doc::new("show pa", 7),
                &[comp("packages", -2), comp("partitions", -2)]
            ),
            ""
        );
        assert_eq!(
            common_suffix(
                &Doc::new("show c", 6),
                &[comp("config", -1), comp("connectors", -1)]
            ),
            "on"
        );
    }

    fn shared(size: (u16, u16)) -> Shared {
        let mut s = Shared::new(CompletionData::default(), DateContext::system());
        s.size_override = Some(size);
        s.layout.size = size;
        s.prompt = "\n [ x ] > ".into();
        s.toolbar = ToolbarModel {
            enabled: true,
            instance: "sql:main".into(),
            repo: "api:mrsm".into(),
            executor: "local".into(),
            ..ToolbarModel::default()
        };
        s
    }

    #[test]
    fn rendering_survives_tiny_terminals() {
        for size in [(1, 1), (5, 1), (10, 2), (20, 3), (40, 8)] {
            let mut s = shared(size);
            s.observe(Doc::new("sh", 2));
            s.after_insert();
            let hint = render(&mut s);
            assert!(size.1 > 2 || !hint.contains("1H"), "{size:?}: {hint:?}");
        }
    }

    #[test]
    fn menu_opens_below_with_room_and_above_at_the_bottom() {
        let mut s = shared((100, 30));
        s.layout.prompt_row = 5;
        s.observe(Doc::new("sh", 2));
        s.after_insert();
        let hint = render(&mut s);
        assert_eq!(hint.matches("\r\n").count(), 3, "{hint:?}");
        assert!(s.above.is_empty());
        // The toolbar sits on the last row.
        assert!(hint.contains("\x1b[30;1H"), "{hint:?}");

        let mut s = shared((100, 30));
        s.layout.prompt_row = 27;
        s.observe(Doc::new("sh", 2));
        s.after_insert();
        let hint = render(&mut s);
        assert_eq!(hint.matches("\r\n").count(), 1, "{hint:?}");
        assert_eq!(s.above.len(), 1);
        assert_eq!(s.above[0].0, 0);
    }

    #[test]
    fn menu_scroll_follows_selection() {
        assert_eq!(scroll_to(0, 16, 16, 24), 2);
        assert_eq!(scroll_to(2, 23, 16, 24), 8);
        assert_eq!(scroll_to(8, 0, 16, 24), 0);
    }

    #[test]
    fn wrap_counting() {
        assert_eq!(wrap_position(&["abc"], 10), (3, 0));
        assert_eq!(wrap_position(&["abcdefghij"], 10), (10, 0));
        assert_eq!(wrap_position(&["abcdefghijk"], 10), (1, 1));
        assert_eq!(required_lines("ab\ncd", 10), 2);
    }
}
