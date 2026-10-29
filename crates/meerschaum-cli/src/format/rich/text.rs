//! `rich.text.Text`: styled text with spans, word wrapping (`rich._wrap.divide_line`) and
//! ANSI decoding (`rich.ansi.AnsiDecoder`).

use std::sync::LazyLock;

use regex::Regex;

use super::cells::{cell_len, chop_cells, set_cell_size};
use super::console::{Console, Justify, Overflow, RenderOptions};
use super::segment::Segment;
use super::style::{Color, Style, StyleRef};

/// `Span(start, end, style)` in character offsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Span {
    /// Start offset.
    pub start: usize,
    /// End offset.
    pub end: usize,
    /// The style.
    pub style: StyleRef,
}

/// `rich.text.Text`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Text {
    chars: Vec<char>,
    /// The spans.
    pub spans: Vec<Span>,
    /// The base style.
    pub style: StyleRef,
    /// `justify`.
    pub justify: Option<Justify>,
    /// `overflow`.
    pub overflow: Option<Overflow>,
    /// `no_wrap`.
    pub no_wrap: Option<bool>,
    /// `end`.
    pub end: String,
}

impl Default for Text {
    fn default() -> Self {
        Text::new("", StyleRef::default())
    }
}

/// rich's `strip_control_codes` (BEL, BS, VT, FF, CR).
fn strip_control_codes(text: &str) -> String {
    text.chars()
        .filter(|c| !matches!(*c as u32, 7 | 8 | 11 | 12 | 13))
        .collect()
}

static RE_WORD: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\s*\S+\s*").unwrap());

impl Text {
    /// `Text(text, style=style)`.
    pub fn new(text: &str, style: impl Into<StyleRef>) -> Text {
        Text {
            chars: strip_control_codes(text).chars().collect(),
            spans: Vec::new(),
            style: style.into(),
            justify: None,
            overflow: None,
            no_wrap: None,
            end: "\n".into(),
        }
    }

    /// `Text(text)`.
    pub fn plain_text(text: &str) -> Text {
        Text::new(text, "")
    }

    /// `text.plain`.
    pub fn plain(&self) -> String {
        self.chars.iter().collect()
    }

    /// `len(text)`.
    pub fn len(&self) -> usize {
        self.chars.len()
    }

    /// `not text`.
    pub fn is_empty(&self) -> bool {
        self.chars.is_empty()
    }

    /// `text.cell_len`.
    pub fn cell_len(&self) -> usize {
        cell_len(&self.plain())
    }

    fn slice(&self, start: usize, end: usize) -> String {
        self.chars[start.min(self.len())..end.min(self.len())]
            .iter()
            .collect()
    }

    /// The `plain` setter: replace the text and trim spans beyond the new end.
    pub fn set_plain(&mut self, new_text: &str) {
        let sanitized: Vec<char> = strip_control_codes(new_text).chars().collect();
        if sanitized == self.chars {
            return;
        }
        let old_length = self.chars.len();
        self.chars = sanitized;
        if old_length > self.chars.len() {
            self.trim_spans();
        }
    }

    fn trim_spans(&mut self) {
        let max = self.chars.len();
        self.spans = self
            .spans
            .iter()
            .filter(|s| s.start < max)
            .map(|s| Span {
                start: s.start,
                end: s.end.min(max),
                style: s.style.clone(),
            })
            .collect();
    }

    /// `blank_copy(plain)`.
    pub fn blank_copy(&self, plain: &str) -> Text {
        Text {
            chars: plain.chars().collect(),
            spans: Vec::new(),
            style: self.style.clone(),
            justify: self.justify,
            overflow: self.overflow,
            no_wrap: self.no_wrap,
            end: self.end.clone(),
        }
    }

    /// `text.append(str, style)`.
    pub fn append_str(&mut self, text: &str, style: impl Into<StyleRef>) -> &mut Self {
        let sanitized: Vec<char> = strip_control_codes(text).chars().collect();
        if !text.is_empty() {
            let style = style.into();
            let offset = self.chars.len();
            let len = sanitized.len();
            if style.is_set() {
                self.spans.push(Span {
                    start: offset,
                    end: offset + len,
                    style,
                });
            }
            self.chars.extend(sanitized);
        }
        self
    }

    /// `text.append(other_text)`.
    pub fn append_text(&mut self, other: &Text) -> &mut Self {
        if !other.is_empty() {
            let offset = self.chars.len();
            if other.style.is_set() {
                self.spans.push(Span {
                    start: offset,
                    end: offset + other.len(),
                    style: other.style.clone(),
                });
            }
            self.chars.extend(other.chars.iter().copied());
            self.spans.extend(other.spans.iter().map(|s| Span {
                start: s.start + offset,
                end: s.end + offset,
                style: s.style.clone(),
            }));
        }
        self
    }

    /// `self + other`.
    pub fn plus(&self, other: &Text) -> Text {
        let mut out = self.clone();
        out.append_text(other);
        out
    }

    /// `self + "string"`.
    pub fn plus_str(&self, other: &str) -> Text {
        let mut out = self.clone();
        out.append_str(other, "");
        out
    }

    /// `Text.assemble(*parts, no_wrap=..., end=...)` with `(text, style)` parts.
    pub fn assemble(parts: Vec<Text>) -> Text {
        let mut out = Text::plain_text("");
        for p in parts {
            out.append_text(&p);
        }
        out
    }

    /// `text.stylize(style, start, end)`.
    pub fn stylize(&mut self, style: impl Into<StyleRef>, start: usize, end: Option<usize>) {
        let style = style.into();
        if !style.is_set() {
            return;
        }
        let length = self.len();
        let end = end.unwrap_or(length);
        if start >= length || end <= start {
            return;
        }
        self.spans.push(Span {
            start,
            end: end.min(length),
            style,
        });
    }

    /// `text.stylize_before(style)`.
    pub fn stylize_before(&mut self, style: impl Into<StyleRef>) {
        let style = style.into();
        if !style.is_set() || self.is_empty() {
            return;
        }
        let end = self.len();
        self.spans.insert(
            0,
            Span {
                start: 0,
                end,
                style,
            },
        );
    }

    /// `text.join(lines)` with this text as the separator.
    pub fn join(&self, lines: &[Text]) -> Text {
        let mut new_text = self.blank_copy("");
        let mut parts: Vec<&Text> = Vec::new();
        if !self.is_empty() {
            for (i, line) in lines.iter().enumerate() {
                parts.push(line);
                if i + 1 < lines.len() {
                    parts.push(self);
                }
            }
        } else {
            parts.extend(lines.iter());
        }
        let mut offset = 0usize;
        for text in parts {
            new_text.chars.extend(text.chars.iter().copied());
            if text.style.is_set() {
                new_text.spans.push(Span {
                    start: offset,
                    end: offset + text.len(),
                    style: text.style.clone(),
                });
            }
            new_text.spans.extend(text.spans.iter().map(|s| Span {
                start: offset + s.start,
                end: offset + s.end,
                style: s.style.clone(),
            }));
            offset += text.len();
        }
        new_text
    }

    /// `text.divide(offsets)`.
    pub fn divide(&self, offsets: &[usize]) -> Vec<Text> {
        if offsets.is_empty() {
            return vec![self.clone()];
        }
        let text_length = self.len();
        let mut divide_offsets = vec![0];
        divide_offsets.extend_from_slice(offsets);
        divide_offsets.push(text_length);
        let ranges: Vec<(usize, usize)> = divide_offsets.windows(2).map(|w| (w[0], w[1])).collect();
        let mut lines: Vec<Text> = ranges
            .iter()
            .map(|(s, e)| {
                let mut t = Text::new(&self.slice(*s, *e), self.style.clone());
                t.justify = self.justify;
                t.overflow = self.overflow;
                t
            })
            .collect();
        if self.spans.is_empty() {
            return lines;
        }
        let line_count = ranges.len() as i64;
        for span in &self.spans {
            let (span_start, span_end) = (span.start, span.end);
            let mut lower: i64 = 0;
            let mut upper: i64 = line_count;
            let mut start_line_no = (lower + upper) / 2;
            let mut line_end;
            loop {
                let (ls, le) = ranges[start_line_no as usize];
                line_end = le;
                if span_start < ls {
                    upper = start_line_no - 1;
                } else if span_start > le {
                    lower = start_line_no + 1;
                } else {
                    break;
                }
                start_line_no = (lower + upper) / 2;
            }
            let end_line_no = if span_end < line_end {
                start_line_no
            } else {
                let mut end_line_no = start_line_no;
                lower = start_line_no;
                upper = line_count;
                loop {
                    let (ls, le) = ranges[end_line_no as usize];
                    if span_end < ls {
                        upper = end_line_no - 1;
                    } else if span_end > le {
                        lower = end_line_no + 1;
                    } else {
                        break;
                    }
                    end_line_no = (lower + upper) / 2;
                }
                end_line_no
            };
            for line_no in start_line_no..=end_line_no {
                let (ls, le) = ranges[line_no as usize];
                let new_start = span_start.saturating_sub(ls);
                let new_end = (span_end.saturating_sub(ls)).min(le - ls);
                if new_end > new_start {
                    lines[line_no as usize].spans.push(Span {
                        start: new_start,
                        end: new_end,
                        style: span.style.clone(),
                    });
                }
            }
        }
        lines
    }

    /// `text.split(separator, allow_blank)`.
    pub fn split(&self, separator: char, allow_blank: bool) -> Vec<Text> {
        if !self.chars.contains(&separator) {
            return vec![self.clone()];
        }
        let mut offsets = Vec::new();
        for (i, c) in self.chars.iter().enumerate() {
            if *c == separator {
                offsets.push(i);
                offsets.push(i + 1);
            }
        }
        let sep = separator.to_string();
        let mut lines: Vec<Text> = self
            .divide(&offsets)
            .into_iter()
            .filter(|l| l.plain() != sep)
            .collect();
        if !allow_blank && self.chars.last() == Some(&separator) {
            lines.pop();
        }
        lines
    }

    /// `text.split(separator)` for a multi-character separator (`allow_blank=False` drops a
    /// trailing empty line).
    pub fn split_str(&self, separator: &str, allow_blank: bool) -> Vec<Text> {
        let plain = self.plain();
        if separator.is_empty() || !plain.contains(separator) {
            return vec![self.clone()];
        }
        let sep_len = separator.chars().count();
        let mut offsets = Vec::new();
        for (byte_index, _) in plain.match_indices(separator) {
            let start = plain[..byte_index].chars().count();
            offsets.push(start);
            offsets.push(start + sep_len);
        }
        let mut lines: Vec<Text> = self
            .divide(&offsets)
            .into_iter()
            .filter(|l| l.plain() != separator)
            .collect();
        if !allow_blank && plain.ends_with(separator) {
            lines.pop();
        }
        lines
    }

    /// `text.rstrip()`.
    pub fn rstrip(&mut self) {
        let plain = self.plain();
        let stripped = plain
            .trim_end_matches(|c: char| c.is_whitespace())
            .to_string();
        self.set_plain(&stripped);
    }

    /// `text.right_crop(amount)`.
    pub fn right_crop(&mut self, amount: usize) {
        let max_offset = self.len().saturating_sub(amount);
        self.spans = self
            .spans
            .iter()
            .filter(|s| s.start < max_offset)
            .map(|s| Span {
                start: s.start,
                end: if s.end < max_offset {
                    s.end
                } else {
                    s.end.min(max_offset)
                },
                style: s.style.clone(),
            })
            .collect();
        self.chars.truncate(max_offset);
    }

    /// `text.rstrip_end(size)`.
    pub fn rstrip_end(&mut self, size: usize) {
        let length = self.len();
        if length > size {
            let excess = length - size;
            let ws = self
                .chars
                .iter()
                .rev()
                .take_while(|c| c.is_whitespace())
                .count();
            if ws > 0 {
                self.right_crop(ws.min(excess));
            }
        }
    }

    /// `text.truncate(max_width, overflow, pad)`.
    pub fn truncate(&mut self, max_width: usize, overflow: Option<Overflow>, pad: bool) {
        let overflow = overflow.or(self.overflow).unwrap_or(Overflow::Fold);
        if overflow != Overflow::Ignore {
            let length = cell_len(&self.plain());
            if length > max_width {
                let new = if overflow == Overflow::Ellipsis {
                    format!(
                        "{}…",
                        set_cell_size(&self.plain(), max_width.saturating_sub(1))
                    )
                } else {
                    set_cell_size(&self.plain(), max_width)
                };
                self.set_plain(&new);
            }
            if pad && length < max_width {
                let spaces = max_width - length;
                self.chars.extend(std::iter::repeat_n(' ', spaces));
            }
        }
    }

    /// `text.pad_left(count)`.
    pub fn pad_left(&mut self, count: usize) {
        if count > 0 {
            let mut chars = vec![' '; count];
            chars.extend(self.chars.iter().copied());
            self.chars = chars;
            for s in &mut self.spans {
                s.start += count;
                s.end += count;
            }
        }
    }

    /// `text.pad_right(count)`.
    pub fn pad_right(&mut self, count: usize) {
        if count > 0 {
            self.chars.extend(std::iter::repeat_n(' ', count));
        }
    }

    /// `text.pad(count)`.
    pub fn pad(&mut self, count: usize) {
        if count > 0 {
            self.pad_left(count);
            self.pad_right(count);
        }
    }

    /// `text.highlight_regex(pattern, style_prefix)` for patterns with named groups.
    pub fn highlight_regex(&mut self, re: &fancy_regex::Regex, prefix: &str) {
        let plain = self.plain();
        // Map byte offsets to char offsets.
        let mut byte_to_char = vec![0usize; plain.len() + 1];
        let mut ci = 0;
        for (bi, _) in plain.char_indices() {
            byte_to_char[bi] = ci;
            ci += 1;
        }
        byte_to_char[plain.len()] = ci;
        let names: Vec<Option<&str>> = re.capture_names().collect();
        let mut pos = 0usize;
        while pos <= plain.len() {
            let caps = match re.captures_from_pos(&plain, pos) {
                Ok(Some(c)) => c,
                _ => break,
            };
            let whole = caps.get(0).expect("match");
            for (i, name) in names.iter().enumerate() {
                let Some(name) = name else { continue };
                if let Some(m) = caps.get(i) {
                    let (s, e) = (byte_to_char[m.start()], byte_to_char[m.end()]);
                    if e > s {
                        self.spans.push(Span {
                            start: s,
                            end: e,
                            style: StyleRef::Name(format!("{prefix}{name}")),
                        });
                    }
                }
            }
            pos = if whole.end() == whole.start() {
                // Advance past an empty match by one character.
                match plain[whole.end()..].chars().next() {
                    Some(c) => whole.end() + c.len_utf8(),
                    None => break,
                }
            } else {
                whole.end()
            };
        }
    }

    /// `Text.__rich_measure__`.
    pub fn measure(&self) -> (usize, usize) {
        let plain = self.plain();
        let lines = py_splitlines(&plain);
        let max_text_width = lines.iter().map(|l| cell_len(l)).max().unwrap_or(0);
        let words: Vec<&str> = plain.split_whitespace().collect();
        let min_text_width = if words.is_empty() {
            max_text_width
        } else {
            words.iter().map(|w| cell_len(w)).max().unwrap_or(0)
        };
        (min_text_width, max_text_width)
    }

    /// `text.wrap(console, width, justify, overflow, no_wrap)`.
    pub fn wrap(
        &self,
        width: usize,
        justify: Option<Justify>,
        overflow: Option<Overflow>,
        no_wrap: Option<bool>,
    ) -> Vec<Text> {
        let wrap_justify = justify.or(self.justify).unwrap_or(Justify::Default);
        let wrap_overflow = overflow.or(self.overflow).unwrap_or(Overflow::Fold);
        let no_wrap =
            no_wrap.or(self.no_wrap).unwrap_or(false) || overflow == Some(Overflow::Ignore);
        let mut lines = Vec::new();
        for line in self.split('\n', true) {
            let mut new_lines = if no_wrap {
                if overflow == Some(Overflow::Ignore) {
                    lines.push(line);
                    continue;
                }
                vec![line]
            } else {
                let offsets = divide_line(&line.plain(), width, wrap_overflow == Overflow::Fold);
                let mut new_lines = line.divide(&offsets);
                for l in &mut new_lines {
                    l.rstrip_end(width);
                }
                new_lines
            };
            justify_lines(&mut new_lines, width, wrap_justify, wrap_overflow);
            for l in &mut new_lines {
                l.truncate(width, Some(wrap_overflow), false);
            }
            lines.extend(new_lines);
        }
        lines
    }

    /// `Text.__rich_console__`.
    pub fn render_with(&self, console: &Console, options: &RenderOptions) -> Vec<Segment> {
        let justify = self.justify.or(options.justify).unwrap_or(Justify::Default);
        let overflow = self.overflow.or(options.overflow).unwrap_or(Overflow::Fold);
        let lines = self.wrap(
            options.max_width,
            Some(justify),
            Some(overflow),
            Some(self.no_wrap.unwrap_or(options.no_wrap)),
        );
        let all = Text::plain_text("\n").join(&lines);
        all.render(console, &self.end)
    }

    /// `text.render(console, end)`: segments by style changes.
    pub fn render(&self, _console: &Console, end: &str) -> Vec<Segment> {
        let text = &self.chars;
        let mut out = Vec::new();
        if self.spans.is_empty() {
            out.push(Segment::plain(text.iter().collect::<String>()));
            if !end.is_empty() {
                out.push(Segment::plain(end));
            }
            return out;
        }
        let mut style_map: Vec<Style> = vec![self.style.resolve()];
        style_map.extend(self.spans.iter().map(|s| s.style.resolve()));
        let mut events: Vec<(usize, bool, usize)> = vec![(0, false, 0)];
        for (i, s) in self.spans.iter().enumerate() {
            events.push((s.start, false, i + 1));
        }
        for (i, s) in self.spans.iter().enumerate() {
            events.push((s.end, true, i + 1));
        }
        events.push((text.len(), true, 0));
        events.sort_by_key(|(offset, leaving, _)| (*offset, *leaving));
        let mut stack: Vec<usize> = Vec::new();
        for w in events.windows(2) {
            let (offset, leaving, style_id) = w[0];
            let next_offset = w[1].0;
            if leaving {
                if let Some(pos) = stack.iter().position(|s| *s == style_id) {
                    stack.remove(pos);
                }
            } else {
                stack.push(style_id);
            }
            if next_offset > offset {
                let mut ids = stack.clone();
                ids.sort();
                let mut style = Style::null();
                let mut first = true;
                for id in ids {
                    if first {
                        style = style_map[id];
                        first = false;
                    } else {
                        style = style.add(&style_map[id]);
                    }
                }
                out.push(Segment::new(
                    text[offset..next_offset.min(text.len())]
                        .iter()
                        .collect::<String>(),
                    Some(style),
                ));
            }
        }
        if !end.is_empty() {
            out.push(Segment::plain(end));
        }
        out
    }

    /// `Text.from_ansi(text, style=...)`.
    pub fn from_ansi(text: &str, style: impl Into<StyleRef>) -> Text {
        let mut joiner = Text::new("\n", style);
        joiner.end = "\n".into();
        let mut decoder = AnsiDecoder::default();
        let lines = decoder.decode(text);
        joiner.join(&lines)
    }
}

/// `str.splitlines()` (the line breaks rich's measure splits on).
fn py_splitlines(s: &str) -> Vec<&str> {
    meerschaum_core::pyfmt::py_splitlines(s, false)
}

/// `Lines.justify(console, width, justify, overflow)`.
pub fn justify_lines(lines: &mut [Text], width: usize, justify: Justify, overflow: Overflow) {
    match justify {
        Justify::Left => {
            for l in lines.iter_mut() {
                l.truncate(width, Some(overflow), true);
            }
        }
        Justify::Center => {
            for l in lines.iter_mut() {
                l.rstrip();
                l.truncate(width, Some(overflow), false);
                let len = cell_len(&l.plain());
                l.pad_left(width.saturating_sub(len) / 2);
                let len = cell_len(&l.plain());
                l.pad_right(width.saturating_sub(len));
            }
        }
        Justify::Right => {
            for l in lines.iter_mut() {
                l.rstrip();
                l.truncate(width, Some(overflow), false);
                let len = cell_len(&l.plain());
                l.pad_left(width.saturating_sub(len));
            }
        }
        // `full` justification is not used by Meerschaum's renderables.
        Justify::Default | Justify::Full => {}
    }
}

/// `rich._wrap.divide_line(text, width, fold)`.
pub fn divide_line(text: &str, width: usize, fold: bool) -> Vec<usize> {
    let mut breaks = Vec::new();
    let mut cell_offset = 0usize;
    // Byte offset -> char offset.
    let char_index = |b: usize| text[..b].chars().count();
    let mut pos = 0usize;
    while let Some(m) = RE_WORD.find_at(text, pos) {
        if m.start() != pos {
            break;
        }
        pos = m.end();
        let word = m.as_str();
        let mut start = char_index(m.start());
        let word_length = cell_len(word.trim_end_matches(|c: char| c.is_whitespace()));
        let remaining = width as i64 - cell_offset as i64;
        if remaining >= word_length as i64 {
            cell_offset += cell_len(word);
        } else if word_length > width {
            if fold {
                let folded = chop_cells(word, width);
                let n = folded.len();
                for (i, line) in folded.iter().enumerate() {
                    if start > 0 {
                        breaks.push(start);
                    }
                    if i + 1 == n {
                        cell_offset = cell_len(line);
                    } else {
                        start += line.chars().count();
                    }
                }
            } else {
                if start > 0 {
                    breaks.push(start);
                }
                cell_offset = cell_len(word);
            }
        } else if cell_offset > 0 && start > 0 {
            breaks.push(start);
            cell_offset = cell_len(word);
        }
    }
    breaks
}

const SGR_STYLE_MAP: &[(u32, &str)] = &[
    (1, "bold"),
    (2, "dim"),
    (3, "italic"),
    (4, "underline"),
    (5, "blink"),
    (6, "blink2"),
    (7, "reverse"),
    (8, "conceal"),
    (9, "strike"),
    (21, "underline2"),
    (22, "not dim not bold"),
    (23, "not italic"),
    (24, "not underline"),
    (25, "not blink"),
    (26, "not blink2"),
    (27, "not reverse"),
    (28, "not conceal"),
    (29, "not strike"),
    (30, "color(0)"),
    (31, "color(1)"),
    (32, "color(2)"),
    (33, "color(3)"),
    (34, "color(4)"),
    (35, "color(5)"),
    (36, "color(6)"),
    (37, "color(7)"),
    (39, "default"),
    (40, "on color(0)"),
    (41, "on color(1)"),
    (42, "on color(2)"),
    (43, "on color(3)"),
    (44, "on color(4)"),
    (45, "on color(5)"),
    (46, "on color(6)"),
    (47, "on color(7)"),
    (49, "on default"),
    (51, "frame"),
    (52, "encircle"),
    (53, "overline"),
    (54, "not frame not encircle"),
    (55, "not overline"),
    (90, "color(8)"),
    (91, "color(9)"),
    (92, "color(10)"),
    (93, "color(11)"),
    (94, "color(12)"),
    (95, "color(13)"),
    (96, "color(14)"),
    (97, "color(15)"),
    (100, "on color(8)"),
    (101, "on color(9)"),
    (102, "on color(10)"),
    (103, "on color(11)"),
    (104, "on color(12)"),
    (105, "on color(13)"),
    (106, "on color(14)"),
    (107, "on color(15)"),
];

static RE_ANSI: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?:\x1b[0-?])|(?:\x1b\](.*?)\x1b\\)|(?:\x1b([(@-Z\\-_]|\[[0-?]*[ -/]*[@-~]))")
        .unwrap()
});

/// `rich.ansi.AnsiDecoder`.
#[derive(Debug, Default)]
pub struct AnsiDecoder {
    style: Style,
}

enum Token {
    Plain(String),
    Sgr(String),
    Other,
}

fn ansi_tokenize(text: &str) -> Vec<Token> {
    let mut out = Vec::new();
    let mut position = 0usize;
    for caps in RE_ANSI.captures_iter(text) {
        let m = caps.get(0).expect("match");
        if m.start() > position {
            out.push(Token::Plain(text[position..m.start()].to_string()));
        }
        let osc = caps.get(1).map(|x| x.as_str());
        let sgr = caps.get(2).map(|x| x.as_str());
        match sgr {
            Some(sgr) if !sgr.is_empty() => {
                if sgr == "(" {
                    // Skip the character set designator.
                    position = m.end() + 1;
                    continue;
                }
                if sgr.ends_with('m') {
                    out.push(Token::Sgr(sgr[1..sgr.len() - 1].to_string()));
                }
            }
            _ => {
                if osc.is_some() {
                    out.push(Token::Other);
                }
            }
        }
        position = m.end();
    }
    if position < text.len() {
        out.push(Token::Plain(text[position..].to_string()));
    }
    out
}

impl AnsiDecoder {
    /// `decoder.decode(text)`: one `Text` per line.
    pub fn decode(&mut self, text: &str) -> Vec<Text> {
        // `re.split(r"(?<=\n)", text)`, then `.rstrip("\n")` per piece.
        let mut pieces: Vec<&str> = Vec::new();
        let mut start = 0;
        for (i, c) in text.char_indices() {
            if c == '\n' {
                pieces.push(&text[start..i + 1]);
                start = i + 1;
            }
        }
        pieces.push(&text[start..]);
        pieces
            .into_iter()
            .map(|p| self.decode_line(p.trim_end_matches('\n')))
            .collect()
    }

    /// `decoder.decode_line(line)`.
    pub fn decode_line(&mut self, line: &str) -> Text {
        let line = line.rsplit('\r').next().unwrap_or(line);
        let mut text = Text::plain_text("");
        for token in ansi_tokenize(line) {
            match token {
                Token::Plain(p) => {
                    if self.style.is_null() {
                        text.append_str(&p, "");
                    } else {
                        text.append_str(&p, self.style);
                    }
                }
                Token::Other => {}
                Token::Sgr(sgr) => {
                    let codes: Vec<u32> = sgr
                        .split(';')
                        .filter(|c| c.is_empty() || c.chars().all(|d| d.is_ascii_digit()))
                        .map(|c| {
                            if c.is_empty() {
                                0
                            } else {
                                c.parse::<u32>().unwrap_or(255).min(255)
                            }
                        })
                        .collect();
                    let mut iter = codes.into_iter();
                    while let Some(code) = iter.next() {
                        if code == 0 {
                            self.style = Style::null();
                        } else if let Some((_, def)) =
                            SGR_STYLE_MAP.iter().find(|(c, _)| *c == code)
                        {
                            self.style = self.style.add(&Style::parse(def).unwrap_or_default());
                        } else if code == 38 || code == 48 {
                            let fg = code == 38;
                            let Some(kind) = iter.next() else { break };
                            let color = if kind == 5 {
                                iter.next().map(|n| Color::from_number(n as u8))
                            } else if kind == 2 {
                                match (iter.next(), iter.next(), iter.next()) {
                                    (Some(r), Some(g), Some(b)) => {
                                        Some(Color::Truecolor(r as u8, g as u8, b as u8))
                                    }
                                    _ => None,
                                }
                            } else {
                                None
                            };
                            if let Some(color) = color {
                                let add = if fg {
                                    Style::from_color(Some(color), None)
                                } else {
                                    Style::from_color(None, Some(color))
                                };
                                self.style = self.style.add(&add);
                            }
                        }
                    }
                }
            }
        }
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn divide_line_wraps_words() {
        assert_eq!(divide_line("hello world foo", 11, true), vec![12]);
        assert_eq!(divide_line("abcdefghij", 4, true), vec![4, 8]);
    }

    #[test]
    fn from_ansi_roundtrip() {
        let t = Text::from_ansi("\x1b[1;94mabc\x1b[0m def", "");
        assert_eq!(t.plain(), "abc def");
        assert_eq!(t.spans.len(), 1);
        assert_eq!(t.spans[0].style.resolve().render("x"), "\x1b[1;94mx\x1b[0m");
    }
}
