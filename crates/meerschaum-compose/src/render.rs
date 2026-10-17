//! A minimal port of the parts of `rich` (v14) that `compose explain` uses, rendering plain text
//! (no ANSI styles): `Text` wrapping and truncation, `Padding`, `Panel` (rounded box with a
//! centered title), and `Table` with `box.MINIMAL`, `show_header=True`, `expand=True`,
//! `collapse_padding=True` and a centered title. Column widths follow `Table._calculate_column_widths`
//! (`_collapse_widths`, `ratio_reduce`, `ratio_distribute`) and wrapping follows
//! `rich._wrap.divide_line`, so the layout matches Python's output cell for cell.
//!
//! Cell widths come from `unicode-width`; rich uses its own table, which agrees for the
//! characters compose prints (ASCII, Latin text, and the status emoji).

use std::sync::LazyLock;

use regex::Regex;
use unicode_width::UnicodeWidthChar;

/// The display width of a character (`get_character_cell_size`).
pub fn char_width(c: char) -> usize {
    if c.is_control() {
        return 0;
    }
    c.width().unwrap_or(0)
}

/// The display width of a string (`cell_len`).
pub fn cell_len(s: &str) -> usize {
    s.chars().map(char_width).sum()
}

fn is_single_cell_widths(s: &str) -> bool {
    s.chars().all(|c| char_width(c) == 1)
}

/// `set_cell_size(text, total)`: crop or pad to exactly `total` cells.
pub fn set_cell_size(text: &str, total: isize) -> String {
    if is_single_cell_widths(text) {
        let size = text.chars().count() as isize;
        if size < total {
            return format!("{text}{}", " ".repeat((total - size) as usize));
        }
        // PARITY: `text[:total]` (a negative `total` drops characters from the end).
        let keep = if total >= 0 {
            total as usize
        } else {
            (size + total).max(0) as usize
        };
        return text.chars().take(keep).collect();
    }
    if total <= 0 {
        return String::new();
    }
    let total = total as usize;
    let cell_size = cell_len(text);
    if cell_size == total {
        return text.to_string();
    }
    if cell_size < total {
        return format!("{text}{}", " ".repeat(total - cell_size));
    }
    let chars: Vec<char> = text.chars().collect();
    let (mut start, mut end) = (0usize, chars.len());
    loop {
        let pos = (start + end) / 2;
        let before: String = chars[..(pos + 1).min(chars.len())].iter().collect();
        let before_len = cell_len(&before);
        let last_width = chars.get(pos).map_or(0, |c| char_width(*c));
        if before_len == total + 1 && last_width == 2 {
            let mut s: String = chars[..pos].iter().collect();
            s.push(' ');
            return s;
        }
        if before_len == total {
            return before;
        }
        if before_len > total {
            end = pos;
        } else {
            start = pos;
        }
    }
}

/// `chop_cells(text, width)`.
pub fn chop_cells(text: &str, width: usize) -> Vec<String> {
    let mut lines: Vec<String> = vec![String::new()];
    let mut total = 0;
    for c in text.chars() {
        let w = char_width(c);
        if total + w > width {
            lines.push(c.to_string());
            total = w;
        } else {
            lines.last_mut().expect("non-empty").push(c);
            total += w;
        }
    }
    lines
}

static RE_WORD: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\s*\S+\s*").expect("valid regex"));
static RE_TRAILING_WS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\s+$").expect("valid regex"));

/// `divide_line(text, width, fold)`: the character offsets to break a line at.
pub fn divide_line(text: &str, width: usize, fold: bool) -> Vec<usize> {
    let mut breaks = Vec::new();
    let mut cell_offset = 0usize;
    let mut pos = 0usize;
    while let Some(m) = RE_WORD.find_at(text, pos) {
        if m.start() != pos {
            break;
        }
        let word = m.as_str();
        // Offsets are in characters, like Python string indices.
        let mut start = text[..m.start()].chars().count();
        let word_length = cell_len(word.trim_end_matches(char::is_whitespace));
        let remaining = width as isize - cell_offset as isize;
        if remaining >= word_length as isize {
            cell_offset += cell_len(word);
        } else if word_length > width {
            if fold {
                let folded = chop_cells(word, width);
                let n = folded.len();
                for (i, line) in folded.iter().enumerate() {
                    if start > 0 {
                        breaks.push(start);
                    }
                    if i == n - 1 {
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
        pos = m.end();
        if m.end() == m.start() {
            break;
        }
    }
    breaks
}

/// `overflow` methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Overflow {
    /// Fold long words onto the next line.
    Fold,
    /// Crop.
    Crop,
    /// Crop with `…`.
    Ellipsis,
}

/// `justify` methods used here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Justify {
    /// Left (pad to the width).
    Left,
    /// Center.
    Center,
}

fn truncate(line: &str, max_width: usize, overflow: Overflow, pad: bool) -> String {
    let length = cell_len(line);
    let mut out = line.to_string();
    if length > max_width {
        out = match overflow {
            Overflow::Ellipsis => {
                let mut s = set_cell_size(line, max_width as isize - 1);
                s.push('…');
                s
            }
            _ => set_cell_size(line, max_width as isize),
        };
    }
    if pad && length < max_width {
        out.push_str(&" ".repeat(max_width - length));
    }
    out
}

fn rstrip_end(line: &str, size: usize) -> String {
    let len = line.chars().count();
    if len > size {
        let excess = len - size;
        if let Some(m) = RE_TRAILING_WS.find(line) {
            let ws = line[m.start()..].chars().count();
            let crop = ws.min(excess);
            return line.chars().take(len - crop).collect();
        }
    }
    line.to_string()
}

/// `Text.wrap(...)` for plain text: the wrapped (and justified) lines.
pub fn wrap(
    text: &str,
    width: usize,
    justify: Justify,
    overflow: Overflow,
    no_wrap: bool,
) -> Vec<String> {
    let mut lines = Vec::new();
    for line in text.split('\n') {
        let line = expand_tabs(line, 8);
        let mut new_lines: Vec<String> = if no_wrap {
            vec![line.clone()]
        } else {
            let offsets = divide_line(&line, width, overflow == Overflow::Fold);
            divide(&line, &offsets)
        };
        for l in new_lines.iter_mut() {
            *l = rstrip_end(l, width);
        }
        for l in new_lines.iter_mut() {
            *l = match justify {
                Justify::Left => truncate(l, width, overflow, true),
                Justify::Center => {
                    let stripped = l.trim_end_matches(char::is_whitespace).to_string();
                    let t = truncate(&stripped, width, overflow, false);
                    let left = width.saturating_sub(cell_len(&t)) / 2;
                    let t = format!("{}{t}", " ".repeat(left));
                    let right = width.saturating_sub(cell_len(&t));
                    format!("{t}{}", " ".repeat(right))
                }
            };
        }
        for l in new_lines.iter_mut() {
            *l = truncate(l, width, overflow, false);
        }
        lines.extend(new_lines);
    }
    lines
}

fn expand_tabs(line: &str, tab_size: usize) -> String {
    if !line.contains('\t') {
        return line.to_string();
    }
    let mut out = String::new();
    let mut col = 0;
    for c in line.chars() {
        if c == '\t' {
            let spaces = tab_size - (col % tab_size);
            out.push_str(&" ".repeat(spaces));
            col += spaces;
        } else {
            out.push(c);
            col += char_width(c);
        }
    }
    out
}

fn divide(line: &str, offsets: &[usize]) -> Vec<String> {
    let chars: Vec<char> = line.chars().collect();
    let mut bounds = vec![0];
    bounds.extend(offsets.iter().copied());
    bounds.push(chars.len());
    bounds
        .windows(2)
        .map(|w| chars[w[0]..w[1].max(w[0])].iter().collect())
        .collect()
}

fn pad_to(line: &str, width: usize) -> String {
    let len = cell_len(line);
    if len >= width {
        set_cell_size(line, width as isize)
    } else {
        format!("{line}{}", " ".repeat(width - len))
    }
}

/// A `(minimum, maximum)` width measurement (`rich.measure.Measurement`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Measurement {
    /// Minimum width.
    pub minimum: usize,
    /// Maximum width.
    pub maximum: usize,
}

impl Measurement {
    fn normalize(self) -> Measurement {
        let minimum = self.minimum.min(self.maximum);
        Measurement {
            minimum,
            maximum: minimum.max(self.maximum),
        }
    }

    fn with_maximum(self, width: usize) -> Measurement {
        Measurement {
            minimum: self.minimum.min(width),
            maximum: self.maximum.min(width),
        }
    }
}

/// A renderable used by the explain table.
#[derive(Debug, Clone, PartialEq)]
pub enum Renderable {
    /// Plain text (`Text`). `no_wrap` is set for `rich.json.JSON`.
    Text {
        /// The text.
        plain: String,
        /// `no_wrap`.
        no_wrap: bool,
    },
    /// A `Panel` around a text with a title (rounded box, `padding=(0, 1)`, `expand=True`).
    Panel {
        /// The panel body.
        body: String,
        /// The title.
        title: String,
    },
}

impl Renderable {
    /// A text renderable.
    pub fn text(plain: impl Into<String>) -> Renderable {
        Renderable::Text {
            plain: plain.into(),
            no_wrap: false,
        }
    }

    /// `rich.json.JSON` text (`no_wrap=True`).
    pub fn json(plain: impl Into<String>) -> Renderable {
        Renderable::Text {
            plain: plain.into(),
            no_wrap: true,
        }
    }
}

fn measure_text(plain: &str) -> Measurement {
    let max = plain.lines().map(cell_len).max().unwrap_or(0);
    let min = plain.split_whitespace().map(cell_len).max().unwrap_or(max);
    Measurement {
        minimum: min,
        maximum: max,
    }
}

/// `Measurement.get(console, options, renderable)` at a maximum width.
fn measure(r: &Renderable, max_width: usize) -> Measurement {
    if max_width < 1 {
        return Measurement {
            minimum: 0,
            maximum: 0,
        };
    }
    let raw = match r {
        Renderable::Text { plain, .. } => measure_text(plain),
        Renderable::Panel { body, title } => {
            // Panel.__rich_measure__: padding (0, 1) plus the border.
            let inner = max_width.saturating_sub(4);
            let body_m = measure_get_text(body, inner);
            let title_m = measure_get_text(&format!(" {title} "), inner);
            let width = body_m.maximum.max(title_m.maximum) + 4;
            Measurement {
                minimum: width,
                maximum: width,
            }
        }
    };
    let m = raw.normalize().with_maximum(max_width);
    if m.maximum < 1 {
        return Measurement {
            minimum: 0,
            maximum: 0,
        };
    }
    m.normalize()
}

fn measure_get_text(plain: &str, max_width: usize) -> Measurement {
    measure(&Renderable::text(plain), max_width)
}

/// `Padding(renderable, (0, right, 0, left)).__rich_measure__`.
fn measure_padded(r: &Renderable, left: usize, right: usize, max_width: usize) -> Measurement {
    if max_width < 1 {
        return Measurement {
            minimum: 0,
            maximum: 0,
        };
    }
    let extra = left + right;
    if (max_width as isize) - (extra as isize) < 1 {
        return Measurement {
            minimum: max_width,
            maximum: max_width,
        };
    }
    let m = measure(r, max_width);
    Measurement {
        minimum: m.minimum + extra,
        maximum: m.maximum + extra,
    }
    .with_maximum(max_width)
    .normalize()
    .with_maximum(max_width)
}

/// Render a renderable into lines of exactly `width` cells (`console.render_lines(pad=True)`).
fn render_lines(r: &Renderable, width: usize, overflow: Overflow) -> Vec<String> {
    match r {
        Renderable::Text { plain, no_wrap } => {
            wrap(plain, width, Justify::Left, overflow, *no_wrap)
                .into_iter()
                .map(|l| pad_to(&l, width))
                .collect()
        }
        Renderable::Panel { body, title } => {
            let title_text = format!(" {title} ");
            let mut child_width = width.saturating_sub(2);
            child_width = (width.saturating_sub(2)).min(child_width.max(cell_len(&title_text) + 2));
            let panel_width = child_width + 2;
            let inner = render_padded(&Renderable::text(body.clone()), 1, 1, child_width, overflow);
            let mut out = Vec::new();
            if panel_width <= 4 {
                out.push(format!("╭{}╮", "─".repeat(panel_width.saturating_sub(2))));
            } else {
                let avail = panel_width - 4;
                let t = truncate(&title_text, avail, Overflow::Fold, false);
                let excess = avail.saturating_sub(cell_len(&t));
                let left = excess / 2;
                out.push(format!(
                    "╭─{}{}{}─╮",
                    "─".repeat(left),
                    t,
                    "─".repeat(excess - left)
                ));
            }
            for line in inner {
                out.push(format!("│{line}│"));
            }
            out.push(format!("╰{}╯", "─".repeat(panel_width.saturating_sub(2))));
            out.into_iter().map(|l| pad_to(&l, width)).collect()
        }
    }
}

fn render_padded(
    r: &Renderable,
    left: usize,
    right: usize,
    width: usize,
    overflow: Overflow,
) -> Vec<String> {
    let inner_width = width.saturating_sub(left + right);
    render_lines(r, inner_width, overflow)
        .into_iter()
        .map(|l| format!("{}{}{}", " ".repeat(left), l, " ".repeat(right)))
        .collect()
}

/// `ratio_reduce(total, ratios, maximums, values)`.
pub fn ratio_reduce(
    total: usize,
    ratios: &[usize],
    maximums: &[usize],
    values: &[usize],
) -> Vec<usize> {
    let ratios: Vec<usize> = ratios
        .iter()
        .zip(maximums)
        .map(|(r, m)| if *m > 0 { *r } else { 0 })
        .collect();
    let mut total_ratio: usize = ratios.iter().sum();
    if total_ratio == 0 {
        return values.to_vec();
    }
    let mut total_remaining = total as f64;
    let mut out = Vec::new();
    for ((ratio, maximum), value) in ratios.iter().zip(maximums).zip(values) {
        if *ratio > 0 && total_ratio > 0 {
            let distributed = (*maximum as f64).min(py_round(
                *ratio as f64 * total_remaining / total_ratio as f64,
            ));
            out.push((*value as f64 - distributed) as usize);
            total_remaining -= distributed;
            total_ratio -= ratio;
        } else {
            out.push(*value);
        }
    }
    out
}

/// Python's `round()` (banker's rounding).
fn py_round(x: f64) -> f64 {
    let r = x.round();
    if (x - x.trunc()).abs() == 0.5 {
        2.0 * (x / 2.0).round()
    } else {
        r
    }
}

/// `ratio_distribute(total, ratios, minimums=None)`.
pub fn ratio_distribute(total: usize, ratios: &[usize]) -> Vec<usize> {
    let mut total_ratio: usize = ratios.iter().sum();
    let mut total_remaining = total as f64;
    let mut out = Vec::new();
    for ratio in ratios {
        let distributed = if total_ratio > 0 {
            (*ratio as f64 * total_remaining / total_ratio as f64)
                .ceil()
                .max(0.0)
        } else {
            total_remaining
        };
        out.push(distributed as usize);
        total_ratio -= ratio;
        total_remaining -= distributed;
    }
    out
}

fn collapse_widths(mut widths: Vec<usize>, wrapable: &[bool], max_width: usize) -> Vec<usize> {
    let mut total: usize = widths.iter().sum();
    let mut excess = total as isize - max_width as isize;
    if wrapable.iter().any(|w| *w) {
        while total > 0 && excess > 0 {
            let max_column = widths
                .iter()
                .zip(wrapable)
                .filter(|(_, w)| **w)
                .map(|(x, _)| *x)
                .max()
                .unwrap_or(0);
            let second = widths
                .iter()
                .zip(wrapable)
                .map(|(x, w)| if *w && *x != max_column { *x } else { 0 })
                .max()
                .unwrap_or(0);
            let diff = max_column - second;
            let ratios: Vec<usize> = widths
                .iter()
                .zip(wrapable)
                .map(|(x, w)| usize::from(*x == max_column && *w))
                .collect();
            if !ratios.iter().any(|r| *r > 0) || diff == 0 {
                break;
            }
            let max_reduce = vec![(excess as usize).min(diff); widths.len()];
            widths = ratio_reduce(excess as usize, &ratios, &max_reduce, &widths);
            total = widths.iter().sum();
            excess = total as isize - max_width as isize;
        }
    }
    widths
}

/// A table row: cells and whether it ends a section.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    /// The cells (missing cells are empty).
    pub cells: Vec<Renderable>,
    /// `end_section`.
    pub end_section: bool,
}

/// A `rich.table.Table(title=..., box=box.MINIMAL, show_header=True, expand=True,
/// collapse_padding=True)` with default columns.
#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    /// The title (rendered centered above the table).
    pub title: String,
    /// The column headers.
    pub headers: Vec<String>,
    /// The rows.
    pub rows: Vec<Row>,
}

impl Table {
    fn padding(&self, col: usize) -> (usize, usize) {
        // padding (0, 1); collapse_padding drops the left padding after the first column.
        if col == 0 { (1, 1) } else { (0, 1) }
    }

    fn column_cells(&self, col: usize) -> Vec<Renderable> {
        let mut cells = vec![Renderable::text(self.headers[col].clone())];
        for row in &self.rows {
            cells.push(
                row.cells
                    .get(col)
                    .cloned()
                    .unwrap_or_else(|| Renderable::text("")),
            );
        }
        cells
    }

    fn measure_column(&self, col: usize, max_width: usize) -> Measurement {
        if max_width < 1 {
            return Measurement {
                minimum: 0,
                maximum: 0,
            };
        }
        let (left, right) = self.padding(col);
        let ms: Vec<Measurement> = self
            .column_cells(col)
            .iter()
            .map(|c| measure_padded(c, left, right, max_width))
            .collect();
        Measurement {
            minimum: ms.iter().map(|m| m.minimum).max().unwrap_or(1),
            maximum: ms.iter().map(|m| m.maximum).max().unwrap_or(max_width),
        }
        .with_maximum(max_width)
    }

    fn column_widths(&self, max_width: usize) -> Vec<usize> {
        let n = self.headers.len();
        let ranges: Vec<Measurement> = (0..n).map(|c| self.measure_column(c, max_width)).collect();
        let mut widths: Vec<usize> = ranges
            .iter()
            .map(|r| if r.maximum == 0 { 1 } else { r.maximum })
            .collect();
        let mut table_width: usize = widths.iter().sum();
        if table_width > max_width {
            widths = collapse_widths(widths, &vec![true; n], max_width);
            table_width = widths.iter().sum();
            if table_width > max_width {
                let excess = table_width - max_width;
                widths = ratio_reduce(excess, &vec![1; n], &widths.clone(), &widths);
            }
            widths = (0..n)
                .map(|c| self.measure_column(c, widths[c]).maximum)
                .collect();
            // PARITY: rich does not recompute `table_width` after re-measuring the columns
            // (table.py `_calculate_column_widths`).
        }
        if table_width < max_width {
            let pads = ratio_distribute(max_width - table_width, &widths);
            widths = widths.iter().zip(pads).map(|(w, p)| w + p).collect();
        }
        widths
    }

    /// Render the table at a console width, as `console.print(table)` writes it.
    pub fn render(&self, console_width: usize) -> String {
        let n = self.headers.len();
        let extra = 2 + n.saturating_sub(1);
        let widths = self.column_widths(console_width.saturating_sub(extra));
        let table_width: usize = widths.iter().sum::<usize>() + extra;
        let mut out = String::new();

        if !self.title.is_empty() {
            for line in wrap(
                &self.title,
                table_width,
                Justify::Center,
                Overflow::Fold,
                false,
            ) {
                out.push_str(&line);
                out.push('\n');
            }
        }

        let top: Vec<String> = widths.iter().map(|w| " ".repeat(*w)).collect();
        out.push(' ');
        out.push_str(&top.join("╷"));
        out.push_str(" \n");

        let mut all_rows: Vec<(Vec<Renderable>, bool)> = vec![(
            self.headers
                .iter()
                .map(|h| Renderable::text(h.clone()))
                .collect(),
            false,
        )];
        for row in &self.rows {
            let mut cells = row.cells.clone();
            cells.resize(n, Renderable::text(""));
            all_rows.push((cells, row.end_section));
        }
        let total_rows = all_rows.len();
        for (index, (cells, end_section)) in all_rows.iter().enumerate() {
            let rendered: Vec<Vec<String>> = cells
                .iter()
                .enumerate()
                .map(|(c, cell)| {
                    let (left, right) = self.padding(c);
                    render_padded(cell, left, right, widths[c], Overflow::Ellipsis)
                })
                .collect();
            let height = rendered.iter().map(Vec::len).max().unwrap_or(1).max(1);
            for line_no in 0..height {
                out.push(' ');
                let parts: Vec<String> = rendered
                    .iter()
                    .enumerate()
                    .map(|(c, lines)| {
                        lines
                            .get(line_no)
                            .cloned()
                            .unwrap_or_else(|| " ".repeat(widths[c]))
                    })
                    .collect();
                out.push_str(&parts.join("│"));
                out.push_str(" \n");
            }
            let last = index == total_rows - 1;
            if index == 0 {
                let segs: Vec<String> = widths.iter().map(|w| "─".repeat(*w)).collect();
                out.push('╶');
                out.push_str(&segs.join("┼"));
                out.push_str("╴\n");
            } else if *end_section && !last {
                let segs: Vec<String> = widths.iter().map(|w| "─".repeat(*w)).collect();
                out.push('╶');
                out.push_str(&segs.join("┼"));
                out.push_str("╴\n");
            }
        }

        let bottom: Vec<String> = widths.iter().map(|w| " ".repeat(*w)).collect();
        out.push(' ');
        out.push_str(&bottom.join("╵"));
        out.push_str(" \n");
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn divides_lines_like_rich() {
        assert_eq!(
            divide_line("Pipe('a', 'b', instance='c')", 20, false),
            vec![15]
        );
        assert_eq!(divide_line("abcdefghij", 4, true), vec![4, 8]);
        assert_eq!(set_cell_size("abc", 2), "ab");
        assert_eq!(set_cell_size("⭕x", 1), " ");
        assert_eq!(
            chop_cells("abcdef", 4),
            vec!["abcd".to_string(), "ef".to_string()]
        );
    }

    #[test]
    fn ratio_helpers_match_rich() {
        assert_eq!(ratio_distribute(10, &[1, 1, 1]), vec![4, 3, 3]);
        assert_eq!(ratio_reduce(3, &[1, 1], &[5, 5], &[10, 10]), vec![8, 9]);
    }
}
