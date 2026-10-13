//! `rich.box` and `rich.table.Table` (layout, padding, borders and the title).

use super::console::{Console, Justify, Measurement, Overflow, Padding, RenderOptions, Renderable};
use super::segment::{self, Line, Segment};
use super::style::{Style, StyleRef};
use super::text::Text;

/// `rich.box.Box`: eight lines of four characters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoxChars {
    rows: [[char; 4]; 8],
}

impl BoxChars {
    const fn new(s: [&str; 8]) -> BoxChars {
        let mut rows = [[' '; 4]; 8];
        let mut i = 0;
        while i < 8 {
            let b = s[i].as_bytes();
            // Decode four UTF-8 characters at compile time.
            let mut j = 0;
            let mut k = 0;
            while j < 4 {
                let (c, len) = decode_utf8(b, k);
                rows[i][j] = c;
                k += len;
                j += 1;
            }
            i += 1;
        }
        BoxChars { rows }
    }

    fn top(&self) -> [char; 4] {
        self.rows[0]
    }

    /// `box.get_top(widths)`.
    pub fn get_top(&self, widths: &[usize]) -> String {
        let [l, h, d, r] = self.top();
        edge_line(l, h, d, r, widths)
    }

    /// `box.get_bottom(widths)`.
    pub fn get_bottom(&self, widths: &[usize]) -> String {
        let [l, h, d, r] = self.rows[7];
        edge_line(l, h, d, r, widths)
    }

    /// `box.get_row(widths, level, edge)`.
    pub fn get_row(&self, widths: &[usize], level: &str, edge: bool) -> String {
        let [l, h, c, r] = match level {
            "head" => self.rows[2],
            "foot" => self.rows[5],
            "mid" => {
                let m = self.rows[3];
                [m[0], ' ', m[2], m[3]]
            }
            _ => self.rows[4],
        };
        let mut s = String::new();
        if edge {
            s.push(l);
        }
        for (i, w) in widths.iter().enumerate() {
            s.extend(std::iter::repeat_n(h, *w));
            if i + 1 < widths.len() {
                s.push(c);
            }
        }
        if edge {
            s.push(r);
        }
        s
    }

    /// `(head_left, head_right, head_vertical)` etc. for the row kinds 0 (head), 1 (mid), 2 (foot).
    fn verticals(&self, kind: usize) -> (char, char, char) {
        let row = match kind {
            0 => self.rows[1],
            1 => self.rows[3],
            _ => self.rows[6],
        };
        (row[0], row[3], row[2])
    }

    /// `box.top_left`, `box.top`, `box.top_right`.
    pub fn top_chars(&self) -> (char, char, char) {
        (self.rows[0][0], self.rows[0][1], self.rows[0][3])
    }

    /// `box.bottom_left`, `box.bottom`, `box.bottom_right`.
    pub fn bottom_chars(&self) -> (char, char, char) {
        (self.rows[7][0], self.rows[7][1], self.rows[7][3])
    }

    /// `box.mid_left`, `box.mid_right`.
    pub fn mid_chars(&self) -> (char, char) {
        (self.rows[3][0], self.rows[3][3])
    }
}

const fn decode_utf8(b: &[u8], i: usize) -> (char, usize) {
    let b0 = b[i] as u32;
    if b0 < 0x80 {
        (b0 as u8 as char, 1)
    } else if b0 < 0xe0 {
        let c = ((b0 & 0x1f) << 6) | (b[i + 1] as u32 & 0x3f);
        (char_or_space(c), 2)
    } else if b0 < 0xf0 {
        let c = ((b0 & 0x0f) << 12) | ((b[i + 1] as u32 & 0x3f) << 6) | (b[i + 2] as u32 & 0x3f);
        (char_or_space(c), 3)
    } else {
        let c = ((b0 & 0x07) << 18)
            | ((b[i + 1] as u32 & 0x3f) << 12)
            | ((b[i + 2] as u32 & 0x3f) << 6)
            | (b[i + 3] as u32 & 0x3f);
        (char_or_space(c), 4)
    }
}

const fn char_or_space(c: u32) -> char {
    match char::from_u32(c) {
        Some(c) => c,
        None => ' ',
    }
}

fn edge_line(l: char, h: char, d: char, r: char, widths: &[usize]) -> String {
    let mut s = String::new();
    s.push(l);
    for (i, w) in widths.iter().enumerate() {
        s.extend(std::iter::repeat_n(h, *w));
        if i + 1 < widths.len() {
            s.push(d);
        }
    }
    s.push(r);
    s
}

/// `box.ASCII`
pub const ASCII: BoxChars = BoxChars::new([
    "+--+", "| ||", "|-+|", "| ||", "|-+|", "|-+|", "| ||", "+--+",
]);
/// `box.ROUNDED`
pub const ROUNDED: BoxChars = BoxChars::new([
    "╭─┬╮",
    "│ ││",
    "├─┼┤",
    "│ ││",
    "├─┼┤",
    "├─┼┤",
    "│ ││",
    "╰─┴╯",
]);
/// `box.SIMPLE`
pub const SIMPLE: BoxChars = BoxChars::new([
    "    ", "    ", " ── ", "    ", "    ", " ── ", "    ", "    ",
]);
/// `box.MINIMAL`
pub const MINIMAL: BoxChars = BoxChars::new([
    "  ╷ ",
    "  │ ",
    "╶─┼╴",
    "  │ ",
    "╶─┼╴",
    "╶─┼╴",
    "  │ ",
    "  ╵ ",
]);
/// `box.HEAVY_HEAD`
pub const HEAVY_HEAD: BoxChars = BoxChars::new([
    "┏━┳┓",
    "┃ ┃┃",
    "┡━╇┩",
    "│ ││",
    "├─┼┤",
    "├─┼┤",
    "│ ││",
    "└─┴┘",
]);

/// A cell as `Table._get_cells` yields it: style, renderable, and the `Padding` it is wrapped
/// in (every cell is wrapped when the table has any padding).
type CellInfo<'r> = (
    Style,
    &'r dyn Renderable,
    Option<(usize, usize, usize, usize)>,
);

/// `rich.table.Column`.
pub struct Column<'a> {
    /// The header.
    pub header: Text,
    /// `justify` (default `left`).
    pub justify: Justify,
    /// `overflow` (default `ellipsis`).
    pub overflow: Overflow,
    /// `no_wrap`.
    pub no_wrap: bool,
    /// `style`.
    pub style: StyleRef,
    /// `header_style`.
    pub header_style: StyleRef,
    /// `width`.
    pub width: Option<usize>,
    /// The cells.
    pub cells: Vec<Box<dyn Renderable + 'a>>,
}

impl Default for Column<'_> {
    fn default() -> Self {
        Column {
            header: Text::plain_text(""),
            justify: Justify::Left,
            overflow: Overflow::Ellipsis,
            no_wrap: false,
            style: StyleRef::default(),
            header_style: StyleRef::default(),
            width: None,
            cells: Vec::new(),
        }
    }
}

/// `rich.table.Table`.
pub struct Table<'a> {
    /// The columns.
    pub columns: Vec<Column<'a>>,
    /// `title`.
    pub title: Option<Text>,
    /// `box`.
    pub box_chars: Option<BoxChars>,
    /// `show_header`.
    pub show_header: bool,
    /// `show_edge`.
    pub show_edge: bool,
    /// `expand`.
    pub expand: bool,
    /// `padding` as `(top, right, bottom, left)`.
    pub padding: (usize, usize, usize, usize),
    /// `pad_edge`.
    pub pad_edge: bool,
    /// `collapse_padding`.
    pub collapse_padding: bool,
    /// `header_style`.
    pub header_style: StyleRef,
    /// `border_style`.
    pub border_style: StyleRef,
    /// `style`.
    pub style: StyleRef,
    /// `show_lines`: a separator between rows.
    pub show_lines: bool,
    rows: usize,
}

impl<'a> Table<'a> {
    /// `Table()` with rich's defaults.
    pub fn new() -> Table<'a> {
        Table {
            columns: Vec::new(),
            title: None,
            box_chars: Some(HEAVY_HEAD),
            show_header: true,
            show_edge: true,
            expand: false,
            padding: (0, 1, 0, 1),
            pad_edge: true,
            collapse_padding: false,
            header_style: StyleRef::Name("table.header".into()),
            border_style: StyleRef::default(),
            style: StyleRef::Name("none".into()),
            show_lines: false,
            rows: 0,
        }
    }

    /// `Table.grid(padding=..., collapse_padding=..., pad_edge=...)`.
    pub fn grid(
        padding: (usize, usize, usize, usize),
        collapse_padding: bool,
        pad_edge: bool,
    ) -> Table<'a> {
        Table {
            box_chars: None,
            show_header: false,
            show_edge: false,
            padding,
            collapse_padding,
            pad_edge,
            ..Table::new()
        }
    }

    /// `table.add_column()`.
    pub fn add_column(&mut self, column: Column<'a>) {
        let mut column = column;
        for _ in 0..self.rows {
            column.cells.push(Box::new(Text::plain_text("")));
        }
        self.columns.push(column);
    }

    /// `table.add_row(*cells)`; `None` cells become `""`.
    pub fn add_row(&mut self, cells: Vec<Option<Box<dyn Renderable + 'a>>>) {
        let mut cells = cells;
        while cells.len() < self.columns.len() {
            cells.push(None);
        }
        for (index, cell) in cells.into_iter().enumerate() {
            if index == self.columns.len() {
                let mut column = Column::default();
                for _ in 0..self.rows {
                    column.cells.push(Box::new(Text::plain_text("")));
                }
                self.columns.push(column);
            }
            let cell: Box<dyn Renderable + 'a> = match cell {
                Some(c) => c,
                None => Box::new(Text::plain_text("")),
            };
            self.columns[index].cells.push(cell);
        }
        self.rows += 1;
    }

    fn extra_width(&self) -> usize {
        let mut width = 0;
        if self.box_chars.is_some() && self.show_edge {
            width += 2;
        }
        if self.box_chars.is_some() {
            width += self.columns.len().saturating_sub(1);
        }
        width
    }

    fn padding_width(&self, column_index: usize) -> usize {
        let (_, mut right, _, mut left) = self.padding;
        if self.collapse_padding {
            left = 0;
            right = right.abs_diff(left);
        }
        if !self.pad_edge {
            if column_index == 0 {
                left = 0;
            }
            if column_index + 1 == self.columns.len() {
                right = 0;
            }
        }
        left + right
    }

    fn cell_padding(
        &self,
        column_index: usize,
        first_row: bool,
        last_row: bool,
    ) -> (usize, usize, usize, usize) {
        let (mut top, mut right, mut bottom, mut left) = self.padding;
        let first_column = column_index == 0;
        let last_column = column_index + 1 == self.columns.len();
        if self.collapse_padding {
            if !first_column {
                left = left.saturating_sub(right);
            }
            if !last_row {
                bottom = top.saturating_sub(bottom);
            }
        }
        if !self.pad_edge {
            if first_column {
                left = 0;
            }
            if last_column {
                right = 0;
            }
            if first_row {
                top = 0;
            }
            if last_row {
                bottom = 0;
            }
        }
        (top, right, bottom, left)
    }

    /// The cells of a column, header first, with `(style, renderable, padding)`.
    fn column_cells(&self, index: usize) -> Vec<CellInfo<'_>> {
        let column = &self.columns[index];
        let mut raw: Vec<(Style, &dyn Renderable)> = Vec::new();
        if self.show_header {
            let header_style = self
                .header_style
                .resolve()
                .add(&column.header_style.resolve());
            raw.push((header_style, &column.header));
        }
        let cell_style = column.style.resolve();
        for cell in &column.cells {
            raw.push((cell_style, cell.as_ref()));
        }
        let n = raw.len();
        let any_padding = self.padding != (0, 0, 0, 0);
        raw.into_iter()
            .enumerate()
            .map(|(i, (style, r))| {
                let pad = any_padding.then(|| self.cell_padding(index, i == 0, i + 1 == n));
                (style, r, pad)
            })
            .collect()
    }

    fn measure_column(
        &self,
        console: &Console,
        options: &RenderOptions,
        index: usize,
    ) -> Measurement {
        let max_width = options.max_width;
        if max_width < 1 {
            return Measurement::new(0, 0);
        }
        let padding_width = self.padding_width(index);
        if let Some(w) = self.columns[index].width {
            return Measurement::new(w + padding_width, w + padding_width).with_maximum(max_width);
        }
        let mut mins = Vec::new();
        let mut maxs = Vec::new();
        for (_, r, pad) in self.column_cells(index) {
            let m = if let Some(pad) = pad {
                let p = Padding {
                    renderable: r,
                    pad,
                    style: StyleRef::default(),
                };
                Measurement::get(console, options, &p)
            } else {
                Measurement::get(console, options, r)
            };
            mins.push(m.minimum);
            maxs.push(m.maximum);
        }
        Measurement::new(
            mins.iter().copied().max().unwrap_or(1),
            maxs.iter().copied().max().unwrap_or(max_width),
        )
        .with_maximum(max_width)
    }

    fn calculate_column_widths(&self, console: &Console, options: &RenderOptions) -> Vec<usize> {
        let max_width = options.max_width;
        let ranges: Vec<Measurement> = (0..self.columns.len())
            .map(|i| self.measure_column(console, options, i))
            .collect();
        let mut widths: Vec<usize> = ranges
            .iter()
            .map(|r| if r.maximum == 0 { 1 } else { r.maximum })
            .collect();
        let mut table_width: usize = widths.iter().sum();
        if table_width > max_width {
            let wrapable: Vec<bool> = self
                .columns
                .iter()
                .map(|c| c.width.is_none() && !c.no_wrap)
                .collect();
            widths = collapse_widths(&widths, &wrapable, max_width);
            table_width = widths.iter().sum();
            if table_width > max_width {
                let excess = table_width - max_width;
                let n = widths.len();
                widths = ratio_reduce(excess, &vec![1; n], &widths, &widths);
                table_width = widths.iter().sum();
            }
            widths = widths
                .iter()
                .enumerate()
                .map(|(i, w)| {
                    self.measure_column(console, &options.with_width(*w), i)
                        .maximum
                })
                .collect();
        }
        if table_width < max_width && self.expand {
            let pads = ratio_distribute(max_width - table_width, &widths, None);
            widths = widths.iter().zip(pads).map(|(w, p)| w + p).collect();
        }
        widths
    }
}

impl Default for Table<'_> {
    fn default() -> Self {
        Table::new()
    }
}

/// Python's `round()` (half to even) on a float.
fn py_round(x: f64) -> i64 {
    let r = x.round();
    if (x - x.trunc()).abs() == 0.5 {
        let t = x.trunc();
        if (t as i64) % 2 == 0 {
            t as i64
        } else {
            r as i64
        }
    } else {
        r as i64
    }
}

/// `rich._ratio.ratio_reduce(total, ratios, maximums, values)`.
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
    let mut total_remaining = total as i64;
    let mut result = Vec::new();
    for ((ratio, maximum), value) in ratios.iter().zip(maximums).zip(values) {
        if *ratio > 0 && total_ratio > 0 {
            let distributed = (*maximum as i64).min(py_round(
                *ratio as f64 * total_remaining as f64 / total_ratio as f64,
            ));
            result.push((*value as i64 - distributed).max(0) as usize);
            total_remaining -= distributed;
            total_ratio -= ratio;
        } else {
            result.push(*value);
        }
    }
    result
}

/// `rich._ratio.ratio_distribute(total, ratios, minimums)`.
pub fn ratio_distribute(total: usize, ratios: &[usize], minimums: Option<&[usize]>) -> Vec<usize> {
    let ratios: Vec<usize> = match minimums {
        Some(m) => ratios
            .iter()
            .zip(m)
            .map(|(r, mn)| if *mn > 0 { *r } else { 0 })
            .collect(),
        None => ratios.to_vec(),
    };
    let mut total_ratio: usize = ratios.iter().sum();
    if total_ratio == 0 {
        // PARITY: rich asserts `total_ratio > 0`; an all-zero layout keeps its widths.
        return vec![0; ratios.len()];
    }
    let mut total_remaining = total as i64;
    let mins: Vec<usize> = minimums
        .map(<[usize]>::to_vec)
        .unwrap_or(vec![0; ratios.len()]);
    let mut out = Vec::new();
    for (ratio, minimum) in ratios.iter().zip(mins) {
        let distributed = if total_ratio > 0 {
            (minimum as i64)
                .max((*ratio as f64 * total_remaining as f64 / total_ratio as f64).ceil() as i64)
        } else {
            total_remaining
        };
        out.push(distributed.max(0) as usize);
        total_ratio -= ratio;
        total_remaining -= distributed;
    }
    out
}

/// `Table._collapse_widths(widths, wrapable, max_width)`.
fn collapse_widths(widths: &[usize], wrapable: &[bool], max_width: usize) -> Vec<usize> {
    let mut widths = widths.to_vec();
    let mut total: usize = widths.iter().sum();
    let mut excess = total as i64 - max_width as i64;
    if wrapable.iter().any(|w| *w) {
        while total > 0 && excess > 0 {
            let max_column = widths
                .iter()
                .zip(wrapable)
                .filter(|(_, w)| **w)
                .map(|(w, _)| *w)
                .max()
                .unwrap_or(0);
            let second = widths
                .iter()
                .zip(wrapable)
                .map(|(w, a)| if *a && *w != max_column { *w } else { 0 })
                .max()
                .unwrap_or(0);
            let diff = max_column - second;
            let ratios: Vec<usize> = widths
                .iter()
                .zip(wrapable)
                .map(|(w, a)| usize::from(*w == max_column && *a))
                .collect();
            if !ratios.iter().any(|r| *r > 0) || diff == 0 {
                break;
            }
            let max_reduce = vec![(excess as usize).min(diff); widths.len()];
            widths = ratio_reduce(excess as usize, &ratios, &max_reduce, &widths);
            total = widths.iter().sum();
            excess = total as i64 - max_width as i64;
        }
    }
    widths
}

impl Renderable for Table<'_> {
    fn render(&self, console: &Console, options: &RenderOptions) -> Vec<Segment> {
        let mut out = Vec::new();
        if self.columns.is_empty() {
            out.push(Segment::line());
            return out;
        }
        let max_width = options.max_width;
        let extra = self.extra_width();
        let widths = self.calculate_column_widths(
            console,
            &options.with_width(max_width.saturating_sub(extra)),
        );
        let table_width: usize = widths.iter().sum::<usize>() + extra;
        let mut render_options = options.with_width(table_width);
        render_options.highlight = false;
        render_options.height = None;

        if let Some(title) = &self.title {
            // `render_annotation`: a `str` title is styled by the caller (`render_str(title,
            // style=title_style or "table.title")`); a `Text` title is rendered as is.
            let mut o = render_options.clone();
            o.justify = Some(Justify::Center);
            out.extend(console.render(title, &o));
        }
        out.extend(self.render_body(console, &render_options, &widths));
        out
    }

    fn measure(&self, console: &Console, options: &RenderOptions) -> Measurement {
        let max_width = options.max_width;
        let extra = self.extra_width();
        let widths = self.calculate_column_widths(
            console,
            &options.with_width(max_width.saturating_sub(extra)),
        );
        let max = widths.iter().sum::<usize>();
        let ranges: Vec<Measurement> = (0..self.columns.len())
            .map(|i| self.measure_column(console, &options.with_width(max), i))
            .collect();
        let min_w: usize = ranges.iter().map(|r| r.minimum).sum::<usize>() + extra;
        let max_w: usize = ranges.iter().map(|r| r.maximum).sum::<usize>() + extra;
        Measurement::new(min_w, max_w).normalize()
    }
}

impl Table<'_> {
    fn render_body(
        &self,
        console: &Console,
        _options: &RenderOptions,
        widths: &[usize],
    ) -> Vec<Segment> {
        let mut out = Vec::new();
        let table_style = self.style.resolve();
        let border_style = table_style.add(&self.border_style.resolve());
        let border = Some(border_style);
        let columns_cells: Vec<Vec<CellInfo<'_>>> = (0..self.columns.len())
            .map(|i| self.column_cells(i))
            .collect();
        let n_rows = columns_cells.first().map(Vec::len).unwrap_or(0);
        let box_chars = self.box_chars;
        if let Some(b) = &box_chars
            && self.show_edge
        {
            out.push(Segment::new(b.get_top(widths), border));
            out.push(Segment::line());
        }
        #[allow(clippy::needless_range_loop)]
        for row_index in 0..n_rows {
            let first = row_index == 0;
            let last = row_index + 1 == n_rows;
            let header_row = first && self.show_header;
            let mut cells: Vec<Vec<Line>> = Vec::new();
            let mut max_height = 1;
            for (col_index, width) in widths.iter().enumerate() {
                let column = &self.columns[col_index];
                let (style, r, pad) = columns_cells[col_index][row_index];
                let ro = RenderOptions {
                    max_width: *width,
                    justify: Some(column.justify),
                    no_wrap: column.no_wrap,
                    overflow: Some(column.overflow),
                    height: None,
                    highlight: false,
                };
                let lines = if let Some(pad) = pad {
                    let p = Padding {
                        renderable: r,
                        pad,
                        style: StyleRef::default(),
                    };
                    console.render_lines(&p, &ro, Some(style), true)
                } else {
                    console.render_lines(r, &ro, Some(style), true)
                };
                max_height = max_height.max(lines.len());
                cells.push(lines);
            }
            let row_height = cells.iter().map(Vec::len).max().unwrap_or(0);
            let shaped: Vec<Vec<Line>> = cells
                .iter()
                .enumerate()
                .map(|(i, cell)| {
                    let (style, _, _) = columns_cells[i][row_index];
                    let style = Some(style).filter(|s| !s.is_null());
                    let aligned = if header_row {
                        segment::align_bottom(cell, widths[i], row_height, style)
                    } else {
                        segment::align_top(cell, widths[i], row_height, style)
                    };
                    segment::set_shape(&aligned, widths[i], Some(max_height), style)
                })
                .collect();
            if let Some(b) = &box_chars {
                let kind = if first {
                    0
                } else if last {
                    2
                } else {
                    1
                };
                let (left, right, divider) = b.verticals(kind);
                for line_no in 0..max_height {
                    if self.show_edge {
                        out.push(Segment::new(left.to_string(), border));
                    }
                    for (ci, cell) in shaped.iter().enumerate() {
                        out.extend(cell[line_no].iter().cloned());
                        if ci + 1 < shaped.len() {
                            out.push(Segment::new(divider.to_string(), border));
                        }
                    }
                    if self.show_edge {
                        out.push(Segment::new(right.to_string(), border));
                    }
                    out.push(Segment::line());
                }
            } else {
                for line_no in 0..max_height {
                    for cell in &shaped {
                        out.extend(cell[line_no].iter().cloned());
                    }
                    out.push(Segment::line());
                }
            }
            if let Some(b) = &box_chars
                && first
                && self.show_header
            {
                out.push(Segment::new(
                    b.get_row(widths, "head", self.show_edge),
                    border,
                ));
                out.push(Segment::line());
            } else if let Some(b) = &box_chars
                && self.show_lines
                && !last
            {
                out.push(Segment::new(
                    b.get_row(widths, "row", self.show_edge),
                    border,
                ));
                out.push(Segment::line());
            }
        }
        if let Some(b) = &box_chars
            && self.show_edge
        {
            out.push(Segment::new(b.get_bottom(widths), border));
            out.push(Segment::line());
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ratios() {
        assert_eq!(ratio_distribute(10, &[1, 1, 1], None), vec![4, 3, 3]);
        assert_eq!(
            ratio_reduce(3, &[1, 1, 1], &[3, 3, 3], &[10, 10, 10]),
            vec![9, 9, 9]
        );
        assert_eq!(py_round(2.5), 2);
        assert_eq!(py_round(3.5), 4);
        assert_eq!(
            SIMPLE.get_row(&[2, 3], "head", true),
            " ──────── ".replace("────────", "──────")
        );
    }
}
