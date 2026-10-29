//! `show data` for pretty mode: the pandas-style truncated view of a DataFrame (wave 18).
//!
//! v4 printed every row, column and cell (`format_dataframe`, a Markdown pipe table). The
//! truncated view keeps that table and bounds it like `pandas.set_option` does: more than
//! `max_rows` rows show `min_rows` of them (the head and the tail around a `…` row), columns that
//! do not fit the terminal are replaced by one `…` column in the middle, and a cell longer than
//! `max_colwidth` characters ends in `…`. The footer states the true size, and what is shown when
//! it is less.
//!
//! The options come from `formatting:dataframe:{max_rows, min_rows, max_columns, max_colwidth}`
//! (no default config entry: the defaults below apply when a key is absent); `--all` disables
//! every limit.

use meerschaum_core::config::{Config, GetOptions};

use super::tabulate::{Cell, Frame, render_table};

/// The ellipsis shown for dropped rows, dropped columns and cut cells.
pub const ELLIPSIS: &str = "…";

/// `formatting:dataframe:max_rows` default.
pub const DEFAULT_MAX_ROWS: usize = 60;
/// `formatting:dataframe:min_rows` default.
pub const DEFAULT_MIN_ROWS: usize = 10;
/// `formatting:dataframe:max_colwidth` default.
pub const DEFAULT_MAX_COLWIDTH: usize = 50;

/// The truncation limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisplayOptions {
    /// More rows than this are truncated.
    pub max_rows: usize,
    /// The number of rows shown when truncated (head + tail).
    pub min_rows: usize,
    /// A fixed column count (`None`: as many as fit `width`).
    pub max_columns: Option<usize>,
    /// Longer cells are cut (`0`: never).
    pub max_colwidth: usize,
    /// The terminal width the table must fit (`None`: unlimited).
    pub width: Option<usize>,
}

impl DisplayOptions {
    /// No limit at all (`--all`).
    pub fn unlimited() -> DisplayOptions {
        DisplayOptions {
            max_rows: usize::MAX,
            min_rows: usize::MAX,
            max_columns: None,
            max_colwidth: 0,
            width: None,
        }
    }

    /// The `formatting:dataframe:*` keys; `width` is the terminal width to fit (the caller
    /// passes `None` when the output is not a terminal).
    pub fn from_config(config: &Config, width: Option<usize>) -> DisplayOptions {
        // Absent keys are normal (no default entry): never warn, never write a default file.
        let quiet = GetOptions {
            patch: false,
            write_missing: false,
            warn: false,
            ..GetOptions::default()
        };
        let get = |key: &str| {
            let (valid, value) = config.get_with(&["formatting", "dataframe", key], quiet);
            valid
                .then(|| value.as_i64())
                .flatten()
                .and_then(|n| usize::try_from(n).ok())
        };
        let max_rows = get("max_rows").unwrap_or(DEFAULT_MAX_ROWS);
        let min_rows = get("min_rows").unwrap_or(DEFAULT_MIN_ROWS).min(max_rows);
        DisplayOptions {
            max_rows,
            min_rows,
            max_columns: get("max_columns").filter(|n| *n > 0),
            max_colwidth: get("max_colwidth").unwrap_or(DEFAULT_MAX_COLWIDTH),
            width,
        }
    }

    /// Whether `rows` rows are more than the view shows.
    pub fn truncates(&self, rows: usize) -> bool {
        rows > self.max_rows
    }

    /// The rows shown at the head and at the tail of a truncated view.
    pub fn head_tail(&self) -> (usize, usize) {
        let min = self.min_rows.max(1);
        (min.div_ceil(2), min / 2)
    }
}

/// Which rows of the table the frame holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fetched {
    /// Every row (truncated here when over `max_rows`).
    All,
    /// The first `head` rows followed by the last rows.
    HeadTail {
        /// How many of the frame's rows are the head.
        head: usize,
    },
    /// Only the first rows (a table without a datetime axis has no cheap tail).
    HeadOnly,
}

fn slice_rows(frame: &Frame, ranges: &[(usize, usize)]) -> Frame {
    Frame {
        columns: frame
            .columns
            .iter()
            .map(|(name, cells)| {
                let kept = ranges
                    .iter()
                    .flat_map(|(a, b)| cells[(*a).min(cells.len())..(*b).min(cells.len())].iter())
                    .cloned()
                    .collect();
                (name.clone(), kept)
            })
            .collect(),
    }
}

fn cut(cell: &Cell, max: usize) -> Cell {
    match cell {
        Cell::Str(s) if max > 0 && s.chars().count() > max => {
            let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
            out.push_str(ELLIPSIS);
            Cell::Str(out)
        }
        other => other.clone(),
    }
}

/// The columns of `frame` shown for `keep` (left count, right count), with a `…` column between
/// them when columns were dropped.
fn pick_columns(frame: &Frame, left: usize, right: usize, rows: usize) -> Frame {
    let n = frame.columns.len();
    if left + right >= n {
        return frame.clone();
    }
    let mut columns: Vec<(String, Vec<Cell>)> = frame.columns[..left].to_vec();
    columns.push((
        ELLIPSIS.to_string(),
        vec![Cell::Str(ELLIPSIS.to_string()); rows],
    ));
    columns.extend_from_slice(&frame.columns[n - right..]);
    Frame { columns }
}

/// The widest line of a rendered table with the given content widths: `|` and per column
/// ` cell |`.
fn table_width(widths: &[usize]) -> usize {
    1 + widths.iter().map(|w| w + 3).sum::<usize>()
}

/// `(left, right)` columns to show out of `n` so the table fits, given each column's width
/// (`left + right == n` means every column is shown).
fn fit_columns(widths: &[usize], opts: &DisplayOptions) -> (usize, usize) {
    let n = widths.len();
    let fits = |shown: &[usize]| opts.width.is_none_or(|width| table_width(shown) <= width);
    let limit = opts.max_columns.map_or(n, |m| m.min(n));
    if limit >= n && fits(widths) {
        return (n, 0);
    }
    // The `…` column: header and cells are one character, padded to the header width + 2.
    let elided = 3;
    let mut k = limit.min(n - 1);
    loop {
        let (left, right) = (k.div_ceil(2), k / 2);
        let shown: Vec<usize> = widths[..left]
            .iter()
            .copied()
            .chain(std::iter::once(elided))
            .chain(widths[n - right..].iter().copied())
            .collect();
        if fits(&shown) || k <= 2 {
            return (left, right);
        }
        k -= 1;
    }
}

/// The ellipsis row between the head and the tail, aligned like the table's columns.
fn ellipsis_row(widths: &[usize], left: &[bool]) -> String {
    let cells: Vec<String> = widths
        .iter()
        .zip(left)
        .map(|(w, left)| {
            let pad = " ".repeat(w.saturating_sub(1));
            if *left {
                format!(" {ELLIPSIS}{pad} ")
            } else {
                format!(" {pad}{ELLIPSIS} ")
            }
        })
        .collect();
    format!("|{}|", cells.join("|"))
}

/// The truncated view of `frame`, which holds `fetched` of the table's `total_rows` rows.
///
/// With [`Fetched::All`] the frame has every row and is cut to the head and tail when it is
/// longer than `max_rows`; the other layouts say which rows a limited read returned.
pub fn format_dataframe_display(
    frame: &Frame,
    total_rows: usize,
    fetched: Fetched,
    opts: &DisplayOptions,
) -> String {
    let n_cols = frame.columns.len();
    let held = frame.num_rows();
    let (head_n, tail_n) = opts.head_tail();
    // `(rows kept from the frame, where the ellipsis row goes)`.
    let (ranges, gap): (Vec<(usize, usize)>, Option<usize>) = match fetched {
        Fetched::All if opts.truncates(held) => (
            vec![(0, head_n), (held - tail_n.min(held), held)],
            Some(head_n),
        ),
        Fetched::All => (vec![(0, held)], None),
        Fetched::HeadTail { head } => {
            let head = head.min(held);
            let tail = held - head;
            let tail = tail.min(tail_n);
            (vec![(0, head), (held - tail, held)], Some(head))
        }
        Fetched::HeadOnly => {
            let shown = held.min(head_n + tail_n);
            (vec![(0, shown)], (total_rows > shown).then_some(shown))
        }
    };
    let mut rows = slice_rows(frame, &ranges);
    for (_, cells) in &mut rows.columns {
        for cell in cells.iter_mut() {
            *cell = cut(cell, opts.max_colwidth);
        }
    }
    let shown_rows = rows.num_rows();
    // Dropping middle columns needs the width of each one on its own.
    let full = render_table(&rows, shown_rows);
    let (left, right) = fit_columns(&full.widths, opts);
    let table = if left + right >= n_cols {
        full
    } else {
        render_table(&pick_columns(&rows, left, right, shown_rows), shown_rows)
    };
    let shown_cols = if left + right >= n_cols {
        n_cols
    } else {
        left + right
    };
    let mut lines = table.lines.clone();
    if let Some(gap) = gap
        && shown_rows > 0
    {
        let at = (2 + gap).min(lines.len());
        lines.insert(at, ellipsis_row(&table.widths, &table.left));
    }
    let plural = |n: usize| if n != 1 { "s" } else { "" };
    let mut footer = format!(
        "[{total_rows} row{} x {n_cols} column{}",
        plural(total_rows),
        plural(n_cols)
    );
    let rows_cut = shown_rows < total_rows;
    let cols_cut = shown_cols < n_cols;
    if rows_cut || cols_cut {
        footer.push_str(" (showing ");
        if rows_cut {
            footer.push_str(&format!("{shown_rows} row{}", plural(shown_rows)));
        }
        if rows_cut && cols_cut {
            footer.push_str(" x ");
        }
        if cols_cut {
            footer.push_str(&format!("{shown_cols} column{}", plural(shown_cols)));
        }
        footer.push(')');
    }
    footer.push(']');
    format!("{}\n{footer}", lines.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(rows: usize, cols: usize) -> Frame {
        Frame {
            columns: (0..cols)
                .map(|c| {
                    (
                        format!("c{c}"),
                        (0..rows).map(|r| Cell::Int((r * 10 + c) as i64)).collect(),
                    )
                })
                .collect(),
        }
    }

    fn opts() -> DisplayOptions {
        DisplayOptions {
            max_rows: 60,
            min_rows: 10,
            max_columns: None,
            max_colwidth: 50,
            width: None,
        }
    }

    #[test]
    fn a_table_within_the_limits_is_the_plain_table() {
        let f = frame(5, 3);
        let shown = format_dataframe_display(&f, 5, Fetched::All, &opts());
        assert_eq!(shown, crate::format::format_dataframe(&f, None));
    }

    #[test]
    fn rows_over_the_maximum_show_head_and_tail_around_an_ellipsis_row() {
        let f = frame(100, 2);
        let shown = format_dataframe_display(&f, 100, Fetched::All, &opts());
        let lines: Vec<&str> = shown.lines().collect();
        // header + separator + 5 + ellipsis + 5 + footer
        assert_eq!(lines.len(), 2 + 5 + 1 + 5 + 1, "{shown}");
        assert!(lines[7].contains(ELLIPSIS), "{shown}");
        assert!(lines[2].contains(" 0 "), "{shown}");
        assert!(lines[12].contains("990"), "{shown}");
        assert_eq!(
            *lines.last().unwrap(),
            "[100 rows x 2 columns (showing 10 rows)]"
        );
    }

    #[test]
    fn exactly_the_maximum_is_not_truncated() {
        let f = frame(60, 2);
        let shown = format_dataframe_display(&f, 60, Fetched::All, &opts());
        assert!(!shown.contains(ELLIPSIS), "{shown}");
        assert!(shown.ends_with("[60 rows x 2 columns]"), "{shown}");
    }

    #[test]
    fn a_limited_read_states_the_true_row_count() {
        let f = frame(10, 2);
        let shown = format_dataframe_display(&f, 12345, Fetched::HeadTail { head: 5 }, &opts());
        assert!(shown.ends_with("[12345 rows x 2 columns (showing 10 rows)]"));
        assert_eq!(shown.lines().filter(|l| l.contains(ELLIPSIS)).count(), 1);
        let head_only = format_dataframe_display(&f, 12345, Fetched::HeadOnly, &opts());
        let lines: Vec<&str> = head_only.lines().collect();
        assert!(lines[lines.len() - 2].contains(ELLIPSIS), "{head_only}");
    }

    #[test]
    fn columns_that_do_not_fit_the_width_are_elided_in_the_middle() {
        let f = frame(3, 12);
        let o = DisplayOptions {
            width: Some(40),
            ..opts()
        };
        let shown = format_dataframe_display(&f, 3, Fetched::All, &o);
        let header = shown.lines().next().unwrap();
        assert!(header.chars().count() <= 40, "{shown}");
        assert!(header.contains("c0") && header.contains("c11"), "{shown}");
        assert!(header.contains(ELLIPSIS), "{shown}");
        assert!(!header.contains("c6"), "{shown}");
        assert!(shown.contains("columns (showing "), "{shown}");
        let fixed = DisplayOptions {
            max_columns: Some(4),
            ..opts()
        };
        let shown = format_dataframe_display(&f, 3, Fetched::All, &fixed);
        assert!(
            shown.ends_with("[3 rows x 12 columns (showing 4 columns)]"),
            "{shown}"
        );
    }

    #[test]
    fn long_cells_are_cut_with_an_ellipsis() {
        let f = Frame {
            columns: vec![("s".into(), vec![Cell::Str("x".repeat(80))])],
        };
        let shown = format_dataframe_display(&f, 1, Fetched::All, &opts());
        let cell = shown.lines().nth(2).unwrap().trim_matches(['|', ' ']);
        assert_eq!(cell.chars().count(), 50);
        assert!(cell.ends_with(ELLIPSIS));
        let none = format_dataframe_display(&f, 1, Fetched::All, &DisplayOptions::unlimited());
        assert!(none.contains(&"x".repeat(80)));
    }

    #[test]
    fn the_config_keys_and_unlimited() {
        let all = DisplayOptions::unlimited();
        assert!(!all.truncates(usize::MAX - 1));
        let f = frame(100, 2);
        let shown = format_dataframe_display(&f, 100, Fetched::All, &all);
        assert!(!shown.contains(ELLIPSIS));
    }
}
