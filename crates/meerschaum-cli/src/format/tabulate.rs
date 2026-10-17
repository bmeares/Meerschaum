//! `format_dataframe` (`meerschaum/utils/formatting/_dataframe.py`): `DataFrame.to_markdown()`,
//! which is `tabulate(df, headers='keys', tablefmt='pipe', showindex=False)` (tabulate 0.10),
//! followed by the `[rows x columns]` footer.
//!
//! [`Frame`] is a small column-oriented table standing in for the pandas `DataFrame`: each
//! column is a list of [`Cell`]s and gets the dtype pandas would infer.

use meerschaum_core::pyfmt::float_repr;

/// A cell value.
#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    /// `str`
    Str(String),
    /// `int`
    Int(i64),
    /// `float`
    Float(f64),
    /// `bool`
    Bool(bool),
    /// `None` (pandas stores it as `NaN` in numeric and string columns).
    Null,
}

/// A table of named columns.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Frame {
    /// `(name, values)` in order.
    pub columns: Vec<(String, Vec<Cell>)>,
}

impl Frame {
    /// The number of rows.
    pub fn num_rows(&self) -> usize {
        self.columns.first().map(|(_, v)| v.len()).unwrap_or(0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ColType {
    None = 0,
    Bool = 1,
    Int = 2,
    Float = 3,
    Str = 5,
}

/// The pandas dtype of a column, as the values `df.values` yields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Dtype {
    Int,
    Float,
    Bool,
    Str,
}

fn infer_dtype(values: &[Cell]) -> Dtype {
    let has = |f: fn(&Cell) -> bool| values.iter().any(f);
    if has(|c| matches!(c, Cell::Str(_))) {
        Dtype::Str
    } else if has(|c| matches!(c, Cell::Float(_) | Cell::Null)) {
        Dtype::Float
    } else if has(|c| matches!(c, Cell::Int(_))) {
        Dtype::Int
    } else if has(|c| matches!(c, Cell::Bool(_))) {
        Dtype::Bool
    } else {
        Dtype::Float
    }
}

/// Python's `format(x, "g")`.
pub fn format_g(x: f64) -> String {
    if x.is_nan() {
        return "nan".into();
    }
    if x.is_infinite() {
        return if x > 0.0 { "inf".into() } else { "-inf".into() };
    }
    if x == 0.0 {
        return if x.is_sign_negative() {
            "-0".into()
        } else {
            "0".into()
        };
    }
    let precision = 6i32;
    let exp = format!("{:.*e}", (precision - 1) as usize, x);
    let (mantissa, e) = exp.split_once('e').unwrap_or((&exp, "0"));
    let e: i32 = e.parse().unwrap_or(0);
    if e < -4 || e >= precision {
        let m = strip_zeros(mantissa);
        let sign = if e < 0 { '-' } else { '+' };
        format!("{m}e{sign}{:02}", e.abs())
    } else {
        let decimals = (precision - 1 - e).max(0) as usize;
        strip_zeros(&format!("{x:.decimals$}"))
    }
}

fn strip_zeros(s: &str) -> String {
    if s.contains('.') {
        s.trim_end_matches('0').trim_end_matches('.').to_string()
    } else {
        s.to_string()
    }
}

/// tabulate's `_afterpoint` for a formatted number string (or -1).
fn afterpoint(s: &str, numeric: bool) -> i64 {
    if !numeric {
        return -1;
    }
    if s.parse::<i64>().is_ok() {
        return -1;
    }
    let pos = s.rfind('.').or_else(|| s.to_lowercase().rfind('e'));
    match pos {
        Some(p) => (s.chars().count() - s[..p].chars().count() - 1) as i64,
        None => -1,
    }
}

fn is_number(s: &str) -> bool {
    let t = s.trim();
    t.parse::<f64>().is_ok() && {
        let f: f64 = t.parse().unwrap_or(0.0);
        !(f.is_infinite() || f.is_nan())
            || ["inf", "-inf", "nan"].contains(&t.to_lowercase().as_str())
    }
}

/// `format_dataframe(df, max_rows)`.
pub fn format_dataframe(frame: &Frame, max_rows: Option<usize>) -> String {
    let n_rows = frame.num_rows();
    let n_cols = frame.columns.len();
    let render_rows = match max_rows {
        Some(m) if n_rows > m => m,
        _ => n_rows,
    };
    let truncated = n_rows - render_rows;

    // `df.values`: a homogeneous numeric frame becomes float64 if any column is float.
    let dtypes: Vec<Dtype> = frame.columns.iter().map(|(_, v)| infer_dtype(v)).collect();
    let all_numeric = dtypes
        .iter()
        .all(|d| matches!(d, Dtype::Int | Dtype::Float));
    let any_float = dtypes.contains(&Dtype::Float);

    let mut cols_text: Vec<Vec<String>> = Vec::new();
    let mut aligns: Vec<&str> = Vec::new();
    for ((_, values), dtype) in frame.columns.iter().zip(&dtypes) {
        let as_float = *dtype == Dtype::Float || (all_numeric && any_float);
        let values = &values[..render_rows.min(values.len())];
        // The cell types tabulate sees.
        let types: Vec<ColType> = values
            .iter()
            .map(|c| match c {
                Cell::Str(s) if s.is_empty() => ColType::None,
                Cell::Str(s) => {
                    if s.parse::<i64>().is_ok() {
                        ColType::Int
                    } else if is_number(s) {
                        ColType::Float
                    } else if s == "True" || s == "False" {
                        ColType::Bool
                    } else {
                        ColType::Str
                    }
                }
                Cell::Bool(_) => ColType::Bool,
                Cell::Int(_) if as_float => ColType::Float,
                Cell::Int(_) => ColType::Int,
                Cell::Float(_) | Cell::Null => ColType::Float,
            })
            .collect();
        let coltype = types.iter().copied().fold(ColType::Bool, Ord::max);
        let numeric = matches!(coltype, ColType::Int | ColType::Float);
        let formatted: Vec<String> = values
            .iter()
            .map(|c| match (c, coltype) {
                (Cell::Str(s), ColType::Float) if !s.is_empty() => {
                    s.parse::<f64>().map(format_g).unwrap_or_else(|_| s.clone())
                }
                (Cell::Str(s), _) => s.clone(),
                (Cell::Int(i), ColType::Float) => format_g(*i as f64),
                (Cell::Int(i), _) => i.to_string(),
                (Cell::Float(f), ColType::Float) => format_g(*f),
                (Cell::Float(f), _) => float_repr(*f),
                (Cell::Null, ColType::Float) => "nan".into(),
                (Cell::Null, _) => "nan".into(),
                (Cell::Bool(b), _) => if *b { "True" } else { "False" }.into(),
            })
            .collect();
        let align = if numeric { "decimal" } else { "left" };
        aligns.push(align);
        cols_text.push(
            formatted
                .into_iter()
                .map(|s| if numeric { s } else { s.trim().to_string() })
                .collect(),
        );
    }

    let headers: Vec<String> = frame.columns.iter().map(|(n, _)| n.clone()).collect();
    const MIN_PADDING: usize = 2;
    let width = |s: &str| s.chars().count();
    let mut padded_cols: Vec<Vec<String>> = Vec::new();
    for (i, col) in cols_text.iter().enumerate() {
        let minw = width(&headers[i]) + MIN_PADDING;
        let strings: Vec<String> = if aligns[i] == "decimal" {
            let decimals: Vec<i64> = col.iter().map(|s| afterpoint(s, is_number(s))).collect();
            let maxd = decimals.iter().copied().max().unwrap_or(-1);
            col.iter()
                .zip(&decimals)
                .map(|(s, d)| format!("{s}{}", " ".repeat((maxd - d).max(0) as usize)))
                .collect()
        } else {
            col.clone()
        };
        let maxwidth = strings
            .iter()
            .map(|s| width(s))
            .max()
            .unwrap_or(0)
            .max(minw);
        padded_cols.push(
            strings
                .iter()
                .map(|s| {
                    let pad = " ".repeat(maxwidth.saturating_sub(width(s)));
                    if aligns[i] == "decimal" {
                        format!("{pad}{s}")
                    } else {
                        format!("{s}{pad}")
                    }
                })
                .collect(),
        );
    }
    let widths: Vec<usize> = padded_cols
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let minw = width(&headers[i]) + MIN_PADDING;
            c.iter().map(|s| width(s)).max().unwrap_or(0).max(minw)
        })
        .collect();
    let header_cells: Vec<String> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| {
            let pad = " ".repeat(widths[i].saturating_sub(width(h)));
            if aligns[i] == "left" {
                format!("{h}{pad}")
            } else {
                format!("{pad}{h}")
            }
        })
        .collect();

    let mut lines: Vec<String> = Vec::new();
    lines.push(format!(
        "|{}|",
        header_cells
            .iter()
            .map(|c| format!(" {c} "))
            .collect::<Vec<_>>()
            .join("|")
    ));
    lines.push(format!(
        "|{}|",
        widths
            .iter()
            .zip(&aligns)
            .map(|(w, a)| {
                let w = w + 2;
                // PARITY: tabulate sees no columns for a zero-row frame and draws the separator
                // without alignment colons (`|-----|`).
                if render_rows == 0 {
                    "-".repeat(w)
                } else if *a == "left" {
                    format!(":{}", "-".repeat(w - 1))
                } else {
                    format!("{}:", "-".repeat(w - 1))
                }
            })
            .collect::<Vec<_>>()
            .join("|")
    ));
    for r in 0..render_rows {
        lines.push(format!(
            "|{}|",
            padded_cols
                .iter()
                .map(|c| format!(" {} ", c[r]))
                .collect::<Vec<_>>()
                .join("|")
        ));
    }
    let body = lines.join("\n");
    let plural = |n: usize| if n != 1 { "s" } else { "" };
    let footer = if truncated > 0 {
        format!(
            "\n[showing first {render_rows} of {n_rows} rows x {n_cols} column{}]",
            plural(n_cols)
        )
    } else {
        format!(
            "\n[{n_rows} row{} x {n_cols} column{}]",
            plural(n_rows),
            plural(n_cols)
        )
    };
    body + &footer
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn g_format() {
        assert_eq!(format_g(1234567.0), "1.23457e+06");
        assert_eq!(format_g(0.000123), "0.000123");
        assert_eq!(format_g(12.0), "12");
        assert_eq!(format_g(-3.0), "-3");
        assert_eq!(format_g(22.25), "22.25");
        assert_eq!(format_g(1e-5), "1e-05");
    }
}
