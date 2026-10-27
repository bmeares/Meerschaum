//! `rich.segment.Segment`: a piece of text with one style, and the line helpers renderables use.

use super::cells::{cell_len, set_cell_size};
use super::style::Style;

/// `Segment(text, style)`. `style: None` and the null style both render as plain text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    /// The text.
    pub text: String,
    /// The style.
    pub style: Option<Style>,
}

/// A line of segments (no `\n`).
pub type Line = Vec<Segment>;

impl Segment {
    /// A segment.
    pub fn new(text: impl Into<String>, style: Option<Style>) -> Segment {
        Segment {
            text: text.into(),
            style,
        }
    }

    /// A plain segment.
    pub fn plain(text: impl Into<String>) -> Segment {
        Segment::new(text, None)
    }

    /// `Segment.line()`.
    pub fn line() -> Segment {
        Segment::plain("\n")
    }

    /// `segment.cell_length`.
    pub fn cell_length(&self) -> usize {
        cell_len(&self.text)
    }

    /// Whether the style is truthy.
    pub fn styled(&self) -> Option<&Style> {
        self.style.as_ref().filter(|s| !s.is_null())
    }
}

/// Python's `str.partition("\n")` loop shared by the line splitters.
fn split_newlines(text: &str) -> Vec<(String, bool)> {
    let mut out = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        match rest.find('\n') {
            Some(i) => {
                out.push((rest[..i].to_string(), true));
                rest = &rest[i + 1..];
            }
            None => {
                out.push((rest.to_string(), false));
                rest = "";
            }
        }
    }
    out
}

/// `Segment.split_and_crop_lines(segments, length, style, pad, include_new_lines)`.
pub fn split_and_crop_lines(
    segments: &[Segment],
    length: usize,
    style: Option<Style>,
    pad: bool,
    include_new_lines: bool,
) -> Vec<Line> {
    let mut lines = Vec::new();
    let mut line: Line = Vec::new();
    for segment in segments {
        if segment.text.contains('\n') {
            for (text, newline) in split_newlines(&segment.text) {
                if !text.is_empty() {
                    line.push(Segment::new(text, segment.style));
                }
                if newline {
                    let mut cropped = adjust_line_length(&line, length, style, pad);
                    if include_new_lines {
                        cropped.push(Segment::line());
                    }
                    lines.push(cropped);
                    line.clear();
                }
            }
        } else {
            line.push(segment.clone());
        }
    }
    if !line.is_empty() {
        lines.push(adjust_line_length(&line, length, style, pad));
    }
    lines
}

/// `Segment.adjust_line_length(line, length, style, pad)`.
pub fn adjust_line_length(
    line: &[Segment],
    length: usize,
    style: Option<Style>,
    pad: bool,
) -> Line {
    let line_length: usize = line.iter().map(Segment::cell_length).sum();
    if line_length < length {
        let mut new_line = line.to_vec();
        if pad {
            new_line.push(Segment::new(" ".repeat(length - line_length), style));
        }
        new_line
    } else if line_length > length {
        let mut new_line = Vec::new();
        let mut current = 0usize;
        for segment in line {
            let seg_len = segment.cell_length();
            if current + seg_len < length {
                new_line.push(segment.clone());
                current += seg_len;
            } else {
                new_line.push(Segment::new(
                    set_cell_size(&segment.text, length - current),
                    segment.style,
                ));
                break;
            }
        }
        new_line
    } else {
        line.to_vec()
    }
}

/// `Segment.split_lines(segments)`.
pub fn split_lines(segments: &[Segment]) -> Vec<Line> {
    let mut lines = Vec::new();
    let mut line: Line = Vec::new();
    for segment in segments {
        if segment.text.contains('\n') {
            for (text, newline) in split_newlines(&segment.text) {
                if !text.is_empty() {
                    line.push(Segment::new(text, segment.style));
                }
                if newline {
                    lines.push(std::mem::take(&mut line));
                }
            }
        } else {
            line.push(segment.clone());
        }
    }
    if !line.is_empty() {
        lines.push(line);
    }
    lines
}

/// `Segment.get_line_length(line)`.
pub fn line_length(line: &[Segment]) -> usize {
    line.iter().map(Segment::cell_length).sum()
}

/// `Segment.set_shape(lines, width, height, style)`.
pub fn set_shape(
    lines: &[Line],
    width: usize,
    height: Option<usize>,
    style: Option<Style>,
) -> Vec<Line> {
    let height = height.unwrap_or(lines.len());
    let mut shaped: Vec<Line> = lines
        .iter()
        .map(|l| adjust_line_length(l, width, style, true))
        .collect();
    // PARITY: rich slices `lines[:_height]` and then replaces the slice with every line, so
    // taller cells are never cropped here.
    while shaped.len() < height {
        shaped.push(vec![Segment::new(" ".repeat(width), style)]);
    }
    shaped
}

/// `Segment.align_top(lines, width, height, style)`.
pub fn align_top(lines: &[Line], width: usize, height: usize, style: Option<Style>) -> Vec<Line> {
    if lines.len() >= height {
        return lines[..height.min(lines.len())].to_vec();
    }
    let mut out = lines.to_vec();
    while out.len() < height {
        out.push(vec![Segment::new(" ".repeat(width), style)]);
    }
    out
}

/// `Segment.align_bottom(lines, width, height, style)`.
pub fn align_bottom(
    lines: &[Line],
    width: usize,
    height: usize,
    style: Option<Style>,
) -> Vec<Line> {
    if lines.len() >= height {
        return lines[..height.min(lines.len())].to_vec();
    }
    let mut out = vec![vec![Segment::new(" ".repeat(width), style)]; height - lines.len()];
    out.extend(lines.iter().cloned());
    out
}

/// `Segment.apply_style(segments, style, post_style)`.
pub fn apply_style(
    segments: Vec<Segment>,
    style: Option<&Style>,
    post_style: Option<&Style>,
) -> Vec<Segment> {
    let mut segments = segments;
    if let Some(style) = style.filter(|s| !s.is_null()) {
        segments = segments
            .into_iter()
            .map(|s| Segment {
                style: Some(style.add(&s.style.unwrap_or_default())),
                text: s.text,
            })
            .collect();
    }
    if let Some(post) = post_style.filter(|s| !s.is_null()) {
        segments = segments
            .into_iter()
            .map(|s| Segment {
                style: Some(match s.style.filter(|st| !st.is_null()) {
                    Some(st) => st.add(post),
                    None => *post,
                }),
                text: s.text,
            })
            .collect();
    }
    segments
}

/// Render segments to a string (`Console._render_buffer`). With `no_color` (`NO_COLOR`) the text
/// is plain: no colors and no other styles, so the output carries no escape sequences.
pub fn render_segments(segments: &[Segment], no_color: bool) -> String {
    let mut out = String::new();
    for s in segments {
        match s.styled() {
            Some(style) if !no_color => out.push_str(&style.render(&s.text)),
            _ => out.push_str(&s.text),
        }
    }
    out
}
