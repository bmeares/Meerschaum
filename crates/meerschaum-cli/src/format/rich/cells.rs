//! `rich.cells`: terminal cell widths.
//!
//! rich 15 measures characters with its own Unicode tables; this port uses `unicode-width`, which
//! agrees on the characters Meerschaum prints (ASCII, Latin-1, box drawing, the config emoji).

use unicode_width::UnicodeWidthChar;

/// `get_character_cell_size(c)`.
pub fn char_width(c: char) -> usize {
    let cp = c as u32;
    if (cp != 0 && cp < 32) || (0x7f..0xa0).contains(&cp) {
        return 0;
    }
    c.width().unwrap_or(0)
}

/// `cell_len(text)`.
pub fn cell_len(text: &str) -> usize {
    let mut total = 0;
    let mut last: Option<char> = None;
    for c in text.chars() {
        match c {
            '\u{200d}' => {}
            '\u{fe0f}' => {
                // PARITY: rich widens a narrow character followed by VS16 only when its table
                // lists it as narrow-to-wide; `unicode-width` already reports emoji presentation.
                last = None;
            }
            _ => {
                let w = char_width(c);
                if w > 0 {
                    last = Some(c);
                }
                total += w;
            }
        }
    }
    let _ = last;
    total
}

/// `split_graphemes(text)`: `(start, end, cells)` spans in char offsets, zero-width characters
/// attached to the previous character.
fn split_graphemes(chars: &[char]) -> Vec<(usize, usize, usize)> {
    let mut spans: Vec<(usize, usize, usize)> = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let w = char_width(chars[i]);
        if w > 0 {
            spans.push((i, i + 1, w));
        } else if let Some(last) = spans.last_mut() {
            last.1 = i + 1;
        } else {
            spans.push((i, i + 1, 0));
        }
        i += 1;
    }
    spans
}

fn is_single_cell(chars: &[char]) -> bool {
    chars.iter().all(|c| {
        let cp = *c as u32;
        (0x20..=0x7e).contains(&cp)
            || (0xa0..=0xac).contains(&cp)
            || (0xae..=0x2ff).contains(&cp)
            || (0x370..=0x482).contains(&cp)
            || (0x2500..=0x25fc).contains(&cp)
            || (0x2800..=0x28ff).contains(&cp)
    })
}

/// `set_cell_size(text, total)`: crop or pad with spaces to exactly `total` cells.
pub fn set_cell_size(text: &str, total: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    if is_single_cell(&chars) {
        let size = chars.len();
        if size < total {
            return format!("{text}{}", " ".repeat(total - size));
        }
        return chars[..total].iter().collect();
    }
    if total == 0 {
        return String::new();
    }
    let size = cell_len(text);
    if size == total {
        return text.to_string();
    }
    if size < total {
        return format!("{text}{}", " ".repeat(total - size));
    }
    split_text(&chars, total).0
}

/// `_split_text(text, cell_position)`.
fn split_text(chars: &[char], cell_position: usize) -> (String, String) {
    if cell_position == 0 {
        return (String::new(), chars.iter().collect());
    }
    let spans = split_graphemes(chars);
    let mut left = 0usize;
    for (start, end, size) in &spans {
        if left == cell_position {
            return (
                chars[..*start].iter().collect(),
                chars[*start..].iter().collect(),
            );
        }
        if left + size > cell_position {
            return (
                format!("{} ", chars[..*start].iter().collect::<String>()),
                format!(" {}", chars[*end..].iter().collect::<String>()),
            );
        }
        left += size;
    }
    (chars.iter().collect(), String::new())
}

/// `chop_cells(text, width)`: split into lines of at most `width` cells.
pub fn chop_cells(text: &str, width: usize) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    if is_single_cell(&chars) {
        let width = width.max(1);
        return chars.chunks(width).map(|c| c.iter().collect()).collect();
    }
    let mut lines = Vec::new();
    let mut line_size = 0usize;
    let mut line_offset = 0usize;
    for (start, _end, size) in split_graphemes(&chars) {
        if line_size + size > width {
            lines.push(chars[line_offset..start].iter().collect());
            line_offset = start;
            line_size = 0;
        }
        line_size += size;
    }
    if line_size > 0 {
        lines.push(chars[line_offset..].iter().collect());
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn widths() {
        assert_eq!(cell_len("abc"), 3);
        assert_eq!(cell_len("🎉 ok"), 5);
        assert_eq!(set_cell_size("abc", 5), "abc  ");
        assert_eq!(set_cell_size("🎉🎉", 3), "🎉 ");
        assert_eq!(chop_cells("abcdefg", 3), vec!["abc", "def", "g"]);
    }
}
