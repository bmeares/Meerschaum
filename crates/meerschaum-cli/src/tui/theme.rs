//! Colors and glyphs of the full-screen views, from the formatting settings: `NO_COLOR` (or
//! `formatting:ansi: false`) gives a monochrome theme that marks selection and state with
//! reverse video and bold only, and `formatting:unicode: false` gives ASCII borders and bars.

use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols::border;
use ratatui::text::Span;

use crate::format::FormatSettings;

/// The ASCII border set (`+`, `-`, `|`).
pub const ASCII_BORDER: border::Set<'static> = border::Set {
    top_left: "+",
    top_right: "+",
    bottom_left: "+",
    bottom_right: "+",
    vertical_left: "|",
    vertical_right: "|",
    horizontal_top: "-",
    horizontal_bottom: "-",
};

/// What a view's state looks like on screen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tone {
    /// Neutral text.
    Normal,
    /// Secondary text (timestamps, hints).
    Dim,
    /// In progress.
    Active,
    /// Succeeded / running job.
    Good,
    /// Failed / stopped with an error.
    Bad,
    /// Waiting, paused, retrying, skipped.
    Warn,
    /// Titles and keys.
    Accent,
}

/// The palette and glyphs one view draws with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Theme {
    /// Colors are allowed.
    pub color: bool,
    /// Unicode glyphs are allowed.
    pub unicode: bool,
}

impl Theme {
    /// The theme for the current formatting settings.
    pub fn from_settings(settings: &FormatSettings) -> Theme {
        Theme {
            color: settings.ansi && !settings.no_color,
            unicode: settings.unicode,
        }
    }

    /// A monochrome ASCII theme (tests, dumb terminals).
    pub fn plain() -> Theme {
        Theme {
            color: false,
            unicode: false,
        }
    }

    /// The style of a tone.
    pub fn style(&self, tone: Tone) -> Style {
        if !self.color {
            return match tone {
                Tone::Accent | Tone::Bad => Style::default().add_modifier(Modifier::BOLD),
                _ => Style::default(),
            };
        }
        let fg = match tone {
            Tone::Normal => return Style::default(),
            Tone::Dim => Color::DarkGray,
            Tone::Active => Color::Cyan,
            Tone::Good => Color::Green,
            Tone::Bad => Color::Red,
            Tone::Warn => Color::Yellow,
            Tone::Accent => Color::Magenta,
        };
        let style = Style::default().fg(fg);
        if tone == Tone::Accent {
            style.add_modifier(Modifier::BOLD)
        } else {
            style
        }
    }

    /// The highlighted row.
    pub fn selected(&self) -> Style {
        if self.color {
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().add_modifier(Modifier::REVERSED)
        }
    }

    /// The border glyphs.
    pub fn border(&self) -> border::Set<'static> {
        if self.unicode {
            border::ROUNDED
        } else {
            ASCII_BORDER
        }
    }

    /// The bar glyphs: `(done, waited, planned)`.
    pub fn bars(&self) -> (&'static str, &'static str, &'static str) {
        if self.unicode {
            ("█", "░", "▒")
        } else {
            ("#", ".", "=")
        }
    }

    /// The selection marker in lists.
    pub fn marker(&self) -> &'static str {
        if self.unicode { "▶ " } else { "> " }
    }

    /// An ellipsis for truncated text.
    pub fn ellipsis(&self) -> &'static str {
        if self.unicode { "…" } else { "~" }
    }
}

/// `text` cut to `width` display columns, ending in `ellipsis` when cut.
pub fn truncate(text: &str, width: usize, ellipsis: &str) -> String {
    use unicode_width::UnicodeWidthStr;
    if text.width() <= width {
        return text.to_string();
    }
    let room = width.saturating_sub(ellipsis.width());
    let mut out = String::new();
    let mut used = 0;
    for c in text.chars() {
        let w = unicode_width::UnicodeWidthChar::width(c).unwrap_or(0);
        if used + w > room {
            break;
        }
        used += w;
        out.push(c);
    }
    if width >= ellipsis.width() {
        out.push_str(ellipsis);
    }
    out
}

/// `text` with its ANSI SGR colors and attributes (the `\x1b[...m` sequences a log line keeps)
/// as styled spans over `base`. Without `color`, the colors are dropped and bold, italic and
/// underline kept. Other escape sequences (cursor moves, OSC titles) are removed.
pub fn ansi_spans(text: &str, base: Style, color: bool) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    let mut style = base;
    let mut run = String::new();
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\x1b' {
            run.push(c);
            continue;
        }
        match chars.peek() {
            Some('[') => {
                chars.next();
                let mut params = String::new();
                let mut fin = None;
                for c in chars.by_ref() {
                    if ('\x40'..='\x7e').contains(&c) {
                        fin = Some(c);
                        break;
                    }
                    params.push(c);
                }
                if fin == Some('m') {
                    if !run.is_empty() {
                        spans.push(Span::styled(std::mem::take(&mut run), style));
                    }
                    style = apply_sgr(style, base, &params, color);
                }
            }
            Some(']') => {
                // OSC: up to BEL or ST (`ESC \`).
                chars.next();
                while let Some(c) = chars.next() {
                    if c == '\x07' || (c == '\x1b' && chars.next_if_eq(&'\\').is_some()) {
                        break;
                    }
                }
            }
            _ => {
                chars.next();
            }
        }
    }
    if !run.is_empty() {
        spans.push(Span::styled(run, style));
    }
    spans
}

/// Apply one SGR parameter list (`1;31`, `38;5;114`, `38;2;r;g;b`, empty = reset).
fn apply_sgr(mut style: Style, base: Style, params: &str, color: bool) -> Style {
    let codes: Vec<u16> = params
        .split([';', ':'])
        .map(|p| p.parse().unwrap_or(0))
        .collect();
    let mut i = 0;
    while i < codes.len() {
        let code = codes[i];
        i += 1;
        let extended = |i: &mut usize| -> Option<Color> {
            match codes.get(*i) {
                Some(5) => {
                    let n = *codes.get(*i + 1)?;
                    *i += 2;
                    Some(Color::Indexed(n.min(255) as u8))
                }
                Some(2) => {
                    let (r, g, b) = (codes.get(*i + 1)?, codes.get(*i + 2)?, codes.get(*i + 3)?);
                    *i += 4;
                    Some(Color::Rgb(
                        (*r).min(255) as u8,
                        (*g).min(255) as u8,
                        (*b).min(255) as u8,
                    ))
                }
                _ => None,
            }
        };
        const BASIC: [Color; 8] = [
            Color::Black,
            Color::Red,
            Color::Green,
            Color::Yellow,
            Color::Blue,
            Color::Magenta,
            Color::Cyan,
            Color::Gray,
        ];
        const BRIGHT: [Color; 8] = [
            Color::DarkGray,
            Color::LightRed,
            Color::LightGreen,
            Color::LightYellow,
            Color::LightBlue,
            Color::LightMagenta,
            Color::LightCyan,
            Color::White,
        ];
        let (fg, bg) = match code {
            0 => {
                style = base;
                (None, None)
            }
            1 => {
                style = style.add_modifier(Modifier::BOLD);
                (None, None)
            }
            2 => {
                style = style.add_modifier(Modifier::DIM);
                (None, None)
            }
            3 => {
                style = style.add_modifier(Modifier::ITALIC);
                (None, None)
            }
            4 => {
                style = style.add_modifier(Modifier::UNDERLINED);
                (None, None)
            }
            7 => {
                style = style.add_modifier(Modifier::REVERSED);
                (None, None)
            }
            9 => {
                style = style.add_modifier(Modifier::CROSSED_OUT);
                (None, None)
            }
            22 => {
                style = style.remove_modifier(Modifier::BOLD | Modifier::DIM);
                (None, None)
            }
            23 => {
                style = style.remove_modifier(Modifier::ITALIC);
                (None, None)
            }
            24 => {
                style = style.remove_modifier(Modifier::UNDERLINED);
                (None, None)
            }
            27 => {
                style = style.remove_modifier(Modifier::REVERSED);
                (None, None)
            }
            29 => {
                style = style.remove_modifier(Modifier::CROSSED_OUT);
                (None, None)
            }
            30..=37 => (Some(BASIC[(code - 30) as usize]), None),
            38 => (extended(&mut i), None),
            39 => (base.fg.or(Some(Color::Reset)), None),
            40..=47 => (None, Some(BASIC[(code - 40) as usize])),
            48 => (None, extended(&mut i)),
            49 => (None, base.bg.or(Some(Color::Reset))),
            90..=97 => (Some(BRIGHT[(code - 90) as usize]), None),
            100..=107 => (None, Some(BRIGHT[(code - 100) as usize])),
            _ => (None, None),
        };
        if color {
            if let Some(fg) = fg {
                style = style.fg(fg);
            }
            if let Some(bg) = bg {
                style = style.bg(bg);
            }
        }
    }
    style
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(ansi: bool, unicode: bool, no_color: bool) -> FormatSettings {
        FormatSettings {
            ansi,
            unicode,
            width: 80,
            cols: 80,
            no_color,
        }
    }

    #[test]
    fn no_color_and_ascii() {
        let t = Theme::from_settings(&settings(true, true, true));
        assert!(!t.color);
        assert_eq!(t.style(Tone::Good), Style::default());
        assert!(t.selected().add_modifier.contains(Modifier::REVERSED));
        let t = Theme::from_settings(&settings(true, false, false));
        assert!(t.color);
        assert_eq!(t.border().top_left, "+");
        assert_eq!(t.bars().0, "#");
        assert_eq!(t.style(Tone::Bad).fg, Some(Color::Red));
    }

    #[test]
    fn truncates_by_display_width() {
        assert_eq!(truncate("abcdef", 6, "~"), "abcdef");
        assert_eq!(truncate("abcdef", 4, "~"), "abc~");
        assert_eq!(truncate("日本語です", 5, "…"), "日本…");
        assert_eq!(truncate("abc", 0, "~"), "");
    }

    #[test]
    fn ansi_spans_keep_colors_and_attributes() {
        let spans = ansi_spans(
            "\u{1b}[1;31mred\u{1b}[0m plain \u{1b}[38;5;114mgreen\u{1b}[38;2;1;2;3mrgb\u{1b}[2K",
            Style::default(),
            true,
        );
        let texts: Vec<&str> = spans.iter().map(|s| s.content.as_ref()).collect();
        assert_eq!(texts, ["red", " plain ", "green", "rgb"]);
        assert_eq!(spans[0].style.fg, Some(Color::Red));
        assert!(spans[0].style.add_modifier.contains(Modifier::BOLD));
        assert_eq!(spans[1].style, Style::default());
        assert_eq!(spans[2].style.fg, Some(Color::Indexed(114)));
        assert_eq!(spans[3].style.fg, Some(Color::Rgb(1, 2, 3)));
        // Monochrome: attributes only; an OSC title is dropped.
        let mono = ansi_spans("\u{1b}]0;title\u{7}\u{1b}[1;32mok", Style::default(), false);
        assert_eq!(mono.len(), 1);
        assert_eq!(mono[0].content, "ok");
        assert_eq!(mono[0].style.fg, None);
        assert!(mono[0].style.add_modifier.contains(Modifier::BOLD));
    }
}
