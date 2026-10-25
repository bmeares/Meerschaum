//! Colors and glyphs of the full-screen views, from the formatting settings: `NO_COLOR` (or
//! `formatting:ansi: false`) gives a monochrome theme that marks selection and state with
//! reverse video and bold only, and `formatting:unicode: false` gives ASCII borders and bars.

use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols::border;

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
}
