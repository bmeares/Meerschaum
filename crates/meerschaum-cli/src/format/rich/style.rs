//! `rich.color.Color` and `rich.style.Style`: parsing style definitions and rendering them as SGR
//! escape sequences for a truecolor terminal (the console `get_console()` builds).

use std::sync::OnceLock;

use super::tables::{ANSI_COLOR_NAMES, DEFAULT_STYLES};

/// A parsed color (`rich.color.Color` without the name).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Color {
    /// `default` (SGR 39 / 49).
    Default,
    /// One of the 16 standard colors.
    Standard(u8),
    /// An 8-bit palette color (16..=255).
    EightBit(u8),
    /// A 24-bit color.
    Truecolor(u8, u8, u8),
}

impl Color {
    /// `Color.parse(color)`.
    pub fn parse(color: &str) -> Result<Color, String> {
        let original = color;
        let color = color.trim().to_lowercase();
        if color == "default" {
            return Ok(Color::Default);
        }
        if let Some((_, n)) = ANSI_COLOR_NAMES.iter().find(|(name, _)| *name == color) {
            return Ok(Color::from_number(*n));
        }
        if let Some(hex) = color.strip_prefix('#') {
            if hex.len() == 6 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
                let p = |i: usize| u8::from_str_radix(&hex[i..i + 2], 16).unwrap_or(0);
                return Ok(Color::Truecolor(p(0), p(2), p(4)));
            }
        } else if let Some(inner) = color
            .strip_prefix("color(")
            .and_then(|r| r.strip_suffix(')'))
        {
            if (1..=3).contains(&inner.len()) && inner.chars().all(|c| c.is_ascii_digit()) {
                let n: u32 = inner.parse().unwrap_or(0);
                if n > 255 {
                    return Err(format!("color number must be <= 255 in {color:?}"));
                }
                return Ok(Color::from_number(n as u8));
            }
        } else if let Some(inner) = color.strip_prefix("rgb(").and_then(|r| r.strip_suffix(')'))
            && !inner.is_empty()
            && inner
                .chars()
                .all(|c| c.is_ascii_digit() || c == ',' || c.is_whitespace())
        {
            let parts: Vec<&str> = inner.split(',').collect();
            if parts.len() != 3 {
                return Err(format!("expected three components in {original:?}"));
            }
            let mut rgb = [0u8; 3];
            for (i, p) in parts.iter().enumerate() {
                let v: u32 = p
                    .trim()
                    .parse()
                    .map_err(|_| format!("{original:?} is not a valid color"))?;
                if v > 255 {
                    return Err(format!("color components must be <= 255 in {original:?}"));
                }
                rgb[i] = v as u8;
            }
            return Ok(Color::Truecolor(rgb[0], rgb[1], rgb[2]));
        }
        Err(format!("{original:?} is not a valid color"))
    }

    /// `Color.from_ansi(number)`.
    pub fn from_number(n: u8) -> Color {
        if n < 16 {
            Color::Standard(n)
        } else {
            Color::EightBit(n)
        }
    }

    /// `Color.get_ansi_codes(foreground)` (no downgrade: the console is truecolor).
    pub fn ansi_codes(&self, foreground: bool) -> Vec<String> {
        match *self {
            Color::Default => vec![if foreground { "39" } else { "49" }.to_string()],
            Color::Standard(n) => {
                let (fore, back) = if n < 8 { (30, 40) } else { (82, 92) };
                vec![(if foreground { fore } else { back } + n as u32).to_string()]
            }
            Color::EightBit(n) => vec![
                if foreground { "38" } else { "48" }.to_string(),
                "5".into(),
                n.to_string(),
            ],
            Color::Truecolor(r, g, b) => vec![
                if foreground { "38" } else { "48" }.to_string(),
                "2".into(),
                r.to_string(),
                g.to_string(),
                b.to_string(),
            ],
        }
    }
}

/// Style attribute bits, in rich's order (`Style._style_map`).
pub mod attr {
    /// `bold`
    pub const BOLD: u16 = 1;
    /// `dim`
    pub const DIM: u16 = 1 << 1;
    /// `italic`
    pub const ITALIC: u16 = 1 << 2;
    /// `underline`
    pub const UNDERLINE: u16 = 1 << 3;
    /// `underline2`
    pub const UNDERLINE2: u16 = 1 << 9;
}

const SGR_BY_BIT: [&str; 13] = [
    "1", "2", "3", "4", "5", "6", "7", "8", "9", "21", "51", "52", "53",
];

fn attribute_bit(word: &str) -> Option<u16> {
    let bit = match word {
        "bold" | "b" => 0,
        "dim" | "d" => 1,
        "italic" | "i" => 2,
        "underline" | "u" => 3,
        "blink" => 4,
        "blink2" => 5,
        "reverse" | "r" => 6,
        "conceal" | "c" => 7,
        "strike" | "s" => 8,
        "underline2" | "uu" => 9,
        "frame" => 10,
        "encircle" => 11,
        "overline" | "o" => 12,
        _ => return None,
    };
    Some(1 << bit)
}

/// `rich.style.Style` (links and meta are not used by Meerschaum).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Style {
    /// The foreground color.
    pub color: Option<Color>,
    /// The background color.
    pub bgcolor: Option<Color>,
    /// Attribute values.
    pub attributes: u16,
    /// Which attributes are set (on or off).
    pub set_attributes: u16,
}

impl Style {
    /// `Style.null()` (also `Style()`).
    pub fn null() -> Style {
        Style::default()
    }

    /// Whether this is the null style (`bool(style)` is `False`).
    pub fn is_null(&self) -> bool {
        self.set_attributes == 0 && self.color.is_none() && self.bgcolor.is_none()
    }

    /// A style with attribute bits set to a value.
    pub fn with_attrs(bits: u16, value: bool) -> Style {
        Style {
            attributes: if value { bits } else { 0 },
            set_attributes: bits,
            ..Style::default()
        }
    }

    /// `Style.parse(definition)`.
    pub fn parse(definition: &str) -> Result<Style, String> {
        if definition.trim() == "none" || definition.is_empty() {
            return Ok(Style::null());
        }
        let mut style = Style::null();
        let mut words = definition.split_whitespace();
        while let Some(original) = words.next() {
            let word = original.to_lowercase();
            if word == "on" {
                let next = words.next().unwrap_or("");
                if next.is_empty() {
                    return Err("color expected after 'on'".into());
                }
                style.bgcolor =
                    Some(Color::parse(next).map_err(|e| {
                        format!("unable to parse {next:?} as background color; {e}")
                    })?);
            } else if word == "not" {
                let next = words.next().unwrap_or("");
                let bit = attribute_bit(next).ok_or_else(|| {
                    format!("expected style attribute after 'not', found {next:?}")
                })?;
                style.attributes &= !bit;
                style.set_attributes |= bit;
            } else if word == "link" {
                if words.next().unwrap_or("").is_empty() {
                    return Err("URL expected after 'link'".into());
                }
            } else if let Some(bit) = attribute_bit(&word) {
                style.attributes |= bit;
                style.set_attributes |= bit;
            } else {
                style.color = Some(
                    Color::parse(&word)
                        .map_err(|e| format!("unable to parse {word:?} as color; {e}"))?,
                );
            }
        }
        Ok(style)
    }

    /// `Style.from_color(color, bgcolor)`.
    pub fn from_color(color: Option<Color>, bgcolor: Option<Color>) -> Style {
        Style {
            color,
            bgcolor,
            ..Style::default()
        }
    }

    /// `self + other` (`Style._add`).
    pub fn add(&self, other: &Style) -> Style {
        if other.is_null() {
            return *self;
        }
        if self.is_null() {
            return *other;
        }
        Style {
            color: other.color.or(self.color),
            bgcolor: other.bgcolor.or(self.bgcolor),
            attributes: (self.attributes & !other.set_attributes)
                | (other.attributes & other.set_attributes),
            set_attributes: self.set_attributes | other.set_attributes,
        }
    }

    /// `style.background_style`.
    pub fn background_style(&self) -> Style {
        Style::from_color(None, self.bgcolor)
    }

    /// `style.without_color` (for `NO_COLOR`).
    pub fn without_color(&self) -> Style {
        Style {
            color: None,
            bgcolor: None,
            ..*self
        }
    }

    /// Whether an attribute bit is on.
    pub fn has(&self, bit: u16) -> bool {
        self.set_attributes & bit != 0 && self.attributes & bit != 0
    }

    /// `_make_ansi_codes(TRUECOLOR)`.
    pub fn ansi_codes(&self) -> String {
        let mut sgr: Vec<String> = Vec::new();
        let attributes = self.attributes & self.set_attributes;
        for (bit, code) in SGR_BY_BIT.iter().enumerate() {
            if attributes & (1 << bit) != 0 {
                sgr.push((*code).to_string());
            }
        }
        if let Some(c) = &self.color {
            sgr.extend(c.ansi_codes(true));
        }
        if let Some(c) = &self.bgcolor {
            sgr.extend(c.ansi_codes(false));
        }
        sgr.join(";")
    }

    /// `style.render(text, color_system=TRUECOLOR)`.
    pub fn render(&self, text: &str) -> String {
        if text.is_empty() {
            return String::new();
        }
        let codes = self.ansi_codes();
        if codes.is_empty() {
            text.to_string()
        } else {
            format!("\x1b[{codes}m{text}\x1b[0m")
        }
    }
}

/// The default theme (`console.get_style(name)` looks names up here before parsing).
fn theme() -> &'static Vec<(&'static str, Style)> {
    static THEME: OnceLock<Vec<(&'static str, Style)>> = OnceLock::new();
    THEME.get_or_init(|| {
        DEFAULT_STYLES
            .iter()
            .map(|(name, attributes, set_attributes, color, bgcolor)| {
                (
                    *name,
                    Style {
                        color: color.and_then(|c| Color::parse(c).ok()),
                        bgcolor: bgcolor.and_then(|c| Color::parse(c).ok()),
                        attributes: *attributes,
                        set_attributes: *set_attributes,
                    },
                )
            })
            .collect()
    })
}

/// `console.get_style(name)`: a theme style, else a parsed definition.
pub fn get_style(name: &str) -> Result<Style, String> {
    if let Some((_, s)) = theme().iter().find(|(n, _)| *n == name) {
        return Ok(*s);
    }
    Style::parse(name)
}

/// A style as rich spans hold it: a definition (resolved at render time) or a `Style`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StyleRef {
    /// A style definition or theme name (`""` is the null style).
    Name(String),
    /// A parsed style.
    Style(Style),
}

impl Default for StyleRef {
    fn default() -> Self {
        StyleRef::Name(String::new())
    }
}

impl StyleRef {
    /// `bool(style)`: a non-empty definition or a non-null style.
    pub fn is_set(&self) -> bool {
        match self {
            StyleRef::Name(n) => !n.is_empty(),
            StyleRef::Style(s) => !s.is_null(),
        }
    }

    /// Resolve with `console.get_style(style, default=Style.null())`.
    pub fn resolve(&self) -> Style {
        match self {
            StyleRef::Name(n) => get_style(n).unwrap_or_default(),
            StyleRef::Style(s) => *s,
        }
    }
}

impl From<&str> for StyleRef {
    fn from(s: &str) -> Self {
        StyleRef::Name(s.to_string())
    }
}

impl From<String> for StyleRef {
    fn from(s: String) -> Self {
        StyleRef::Name(s)
    }
}

impl From<Style> for StyleRef {
    fn from(s: Style) -> Self {
        StyleRef::Style(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_render() {
        assert_eq!(
            Style::parse("bold bright_green").unwrap().render("x"),
            "\x1b[1;92mx\x1b[0m"
        );
        assert_eq!(
            Style::parse("pale_green3").unwrap().render("x"),
            "\x1b[38;5;114mx\x1b[0m"
        );
        assert_eq!(
            Style::parse("#d177a4").unwrap().render("x"),
            "\x1b[38;2;209;119;164mx\x1b[0m"
        );
        assert_eq!(
            Style::parse("black on magenta").unwrap().render("x"),
            "\x1b[30;45mx\x1b[0m"
        );
        assert!(Style::parse("indian red").is_err());
        assert_eq!(
            get_style("repr.number").unwrap().render("1"),
            "\x1b[1;36m1\x1b[0m"
        );
    }
}
