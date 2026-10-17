//! `rich.console.Console` (the subset `get_console()` uses: a forced truecolor terminal whose
//! output is captured), `ConsoleOptions`, `Measurement`, and the `Padding` / `Styled` wrappers.

use super::segment::{self, Line, Segment, render_segments, split_and_crop_lines};
use super::style::{Style, StyleRef};
use super::text::Text;

/// `JustifyMethod`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Justify {
    /// `"default"`
    Default,
    /// `"left"`
    Left,
    /// `"center"`
    Center,
    /// `"right"`
    Right,
    /// `"full"`
    Full,
}

/// `OverflowMethod`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Overflow {
    /// `"fold"`
    Fold,
    /// `"crop"`
    Crop,
    /// `"ellipsis"`
    Ellipsis,
    /// `"ignore"`
    Ignore,
}

/// `ConsoleOptions` (the fields renderables read).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderOptions {
    /// `max_width`.
    pub max_width: usize,
    /// `justify`.
    pub justify: Option<Justify>,
    /// `overflow`.
    pub overflow: Option<Overflow>,
    /// `no_wrap`.
    pub no_wrap: bool,
    /// `highlight`.
    pub highlight: bool,
    /// `height`.
    pub height: Option<usize>,
}

impl RenderOptions {
    /// `options.update_width(width)`.
    pub fn with_width(&self, width: usize) -> RenderOptions {
        RenderOptions {
            max_width: width,
            ..self.clone()
        }
    }
}

/// `Measurement(minimum, maximum)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Measurement {
    /// Minimum cells.
    pub minimum: usize,
    /// Maximum cells.
    pub maximum: usize,
}

impl Measurement {
    /// A measurement.
    pub fn new(minimum: usize, maximum: usize) -> Measurement {
        Measurement { minimum, maximum }
    }

    /// `normalize()`.
    pub fn normalize(self) -> Measurement {
        let minimum = self.minimum.min(self.maximum);
        Measurement::new(minimum, minimum.max(self.maximum))
    }

    /// `with_maximum(width)`.
    pub fn with_maximum(self, width: usize) -> Measurement {
        Measurement::new(self.minimum.min(width), self.maximum.min(width))
    }

    /// `Measurement.get(console, options, renderable)`.
    pub fn get(console: &Console, options: &RenderOptions, r: &dyn Renderable) -> Measurement {
        let max_width = options.max_width;
        if max_width < 1 {
            return Measurement::new(0, 0);
        }
        let m = r
            .measure(console, options)
            .normalize()
            .with_maximum(max_width);
        if m.maximum < 1 {
            return Measurement::new(0, 0);
        }
        m.normalize()
    }
}

/// A rich renderable (`__rich_console__` and `__rich_measure__`).
pub trait Renderable {
    /// Render to segments.
    fn render(&self, console: &Console, options: &RenderOptions) -> Vec<Segment>;

    /// Measure (`__rich_measure__`); renderables without one measure as `(0, max_width)`.
    fn measure(&self, _console: &Console, options: &RenderOptions) -> Measurement {
        Measurement::new(0, options.max_width)
    }
}

impl Renderable for Text {
    fn render(&self, console: &Console, options: &RenderOptions) -> Vec<Segment> {
        self.render_with(console, options)
    }

    fn measure(&self, _console: &Console, _options: &RenderOptions) -> Measurement {
        let (min, max) = Text::measure(self);
        Measurement::new(min, max)
    }
}

/// The console `get_console()` returns: `Console(force_terminal=True, color_system='truecolor')`
/// with output captured.
#[derive(Debug, Clone)]
pub struct Console {
    /// The width in cells.
    pub width: usize,
    /// `NO_COLOR` is set (colors are removed, attributes kept).
    pub no_color: bool,
}

impl Console {
    /// A console of a given width.
    pub fn new(width: usize) -> Console {
        Console {
            width,
            no_color: false,
        }
    }

    /// `console.options`.
    pub fn options(&self) -> RenderOptions {
        RenderOptions {
            max_width: self.width,
            justify: None,
            overflow: None,
            no_wrap: false,
            highlight: true,
            height: None,
        }
    }

    /// `console.render(renderable, options)`.
    pub fn render(&self, r: &dyn Renderable, options: &RenderOptions) -> Vec<Segment> {
        if options.max_width < 1 {
            return Vec::new();
        }
        r.render(self, options)
    }

    /// `console.render_lines(renderable, options, style=style, pad=pad)`.
    pub fn render_lines(
        &self,
        r: &dyn Renderable,
        options: &RenderOptions,
        style: Option<Style>,
        pad: bool,
    ) -> Vec<Line> {
        let mut rendered = self.render(r, options);
        let style = style.filter(|s| !s.is_null());
        if let Some(s) = &style {
            rendered = segment::apply_style(rendered, Some(s), None);
        }
        let mut lines = split_and_crop_lines(&rendered, options.max_width, style, pad, false);
        if let Some(height) = options.height {
            lines.truncate(height);
            while lines.len() < height {
                lines.push(vec![Segment::new(" ".repeat(options.max_width), style)]);
            }
        }
        lines
    }

    /// `console.print(renderable)` captured: the rendered text including the final newline.
    pub fn print(&self, r: &dyn Renderable) -> String {
        let options = self.options();
        let segments = self.render(r, &options);
        self.finish(&segments)
    }

    /// `console.print(text)` captured: a `Text` is first joined with the separator `Text`
    /// (`Text(" ", end="\n").join([text])`), which drops its own `justify`, `no_wrap` and `end`.
    pub fn print_text(&self, text: &Text) -> String {
        let sep = Text::plain_text(" ");
        let joined = sep.join(std::slice::from_ref(text));
        self.print(&joined)
    }

    /// Crop the segments to the console width and render them (`_render_buffer`).
    pub fn finish(&self, segments: &[Segment]) -> String {
        let lines = split_and_crop_lines(segments, self.width, None, false, true);
        let flat: Vec<Segment> = lines.into_iter().flatten().collect();
        render_segments(&flat, self.no_color)
    }

    /// `rich_text_to_str(text)`: print the text and drop the final newline.
    pub fn text_to_str(&self, text: &Text) -> String {
        let mut s = self.print_text(text);
        s.pop();
        s
    }
}

/// `rich.padding.Padding(renderable, (top, right, bottom, left))` with `expand=True`.
pub struct Padding<'a> {
    /// The inner renderable.
    pub renderable: &'a dyn Renderable,
    /// Top, right, bottom, left.
    pub pad: (usize, usize, usize, usize),
    /// The padding style.
    pub style: StyleRef,
}

impl Renderable for Padding<'_> {
    fn render(&self, console: &Console, options: &RenderOptions) -> Vec<Segment> {
        let style = self.style.resolve();
        let style_opt = Some(style);
        let (top, right, bottom, left) = self.pad;
        let width = options.max_width;
        let mut inner = options.with_width(width.saturating_sub(left + right));
        if let Some(h) = inner.height {
            inner.height = Some(h.saturating_sub(top + bottom));
        }
        let lines = console.render_lines(self.renderable, &inner, style_opt, true);
        let mut out = Vec::new();
        let blank = Segment::new(format!("{}\n", " ".repeat(width)), style_opt);
        for _ in 0..top {
            out.push(blank.clone());
        }
        for line in lines {
            if left > 0 {
                out.push(Segment::new(" ".repeat(left), style_opt));
            }
            out.extend(line);
            if right > 0 {
                out.push(Segment::new(" ".repeat(right), style_opt));
            }
            out.push(Segment::line());
        }
        for _ in 0..bottom {
            out.push(blank.clone());
        }
        out
    }

    fn measure(&self, console: &Console, options: &RenderOptions) -> Measurement {
        let max_width = options.max_width;
        let extra = self.pad.1 + self.pad.3;
        if max_width < extra + 1 {
            return Measurement::new(max_width, max_width);
        }
        let m = Measurement::get(console, options, self.renderable);
        Measurement::new(m.minimum + extra, m.maximum + extra).with_maximum(max_width)
    }
}

/// `rich.console.Group(*renderables)`: the renderables one after another.
pub struct Group<'a> {
    /// The renderables.
    pub renderables: Vec<Box<dyn Renderable + 'a>>,
}

impl Renderable for Group<'_> {
    fn render(&self, console: &Console, options: &RenderOptions) -> Vec<Segment> {
        let mut out = Vec::new();
        for r in &self.renderables {
            out.extend(console.render(r.as_ref(), options));
        }
        out
    }

    fn measure(&self, console: &Console, options: &RenderOptions) -> Measurement {
        if self.renderables.is_empty() {
            return Measurement::new(0, 0);
        }
        let ms: Vec<Measurement> = self
            .renderables
            .iter()
            .map(|r| Measurement::get(console, options, r.as_ref()))
            .collect();
        Measurement::new(
            ms.iter().map(|m| m.minimum).max().unwrap_or(0),
            ms.iter().map(|m| m.maximum).max().unwrap_or(0),
        )
    }
}

/// `rich.styled.Styled(renderable, style)`.
pub struct Styled<'a> {
    /// The inner renderable.
    pub renderable: &'a dyn Renderable,
    /// The style applied to every segment.
    pub style: Style,
}

impl Renderable for Styled<'_> {
    fn render(&self, console: &Console, options: &RenderOptions) -> Vec<Segment> {
        let rendered = console.render(self.renderable, options);
        segment::apply_style(rendered, Some(&self.style), None)
    }

    fn measure(&self, console: &Console, options: &RenderOptions) -> Measurement {
        Measurement::get(console, options, self.renderable)
    }
}

/// `console.render_str(text)` for plain strings (no markup tags or emoji codes in the inputs
/// Meerschaum passes).
pub fn render_str(text: &str, style: impl Into<StyleRef>) -> Text {
    Text::new(text, style)
}
