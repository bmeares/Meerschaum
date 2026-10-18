//! `rich.panel.Panel`, `rich.tree.Tree` and `rich.columns.Columns`.

use super::cells::cell_len;
use super::console::{Console, Measurement, Padding, RenderOptions, Renderable, Styled};
use super::segment::{self, Segment};
use super::style::{Style, StyleRef, attr};
use super::table::{BoxChars, Column, ROUNDED, Table};
use super::text::Text;

/// `rich.panel.Panel(renderable, title=..., border_style=..., expand=True)`.
pub struct Panel<'a> {
    /// The content.
    pub renderable: Box<dyn Renderable + 'a>,
    /// The box.
    pub box_chars: BoxChars,
    /// `title` (a `Text`).
    pub title: Option<Text>,
    /// `title_align`: `left`, `center` or `right`.
    pub title_align: &'static str,
    /// `expand`.
    pub expand: bool,
    /// `style`.
    pub style: StyleRef,
    /// `border_style`.
    pub border_style: StyleRef,
    /// `padding` as `(top, right, bottom, left)`.
    pub padding: (usize, usize, usize, usize),
}

impl<'a> Panel<'a> {
    /// `Panel(renderable)` with rich's defaults.
    pub fn new(renderable: Box<dyn Renderable + 'a>) -> Panel<'a> {
        Panel {
            renderable,
            box_chars: ROUNDED,
            title: None,
            title_align: "center",
            expand: true,
            style: StyleRef::Name("none".into()),
            border_style: StyleRef::Name("none".into()),
            padding: (0, 1, 0, 1),
        }
    }

    fn title_text(&self) -> Option<Text> {
        let title = self.title.as_ref().filter(|t| !t.is_empty())?;
        let mut t = title.clone();
        t.end = String::new();
        let plain = t.plain().replace('\n', " ");
        t.set_plain(&plain);
        t.no_wrap = Some(true);
        t.pad(1);
        Some(t)
    }
}

fn align_text(text: &Text, width: usize, align: &str, character: char, style: Style) -> Text {
    let mut text = text.clone();
    text.truncate(width, None, false);
    let excess = width.saturating_sub(cell_len(&text.plain()));
    if text.style.is_set() {
        let s = text.style.resolve();
        text.stylize(s, 0, None);
    }
    if excess == 0 {
        return text;
    }
    let fill = |n: usize| Text::new(&character.to_string().repeat(n), style);
    let mut out = match align {
        "left" => Text::assemble(vec![text, fill(excess)]),
        "center" => {
            let left = excess / 2;
            Text::assemble(vec![fill(left), text, fill(excess - left)])
        }
        _ => Text::assemble(vec![fill(excess), text]),
    };
    out.no_wrap = Some(true);
    out.end = String::new();
    out
}

impl Renderable for Panel<'_> {
    fn render(&self, console: &Console, options: &RenderOptions) -> Vec<Segment> {
        let any_padding = self.padding != (0, 0, 0, 0);
        let padded;
        let renderable: &dyn Renderable = if any_padding {
            padded = Padding {
                renderable: self.renderable.as_ref(),
                pad: self.padding,
                style: StyleRef::default(),
            };
            &padded
        } else {
            self.renderable.as_ref()
        };
        let style = self.style.resolve();
        let border_style = style.add(&self.border_style.resolve());
        let border = Some(border_style);
        let width = options.max_width;
        let box_chars = self.box_chars;
        let mut title_text = self.title_text();
        if let Some(t) = &mut title_text {
            t.stylize_before(border_style);
        }
        let mut child_width = if self.expand {
            width.saturating_sub(2)
        } else {
            console_measure(
                console,
                renderable,
                &options.with_width(width.saturating_sub(2)),
            )
            .maximum
        };
        if let Some(t) = &title_text {
            child_width = options
                .max_width
                .saturating_sub(2)
                .min(child_width.max(t.cell_len() + 2));
        }
        let width = child_width + 2;
        let mut child_options = options.with_width(child_width);
        child_options.highlight = false;
        let lines = console.render_lines(renderable, &child_options, Some(style), true);
        let (mid_left, mid_right) = box_chars.mid_chars();
        let (top_left, top, top_right) = box_chars.top_chars();
        let mut out = Vec::new();
        match title_text {
            Some(t) if width > 4 => {
                let t = align_text(&t, width - 4, self.title_align, top, border_style);
                out.push(Segment::new(format!("{top_left}{top}"), border));
                out.extend(console.render(&t, &child_options.with_width(width - 4)));
                out.push(Segment::new(format!("{top}{top_right}"), border));
            }
            _ => out.push(Segment::new(box_chars.get_top(&[width - 2]), border)),
        }
        out.push(Segment::line());
        for line in lines {
            out.push(Segment::new(mid_left.to_string(), border));
            out.extend(line);
            out.push(Segment::new(mid_right.to_string(), border));
            out.push(Segment::line());
        }
        out.push(Segment::new(box_chars.get_bottom(&[width - 2]), border));
        out.push(Segment::line());
        out
    }

    fn measure(&self, console: &Console, options: &RenderOptions) -> Measurement {
        let title = self.title_text();
        let padding = self.padding.1 + self.padding.3;
        let o = options.with_width(options.max_width.saturating_sub(padding + 2));
        let mut max = Measurement::get(console, &o, self.renderable.as_ref()).maximum;
        if let Some(t) = &title {
            max = max.max(Measurement::get(console, &o, t).maximum);
        }
        let width = max + padding + 2;
        Measurement::new(width, width)
    }
}

/// `console.measure(renderable, options)`.
fn console_measure(console: &Console, r: &dyn Renderable, options: &RenderOptions) -> Measurement {
    Measurement::get(console, options, r)
}

/// `rich.tree.Tree`.
pub struct Tree<'a> {
    /// The label.
    pub label: Box<dyn Renderable + 'a>,
    /// `style`.
    pub style: StyleRef,
    /// `guide_style`.
    pub guide_style: StyleRef,
    /// The children.
    pub children: Vec<Tree<'a>>,
}

const TREE_GUIDES: [[&str; 4]; 3] = [
    ["    ", "│   ", "├── ", "└── "],
    ["    ", "┃   ", "┣━━ ", "┗━━ "],
    ["    ", "║   ", "╠══ ", "╚══ "],
];
const SPACE: usize = 0;
const CONTINUE: usize = 1;
const FORK: usize = 2;
const END: usize = 3;

impl<'a> Tree<'a> {
    /// `Tree(label, guide_style=...)`.
    pub fn new(label: Box<dyn Renderable + 'a>, guide_style: impl Into<StyleRef>) -> Tree<'a> {
        Tree {
            label,
            style: StyleRef::Name("tree".into()),
            guide_style: guide_style.into(),
            children: Vec::new(),
        }
    }

    /// `tree.add(label)`: the child inherits the parent's styles.
    pub fn add(&mut self, label: Box<dyn Renderable + 'a>) -> &mut Tree<'a> {
        let child = Tree {
            label,
            style: self.style.clone(),
            guide_style: self.guide_style.clone(),
            children: Vec::new(),
        };
        self.children.push(child);
        self.children.last_mut().expect("child")
    }
}

fn make_guide(index: usize, style: Style) -> Segment {
    let guide = if style.has(attr::BOLD) {
        1
    } else if style.has(attr::UNDERLINE2) {
        2
    } else {
        0
    };
    Segment::new(TREE_GUIDES[guide][index], Some(style))
}

impl Renderable for Tree<'_> {
    fn render(&self, console: &Console, options: &RenderOptions) -> Vec<Segment> {
        let mut out = Vec::new();
        let null = Style::null();
        let guide_style = self.guide_style.resolve();
        let mut levels: Vec<Segment> = vec![make_guide(CONTINUE, guide_style)];
        // The stack holds (siblings, next index).
        let root = [self];
        let mut stack: Vec<(Vec<&Tree>, usize)> = vec![(root.to_vec(), 0)];
        let mut guide_style_stack: Vec<Style> = vec![self.guide_style.resolve()];
        let mut style_stack: Vec<Style> = vec![self.style.resolve()];
        let remove_guide_styles = Style::with_attrs(attr::BOLD | attr::UNDERLINE2, false);
        while let Some((nodes, idx)) = stack.pop() {
            if idx >= nodes.len() {
                levels.pop();
                if !levels.is_empty() {
                    let last_style = levels.last().and_then(|l| l.style).unwrap_or(null);
                    let n = levels.len();
                    levels[n - 1] = make_guide(FORK, last_style);
                    guide_style_stack.pop();
                    style_stack.pop();
                }
                continue;
            }
            let node = nodes[idx];
            let last = idx + 1 == nodes.len();
            stack.push((nodes.clone(), idx + 1));
            if last {
                let n = levels.len();
                let s = levels[n - 1].style.unwrap_or(null);
                levels[n - 1] = make_guide(END, s);
            }
            let gs_current = *guide_style_stack.last().unwrap_or(&null);
            let guide_style = gs_current.add(&node.guide_style.resolve());
            let s_current = *style_stack.last().unwrap_or(&null);
            let style = s_current.add(&node.style.resolve());
            let mut prefix: Vec<Segment> = levels[1.min(levels.len())..].to_vec();
            let prefix_width: usize = prefix.iter().map(Segment::cell_length).sum();
            let styled = Styled {
                renderable: node.label.as_ref(),
                style,
            };
            let mut o = options.with_width(options.max_width.saturating_sub(prefix_width));
            o.highlight = false;
            o.height = None;
            let lines = console.render_lines(&styled, &o, None, options.justify.is_some());
            for (i, line) in lines.into_iter().enumerate() {
                if !prefix.is_empty() {
                    out.extend(segment::apply_style(
                        prefix.clone(),
                        Some(&style.background_style()),
                        Some(&remove_guide_styles),
                    ));
                }
                out.extend(line);
                out.push(Segment::line());
                if i == 0 && !prefix.is_empty() {
                    let n = prefix.len();
                    let s = prefix[n - 1].style.unwrap_or(null);
                    prefix[n - 1] = make_guide(if last { SPACE } else { CONTINUE }, s);
                }
            }
            if !node.children.is_empty() {
                let n = levels.len();
                let s = levels[n - 1].style.unwrap_or(null);
                levels[n - 1] = make_guide(if last { SPACE } else { CONTINUE }, s);
                levels.push(make_guide(
                    if node.children.len() == 1 { END } else { FORK },
                    guide_style,
                ));
                // `StyleStack.push` combines with the current style.
                style_stack.push(s_current.add(&node.style.resolve()));
                guide_style_stack.push(gs_current.add(&node.guide_style.resolve()));
                stack.push((node.children.iter().collect(), 0));
            }
        }
        out
    }

    fn measure(&self, console: &Console, options: &RenderOptions) -> Measurement {
        let mut minimum = 0;
        let mut maximum = 0;
        fn walk(
            t: &Tree,
            level: usize,
            console: &Console,
            options: &RenderOptions,
            min: &mut usize,
            max: &mut usize,
        ) {
            let m = Measurement::get(console, options, t.label.as_ref());
            let indent = level * 4;
            *min = (*min).max(m.minimum + indent);
            *max = (*max).max(m.maximum + indent);
            for c in &t.children {
                walk(c, level + 1, console, options, min, max);
            }
        }
        walk(self, 0, console, options, &mut minimum, &mut maximum);
        Measurement::new(minimum, maximum)
    }
}

impl<'a> Renderable for Box<dyn Renderable + 'a> {
    fn render(&self, console: &Console, options: &RenderOptions) -> Vec<Segment> {
        self.as_ref().render(console, options)
    }

    fn measure(&self, console: &Console, options: &RenderOptions) -> Measurement {
        self.as_ref().measure(console, options)
    }
}

/// `rich.columns.Columns(renderables)` with the default padding `(0, 1)`.
pub struct Columns<'a> {
    /// The renderables.
    pub renderables: Vec<Box<dyn Renderable + 'a>>,
    /// `equal`: every column as wide as the widest renderable (each wrapped in `Constrain`).
    pub equal: bool,
}

/// `rich.constrain.Constrain(renderable, width)`.
struct Constrain<'a> {
    renderable: &'a dyn Renderable,
    width: usize,
}

impl Renderable for Constrain<'_> {
    fn render(&self, console: &Console, options: &RenderOptions) -> Vec<Segment> {
        let o = options.with_width(self.width.min(options.max_width));
        console.render(self.renderable, &o)
    }

    fn measure(&self, console: &Console, options: &RenderOptions) -> Measurement {
        let o = options.with_width(self.width);
        Measurement::get(console, &o, self.renderable)
    }
}

impl Renderable for Columns<'_> {
    fn render(&self, console: &Console, options: &RenderOptions) -> Vec<Segment> {
        if self.renderables.is_empty() {
            return Vec::new();
        }
        let padding = (0usize, 1usize, 0usize, 1usize);
        let width_padding = padding.1.max(padding.3);
        let max_width = options.max_width;
        let mut renderable_widths: Vec<usize> = self
            .renderables
            .iter()
            .map(|r| Measurement::get(console, options, r.as_ref()).maximum)
            .collect();
        if self.equal {
            let max = renderable_widths.iter().copied().max().unwrap_or(0);
            renderable_widths = vec![max; renderable_widths.len()];
        }
        let equal_width = renderable_widths.first().copied().unwrap_or(0);
        let item_count = self.renderables.len();
        let iter_widths = |column_count: usize| -> Vec<usize> {
            let mut v: Vec<usize> = renderable_widths.clone();
            if !item_count.is_multiple_of(column_count) {
                v.extend(std::iter::repeat_n(
                    0,
                    column_count - item_count % column_count,
                ));
            }
            v
        };
        let mut column_count = item_count;
        while column_count > 1 {
            let mut widths: Vec<usize> = Vec::new();
            let mut column_no = 0usize;
            let mut broke = false;
            for w in iter_widths(column_count) {
                if widths.len() <= column_no {
                    widths.resize(column_no + 1, 0);
                }
                widths[column_no] = widths[column_no].max(w);
                let total = widths.iter().sum::<usize>() + width_padding * (widths.len() - 1);
                if total > max_width {
                    column_count = widths.len() - 1;
                    broke = true;
                    break;
                }
                column_no = (column_no + 1) % column_count;
            }
            if !broke {
                break;
            }
        }
        let column_count = column_count.max(1);
        let mut table = Table::grid(padding, true, false);
        let mut cells: Vec<Option<&dyn Renderable>> =
            self.renderables.iter().map(|r| Some(r.as_ref())).collect();
        if !item_count.is_multiple_of(column_count) {
            for _ in 0..(column_count - item_count % column_count) {
                cells.push(None);
            }
        }
        for _ in 0..column_count.min(cells.len()) {
            table.add_column(Column::default());
        }
        let equal = self.equal;
        for row in cells.chunks(column_count) {
            let row: Vec<Option<Box<dyn Renderable + '_>>> = row
                .iter()
                .map(|c| {
                    c.map(|r| {
                        if equal {
                            Box::new(Constrain {
                                renderable: r,
                                width: equal_width,
                            }) as Box<dyn Renderable>
                        } else {
                            Box::new(Ref(r)) as Box<dyn Renderable>
                        }
                    })
                })
                .collect();
            table.add_row(row);
        }
        console.render(&table, options)
    }
}

/// A borrowed renderable as a boxed one.
struct Ref<'a>(&'a dyn Renderable);

impl Renderable for Ref<'_> {
    fn render(&self, console: &Console, options: &RenderOptions) -> Vec<Segment> {
        self.0.render(console, options)
    }

    fn measure(&self, console: &Console, options: &RenderOptions) -> Measurement {
        self.0.measure(console, options)
    }
}
