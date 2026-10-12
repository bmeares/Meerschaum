//! `rich.pretty.Pretty` (`traverse`, `Node`, `_Line` expansion) and `rich.highlighter.ReprHighlighter`,
//! over [`PyObj`], a Python object model of the values Meerschaum pretty-prints.

use std::sync::LazyLock;

use super::cells::cell_len;
use super::console::{Console, Measurement, RenderOptions, Renderable};
use super::segment::Segment;
use super::text::Text;

/// A Python object as `pprint` sees it.
#[derive(Debug, Clone, PartialEq)]
pub enum PyObj {
    /// `dict` (insertion order).
    Dict(Vec<(PyObj, PyObj)>),
    /// `list`
    List(Vec<PyObj>),
    /// `tuple`
    Tuple(Vec<PyObj>),
    /// `set` (iteration order).
    Set(Vec<PyObj>),
    /// `str`
    Str(String),
    /// `int`
    Int(i128),
    /// `float`
    Float(f64),
    /// `bool`
    Bool(bool),
    /// `None`
    None,
    /// Any other object, by its `repr()`.
    Other(String),
}

impl PyObj {
    /// `repr(obj)`.
    pub fn repr(&self) -> String {
        match self {
            PyObj::Dict(items) => {
                let inner: Vec<String> = items
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k.repr(), v.repr()))
                    .collect();
                format!("{{{}}}", inner.join(", "))
            }
            PyObj::List(items) => format!(
                "[{}]",
                items.iter().map(PyObj::repr).collect::<Vec<_>>().join(", ")
            ),
            PyObj::Tuple(items) => {
                if items.len() == 1 {
                    format!("({},)", items[0].repr())
                } else {
                    format!(
                        "({})",
                        items.iter().map(PyObj::repr).collect::<Vec<_>>().join(", ")
                    )
                }
            }
            PyObj::Set(items) => {
                if items.is_empty() {
                    "set()".into()
                } else {
                    format!(
                        "{{{}}}",
                        items.iter().map(PyObj::repr).collect::<Vec<_>>().join(", ")
                    )
                }
            }
            PyObj::Str(s) => meerschaum_core::pyfmt::str_repr(s),
            PyObj::Int(i) => i.to_string(),
            PyObj::Float(f) => meerschaum_core::pyfmt::float_repr(*f),
            PyObj::Bool(b) => if *b { "True" } else { "False" }.into(),
            PyObj::None => "None".into(),
            PyObj::Other(r) => r.clone(),
        }
    }

    /// `str(obj)`.
    pub fn py_str(&self) -> String {
        match self {
            PyObj::Str(s) => s.clone(),
            other => other.repr(),
        }
    }

    /// From a core [`meerschaum_core::Value`].
    pub fn from_value(v: &meerschaum_core::Value) -> PyObj {
        use meerschaum_core::Value;
        match v {
            Value::Null => PyObj::None,
            Value::Bool(b) => PyObj::Bool(*b),
            Value::Int(i) => PyObj::Int(*i as i128),
            Value::Float(f) => PyObj::Float(*f),
            Value::Str(s) => PyObj::Str(s.clone()),
            Value::List(l) => PyObj::List(l.iter().map(PyObj::from_value).collect()),
            Value::Map(m) => PyObj::Dict(
                m.iter()
                    .map(|(k, v)| (PyObj::Str(k.clone()), PyObj::from_value(v)))
                    .collect(),
            ),
        }
    }

    /// To JSON (`json.dumps` input); `None` for objects JSON cannot encode.
    pub fn to_value(&self) -> Option<meerschaum_core::Value> {
        use meerschaum_core::{Map, Value};
        Some(match self {
            PyObj::Dict(items) => {
                let mut m = Map::new();
                for (k, v) in items {
                    let key = match k {
                        PyObj::Str(s) => s.clone(),
                        PyObj::Int(i) => i.to_string(),
                        PyObj::Bool(b) => if *b { "true" } else { "false" }.into(),
                        PyObj::None => "null".into(),
                        PyObj::Float(f) => meerschaum_core::pyfmt::float_repr(*f),
                        _ => return None,
                    };
                    m.insert(key, v.to_value()?);
                }
                Value::Map(m)
            }
            PyObj::List(items) | PyObj::Tuple(items) => Value::List(
                items
                    .iter()
                    .map(PyObj::to_value)
                    .collect::<Option<Vec<_>>>()?,
            ),
            PyObj::Set(_) | PyObj::Other(_) => return None,
            PyObj::Str(s) => Value::Str(s.clone()),
            PyObj::Int(i) => Value::Int(i64::try_from(*i).ok()?),
            PyObj::Float(f) => Value::Float(*f),
            PyObj::Bool(b) => Value::Bool(*b),
            PyObj::None => Value::Null,
        })
    }
}

/// `rich.pretty.Node`.
#[derive(Debug, Clone, Default)]
struct Node {
    key_repr: String,
    value_repr: String,
    open_brace: String,
    close_brace: String,
    empty: String,
    last: bool,
    is_tuple: bool,
    children: Option<Vec<Node>>,
    key_separator: &'static str,
    separator: &'static str,
}

impl Node {
    fn new() -> Node {
        Node {
            key_separator: ": ",
            separator: ", ",
            ..Node::default()
        }
    }

    fn tokens(&self, out: &mut Vec<String>) {
        if !self.key_repr.is_empty() {
            out.push(self.key_repr.clone());
            out.push(self.key_separator.to_string());
        }
        if !self.value_repr.is_empty() {
            out.push(self.value_repr.clone());
        } else if let Some(children) = &self.children {
            if !children.is_empty() {
                out.push(self.open_brace.clone());
                if self.is_tuple && children.len() == 1 {
                    children[0].tokens(out);
                    out.push(",".into());
                } else {
                    for child in children {
                        child.tokens(out);
                        if !child.last {
                            out.push(self.separator.to_string());
                        }
                    }
                }
                out.push(self.close_brace.clone());
            } else {
                out.push(self.empty.clone());
            }
        }
    }

    fn check_length(&self, start_length: usize, max_length: usize) -> bool {
        let mut total = start_length;
        let mut tokens = Vec::new();
        self.tokens(&mut tokens);
        for t in tokens {
            total += cell_len(&t);
            if total > max_length {
                return false;
            }
        }
        true
    }

    fn to_str(&self) -> String {
        let mut tokens = Vec::new();
        self.tokens(&mut tokens);
        tokens.concat()
    }
}

/// A container item: the key (for mappings) and the value.
type Item<'a> = (Option<&'a PyObj>, &'a PyObj);

fn traverse(obj: &PyObj, root: bool) -> Node {
    let mut node = Node::new();
    let (open, close, empty, items): (&str, &str, &str, Option<Vec<Item<'_>>>) = match obj {
        PyObj::Dict(items) => (
            "{",
            "}",
            "{}",
            Some(items.iter().map(|(k, v)| (Some(k), v)).collect()),
        ),
        PyObj::List(items) => (
            "[",
            "]",
            "[]",
            Some(items.iter().map(|v| (None, v)).collect()),
        ),
        PyObj::Tuple(items) => (
            "(",
            ")",
            "()",
            Some(items.iter().map(|v| (None, v)).collect()),
        ),
        PyObj::Set(items) => (
            "{",
            "}",
            "set()",
            Some(items.iter().map(|v| (None, v)).collect()),
        ),
        _ => ("", "", "", None),
    };
    match items {
        Some(items) if !items.is_empty() => {
            node.open_brace = open.into();
            node.close_brace = close.into();
            node.last = root;
            let n = items.len();
            let children = items
                .into_iter()
                .enumerate()
                .map(|(i, (k, v))| {
                    let mut child = traverse(v, false);
                    if let Some(k) = k {
                        child.key_repr = k.repr();
                    }
                    child.last = i + 1 == n;
                    child
                })
                .collect();
            node.children = Some(children);
        }
        Some(_) => {
            node.empty = empty.into();
            node.children = Some(Vec::new());
            node.last = root;
        }
        None => {
            node.value_repr = obj.repr();
            node.last = root;
        }
    }
    node.is_tuple = matches!(obj, PyObj::Tuple(_));
    node
}

/// `rich.pretty._Line`.
#[derive(Debug, Clone, Default)]
struct PLine {
    node: Option<Node>,
    text: String,
    suffix: String,
    whitespace: String,
    last: bool,
}

impl PLine {
    fn expandable(&self) -> bool {
        self.node
            .as_ref()
            .and_then(|n| n.children.as_ref())
            .is_some_and(|c| !c.is_empty())
    }

    fn check_length(&self, max_length: usize) -> bool {
        let start = self.whitespace.chars().count() + cell_len(&self.text) + cell_len(&self.suffix);
        self.node
            .as_ref()
            .is_none_or(|n| n.check_length(start, max_length))
    }

    fn expand(&self, indent_size: usize) -> Vec<PLine> {
        let node = self.node.as_ref().expect("expandable");
        let children = node.children.as_ref().expect("children");
        let mut out = Vec::new();
        let text = if node.key_repr.is_empty() {
            node.open_brace.clone()
        } else {
            format!("{}{}{}", node.key_repr, node.key_separator, node.open_brace)
        };
        out.push(PLine {
            text,
            whitespace: self.whitespace.clone(),
            ..PLine::default()
        });
        let child_ws = format!("{}{}", self.whitespace, " ".repeat(indent_size));
        let tuple_of_one = node.is_tuple && children.len() == 1;
        let n = children.len();
        for (i, child) in children.iter().enumerate() {
            let separator = if tuple_of_one { "," } else { node.separator };
            out.push(PLine {
                node: Some(child.clone()),
                whitespace: child_ws.clone(),
                suffix: separator.into(),
                last: i + 1 == n && !tuple_of_one,
                ..PLine::default()
            });
        }
        out.push(PLine {
            text: node.close_brace.clone(),
            whitespace: self.whitespace.clone(),
            suffix: self.suffix.clone(),
            last: self.last,
            ..PLine::default()
        });
        out
    }

    fn to_str(&self) -> String {
        let node = self.node.as_ref().map(Node::to_str).unwrap_or_default();
        if self.last {
            format!("{}{}{}", self.whitespace, self.text, node)
        } else {
            format!(
                "{}{}{}{}",
                self.whitespace,
                self.text,
                node,
                self.suffix.trim_end()
            )
        }
    }
}

/// `pretty_repr(obj, max_width=..., indent_size=4)`.
pub fn pretty_repr(obj: &PyObj, max_width: usize, indent_size: usize) -> String {
    let node = traverse(obj, true);
    let mut lines = vec![PLine {
        node: Some(node),
        ..PLine::default()
    }];
    let mut line_no = 0;
    while line_no < lines.len() {
        let line = &lines[line_no];
        if line.expandable() && !line.check_length(max_width) {
            let expanded = line.expand(indent_size);
            lines.splice(line_no..line_no + 1, expanded);
        }
        line_no += 1;
    }
    lines
        .iter()
        .map(PLine::to_str)
        .collect::<Vec<_>>()
        .join("\n")
}

/// `ReprHighlighter.highlights`.
static REPR_HIGHLIGHTS: LazyLock<Vec<fancy_regex::Regex>> = LazyLock::new(|| {
    let combined = [
        r"(?P<ipv4>[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})",
        r"(?P<ipv6>([A-Fa-f0-9]{1,4}::?){1,7}[A-Fa-f0-9]{1,4})",
        r"(?P<eui64>(?:[0-9A-Fa-f]{1,2}-){7}[0-9A-Fa-f]{1,2}|(?:[0-9A-Fa-f]{1,2}:){7}[0-9A-Fa-f]{1,2}|(?:[0-9A-Fa-f]{4}\.){3}[0-9A-Fa-f]{4})",
        r"(?P<eui48>(?:[0-9A-Fa-f]{1,2}-){5}[0-9A-Fa-f]{1,2}|(?:[0-9A-Fa-f]{1,2}:){5}[0-9A-Fa-f]{1,2}|(?:[0-9A-Fa-f]{4}\.){2}[0-9A-Fa-f]{4})",
        r"(?P<uuid>[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12})",
        r"(?P<call>[\w.]*?)\(",
        r"\b(?P<bool_true>True)\b|\b(?P<bool_false>False)\b|\b(?P<none>None)\b",
        r"(?P<ellipsis>\.\.\.)",
        r"(?P<number_complex>(?<!\w)(?:\-?[0-9]+\.?[0-9]*(?:e[-+]?\d+?)?)(?:[-+](?:[0-9]+\.?[0-9]*(?:e[-+]?\d+)?))?j)",
        r"(?P<number>(?<!\w)\-?[0-9]+\.?[0-9]*(e[-+]?\d+?)?\b|0x[0-9a-fA-F]*)",
        r"(?P<path>\B(/[-\w._+]+)*\/)(?P<filename>[-\w._+]*)?",
        r#"(?<![\\\w])(?P<str>b?'''.*?(?<!\\)'''|b?'.*?(?<!\\)'|b?""".*?(?<!\\)"""|b?".*?(?<!\\)")"#,
        r"(?P<url>(file|https|http|ws|wss)://[-0-9a-zA-Z$_+!`(),.?/;:&=%#~@]*)",
    ]
    .join("|");
    [
        r"(?P<tag_start><)(?P<tag_name>[-\w.:|]*)(?P<tag_contents>[\w\W]*)(?P<tag_end>>)"
            .to_string(),
        r#"(?P<attrib_name>[\w_]{1,50})=(?P<attrib_value>"?[\w_]+"?)?"#.to_string(),
        r"(?P<brace>[\]\[{}()])".to_string(),
        combined,
    ]
    .iter()
    .map(|p| fancy_regex::Regex::new(p).expect("valid ReprHighlighter regex"))
    .collect()
});

/// `ReprHighlighter()(text)`.
pub fn highlight_repr(text: &mut Text) {
    for re in REPR_HIGHLIGHTS.iter() {
        text.highlight_regex(re, "repr.");
    }
}

/// `rich.pretty.Pretty(obj)` with the console highlighter.
pub struct Pretty<'a> {
    /// The object.
    pub obj: &'a PyObj,
    /// Apply `ReprHighlighter`.
    pub highlight: bool,
}

impl Renderable for Pretty<'_> {
    fn render(&self, console: &Console, options: &RenderOptions) -> Vec<Segment> {
        let pretty = pretty_repr(self.obj, options.max_width, 4);
        let mut text = Text::from_ansi(&pretty, "pretty");
        text.justify = options.justify;
        text.overflow = options.overflow;
        // `pick_bool(self.no_wrap, options.no_wrap)` with `Pretty.no_wrap=False`.
        text.no_wrap = Some(false);
        if self.highlight {
            highlight_repr(&mut text);
        }
        text.render_with(console, options)
    }

    fn measure(&self, _console: &Console, options: &RenderOptions) -> Measurement {
        let pretty = pretty_repr(self.obj, options.max_width, 4);
        let w = pretty.lines().map(cell_len).max().unwrap_or(0);
        Measurement::new(w, w)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expands_when_too_wide() {
        let obj = PyObj::Dict(vec![
            (PyObj::Str("a".into()), PyObj::Int(1)),
            (
                PyObj::Str("b".into()),
                PyObj::List(vec![PyObj::Bool(true), PyObj::None]),
            ),
        ]);
        assert_eq!(pretty_repr(&obj, 80, 4), "{'a': 1, 'b': [True, None]}");
        assert_eq!(
            pretty_repr(&obj, 10, 4),
            "{\n    'a': 1,\n    'b': [\n        True,\n        None\n    ]\n}"
        );
    }
}
