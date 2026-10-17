//! `rich.json.JSON(json_str)` printed with `get_console().print(...)`: `json.dumps(data,
//! indent=2, ensure_ascii=False)` highlighted with rich's `JSONHighlighter`.

use std::sync::LazyLock;

use meerschaum_core::pyfmt::{JsonStyle, json_dumps};
use meerschaum_core::value::Value;

use crate::format::Formatter;
use crate::format::rich::style::StyleRef;
use crate::format::rich::text::{Span, Text};

const JSON_STR: &str = r#"(?<![\\\w])(?P<str>b?".*?(?<!\\)")"#;

static HIGHLIGHT: LazyLock<fancy_regex::Regex> = LazyLock::new(|| {
    let combined = [
        r"(?P<brace>[\{\[\(\)\]\}])",
        r"\b(?P<bool_true>true)\b|\b(?P<bool_false>false)\b|\b(?P<null>null)\b",
        r"(?P<number>(?<!\w)\-?[0-9]+\.?[0-9]*(e[\-\+]?\d+?)?\b|0x[0-9a-fA-F]*)",
        JSON_STR,
    ]
    .join("|");
    fancy_regex::Regex::new(&combined).expect("JSONHighlighter regex")
});

static STR_RE: LazyLock<fancy_regex::Regex> =
    LazyLock::new(|| fancy_regex::Regex::new(JSON_STR).expect("JSON_STR regex"));

/// The highlighted `Text` of `JSON.from_data(value)`.
pub fn json_text(value: &Value) -> Text {
    let dumped = json_dumps(value, JsonStyle::Indent(2), false);
    let mut text = Text::plain_text(&dumped);
    text.highlight_regex(&HIGHLIGHT, "json.");
    // `JSONHighlighter.highlight`: a string followed (after whitespace) by `:` is a key.
    let plain: Vec<char> = dumped.chars().collect();
    let mut byte_to_char = vec![0usize; dumped.len() + 1];
    for (ci, (bi, _)) in dumped.char_indices().enumerate() {
        byte_to_char[bi] = ci;
    }
    byte_to_char[dumped.len()] = plain.len();
    let mut pos = 0usize;
    while pos <= dumped.len() {
        let Ok(Some(m)) = STR_RE.find_from_pos(&dumped, pos) else {
            break;
        };
        let (start, end) = (byte_to_char[m.start()], byte_to_char[m.end()]);
        let mut cursor = end;
        while cursor < plain.len() {
            let c = plain[cursor];
            cursor += 1;
            if c == ':' {
                text.spans.push(Span {
                    start,
                    end,
                    style: StyleRef::Name("json.key".into()),
                });
            } else if matches!(c, ' ' | '\n' | '\r' | '\t') {
                continue;
            }
            break;
        }
        pos = if m.end() > m.start() {
            m.end()
        } else {
            m.end() + 1
        };
    }
    text
}

/// `get_console().print(rich.json.JSON(json.dumps(value)))`.
pub fn print_json(f: &Formatter<'_>, value: &Value) -> String {
    f.console().print_text(&json_text(value))
}
