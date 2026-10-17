//! `{{ Pipe() }}` templating (§6.3): a port of `replace_pipes_syntax` and friends from
//! `meerschaum/utils/pipes.py`, split into a parser that produces an AST ([`ParsedText`]) and an
//! evaluator that asks a [`TemplateResolver`] for anything that needs an instance (a referenced
//! pipe's registered parameters, its quoted table name).
//!
//! Syntax:
//!
//! - `{{ Pipe('ck', 'mk', 'lk', instance='sql:main') }}` (or `mrsm.Pipe`) with no attribute chain
//!   becomes the pipe's table reference.
//! - `{{ Pipe(...).columns['datetime'] }}`, `.columns`, `.dtypes`, `.target`, `.indices`: an
//!   attribute chain of `.name` and `[constant]` accessors.
//! - `{{ self.metric }}`, `{{ self.parameters['x']['y'] }}` when resolving a pipe's own
//!   parameters (`self.parameters` reads the raw, unresolved parameters).
//!
//! When the whole (stripped) text is a single template, the resolved value keeps its type (a
//! dict for `.columns`); otherwise values are spliced into the text with `json.dumps` (dicts,
//! lists, numbers, booleans, `None`) or `str()`.
//!
//! [`referenced_pipes`] and [`referenced_pipes_in_parameters`] statically extract every referenced
//! pipe for the engine's implicit edges (§5.1).

use std::sync::LazyLock;

use regex::Regex;

use crate::pipe::{Pipe, PipeDefaults};
use crate::pyfmt::{JsonStyle, json_dumps, literal_eval_constant, parse_arguments_str, py_str};
use crate::value::{Map, Value};

// PARITY: v4's regex does not treat backslashes or extra surrounding braces as escapes;
// `\{{ self.metric }}` and `{{{ self.metric }}}` still substitute the inner template.
/// The `{{ Pipe(...) }}` pattern from `replace_pipes_syntax`.
pub const PIPE_PATTERN: &str = r"\{\{\s*(?:mrsm\.)?Pipe\((.*?)\)((?:\.[\w]+|\[[^\]]+\])*)\s*\}\}";
/// The `{{ self... }}` pattern from `replace_pipes_syntax`.
pub const SELF_PATTERN: &str = r"\{\{\s*self((?:\.[\w]+|\[[^\]]+\])*)\s*\}\}";

static PIPE_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(PIPE_PATTERN).expect("valid regex"));
static SELF_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(SELF_PATTERN).expect("valid regex"));

/// One step of an attribute chain.
#[derive(Debug, Clone, PartialEq)]
pub enum Accessor {
    /// `.name`
    Attr(String),
    /// `[constant]` (a string, number, `None`, `True` or `False`).
    Index(Value),
}

/// What a template refers to.
#[derive(Debug, Clone, PartialEq)]
pub enum TemplateTarget {
    /// `Pipe(...)` with the raw argument source and its parsed `(args, kwargs)`.
    Pipe {
        /// The text between the parentheses.
        args_src: String,
        /// The parsed arguments, or the parse error.
        call: Result<(Vec<Value>, Map), String>,
    },
    /// `self`.
    SelfPipe,
}

/// A parsed `{{ ... }}` occurrence.
#[derive(Debug, Clone, PartialEq)]
pub struct Template {
    /// The full matched text (`match.group(0)`).
    pub raw: String,
    /// Byte offset of the match in the source text.
    pub start: usize,
    /// Byte offset of the end of the match.
    pub end: usize,
    /// `Pipe(...)` or `self`.
    pub target: TemplateTarget,
    /// The attribute chain source (`.columns['datetime']`).
    pub chain_src: String,
    /// The parsed attribute chain, or the reason it cannot be evaluated.
    pub chain: Result<Vec<Accessor>, String>,
}

/// A text parsed for templates. Templates are ordered like Python's matches: every
/// `Pipe(...)` match first, then every `self` match.
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedText {
    /// The source text.
    pub text: String,
    /// The templates found.
    pub templates: Vec<Template>,
}

const PY_KEYWORDS: &[&str] = &[
    "False", "None", "True", "and", "as", "assert", "async", "await", "break", "class", "continue",
    "def", "del", "elif", "else", "except", "finally", "for", "from", "global", "if", "import",
    "in", "is", "lambda", "nonlocal", "not", "or", "pass", "raise", "return", "try", "while",
    "with", "yield",
];

/// Parse an attribute chain like `.columns['datetime']` the way `evaluate_pipe_access_chain`'s
/// restricted `ast` evaluator accepts it.
pub fn parse_chain(src: &str) -> Result<Vec<Accessor>, String> {
    let mut out = Vec::new();
    let chars: Vec<char> = src.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '.' => {
                let start = i + 1;
                let mut j = start;
                while j < chars.len() && (chars[j].is_alphanumeric() || chars[j] == '_') {
                    j += 1;
                }
                let name: String = chars[start..j].iter().collect();
                if name.is_empty()
                    || name.chars().next().is_some_and(|c| c.is_numeric())
                    || PY_KEYWORDS.contains(&name.as_str())
                {
                    return Err(format!("invalid syntax in access chain {src:?}"));
                }
                out.push(Accessor::Attr(name));
                i = j;
            }
            '[' => {
                let start = i + 1;
                let mut j = start;
                while j < chars.len() && chars[j] != ']' {
                    j += 1;
                }
                if j >= chars.len() {
                    return Err(format!("unclosed '[' in access chain {src:?}"));
                }
                let inner: String = chars[start..j].iter().collect();
                let key = literal_eval_constant(&inner)
                    .map_err(|e| format!("Unsupported AST node in {inner:?}: {e}"))?;
                out.push(Accessor::Index(key));
                i = j + 1;
            }
            c if c.is_whitespace() => i += 1,
            _ => return Err(format!("invalid syntax in access chain {src:?}")),
        }
    }
    Ok(out)
}

/// Parse a text for `{{ Pipe(...) }}` templates, and `{{ self... }}` templates when
/// `include_self` (Python only looks for them when resolving a pipe's own parameters).
pub fn parse(text: &str, include_self: bool) -> ParsedText {
    let mut templates = Vec::new();
    for caps in PIPE_RE.captures_iter(text) {
        let m = caps.get(0).expect("group 0");
        let args_src = caps.get(1).map_or("", |g| g.as_str()).to_string();
        let chain_src = caps.get(2).map_or("", |g| g.as_str()).to_string();
        templates.push(Template {
            raw: m.as_str().to_string(),
            start: m.start(),
            end: m.end(),
            target: TemplateTarget::Pipe {
                call: parse_arguments_str(&args_src),
                args_src,
            },
            chain: parse_chain(&chain_src),
            chain_src,
        });
    }
    if include_self {
        for caps in SELF_RE.captures_iter(text) {
            let m = caps.get(0).expect("group 0");
            let chain_src = caps.get(1).map_or("", |g| g.as_str()).to_string();
            templates.push(Template {
                raw: m.as_str().to_string(),
                start: m.start(),
                end: m.end(),
                target: TemplateTarget::SelfPipe,
                chain: parse_chain(&chain_src),
                chain_src,
            });
        }
    }
    ParsedText {
        text: text.to_string(),
        templates,
    }
}

/// What the evaluator needs from the outside world. Every method has a pure-data default.
pub trait TemplateResolver {
    /// The configuration defaults for constructing referenced pipes.
    fn defaults(&self) -> PipeDefaults {
        PipeDefaults::default()
    }

    /// The table reference for `{{ Pipe(...) }}` with no attribute chain. Python returns the
    /// quoted, schema-qualified name (`sql_item_name`) on SQL instances and `pipe.target`
    /// elsewhere; the default is `pipe.target` without truncation.
    fn table_reference(&self, pipe: &Pipe) -> Result<String, String> {
        pipe.target(None).map_err(|e| e.to_string())
    }

    /// Load a referenced pipe's registered state (its parameters from the instance). The
    /// default returns the pipe as constructed.
    fn load_pipe(&self, pipe: &Pipe) -> Pipe {
        pipe.clone()
    }

    /// `getattr(pipe, name)` for a loaded pipe. The default serves the pure-data attributes.
    fn attribute(&self, pipe: &Pipe, name: &str) -> Result<Value, String> {
        pipe.attribute(name)
            .ok_or_else(|| format!("'Pipe' object has no attribute '{name}'"))
    }

    /// `pipe.attributes['parameters']`: the raw parameters (used for `{{ self.parameters }}`).
    fn raw_parameters(&self, pipe: &Pipe) -> Map {
        pipe.raw_parameters().clone()
    }

    /// The config registry used to resolve `MRSM{}` symlinks in parameters (none by default).
    fn config(&self) -> Option<&crate::config::Config> {
        None
    }
}

/// A resolver that serves registered stub pipes from memory, keyed by their four keys.
#[derive(Debug, Clone, Default)]
pub struct InMemoryResolver {
    /// Configuration defaults.
    pub defaults: PipeDefaults,
    /// The "registered" pipes; a referenced pipe with the same keys takes these parameters.
    pub pipes: Vec<Pipe>,
}

impl TemplateResolver for InMemoryResolver {
    fn defaults(&self) -> PipeDefaults {
        self.defaults.clone()
    }

    fn load_pipe(&self, pipe: &Pipe) -> Pipe {
        match self.pipes.iter().find(|p| *p == pipe) {
            Some(registered) => {
                let mut out = pipe.clone();
                out.set_parameters(crate::value::apply_patch_maps(
                    registered.raw_parameters(),
                    pipe.raw_parameters(),
                ));
                out
            }
            None => pipe.clone(),
        }
    }
}

fn index_value(value: &Value, key: &Value) -> Result<Value, String> {
    match (value, key) {
        (Value::Map(m), Value::Str(k)) => m
            .get(k)
            .cloned()
            .ok_or_else(|| format!("KeyError: {}", crate::pyfmt::py_repr(key))),
        (Value::Map(_), other) => Err(format!("KeyError: {}", crate::pyfmt::py_repr(other))),
        (Value::List(l), Value::Int(_) | Value::Bool(_)) => {
            let i = key.as_i64().unwrap_or(0);
            usize::try_from(i)
                .ok()
                .and_then(|i| l.get(i).cloned())
                .ok_or_else(|| "IndexError: list index out of range".to_string())
        }
        (Value::Str(s), Value::Int(_) | Value::Bool(_)) => {
            let i = key.as_i64().unwrap_or(0);
            usize::try_from(i)
                .ok()
                .and_then(|i| s.chars().nth(i))
                .map(|c| Value::Str(c.to_string()))
                .ok_or_else(|| "IndexError: string index out of range".to_string())
        }
        (v, k) => Err(format!(
            "TypeError: '{}' object is not subscriptable with {}",
            v.type_name(),
            k.type_name()
        )),
    }
}

/// The maximum nesting of template resolution (a referenced pipe whose parameters reference
/// another pipe, ...). Python is bounded by its recursion limit instead; deeper chains (in
/// practice, self-referential templates) fail and are left as written, as in Python.
pub const MAX_TEMPLATE_DEPTH: usize = 32;

/// Attributes that do not depend on parameters, so they need no parameter resolution.
const KEY_ATTRIBUTES: &[&str] = &[
    "connector_keys",
    "connector_key",
    "connector",
    "metric_key",
    "metric",
    "location_key",
    "location",
    "instance_keys",
    "instance",
    "temporary",
    "keys",
    "meta",
];

/// Evaluate one template. `self_pipe` is the pipe whose parameters are being resolved.
pub fn evaluate(
    template: &Template,
    self_pipe: Option<&Pipe>,
    resolver: &dyn TemplateResolver,
) -> Result<Value, String> {
    evaluate_at_depth(template, self_pipe, resolver, 0, &mut Vec::new())
}

/// A copy of `pipe` with its parameters resolved (references, templates, symlinks), as Python's
/// `pipe.parameters` property returns them.
fn with_resolved_parameters(
    pipe: &Pipe,
    resolver: &dyn TemplateResolver,
    depth: usize,
    warnings: &mut Vec<String>,
) -> Result<Pipe, String> {
    if depth >= MAX_TEMPLATE_DEPTH {
        return Err("maximum recursion depth exceeded".to_string());
    }
    let resolved = crate::pipe::resolve::resolve_at_depth(pipe, resolver, depth + 1)
        .map_err(|e| e.to_string())?;
    warnings.extend(resolved.warnings);
    let mut out = pipe.clone();
    out.set_parameters(resolved.parameters);
    out.resolution_snapshot = true;
    Ok(out)
}

/// [`evaluate`] at a nesting depth; warnings from nested parameter resolution (Python emits
/// them as they happen) are appended to `warnings`.
pub(crate) fn evaluate_at_depth(
    template: &Template,
    self_pipe: Option<&Pipe>,
    resolver: &dyn TemplateResolver,
    depth: usize,
    warnings: &mut Vec<String>,
) -> Result<Value, String> {
    // PARITY: Python tests `'self' in match.group(0)`, so a `Pipe(...)` template whose text
    // merely contains "self" is evaluated as `self` with its argument text as the access chain
    // (`utils/pipes.py:183`), which then fails to parse and is left as-is.
    let (pipe, chain) = match (&template.target, self_pipe) {
        (_, Some(sp)) if template.raw.contains("self") => {
            let chain_src = match &template.target {
                TemplateTarget::SelfPipe => template.chain_src.clone(),
                TemplateTarget::Pipe { args_src, .. } => args_src.clone(),
            };
            (sp.clone(), parse_chain(&chain_src)?)
        }
        (TemplateTarget::SelfPipe, None) => return Err("`self` outside of a pipe".to_string()),
        (TemplateTarget::SelfPipe, Some(sp)) => (sp.clone(), template.chain.clone()?),
        (TemplateTarget::Pipe { call, .. }, _) => {
            let (args, mut kwargs) = call.clone()?;
            if let Some(sp) = self_pipe {
                if !["instance", "mrsm_instance", "instance_keys"]
                    .iter()
                    .any(|k| kwargs.contains_key(*k))
                {
                    kwargs.insert(
                        "instance".into(),
                        Value::Str(sp.instance_keys().to_string()),
                    );
                }
            }
            let pipe =
                Pipe::from_call(&args, &kwargs, &resolver.defaults()).map_err(|e| e.to_string())?;
            (pipe, template.chain.clone()?)
        }
    };

    let is_self = self_pipe.is_some_and(|sp| *sp == pipe);
    let loaded = match self_pipe {
        Some(sp) if is_self => sp.clone(),
        _ => resolver.load_pipe(&pipe),
    };
    if chain.is_empty() {
        let resolved = with_resolved_parameters(&loaded, resolver, depth, warnings)?;
        return resolver.table_reference(&resolved).map(Value::Str);
    }

    let mut iter = chain.iter();
    let first = iter.next().expect("non-empty chain");
    let mut value = match first {
        Accessor::Attr(name) if is_self && name == "parameters" => {
            Value::Map(resolver.raw_parameters(&loaded))
        }
        Accessor::Attr(name) if is_self && name.starts_with("parameters") => {
            return Err(format!(
                "invalid syntax: pipe.attributes['parameters']{}",
                &name["parameters".len()..]
            ));
        }
        Accessor::Attr(name) if KEY_ATTRIBUTES.contains(&name.as_str()) => {
            resolver.attribute(&loaded, name)?
        }
        Accessor::Attr(name) => {
            let resolved = with_resolved_parameters(&loaded, resolver, depth, warnings)?;
            resolver.attribute(&resolved, name)?
        }
        Accessor::Index(Value::Str(key)) if key == "parameters" => {
            Value::Map(resolver.raw_parameters(&loaded))
        }
        Accessor::Index(Value::Str(key)) => {
            if KEY_ATTRIBUTES.contains(&key.as_str()) {
                loaded.get_item(key)
            } else {
                with_resolved_parameters(&loaded, resolver, depth, warnings)?.get_item(key)
            }
        }
        Accessor::Index(_) => {
            return Err("TypeError: attribute name must be string".to_string());
        }
    };
    for accessor in iter {
        value = match accessor {
            Accessor::Index(key) => index_value(&value, key)?,
            Accessor::Attr(name) => {
                return Err(format!(
                    "'{}' object has no attribute '{name}'",
                    value.type_name()
                ));
            }
        };
    }
    Ok(value)
}

/// The result of [`replace_pipes_syntax`].
#[derive(Debug, Clone, PartialEq)]
pub struct Replaced {
    /// The substituted text, or the typed value when the text is a single template.
    pub value: Value,
    /// Warnings for templates that failed to resolve (they are left as written).
    pub warnings: Vec<String>,
}

/// Port of `replace_pipes_syntax(text, _pipe=self_pipe)`.
pub fn replace_pipes_syntax(
    text: &str,
    self_pipe: Option<&Pipe>,
    resolver: &dyn TemplateResolver,
) -> Replaced {
    replace_at_depth(text, self_pipe, resolver, 0)
}

pub(crate) fn replace_at_depth(
    text: &str,
    self_pipe: Option<&Pipe>,
    resolver: &dyn TemplateResolver,
    depth: usize,
) -> Replaced {
    let parsed = parse(text, self_pipe.is_some());
    let mut warnings = Vec::new();
    if parsed.templates.is_empty() {
        return Replaced {
            value: Value::Str(text.to_string()),
            warnings,
        };
    }
    let nonce = uuid::Uuid::new_v4().simple().to_string();
    let placeholders: Vec<String> = (0..parsed.templates.len())
        .map(|i| format!("__mrsm_pipe_placeholder_{nonce}{i:04}__"))
        .collect();

    let mut substituted = text.to_string();
    for (ph, t) in placeholders.iter().zip(&parsed.templates) {
        substituted = substituted.replace(&t.raw, ph);
    }

    let mut resolved = Vec::new();
    for t in &parsed.templates {
        match evaluate_at_depth(t, self_pipe, resolver, depth, &mut warnings) {
            Ok(v) => resolved.push(v),
            Err(e) => {
                warnings.push(format!("Failed to resolve pipe syntax '{}': {e}", t.raw));
                resolved.push(Value::Str(t.raw.clone()));
            }
        }
    }

    if parsed.templates.len() == 1 && text.trim() == parsed.templates[0].raw {
        return Replaced {
            value: resolved.into_iter().next().unwrap_or(Value::Null),
            warnings,
        };
    }

    let mut out = substituted;
    for (ph, value) in placeholders.iter().zip(&resolved) {
        let rendered = match value {
            Value::Map(_)
            | Value::List(_)
            | Value::Bool(_)
            | Value::Int(_)
            | Value::Float(_)
            | Value::Null => json_dumps(value, JsonStyle::Default, true),
            Value::Str(s) => s.clone(),
        };
        out = out.replace(ph, &rendered);
    }
    Replaced {
        value: Value::Str(out),
        warnings,
    }
}

/// A statically extracted `Pipe(...)` reference.
#[derive(Debug, Clone, PartialEq)]
pub struct PipeReference {
    /// The template text.
    pub raw: String,
    /// The referenced pipe (with the referencing pipe's instance when none is given).
    pub pipe: Pipe,
    /// The attribute chain (empty for a table reference).
    pub chain: Vec<Accessor>,
}

/// Every `Pipe(...)` reference in a text that can be constructed, in order of appearance.
/// `self_pipe` supplies the default instance, as in [`replace_pipes_syntax`].
pub fn referenced_pipes(
    text: &str,
    self_pipe: Option<&Pipe>,
    defaults: &PipeDefaults,
) -> Vec<PipeReference> {
    let parsed = parse(text, false);
    let mut out = Vec::new();
    for t in parsed.templates {
        if self_pipe.is_some() && t.raw.contains("self") {
            continue;
        }
        let TemplateTarget::Pipe {
            call: Ok((args, mut kwargs)),
            ..
        } = t.target
        else {
            continue;
        };
        if let Some(sp) = self_pipe {
            if !["instance", "mrsm_instance", "instance_keys"]
                .iter()
                .any(|k| kwargs.contains_key(*k))
            {
                kwargs.insert(
                    "instance".into(),
                    Value::Str(sp.instance_keys().to_string()),
                );
            }
        }
        if let Ok(pipe) = Pipe::from_call(&args, &kwargs, defaults) {
            out.push(PipeReference {
                raw: t.raw,
                pipe,
                chain: t.chain.unwrap_or_default(),
            });
        }
    }
    out
}

/// Every `Pipe(...)` reference in a parameters tree (string values of nested dicts and lists,
/// the same values `get_parameters` substitutes), deduplicated by pipe keys.
pub fn referenced_pipes_in_parameters(
    parameters: &Map,
    self_pipe: Option<&Pipe>,
    defaults: &PipeDefaults,
) -> Vec<Pipe> {
    fn walk(v: &Value, self_pipe: Option<&Pipe>, d: &PipeDefaults, out: &mut Vec<Pipe>) {
        match v {
            Value::Map(m) => m.values().for_each(|x| walk(x, self_pipe, d, out)),
            Value::List(l) => l.iter().for_each(|x| walk(x, self_pipe, d, out)),
            Value::Str(s) => {
                for r in referenced_pipes(s, self_pipe, d) {
                    if !out.contains(&r.pipe) {
                        out.push(r.pipe);
                    }
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    for v in parameters.values() {
        walk(v, self_pipe, defaults, &mut out);
    }
    out
}

/// Apply [`replace_pipes_syntax`] to every string in a parameters tree (`recursive_replace` in
/// `get_parameters`).
pub fn replace_in_parameters(
    parameters: &Map,
    self_pipe: Option<&Pipe>,
    resolver: &dyn TemplateResolver,
    warnings: &mut Vec<String>,
) -> Map {
    replace_in_parameters_at_depth(parameters, self_pipe, resolver, warnings, 0)
}

pub(crate) fn replace_in_parameters_at_depth(
    parameters: &Map,
    self_pipe: Option<&Pipe>,
    resolver: &dyn TemplateResolver,
    warnings: &mut Vec<String>,
    depth: usize,
) -> Map {
    fn walk(
        v: &Value,
        self_pipe: Option<&Pipe>,
        r: &dyn TemplateResolver,
        warnings: &mut Vec<String>,
        depth: usize,
    ) -> Value {
        match v {
            Value::Map(m) => Value::Map(
                m.iter()
                    .map(|(k, x)| (k.clone(), walk(x, self_pipe, r, warnings, depth)))
                    .collect(),
            ),
            Value::List(l) => Value::List(
                l.iter()
                    .map(|x| walk(x, self_pipe, r, warnings, depth))
                    .collect(),
            ),
            Value::Str(s) => {
                let out = replace_at_depth(s, self_pipe, r, depth);
                warnings.extend(out.warnings);
                out.value
            }
            other => other.clone(),
        }
    }
    parameters
        .iter()
        .map(|(k, v)| (k.clone(), walk(v, self_pipe, resolver, warnings, depth)))
        .collect()
}

/// `str()` of a resolved value, for callers that need text (SQL definitions).
pub fn value_to_text(value: &Value) -> String {
    py_str(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolver() -> InMemoryResolver {
        let src = Pipe::builder("sql:main", "src")
            .parameters(
                Value::from_json_str(
                    r#"{"columns": {"datetime": "ts", "id": "id"}, "target": "source_table"}"#,
                )
                .unwrap()
                .as_map()
                .unwrap()
                .clone(),
            )
            .build()
            .unwrap();
        InMemoryResolver {
            defaults: PipeDefaults::default(),
            pipes: vec![src],
        }
    }

    #[test]
    fn table_and_attributes() {
        let r = resolver();
        let out = replace_pipes_syntax("SELECT * FROM {{ Pipe('sql:main', 'src') }}", None, &r);
        assert_eq!(out.value, Value::from("SELECT * FROM source_table"));
        let out = replace_pipes_syntax(" {{ Pipe('sql:main', 'src').columns }} ", None, &r);
        assert_eq!(
            out.value,
            Value::from_json_str(r#"{"datetime": "ts", "id": "id"}"#).unwrap()
        );
        let out = replace_pipes_syntax(
            "x = {{ Pipe('sql:main', 'src').columns['datetime'] }} and {{ Pipe('sql:main', 'src').columns }}",
            None,
            &r,
        );
        assert_eq!(
            out.value,
            Value::from(r#"x = ts and {"datetime": "ts", "id": "id"}"#)
        );
    }

    #[test]
    fn self_and_failures() {
        let r = resolver();
        let me = Pipe::builder("plugin:x", "me")
            .parameters(
                Value::from_json_str(r#"{"a": {"b": 5}}"#)
                    .unwrap()
                    .as_map()
                    .unwrap()
                    .clone(),
            )
            .build()
            .unwrap();
        let out = replace_pipes_syntax("{{ self.parameters['a']['b'] }}", Some(&me), &r);
        assert_eq!(out.value, Value::Int(5));
        let out = replace_pipes_syntax("m={{ self.metric }}", Some(&me), &r);
        assert_eq!(out.value, Value::from("m=me"));
        let out = replace_pipes_syntax("{{ Pipe('a', 'b')[-1] }}", None, &r);
        assert_eq!(out.value, Value::from("{{ Pipe('a', 'b')[-1] }}"));
        assert_eq!(out.warnings.len(), 1);
    }

    #[test]
    fn extraction() {
        let refs = referenced_pipes(
            "SELECT * FROM {{ Pipe('sql:main', 'a') }} JOIN {{ mrsm.Pipe('sql:main', 'b', 'x', instance='sql:other').target }}",
            None,
            &PipeDefaults::default(),
        );
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[1].pipe.instance_keys(), "sql:other");
        assert_eq!(refs[1].chain, vec![Accessor::Attr("target".into())]);
    }
}
